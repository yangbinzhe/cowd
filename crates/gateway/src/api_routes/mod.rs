// ── API Routes ─────────────────────────────────────────────────
// Core gateway routes shared between TUI and HTTP API.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::State as AxumState,
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json},
    Router,
};
use runtime::approval_gate::SmartApprovalGate;
#[cfg(test)]
use runtime::ApprovalConfig;
#[cfg(test)]
use runtime::{
    ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextRole, ContextRuntimeKernel,
    ContextSourceKind,
};
use serde::Serialize;

use runtime::ProfileManager;
use tools::ToolCatalog;

use crate::event_bus::SessionEventBus;
#[cfg(test)]
use crate::gateway::ActiveSessions;
use crate::services::GatewayServices;
#[cfg(test)]
use crate::session_kernel::SessionKernel;
#[cfg(test)]
use crate::task_kernel::TaskKernel;
#[cfg(test)]
use memory::cognitive::CognitiveContextManager;
use memory::session_store::UnifiedSessionStore;
use memory::store::session::SessionRecord;
#[cfg(test)]
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
#[cfg(test)]
use memory::MemoryScope;

mod agent_routes;
mod approval_routes;
mod audit_routes;
mod capability_contract;
pub(crate) mod connector_routes;
mod context_routes;
mod core_routes;
mod cross_plane_routes;
mod edge_routes;
mod evolution_routes;
mod growth_routes;
mod harness_eval_routes;
mod matrix_outcomes;
mod matrix_routes;
pub(crate) mod memory_routes;
mod message_connector_routes;
mod message_routes;
mod mfg_outcomes;
mod mfg_routes;
mod mission_routes;
mod profile_routes;
mod public_routes;
mod reality_routes;
mod resource_routes;
pub(crate) mod route_manifest;
mod runtime_routes;
mod session_routes;
mod skill_routes;
mod slash_routes;
mod surface_routes;
mod system_routes;
mod task_routes;
mod workspace_routes;

// ── Shared application state ───────────────────────────────────

pub struct AppState {
    pub tool_registry: Arc<ToolCatalog>,
    pub config: Option<serde_json::Value>,
    pub event_bus: Arc<SessionEventBus>,
    pub static_webui: crate::gateway_static::StaticWebUiSource,
    pub approval_gate: Option<Arc<SmartApprovalGate>>,
    pub auth_token: Option<String>,
    pub workspace_root: PathBuf,
    pub config_home: PathBuf,
    pub profile_id: String,
    pub profile_manager: Arc<ProfileManager>,
    pub services: Arc<GatewayServices>,
    pub session_lease_registry: Option<Arc<session::SessionLeaseRegistry>>,
}

impl AppState {
    pub(crate) fn startup_config_snapshot(&self) -> Option<&serde_json::Value> {
        self.config.as_ref()
    }

    pub(crate) fn runtime_config_json_snapshot(&self) -> Option<serde_json::Value> {
        self.services
            .system
            .runtime_config_json(&self.workspace_root, &self.config_home)
            .ok()
            .or_else(|| self.config.clone())
    }

    pub(crate) fn redacted_runtime_config_json_snapshot(&self) -> Option<serde_json::Value> {
        self.services
            .system
            .redacted_runtime_config_json(&self.workspace_root, &self.config_home)
            .ok()
            .or_else(|| {
                self.config
                    .clone()
                    .map(|value| self.services.system.redact_config_json(value))
            })
    }
}

impl AppState {
    pub(crate) fn has_unified_store(&self) -> bool {
        self.services.session.has_unified_store()
    }

    fn event_bus(&self) -> Arc<SessionEventBus> {
        self.services
            .session
            .event_bus()
            .unwrap_or_else(|| Arc::clone(&self.event_bus))
    }

    pub(crate) fn list_active_session_ids(&self) -> Vec<String> {
        self.services.session.list_active_session_ids()
    }
}

// ── Auth middleware ────────────────────────────────────────────

/// If `auth_token` is set on `AppState`, require `Authorization: Bearer <token>`.
/// When `auth_token` is `None`, all requests are allowed (no auth configured).
async fn auth_middleware(
    AxumState(state): AxumState<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if let Some(token) = &state.auth_token {
        if is_webui_internal_request(request.headers()) {
            return Ok(next.run(request).await);
        }

        let auth_header = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v: &axum::http::HeaderValue| v.to_str().ok());

        match auth_header {
            Some(h) if h == format!("Bearer {token}") => Ok(next.run(request).await),
            _ => Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "unauthorized".to_string(),
                }),
            )),
        }
    } else {
        Ok(next.run(request).await)
    }
}

fn is_webui_internal_request(headers: &HeaderMap) -> bool {
    let Some(fetch_site) = headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    if fetch_site != "same-origin" {
        return false;
    }

    let destination = headers
        .get("sec-fetch-dest")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    destination == "empty" || destination == "document"
}

// ── Router ─────────────────────────────────────────────────────

pub fn api_router(state: Arc<AppState>) -> Router {
    let public_routes = public_routes::router();

    let protected_routes = Router::new()
        .merge(approval_routes::router())
        .merge(agent_routes::router())
        .merge(audit_routes::router())
        .merge(message_connector_routes::router())
        .merge(connector_routes::router())
        .merge(context_routes::router())
        .merge(core_routes::router())
        .merge(cross_plane_routes::router())
        .merge(edge_routes::router())
        .merge(evolution_routes::router())
        .merge(growth_routes::router())
        .merge(harness_eval_routes::router())
        .merge(matrix_routes::router())
        .merge(mfg_routes::router())
        .merge(mission_routes::router())
        .merge(memory_routes::router())
        .merge(message_routes::router())
        .merge(profile_routes::router())
        .merge(reality_routes::router())
        .merge(resource_routes::router())
        .merge(runtime_routes::router())
        .merge(session_routes::router())
        .merge(skill_routes::router())
        .merge(slash_routes::router())
        .merge(surface_routes::router())
        .merge(system_routes::router())
        .merge(task_routes::router())
        .merge(workspace_routes::router())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    public_routes.merge(protected_routes).with_state(state)
}

// ── Response types ─────────────────────────────────────────────

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn default_config_home() -> PathBuf {
    std::env::var_os("COWD_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| PathBuf::from(".cowd"))
}

pub(crate) fn new_api_session_record(session_id: &str, model: Option<String>) -> SessionRecord {
    let now = chrono::Utc::now().to_rfc3339();
    let title = format!("Session {}", session_id.chars().take(8).collect::<String>());
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "api_server".to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(serde_json::json!({ "title": title }).to_string()),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

pub(crate) async fn sync_runtime_session_metadata_to_store(
    store: &UnifiedSessionStore,
    session_id: &str,
    session: &runtime::Session,
) -> Result<(), String> {
    let now = chrono::Utc::now().to_rfc3339();
    let existing_record = store
        .get_session(session_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut record = existing_record
        .clone()
        .unwrap_or_else(|| new_api_session_record(session_id, session.model.clone()));

    record.model = session.model.clone().or(record.model);
    record.last_activity = now;
    record.message_count = session.messages.len() as i64;
    record.input_tokens = session
        .messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .map(|u| i64::from(u.input_tokens))
        .sum();
    record.output_tokens = session
        .messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .map(|u| i64::from(u.output_tokens))
        .sum();

    if existing_record.is_some() {
        store
            .update_session(&record)
            .await
            .map_err(|e| e.to_string())?;
    } else {
        store
            .create_session(&record)
            .await
            .map_err(|e| e.to_string())?;
    }

    store
        .delete_messages_from(session_id, 0)
        .await
        .map_err(|e| e.to_string())?;
    store
        .delete_events_by_type_from(session_id, "message_appended", 0)
        .await
        .map_err(|e| e.to_string())?;

    let mut message_events = Vec::with_capacity(session.messages.len());
    for (sequence, message) in session.messages.iter().enumerate() {
        let message_record = message.to_session_message(session_id, sequence);
        store
            .insert_message(&message_record)
            .await
            .map_err(|e| e.to_string())?;

        let message_json = serde_json::from_str::<serde_json::Value>(&message.to_json().render())
            .unwrap_or(serde_json::Value::Null);
        message_events.push(memory::SessionEvent {
            session_id: session_id.to_string(),
            event_type: "message_appended".to_string(),
            event_json: serde_json::json!({
                "type": "message_appended",
                "sequence": sequence,
                "role": message.role.role_str(),
                "message": message_json,
            })
            .to_string(),
            sequence,
            created_at_ms: message_record.created_at_ms,
        });
    }
    store
        .append_events_allocating_sequence(&message_events)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn api_error(status: StatusCode, error: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
        }),
    )
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        body::Body,
        http::{Request, StatusCode},
    };
    use memory::config::{BudgetConfig, StoreConfig};
    use runtime::permission_enforcer::DestructivePatternDetector;
    use runtime::{ContextProfile, ResumeContextSource};
    use std::sync::Arc;
    use std::time::Instant;
    use tower::ServiceExt;

    struct ApprovalResumeTestExecutor;

    struct CrossPlaneApprovalTestBackend {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl runtime::execution_core::ScopedNodeBackend for CrossPlaneApprovalTestBackend {
        async fn execute(
            &self,
            ticket: &runtime::execution_core::NodeExecutionTicket,
        ) -> Result<
            runtime::execution_core::NodeExecutionOutcome,
            runtime::execution_core::NodeExecutorError,
        > {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(runtime::execution_core::NodeExecutionOutcome::new(
                harness_contract::execution_graph::ExecutionNodeResult {
                    status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                    result_ref: Some(format!("cross-plane-sent:{}", ticket.node_id)),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }

    #[async_trait::async_trait]
    impl runtime::execution_core::NodeExecutor for ApprovalResumeTestExecutor {
        fn kind(&self) -> &str {
            "approval_resume_test_tool"
        }

        fn validate(
            &self,
            _node: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), runtime::execution_core::NodeExecutorError> {
            Ok(())
        }

        async fn start(
            &self,
            context: runtime::execution_core::NodeExecutionContext,
        ) -> Result<
            runtime::execution_core::NodeExecutionTicket,
            runtime::execution_core::NodeExecutorError,
        > {
            Ok(runtime::execution_core::NodeExecutionTicket {
                graph_id: context.graph.id.clone(),
                node_id: context.node.id,
                executor_kind: self.kind().to_string(),
                attempt: context.attempt,
                idempotency_key: context.node.idempotency_key,
                payload_ref: context.node.payload_ref,
            })
        }

        async fn poll_or_await(
            &self,
            ticket: &runtime::execution_core::NodeExecutionTicket,
        ) -> Result<
            runtime::execution_core::NodeExecutionOutcome,
            runtime::execution_core::NodeExecutorError,
        > {
            Ok(runtime::execution_core::NodeExecutionOutcome::new(
                harness_contract::execution_graph::ExecutionNodeResult {
                    status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                    result_ref: Some(format!("tool-result:{}", ticket.node_id)),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }

    #[derive(Clone, Default)]
    struct CapturedTraceEvents {
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    static TRACE_CAPTURE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    static MISSION_ROUTE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();

    fn trace_capture_lock() -> &'static tokio::sync::Mutex<()> {
        TRACE_CAPTURE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn mission_route_lock() -> &'static tokio::sync::Mutex<()> {
        MISSION_ROUTE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    impl CapturedTraceEvents {
        fn lines(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    struct TraceFieldVisitor {
        fields: Vec<String>,
    }

    impl tracing::field::Visit for TraceFieldVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields.push(format!("{}={value:?}", field.name()));
        }
    }

    impl<S> tracing_subscriber::Layer<S> for CapturedTraceEvents
    where
        S: tracing::Subscriber,
    {
        fn register_callsite(
            &self,
            _metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::always()
        }

        fn enabled(
            &self,
            _metadata: &tracing::Metadata<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) -> bool {
            true
        }

        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = TraceFieldVisitor { fields: Vec::new() };
            event.record(&mut visitor);
            self.events.lock().unwrap().push(format!(
                "{} {} {}",
                event.metadata().level(),
                event.metadata().target(),
                visitor.fields.join(" ")
            ));
        }
    }

    fn test_profile_manager() -> Arc<ProfileManager> {
        let dir = std::env::temp_dir().join(format!("cowd-api-profiles-{}", uuid::Uuid::new_v4()));
        let manager = Arc::new(ProfileManager::new_with_profiles_dir(dir));
        manager.initialize().unwrap();
        manager
    }

    fn test_session_kernel(
        sessions: Arc<ActiveSessions>,
        store: Option<Arc<UnifiedSessionStore>>,
        event_bus: Arc<SessionEventBus>,
    ) -> Arc<SessionKernel> {
        Arc::new(SessionKernel::new(sessions, store, event_bus))
    }

    fn test_task_kernel() -> Arc<TaskKernel> {
        let path =
            std::env::temp_dir().join(format!("cowd-api-task-{}.json", uuid::Uuid::new_v4()));
        Arc::new(TaskKernel::open(path).expect("task kernel should open"))
    }

    fn test_services(
        session_kernel: Arc<SessionKernel>,
        task_kernel: Arc<TaskKernel>,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
    ) -> Arc<crate::services::GatewayServices> {
        let sessions = Arc::new(ActiveSessions::new());
        let lifecycle_kernel =
            Arc::new(crate::session_lifecycle_kernel::SessionLifecycleKernel::new());
        let runtime_services =
            runtime::RuntimeServices::in_memory().expect("test runtime services");
        let runtime_store = session_kernel.unified_store().unwrap_or_else(|| {
            Arc::new(UnifiedSessionStore::open_in_memory().expect("test session store"))
        });
        runtime_services
            .install_session_store(runtime_store)
            .expect("test session router");
        let runtime = Arc::new(
            crate::runtime_service::RuntimeService::new(
                sessions,
                Arc::new(session::SessionLeaseRegistry::default()),
                session_kernel,
                lifecycle_kernel,
                Instant::now(),
                Arc::new(runtime::ProviderRegistry::empty()),
                Arc::new(runtime::UpgradeCoordinator::new()),
                runtime_services,
            )
            .expect("test runtime service"),
        );
        let approval_dir =
            std::env::temp_dir().join(format!("cowd-api-approval-{}", uuid::Uuid::new_v4()));
        let approval_repository = approval::FileApprovalRepository::new(
            approval_dir.join("approval_history.json"),
            approval_dir.join("always_approved.json"),
        );
        Arc::new(crate::services::GatewayServices::new_with_config_home(
            runtime,
            task_kernel,
            surface_host.unwrap_or_else(|| Arc::new(crate::surface_host::SurfaceHost::default())),
            None,
            test_approval_gate(),
            approval_repository,
            isolated_test_config_home(),
        ))
    }

    pub(crate) fn test_state() -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new(); // returns Arc<Self>
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        })
    }

    fn test_state_with_config(config: serde_json::Value) -> Arc<AppState> {
        test_state_with_config_and_runtime(config, None)
    }

    fn test_state_with_lease_registry(
        registry: Arc<session::SessionLeaseRegistry>,
    ) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: Some(registry),
        })
    }

    fn test_state_with_config_and_runtime(
        config: serde_json::Value,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
    ) -> Arc<AppState> {
        test_state_with_config_runtime_and_workspace(
            config,
            surface_host,
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        )
    }

    fn test_state_with_config_runtime_and_workspace(
        config: serde_json::Value,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
        workspace_root: PathBuf,
    ) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        let config_home = isolated_test_config_home_with_config(&config);
        Arc::new(AppState {
            tool_registry: tools,
            config: Some(config),
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root,
            config_home,
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, surface_host),
            session_lease_registry: None,
        })
    }

    fn unique_test_workspace(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cowd-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn isolated_test_config_home() -> PathBuf {
        unique_test_workspace("config-home")
    }

    fn isolated_test_config_home_with_config(config: &serde_json::Value) -> PathBuf {
        let path = isolated_test_config_home();
        let rendered = serde_yaml::to_string(config).expect("test config renders as yaml");
        std::fs::write(path.join("config.yaml"), rendered).expect("test config writes");
        path
    }

    fn test_state_with_store(store: Arc<UnifiedSessionStore>) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel =
            test_session_kernel(sessions.clone(), Some(store.clone()), event_bus.clone());
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        })
    }

    fn test_state_with_store_and_workspace(
        store: Arc<UnifiedSessionStore>,
        workspace_root: PathBuf,
        config_home: PathBuf,
    ) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel =
            test_session_kernel(sessions.clone(), Some(store.clone()), event_bus.clone());
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root,
            config_home,
            profile_id: "enterprise".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        })
    }

    fn test_memory_config(sqlite_path: &std::path::Path) -> memory::MemoryConfig {
        memory::MemoryConfig {
            store: StoreConfig {
                sqlite_path: sqlite_path.to_path_buf(),
                blob_dir: sqlite_path.parent().unwrap().join("blobs"),
                ..Default::default()
            },
            budget: BudgetConfig {
                context_window: 8_000,
                reserved_system: 2_000,
                reserved_response: 1_000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn test_state_with_memory(memory_manager: Arc<CognitiveContextManager>) -> Arc<AppState> {
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: Arc::new(
                crate::services::GatewayServices::with_memory_for_tests(memory_manager)
                    .with_task_kernel_for_tests(task_kernel),
            ),
            session_lease_registry: None,
        })
    }

    fn test_state_with_memory_and_workspace(
        memory_manager: Arc<CognitiveContextManager>,
        workspace_root: PathBuf,
    ) -> Arc<AppState> {
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root,
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: Arc::new(
                crate::services::GatewayServices::with_memory_for_tests(memory_manager)
                    .with_task_kernel_for_tests(task_kernel),
            ),
            session_lease_registry: None,
        })
    }

    fn test_approval_gate() -> Arc<SmartApprovalGate> {
        Arc::new(SmartApprovalGate::new(
            Arc::new(DestructivePatternDetector::new(std::env::temp_dir())),
            ApprovalConfig::default(),
            None,
        ))
    }

    fn test_state_with_approval_gate(gate: Arc<SmartApprovalGate>) -> Arc<AppState> {
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: Some(gate.clone()),
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: Arc::new(
                crate::services::GatewayServices::with_approval_for_tests(gate)
                    .with_task_kernel_for_tests(task_kernel),
            ),
            session_lease_registry: None,
        })
    }

    fn test_state_with_workspace(workspace_root: PathBuf, config_home: PathBuf) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root,
            config_home,
            profile_id: "enterprise".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        })
    }

    #[test]
    fn session_service_shares_session_kernel_handles() {
        let state = test_state_with_store(Arc::new(UnifiedSessionStore::open_in_memory().unwrap()));

        assert!(Arc::ptr_eq(
            &state
                .services
                .session
                .event_bus()
                .expect("service event bus should exist"),
            &state.event_bus
        ));
        assert!(Arc::ptr_eq(
            &state
                .services
                .session
                .unified_store()
                .expect("service store should exist"),
            &state
                .services
                .session
                .unified_store()
                .expect("service store should exist")
        ));
    }

    #[tokio::test]
    async fn runtime_outbox_management_reports_poison_and_retries_both_directions() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record("outbox-session", None))
            .await
            .unwrap();
        let state = test_state_with_store(Arc::clone(&store));
        let request = memory::SessionRuntimeOutboxRequest {
            request_id: "ingress-poison".to_string(),
            turn_id: "turn-1".to_string(),
            message_id: "user-1".to_string(),
            created_at_ms: 1,
        };
        store
            .append_ingress_with_runtime_outbox(
                "outbox-session",
                "user",
                Some("[{\"type\":\"text\",\"text\":\"hello\"}]"),
                1,
                &request,
            )
            .await
            .unwrap();
        let ingress_claim = store
            .claim_session_runtime_outbox("test-worker", 1, 10, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        store
            .fail_session_runtime_outbox(
                "ingress-poison",
                "test-worker",
                ingress_claim.revision,
                memory::OutboxFailureClass::CorruptPayload,
                "bad payload",
                2,
                1,
                2,
            )
            .await
            .unwrap();
        let event_store = state
            .services
            .runtime
            .as_ref()
            .unwrap()
            .runtime_services()
            .event_store()
            .clone();
        event_store
            .enqueue_session_terminal(
                "terminal-poison",
                "assistant-1",
                "outbox-session",
                9,
                "bad payload",
            )
            .unwrap();
        let terminal_claim = event_store
            .claim_session_terminals("test-worker", 1, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        event_store
            .fail_session_terminal(
                "terminal-poison",
                "test-worker",
                terminal_claim.revision,
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "bad payload",
                2,
                1,
                2,
            )
            .unwrap();

        let app = api_router(state);
        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/outbox")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let body = to_bytes(status.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["healthy"], false);
        assert_eq!(json["ingress"]["poison"][0]["request_id"], "ingress-poison");
        assert_eq!(
            json["terminal"]["poison"][0]["terminal_id"],
            "terminal-poison"
        );

        for (direction, id) in [
            ("ingress", "ingress-poison"),
            ("terminal", "terminal-poison"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/runtime/outbox/{direction}/{id}/retry"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"actor":"operator","reason":"repaired"}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    fn test_temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cowd-api-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    async fn wait_for_harness_eval_route_status(
        app: axum::Router,
        run_id: &str,
        expected: &str,
    ) -> serde_json::Value {
        for _ in 0..40 {
            let detail = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/harness-eval/runs/{run_id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = to_bytes(detail.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if json["run"]["status"] == expected {
                return json;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let detail = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/harness-eval/runs/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn gateway_capability_contract_endpoints_are_available() {
        let app = api_router(test_state());
        for uri in [
            "/api/gateway/capability-contract",
            "/api/gateway/openapi.json",
            "/api/gateway/openai-tools",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            match uri {
                "/api/gateway/capability-contract" => {
                    assert_eq!(json["kind"], "gateway.capability_contract");
                    assert!(json["route_count"].as_u64().unwrap_or_default() > 50);
                    assert!(json["capabilities"].as_array().is_some_and(|items| {
                        items.iter().any(|capability| {
                            capability["http"]["path"] == "/api/gateway/openapi.json"
                        })
                    }));
                }
                "/api/gateway/openapi.json" => {
                    assert_eq!(json["openapi"], "3.1.0");
                    assert!(json["paths"]["/api/gateway/capability-contract"]["get"].is_object());
                }
                "/api/gateway/openai-tools" => {
                    assert_eq!(json["kind"], "gateway.openai_tools");
                    assert!(json["tools"].as_array().is_some_and(|items| {
                        items.iter().all(|tool| {
                            tool["type"] == "function"
                                && tool["function"]["name"].as_str().is_some()
                                && tool["function"]["parameters"]["type"] == "object"
                        })
                    }));
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn skill_maintenance_evaluate_route_calls_skill_service() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/skills/maintenance/evaluate")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "route-req-1",
                            "skill_id": "plan-review",
                            "selected_count": 5,
                            "success_count": 3,
                            "failure_count": 1,
                            "correction_count": 2,
                            "activation_gap_count": 0,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "skills.maintenance.evaluation");
        assert_eq!(json["request_id"], "route-req-1");
        assert_eq!(json["skill_id"], "plan-review");
        assert_eq!(json["action"], "generate_revision_candidate");
    }

    #[tokio::test]
    async fn skill_lifecycle_routes_create_and_list_real_runs() {
        let workspace = test_temp_dir("skill-lifecycle-workspace");
        let config_home = test_temp_dir("skill-lifecycle-config");
        let skill_root = workspace.join(".cowd").join("skills").join("route-demo");
        std::fs::create_dir_all(&skill_root).unwrap();
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: route-demo\ndescription: Route demo\n---\n# Route Demo\n",
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));
        let validate = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/skills/local:route-demo/actions/validate")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"session_id": "route-test"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(validate.status(), StatusCode::OK);
        let validate_body = to_bytes(validate.into_body(), usize::MAX).await.unwrap();
        let validate_json: serde_json::Value = serde_json::from_slice(&validate_body).unwrap();
        assert_eq!(validate_json["kind"], "skills.action.receipt");
        assert_eq!(validate_json["receipt"]["status"], "succeeded");
        let run_id = validate_json["run"]["run_id"].as_str().unwrap();

        let runs = app
            .oneshot(
                Request::builder()
                    .uri("/api/skills/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(runs.status(), StatusCode::OK);
        let runs_body = to_bytes(runs.into_body(), usize::MAX).await.unwrap();
        let runs_json: serde_json::Value = serde_json::from_slice(&runs_body).unwrap();
        assert_eq!(runs_json["kind"], "skills.runs");
        assert!(runs_json["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["run_id"] == run_id));

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn skill_translate_route_rejects_empty_content_before_model_call() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/skills/local:missing/translate")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::json!({"content": ""}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "content is required");
    }

    #[tokio::test]
    async fn branch_session_copies_stored_messages_into_new_session() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let source_id = "branch-source";
        let mut source = new_api_session_record(source_id, Some("test-model".into()));
        source.metadata_json = Some(serde_json::json!({"title": "Source Topic"}).to_string());
        source.message_count = 2;
        store.create_session(&source).await.unwrap();
        store
            .insert_messages_batch(&[
                memory::store::session::SessionMessage {
                    session_id: source_id.to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    content_json: serde_json::json!([{"type":"text","text":"hello"}]).to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 10,
                },
                memory::store::session::SessionMessage {
                    session_id: source_id.to_string(),
                    sequence: 1,
                    role: "assistant".to_string(),
                    content_json: serde_json::json!([{"type":"text","text":"world"}]).to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 11,
                },
            ])
            .await
            .unwrap();

        let app = api_router(test_state_with_store(store.clone()));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{source_id}/branch"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let branch_id = json["id"].as_str().expect("branch id should be returned");
        let copied = store.get_messages(branch_id, 0, 10).await.unwrap();
        assert_eq!(copied.len(), 2);
        assert_eq!(copied[0].session_id, branch_id);
        assert_eq!(copied[0].sequence, 0);
        assert!(copied[0].content_json.contains("hello"));
        let branch_record = store
            .get_session(branch_id)
            .await
            .unwrap()
            .expect("branch record should exist");
        assert_eq!(branch_record.message_count, 2);
        assert!(branch_record
            .metadata_json
            .as_deref()
            .unwrap_or_default()
            .contains("branch-source"));
        let source_events = store.get_events(source_id, 0).await.unwrap();
        assert!(source_events.iter().any(|event| {
            event.event_type == "SessionBranched"
                && event.event_json.contains(branch_id)
                && event.event_json.contains("\"copied_message_count\":2")
        }));
        let branch_events = store.get_events(branch_id, 0).await.unwrap();
        assert!(branch_events.iter().any(|event| {
            event.event_type == "BranchCreated"
                && event.event_json.contains(source_id)
                && event.event_json.contains("\"copied_message_count\":2")
        }));
    }

    #[tokio::test]
    async fn harness_eval_routes_create_smoke_run_and_report_latest() {
        let workspace = test_temp_dir("harness-eval-route-workspace");
        let report_dir = test_temp_dir("harness-eval-route-reports");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({ "eval_report_dir": report_dir }),
            None,
            workspace.clone(),
        ));

        let run = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/harness-eval/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "level": "quick",
                            "budget": "low",
                            "allow_real_model": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(run.status(), StatusCode::OK);
        let run_body = to_bytes(run.into_body(), usize::MAX).await.unwrap();
        let run_json: serde_json::Value = serde_json::from_slice(&run_body).unwrap();
        assert_eq!(run_json["kind"], "harness_eval.run");
        assert_eq!(run_json["run"]["status"], "running");
        let run_id = run_json["run"]["run_id"].as_str().unwrap();
        let completed_run =
            wait_for_harness_eval_route_status(app.clone(), run_id, "completed").await;
        assert_eq!(completed_run["run"]["status"], "completed");

        let latest = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/harness-eval/reports/latest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(latest.status(), StatusCode::OK);
        let latest_body = to_bytes(latest.into_body(), usize::MAX).await.unwrap();
        let latest_json: serde_json::Value = serde_json::from_slice(&latest_body).unwrap();
        assert_eq!(latest_json["kind"], "harness_eval.latest_report");
        assert_eq!(latest_json["report"]["status"], "passed");
        let report_id = latest_json["report"]["id"].as_str().unwrap();

        let artifacts = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/harness-eval/reports/{report_id}/artifacts"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(artifacts.status(), StatusCode::OK);
        let artifacts_body = to_bytes(artifacts.into_body(), usize::MAX).await.unwrap();
        let artifacts_json: serde_json::Value = serde_json::from_slice(&artifacts_body).unwrap();
        assert_eq!(artifacts_json["kind"], "harness_eval.artifacts");
        assert_eq!(artifacts_json["report_id"], report_id);
        assert!(artifacts_json["count"].as_u64().unwrap_or_default() > 0);

        let gate = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/harness-eval/reports/{report_id}/gate"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(gate.status(), StatusCode::OK);
        let gate_body = to_bytes(gate.into_body(), usize::MAX).await.unwrap();
        let gate_json: serde_json::Value = serde_json::from_slice(&gate_body).unwrap();
        assert_eq!(gate_json["kind"], "harness_eval.report_gate");
        assert_eq!(gate_json["report_gate"]["status"], "passed");

        let scenarios = app
            .oneshot(
                Request::builder()
                    .uri("/api/harness-eval/scenarios")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scenarios.status(), StatusCode::OK);
        let scenarios_body = to_bytes(scenarios.into_body(), usize::MAX).await.unwrap();
        let scenarios_json: serde_json::Value = serde_json::from_slice(&scenarios_body).unwrap();
        assert!(scenarios_json["next_gen_harness_closure"]
            .as_array()
            .is_some_and(|items| items.len() >= 7));

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(report_dir);
    }

    #[tokio::test]
    async fn evolution_routes_link_signal_proposal_skill_draft_and_sandbox_eval() {
        let workspace = test_temp_dir("evolution-route-workspace");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace.clone(),
        ));

        let signal = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/evolution/signals")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "signal_type": "memory_noise",
                            "source": {
                                "owner": "runtime",
                                "session_id": "session-1",
                                "agent_id": null,
                                "team_id": null,
                                "run_id": null
                            },
                            "evidence_refs": ["memory:packet:noise"],
                            "severity": "warning",
                            "summary": "memory packet contained unrelated context",
                            "suggested_action": "tighten scope and salience gates",
                            "immediate_task_can_continue": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(signal.status(), StatusCode::OK);
        let signal_body = to_bytes(signal.into_body(), usize::MAX).await.unwrap();
        let signal_json: serde_json::Value = serde_json::from_slice(&signal_body).unwrap();
        assert_eq!(signal_json["kind"], "evolution.signal");

        let proposal = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/evolution/proposals")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"signal_ids":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal.status(), StatusCode::OK);
        let proposal_body = to_bytes(proposal.into_body(), usize::MAX).await.unwrap();
        let proposal_json: serde_json::Value = serde_json::from_slice(&proposal_body).unwrap();
        let proposal_id = proposal_json["proposal"]["proposal_id"].as_str().unwrap();
        assert_eq!(
            proposal_json["diagnosis"]["root_cause_kind"],
            "memory_governance_gap"
        );
        assert_eq!(proposal_json["plan_draft"]["blocked_mainline_write"], true);

        let diagnoses = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/evolution/diagnoses")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(diagnoses.status(), StatusCode::OK);

        let draft = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/evolution/proposals/{proposal_id}/skill-draft"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(draft.status(), StatusCode::OK);
        let draft_body = to_bytes(draft.into_body(), usize::MAX).await.unwrap();
        let draft_json: serde_json::Value = serde_json::from_slice(&draft_body).unwrap();
        assert_eq!(draft_json["kind"], "skills.evolution_draft");
        assert!(draft_json["draft"]["markdown"]
            .as_str()
            .unwrap()
            .contains("Acceptance Gates"));

        let candidate = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/proposals/{proposal_id}/candidates"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "baseline_ref": "baseline:main",
                            "candidate_ref": "candidate:worktree"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(candidate.status(), StatusCode::OK);
        let candidate_body = to_bytes(candidate.into_body(), usize::MAX).await.unwrap();
        let candidate_json: serde_json::Value = serde_json::from_slice(&candidate_body).unwrap();
        let candidate_id = candidate_json["candidate"]["candidate_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(candidate_json["candidate"]["mainline_modified"], false);
        assert_eq!(candidate_json["plan"]["target_owner"], "reality_core");

        let candidates = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/evolution/candidates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(candidates.status(), StatusCode::OK);
        let candidates_body = to_bytes(candidates.into_body(), usize::MAX).await.unwrap();
        let candidates_json: serde_json::Value = serde_json::from_slice(&candidates_body).unwrap();
        assert_eq!(candidates_json["count"], 1);

        let blocked_adoption = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/candidates/{candidate_id}/adoption"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"approved_for_adoption"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(blocked_adoption.status(), StatusCode::BAD_REQUEST);

        let sandbox = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/candidates/{candidate_id}/run"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sandbox.status(), StatusCode::OK);
        let sandbox_body = to_bytes(sandbox.into_body(), usize::MAX).await.unwrap();
        let sandbox_json: serde_json::Value = serde_json::from_slice(&sandbox_body).unwrap();
        assert_eq!(sandbox_json["eval"]["mainline_modified"], false);
        assert_eq!(
            sandbox_json["eval"]["candidate_id"].as_str(),
            Some(candidate_id.as_str())
        );
        assert!(
            sandbox_json["eval"]["artifact_paths"]
                .as_array()
                .unwrap()
                .len()
                >= 3
        );

        let adopted = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/candidates/{candidate_id}/adoption"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"approved_for_adoption"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(adopted.status(), StatusCode::OK);
        let adopted_body = to_bytes(adopted.into_body(), usize::MAX).await.unwrap();
        let adopted_json: serde_json::Value = serde_json::from_slice(&adopted_body).unwrap();
        assert_eq!(adopted_json["receipt"]["accepted"], true);

        let comparison = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/candidates/{candidate_id}/evaluate"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(comparison.status(), StatusCode::OK);
        let comparison_body = to_bytes(comparison.into_body(), usize::MAX).await.unwrap();
        let comparison_json: serde_json::Value = serde_json::from_slice(&comparison_body).unwrap();
        assert_eq!(comparison_json["comparison"]["regression_count"], 0);

        let promotion = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/candidates/{candidate_id}/promote"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(promotion.status(), StatusCode::OK);
        let promotion_body = to_bytes(promotion.into_body(), usize::MAX).await.unwrap();
        let promotion_json: serde_json::Value = serde_json::from_slice(&promotion_body).unwrap();
        assert_eq!(promotion_json["promotion"]["accepted"], true);
        let version_id = promotion_json["promotion"]["version_record"]["version_id"]
            .as_str()
            .expect("version id");

        let rollback = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/versions/{version_id}/rollback"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rollback.status(), StatusCode::OK);
        let rollback_body = to_bytes(rollback.into_body(), usize::MAX).await.unwrap();
        let rollback_json: serde_json::Value = serde_json::from_slice(&rollback_body).unwrap();
        assert_eq!(rollback_json["rollback"]["accepted"], true);
        assert_eq!(rollback_json["memory"]["kind"], "recovery_pattern");

        let chain = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/evolution/chain/{proposal_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(chain.status(), StatusCode::OK);

        let decision = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/proposals/{proposal_id}/decision"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"decision":"approved"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(decision.status(), StatusCode::OK);
        let decision_body = to_bytes(decision.into_body(), usize::MAX).await.unwrap();
        let decision_json: serde_json::Value = serde_json::from_slice(&decision_body).unwrap();
        assert_eq!(decision_json["proposal"]["status"], "approved");
        assert_eq!(decision_json["mainline_modified"], false);

        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn gateway_health_reports_pid_addr_static_source() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["gateway"], "gateway-runtime-host");
        assert_eq!(json["api_router"], "gateway-api-router");
        assert!(json["process"]["pid_file"]
            .as_str()
            .unwrap()
            .contains("cowd"));
        assert!(json["process"]["addr_file"]
            .as_str()
            .unwrap()
            .contains("addr"));
        assert_eq!(json["static_webui"]["config_key"], "gateway.webui_dir");
        assert_eq!(json["static_webui"]["required"], false);
        assert_eq!(json["static_webui"]["status"], "missing_config");
        assert_eq!(json["runtime"]["session_kernel"], true);
        assert_eq!(json["runtime"]["event_bus"], true);
        assert!(
            json["storage"]["registry"]["handle_count"]
                .as_u64()
                .unwrap_or_default()
                >= 11
        );
        assert!(json["storage"]["registry"]["root"]
            .as_str()
            .unwrap()
            .contains("storage"));
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "storage.matrix.layout"));
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "growth.v1.init"
                && item["domain"] == "growth"
                && item["status"].as_str().is_some()));
        assert!(json["storage"]["locks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["domain"] == "tasks"));
    }

    #[tokio::test]
    async fn gateway_storage_health_reports_registry_and_locks() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let handles = json["storage"]["registry"]["handles"].as_array().unwrap();
        assert!(handles.iter().any(|item| item["domain"] == "session"));
        assert!(handles.iter().any(|item| item["domain"] == "memory"));
        assert!(handles.iter().any(|item| item["domain"] == "matrix"));
        assert!(handles
            .iter()
            .any(|item| item["domain"] == "resource_directory"));
        assert!(handles.iter().any(|item| item["domain"] == "tasks"));
        assert!(
            json["storage"]["locks"].as_array().unwrap().len() >= 7,
            "storage lock list should include all core sqlite domains"
        );
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "storage.tasks.layout"));
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "growth.v1.init"
                && item["description"] == "initialize growth durable event and promotion schema"));
    }

    #[tokio::test]
    async fn gateway_storage_health_reports_applied_growth_migration() {
        let tmp = std::env::temp_dir().join(format!(
            "cowd-gateway-growth-health-test-{}",
            uuid::Uuid::new_v4()
        ));
        let config_home = tmp.join("config");
        let registry = storage::StorageRegistry::default_for_config_home(&config_home);
        let handle = registry.sqlite_handle("growth").unwrap();
        std::fs::create_dir_all(handle.path.parent().unwrap()).unwrap();
        let conn = storage::SqliteConnectionFactory::default()
            .open_handle(handle)
            .unwrap();
        storage::MigrationRunner::run_sqlite_domain(
            &conn,
            handle,
            &crate::services::growth_storage_migrations(),
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home,
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "growth.v1.init"
                && item["domain"] == "growth"
                && item["status"] == "applied"
                && item["version"] == 1));
        let _ = std::fs::remove_dir_all(tmp);
    }

    #[tokio::test]
    async fn gateway_status_includes_storage_registry_summary() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["storage"]["registry"]["status"], "registered");
        assert!(
            json["storage"]["registry"]["handle_count"]
                .as_u64()
                .unwrap_or_default()
                >= 11
        );
        assert!(
            json["storage"]["registry"]["missing_count"]
                .as_u64()
                .unwrap_or_default()
                > 0
        );
    }

    #[tokio::test]
    async fn gateway_ready_reports_required_runtime_services() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let required = json["required"].as_array().unwrap();
        assert!(required.iter().any(|item| item == "gateway-runtime-host"));
        assert!(required.iter().any(|item| item == "gateway-api-router"));
        assert!(required.iter().any(|item| item == "session-kernel"));
        assert!(required.iter().any(|item| item == "storage-registry"));
        let old_required_webui = ["static", "webui", "index"].join("-");
        assert!(!required.iter().any(|item| item == &old_required_webui));
        assert!(json["optional"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "static-webui"));
    }

    #[tokio::test]
    async fn webui_manifest_explains_gateway_runtime_host_router_relationship() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/webui/manifest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["kind"], "cowd.webui.manifest");
        assert!(json.get("daemon").is_none());
        assert!(json.get("socket_transition").is_none());
        assert_eq!(json["runtime_host"], "gateway internal runtime host");
        assert_eq!(json["api_router"], "gateway service route table");
        assert_eq!(
            json["control_channel"],
            "runtime host local control channel"
        );
    }

    #[tokio::test]
    async fn runtime_turn_routes_submit_project_and_cancel_receipts() {
        let app = api_router(test_state());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/turns")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "session-turn-api",
                            "task_id": "task-turn-api",
                            "prompt": "verify runtime turn route",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let submitted: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(submitted["ok"], true);
        assert_eq!(submitted["dispatch"], "runtime_service");
        assert_eq!(submitted["turn"]["status"], "pending");
        let turn_id = submitted["turn"]["turn_id"]
            .as_str()
            .expect("turn id should be present")
            .to_string();

        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/turns/{turn_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let detail: serde_json::Value =
            serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(detail["turn"]["task_id"], "task-turn-api");

        let cancelled = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/runtime/turns/{turn_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cancelled: serde_json::Value =
            serde_json::from_slice(&to_bytes(cancelled.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(cancelled["ok"], true);
        assert_eq!(cancelled["turn"]["status"], "cancelled");

        let snapshot = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let snapshot: serde_json::Value =
            serde_json::from_slice(&to_bytes(snapshot.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(snapshot["turns"][0]["turn_id"], turn_id);
    }

    #[tokio::test]
    async fn mission_routes_expose_runtime_projection_and_session_control() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let session_id = format!("mission-route-test-{}", uuid::Uuid::new_v4());
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "verify mission route",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(created["ok"], true);
        assert_eq!(created["mission"]["kind"], "mission.runtime");
        assert!(created["mission"]["sessions"]
            .as_array()
            .expect("mission sessions")
            .iter()
            .any(|session| session["session_id"].as_str() == Some(session_id.as_str())));

        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/mission/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        let detail: serde_json::Value =
            serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(detail["kind"], "mission.session");
        assert_eq!(
            detail["session"]["session_id"].as_str(),
            Some(session_id.as_str())
        );

        let backgrounded = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/mission/sessions/{session_id}/background"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(backgrounded.status(), StatusCode::OK);
        let backgrounded: serde_json::Value = serde_json::from_slice(
            &to_bytes(backgrounded.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(backgrounded["receipt"]["status"], "accepted");
        assert!(backgrounded["mission"]["sessions"]
            .as_array()
            .expect("mission sessions")
            .iter()
            .any(
                |session| session["session_id"].as_str() == Some(session_id.as_str())
                    && session["status"].as_str() == Some("background")
            ));

        let projection = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/mission/projection")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(projection.status(), StatusCode::OK);
        let projection: serde_json::Value =
            serde_json::from_slice(&to_bytes(projection.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(projection["envelope"]["service"], "mission");
        assert!(projection["mission"]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| session["session_id"].as_str() == Some(session_id.as_str())));

        let interpreted = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control/interpret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "current_session_id": session_id,
                            "command_text": "dispatch pending mission work",
                            "execute": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(interpreted.status(), StatusCode::OK);
        let interpreted: serde_json::Value =
            serde_json::from_slice(&to_bytes(interpreted.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            interpreted["kind"],
            "mission_control.command_interpretation"
        );
        assert_eq!(interpreted["ok"], true);
        assert_eq!(interpreted["interpretation"]["status"], "interpreted");
        assert_eq!(
            interpreted["interpretation"]["target_kind"].as_str(),
            Some("dispatch")
        );
    }

    #[tokio::test]
    async fn mission_control_route_exposes_runtime_projection_and_command_router() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("mission-control-route-a-{suffix}");
        let session_b = format!("mission-control-route-b-{suffix}");

        let created_a = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control/command")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "target": "mission",
                            "action": "start_session",
                            "actor": "test-human",
                            "payload": {
                                "title": "mission control command a",
                                "session_id": session_a,
                            },
                            "evidence_refs": ["test:mission-control"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created_a.status(), StatusCode::OK);
        let created_a: serde_json::Value =
            serde_json::from_slice(&to_bytes(created_a.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(created_a["kind"], "mission_control.command_result");
        assert_eq!(created_a["ok"], true);
        assert_eq!(created_a["receipt"]["status"], "executed");
        assert_eq!(
            created_a["projection"]["kind"],
            "mission_control.projection"
        );

        let created_b = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control/command")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "target": "mission",
                            "action": "start_session",
                            "actor": "test-human",
                            "payload": {
                                "title": "mission control command b",
                                "session_id": session_b,
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created_b.status(), StatusCode::OK);

        let routed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control/command")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "target": { "session": { "session_id": session_a } },
                            "action": "route_to_session",
                            "actor": "test-human",
                            "payload": {
                                "target_session_id": session_b,
                                "command": "review from mission control"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(routed.status(), StatusCode::OK);
        let routed: serde_json::Value =
            serde_json::from_slice(&to_bytes(routed.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(routed["receipt"]["status"], "queued");

        let dispatch = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control/sessions/dispatch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "max_commands": 20,
                            "dispatch_mode": "mark_claimed_only",
                            "allow_background": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let dispatch: serde_json::Value =
            serde_json::from_slice(&to_bytes(dispatch.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            dispatch["kind"],
            "mission_control.session_dispatch_submission"
        );
        assert_eq!(dispatch["ok"], true);
        let dispatch_report = dispatch["result"].get("Ok").unwrap_or(&dispatch["result"]);
        assert!(dispatch_report["claimed"].as_u64().is_some());
        assert!(dispatch_report["receipts"]
            .as_array()
            .expect("execution graph submission receipts")
            .iter()
            .all(|receipt| receipt["graph_id"].as_str().is_some()
                && receipt["commit_cursor"].as_u64().is_some()));

        let stewards_before = runtime::global_steward_runtime_service().projection();
        let steward_started = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control/command")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "target": { "session": { "session_id": session_a } },
                            "action": "start_steward",
                            "actor": "test-human",
                            "payload": {
                                "objective": "supervise mission control route"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let steward_started: serde_json::Value = serde_json::from_slice(
            &to_bytes(steward_started.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(steward_started["receipt"]["status"], "failed");
        assert_eq!(
            steward_started["receipt"]["result"]["status"],
            "capability_unavailable"
        );
        assert_eq!(
            steward_started["receipt"]["result"]["capability"],
            "steward_execution"
        );
        assert_eq!(
            runtime::global_steward_runtime_service().projection(),
            stewards_before
        );

        let control = app
            .oneshot(
                Request::builder()
                    .uri("/api/mission/control")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(control.status(), StatusCode::OK);
        let control: serde_json::Value =
            serde_json::from_slice(&to_bytes(control.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(control["projection"]["kind"], "mission_control.projection");
        assert!(control["projection"]["sessions"].as_array().unwrap().len() >= 2);
        assert!(
            control["projection"]["event_digest"]["total_recent_events"]
                .as_u64()
                .unwrap_or_default()
                > 0
        );
        assert_eq!(
            control["projection"]["relations"]["kind"],
            "runtime.session_relations"
        );
    }

    #[tokio::test]
    async fn mission_routes_write_approvals_relations_proxies_and_routes() {
        let _guard = mission_route_lock().lock().await;
        let _env_guard = crate::test_process_env_lock();
        let app = api_router(test_state());
        let session_a = format!("mission-route-a-{}", uuid::Uuid::new_v4());
        let session_b = format!("mission-route-b-{}", uuid::Uuid::new_v4());
        for session_id in [&session_a, &session_b] {
            let created = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/mission/sessions")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::json!({
                                "title": format!("route session {session_id}"),
                                "session_id": session_id,
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(created.status(), StatusCode::CREATED);
        }

        let team = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/mission/sessions/{session_a}/teams/runtime"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "research architecture and review implementation",
                            "backend": "in_process"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(team.status(), StatusCode::OK);
        let team_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(team.into_body(), usize::MAX).await.unwrap()).unwrap();
        assert_eq!(team_json["ok"], true);
        assert!(team_json["team"]["graph_id"].as_str().is_some());
        assert!(matches!(
            team_json["status"].as_str(),
            Some("completed" | "blocked" | "failed" | "running")
        ));

        let approval = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/approvals")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "source": {
                                "kind": "session",
                                "session_id": session_a.clone(),
                                "agent_id": null,
                                "team_id": null,
                                "mission_id": "mission-a"
                            },
                            "action": "apply_patch",
                            "summary": "modify runtime",
                            "risk": "medium",
                            "evidence_refs": ["trace:1"],
                            "timeout_policy": "pending"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(approval.status(), StatusCode::CREATED);
        let approval_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(approval.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(approval_json["ok"], true);
        let approval_id = approval_json["approval"]["approval_id"]
            .as_str()
            .expect("approval id")
            .to_string();
        assert_eq!(approval_json["approval"]["status"], "pending");
        assert!(
            approval_json["approvals"]["pending_count"]
                .as_u64()
                .expect("pending count")
                >= 1
        );

        let approval_pending = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/pending")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(approval_pending.status(), StatusCode::OK);
        let approval_pending_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(approval_pending.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            approval_pending_json["kind"],
            "gateway.unified_approval_pending"
        );
        assert!(approval_pending_json["pending"]
            .as_array()
            .expect("pending")
            .iter()
            .any(|approval| approval["approval_id"].as_str() == Some(approval_id.as_str())));

        let approval_decision = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval_id,
                            "approved": true,
                            "persistence": "once",
                            "reason": "verified"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(approval_decision.status(), StatusCode::OK);
        let approval_decision_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(approval_decision.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            approval_decision_json["receipt"]["approval_id"].as_str(),
            Some(approval_id.as_str())
        );
        assert_eq!(approval_decision_json["receipt"]["status"], "approved");

        let relation = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/relations")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "from_session_id": session_a.clone(),
                            "to_session_id": session_b.clone(),
                            "kind": "reviews",
                            "summary": "A reviews B",
                            "evidence_refs": ["trace:2"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(relation.status(), StatusCode::CREATED);
        let relation_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(relation.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert!(
            relation_json["relations"]["relation_count"]
                .as_u64()
                .expect("relation count")
                >= 1
        );
        assert_eq!(relation_json["relation"]["from_session_id"], session_a);
        assert_eq!(relation_json["relation"]["to_session_id"], session_b);

        let proxy = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/proxies")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": session_b.clone(),
                            "summary": "B summary",
                            "evidence_refs": ["trace:3"],
                            "decisions": ["ship"],
                            "open_questions": ["risk?"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proxy.status(), StatusCode::OK);
        let proxy_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(proxy.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(proxy_json["proxy"]["session_id"], session_b);

        let routed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/route")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "from_session_id": session_a.clone(),
                            "target_ref": format!("@{session_b}"),
                            "command": "review"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(routed.status(), StatusCode::OK);
        let routed_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(routed.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            routed_json["receipt"]["resolved_session_id"].as_str(),
            Some(session_b.as_str())
        );
        assert_eq!(routed_json["routed"]["kind"], "mission.session_command");
        assert_eq!(
            routed_json["routed"]["command"]["target_session_id"].as_str(),
            Some(session_b.as_str())
        );
        assert!(routed_json["mission"]["routed_commands"]
            .as_array()
            .expect("routed commands")
            .iter()
            .any(|command| command["target_session_id"].as_str() == Some(session_b.as_str())));
    }

    #[tokio::test]
    async fn mission_routes_manage_steward_runtime_lifecycle() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let session_id = format!("mission-steward-session-{}", uuid::Uuid::new_v4());
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "steward controlled session",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let stewards_before = runtime::global_steward_runtime_service().projection();

        let started = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/stewards")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "mission_id": "mission-runtime",
                            "root_session_id": session_id,
                            "profile_id": "stewarded",
                            "objective": "supervise high risk implementation"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(started.status(), StatusCode::CREATED);
        let started_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(started.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(started_json["ok"], false);
        assert_eq!(started_json["status"], "capability_unavailable");
        assert_eq!(started_json["capability"], "steward_execution");
        assert_eq!(started_json["available_in"], "V8");
        assert_eq!(started_json["side_effects_started"], false);

        let tick_all = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/stewards/tick-all")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tick_all.status(), StatusCode::OK);
        let tick_all_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(tick_all.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(tick_all_json["ok"], false);
        assert_eq!(tick_all_json["status"], "capability_unavailable");
        assert_eq!(tick_all_json["capability"], "steward_execution");
        assert_eq!(tick_all_json["side_effects_started"], false);
        assert_eq!(
            runtime::global_steward_runtime_service().projection(),
            stewards_before
        );
    }

    #[tokio::test]
    async fn mission_projection_exposes_durable_mission_events() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let session_id = format!("runtime-events-session-{}", uuid::Uuid::new_v4());
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "runtime event session",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let events = app
            .oneshot(
                Request::builder()
                    .uri("/api/mission/projection")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);
        let events_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(events.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(events_json["mission"]["kind"], "mission.runtime");
        assert!(events_json["events"]
            .as_array()
            .or_else(|| events_json["mission"]["events"].as_array())
            .expect("events")
            .iter()
            .any(|event| event["event_type"].as_str() == Some("mission.session.started")));
    }

    #[tokio::test]
    async fn runtime_event_replay_does_not_gain_steward_work_from_unavailable_start() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let stewards_before = runtime::global_steward_runtime_service().projection();
        let steward_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/stewards")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "mission_id": "mission-replay",
                            "root_session_id": "session-replay",
                            "profile_id": "stewarded",
                            "objective": "verify recovery"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(steward_response.status(), StatusCode::CREATED);
        let steward_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(steward_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(steward_json["ok"], false);
        assert_eq!(steward_json["status"], "capability_unavailable");
        assert_eq!(steward_json["capability"], "steward_execution");
        assert_eq!(steward_json["side_effects_started"], false);
        assert_eq!(
            runtime::global_steward_runtime_service().projection(),
            stewards_before
        );

        let report = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/events/replay-report?limit=2000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(report.status(), StatusCode::OK);
        let report_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(report.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(report_json["kind"], "runtime.events.replay_report");
        assert!(!report_json["report"]["actions"]
            .as_array()
            .expect("actions")
            .iter()
            .any(|action| action["action"] == "pause_recovery_required"));
    }

    #[tokio::test]
    async fn cowd_capabilities_route_exposes_core_registry() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let capabilities = json["capabilities"].as_array().unwrap();

        assert!(capabilities
            .iter()
            .any(|capability| capability["id"] == "cowd.structured_data.core"));
        assert!(!capabilities
            .iter()
            .any(|capability| capability["id"] == "mfg.manufacturing.application"));
    }

    #[tokio::test]
    async fn cowd_projection_route_separates_cli_from_webui_surface() {
        let app = api_router(test_state());
        let webui = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/projection?surface=webui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cli = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/projection?surface=cli")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(webui.status(), StatusCode::OK);
        assert_eq!(cli.status(), StatusCode::OK);
        let webui_body = to_bytes(webui.into_body(), usize::MAX).await.unwrap();
        let cli_body = to_bytes(cli.into_body(), usize::MAX).await.unwrap();
        let webui_json: serde_json::Value = serde_json::from_slice(&webui_body).unwrap();
        let cli_json: serde_json::Value = serde_json::from_slice(&cli_body).unwrap();

        assert_eq!(webui_json["surface"], "webui");
        assert_eq!(cli_json["surface"], "cli");
        assert_eq!(webui_json["capability_count"], cli_json["capability_count"]);
        assert!(webui_json["capabilities"][0]["management_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "bulk_actions"));
        assert_eq!(
            cli_json["capabilities"][0]["management_fields"],
            serde_json::json!(["json_output", "core_controls"])
        );
    }

    #[tokio::test]
    async fn mfg_app_route_projects_manufacturing_as_application_descriptor() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/apps/mfg/app")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["app_id"], "mfg.manufacturing");
        assert_eq!(json["layer"], "application");
        assert!(json["cowd_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "cowd.structured_data.core"));
        assert_eq!(json["domains"][0]["domain_id"], "server_manufacturing");
    }

    #[tokio::test]
    async fn mfg_app_route_projects_manufacturing_as_mfg_application() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/apps/mfg/app")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["app_id"], "mfg.manufacturing");
        assert_eq!(json["layer"], "application");
        assert!(json["cowd_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "cowd.structured_data.core"));
        assert!(!json["cowd_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "cowd.matrix.runtime"));
        assert!(json["surfaces"]
            .as_array()
            .unwrap()
            .iter()
            .all(|surface| surface["entrypoints"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entrypoint| entrypoint == "/api/apps/mfg/app")));
    }

    #[tokio::test]
    async fn mfg_reality_facade_consumes_matrix_without_exposing_matrix_management() {
        let workspace = test_temp_dir("mfg-reality-facade");
        let config_home = test_temp_dir("mfg-reality-facade-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/apps/mfg/reality/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let body = to_bytes(health.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "mfg.reality.health");
        assert_eq!(json["boundary"]["consumer"], "mfg");
        assert_eq!(json["boundary"]["core"], "reality");
        assert_eq!(json["boundary"]["engine"], "matrix");

        let plan = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/apps/mfg/reality/data-plane/ingest-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "mfg-reality-test",
                            "ingest": {
                                "source_ref": "source-pack://mfg-test",
                                "fact_type": "manufacturing_quality_event",
                                "partition_ref": "line:A",
                                "high_watermark": "2026-06-22T00:00:00Z",
                                "estimated_rows": 8,
                                "raw_checksum": "sha256:test",
                                "metric_ids": ["torque_deviation_rate"]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(plan.status(), StatusCode::OK);
        let body = to_bytes(plan.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "mfg.reality.data_plane.ingest_plan");
        assert_eq!(
            json["boundary"]["ownership"],
            "MFG consumes Reality Core projections; Reality Core owns Matrix management."
        );

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn mfg_decision_trace_projects_matrix_to_cockpit_report() {
        let workspace = test_temp_dir("mfg-decision-trace");
        let config_home = test_temp_dir("mfg-decision-trace-config");
        let app = api_router(test_state_with_workspace(workspace, config_home));

        let profile = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/apps/mfg/cockpit/profiles/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "profile": {
                                "profile_id": "trace-profile",
                                "owner_ref": "user:test",
                                "display_name": "Trace Profile",
                                "focus_refs": ["line:A"],
                                "focus_metric_ids": ["torque_deviation_rate"],
                                "thresholds": {"torque_deviation_rate": 0.08},
                                "cadence": "daily"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let profile_status = profile.status();
        let profile_body = to_bytes(profile.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            profile_status,
            StatusCode::OK,
            "profile response: {}",
            String::from_utf8_lossy(&profile_body)
        );

        let report = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/apps/mfg/cockpit/profiles/trace-profile/reports/generate")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "report": {
                                "report_id": "trace-report",
                                "cadence": "daily",
                                "delivery_ref": "channel://test/operator",
                                "note": "decision trace test"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(report.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/apps/mfg/decision-trace?report_id=trace-report")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "decision trace response: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["kind"], "mfg.decision_trace");
        assert_eq!(
            json["chain"],
            "source -> fact -> metric -> evidence -> incident -> action -> report"
        );
        assert!(json["rows"].as_array().unwrap().iter().any(|row| {
            row["stage"] == "report"
                && row["ref"] == "trace-report"
                && row["endpoint"] == "/api/apps/mfg/cockpit/reports/:id/delivery-state"
        }));
        assert_eq!(json["objects"]["report"]["report_id"], "trace-report");
    }

    #[tokio::test]
    async fn cowd_structured_sources_and_structured_ingest_plan_routes_expose_contract_adapter() {
        let workspace = test_temp_dir("cowd-structured-index");
        let config_home = test_temp_dir("cowd-structured-config");
        let app = api_router(test_state_with_workspace(workspace.clone(), config_home));

        let source_upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "cowd-structured-source",
                            "session_id": "session-cowd-structured",
                            "source_pack": {
                                "source_pack_id": "pack-1",
                                "source_name": "erp",
                                "owner": "operations",
                                "access_mode": "connector",
                                "refresh_mode": "incremental",
                                "entity_mappings": [{
                                    "source_entity": "plant",
                                    "matrix_entity_type": "factory",
                                    "source_key_field": "plant_id"
                                }],
                                "fact_mappings": [{
                                    "source_table": "inventory",
                                    "fact_type": "inventory_balance",
                                    "metric_key": "stock_on_hand",
                                    "entity_ref_fields": ["plant_id"],
                                    "measure_fields": ["qty"],
                                    "dedup_key": "plant_id:sku:week",
                                    "delta_signature": "qty"
                                }],
                                "reconciliation_rules": ["dedup_key_unique"],
                                "quality_rules": ["qty_non_negative"]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(source_upsert.status(), StatusCode::OK);

        let fact_ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "cowd-structured-fact",
                            "session_id": "session-cowd-structured",
                            "facts": [{
                                "fact_id": "fact-stock-1",
                                "snapshot_id": "snapshot-week-30",
                                "fact_type": "inventory_balance",
                                "entity_refs": ["factory:sz"],
                                "metric_key": "stock_on_hand",
                                "dimensions": {"week": "2026-W30"},
                                "measures": {"qty": 42},
                                "source_ref": "pack-1",
                                "confidence": 0.97
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fact_ingest.status(), StatusCode::OK);
        let body = to_bytes(fact_ingest.into_body(), usize::MAX).await.unwrap();
        let fact_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let attention_id = fact_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap()
            .to_string();

        let evidence_build = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "Inventory balance requires structured evidence"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence_build.status(), StatusCode::OK);

        let sources = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/sources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let facts = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/facts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/evidence")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cowd/structured/ingest-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "source_ref": "pack-1",
                            "fact_type": "inventory_balance",
                            "partition_ref": "2026-W30",
                            "high_watermark": "2026-06-14T00:00:00Z",
                            "estimated_rows": 42,
                            "raw_checksum": "sha256:test",
                            "metric_ids": ["stock_on_hand"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let watermarks = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/watermarks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(sources.status(), StatusCode::OK);
        assert_eq!(facts.status(), StatusCode::OK);
        assert_eq!(evidence.status(), StatusCode::OK);
        assert_eq!(ingest.status(), StatusCode::OK);
        assert_eq!(watermarks.status(), StatusCode::OK);
        let sources_body = to_bytes(sources.into_body(), usize::MAX).await.unwrap();
        let facts_body = to_bytes(facts.into_body(), usize::MAX).await.unwrap();
        let evidence_body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let ingest_body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let watermarks_body = to_bytes(watermarks.into_body(), usize::MAX).await.unwrap();
        let sources_json: serde_json::Value = serde_json::from_slice(&sources_body).unwrap();
        let facts_json: serde_json::Value = serde_json::from_slice(&facts_body).unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&evidence_body).unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&ingest_body).unwrap();
        let watermarks_json: serde_json::Value = serde_json::from_slice(&watermarks_body).unwrap();

        assert_eq!(sources_json["contract"], "cowd.structured_data.v1");
        assert_eq!(sources_json["list_status"], "ready");
        assert_eq!(sources_json["count"], 1);
        assert_eq!(sources_json["items"][0]["source_id"], "pack-1");
        assert_eq!(facts_json["list_status"], "ready");
        assert_eq!(facts_json["items"][0]["fact_id"], "fact-stock-1");
        assert_eq!(evidence_json["list_status"], "ready");
        assert_eq!(
            evidence_json["items"][0]["problem_statement"],
            "Inventory balance requires structured evidence"
        );
        assert_eq!(ingest_json["source_ref"], "pack-1");
        assert_eq!(ingest_json["fact_type"], "inventory_balance");
        assert_eq!(
            ingest_json["affected_metric_ids"],
            serde_json::json!(["stock_on_hand"])
        );
        assert_eq!(
            ingest_json["watermark"]["high_watermark"],
            "2026-06-14T00:00:00Z"
        );
        assert_eq!(watermarks_json["list_status"], "ready");
        assert_eq!(watermarks_json["items"][0]["source_ref"], "pack-1");
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn matrix_source_snapshot_run_maps_rows_through_gateway_api() {
        let workspace = test_temp_dir("matrix-source-snapshot-workspace");
        let config_home = test_temp_dir("matrix-source-snapshot-config");
        let app = api_router(test_state_with_workspace(workspace.clone(), config_home));

        let adapters = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/sources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(adapters.status(), StatusCode::OK);
        let adapters_body = to_bytes(adapters.into_body(), usize::MAX).await.unwrap();
        let adapters_json: serde_json::Value = serde_json::from_slice(&adapters_body).unwrap();
        assert!(adapters_json["adapters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|adapter| adapter["adapter_id"] == "feishu_bitable"));

        let source_upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "source_pack": {
                                "source_pack_id": "pack-snapshot-orders",
                                "source_name": "supply_gateway_fixture",
                                "owner": "operations",
                                "access_mode": "file",
                                "refresh_mode": "snapshot",
                                "entity_mappings": [
                                    {
                                        "source_entity": "supplier",
                                        "matrix_entity_type": "supplier",
                                        "source_key_field": "supplier_id"
                                    },
                                    {
                                        "source_entity": "part",
                                        "matrix_entity_type": "part",
                                        "source_key_field": "part_id"
                                    }
                                ],
                                "fact_mappings": [{
                                    "source_table": "orders",
                                    "fact_type": "supply.order",
                                    "metric_key": "supply_qty",
                                    "entity_ref_fields": ["supplier_id", "part_id"],
                                    "measure_fields": ["qty"],
                                    "event_time_field": "event_time",
                                    "dedup_key": "order_id",
                                    "delta_signature": "order_id"
                                }],
                                "relation_mappings": [{
                                    "source_table": "orders",
                                    "relation_type": "supplies",
                                    "from_source_key_field": "supplier_id",
                                    "to_source_key_field": "part_id",
                                    "attribute_fields": ["qty"],
                                    "dedup_key": "order_id"
                                }],
                                "reconciliation_rules": ["source_snapshot_is_idempotent"],
                                "quality_rules": ["dedup_key_required"]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(source_upsert.status(), StatusCode::OK);

        let plan = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/pack-snapshot-orders/snapshots/plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "resource_ref": "file://orders.csv",
                            "estimated_rows": 2
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(plan.status(), StatusCode::OK);

        let run = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/pack-snapshot-orders/snapshots/run")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "snapshot": {
                                "snapshot_id": "snapshot-gateway-orders-1",
                                "source_system": "supply_gateway_fixture",
                                "source_kind": "file",
                                "resource_ref": "file://orders.csv",
                                "schema_version": "source:csv:orders",
                                "row_count": 2,
                                "checksum": "sha256:fixture",
                                "confidence": 0.95
                            },
                            "rows": [
                                {
                                    "order_id": "O1",
                                    "supplier_id": "S1",
                                    "part_id": "P1",
                                    "qty": 12,
                                    "event_time": "2026-07-02T00:00:00Z"
                                },
                                {
                                    "order_id": "O2",
                                    "supplier_id": "S2",
                                    "part_id": "P2",
                                    "qty": 4,
                                    "event_time": "2026-07-02T01:00:00Z"
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run.status(), StatusCode::OK);
        let run_body = to_bytes(run.into_body(), usize::MAX).await.unwrap();
        let run_json: serde_json::Value = serde_json::from_slice(&run_body).unwrap();
        assert_eq!(run_json["kind"], "matrix.source_snapshot.run");
        assert_eq!(run_json["apply_report"]["fact_count"], 2);
        assert_eq!(run_json["apply_report"]["relation_count"], 2);

        let snapshots = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/source-packs/pack-snapshot-orders/snapshots")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshots.status(), StatusCode::OK);

        let health = app
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let health_body = to_bytes(health.into_body(), usize::MAX).await.unwrap();
        let health_json: serde_json::Value = serde_json::from_slice(&health_body).unwrap();
        assert_eq!(health_json["source_snapshot_count"], 1);
        assert_eq!(health_json["fact_count"], 2);
        assert_eq!(health_json["relation_count"], 2);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn cowd_surfaces_route_declares_webui_tui_parity_and_cli_minimality() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/surfaces")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["webui_tui_full_parity"], true);
        assert_eq!(json["cli_is_minimal_control"], true);
        assert_eq!(json["webui"]["role"], "enhanced_management");
        assert_eq!(json["tui"]["role"], "console_full_capability");
        assert_eq!(json["cli"]["role"], "minimal_core_control");
    }

    #[tokio::test]
    async fn cowd_release_gate_route_reports_missing_timeline_evidence() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/release-gate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["gate_id"], "cowd.release_gate.v1");
        assert_eq!(json["status"], "fail");
        assert!(json["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["check_id"] == "surface.cli.minimal" && check["status"] == "pass"));
        assert!(json["checks"].as_array().unwrap().iter().any(|check| {
            check["check_id"] == "execution_outcome.timeline.available" && check["status"] == "fail"
        }));
        assert!(json["checks"].as_array().unwrap().iter().any(|check| {
            check["check_id"] == "structured_data.memory_context.bridge"
                && check["status"] == "fail"
        }));
    }

    #[tokio::test]
    async fn cowd_structured_mfg_full_loop_passes_release_gate() {
        let workspace = test_temp_dir("cowd-structured-mfg-full-loop");
        let config_home = test_temp_dir("cowd-structured-mfg-full-loop-config");
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "session-full-loop";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".to_string()),
            ))
            .await
            .unwrap();
        store
            .append_session_domain_event(&memory::SessionDomainEvent::new(
                session_id,
                0,
                memory::SessionDomainScope::Turn,
                "execution.outcome",
                serde_json::json!({"status": "ok", "title": "full loop outcome"}),
                current_time_ms(),
            ))
            .await
            .unwrap();
        let mut skill_activation_event = memory::SessionDomainEvent::new(
            session_id,
            1,
            memory::SessionDomainScope::Context,
            "skill_candidates",
            serde_json::json!({
                "source": "conversation_runtime.skill_activation",
                "turn_index": 1,
                "query": "structured manufacturing full loop",
                "selected": "supply-risk-analyst",
                "candidates": [{
                    "name": "supply-risk-analyst",
                    "score": 12,
                    "reasons": ["name:supply", "name:risk"],
                    "path": null,
                    "source": "profile"
                }],
                "invocation_evidence": {
                    "skill_id": "supply-risk-analyst",
                    "skill_version": null,
                    "adapter": "prompt_only",
                    "entrypoint": "SKILL.md",
                    "outcome": "selected_for_runtime"
                },
                "structured_dependencies": []
            }),
            current_time_ms(),
        );
        skill_activation_event.refs.push(memory::SessionDomainRef {
            ref_type: "skill".to_string(),
            id: "supply-risk-analyst".to_string(),
            label: Some("selected".to_string()),
        });
        skill_activation_event.refs.push(memory::SessionDomainRef {
            ref_type: "skill_invocation".to_string(),
            id: "supply-risk-analyst".to_string(),
            label: Some("selected_for_runtime".to_string()),
        });
        store
            .append_session_domain_event(&skill_activation_event)
            .await
            .unwrap();
        let mut skill_memory_event = memory::SessionDomainEvent::new(
            session_id,
            2,
            memory::SessionDomainScope::Context,
            "skill_memory_candidate",
            serde_json::json!({
                "source": "conversation_runtime.skill_memory_candidate",
                "turn_index": 1,
                "query": "structured manufacturing full loop",
                "selected": "supply-risk-analyst",
                "candidate": {
                    "kind": "Refresh",
                    "content": "skill selected for task; source=runtime_skill; query=structured manufacturing full loop; selected=supply-risk-analyst; score=12; reasons=name:supply,name:risk"
                },
                "source_event": "skill_candidates"
            }),
            current_time_ms(),
        );
        skill_memory_event.refs.push(memory::SessionDomainRef {
            ref_type: "skill".to_string(),
            id: "supply-risk-analyst".to_string(),
            label: Some("memory_candidate_source".to_string()),
        });
        store
            .append_session_domain_event(&skill_memory_event)
            .await
            .unwrap();
        let app = api_router(test_state_with_store_and_workspace(
            store,
            workspace.clone(),
            config_home,
        ));

        let source_upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "full-loop-source",
                            "session_id": session_id,
                            "source_pack": {
                                "source_pack_id": "pack-full-loop",
                                "source_name": "mes",
                                "owner": "manufacturing",
                                "access_mode": "connector",
                                "refresh_mode": "incremental",
                                "entity_mappings": [{
                                    "source_entity": "line",
                                    "matrix_entity_type": "production_line",
                                    "source_key_field": "line_id"
                                }],
                                "fact_mappings": [{
                                    "source_table": "line_output",
                                    "fact_type": "production_output",
                                    "metric_key": "units_completed",
                                    "entity_ref_fields": ["line_id"],
                                    "measure_fields": ["units"],
                                    "dedup_key": "line_id:shift",
                                    "delta_signature": "units"
                                }],
                                "reconciliation_rules": ["dedup_key_unique"],
                                "quality_rules": ["units_non_negative"]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(source_upsert.status(), StatusCode::OK);

        let fact_ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "full-loop-fact",
                            "session_id": session_id,
                            "facts": [{
                                "fact_id": "fact-full-loop",
                                "snapshot_id": "snapshot-full-loop",
                                "fact_type": "production_output",
                                "entity_refs": ["production_line:l1"],
                                "metric_key": "units_completed",
                                "dimensions": {"shift": "A"},
                                "measures": {"units": 128},
                                "source_ref": "pack-full-loop",
                                "confidence": 0.96
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fact_ingest.status(), StatusCode::OK);
        let body = to_bytes(fact_ingest.into_body(), usize::MAX).await.unwrap();
        let fact_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let attention_id = fact_json["attention"][0]["attention_id"].as_str().unwrap();

        let evidence_build = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "Production output requires full-loop evidence"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence_build.status(), StatusCode::OK);

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cowd/structured/ingest-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "source_ref": "pack-full-loop",
                            "fact_type": "production_output",
                            "partition_ref": "shift-A",
                            "high_watermark": "2026-06-14T00:00:00Z",
                            "estimated_rows": 128,
                            "metric_ids": ["units_completed"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);

        for uri in [
            "/api/cowd/structured/sources",
            "/api/cowd/structured/facts",
            "/api/cowd/structured/evidence",
            "/api/cowd/structured/watermarks",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["list_status"], "ready", "{uri}");
            assert!(json["count"].as_u64().unwrap_or_default() >= 1, "{uri}");
        }

        let gate = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/release-gate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(gate.status(), StatusCode::OK);
        let body = to_bytes(gate.into_body(), usize::MAX).await.unwrap();
        let gate_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(gate_json["status"], "pass", "{gate_json:#}");
        assert!(gate_json["checks"].as_array().unwrap().iter().any(|check| {
            check["check_id"] == "structured_data.indexes.ready" && check["status"] == "pass"
        }));
        assert!(gate_json["checks"].as_array().unwrap().iter().any(|check| {
            check["check_id"] == "structured_data.memory_context.bridge"
                && check["status"] == "pass"
        }));

        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn matrix_foundation_ingests_fact_and_builds_evidence_packet() {
        let workspace = test_temp_dir("matrix-foundation");
        let config_home = test_temp_dir("matrix-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-test-1",
                            "session_id": "session-matrix",
                            "facts": [{
                                "fact_id": "fact-gpu-shortage",
                                "snapshot_id": "snapshot-week-24",
                                "fact_type": "supply.material_shortage",
                                "entity_refs": ["component:gpu-a"],
                                "metric_key": "material_shortage_risk",
                                "dimensions": {"week": "2026-W24"},
                                "measures": {"short_qty": 42},
                                "source_ref": "connector:local.docs:gpu-shortage",
                                "confidence": 0.91
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);
        let body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(ingest_json["ingested"], 1);
        let attention_id = ingest_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap()
            .to_string();

        let hot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/attention/hot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hot.status(), StatusCode::OK);
        let body = to_bytes(hot.into_body(), usize::MAX).await.unwrap();
        let hot_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(hot_json["items"].as_array().unwrap().len(), 1);

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "GPU shortage may affect server shipments"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);
        let body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let packet_id = evidence_json["packet"]["packet_id"].as_str().unwrap();
        assert!(evidence_json["packet"]["missing_evidence"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/matrix/evidence/{packet_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        assert!(config_home.join("storage").join("matrix.sqlite").exists());
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn reality_core_routes_expose_stable_read_only_projection() {
        let workspace = test_temp_dir("reality-core");
        let config_home = test_temp_dir("reality-core-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        for (uri, kind) in [
            ("/api/reality/status", "reality.status"),
            ("/api/reality/capabilities", "reality.capabilities"),
            ("/api/reality/static", "reality.static"),
            ("/api/reality/flow", "reality.fact_flow"),
            (
                "/api/reality/recall/report?q=reality",
                "reality.recall_report",
            ),
            (
                "/api/reality/context/envelope?q=reality",
                "reality.context_envelope",
            ),
            ("/api/reality/evidence/missing-evidence", "reality.evidence"),
            ("/api/reality/promotions", "reality.promotions"),
            ("/api/reality/governance", "reality.governance"),
            ("/api/reality/boundaries", "reality.boundaries"),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["kind"], kind, "{uri}");
            assert!(json.get("envelope").is_some(), "{uri}");
        }

        let flow = app
            .oneshot(
                Request::builder()
                    .uri("/api/reality/flow?session_id=session-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(flow.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["source"], "growth.promotions");
        assert!(json["stages"].as_array().is_some());

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn reality_recall_report_and_context_include_fact_and_matrix_sources() {
        let workspace = test_temp_dir("reality-recall");
        let config_home = test_temp_dir("reality-recall-config");
        let state = test_state_with_workspace(workspace.clone(), config_home.clone());
        let app = api_router(state.clone());

        let record = harness_contract::growth::LearningRecord::from_input(
            harness_contract::growth::GrowthInput {
                selected_pattern: harness_contract::core::ExecutionPattern::Execute,
                complexity: harness_contract::core::TaskComplexity::Complex,
                risk: harness_contract::core::TaskRisk::Medium,
                context_omitted: 0,
                tool_requires_checkpoint: false,
                tool_requires_human_confirm: false,
                verification_can_finalize: true,
                bench_passed: true,
            },
        );
        let mut event = harness_contract::growth::GrowthEvent::from_input(
            harness_contract::growth::GrowthEventInput {
                session_id: "session-reality-recall".to_string(),
                source_event_kind: "runtime.context.reality_test".to_string(),
                strategy_pattern: harness_contract::core::ExecutionPattern::Execute,
                learning_record: record,
                evidence_refs: vec![harness_contract::growth::GrowthEvidenceRef::new(
                    "test_evidence",
                    "trace:gpu-shortage",
                    "GPU shortage trace",
                )],
            },
        );
        event.memory_candidates = vec![harness_contract::growth::GrowthMemoryCandidate {
            id: "candidate-gpu-shortage".to_string(),
            kind: harness_contract::growth::GrowthMemoryCandidateKind::AuthorityPromotion,
            summary: "GPU shortage requires expedited supplier allocation".to_string(),
            reason: "observed shortage was confirmed by runtime evidence".to_string(),
            confidence_bp: 9_100,
        }];
        event.matrix_signals = vec![harness_contract::growth::GrowthMatrixSignal {
            fact_type: "supply.material_shortage".to_string(),
            dimensions: serde_json::json!({"component": "gpu", "week": "2026-W24"}),
            measures: serde_json::json!({"short_qty": 42, "risk": "high"}),
            confidence_bp: 9_200,
        }];
        let receipt = state
            .services
            .growth
            .ingest_growth_event(
                &state.config_home,
                &state.services.memory,
                &state.services.matrix,
                event,
            )
            .await;
        assert!(receipt.errors.is_empty(), "{receipt:#?}");
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "fact.memory" && item.status == "promote"));
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "matrix.fact" && item.status == "promoted"));

        let recall = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/reality/recall/report?q=GPU%20shortage&max_items=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recall.status(), StatusCode::OK);
        let body = to_bytes(recall.into_body(), usize::MAX).await.unwrap();
        let recall_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let source_names = recall_json["recall_report"]["sources"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|source| source["source"].as_str())
            .collect::<Vec<_>>();
        assert!(source_names.contains(&"fact"), "{recall_json:#}");
        assert!(source_names.contains(&"matrix"), "{recall_json:#}");
        assert!(recall_json["recall_report"]["selected"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["source"] == "fact"));
        assert!(recall_json["recall_report"]["selected"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["source"] == "matrix"));

        let context = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=GPU%20shortage")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(context.status(), StatusCode::OK);
        let body = to_bytes(context.into_body(), usize::MAX).await.unwrap();
        let context_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let selected = context_json["envelope"]["selected"].as_array().unwrap();
        assert!(
            selected.iter().any(|item| item["source"] == "Fact"),
            "{context_json:#}"
        );
        assert!(
            selected.iter().any(|item| item["source"] == "Matrix"),
            "{context_json:#}"
        );

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn matrix_routes_expose_structured_fact_engine() {
        let workspace = test_temp_dir("matrix-foundation");
        let config_home = test_temp_dir("matrix-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let matrix_health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let matrix_health_again = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(matrix_health.status(), StatusCode::OK);
        assert_eq!(matrix_health_again.status(), StatusCode::OK);
        let matrix_health_body = to_bytes(matrix_health.into_body(), usize::MAX)
            .await
            .unwrap();
        let matrix_health_again_body = to_bytes(matrix_health_again.into_body(), usize::MAX)
            .await
            .unwrap();
        let matrix_health_json: serde_json::Value =
            serde_json::from_slice(&matrix_health_body).unwrap();
        let matrix_health_again_json: serde_json::Value =
            serde_json::from_slice(&matrix_health_again_body).unwrap();
        assert_eq!(matrix_health_json, matrix_health_again_json);

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-test-1",
                            "session_id": "session-matrix",
                            "facts": [{
                                "fact_id": "fact-matrix-gpu-shortage",
                                "snapshot_id": "snapshot-week-24",
                                "fact_type": "supply.material_shortage",
                                "entity_refs": ["component:gpu-a"],
                                "metric_key": "material_shortage_risk",
                                "dimensions": {"week": "2026-W24"},
                                "measures": {"short_qty": 42},
                                "source_ref": "connector:local.docs:gpu-shortage",
                                "confidence": 0.91
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);
        let body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(ingest_json["ingested"], 1);
        let attention_id = ingest_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap()
            .to_string();

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "Matrix evidence should share Matrix storage"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);
        let body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let packet_id = evidence_json["packet"]["packet_id"].as_str().unwrap();

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/matrix/evidence/{packet_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        assert!(config_home.join("storage").join("matrix.sqlite").exists());
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn matrix_fact_and_evidence_append_execution_outcomes_to_runtime_timeline() {
        let workspace = test_temp_dir("matrix-outcome-timeline");
        let config_home = test_temp_dir("matrix-outcome-config");
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let app = api_router(test_state_with_store_and_workspace(
            store,
            workspace.clone(),
            config_home.clone(),
        ));
        let session_id = "matrix-outcome-session";

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-outcome-fact",
                            "session_id": session_id,
                            "facts": [{
                                "fact_id": "fact-outcome-stock",
                                "snapshot_id": "snapshot-outcome",
                                "fact_type": "inventory_balance",
                                "entity_refs": ["factory:sz"],
                                "metric_key": "stock_on_hand",
                                "dimensions": {"week": "2026-W30"},
                                "measures": {"qty": 64},
                                "source_ref": "pack-outcome",
                                "confidence": 0.93
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);
        let body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let attention_id = ingest_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap();

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-outcome-evidence",
                            "session_id": session_id,
                            "attention_id": attention_id,
                            "problem_statement": "Inventory balance outcome should reach timeline"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);

        let timeline = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(timeline.status(), StatusCode::OK);
        let body = to_bytes(timeline.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let outcome_events = json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "execution.outcome")
            .collect::<Vec<_>>();
        assert_eq!(outcome_events.len(), 2);
        assert!(outcome_events.iter().any(|event| {
            event["refs"].as_array().is_some_and(|refs| {
                refs.iter().any(|reference| {
                    reference["type"] == "structured_fact"
                        && reference["id"] == "fact-outcome-stock"
                })
            })
        }));
        assert!(outcome_events.iter().any(|event| {
            event["refs"].as_array().is_some_and(|refs| {
                refs.iter()
                    .any(|reference| reference["type"] == "structured_evidence")
            })
        }));
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn matrix_metric_recompute_projects_changes_and_attention() {
        let workspace = test_temp_dir("matrix-metric");
        let config_home = test_temp_dir("matrix-metric-config");
        let app = api_router(test_state_with_workspace(workspace.clone(), config_home));

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "facts": [
                                {
                                    "fact_id": "fact-plan-api-1",
                                    "snapshot_id": "snapshot-plan-api-1",
                                    "fact_type": "plan.weekly_demand",
                                    "entity_refs": ["product:server-a"],
                                    "metric_key": "plan_bom_delta",
                                    "dimensions": {"week": "2026-W24"},
                                    "measures": {"demand_qty": 100},
                                    "confidence": 0.8
                                },
                                {
                                    "fact_id": "fact-plan-api-2",
                                    "snapshot_id": "snapshot-plan-api-2",
                                    "fact_type": "plan.weekly_demand",
                                    "entity_refs": ["product:server-a"],
                                    "metric_key": "plan_bom_delta",
                                    "dimensions": {"week": "2026-W24"},
                                    "measures": {"demand_qty": 140},
                                    "confidence": 0.9
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);

        let recompute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/metrics/recompute")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recompute.status(), StatusCode::OK);
        let body = to_bytes(recompute.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result"]["metric_state_count"], 1);
        assert_eq!(json["result"]["change_count"], 1);
        assert_eq!(json["result"]["metric_states"][0]["value"], 240.0);

        let metric = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/metrics/plan_bom_delta")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metric.status(), StatusCode::OK);

        let changes = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/changes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(changes.status(), StatusCode::OK);
        let body = to_bytes(changes.into_body(), usize::MAX).await.unwrap();
        let changes_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(changes_json["changes"].as_array().unwrap().len(), 1);

        let hot = app
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/attention/hot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hot.status(), StatusCode::OK);
        let body = to_bytes(hot.into_body(), usize::MAX).await.unwrap();
        let hot_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(hot_json["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["reason_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "metric_delta_detected")));
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn matrix_evidence_context_and_mfg_incident_create_workflow_graph() {
        let workspace = test_temp_dir("matrix-mfg-agent");
        let config_home = test_temp_dir("matrix-mfg-agent-config");
        let app = api_router(test_state_with_workspace(workspace.clone(), config_home));

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "facts": [{
                                "fact_id": "fact-agent-risk",
                                "snapshot_id": "snapshot-agent-risk",
                                "fact_type": "supply.material_shortage",
                                "entity_refs": ["component:gpu-agent"],
                                "metric_key": "material_shortage_risk",
                                "dimensions": {"week": "2026-W26"},
                                "measures": {"short_qty": 180},
                                "confidence": 0.92
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);

        let recompute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/metrics/recompute")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recompute.status(), StatusCode::OK);
        let body = to_bytes(recompute.into_body(), usize::MAX).await.unwrap();
        let recompute_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let attention_id = recompute_json["result"]["attention"][0]["attention_id"]
            .as_str()
            .unwrap();

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "GPU shortage threatens server shipment"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);
        let body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(evidence_json["packet"]["metric_evidence"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
        assert!(evidence_json["packet"]["change_evidence"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
        let packet_id = evidence_json["packet"]["packet_id"].as_str().unwrap();

        let context = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/matrix/evidence/{packet_id}/context"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(context.status(), StatusCode::OK);
        let body = to_bytes(context.into_body(), usize::MAX).await.unwrap();
        let context_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            context_json["context_item"]["id"],
            format!("structured-evidence:{packet_id}")
        );

        let incident = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/apps/mfg/incidents")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "GPU material shortage incident",
                            "evidence_packet_id": packet_id
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(incident.status(), StatusCode::OK);
        let body = to_bytes(incident.into_body(), usize::MAX).await.unwrap();
        let incident_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(incident_json["incident"]["evidence_packet_id"], packet_id);
        assert!(incident_json["workflow_graph"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["node_id"] == "mfg_researcher"));
        assert!(incident_json["workflow_graph"]["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|evidence| evidence["reference"] == format!("mfg:evidence:{packet_id}")));
        let incident_id = incident_json["incident"]["incident_id"].as_str().unwrap();

        let room = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/apps/mfg/incidents/{incident_id}/room"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(room.status(), StatusCode::OK);
        let room_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(room.into_body(), usize::MAX).await.unwrap()).unwrap();
        assert_eq!(
            room_json["workflow_graph"]["workflow_id"],
            incident_json["workflow_graph"]["workflow_id"]
        );
        assert!(room_json.get("agent_graph").is_none());

        let skill_plan = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/apps/mfg/incidents/{incident_id}/skills/plan"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"limit":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(skill_plan.status(), StatusCode::OK);
        let skill_plan_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(skill_plan.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let skill_id = skill_plan_json["plan"]["selected_skills"][0]["skill_id"]
            .as_str()
            .unwrap();
        let skill_node_id = app_mfg::skill_agent_node_id(skill_id);
        assert!(skill_plan_json["workflow_graph"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["node_id"] == skill_node_id));
        assert!(skill_plan_json.get("agent_graph").is_none());

        let skill_run = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/apps/mfg/incidents/{incident_id}/skills/{skill_id}/run"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(skill_run.status(), StatusCode::OK);
        let skill_run_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(skill_run.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(skill_run_json["skill_run"]["status"], "completed");
        assert!(skill_run_json["workflow_graph"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["node_id"] == skill_node_id && node["status"] == "completed"));
        assert!(skill_run_json.get("agent_graph").is_none());

        let analysis = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/apps/mfg/incidents/{incident_id}/analyze"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(analysis.status(), StatusCode::OK);
        let body = to_bytes(analysis.into_body(), usize::MAX).await.unwrap();
        let analysis_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(analysis_json["analysis"]["incident_id"], incident_id);
        assert!(analysis_json["analysis"]["attribution_candidates"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
        assert!(analysis_json["analysis"]["impact_paths"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
        assert!(analysis_json["analysis"]["recommended_actions"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
        let analysis_id = analysis_json["analysis"]["analysis_id"].as_str().unwrap();
        let action_id = analysis_json["analysis"]["recommended_actions"][0]["action_id"]
            .as_str()
            .unwrap();

        let fetched_analysis = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/apps/mfg/analyses/{analysis_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched_analysis.status(), StatusCode::OK);

        let execution = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/apps/mfg/analyses/{analysis_id}/actions/{action_id}/execute"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "mode": "commit",
                            "operator_id": "user:ops-planner",
                            "note": "queue reviewed recovery action"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execution.status(), StatusCode::OK);
        let body = to_bytes(execution.into_body(), usize::MAX).await.unwrap();
        let execution_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(execution_json["execution"]["mode"], "commit");
        assert_eq!(
            execution_json["execution"]["status"],
            "queued_for_human_review"
        );
        let execution_id = execution_json["execution"]["execution_id"]
            .as_str()
            .unwrap();

        let feedback = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/apps/mfg/executions/{execution_id}/feedback"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "outcome": "resolved",
                            "note": "supplier recovery completed",
                            "metric_delta": -180
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(feedback.status(), StatusCode::OK);
        let body = to_bytes(feedback.into_body(), usize::MAX).await.unwrap();
        let feedback_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(feedback_json["execution"]["status"], "feedback_resolved");

        let fetched_execution = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/apps/mfg/executions/{execution_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched_execution.status(), StatusCode::OK);

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/apps/mfg/incidents/{incident_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        let body = to_bytes(fetched.into_body(), usize::MAX).await.unwrap();
        let fetched_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(fetched_json["incident"]["status"], "closed");
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn task_execution_graph_is_committed_and_projected() {
        let app = api_router(test_state());
        let started = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "coordinate multi agent",
                            "yolo_mode": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(started.status(), StatusCode::CREATED);
        let body = to_bytes(started.into_body(), usize::MAX).await.unwrap();
        let task: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let task_id = task["id"].as_str().unwrap();

        let runs = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/agents/execution-graphs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(runs.status(), StatusCode::OK);
        let body = to_bytes(runs.into_body(), usize::MAX).await.unwrap();
        let runs_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(runs_json["kind"], "execution_graphs");
        assert_eq!(runs_json["graphs"].as_array().unwrap().len(), 0);

        let upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/execution-graph"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "coordinate multi agent",
                            "nodes": [
                                {
                                    "id": "planner",
                                    "kind": "agent_task",
                                    "payload_ref": "task:planner",
                                    "executor_kind": "agent_task",
                                    "idempotency_key": "task:planner:1",
                                    "lease_ref": null,
                                    "acceptance": {"criteria": [], "required_evidence": [], "minimum_score_basis_points": null},
                                    "retry_policy": {"max_attempts": 1, "retryable_failure_kinds": [], "base_backoff_ms": 500, "maximum_backoff_ms": 30000},
                                    "resource_scopes": []
                                },
                                {
                                    "id": "review",
                                    "kind": "verify",
                                    "payload_ref": "task:review",
                                    "executor_kind": "verify",
                                    "idempotency_key": "task:review:1",
                                    "lease_ref": null,
                                    "acceptance": {"criteria": [], "required_evidence": [], "minimum_score_basis_points": null},
                                    "retry_policy": {"max_attempts": 1, "retryable_failure_kinds": [], "base_backoff_ms": 500, "maximum_backoff_ms": 30000},
                                    "resource_scopes": []
                                }
                            ],
                            "edges": [{"from": "planner", "to": "review", "kind": "depends_on"}]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upsert.status(), StatusCode::OK);
        let body = to_bytes(upsert.into_body(), usize::MAX).await.unwrap();
        let graph: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(graph["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(graph["revision"], 1);

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/tasks/{task_id}/execution-graph"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        let body = to_bytes(fetched.into_body(), usize::MAX).await.unwrap();
        let fetched_graph: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(fetched_graph["nodes"][1]["node_id"], "review");
    }

    #[tokio::test]
    async fn runtime_agent_routes_reject_commands_without_recoverable_backend_handle() {
        let state = test_state();
        let agent_id = format!("agent-route-{}", uuid::Uuid::new_v4());
        let services = state
            .services
            .runtime
            .as_ref()
            .expect("runtime service")
            .runtime_services();
        services
            .agent_runtime()
            .restore_verified_run(runtime::AgentRunSnapshot {
                run_id: format!("run-{agent_id}"),
                agent_id: agent_id.clone(),
                task_id: "task-route".to_string(),
                session_id: "session-route".to_string(),
                graph_id: "graph-route".to_string(),
                node_id: "node-route".to_string(),
                attempt: 1,
                expected_graph_revision: 1,
                backend: runtime::AgentBackendKind::InProcess,
                status: harness_contract::agent::AgentStatus::Running,
                revision: 0,
                model: None,
                provider: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore agent");
        let app = api_router(state);

        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/agents/{agent_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);

        let cancel = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/runtime/agents/{agent_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::OK);
        let body = to_bytes(cancel.into_body(), usize::MAX).await.unwrap();
        let cancel_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(cancel_json["receipt"]["accepted"], false);
        assert_eq!(
            cancel_json["receipt"]["reject_reason"],
            "unsupported_by_backend"
        );

        let events = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/agents/{agent_id}/events"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);
        let body = to_bytes(events.into_body(), usize::MAX).await.unwrap();
        let events_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(events_json["count"].as_u64().unwrap_or_default() >= 2);
    }

    #[tokio::test]
    async fn runtime_agent_routes_preserve_rejection_for_unrecoverable_process_handles() {
        let state = test_state();
        let agent_id = format!("agent-command-{}", uuid::Uuid::new_v4());
        let services = state
            .services
            .runtime
            .as_ref()
            .expect("runtime service")
            .runtime_services();
        services
            .agent_runtime()
            .restore_verified_run(runtime::AgentRunSnapshot {
                run_id: format!("run-{agent_id}"),
                agent_id: agent_id.clone(),
                task_id: "task-command".to_string(),
                session_id: "session-command".to_string(),
                graph_id: "graph-command".to_string(),
                node_id: "node-command".to_string(),
                attempt: 1,
                expected_graph_revision: 1,
                backend: runtime::AgentBackendKind::ProcessJsonl,
                status: harness_contract::agent::AgentStatus::Running,
                revision: 0,
                model: None,
                provider: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore agent");
        let app = api_router(state);

        for path in ["input", "interrupt", "shutdown"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/runtime/agents/{agent_id}/{path}"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"payload": {"text": path}}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["receipt"]["accepted"], false);
            assert_eq!(json["receipt"]["reject_reason"], "unsupported_by_backend");
        }
    }

    #[tokio::test]
    async fn agent_team_profiles_crud_persists_receipts() {
        let workspace = test_temp_dir("agent-team-profiles");
        let config_home = test_temp_dir("agent-team-profiles-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/agents/team-profiles")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": "qa-review-team",
                            "name": "QA Review Team",
                            "objective": "review manufacturing quality incidents",
                            "leader": "planner",
                            "members": ["planner", "executor", "reviewer"],
                            "policy": { "max_parallel_agents": 3 },
                            "evaluation": { "success_metric": "accepted_review" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
        let body = to_bytes(create.into_body(), usize::MAX).await.unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(created["profile"]["id"], "qa-review-team");
        assert_eq!(
            created["receipt"]["changed_refs"][0],
            "agent-team-profile:qa-review-team"
        );

        let list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/agents/team-profiles")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let body = to_bytes(list.into_body(), usize::MAX).await.unwrap();
        let listed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(listed["count"], 1);

        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/agents/team-profiles/qa-review-team")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);

        let update = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/agents/team-profiles/qa-review-team")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "name": "QA Review Team",
                            "objective": "review incident and release evidence",
                            "leader": "reviewer",
                            "members": ["planner", "reviewer"],
                            "policy": { "max_parallel_agents": 2 },
                            "evaluation": { "quality_gate": "all_tests_pass" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(update.status(), StatusCode::OK);
        let body = to_bytes(update.into_body(), usize::MAX).await.unwrap();
        let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(updated["profile"]["leader"], "reviewer");
        assert_eq!(updated["receipt"]["status"], "ok");

        let delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/agents/team-profiles/qa-review-team")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::OK);

        let missing = app
            .oneshot(
                Request::builder()
                    .uri("/api/agents/team-profiles/qa-review-team")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_cache_api_reports_stats() {
        let workspace = test_temp_dir("tool-cache-api");
        let config_home = test_temp_dir("tool-cache-api-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools/cache")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["tool_name"], "tool_cache_stats");
        assert_eq!(json["status"], "ok");
        assert!(json["data"]["entries"].is_number());

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_execute_rejects_write_tools_and_path_escape() {
        let workspace = test_temp_dir("tool-execute-safety");
        let config_home = test_temp_dir("tool-execute-safety-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let rejected_write = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "name": "write_file",
                            "input": { "path": "owned.txt", "content": "no" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected_write.status(), StatusCode::FORBIDDEN);

        let rejected_escape = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "name": "read_file",
                            "input": { "path": "../outside.txt" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected_escape.status(), StatusCode::BAD_REQUEST);

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_mutation_api_previews_and_applies_transaction() {
        let workspace = test_temp_dir("tool-mutation-api");
        let config_home = test_temp_dir("tool-mutation-api-config");
        std::fs::write(workspace.join("a.txt"), "alpha\n").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let preview = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/mutations/preview")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "edits": [{
                                "path": "a.txt",
                                "old_string": "alpha",
                                "new_string": "beta"
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preview.status(), StatusCode::OK);
        let body = to_bytes(preview.into_body(), usize::MAX).await.unwrap();
        let preview_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(preview_json["data"]["type"], "mutation_preview");
        let expected_hash = preview_json["data"]["files"][0]["expectedHash"]
            .as_str()
            .unwrap();

        let apply = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/mutations/apply")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "edits": [{
                                "path": "a.txt",
                                "old_string": "alpha",
                                "new_string": "beta"
                            }],
                            "expected_hashes": {
                                "a.txt": expected_hash
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(apply.status(), StatusCode::OK);
        assert_eq!(
            std::fs::read_to_string(workspace.join("a.txt")).unwrap(),
            "beta\n"
        );

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_checkpoint_api_returns_receipts() {
        let workspace = test_temp_dir("tool-checkpoint-api");
        let config_home = test_temp_dir("tool-checkpoint-api-config");
        std::fs::write(workspace.join("a.txt"), "before\n").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/checkpoints")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "label": "before edit" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::OK);
        let body = to_bytes(create.into_body(), usize::MAX).await.unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let checkpoint_id = created["data"]["id"].as_str().unwrap().to_string();
        assert_eq!(created["tool_name"], "checkpoint_create");
        assert_eq!(
            created["changed_refs"][0],
            format!("checkpoint:{checkpoint_id}")
        );

        std::fs::write(workspace.join("a.txt"), "after\n").unwrap();
        let diff = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/tools/checkpoints/{checkpoint_id}/diff"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let diff_status = diff.status();
        let body = to_bytes(diff.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            diff_status,
            StatusCode::OK,
            "checkpoint diff failed: {}",
            String::from_utf8_lossy(&body)
        );
        let diff_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(diff_json["data"]["changedFiles"][0], "a.txt");

        let restore = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tools/checkpoints/{checkpoint_id}/restore"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(restore.status(), StatusCode::OK);
        assert_eq!(
            std::fs::read_to_string(workspace.join("a.txt")).unwrap(),
            "before\n"
        );

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_batch_readonly_api_rejects_write_tools() {
        let workspace = test_temp_dir("tool-batch-api");
        let config_home = test_temp_dir("tool-batch-api-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let rejected = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/batch-readonly")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "calls": [{
                                "name": "write_file",
                                "input": { "path": "a.txt", "content": "no" }
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_intent_and_fanout_plan_are_readonly() {
        let app = api_router(test_state());

        let intent = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/intent-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "prompt": "review this WebUI change" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(intent.status(), StatusCode::OK);
        let body = to_bytes(intent.into_body(), usize::MAX).await.unwrap();
        let intent_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(intent_json["kind"], "tool.intent_plan");
        assert!(intent_json["recommended_tools"].as_array().unwrap().len() > 1);

        let fanout = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/context-fanout/plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "prompt": "发布前验收" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fanout.status(), StatusCode::OK);
        let body = to_bytes(fanout.into_body(), usize::MAX).await.unwrap();
        let fanout_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(fanout_json["kind"], "tool.context_fanout_plan");
        assert_eq!(fanout_json["batch_ready"], true);
    }

    #[tokio::test]
    async fn workspace_api_reports_profile_and_lists_files() {
        let workspace = test_temp_dir("workspace-list");
        let config_home = test_temp_dir("workspace-config");
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        std::fs::write(workspace.join("src").join("main.rs"), "fn main() {}\n").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let workspace_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(workspace_response.status(), StatusCode::OK);
        let body = to_bytes(workspace_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["profile_id"], "enterprise");
        assert_eq!(json["workspace_root"], workspace.display().to_string());

        let files_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?dir=src")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(files_response.status(), StatusCode::OK);
        let body = to_bytes(files_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["dir"], "src");
        assert_eq!(json["files"][0]["name"], "main.rs");
        assert_eq!(json["files"][0]["path"], "src/main.rs");
        assert_eq!(json["files"][0]["type"], "file");

        std::fs::create_dir_all(workspace.join("src/bin")).unwrap();
        std::fs::write(workspace.join("src/bin").join("tool.rs"), "fn tool() {}\n").unwrap();
        std::fs::create_dir_all(workspace.join("target/debug")).unwrap();
        std::fs::write(workspace.join("target/debug/ignored.rs"), "ignored").unwrap();
        let recursive_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?recursive=true&limit=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recursive_response.status(), StatusCode::OK);
        let body = to_bytes(recursive_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let paths = json["files"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item["path"].as_str())
            .collect::<Vec<_>>();
        assert!(paths.contains(&"src/main.rs"), "{paths:?}");
        assert!(paths.contains(&"src/bin/tool.rs"), "{paths:?}");
        assert!(!paths.iter().any(|path| path.starts_with("target/")));
        assert_eq!(json["recursive"], true);
        assert_eq!(json["truncated"], false);
    }

    #[tokio::test]
    async fn workspace_api_creates_reads_and_rejects_escape_paths() {
        let workspace = test_temp_dir("workspace-create");
        let config_home = test_temp_dir("workspace-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/workspace/files")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "path": "notes/audit.txt",
                            "content": "workspace isolation verified"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        assert_eq!(
            std::fs::read_to_string(workspace.join("notes/audit.txt")).unwrap(),
            "workspace isolation verified"
        );

        let raw_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/file/raw?path=notes/audit.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raw_response.status(), StatusCode::OK);
        let body = to_bytes(raw_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"workspace isolation verified");

        let escape_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?dir=..")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(escape_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn workspace_download_returns_files_and_directory_tar() {
        let workspace = test_temp_dir("workspace-download");
        let config_home = test_temp_dir("workspace-download-config");
        std::fs::create_dir_all(workspace.join("docs/nested")).unwrap();
        std::fs::write(workspace.join("docs/readme.md"), "# readme").unwrap();
        std::fs::write(workspace.join("docs/nested/a.txt"), "nested").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let file_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/download?path=docs%2Freadme.md")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(file_response.status(), StatusCode::OK);
        assert_eq!(
            file_response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"readme.md\""
        );
        let body = to_bytes(file_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"# readme");

        let dir_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/download?path=docs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(dir_response.status(), StatusCode::OK);
        assert_eq!(
            dir_response.headers()[header::CONTENT_TYPE],
            "application/x-tar"
        );
        let body = to_bytes(dir_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let mut archive = tar::Archive::new(std::io::Cursor::new(body));
        let names = archive
            .entries()
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect::<Vec<_>>();
        assert!(names.iter().any(|name| name == "docs/readme.md"));
        assert!(names.iter().any(|name| name == "docs/nested/a.txt"));
    }

    #[tokio::test]
    async fn workspace_upload_meta_delete_and_attachments_are_real() {
        let workspace = test_temp_dir("workspace-upload");
        let config_home = test_temp_dir("workspace-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let mkdir_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/workspace/dirs")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"path":"uploads"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mkdir_response.status(), StatusCode::CREATED);

        let boundary = "cowd-test-boundary";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"dir\"\r\n\r\nuploads\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"sample.md\"\r\nContent-Type: text/markdown\r\n\r\n# uploaded\r\n\r\n--{boundary}--\r\n"
        );
        let upload_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/upload")
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upload_response.status(), StatusCode::CREATED);
        let body = to_bytes(upload_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["path"], "uploads/sample.md");
        assert!(json["sha256"].as_str().unwrap().starts_with("sha256:"));
        assert_eq!(
            std::fs::read_to_string(workspace.join("uploads/sample.md")).unwrap(),
            "# uploaded\r\n"
        );

        let meta_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/meta?path=uploads%2Fsample.md")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(meta_response.status(), StatusCode::OK);
        let body = to_bytes(meta_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["item"]["path"], "uploads/sample.md");

        let add_attachment = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-1/attachments")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"path":"uploads/sample.md","label":"Uploaded markdown"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_attachment.status(), StatusCode::CREATED);
        let body = to_bytes(add_attachment.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let ref_id = json["attachment"]["ref_id"].as_str().unwrap().to_string();
        assert_eq!(json["attachment"]["path"], "uploads/sample.md");

        let list_attachment = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/sessions/session-1/attachments")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_attachment.status(), StatusCode::OK);
        let body = to_bytes(list_attachment.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count"], 1);

        let delete_attachment = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/sessions/session-1/attachments/{ref_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_attachment.status(), StatusCode::OK);

        let delete_file = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/workspace/files?path=uploads%2Fsample.md")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_file.status(), StatusCode::OK);
        assert!(!workspace.join("uploads/sample.md").exists());
    }

    #[tokio::test]
    async fn resource_upload_query_and_evidence_do_not_touch_workspace() {
        let workspace = test_temp_dir("resource-upload-workspace");
        let config_home = test_temp_dir("resource-upload-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let boundary = "cowd-resource-boundary";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"session_id\"\r\n\r\nsession-1\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"source\"\r\n\r\nwebui\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"voice.mp3\"\r\nContent-Type: application/octet-stream\r\n\r\nfake mp3 data\r\n--{boundary}--\r\n"
        );
        let upload_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/resources")
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upload_response.status(), StatusCode::CREATED);
        let body = to_bytes(upload_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let resource_id = json["resource"]["id"].as_str().unwrap().to_string();
        assert!(json["resource"]["uri"]
            .as_str()
            .unwrap()
            .starts_with("resource://"));
        assert_eq!(json["resource"]["kind"], "audio");
        assert_eq!(json["resource"]["detected_mime"], "audio/mpeg");
        assert!(json["hint"]["guardrails"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or("")
                .contains("Do not claim audio content")));
        assert!(!workspace.join("voice.mp3").exists());

        let metadata_path = config_home
            .join("storage")
            .join("resources")
            .join("metadata")
            .join(format!("{resource_id}.json"));
        assert!(metadata_path.exists());

        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/resources/{resource_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);

        let evidence_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/resources/{resource_id}/evidence"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence_response.status(), StatusCode::OK);
        let evidence_body = to_bytes(evidence_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&evidence_body).unwrap();
        assert!(evidence_json["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["action"] == "register_resource_from_path"));
    }

    #[tokio::test]
    async fn profile_api_creates_switches_and_deletes_profiles() {
        let app = api_router(test_state());

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/profiles")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["active_profile"], "default");
        assert_eq!(json["runtime_profile"], "default");

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "name": "Enterprise Ops" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["profile"]["id"], "enterprise_ops");
        assert_eq!(json["restart_required"], false);

        let switch_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles/switch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "profile": "enterprise_ops" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(switch_response.status(), StatusCode::OK);
        let body = to_bytes(switch_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["active_profile"], "enterprise_ops");
        assert_eq!(json["runtime_profile"], "default");
        assert_eq!(json["restart_required"], true);

        let delete_active_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/profiles/enterprise_ops")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_active_response.status(), StatusCode::BAD_REQUEST);

        let switch_back_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles/switch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "profile": "default" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(switch_back_response.status(), StatusCode::OK);

        let delete_response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/profiles/enterprise_ops")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_returns_empty() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_reads_unified_store_metadata() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record(
                "stored-session",
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_filters_and_paginates_unified_store() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());

        let mut auth_a = new_api_session_record("auth-a", Some("claude-sonnet-4-6".into()));
        auth_a.metadata_json = Some(serde_json::json!({"title":"Auth Audit A"}).to_string());
        auth_a.message_count = 3;
        auth_a.last_activity = "2026-06-04T00:03:00Z".to_string();
        store.create_session(&auth_a).await.unwrap();

        let mut auth_b = new_api_session_record("auth-b", Some("claude-sonnet-4-6".into()));
        auth_b.metadata_json = Some(serde_json::json!({"title":"Auth Audit B"}).to_string());
        auth_b.message_count = 8;
        auth_b.last_activity = "2026-06-04T00:08:00Z".to_string();
        store.create_session(&auth_b).await.unwrap();

        let mut closed = new_api_session_record("auth-closed", Some("claude-sonnet-4-6".into()));
        closed.metadata_json = Some(serde_json::json!({"title":"Auth Closed"}).to_string());
        closed.status = "closed".to_string();
        closed.message_count = 99;
        store.create_session(&closed).await.unwrap();

        let mut other_model =
            new_api_session_record("auth-other-model", Some("claude-haiku-4-5".into()));
        other_model.metadata_json =
            Some(serde_json::json!({"title":"Auth Other Model"}).to_string());
        store.create_session(&other_model).await.unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions?q=auth&model=claude-sonnet-4-6&status=active&sort=message_count&order=desc&limit=1&offset=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 2);
        assert_eq!(json["limit"], 1);
        assert_eq!(json["sessions"][0]["id"], "auth-b");
        assert_eq!(json["sessions"][0]["status"], "active");
        assert_eq!(json["sessions"][0]["model"], "claude-sonnet-4-6");
    }

    #[tokio::test]
    async fn get_session_prefers_unified_store_metadata() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "metadata-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("stored-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], session_id);
        assert_eq!(json["model"], "stored-model");
        assert!(json["created_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn patch_session_updates_cold_store_metadata() {
        let session_id = "patch-session";
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/sessions/{session_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"title":"Patch Session Title","model":"patched-model"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let record = store
            .get_session(session_id)
            .await
            .unwrap()
            .expect("stored session");
        assert_eq!(record.model.as_deref(), Some("patched-model"));
        assert!(record
            .metadata_json
            .as_deref()
            .unwrap_or("")
            .contains("Patch Session Title"));
    }

    #[tokio::test]
    async fn sync_runtime_session_persists_messages_and_events_idempotently() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        let session_id = "sync-session";
        let mut session = runtime::Session::new();
        session.session_id = session_id.to_string();
        session.model = Some("test-model".to_string());
        session.push_user_text("hello store").unwrap();
        session
            .push_message(runtime::ConversationMessage::assistant(vec![
                runtime::ContentBlock::Text {
                    text: "hello user".to_string(),
                },
            ]))
            .unwrap();

        sync_runtime_session_metadata_to_store(&store, session_id, &session)
            .await
            .unwrap();
        sync_runtime_session_metadata_to_store(&store, session_id, &session)
            .await
            .unwrap();

        let record = store.get_session(session_id).await.unwrap().unwrap();
        let messages = store.get_messages(session_id, 0, 10).await.unwrap();
        let events = store.get_events(session_id, 0).await.unwrap();

        assert_eq!(record.message_count, 2);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "user");
        assert_eq!(messages[1].role, "assistant");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "message_appended");
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].sequence, 1);

        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "TextDelta".to_string(),
                event_json: serde_json::json!({
                    "type": "TextDelta",
                    "content": "streamed",
                })
                .to_string(),
                sequence: 99,
                created_at_ms: 99,
            })
            .await
            .unwrap();

        session.messages.truncate(1);
        sync_runtime_session_metadata_to_store(&store, session_id, &session)
            .await
            .unwrap();
        let record = store.get_session(session_id).await.unwrap().unwrap();
        let messages = store.get_messages(session_id, 0, 10).await.unwrap();
        let events = store.get_events(session_id, 0).await.unwrap();

        assert_eq!(record.message_count, 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, "user");
        assert_eq!(events.len(), 2);
        let text_delta = events
            .iter()
            .find(|event| event.event_type == "TextDelta")
            .expect("non-message timeline events must survive transcript sync");
        assert_eq!(text_delta.sequence, 99);
        let message_event = events
            .iter()
            .find(|event| event.event_type == "message_appended")
            .expect("current transcript must have one message projection");
        assert!(message_event.sequence > text_delta.sequence);
    }

    #[tokio::test]
    async fn session_messages_support_sequence_paging_and_limit_cap() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "message-page-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let messages: Vec<memory::store::session::SessionMessage> = (0..1000)
            .map(|i| memory::store::session::SessionMessage {
                session_id: session_id.to_string(),
                sequence: i,
                role: if i % 2 == 0 { "user" } else { "assistant" }.to_string(),
                content_json: serde_json::json!([{"type":"text","text":format!("message {i}")}])
                    .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: i as u64,
            })
            .collect();
        store.insert_messages_batch(&messages).await.unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/messages?from_seq=990&limit=999"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 1000);
        assert_eq!(json["limit"], 500);
        assert_eq!(json["from_seq"], 990);
        assert_eq!(json["next_seq"], 1000);
        assert_eq!(json["has_more"], false);
        assert_eq!(json["messages"].as_array().unwrap().len(), 10);
        assert_eq!(json["messages"][0]["sequence"], 990);
        assert_eq!(json["messages"][9]["sequence"], 999);
    }

    #[tokio::test]
    async fn delete_session_removes_cold_store_record() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "cold-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(store.get_session(session_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn session_events_reads_unified_store_event_log() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "event-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({
                    "type": "message_appended",
                    "sequence": 0,
                    "role": "user",
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 1_234,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/events?from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["events"][0]["type"], "message_appended");
        assert_eq!(json["events"][0]["sequence"], 0);
        assert_eq!(json["events"][0]["payload"]["role"], "user");
        assert_eq!(json["has_more"], false);
    }

    #[tokio::test]
    async fn session_cancel_records_gateway_control_event() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "cancel-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let app = api_router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{session_id}/cancel"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "actor_id": "tui:test",
                            "reason": "test_cancel",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["status"], "cancel_requested");
        assert_eq!(json["actor_id"], "tui:test");
        assert_eq!(json["aborted"], false);
        assert_eq!(json["run_id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn runtime_timeline_projection_is_paged() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-timeline-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ToolStart".to_string(),
                event_json: serde_json::json!({"tool": "bash"}).to_string(),
                sequence: 0,
                created_at_ms: 10,
            })
            .await
            .unwrap();
        store
            .append_session_domain_event(&memory::SessionDomainEvent::new(
                session_id,
                1,
                memory::SessionDomainScope::Memory,
                "memory.pulse.created",
                serde_json::json!({"candidates": 2}),
                11,
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&from_seq=0&limit=1"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 2);
        assert_eq!(json["events"].as_array().unwrap().len(), 1);
        assert_eq!(json["events"][0]["kind"], "ToolStart");
        assert_eq!(json["events"][0]["scope"], "tool");
        assert_eq!(json["next_seq"], 1);
        assert_eq!(json["has_more"], true);
        assert_eq!(json["degraded"], false);
    }

    #[tokio::test]
    async fn runtime_timeline_projects_execution_graph_summary() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-execution_graph-summary-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let event = runtime::RuntimeEventInput {
            stream_id: session_id.to_string(),
            scope: runtime::RuntimeEventScope::ExecutionGraph,
            kind: "agent.execution_graph.reviewed".to_string(),
            status: Some("completed".to_string()),
            actor: Some("gateway-test".to_string()),
            refs: vec![
                runtime::RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: "graph-summary".to_string(),
                },
                runtime::RuntimeEventRef {
                    kind: "collaboration_board".to_string(),
                    id: "board-summary".to_string(),
                },
            ],
            payload: serde_json::json!({
                "board_id": "board-summary",
                "graph": {
                    "graph_id": "graph-summary",
                    "status": "completed",
                    "nodes": [
                        {"kind": "AgentTask", "node_id": "task-1"},
                        {"kind": "Synthesis", "node_id": "synthesis-board-summary"}
                    ]
                },
                "scorecard": {
                    "completion_rate": 1.0,
                    "synthesis_lift": 1.2,
                    "complementarity_score": 0.75,
                    "conflict_count": 1
                },
                "value_verdict": {
                    "positive_lift": true,
                    "continue_multi_agent": true,
                    "value_score": 70,
                    "reasons": ["positive_multi_agent_lift"]
                },
                "maintenance_candidates": [{"id": "candidate-summary"}]
            }),
        };

        let state = test_state_with_store(store);
        state.services.runtime_events.store().append(event).unwrap();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["execution_graph_summary"]["count"], 1);
        assert_eq!(
            json["execution_graph_summary"]["latest"]["graph_id"],
            "graph-summary"
        );
        assert_eq!(
            json["execution_graph_summary"]["latest"]["board_id"],
            "board-summary"
        );
        assert_eq!(
            json["execution_graph_summary"]["latest"]["completion_rate"],
            1.0
        );
        assert_eq!(
            json["execution_graph_summary"]["latest"]["value_verdict"]["positive_lift"],
            true
        );
        assert_eq!(json["execution_graph_summary"]["agent_tasks"], 1);
        assert_eq!(json["execution_graph_summary"]["memory_candidates"], 1);
        assert_eq!(json["execution_graph_summary"]["conflicts"], 1);
        assert_eq!(json["agent_value"]["status"], "review_required");
        assert_eq!(json["agent_value"]["recommendation"], "review_conflicts");
        assert_eq!(json["agent_value"]["policy_passed"], false);
        assert_eq!(json["agent_value"]["latest"]["agent_tasks"], 1);
        assert_eq!(json["agent_value"]["latest"]["value_score"], 70);
    }

    #[tokio::test]
    async fn runtime_timeline_projects_health_summary() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-health-summary-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        store
            .append_session_domain_event(&memory::SessionDomainEvent::new(
                session_id,
                0,
                memory::SessionDomainScope::ApplicationTask,
                "task.started",
                serde_json::json!({"task_id": "task-health"}),
                10,
            ))
            .await
            .unwrap();
        store
            .append_session_domain_event(&memory::SessionDomainEvent::new(
                session_id,
                1,
                memory::SessionDomainScope::Policy,
                "runtime.policy.decided",
                serde_json::json!({
                    "agent_mode": "Parallel",
                    "requires_review": false,
                    "complexity": {
                        "level": "Complex",
                        "score": 72,
                        "signals": [{"name": "verification_required"}]
                    }
                }),
                11,
            ))
            .await
            .unwrap();
        state
            .services
            .runtime_events
            .append(
                session_id,
                runtime::RuntimeEventScope::ExecutionGraph,
                "agent.execution_graph.reviewed",
                serde_json::json!({
                    "value_verdict": {
                        "positive_lift": true,
                        "continue_multi_agent": true,
                        "value_score": 73,
                        "reasons": ["positive_multi_agent_lift"]
                    }
                }),
            )
            .unwrap();
        store
            .append_session_domain_event(&memory::SessionDomainEvent::new(
                session_id,
                3,
                memory::SessionDomainScope::ApplicationTask,
                "task.completed",
                serde_json::json!({"task_id": "task-health"}),
                13,
            ))
            .await
            .unwrap();

        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["health_summary"]["status"], "healthy");
        assert_eq!(json["health_summary"]["event_count"], 4);
        assert_eq!(json["health_summary"]["failed_events"], 0);
        assert_eq!(json["health_summary"]["degraded_events"], 0);
        assert_eq!(json["health_summary"]["open_tasks"], 0);
        assert_eq!(json["health_summary"]["positive_agent_lift"], true);
        assert_eq!(json["health_summary"]["latest_value_score"], 73);
        assert_eq!(
            json["health_summary"]["latest_policy"]["agent_mode"],
            "Parallel"
        );
        assert_eq!(json["health_summary"]["scope_counts"]["task"], 2);
        assert_eq!(json["health_summary"]["scope_counts"]["policy"], 1);
        assert_eq!(json["health_summary"]["scope_counts"]["execution_graph"], 1);
        assert_eq!(json["value_loop"]["status"], "incomplete");
        assert_eq!(json["value_loop"]["required_observed"], 3);
        assert_eq!(json["value_loop"]["missing_required_count"], 4);
        assert_eq!(json["value_loop"]["positive_agent_lift"], true);
    }

    #[tokio::test]
    async fn runtime_projection_degrades_missing_sources() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/timeline?session_id=missing-store")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["degraded"], true);
        assert_eq!(json["events"].as_array().unwrap().len(), 0);
        assert_eq!(json["execution_graph_summary"]["count"], 0);
        assert_eq!(json["health_summary"]["status"], "degraded");
        assert_eq!(json["health_summary"]["score"], 35);
        assert_eq!(json["health_summary"]["degraded_events"], 0);
        assert_eq!(
            json["health_summary"]["reasons"][0],
            "session store not available"
        );
        assert_eq!(json["value_loop"]["status"], "degraded");
        assert_eq!(json["value_loop"]["missing_required_count"], 7);
        assert_eq!(
            json["value_loop"]["reasons"][0],
            "session store not available"
        );
        assert_eq!(json["agent_value"]["status"], "degraded");
        assert_eq!(
            json["agent_value"]["recommendation"],
            "collect_execution_graph_review"
        );
    }

    #[tokio::test]
    async fn runtime_effective_config_exposes_default_control_policy() {
        let root = test_temp_dir("runtime-control-default");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/config/effective")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["source"], "default");
        assert_eq!(json["scenario"], "coding");
        assert_eq!(json["control_policy"]["enabled"], true);
        assert_eq!(json["control_policy"]["agent"]["max_parallel_agents"], 4);
        assert_eq!(
            json["control_policy"]["task"]["max_failures_before_review"],
            2
        );
        assert!(json["control_policy"]["task"].get("thresholds").is_none());
        assert!(json["warnings"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn runtime_effective_config_exposes_configured_control_policy() {
        let root = test_temp_dir("runtime-control-config");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
runtime:
  scenario: office
  control:
    enabled: false
    agent:
      max_parallel_agents: 2
      min_collaboration_score: 77
    context:
      yolo_budget_tokens: 7000
"#,
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/config/effective")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["source"], "config");
        assert_eq!(json["scenario"], "office");
        assert_eq!(json["control_policy"]["enabled"], false);
        assert_eq!(json["control_policy"]["agent"]["max_parallel_agents"], 2);
        assert_eq!(
            json["control_policy"]["agent"]["min_collaboration_score"],
            77
        );
        assert_eq!(
            json["control_policy"]["context"]["yolo_budget_tokens"],
            7000
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runtime_control_plane_reports_degraded_kernel_without_store() {
        let root = test_temp_dir("runtime-control-plane-degraded");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_control_plane");
        assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(json["status"], "degraded");
        assert_eq!(json["degraded"], true);
        assert_eq!(json["components"]["session"]["durable_store"], false);
        assert_eq!(
            json["components"]["session"]["source_of_truth"],
            "unavailable"
        );
        assert_eq!(json["components"]["context"]["durable_history"], false);
        assert_eq!(json["components"]["memory"]["status"], "unavailable");
        assert_eq!(json["components"]["permissions"]["auth_required"], false);
        assert_eq!(
            json["components"]["session"]["leases"]["status"],
            "unavailable"
        );
        assert_eq!(json["diagnostics"]["durable_session_store"], false);
        assert_eq!(json["diagnostics"]["memory_attached"], false);
        assert_eq!(
            json["diagnostics"]["stored_sessions"],
            serde_json::Value::Null
        );
        assert_eq!(json["diagnostics"]["component_count"], 10);
        assert_eq!(json["diagnostics"]["degraded_component_count"], 2);
        assert_eq!(json["diagnostics"]["attention_component_count"], 2);
        assert_eq!(
            json["diagnostics"]["capability_count"],
            serde_json::json!(
                11 + json["diagnostics"]["connector_capability_count"]
                    .as_u64()
                    .unwrap()
            )
        );
        assert!(json["diagnostics"]["elapsed_ms"].as_u64().is_some());
        assert!(matches!(
            json["diagnostics"]["performance_status"].as_str(),
            Some("healthy" | "attention" | "degraded")
        ));
        assert_eq!(json["diagnostics"]["provider_configured"], false);
        assert_eq!(json["diagnostics"]["provider_count"], 0);
        assert_eq!(json["diagnostics"]["provider_model_count"], 0);
        assert_eq!(json["diagnostics"]["configured_model_resolved"], true);
        assert_eq!(json["diagnostics"]["production_ready"], false);
        assert_eq!(json["diagnostics"]["required_check_count"], 11);
        assert_eq!(json["diagnostics"]["ready_required_count"], 7);
        assert_eq!(json["diagnostics"]["blocked_required_count"], 4);
        assert_eq!(json["readiness"]["production_ready"], false);
        assert_eq!(json["readiness"]["score"], 63);
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "session.sqlite_source_of_truth"));
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "memory.manager"));
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.registry"));
        assert!(json["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action
                .as_str()
                .unwrap_or_default()
                .contains("SQLite session store")));
        assert!(json["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action
                .as_str()
                .unwrap_or_default()
                .contains("runtime provider")));
        assert!(json["degraded_reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "session store not available"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runtime_session_lease_routes_share_runtime_host_registry_projection() {
        let registry = Arc::new(session::SessionLeaseRegistry::default());
        let app = api_router(test_state_with_lease_registry(registry));

        let acquire = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/acquire")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "session-a",
                            "owner": "tui:one",
                            "mode": "exclusive"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(acquire.status(), StatusCode::OK);
        let body = to_bytes(acquire.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["owner"], "tui:one");
        assert_eq!(json["mode"], "exclusive");
        assert!(json["acquired_at_ms"].as_u64().is_some());

        let list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/session-leases")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let body = to_bytes(list.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "available");
        assert_eq!(json["total"], 1);
        assert_eq!(json["leases"][0]["session_id"], "session-a");

        let control = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(control.status(), StatusCode::OK);
        let body = to_bytes(control.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["components"]["session"]["leases"]["attached"], true);
        assert_eq!(json["components"]["session"]["leases"]["total"], 1);
        assert_eq!(
            json["components"]["session"]["leases"]["leases"][0]["owner"],
            "tui:one"
        );

        let release = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/release")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "session-a",
                            "owner": "tui:one"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(release.status(), StatusCode::OK);
        let body = to_bytes(release.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["released"], true);
    }

    #[tokio::test]
    async fn runtime_control_plane_reports_durable_store_and_task_state() {
        let root = test_temp_dir("runtime-control-plane-durable");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let state = test_state_with_store_and_workspace(store, workspace, config_home);
        state
            .services
            .task
            .start_goal("control plane smoke task", true)
            .unwrap();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_control_plane");
        assert_eq!(json["status"], "attention");
        assert_eq!(json["degraded"], false);
        assert_eq!(json["components"]["session"]["durable_store"], true);
        assert_eq!(json["components"]["session"]["source_of_truth"], "sqlite");
        assert_eq!(json["components"]["context"]["durable_history"], true);
        assert_eq!(json["components"]["task"]["total"], 1);
        assert_eq!(json["components"]["task"]["open"], 1);
        assert_eq!(json["components"]["task"]["status_counts"]["running"], 1);
        assert_eq!(json["diagnostics"]["durable_session_store"], true);
        assert_eq!(json["diagnostics"]["memory_attached"], false);
        assert_eq!(json["diagnostics"]["active_sessions"], 0);
        assert_eq!(json["diagnostics"]["stored_sessions"], 0);
        assert_eq!(json["diagnostics"]["open_tasks"], 1);
        assert_eq!(json["diagnostics"]["component_count"], 10);
        assert_eq!(json["diagnostics"]["degraded_component_count"], 0);
        assert_eq!(json["diagnostics"]["attention_component_count"], 2);
        assert!(json["diagnostics"]["elapsed_ms"].as_u64().is_some());
        assert!(matches!(
            json["diagnostics"]["performance_status"].as_str(),
            Some("healthy" | "attention" | "degraded")
        ));
        assert_eq!(json["diagnostics"]["provider_configured"], false);
        assert_eq!(json["components"]["provider"]["status"], "unconfigured");
        assert_eq!(json["diagnostics"]["production_ready"], false);
        assert_eq!(json["diagnostics"]["required_check_count"], 11);
        assert_eq!(json["diagnostics"]["ready_required_count"], 9);
        assert_eq!(json["diagnostics"]["blocked_required_count"], 2);
        assert_eq!(json["readiness"]["production_ready"], false);
        assert_eq!(json["readiness"]["score"], 81);
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "memory.manager"));
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.registry"));
        assert!(json["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action
                .as_str()
                .unwrap_or_default()
                .contains("memory manager")));
        assert_eq!(
            json["components"]["channels"]["adapters"][0]["id"],
            "wechat-ilink"
        );
        assert!(json["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "permission.cross_plane"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runtime_control_plane_counts_file_backed_sqlite_sessions_after_reopen() {
        let dir = test_temp_dir("runtime-control-plane-db");
        let db_path = dir.join("sessions.db");
        {
            let store = UnifiedSessionStore::open(&db_path).unwrap();
            store
                .create_session(&new_api_session_record(
                    "control-db-session-a",
                    Some("model-a".into()),
                ))
                .await
                .unwrap();
            store
                .create_session(&new_api_session_record(
                    "control-db-session-b",
                    Some("model-b".into()),
                ))
                .await
                .unwrap();
        }
        assert!(
            db_path.exists(),
            "file-backed session database should exist"
        );

        let workspace = dir.join("workspace");
        let config_home = dir.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let reopened = Arc::new(UnifiedSessionStore::open(&db_path).unwrap());
        let app = api_router(test_state_with_store_and_workspace(
            reopened,
            workspace,
            config_home,
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_control_plane");
        assert_eq!(json["components"]["session"]["durable_store"], true);
        assert_eq!(json["components"]["session"]["source_of_truth"], "sqlite");
        assert_eq!(json["diagnostics"]["durable_session_store"], true);
        assert_eq!(json["diagnostics"]["stored_sessions"], 2);
        assert_eq!(json["diagnostics"]["active_sessions"], 0);
        assert_eq!(json["diagnostics"]["open_tasks"], 0);
        assert!(json["diagnostics"]["elapsed_ms"].as_u64().is_some());
        assert!(matches!(
            json["diagnostics"]["performance_status"].as_str(),
            Some("healthy" | "attention" | "degraded")
        ));
        assert_eq!(json["diagnostics"]["production_ready"], false);
        assert_eq!(json["diagnostics"]["required_check_count"], 11);
        assert_eq!(json["diagnostics"]["ready_required_count"], 9);
        assert_eq!(json["diagnostics"]["blocked_required_count"], 2);
        assert_eq!(json["readiness"]["score"], 81);
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "context.durable_history" && check["status"] == "ready"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn runtime_control_plane_reports_provider_config_without_secrets() {
        let root = test_temp_dir("runtime-control-provider-config");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: "sonnet-enterprise"
providers:
  anthropic:
    base_url: "https://api.anthropic.example/v1"
    api_key: "secret-provider-key"
    models: ["sonnet-enterprise", "haiku-enterprise"]
    protocol: "anthropic"
"#,
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["components"]["provider"]["status"], "available");
        assert_eq!(json["components"]["provider"]["provider_count"], 1);
        assert_eq!(json["components"]["provider"]["model_count"], 2);
        assert_eq!(
            json["components"]["provider"]["configured_model"],
            "sonnet-enterprise"
        );
        assert_eq!(
            json["components"]["provider"]["configured_model_provider"],
            "anthropic"
        );
        assert_eq!(
            json["components"]["provider"]["configured_model_resolved"],
            true
        );
        assert!(json["components"]["provider"]["catalog_generation"]
            .as_str()
            .unwrap_or_default()
            .starts_with("provider-catalog-v1-"));
        assert_eq!(
            json["components"]["provider"]["catalog"]["models"][0]["effective_protocol"],
            "anthropic"
        );
        assert_eq!(
            json["components"]["provider"]["provider_names"]
                .as_array()
                .unwrap(),
            &vec![serde_json::Value::from("anthropic")]
        );
        assert_eq!(json["diagnostics"]["provider_configured"], true);
        assert_eq!(json["diagnostics"]["provider_count"], 1);
        assert_eq!(json["diagnostics"]["provider_model_count"], 2);
        assert_eq!(json["diagnostics"]["configured_model_resolved"], true);
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.registry" && check["status"] == "ready"));
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.model_routing" && check["status"] == "ready"));
        assert!(!json.to_string().contains("secret-provider-key"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn config_providers_and_update_config_are_real_and_redacted() {
        let root = test_temp_dir("system-config-providers");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: "model-a"
providers:
  local:
    base_url: "https://local.example/v1"
    api_key: "secret-local-key"
    models: ["model-a", "model-b"]
    protocol: "completions"
"#,
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(workspace, config_home.clone()));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/config/providers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["provider_count"], 1);
        assert_eq!(json["provider_model_count"], 2);
        assert_eq!(json["configured_model"], "model-a");
        assert_eq!(json["models"][1]["id"], "model-b");
        assert_eq!(json["models"][1]["effective_protocol"], "completions");
        assert_eq!(json["models"][1]["protocol_configured"], true);
        assert!(json["catalog_generation"]
            .as_str()
            .unwrap_or_default()
            .starts_with("provider-catalog-v1-"));
        assert_eq!(json["catalog"]["providers"][0]["id"], "local");
        assert_eq!(json["catalog"]["models"][1]["id"], "model-b");
        assert_eq!(json["catalog"]["profiles"][0]["id"], "default");
        assert_eq!(json["providers"][0]["effective_protocol"], "completions");
        assert_eq!(json["providers"][0]["protocol_configured"], true);
        assert_eq!(json["providers"][0]["credential_present"], true);
        assert!(!json.to_string().contains("secret-local-key"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/config/provider-catalog")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let catalog_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            catalog_json["catalog"]["generation"],
            json["catalog_generation"]
        );
        assert_eq!(catalog_json["catalog"]["models"][0]["provider"], "local");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["providers"]["local"]["api_key"], "[redacted]");
        assert!(!json.to_string().contains("secret-local-key"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/config")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"model-b"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let written = std::fs::read_to_string(config_home.join("config.yaml")).unwrap();
        assert!(written.contains("model-b"));

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/config")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"missing-model"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"]
            .as_str()
            .unwrap_or_default()
            .contains("model `missing-model` is not declared"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn slash_catalog_dispatch_and_history_are_available() {
        let root = test_temp_dir("slash-gateway");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();

        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/slash?surface=webui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["commands"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command["name"] == "/status"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/slash/slash.status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["slash"]["id"], "slash.status");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/slash/resolve")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"input":"/status","surface":"webui"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["resolution"]["slash"]["name"], "/status");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/slash/dispatch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"command":"/status","args":{"session_id":"s1"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["slash"], "/status");
        assert!(matches!(
            json["status"].as_str(),
            Some("complete" | "degraded")
        ));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/slash/dispatch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"command":"/compact","args":{}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["status"], "dispatch_required");
        assert_eq!(json["data"]["dispatch"], "runtime_service");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/slash/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 2);
    }

    #[tokio::test]
    async fn runtime_provider_reload_replaces_runtime_registry_from_config() {
        let root = test_temp_dir("runtime-provider-reload");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: "reload-model"
providers:
  reload:
    base_url: "https://reload.example/v1"
    api_key: "reload-secret-key"
    models: ["reload-model", "reload-fast"]
    protocol: "completions"
"#,
        )
        .unwrap();

        let state = test_state_with_workspace(workspace, config_home);
        let provider_registry = state.services.runtime.as_ref().unwrap().provider_registry();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/providers/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_provider_reload");
        assert_eq!(json["status"], "applied");
        assert_eq!(json["applied"], true);
        assert_eq!(json["provider_count"], 1);
        assert_eq!(json["provider_model_count"], 2);
        assert_eq!(json["configured_model"], "reload-model");
        assert_eq!(json["configured_model_provider"], "reload");
        assert_eq!(json["configured_model_resolved"], true);
        assert!(!json.to_string().contains("reload-secret-key"));
        let provider_snapshot = provider_registry.pin();
        let provider = provider_snapshot
            .resolve("reload-model")
            .expect("reloaded provider should resolve model");
        assert_eq!(provider.name, "reload");
        assert_eq!(provider.models, vec!["reload-model", "reload-fast"]);

        let invalid_root = test_temp_dir("runtime-provider-reload-invalid");
        let invalid_workspace = invalid_root.join("workspace");
        let invalid_config_home = invalid_root.join("home");
        std::fs::create_dir_all(&invalid_workspace).unwrap();
        std::fs::create_dir_all(&invalid_config_home).unwrap();
        std::fs::write(
            invalid_config_home.join("config.yaml"),
            r#"
model: "broken-model"
providers:
  broken:
    base_url: "https://broken.example/v1"
    api_key: "broken-secret-key"
    models: ["broken-model"]
    protocol: "unsupported-protocol"
"#,
        )
        .unwrap();

        let invalid_state = test_state_with_workspace(invalid_workspace, invalid_config_home);
        let invalid_registry = invalid_state
            .services
            .runtime
            .as_ref()
            .unwrap()
            .provider_registry();
        let app = api_router(invalid_state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/providers/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_provider_reload");
        assert_eq!(json["status"], "failed");
        assert_eq!(json["applied"], false);
        assert_eq!(json["configured_model_resolved"], false);
        assert!(json["warnings"]
            .to_string()
            .contains("unsupported-protocol"));
        assert!(!json.to_string().contains("broken-secret-key"));
        assert!(invalid_registry.pin().resolve("broken-model").is_none());
        let retained_snapshot = provider_registry.pin();
        assert_eq!(
            retained_snapshot
                .resolve("reload-model")
                .expect("existing provider should remain after failed reload")
                .name,
            "reload"
        );

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(invalid_root);
    }

    #[tokio::test]
    async fn runtime_config_reload_applies_gateway_runtime_dependencies() {
        let root = test_temp_dir("runtime-config-reload");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        let webui_dir = root.join("webui");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::create_dir_all(&webui_dir).unwrap();
        std::fs::write(webui_dir.join("index.html"), "<!doctype html>reload").unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            format!(
                r#"
model: "reload-model"
providers:
  reload:
    base_url: "https://reload.example/v1"
    api_key: "reload-secret-key"
    models: ["reload-model"]
    protocol: "completions"
gateway:
  enabled: true
  webui_dir: "{}"
  platforms:
    - platform_type: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
    - platform_type: "feishu"
      enabled: true
      app_id: "app-id"
      app_secret: "app-secret"
"#,
                webui_dir.display()
            ),
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/config/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "gateway.config.reload");
        assert_eq!(json["applied"], true);
        assert_eq!(json["applied_sections"]["providers"]["provider_count"], 1);
        assert_eq!(
            json["applied_sections"]["surface_runtime_configs"]["count"],
            1
        );
        assert_eq!(json["applied_sections"]["static_webui"]["status"], "ready");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/message-connectors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let connectors: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let feishu = connectors["connectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|connector| connector["connector"] == "feishu")
            .expect("feishu message connector should be projected from reloaded config");
        assert_eq!(feishu["configured"], true);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_config_reload_rejects_invalid_config_without_replacing_running_state() {
        let root = test_temp_dir("runtime-config-reload-invalid-preserve");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let config_path = config_home.join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
model: "stable-model"
providers:
  stable:
    base_url: "https://stable.example/v1"
    api_key: "stable-secret-key"
    models: ["stable-model"]
    protocol: "completions"
"#,
        )
        .unwrap();

        let state = test_state_with_workspace(workspace, config_home);
        let provider_registry = state.services.runtime.as_ref().unwrap().provider_registry();
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/config/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["applied"], true);
        assert!(provider_registry.pin().resolve("stable-model").is_some());

        std::fs::write(&config_path, "model: [\n").unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/config/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "invalid");
        assert_eq!(json["applied"], false);
        assert!(provider_registry.pin().resolve("stable-model").is_some());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/config/reload/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["kind"], "gateway.config.reload.status");
        assert_eq!(status["status"], "invalid");
        assert_eq!(status["applied"], false);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(trace_capture)]
    async fn runtime_control_plane_emits_structured_trace_event() {
        use tracing_subscriber::prelude::*;

        let root = test_temp_dir("runtime-control-plane-trace");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let state = test_state_with_store_and_workspace(store, workspace, config_home);
        state
            .services
            .task
            .start_goal("trace control plane", false)
            .unwrap();
        let _trace_guard = trace_capture_lock().lock().await;
        let capture = CapturedTraceEvents::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());

        let _default_trace_subscriber = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();
        let Json(json) = runtime_routes::get_runtime_control_plane(AxumState(state)).await;
        assert_eq!(json["kind"], "runtime_control_plane");
        let lines = capture.lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("runtime control plane inspected"),
            "expected control-plane trace event, got: {joined}"
        );
        assert!(joined.contains("cowd.runtime.control_plane"));
        assert!(joined.contains("status=\"attention\""));
        assert!(joined.contains("performance_status="));
        assert!(joined.contains("elapsed_ms="));
        assert!(joined.contains("production_ready=false"));
        assert!(joined.contains("readiness_score=81"));
        assert!(joined.contains("blocked_required_count=2"));
        assert!(joined.contains("degraded=false"));
        assert!(joined.contains("durable_session_store=true"));
        assert!(joined.contains("memory_attached=false"));
        assert!(joined.contains("provider_configured=false"));
        assert!(joined.contains("provider_count=0"));
        assert!(joined.contains("provider_model_count=0"));
        assert!(joined.contains("configured_model_resolved=true"));
        assert!(joined.contains("stored_sessions=0"));
        assert!(joined.contains("open_tasks=1"));
        assert!(joined.contains("component_count=10"));
        assert!(joined.contains("capability_count="));

        let _ = std::fs::remove_dir_all(root);
    }

    fn test_context_envelope(
        session_id: &str,
        envelope_id: &str,
        intent: &str,
    ) -> serde_json::Value {
        let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile: ContextProfile::MainTurn,
            identity: ContextIdentity::main(session_id),
            intent: intent.to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![ContextItem::new(
                format!("{envelope_id}-item"),
                ContextSourceKind::Memory,
                ContextRole::Orientation,
                "orientation",
            )],
            omitted: Vec::new(),
            total_budget_tokens: 4_000,
        });
        envelope.id = envelope_id.to_string();
        serde_json::json!({
            "type": "ContextEnvelope",
            "envelope_id": envelope.id,
            "run_id": format!("run-{envelope_id}"),
            "session_id": session_id,
            "envelope": envelope,
        })
    }

    #[tokio::test]
    async fn session_context_history_reads_context_events_only() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-history-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, event_type, payload) in [
            (
                0,
                "TextDelta",
                serde_json::json!({"type":"TextDelta","content":"skip"}),
            ),
            (
                1,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-1", "first"),
            ),
            (
                2,
                "ToolStart",
                serde_json::json!({"type":"ToolStart","name":"skip"}),
            ),
            (
                3,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-2", "second"),
            ),
        ] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: event_type.to_string(),
                    event_json: payload.to_string(),
                    sequence: sequence as usize,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 2);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 2);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 2);
        assert_eq!(json["envelopes"][0]["sequence"], 1);
        assert_eq!(json["envelopes"][0]["envelope_id"], "env-1");
        assert_eq!(json["envelopes"][0]["run_id"], "run-env-1");
        assert_eq!(json["envelopes"][1]["envelope"]["intent"], "second");
        assert_eq!(json["summaries"][0]["envelope_id"], "env-1");
        assert_eq!(json["summaries"][0]["profile"], "MainTurn");
        assert_eq!(json["summaries"][0]["intent"], "first");
        assert_eq!(json["summaries"][0]["selected_count"], 1);
        assert_eq!(json["summaries"][0]["omitted_count"], 0);
    }

    #[tokio::test]
    async fn session_context_history_can_return_summaries_without_full_envelopes() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-summary-only-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(session_id, "env-summary", "summary").to_string(),
                sequence: 5,
                created_at_ms: 5,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["include_envelopes"], false);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 0);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 1);
        assert_eq!(json["summaries"][0]["envelope_id"], "env-summary");
        assert_eq!(json["summaries"][0]["intent"], "summary");
    }

    #[tokio::test]
    async fn session_context_history_paginates_summary_timeline() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-summary-page-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, envelope_id, intent) in [
            (1, "env-page-1", "first"),
            (3, "env-page-3", "second"),
            (5, "env-page-5", "third"),
        ] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: "ContextEnvelope".to_string(),
                    event_json: test_context_envelope(session_id, envelope_id, intent).to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?limit=2&include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 3);
        assert_eq!(json["has_more"], true);
        assert_eq!(json["next_seq"], 4);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 0);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 2);
        assert_eq!(json["summaries"][0]["envelope_id"], "env-page-1");
        assert_eq!(json["summaries"][1]["envelope_id"], "env-page-3");
    }

    #[tokio::test]
    async fn session_context_history_matches_sqlite_event_log() {
        let dir = test_temp_dir("context-db-timeline");
        let db_path = dir.join("sessions.sqlite");
        let store = Arc::new(UnifiedSessionStore::open(&db_path).unwrap());
        let session_id = "context-db-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, event_type, payload) in [
            (
                0,
                "TextDelta",
                serde_json::json!({"type":"TextDelta","content":"not context"}),
            ),
            (
                1,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-db-1", "first db context"),
            ),
            (
                2,
                "ToolComplete",
                serde_json::json!({"type":"ToolComplete","summary":"not context"}),
            ),
            (
                3,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-db-3", "second db context"),
            ),
        ] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: event_type.to_string(),
                    event_json: payload.to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let db_context_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND event_type = 'ContextEnvelope'",
                [session_id],
                |row| row.get(0),
            )
            .unwrap();
        let db_all_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?limit=1&include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(db_all_count, 4);
        assert_eq!(db_context_count, 2);
        assert_eq!(json["total"], db_context_count);
        assert_eq!(json["has_more"], true);
        assert_eq!(json["next_seq"], 2);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 0);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 1);
        assert_eq!(json["summaries"][0]["sequence"], 1);
        assert_eq!(json["summaries"][0]["envelope_id"], "env-db-1");

        let detail_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/env-db-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail_body = to_bytes(detail_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail_json: serde_json::Value = serde_json::from_slice(&detail_body).unwrap();
        assert_eq!(detail_json["context"]["sequence"], 3);
        assert_eq!(
            detail_json["context"]["envelope"]["intent"],
            "second db context"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(trace_capture)]
    async fn session_context_history_emits_structured_trace_events() {
        use tracing_subscriber::prelude::*;

        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-log-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(session_id, "env-log-1", "logged").to_string(),
                sequence: 7,
                created_at_ms: 77,
            })
            .await
            .unwrap();

        let _trace_guard = trace_capture_lock().lock().await;
        let capture = CapturedTraceEvents::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());

        let _default_trace_subscriber = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();
        let state = test_state_with_store(store);
        let app = api_router(state);
        let history_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(history_response.status(), StatusCode::OK);
        let history_body = to_bytes(history_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let history_json: serde_json::Value = serde_json::from_slice(&history_body).unwrap();
        assert_eq!(history_json["session_id"], session_id);
        assert_eq!(history_json["include_envelopes"], false);
        assert_eq!(history_json["total"], 1);
        assert_eq!(history_json["summaries"].as_array().unwrap().len(), 1);

        let detail_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/env-log-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail_body = to_bytes(detail_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail_json: serde_json::Value = serde_json::from_slice(&detail_body).unwrap();
        assert_eq!(detail_json["context"]["envelope_id"], "env-log-1");

        let lines = capture.lines();
        let joined = lines.join("\n");
        if !joined.is_empty() {
            assert!(
                joined.contains("context history loaded")
                    || joined.contains("context envelope loaded"),
                "unexpected structured context trace output: {joined}"
            );
            assert!(joined.contains("context-log-session"));
            if joined.contains("context history loaded") {
                assert!(joined.contains("include_envelopes=false"));
                assert!(joined.contains("total=1"));
            } else {
                assert!(
                    joined.contains("envelope_id=env-log-1")
                        || joined.contains("envelope_id=\"env-log-1\"")
                );
                assert!(joined.contains("sequence=7"));
            }
        }
    }

    #[tokio::test]
    async fn context_envelope_route_reads_by_envelope_id() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-id-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(session_id, "env-target", "inspect").to_string(),
                sequence: 4,
                created_at_ms: 4,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/env-target")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["source"], "history");
        assert_eq!(json["context"]["session_id"], session_id);
        assert_eq!(json["context"]["sequence"], 4);
        assert_eq!(json["context"]["envelope"]["id"], "env-target");
    }

    #[tokio::test]
    async fn context_recommendation_action_records_session_event() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-recommendation-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store.clone());
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/sessions/{session_id}/context/recommendations"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "envelope_id": "env-1",
                            "recommendation": "Start a handoff",
                            "action": "acknowledged",
                            "note": "handled"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let events = store.get_events(session_id, 0).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "ContextRecommendationAction");
        let payload: serde_json::Value = serde_json::from_str(&events[0].event_json).unwrap();
        assert_eq!(payload["envelope_id"], "env-1");
        assert_eq!(payload["recommendation"], "Start a handoff");
        assert_eq!(payload["note"], "handled");
    }

    #[tokio::test]
    async fn context_recommendation_stats_groups_actions() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-recommendation-stats-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, action) in [(0, "acknowledged"), (1, "applied")] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: "ContextRecommendationAction".to_string(),
                    event_json: serde_json::json!({
                        "type": "ContextRecommendationAction",
                        "session_id": session_id,
                        "envelope_id": format!("env-{sequence}"),
                        "recommendation": "Start a handoff",
                        "action": action,
                    })
                    .to_string(),
                    sequence: sequence as usize,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context/recommendations?limit=20"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 2);
        assert_eq!(
            json["recommendations"][0]["recommendation"],
            "Start a handoff"
        );
        assert_eq!(json["recommendations"][0]["count"], 2);
        assert_eq!(json["recommendations"][0]["actions"]["acknowledged"], 1);
        assert_eq!(json["recommendations"][0]["actions"]["applied"], 1);
        assert_eq!(json["recommendations"][0]["latest_envelope_id"], "env-1");
    }

    #[test]
    fn task_resume_context_packet_summarizes_current_task() {
        let path = std::env::temp_dir().join(format!(
            "cowd-api-task-packet-{}.json",
            uuid::Uuid::new_v4()
        ));
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("ship context runtime", true).unwrap();
        let phase_id = task.phases[0].id.clone();
        kernel
            .record_phase_artifact(
                &task.id,
                &phase_id,
                "evidence",
                "test",
                "cargo test -p runtime context_runtime",
            )
            .unwrap();
        let task = kernel.current().unwrap();

        let packet = message_routes::task_resume_context_packet("session-task", &task);

        assert_eq!(packet.session_id, "session-task");
        assert_eq!(packet.source, ResumeContextSource::ExecutionGraph);
        assert!(packet
            .active_task
            .as_deref()
            .is_some_and(|task| task.contains("ship context runtime")));
        assert!(packet
            .recent_decisions
            .iter()
            .any(|event| event.contains("artifact")));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn tools_returns_list() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn approval_routes_resolve_global_queue_request() {
        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        let app = api_router(state);
        let approval = runtime_services
            .approval_queue()
            .submit(runtime::SubmitGlobalApprovalRequest {
                source: runtime::ApprovalSource {
                    kind: runtime::ApprovalSourceKind::Session,
                    session_id: Some(format!("approval-route-{}", uuid::Uuid::new_v4())),
                    agent_id: None,
                    team_id: None,
                    mission_id: Some("mission-approval-route".to_string()),
                },
                action: "apply_patch".to_string(),
                summary: "modify runtime file".to_string(),
                risk: harness_contract::core::TaskRisk::High,
                evidence_refs: vec!["approval-route:test".to_string()],
                timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
            })
            .expect("approval submitted");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/pending")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let pending_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(pending_json["kind"], "gateway.unified_approval_pending");
        assert!(pending_json["pending"]
            .as_array()
            .expect("pending approvals")
            .iter()
            .any(|item| item["approval_id"].as_str() == Some(approval.approval_id.as_str())));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval.approval_id,
                            "approved": true,
                            "persistence": "once"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            runtime_services
                .approval_queue()
                .get(&approval.approval_id)
                .expect("approval exists")
                .status,
            runtime::GlobalApprovalStatus::Approved
        );
    }

    #[tokio::test]
    async fn approval_api_resumes_the_same_execution_graph_and_rejects_stale_decisions() {
        use harness_contract::execution_graph::{
            ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };

        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        runtime_services
            .executor_registry()
            .register(Arc::new(ApprovalResumeTestExecutor))
            .expect("test tool executor");

        let mut graph = ExecutionGraph::new("gateway approval resume");
        let approval = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            "approval",
            serde_json::json!({
                "action": "write",
                "summary": "approve graph continuation",
                "session_id": "approval-api-session"
            })
            .to_string(),
        );
        let tool = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "approval_resume_test_tool",
            "tool:after-approval",
        );
        graph.edges.push(ExecutionEdge {
            from: approval.id.clone(),
            to: tool.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes = vec![approval.clone(), tool.clone()];
        let graph_id = graph.id.clone();
        let report = runtime_services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph reaches approval wait");
        assert_eq!(report.waiting, 1);
        let waiting = runtime_services
            .graph_state_store()
            .load(&graph_id)
            .expect("waiting graph");
        assert_eq!(
            waiting.node_statuses[&approval.id],
            ExecutionNodeStatus::WaitingApproval
        );
        assert_eq!(
            waiting.node_statuses[&tool.id],
            ExecutionNodeStatus::Planned
        );
        assert!(matches!(
            runtime_services
                .graph_runner()
                .command(
                    &graph_id,
                    harness_contract::execution_graph::ExecutionGraphCommand::SubmitApproval {
                        expected_revision: waiting.revision.saturating_sub(1),
                        node_id: approval.id.clone(),
                        approved: true,
                        decision_ref: "stale-test-decision".to_string(),
                    },
                )
                .await,
            Err(runtime::execution_core::ExecutionRunnerError::Commit(
                runtime::execution_core::graph::ExecutionCommitError::StaleRevision { .. }
            ))
        ));

        let approval_id = format!("approval:{graph_id}:{}", approval.id);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval_id,
                            "approved": true,
                            "reason": "verified by operator"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["execution_graph"]["graph_id"], graph_id);
        assert_eq!(body["execution_graph"]["node_status"], "completed");

        let terminal = runtime_services
            .graph_state_store()
            .load(&graph_id)
            .expect("terminal graph");
        assert_eq!(
            terminal.node_statuses[&approval.id],
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            terminal.node_statuses[&tool.id],
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            terminal.node_results[&tool.id].result_ref.as_deref(),
            Some(format!("tool-result:{}", tool.id).as_str())
        );

        let duplicate = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"id": approval_id, "approved": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::OK);
        let duplicate_body: serde_json::Value =
            serde_json::from_slice(&to_bytes(duplicate.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(duplicate_body["status"], "already_applied");

        let conflicting = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"id": approval_id, "approved": false}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(conflicting.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cross_plane_waiting_approval_resumes_tool_and_reaches_terminal_graph() {
        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        let action = runtime::CrossPlaneAction::new("operator", "channel.send");
        let decision = runtime::CrossPlanePolicyDecision {
            decision: runtime::PolicyDecisionKind::RequireSingleApproval,
            reason: "operator approval required".to_string(),
            matched_grant: None,
            required_approval: Some(runtime::GrantType::SingleUse),
            degrade_to: None,
        };
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let projection = state
            .services
            .cross_plane
            .execute_commit_graph(
                &action,
                &decision,
                &format!("cross-plane-approval-{}", uuid::Uuid::new_v4()),
                Arc::new(CrossPlaneApprovalTestBackend {
                    calls: Arc::clone(&calls),
                }),
            )
            .await
            .expect("cross-plane graph reaches approval wait");
        let approval = projection
            .nodes
            .iter()
            .find(|node| {
                node.kind == harness_contract::execution_graph::ExecutionNodeKind::Approval
            })
            .expect("approval node");
        assert_eq!(
            approval.status,
            harness_contract::execution_graph::ExecutionNodeStatus::WaitingApproval
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);

        let approval_id = format!("approval:{}:{}", projection.graph_id, approval.node_id);
        let response = api_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"id": approval_id, "approved": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let terminal = runtime_services
            .graph_runner()
            .projection(&projection.graph_id)
            .await
            .expect("terminal cross-plane graph");
        let tool = terminal
            .nodes
            .iter()
            .find(|node| {
                node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            })
            .expect("tool node");
        assert_eq!(
            tool.status,
            harness_contract::execution_graph::ExecutionNodeStatus::Completed
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn approval_config_and_solo_routes_are_available_without_gate() {
        let app = api_router(test_state());
        let config_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(config_response.status(), StatusCode::OK);
        let body = to_bytes(config_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["solo_mode"], false);

        let solo_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/solo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(solo_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn platform_readiness_defaults_to_disabled_without_config() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/platforms")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let platforms = json.as_array().unwrap();
        assert!(platforms.iter().any(|item| item["name"] == "feishu"
            && item["status"] == "disabled"
            && item["credential_present"] == false));
        assert!(platforms.iter().any(|item| item["name"] == "wechat-ilink"
            && item["capabilities"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("message.ingress"))
            && item["capabilities"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("message.send.text"))));
    }

    #[tokio::test]
    async fn platform_readiness_reports_missing_fields_without_leaking_secrets() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [
                    {
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "cli_app_id",
                        "app_secret": ""
                    }
                ]
            }
        })));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/platforms/feishu")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["readiness"]["status"], "degraded");
        assert_eq!(json["readiness"]["credential_present"], false);
        assert!(json["readiness"]["missing_required"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("app_secret")));
        assert!(!json.to_string().contains("cli_app_id"));
    }

    #[tokio::test]
    async fn connector_routes_expose_contract_snapshot_with_local_service_account() {
        let workspace = unique_test_workspace("connector-empty");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let summary: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(summary["kind"], "connector_summary");
        assert_eq!(summary["summary"]["account_count"], 1);
        assert!(summary["summary"]["capability_count"].as_u64().unwrap() >= 8);
        assert_eq!(summary["summary"]["resource_count"], 0);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let capabilities: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(capabilities["kind"], "connector_capabilities");
        let list = capabilities["capabilities"].as_array().unwrap();
        assert!(list
            .iter()
            .any(|item| item["capability_id"] == "channel.feishu.send_text"));
        assert!(list
            .iter()
            .any(|item| item["capability_id"] == "governance.cross_plane.audit"));
        assert!(list
            .iter()
            .any(|item| item["capability_id"] == "service.local.docs.read"
                && item["plane"] == "service"));
        assert!(!list.iter().any(|item| item["capability_id"]
            .as_str()
            .unwrap_or_default()
            .contains("feishu_document_operation")));
    }

    #[tokio::test]
    async fn connector_accounts_project_configured_platform_health_without_secrets() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [
                    {
                        "name": "feishu-main",
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "cli_app_id",
                        "app_secret": ""
                    }
                ]
            }
        })));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/accounts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_accounts");
        assert_eq!(json["total"], 2);
        assert_eq!(json["accounts"][0]["provider"], "feishu");
        assert_eq!(json["accounts"][0]["account_id"], "feishu-main");
        assert_eq!(json["accounts"][0]["auth_mode"], "app_secret");
        assert_eq!(json["accounts"][0]["health"]["status"], "degraded");
        assert!(json["accounts"][0]["enabled_bindings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "channel.feishu.send_text"));
        assert!(!json.to_string().contains("cli_app_id"));
    }

    #[tokio::test]
    async fn connector_routes_project_configured_mcp_servers_into_runtime_contract() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "mcpServers": {
                "github.com": {
                    "type": "stdio",
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-github"],
                    "env": {
                        "GITHUB_TOKEN": "secret-token"
                    }
                },
                "broken": {
                    "type": "stdio"
                }
            }
        })));

        let accounts = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/accounts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accounts.status(), StatusCode::OK);
        let body = to_bytes(accounts.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_accounts");
        assert_eq!(json["total"], 3);
        assert!(json["accounts"].as_array().unwrap().iter().any(|account| {
            account["provider"] == "mcp"
                && account["account_id"] == "github.com"
                && account["auth_mode"] == "stdio"
                && account["health"]["status"] == "ready"
                && account["enabled_bindings"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item == "mcp.github_com.server")
        }));
        assert!(json["accounts"].as_array().unwrap().iter().any(|account| {
            account["provider"] == "mcp"
                && account["account_id"] == "broken"
                && account["health"]["status"] == "degraded"
        }));
        assert!(!json.to_string().contains("secret-token"));

        let capabilities = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(capabilities.status(), StatusCode::OK);
        let body = to_bytes(capabilities.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |capability| capability["capability_id"] == "mcp.github_com.server"
                    && capability["plane"] == "mcp"
                    && capability["supports_commit"] == false
            ));

        let mcp_servers = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/mcp/servers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mcp_servers.status(), StatusCode::OK);
        let body = to_bytes(mcp_servers.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_mcp_servers");
        assert_eq!(json["summary"]["total"], 2);
        assert_eq!(json["summary"]["ready"], 1);
        assert_eq!(json["summary"]["degraded"], 1);
        assert!(json["servers"].as_array().unwrap().iter().any(|server| {
            server["name"] == "broken"
                && server["status"] == "degraded"
                && server["missing_required"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item == "command")
        }));
        assert!(!json.to_string().contains("secret-token"));

        let mcp_probe = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/mcp/servers?probe=true&timeout_ms=75")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mcp_probe.status(), StatusCode::OK);
        let body = to_bytes(mcp_probe.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["probe"]["requested"], true);
        assert_eq!(json["probe"]["timeout_ms"], 75);
        assert!(json["servers"].as_array().unwrap().iter().any(|server| {
            server["name"] == "github.com"
                && server["probe"]["requested"] == true
                && server["probe"]["mode"] == "config_only"
                && server["probe"]["status"] == "declared"
        }));
        assert!(json["servers"].as_array().unwrap().iter().any(|server| {
            server["name"] == "broken"
                && server["probe"]["requested"] == true
                && server["probe"]["status"] == "degraded"
        }));
        assert!(!json.to_string().contains("secret-token"));
    }

    #[tokio::test]
    async fn local_docs_service_connector_executes_through_cross_plane_receipt() {
        let workspace = unique_test_workspace("connector-mock-docs");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let tools = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/services/local.docs/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tools.status(), StatusCode::OK);
        let body = to_bytes(tools.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_service_tools");
        assert_eq!(json["service"]["id"], "local.docs");
        assert!(json["tools"].as_array().unwrap().iter().any(|tool| {
            tool["capability_id"] == "service.local.docs.read" && tool["plane"] == "service"
        }));

        let key = format!("mock-docs-{}", uuid::Uuid::new_v4());
        let request = serde_json::json!({
            "actor_principal": format!("user:{key}"),
            "tool_id": "service.local.docs.read",
            "resource_id": "doc-1",
            "title": "Architecture",
            "mode": "dry_run",
            "idempotency_key": key
        });
        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(first_json["kind"], "connector_service_execution");
        assert_eq!(first_json["service"], "local.docs");
        assert_eq!(first_json["replayed"], false);
        assert_eq!(first_json["resource_persisted"], true);
        assert_eq!(
            first_json["result"]["resource"]["reference"],
            "service://local.docs/document/doc-1"
        );
        assert_eq!(
            first_json["receipt"]["action"]["requested_capability"],
            "service.local.docs.read"
        );
        assert_eq!(
            first_json["receipt"]["action"]["resource_ref"],
            "service://local.docs/document/doc-1"
        );
        let receipt_id = first_json["receipt"]["id"].as_str().unwrap().to_string();

        let resources = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resources.status(), StatusCode::OK);
        let body = to_bytes(resources.into_body(), usize::MAX).await.unwrap();
        let resources_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(resources_json["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |resource| resource["reference"] == "service://local.docs/document/doc-1"
                    && resource["title"] == "Architecture"
            ));

        let replay = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        let body = to_bytes(replay.into_body(), usize::MAX).await.unwrap();
        let replay_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(replay_json["replayed"], true);
        assert_eq!(replay_json["receipt"]["id"], receipt_id);
    }

    #[tokio::test]
    async fn connector_resources_survive_new_app_state_for_same_workspace() {
        let workspace = unique_test_workspace("connector-resources");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace.clone(),
        ));
        let request = serde_json::json!({
            "actor_principal": "user:resource-persistence",
            "tool_id": "service.local.docs.read",
            "resource_id": "persisted-doc",
            "title": "Persisted Runtime Resource",
            "mode": "dry_run",
            "idempotency_key": format!("persisted-doc-{}", uuid::Uuid::new_v4())
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let reopened = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let resources = reopened
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources?q=Persisted")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resources.status(), StatusCode::OK);
        let body = to_bytes(resources.into_body(), usize::MAX).await.unwrap();
        let resources_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(resources_json["status"], "available");
        assert!(resources_json["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|resource| resource["reference"]
                == "service://local.docs/document/persisted-doc"
                && resource["title"] == "Persisted Runtime Resource"));
    }

    #[tokio::test]
    async fn connector_resources_clamp_large_page_requests() {
        let workspace = unique_test_workspace("connector-resource-page-limit");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources?limit=999&offset=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resources");
        assert_eq!(json["limit"], 200);
    }

    #[tokio::test]
    async fn connector_resource_revalidation_route_updates_index_state() {
        let workspace = unique_test_workspace("connector-resource-revalidate");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let request = serde_json::json!({
            "actor_principal": "user:connector-resource-revalidate",
            "tool_id": "service.local.docs.read",
            "resource_id": "revalidate-doc",
            "title": "Revalidate Doc",
            "mode": "commit",
            "idempotency_key": format!("revalidate-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let revalidate = serde_json::json!({
            "reference": "service://local.docs/document/revalidate-doc",
            "state": "stale"
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/resources/revalidate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(revalidate.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resource_revalidation");
        assert_eq!(json["ok"], true);
        assert_eq!(json["resource"]["indexed_state"], "stale");

        let resources = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources?q=Revalidate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resources.status(), StatusCode::OK);
        let body = to_bytes(resources.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|resource| resource["indexed_state"] == "stale"));
    }

    #[tokio::test]
    async fn connector_resource_promote_memory_creates_metadata_only_memory() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-resource-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory_and_workspace(manager, tmp.clone()));
        let request = serde_json::json!({
            "actor_principal": "user:connector-resource-memory",
            "tool_id": "service.local.docs.read",
            "resource_id": "memory-doc",
            "title": "Memory Bridge Doc",
            "mode": "commit",
            "idempotency_key": format!("memory-bridge-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let promote = serde_json::json!({
            "reference": "service://local.docs/document/memory-doc",
            "session_id": "resource-memory-session"
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/resources/promote-memory")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(promote.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resource_memory_promotion");
        assert_eq!(json["ok"], true);
        assert_eq!(json["layer"], "L3");
        let first_memory_id = json["memory_id"].clone();

        let replay = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/resources/promote-memory")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(promote.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        let body = to_bytes(replay.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resource_memory_promotion");
        assert_eq!(json["ok"], true);
        assert_eq!(json["replayed"], true);
        assert_eq!(json["memory_id"], first_memory_id);

        let entries = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries.status(), StatusCode::OK);
        let body = to_bytes(entries.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entry = json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["title"] == "Connector resource: Memory Bridge Doc")
            .expect("promoted resource memory should exist");
        let content = entry["content"].as_str().unwrap_or_default();
        assert!(content.contains("service://local.docs/document/memory-doc"));
        assert!(content.contains("body_policy: metadata_only"));
        assert!(!content.contains("external document body"));
        let duplicate_count = json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| {
                entry["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("ref: service://local.docs/document/memory-doc")
            })
            .count();
        assert_eq!(duplicate_count, 1);
        std::fs::remove_dir_all(tmp).ok();
    }

    #[tokio::test]
    async fn cross_plane_policy_simulation_does_not_consume_single_use_grant() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:test-{suffix}");
        let capability = format!("service.feishu.drive.download.{suffix}");
        let grant_id = format!("grant-{suffix}");
        let grant = serde_json::json!({
            "id": grant_id,
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "actor_principal": principal,
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");

        let audit = app
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(audit.status(), StatusCode::OK);
        let audit_body = to_bytes(audit.into_body(), usize::MAX).await.unwrap();
        let audit_json: serde_json::Value = serde_json::from_slice(&audit_body).unwrap();
        let records = audit_json["records"].as_array().unwrap();
        assert!(
            records.iter().all(|record| {
                record["evidence"]["consumed_grant_id"].as_str() != Some(grant_id.as_str())
            }),
            "policy simulation must not consume single-use grants"
        );
    }

    #[tokio::test]
    async fn connector_service_commit_queues_graph_without_consuming_grant() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:service-commit-{suffix}");
        let capability = "service.local.docs.read";
        let grant_id = format!("grant-service-commit-{suffix}");
        let grant = serde_json::json!({
            "id": grant_id,
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "actor_principal": principal,
            "source_channel": "channel://wechat/chat/service-commit",
            "session_id": "service-commit-session",
            "tool_id": capability,
            "resource_id": format!("doc-{suffix}"),
            "title": "Service Commit",
            "mode": "commit",
            "idempotency_key": format!("idem-service-commit-{suffix}")
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result"]["status"], "executed");
        assert_eq!(json["receipt"]["dispatch_status"], "service_executed");
        assert!(json["execution_graph"]["graph_id"].as_str().is_some());
        assert_eq!(
            json["receipt"]["execution_graph_id"],
            json["execution_graph"]["graph_id"]
        );
        assert_eq!(json["receipt"]["audit_record_id"].as_str().is_some(), true);

        let audit = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let audit_body = to_bytes(audit.into_body(), usize::MAX).await.unwrap();
        let audit_json: serde_json::Value = serde_json::from_slice(&audit_body).unwrap();
        let planned = audit_json["records"]
            .as_array()
            .unwrap()
            .iter()
            .find(|record| record["action"]["actor_principal"] == principal)
            .expect("commit planning must be audited");
        assert!(planned["evidence"]["consumed_grant_id"].is_null());

        let action = serde_json::json!({
            "actor_principal": principal,
            "requested_capability": capability,
            "provider_account": "local.docs",
            "source_channel": "channel://wechat/chat/service-commit",
            "resource_ref": null,
            "target_ref": null,
            "session_id": "service-commit-session",
            "risk": "medium",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let second = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_identity_resolve_matches_cross_channel_contact_key() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let email = format!("demo-{suffix}@example.com");
        let principal = format!("user:demo-{suffix}");
        let identity = serde_json::json!({
            "id": format!("idb-{suffix}"),
            "principal_id": principal,
            "identity_ref": format!("channel://feishu/user/demo?email={email}"),
            "trust": "verified",
            "source": "test",
            "created_at": "2026-06-07T00:00:00Z",
            "expires_at": null
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/identities")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(identity.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let request = serde_json::json!({
            "identity_ref": format!("channel://wechat/user/demo?email={email}")
        });
        let resolved = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/identity/resolve")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resolved.status(), StatusCode::OK);
        let body = to_bytes(resolved.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "cross_plane_identity_resolution");
        assert_eq!(json["resolved"]["principal_id"], principal);
        assert_eq!(json["resolved"]["trust"], "verified");
        assert_eq!(json["resolved"]["match_kind"], "contact_key");
    }

    #[tokio::test]
    async fn cross_plane_policy_simulation_resolves_actor_identity_before_decision() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let email = format!("policy-{suffix}@example.com");
        let principal = format!("user:policy-{suffix}");
        let capability = format!("service.feishu.drive.download.{suffix}");

        let identity = serde_json::json!({
            "id": format!("idb-policy-{suffix}"),
            "principal_id": principal,
            "identity_ref": format!("channel://feishu/user/policy?email={email}"),
            "trust": "verified",
            "source": "test",
            "created_at": "2026-06-07T00:00:00Z",
            "expires_at": null
        });
        let grant = serde_json::json!({
            "id": format!("grant-policy-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });

        for (uri, body) in [
            ("/api/cross-plane/identities", identity),
            ("/api/cross-plane/grants", grant),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let action = serde_json::json!({
            "actor_principal": "",
            "actor_identity_ref": format!("channel://wechat/user/policy?email={email}"),
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "unknown"
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["decision"]["decision"], "allow");
        assert_eq!(json["action"]["actor_principal"], principal);
        assert_eq!(
            json["decision"]["matched_grant"]["principal_id"],
            json["action"]["actor_principal"]
        );
    }

    #[tokio::test]
    async fn cross_plane_preflight_combines_identity_policy_and_platform_without_consuming_grant() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let email = format!("preflight-{suffix}@example.com");
        let principal = format!("user:preflight-{suffix}");
        let capability = format!("service.feishu.drive.download.{suffix}");
        let identity = serde_json::json!({
            "id": format!("idb-preflight-{suffix}"),
            "principal_id": principal,
            "identity_ref": format!("channel://feishu/user/preflight?email={email}"),
            "trust": "verified",
            "source": "test",
            "created_at": "2026-06-07T00:00:00Z",
            "expires_at": null
        });
        let grant = serde_json::json!({
            "id": format!("grant-preflight-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });

        for (uri, body) in [
            ("/api/cross-plane/identities", identity),
            ("/api/cross-plane/grants", grant),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let action = serde_json::json!({
            "actor_principal": "",
            "actor_identity_ref": format!("channel://wechat/user/preflight?email={email}"),
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "unknown"
        });
        let preflight = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/preflight")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::OK);
        let preflight_body = to_bytes(preflight.into_body(), usize::MAX).await.unwrap();
        let preflight_json: serde_json::Value = serde_json::from_slice(&preflight_body).unwrap();
        assert_eq!(preflight_json["kind"], "cross_plane_action_preflight");
        assert_eq!(preflight_json["executable"], true);
        assert_eq!(preflight_json["target_platform"], "feishu");
        assert_eq!(preflight_json["platform_readiness"]["configured"], true);
        assert!(matches!(
            preflight_json["platform_readiness"]["status"].as_str(),
            Some("ready" | "configured")
        ));
        assert_eq!(preflight_json["decision"]["decision"], "allow");
        assert_eq!(preflight_json["action"]["actor_principal"], principal);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");

        let second = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_execute_dry_run_audits_without_consuming_grant() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:execute-dry-run-{suffix}");
        let capability = format!("channel.feishu.send_text.{suffix}");
        let grant_id = format!("grant-execute-dry-run-{suffix}");
        let grant = serde_json::json!({
            "id": grant_id,
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "actor_principal": principal,
            "actor_identity_ref": null,
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let execute = serde_json::json!({
            "mode": "dry_run",
            "idempotency_key": format!("idem-{suffix}"),
            "action": action
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "cross_plane_action_execution");
        assert_eq!(json["status"], "planned");
        assert_eq!(json["dispatch_status"], "dry_run");
        assert_eq!(json["executable"], false);
        assert_eq!(json["dispatched"], false);
        assert!(json["audit_record_id"]
            .as_str()
            .unwrap()
            .starts_with("cpa-"));

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json["action"].to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json["action"].to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_execute_replays_idempotency_key_without_duplicate_audit() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:execute-idempotent-{suffix}");
        let capability = format!("channel.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-execute-idempotent-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "dry_run",
            "idempotency_key": format!("idem-{suffix}"),
            "action": {
                "actor_principal": principal,
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/test",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": null,
                "resource_ref": null,
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["idempotent_replay"], false);

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["idempotent_replay"], true);
        assert_eq!(
            second_json["execution_receipt"]["id"],
            first_json["execution_receipt"]["id"]
        );
        assert_eq!(
            second_json["audit_record_id"],
            first_json["audit_record_id"]
        );

        let executions = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/action/executions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let executions_body = to_bytes(executions.into_body(), usize::MAX).await.unwrap();
        let executions_json: serde_json::Value = serde_json::from_slice(&executions_body).unwrap();
        let matching = executions_json["executions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|receipt| receipt["id"] == first_json["execution_receipt"]["id"])
            .count();
        assert_eq!(matching, 1);
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_blocks_without_live_adapter_and_preserves_grant() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:execute-commit-{suffix}");
        let capability = format!("channel.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-execute-commit-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "actor_principal": principal,
            "actor_identity_ref": null,
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let execute = serde_json::json!({
            "mode": "commit",
            "action": action
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "blocked");
        assert_eq!(json["dispatch_status"], "execution_graph_rejected");
        assert_eq!(json["executable"], false);
        assert_eq!(json["adapter_capability"]["live_supported"], true);
        assert_eq!(json["adapter_capability"]["adapter_bound"], false);
        assert!(json["blockers"].as_array().unwrap().iter().any(|value| {
            value
                .as_str()
                .unwrap_or_default()
                .starts_with("execution_graph:")
        }));
        assert!(json["execution_graph"].is_null());

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json["action"].to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_adapter_registry_reports_supported_and_unsupported_live_operations() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [
                    {
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    },
                    {
                        "platformType": "wecom",
                        "enabled": true,
                        "corp_id": "corp",
                        "corp_secret": "secret",
                        "agent_id": "agent"
                    }
                ]
            }
        })));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/action/adapters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "cross_plane_action_adapters");
        let capabilities = json["capabilities"].as_array().unwrap();
        assert!(capabilities.iter().any(|item| {
            item["platform"] == "feishu"
                && item["operation"] == "send_text"
                && item["live_supported"] == true
                && item["adapter_bound"] == false
        }));
        assert!(!capabilities
            .iter()
            .any(|item| item["platform"] == "wecom" && item["operation"] == "callback"));
    }

    #[tokio::test]
    async fn cross_plane_preflight_builds_surface_dispatch_target_plan() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:dispatch-target-{suffix}");
        let capability = format!("channel.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-target-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "actor_principal": principal,
            "actor_identity_ref": null,
            "source_channel": "channel://wechat/chat/source",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": "channel://feishu/user/open-id-1/thread/chat-id-1",
            "resource_ref": "text://hello from cross plane",
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let preflight = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/preflight")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::OK);
        let body = to_bytes(preflight.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["dispatch_target"]["ready"], true);
        assert_eq!(json["dispatch_target"]["platform"], "feishu");
        assert_eq!(json["dispatch_target"]["operation"], "send_text");
        assert_eq!(
            json["dispatch_target"]["session_key"],
            "feishu:open-id-1:chat-id-1"
        );
        assert_eq!(
            json["dispatch_target"]["outbound_message"]["text"],
            "hello from cross plane"
        );
        assert_eq!(
            json["dispatch_target"]["outbound_message"]["metadata"]["requested_capability"],
            capability
        );
    }

    #[tokio::test]
    async fn cross_plane_execute_persists_surface_dispatch_target_snapshot() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:dispatch-receipt-{suffix}");
        let capability = format!("channel.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-receipt-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "dry_run",
            "idempotency_key": format!("idem-dispatch-receipt-{suffix}"),
            "action": {
                "actor_principal": principal,
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/demo-chat",
                "resource_ref": "text://receipt payload",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["ready"],
            true
        );
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["session_key"],
            "feishu:demo-chat"
        );

        let executions = app
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/action/executions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let executions_body = to_bytes(executions.into_body(), usize::MAX).await.unwrap();
        let executions_json: serde_json::Value = serde_json::from_slice(&executions_body).unwrap();
        assert!(executions_json["executions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|receipt| receipt["dispatch_target"]["session_key"] == "feishu:demo-chat"));
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_reports_surface_unavailable_without_sidecar() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:dispatch-live-{suffix}");
        let capability = format!("channel.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-live-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-live-{suffix}"),
            "action": {
                "actor_principal": principal,
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": "text://live payload",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "dispatch_failed");
        assert_eq!(executed_json["dispatched"], false);
        assert!(executed_json["blockers"].as_array().unwrap().is_empty());
        assert!(executed_json["execution_graph"]["graph_id"]
            .as_str()
            .is_some());
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_resolves_image_target_but_requires_surface_sidecar() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:dispatch-image-{suffix}");
        let capability = format!("channel.feishu.send_image.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-image-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-image-{suffix}"),
            "action": {
                "actor_principal": principal,
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": "image://https://example.test/panel.png",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "dispatch_failed");
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_kind"],
            "image"
        );
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_resolves_workspace_file_target_but_requires_surface_sidecar(
    ) {
        let root = test_temp_dir("cross-plane-file-dispatch");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(workspace.join("reports")).unwrap();
        let report_path = workspace.join("reports").join("panel.txt");
        std::fs::write(&report_path, "dispatchable report").unwrap();
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
            workspace.clone(),
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:dispatch-file-{suffix}");
        let capability = format!("channel.feishu.send_file.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-file-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-file-{suffix}"),
            "action": {
                "actor_principal": principal,
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": "file://reports/panel.txt",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "dispatch_failed");
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_kind"],
            "file"
        );
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_ref"],
            "reports/panel.txt"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_blocks_file_outside_workspace() {
        let root = test_temp_dir("cross-plane-file-block");
        let workspace = root.join("workspace");
        let outside = root.join("outside.txt");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(&outside, "must not send").unwrap();
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
            workspace,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:dispatch-file-block-{suffix}");
        let capability = format!("channel.feishu.send_file.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-file-block-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-file-block-{suffix}"),
            "action": {
                "actor_principal": principal,
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": format!("file://{}", outside.display()),
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "not_started");
        assert!(executed_json["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| blocker
                .as_str()
                .unwrap_or_default()
                .contains("payload_blocked")));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn context_current_returns_degraded_envelope_without_memory() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=ship&session_id=session-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["envelope"]["identity"]["session_id"], "session-1");
        assert_eq!(json["envelope"]["intent"], "ship");
        assert_eq!(
            json["envelope"]["assembled"]["stable_head"][0],
            "cowd-context-runtime:v0.8.13"
        );
        assert_eq!(
            json["envelope"]["diagnostics"]["degraded_sources"][0],
            "Memory"
        );
        assert_eq!(json["lean_probe"]["envelope_id"], json["envelope"]["id"]);
        assert_eq!(json["lean_probe"]["pressure_level"], "Nominal");
        assert_eq!(json["lean_probe"]["degradation_path"], "SourceFallback");
        assert_eq!(json["policy_decision"]["action"], "PreferOrientationPacket");
        assert_eq!(
            json["policy_decision"]["stable_head_hash"],
            json["lean_probe"]["stable_head_hash"]
        );
        assert_eq!(json["cache_stability"]["stable_head_reusable"], true);
        assert_eq!(
            json["snapshot"]["stable_head_hash"],
            json["lean_probe"]["stable_head_hash"]
        );
        assert_eq!(
            json["budget_explanation"]["total_tokens"],
            json["envelope"]["budget"]["total_tokens"]
        );
        assert_eq!(json["mode_coverage"]["all_profiles_covered"], true);
        assert_eq!(json["mode_coverage"]["all_stable_heads_reusable"], true);
        assert_eq!(
            json["mode_coverage"]["entries"].as_array().unwrap().len(),
            11
        );
        let profiles = json["mode_coverage"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|entry| entry["profile"].as_str())
            .collect::<Vec<_>>();
        assert!(profiles.contains(&"SurfaceQuickReply"));
        assert!(profiles.contains(&"SurfaceTaskIntake"));
        assert!(profiles.contains(&"DeepInvestigation"));
    }

    #[tokio::test]
    async fn context_current_accepts_profile_query_for_synthetic_envelope() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=ship&session_id=session-1&profile=yolo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["envelope"]["profile"], "YoloGoal");
        assert_eq!(json["envelope"]["identity"]["mode"], "YoloGoal");
        assert_eq!(json["envelope"]["budget"]["leases"][0]["source"], "Task");
        assert!(json["envelope"]["assembled"]["runtime_header"][0]
            .as_str()
            .unwrap()
            .contains("profile:YoloGoal"));
        assert!(json["envelope"]["assembled"]["runtime_header"][0]
            .as_str()
            .unwrap()
            .contains("mode:YoloGoal"));
        assert!(json["mode_coverage"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["profile"] == "SubAgent" && entry["mode"] == "SubAgent"));
    }

    #[tokio::test]
    async fn context_current_can_project_agent_view() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=review&session_id=session-1&agent_id=reviewer&agent_task=review%20the%20plan")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["agent_view"]["child_agent_id"], "reviewer");
        assert_eq!(json["agent_view"]["parent_agent_id"], "primary");
        assert_eq!(json["agent_view"]["envelope"]["profile"], "SubAgent");
        assert_eq!(
            json["agent_view"]["envelope"]["diagnostics"]["stable_head_hash"],
            json["envelope"]["diagnostics"]["stable_head_hash"]
        );
    }

    #[tokio::test]
    async fn context_current_injects_connector_resource_refs_without_resource_body() {
        let workspace = unique_test_workspace("context-resource");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let request = serde_json::json!({
            "actor_principal": "user:context-resource",
            "tool_id": "service.local.docs.read",
            "resource_id": "context-doc",
            "title": "Context Resource Plan",
            "mode": "dry_run",
            "idempotency_key": format!("context-resource-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=Context&session_id=session-resource")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let selected = json["envelope"]["selected"].as_array().unwrap();
        let resource_item = selected
            .iter()
            .find(|item| item["id"] == "service://local.docs/document/context-doc")
            .expect("resource context item should be selected");
        assert_eq!(resource_item["source"], "Workspace");
        assert_eq!(resource_item["role"], "Evidence");
        assert!(resource_item["content"]
            .as_str()
            .unwrap()
            .contains("indexed_state: unknown"));
        assert!(!resource_item["content"]
            .as_str()
            .unwrap()
            .contains("Mock document"));
        assert_eq!(
            resource_item["evidence"][0],
            "service://local.docs/document/context-doc"
        );
    }

    #[tokio::test]
    async fn evidence_resolver_returns_connector_resource_metadata_only() {
        let workspace = unique_test_workspace("resource-evidence");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let request = serde_json::json!({
            "actor_principal": "user:resource-evidence",
            "tool_id": "service.local.docs.read",
            "resource_id": "evidence-doc",
            "title": "Evidence Resource",
            "mode": "dry_run",
            "idempotency_key": format!("resource-evidence-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/evidence/resolve?ref=service%3A%2F%2Flocal.docs%2Fdocument%2Fevidence-doc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "resource");
        assert_eq!(json["available"], true);
        assert_eq!(json["resource"]["title"], "Evidence Resource");
        assert_eq!(json["body"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn session_runs_route_reads_runtime_run_events_only() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-runs-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, event_type, payload) in [
            (
                0,
                "TextDelta",
                serde_json::json!({"type":"TextDelta","content":"skip"}),
            ),
            (
                1,
                "RuntimeRun",
                message_routes::runtime_run_started_payload(
                    session_id,
                    "run-1",
                    ContextProfile::MainTurn,
                    "ship",
                    10,
                ),
            ),
            (
                2,
                "RuntimeRun",
                message_routes::runtime_run_completed_payload(
                    session_id,
                    "run-1",
                    None,
                    ContextProfile::MainTurn,
                    "completed",
                    Some(2),
                    Some("ctx-1".to_string()),
                    None,
                    10,
                    25,
                ),
            ),
            (
                3,
                "RuntimeRun",
                serde_json::json!({
                    "type": "RuntimeRun",
                    "phase": "completed",
                    "run_id": "agent-run-1",
                    "parent_run_id": "run-1",
                    "session_id": session_id,
                    "kind": "agent_task",
                    "profile": ContextProfile::SubAgent,
                    "status": "failed",
                    "started_at_ms": 11,
                    "completed_at_ms": 20,
                    "duration_ms": 9,
                    "error": "review failed",
                    "refs": [],
                }),
            ),
        ] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: event_type.to_string(),
                    event_json: payload.to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/runs?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 3);
        assert_eq!(json["runs"].as_array().unwrap().len(), 3);
        assert_eq!(json["runs"][0]["run"]["phase"], "started");
        assert_eq!(json["runs"][0]["run"]["kind"], "main_turn");
        assert_eq!(json["runs"][0]["run"]["started_at_ms"], 10);
        assert_eq!(json["runs"][1]["run"]["status"], "completed");
        assert_eq!(json["runs"][1]["run"]["context_envelope_id"], "ctx-1");
        assert_eq!(json["runs"][1]["run"]["duration_ms"], 15);
        assert_eq!(
            json["runs"][1]["run"]["refs"][0]["type"],
            "context_envelope"
        );
        assert_eq!(json["tree"]["roots"][0], "run-1");
        assert_eq!(json["tree"]["children"]["run-1"][0], "agent-run-1");
        assert_eq!(json["tree"]["summary"]["span_count"], 2);
        assert_eq!(json["tree"]["summary"]["failed_count"], 1);
    }

    #[tokio::test]
    async fn session_runtime_run_context_reference_resolves_envelope_detail() {
        let dir = test_temp_dir("runtime-context-link");
        let db_path = dir.join("sessions.sqlite");
        let store = Arc::new(UnifiedSessionStore::open(&db_path).unwrap());
        let session_id = "runtime-context-link-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(
                    session_id,
                    "ctx-linked-runtime",
                    "linked runtime context",
                )
                .to_string(),
                sequence: 10,
                created_at_ms: 10,
            })
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "RuntimeRun".to_string(),
                event_json: message_routes::runtime_run_completed_payload(
                    session_id,
                    "run-linked",
                    None,
                    ContextProfile::MainTurn,
                    "completed",
                    Some(1),
                    Some("ctx-linked-runtime".to_string()),
                    None,
                    20,
                    40,
                )
                .to_string(),
                sequence: 20,
                created_at_ms: 20,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let runs_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/runs?limit=5"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(runs_response.status(), StatusCode::OK);
        let body = to_bytes(runs_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let runs_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(runs_json["runs"].as_array().unwrap().len(), 1);
        assert_eq!(
            runs_json["runs"][0]["run"]["context_envelope_id"],
            "ctx-linked-runtime"
        );
        assert_eq!(
            runs_json["runs"][0]["run"]["refs"][0]["type"],
            "context_envelope"
        );
        assert_eq!(runs_json["runs"][0]["run"]["duration_ms"], 20);

        let detail_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/ctx-linked-runtime")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail_body = to_bytes(detail_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail_json: serde_json::Value = serde_json::from_slice(&detail_body).unwrap();
        assert_eq!(detail_json["context"]["session_id"], session_id);
        assert_eq!(detail_json["context"]["sequence"], 10);
        assert_eq!(
            detail_json["context"]["envelope"]["intent"],
            "linked runtime context"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn runtime_timeline_preserves_runtime_run_context_refs() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-context-ref-timeline";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({
                    "type": "message_appended",
                    "sequence": 0,
                    "role": "user"
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 10,
            })
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(
                    session_id,
                    "ctx-runtime-timeline",
                    "timeline linked context",
                )
                .to_string(),
                sequence: 1,
                created_at_ms: 11,
            })
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "RuntimeRun".to_string(),
                event_json: message_routes::runtime_run_completed_payload(
                    session_id,
                    "run-runtime-timeline",
                    None,
                    ContextProfile::MainTurn,
                    "completed",
                    Some(1),
                    Some("ctx-runtime-timeline".to_string()),
                    None,
                    20,
                    30,
                )
                .to_string(),
                sequence: 2,
                created_at_ms: 12,
            })
            .await
            .unwrap();
        store
            .append_session_domain_event(&memory::SessionDomainEvent::new(
                session_id,
                3,
                memory::SessionDomainScope::Policy,
                "runtime.policy.decided",
                serde_json::json!({
                    "agent_mode": "Solo",
                    "requires_review": false,
                    "complexity": {"level": "Simple", "score": 30}
                }),
                13,
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let timeline_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(timeline_response.status(), StatusCode::OK);
        let timeline_body = to_bytes(timeline_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let timeline: serde_json::Value = serde_json::from_slice(&timeline_body).unwrap();
        assert_eq!(timeline["total"], 4);
        assert_eq!(timeline["events"][0]["kind"], "message_appended");
        assert_eq!(timeline["events"][1]["kind"], "ContextEnvelope");
        let runtime_run = timeline["events"]
            .as_array()
            .expect("timeline events")
            .iter()
            .find(|event| event["kind"] == "RuntimeRun")
            .expect("runtime run projection");
        assert_eq!(runtime_run["status"], "completed");
        assert_eq!(runtime_run["refs"][0]["type"], "context_envelope");
        assert_eq!(runtime_run["refs"][0]["id"], "ctx-runtime-timeline");
        assert_eq!(
            timeline["health_summary"]["latest_policy"]["agent_mode"],
            "Solo"
        );
        assert_eq!(timeline["health_summary"]["scope_counts"]["turn"], 1);
        assert_eq!(timeline["health_summary"]["scope_counts"]["context"], 1);
        assert_eq!(timeline["health_summary"]["scope_counts"]["message"], 1);
        assert_eq!(timeline["health_summary"]["scope_counts"]["policy"], 1);

        let detail_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/ctx-runtime-timeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail_body = to_bytes(detail_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail_json: serde_json::Value = serde_json::from_slice(&detail_body).unwrap();
        assert_eq!(
            detail_json["context"]["envelope"]["intent"],
            "timeline linked context"
        );
    }

    #[tokio::test]
    async fn runtime_run_large_page_query_is_bounded() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-runs-large-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();

        for sequence in 0..120u64 {
            let run_id = format!("run-{sequence}");
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: "RuntimeRun".to_string(),
                    event_json: message_routes::runtime_run_completed_payload(
                        session_id,
                        &run_id,
                        None,
                        ContextProfile::MainTurn,
                        "completed",
                        Some(1),
                        None,
                        None,
                        sequence.saturating_mul(10),
                        sequence.saturating_mul(10).saturating_add(3),
                    )
                    .to_string(),
                    sequence: sequence as usize,
                    created_at_ms: sequence,
                })
                .await
                .unwrap();
        }

        let app = api_router(test_state_with_store(store));
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/runs?limit=25"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["total"], 120);
        assert_eq!(json["runs"].as_array().unwrap().len(), 25);
        assert_eq!(json["tree"]["summary"]["span_count"], 25);
        assert_eq!(json["tree"]["summary"]["root_count"], 25);
    }

    #[tokio::test]
    async fn evidence_resolver_reads_tool_events_by_ref() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "evidence-tool-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ToolComplete".to_string(),
                event_json: serde_json::json!({
                    "type": "ToolComplete",
                    "id": "tool-1",
                    "name": "bash",
                    "summary": "tests passed",
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 1,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/evidence/resolve?session_id={session_id}&ref=tool%3A%2F%2Ftool-1%2Fevidence%2Fevent-1"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["available"], true);
        assert_eq!(json["kind"], "tool");
        assert_eq!(json["events"][0]["payload"]["summary"], "tests passed");
    }

    #[tokio::test]
    async fn evidence_resolver_rejects_unsupported_refs() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/evidence/resolve?ref=unknown%3A%2F%2Fvalue")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn task_api_starts_reports_and_blocks_after_repeated_failures() {
        let app = api_router(test_state());
        let start_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "finish v0.8.10",
                            "yolo_mode": true,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(start_response.status(), StatusCode::CREATED);
        let start_body = to_bytes(start_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let started: serde_json::Value = serde_json::from_slice(&start_body).unwrap();
        let task_id = started["id"].as_str().expect("task id").to_string();
        assert_eq!(started["status"], "running");
        assert_eq!(started["yolo_mode"], true);

        for reason in ["first", "second", "external input required"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/tasks/{task_id}/failure"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({ "reason": reason }).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let status_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tasks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["tasks"][0]["status"], "blocked");
        assert_eq!(
            status_json["tasks"][0]["blocker_reason"],
            "external input required"
        );
    }

    #[tokio::test]
    async fn task_api_records_phase_artifacts_and_review() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let app = api_router(test_state_with_store(store));
        let start_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "ship task phase",
                            "yolo_mode": true,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(start_response.status(), StatusCode::CREATED);
        let start_body = to_bytes(start_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let started: serde_json::Value = serde_json::from_slice(&start_body).unwrap();
        let task_id = started["id"].as_str().unwrap().to_string();

        let phase_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "name": "browser-e2e",
                            "objective": "cover WebUI task panel",
                            "plan": ["add playwright spec"],
                            "acceptance": ["2 e2e tests pass"],
                            "test_commands": ["cargo test -p gateway task_kernel -- --nocapture"],
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(phase_response.status(), StatusCode::CREATED);
        let phase_body = to_bytes(phase_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let phase_json: serde_json::Value = serde_json::from_slice(&phase_body).unwrap();
        let phase_id = phase_json["phases"].as_array().unwrap().last().unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(phase_json["current_phase"], "browser-e2e");

        let artifact_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases/{phase_id}/artifacts"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "kind": "test",
                            "label": "playwright",
                            "value": "2 passed",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(artifact_response.status(), StatusCode::OK);

        let review_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases/{phase_id}/review"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "result": "accepted",
                            "completed": true,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(review_response.status(), StatusCode::OK);
        let review_body = to_bytes(review_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let review_json: serde_json::Value = serde_json::from_slice(&review_body).unwrap();
        let reviewed_phase = review_json["phases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|phase| phase["id"] == phase_id)
            .unwrap();
        assert_eq!(reviewed_phase["status"], "completed");
        assert_eq!(reviewed_phase["review_result"], "accepted");
        assert_eq!(reviewed_phase["artifacts"][0]["label"], "playwright");

        let timeline_response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={task_id}&from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(timeline_response.status(), StatusCode::OK);
        let timeline_body = to_bytes(timeline_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let timeline_json: serde_json::Value = serde_json::from_slice(&timeline_body).unwrap();
        let kinds = timeline_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["kind"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "task.started",
                "task.phase.started",
                "task.phase.artifact.recorded",
                "task.phase.reviewed",
            ]
        );
        assert_eq!(timeline_json["events"][0]["scope"], "task");
        assert_eq!(timeline_json["events"][3]["payload"]["status"], "reviewing");
    }

    #[tokio::test]
    async fn memory_without_config_returns_disabled() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["status"], "disabled");
        assert_eq!(json["context_health"]["level"], "unavailable");
        assert_eq!(json["kernel_health"]["degraded"], true);
        assert_eq!(
            json["kernel_health"]["degraded_reasons"][0],
            "memory not configured"
        );
    }

    #[tokio::test]
    async fn memory_maintenance_without_config_degrades() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["degraded_reason"], "memory not configured");
        assert!(json["candidates"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_maintenance_scan_and_transition() {
        let dir =
            std::env::temp_dir().join(format!("cowd-api-maintenance-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        let id = MemoryId::new_v4();
        manager
            .remember(MemoryEntry {
                id,
                layer: MemoryLayer::L2,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::UserExplicit,
                title: "Old context rule".to_string(),
                content: "Prefer bounded context packets".to_string(),
                embedding: None,
                tags: vec![],
                relations: vec![],
                confidence: 0.7,
                access_count: 0,
                staleness: 0.95,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::Session("maintenance-test".to_string()),
                session_id: None,
                source_agent: None,
                visibility: AgentVisibility::Shared,
            })
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let scan_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/maintenance")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"stale_threshold":0.9}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scan_response.status(), StatusCode::OK);
        let scan_body = to_bytes(scan_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let scan_json: serde_json::Value = serde_json::from_slice(&scan_body).unwrap();
        let candidate_id = scan_json["candidates"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(scan_json["candidates"][0]["kind"], "stale");

        let ack_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/memory/maintenance/{candidate_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"status":"acknowledged"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack_response.status(), StatusCode::OK);
        let ack_body = to_bytes(ack_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let ack_json: serde_json::Value = serde_json::from_slice(&ack_body).unwrap();
        assert_eq!(ack_json["candidate"]["status"], "acknowledged");
    }

    #[tokio::test]
    async fn memory_maintenance_rejects_invalid_status_filter() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance?status=unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["degraded_reason"], "memory not configured");

        let dir = std::env::temp_dir().join(format!(
            "cowd-api-maintenance-invalid-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance?status=unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn memory_recall_explain_reports_source_mode_and_score() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::ProjectKnowledge,
                "SessionKernel migration",
                "SessionKernel owns durable sessions and task phase review evidence.",
                Priority::High,
                vec!["session".into(), "task".into()],
                MemoryScope::Project("api-test".to_string()),
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/recall/explain?q=SessionKernel&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["query"], "SessionKernel");
        assert_eq!(json["degraded"], false);
        assert_eq!(json["results"][0]["source_layer"], "L3");
        assert_eq!(json["results"][0]["category"], "ProjectKnowledge");
        assert!(json["results"][0]["score"].as_f64().is_some());
        assert!(json["results"][0]["mode"].as_str().is_some());
        assert!(json["results"][0]["snippet"]
            .as_str()
            .unwrap_or_default()
            .contains("SessionKernel"));

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_packet_returns_explainable_packet() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-packet-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let entry = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L2,
            category: MemoryCategory::ProjectKnowledge,
            priority: Priority::High,
            source: MemorySource::UserExplicit,
            title: "PACKET_API_ALPHA".to_string(),
            content: "PACKET_API_ALPHA should appear in an explainable packet.".to_string(),
            embedding: None,
            tags: vec!["packet".to_string()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Session("api-memory-packet".to_string()),
            session_id: Some("api-memory-packet".to_string()),
            source_agent: Some("api".to_string()),
            visibility: AgentVisibility::Shared,
        };
        manager.remember(entry).await.unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/packet?q=PACKET_API_ALPHA&max_items=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert!(json["packet"]["selected"].as_array().unwrap().len() <= 5);

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_links_returns_kernel_links() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-links-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let target_id = manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::Reference,
                "Link Target",
                "target",
                Priority::Normal,
                vec!["api-link".to_string()],
                MemoryScope::Global,
            )
            .await
            .unwrap();
        let source = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::UserExplicit,
            title: "Link Source".to_string(),
            content: "source".to_string(),
            embedding: None,
            tags: vec![],
            relations: vec![memory::Relation {
                target_id,
                kind: memory::RelationKind::DependsOn,
                strength: 0.8,
                temporal: None,
                entity: None,
            }],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::Shared,
        };
        manager.remember(source).await.unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/links")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["total"].as_u64().unwrap() >= 1);

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_layers_and_entries_read_real_store() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                MemoryLayer::L4,
                MemoryCategory::Shared,
                "Team Decision",
                "Use SessionKernel as the source of truth for v0.8.10.",
                Priority::High,
                vec!["team_relevant".into()],
                MemoryScope::Global,
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let status_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["enabled"], true);
        assert_eq!(status_json["status"], "ready");
        assert_eq!(status_json["context_health"]["level"], "healthy");
        assert_eq!(status_json["kernel_health"]["degraded"], false);
        assert_eq!(status_json["kernel_health"]["stale_pressure"], 0.0);
        assert!(status_json["kernel_health"]["evidence_coverage"]
            .as_f64()
            .is_some());

        let layers_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/layers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(layers_response.status(), StatusCode::OK);
        let layers_body = to_bytes(layers_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let layers_json: serde_json::Value = serde_json::from_slice(&layers_body).unwrap();
        let l4_count = layers_json["layers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|layer| layer["layer"] == "L4")
            .and_then(|layer| layer["entry_count"].as_u64())
            .unwrap_or_default();
        assert_eq!(l4_count, 1);

        let entries_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries_response.status(), StatusCode::OK);
        let entries_body = to_bytes(entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let entries_json: serde_json::Value = serde_json::from_slice(&entries_body).unwrap();
        assert_eq!(entries_json["entries"].as_array().unwrap().len(), 1);
        assert_eq!(entries_json["entries"][0]["title"], "Team Decision");
    }

    #[tokio::test]
    async fn memory_entry_update_route_updates_real_store() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-update-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/L3")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "Update target",
                            "content": "original memory content",
                            "category": "Reference",
                            "priority": "Normal",
                            "tags": ["before"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let create_body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let create_json: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
        let id = create_json["id"].as_str().unwrap();

        let update_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/memory/entry/{id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "content": "updated memory content",
                            "priority": "High",
                            "tags": ["after", "webui"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(update_response.status(), StatusCode::OK);

        let entries_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries_response.status(), StatusCode::OK);
        let entries_body = to_bytes(entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let entries_json: serde_json::Value = serde_json::from_slice(&entries_body).unwrap();
        let entry = entries_json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == id)
            .unwrap();
        assert_eq!(entry["content"], "updated memory content");
        assert_eq!(entry["priority"], "High");
        assert_eq!(entry["tags"][0], "after");
    }

    #[tokio::test]
    async fn audit_export_includes_memory_write_audit() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-audit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/L3")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "audit-export-memory",
                            "content": "COWD_AUDIT_EXPORT_MEMORY_WRITE",
                            "category": "Reference",
                            "priority": "High",
                            "tags": ["audit", "e2e"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        let export_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/audit/export?source=memory&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(export_response.status(), StatusCode::OK);
        let body = to_bytes(export_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "audit_export");
        assert_eq!(json["source"], "memory");
        assert_eq!(json["totals"]["memory"], 1);
        assert_eq!(json["records"][0]["source"], "memory");
        assert_eq!(
            json["records"][0]["record"]["summary"],
            "COWD_AUDIT_EXPORT_MEMORY_WRITE"
        );
        assert_eq!(json["memory"][0]["operation"], "Create");
        assert_eq!(json["memory"][0]["layer"], "L3");

        let _ = std::fs::remove_dir_all(tmp);
    }

    #[tokio::test]
    async fn memory_symbol_links_roundtrip_real_store() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-symbol-links-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let memory_id = manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::Reference,
                "Auth impact note",
                "authenticate_user controls login policy and API auth behavior.",
                Priority::High,
                vec!["symbol".into(), "auth".into()],
                MemoryScope::Global,
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let link_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/symbol-links")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "symbol_id": "src/auth.rs:authenticate_user:42",
                            "memory_id": memory_id.to_string(),
                            "turn_index": 7,
                            "reference_type": "impact"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(link_response.status(), StatusCode::CREATED);

        let lookup_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/symbol-links?symbol=authenticate_user")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lookup_response.status(), StatusCode::OK);
        let body = to_bytes(lookup_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 1);
        assert_eq!(json["entries"][0]["id"], memory_id.to_string());
        assert_eq!(json["entries"][0]["title"], "Auth impact note");
    }

    #[tokio::test]
    async fn config_returns_version() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verify_auth_allows_no_auth_configuration() {
        let state = test_state();
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/verify")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_required_when_token_set() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn system_routes_stay_protected_when_auth_token_set() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn webui_same_origin_requests_bypass_browser_token_handling() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .header("sec-fetch-site", "same-origin")
                    .header("sec-fetch-dest", "empty")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cross_site_requests_still_require_bearer_auth() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .header("sec-fetch-site", "cross-site")
                    .header("sec-fetch-dest", "empty")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn profile_and_workspace_routes_stay_protected_when_auth_token_set() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        });
        let app = api_router(state);

        for uri in [
            "/api/profiles",
            "/api/workspace",
            "/api/approval/pending",
            "/api/cross-plane/summary",
            "/api/message-connectors/wechat-ilink/accounts",
            "/api/memory/status",
            "/api/tasks",
            "/api/runtime/control-plane",
            "/api/context/current",
            "/api/evidence/resolve?ref=session%3A%2F%2Ftest",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn auth_passes_with_valid_token() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: None,
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .header("Authorization", "Bearer test-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
