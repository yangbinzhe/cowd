// ── API Routes ─────────────────────────────────────────────────
// Core gateway routes shared between TUI and HTTP API.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::State as AxumState,
    http::{header, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json},
    Router,
};
#[cfg(test)]
use channel_adapters::platform::PlatformRuntime;
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
use tools::GlobalToolRegistry;

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
mod channel_routes;
pub(crate) mod connector_routes;
mod context_routes;
mod core_routes;
mod cross_plane_routes;
mod growth_routes;
mod matrix_outcomes;
mod matrix_routes;
pub(crate) mod memory_routes;
mod message_routes;
mod mfg_outcomes;
mod mfg_routes;
mod profile_routes;
mod public_routes;
mod runtime_routes;
mod session_routes;
mod skill_routes;
mod slash_routes;
mod system_routes;
mod task_routes;
mod workspace_routes;

// ── Shared application state ───────────────────────────────────

pub struct AppState {
    pub tool_registry: Arc<GlobalToolRegistry>,
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

type RuntimeEntry = Arc<tokio::sync::Mutex<crate::BuiltRuntime>>;

#[derive(Clone)]
struct ActiveTurnControl {
    run_id: String,
    cancellation_token: runtime::CancellationToken,
    hook_abort_signal: runtime::HookAbortSignal,
}

impl ActiveTurnControl {
    fn abort(&self) {
        self.cancellation_token.cancel();
        self.hook_abort_signal.abort();
    }
}

static ACTIVE_TURN_CONTROLS: OnceLock<Mutex<HashMap<String, ActiveTurnControl>>> = OnceLock::new();

fn active_turn_controls() -> &'static Mutex<HashMap<String, ActiveTurnControl>> {
    ACTIVE_TURN_CONTROLS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_active_turn_control(
    session_id: String,
    run_id: String,
    cancellation_token: runtime::CancellationToken,
    hook_abort_signal: runtime::HookAbortSignal,
) {
    let control = ActiveTurnControl {
        run_id,
        cancellation_token,
        hook_abort_signal,
    };
    active_turn_controls()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(session_id, control);
}

pub(crate) fn clear_active_turn_control(session_id: &str, run_id: &str) {
    let mut controls = active_turn_controls()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if controls
        .get(session_id)
        .is_some_and(|control| control.run_id == run_id)
    {
        controls.remove(session_id);
    }
}

pub(crate) fn abort_active_turn(session_id: &str) -> Option<String> {
    let control = active_turn_controls()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(session_id)
        .cloned();
    control.map(|control| {
        control.abort();
        control.run_id
    })
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

    pub(crate) fn active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.services.session.active_runtime(session_id)
    }

    fn register_runtime(
        &self,
        session_id: String,
        runtime: crate::BuiltRuntime,
    ) -> Result<Option<RuntimeEntry>, String> {
        self.services.session.register_runtime(session_id, runtime)
    }

    fn remove_active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.services.session.remove_active_runtime(session_id)
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

// ── Router ─────────────────────────────────────────────────────

pub fn api_router(state: Arc<AppState>) -> Router {
    let public_routes = public_routes::router();

    let protected_routes = Router::new()
        .merge(approval_routes::router())
        .merge(agent_routes::router())
        .merge(audit_routes::router())
        .merge(channel_routes::router())
        .merge(connector_routes::router())
        .merge(context_routes::router())
        .merge(core_routes::router())
        .merge(cross_plane_routes::router())
        .merge(growth_routes::router())
        .merge(matrix_routes::router())
        .merge(mfg_routes::router())
        .merge(memory_routes::router())
        .merge(message_routes::router())
        .merge(profile_routes::router())
        .merge(runtime_routes::router())
        .merge(session_routes::router())
        .merge(skill_routes::router())
        .merge(slash_routes::router())
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

    for (sequence, message) in session.messages.iter().enumerate() {
        let message_record = message.to_session_message(session_id, sequence);
        store
            .insert_message(&message_record)
            .await
            .map_err(|e| e.to_string())?;

        let message_json = serde_json::from_str::<serde_json::Value>(&message.to_json().render())
            .unwrap_or(serde_json::Value::Null);
        let event = memory::SessionEvent {
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
        };
        store
            .append_event(&event)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

async fn append_session_timeline_event(
    store: &UnifiedSessionStore,
    session_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) {
    let sequence = match store.next_event_sequence(session_id).await {
        Ok(sequence) => sequence,
        Err(error) => {
            tracing::warn!(%session_id, %event_type, error = %error, "failed to allocate session event sequence");
            return;
        }
    };
    let created_at_ms = current_time_ms();
    let event = memory::SessionEvent {
        session_id: session_id.to_string(),
        event_type: event_type.to_string(),
        event_json: payload.to_string(),
        sequence,
        created_at_ms,
    };
    if let Err(error) = store.append_event(&event).await {
        tracing::warn!(%session_id, %event_type, error = %error, "failed to append session event");
    }
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
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        body::Body,
        http::{Request, StatusCode},
    };
    use channel_adapters::platform::adapter::{
        InboundMessage, OutboundMessage, PlatformAdapter, PlatformError, SendResult,
    };
    use channel_adapters::platform::config::PlatformRuntimeConfig;
    use channel_adapters::platform::types::Platform;
    use memory::config::{BudgetConfig, StoreConfig};
    use runtime::permission_enforcer::DestructivePatternDetector;
    use runtime::{ContextProfile, ResumeContextSource};
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::time::Duration;
    use tower::ServiceExt;

    #[derive(Clone, Default)]
    struct CapturedTraceEvents {
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    static TRACE_CAPTURE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();

    fn trace_capture_lock() -> &'static tokio::sync::Mutex<()> {
        TRACE_CAPTURE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
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
        platform_runtime: Option<Arc<PlatformRuntime>>,
    ) -> Arc<crate::services::GatewayServices> {
        let sessions = Arc::new(ActiveSessions::new());
        let lifecycle_kernel =
            Arc::new(crate::session_lifecycle_kernel::SessionLifecycleKernel::new());
        let runtime = Arc::new(crate::runtime_service::RuntimeService::new(
            sessions,
            Arc::new(session::SessionLeaseRegistry::default()),
            session_kernel,
            lifecycle_kernel,
            Instant::now(),
        ));
        let approval_dir =
            std::env::temp_dir().join(format!("cowd-api-approval-{}", uuid::Uuid::new_v4()));
        let approval_repository = approval::FileApprovalRepository::new(
            approval_dir.join("approval_history.json"),
            approval_dir.join("always_approved.json"),
        );
        Arc::new(crate::services::GatewayServices::new(
            runtime,
            task_kernel,
            platform_runtime,
            None,
            test_approval_gate(),
            approval_repository,
        ))
    }

    struct MockPlatformAdapter {
        name: String,
        connected: bool,
        sent: Arc<std::sync::Mutex<Vec<OutboundMessage>>>,
        media_sent: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl MockPlatformAdapter {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                connected: false,
                sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                media_sent: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn new_with_sent(name: &str, sent: Arc<std::sync::Mutex<Vec<OutboundMessage>>>) -> Self {
            Self {
                name: name.to_string(),
                connected: false,
                sent,
                media_sent: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn new_with_media(name: &str, media_sent: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
            Self {
                name: name.to_string(),
                connected: false,
                sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                media_sent,
            }
        }
    }

    #[async_trait::async_trait]
    impl PlatformAdapter for MockPlatformAdapter {
        fn platform(&self) -> Platform {
            Platform::Custom(self.name.clone())
        }

        fn platform_name(&self) -> &str {
            &self.name
        }

        async fn connect(&mut self) -> Result<(), PlatformError> {
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), PlatformError> {
            self.connected = false;
            Ok(())
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        async fn receive(&mut self) -> Result<Option<InboundMessage>, PlatformError> {
            Ok(None)
        }

        async fn send(&self, msg: &OutboundMessage) -> Result<SendResult, PlatformError> {
            self.sent.lock().unwrap().push(msg.clone());
            Ok(SendResult::success(Some(format!(
                "mock-{}",
                msg.session_key.user_id
            ))))
        }

        async fn send_image(
            &self,
            chat_id: &str,
            image_url: &str,
            caption: Option<&str>,
        ) -> Result<(), PlatformError> {
            self.media_sent.lock().unwrap().push(format!(
                "image-url:{chat_id}:{image_url}:{}",
                caption.unwrap_or("")
            ));
            Ok(())
        }

        async fn send_image_file(
            &self,
            chat_id: &str,
            image_path: &str,
            caption: Option<&str>,
        ) -> Result<(), PlatformError> {
            self.media_sent.lock().unwrap().push(format!(
                "image-file:{chat_id}:{image_path}:{}",
                caption.unwrap_or("")
            ));
            Ok(())
        }

        async fn send_document(
            &self,
            chat_id: &str,
            file_path: &str,
            file_name: Option<&str>,
            caption: Option<&str>,
        ) -> Result<(), PlatformError> {
            self.media_sent.lock().unwrap().push(format!(
                "file:{chat_id}:{file_path}:{}:{}",
                file_name.unwrap_or(""),
                caption.unwrap_or("")
            ));
            Ok(())
        }
    }

    async fn test_platform_runtime_with_bound_adapter(name: &str) -> Arc<PlatformRuntime> {
        let runtime = Arc::new(PlatformRuntime::new(PlatformRuntimeConfig::default()));
        runtime
            .register_adapter(Box::new(MockPlatformAdapter::new(name)))
            .await
            .unwrap();
        runtime.start().await.unwrap();
        runtime
    }

    async fn test_platform_runtime_with_sent_adapter(
        name: &str,
    ) -> (
        Arc<PlatformRuntime>,
        Arc<std::sync::Mutex<Vec<OutboundMessage>>>,
    ) {
        let runtime = Arc::new(PlatformRuntime::new(PlatformRuntimeConfig::default()));
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        runtime
            .register_adapter(Box::new(MockPlatformAdapter::new_with_sent(
                name,
                sent.clone(),
            )))
            .await
            .unwrap();
        runtime.start().await.unwrap();
        (runtime, sent)
    }

    async fn test_platform_runtime_with_media_adapter(
        name: &str,
    ) -> (Arc<PlatformRuntime>, Arc<std::sync::Mutex<Vec<String>>>) {
        let runtime = Arc::new(PlatformRuntime::new(PlatformRuntimeConfig::default()));
        let media_sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        runtime
            .register_adapter(Box::new(MockPlatformAdapter::new_with_media(
                name,
                media_sent.clone(),
            )))
            .await
            .unwrap();
        runtime.start().await.unwrap();
        (runtime, media_sent)
    }

    fn test_state() -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, None),
            session_lease_registry: Some(registry),
        })
    }

    fn test_state_with_config_and_runtime(
        config: serde_json::Value,
        platform_runtime: Option<Arc<PlatformRuntime>>,
    ) -> Arc<AppState> {
        test_state_with_config_runtime_and_workspace(
            config,
            platform_runtime,
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        )
    }

    fn test_state_with_config_runtime_and_workspace(
        config: serde_json::Value,
        platform_runtime: Option<Arc<PlatformRuntime>>,
        workspace_root: PathBuf,
    ) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        let session_kernel = test_session_kernel(sessions.clone(), None, event_bus.clone());
        let task_kernel = test_task_kernel();
        Arc::new(AppState {
            tool_registry: tools,
            config: Some(config),
            event_bus,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            approval_gate: None,
            auth_token: None,
            workspace_root,
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_kernel, task_kernel, platform_runtime),
            session_lease_registry: None,
        })
    }

    fn unique_test_workspace(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cowd-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn test_state_with_store(store: Arc<UnifiedSessionStore>) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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

    fn test_temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cowd-api-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
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
                && item["status"] == "pending"));
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
            .append_runtime_event(&memory::RuntimeEvent::new(
                session_id,
                0,
                memory::RuntimeEventScope::Turn,
                "execution.outcome",
                serde_json::json!({"status": "ok", "title": "full loop outcome"}),
                current_time_ms(),
            ))
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
        assert_eq!(gate_json["status"], "pass");
        assert!(gate_json["checks"].as_array().unwrap().iter().any(|check| {
            check["check_id"] == "structured_data.indexes.ready" && check["status"] == "pass"
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
                                "source_ref": "connector:mock.docs:gpu-shortage",
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
                                "source_ref": "connector:mock.docs:gpu-shortage",
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
    async fn matrix_evidence_context_and_mfg_incident_create_agent_graph() {
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
        assert!(incident_json["agent_graph"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["id"] == "mfg_researcher"));
        assert!(incident_json["agent_graph"]["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|evidence| evidence["reference"] == format!("mfg:evidence:{packet_id}")));
        let incident_id = incident_json["incident"]["incident_id"].as_str().unwrap();

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
    async fn agent_run_persists_evidence_to_session_event() {
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
                    .uri("/api/agents/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(runs.status(), StatusCode::OK);
        let body = to_bytes(runs.into_body(), usize::MAX).await.unwrap();
        let runs_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(runs_json["kind"], "agent_run_graphs");
        assert_eq!(runs_json["runs"][0]["session_id"], task_id);

        let upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/agent-graph"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "coordinate multi agent",
                            "nodes": [
                                {
                                    "id": "planner",
                                    "role": "planner",
                                    "title": "Plan",
                                    "objective": "split work",
                                    "depends_on": [],
                                    "status": "ready",
                                    "created_at_ms": 1,
                                    "updated_at_ms": 1
                                },
                                {
                                    "id": "review",
                                    "role": "reviewer",
                                    "title": "Review",
                                    "objective": "challenge result",
                                    "depends_on": ["planner"],
                                    "status": "pending",
                                    "created_at_ms": 1,
                                    "updated_at_ms": 1
                                }
                            ]
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

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/tasks/{task_id}/agent-graph"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        let body = to_bytes(fetched.into_body(), usize::MAX).await.unwrap();
        let fetched_graph: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(fetched_graph["nodes"][1]["id"], "review");
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
        assert_eq!(diff.status(), StatusCode::OK);
        let body = to_bytes(diff.into_body(), usize::MAX).await.unwrap();
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
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].event_type, "TextDelta");
    }

    #[tokio::test]
    async fn append_session_timeline_event_persists_stream_events_in_order() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        let session_id = "timeline-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();

        append_session_timeline_event(
            &store,
            session_id,
            "TurnStarted",
            serde_json::json!({"type":"TurnStarted"}),
        )
        .await;
        append_session_timeline_event(
            &store,
            session_id,
            "TextDelta",
            serde_json::json!({"type":"TextDelta","content":"hello"}),
        )
        .await;

        let events = store.get_events(session_id, 0).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "TurnStarted");
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].event_type, "TextDelta");
        assert_eq!(events[1].sequence, 1);
        assert!(events[1].event_json.contains("hello"));
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

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/events"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["events"][0]["type"], "TurnCancelRequested");
        assert_eq!(json["events"][0]["payload"]["actor_id"], "tui:test");
        assert_eq!(json["events"][0]["payload"]["reason"], "test_cancel");
        assert_eq!(json["events"][0]["payload"]["aborted"], false);
        assert_eq!(
            json["events"][0]["payload"]["run_id"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn active_turn_registry_aborts_runtime_control_signals() {
        let session_id = format!("cancel-active-{}", uuid::Uuid::new_v4());
        let run_id = "run-active-cancel".to_string();
        let cancellation_token = runtime::CancellationToken::new();
        let hook_abort_signal = runtime::HookAbortSignal::new();

        register_active_turn_control(
            session_id.clone(),
            run_id.clone(),
            cancellation_token.clone(),
            hook_abort_signal.clone(),
        );

        assert_eq!(abort_active_turn(&session_id), Some(run_id.clone()));
        assert!(cancellation_token.is_cancelled());
        assert!(hook_abort_signal.is_aborted());

        clear_active_turn_control(&session_id, &run_id);
        assert_eq!(abort_active_turn(&session_id), None);
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
            .append_runtime_event(&memory::RuntimeEvent::new(
                session_id,
                1,
                memory::RuntimeEventScope::Memory,
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
    async fn runtime_timeline_projects_workgraph_summary() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-workgraph-summary-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let mut event = memory::RuntimeEvent::new(
            session_id,
            0,
            memory::RuntimeEventScope::Workgraph,
            "agent.workgraph.reviewed",
            serde_json::json!({
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
            10,
        );
        event.refs = vec![
            memory::RuntimeRef {
                ref_type: "workgraph".to_string(),
                id: "graph-summary".to_string(),
                label: None,
            },
            memory::RuntimeRef {
                ref_type: "collaboration_board".to_string(),
                id: "board-summary".to_string(),
                label: None,
            },
        ];
        store.append_runtime_event(&event).await.unwrap();

        let state = test_state_with_store(store);
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
        assert_eq!(json["workgraph_summary"]["count"], 1);
        assert_eq!(
            json["workgraph_summary"]["latest"]["graph_id"],
            "graph-summary"
        );
        assert_eq!(
            json["workgraph_summary"]["latest"]["board_id"],
            "board-summary"
        );
        assert_eq!(json["workgraph_summary"]["latest"]["completion_rate"], 1.0);
        assert_eq!(
            json["workgraph_summary"]["latest"]["value_verdict"]["positive_lift"],
            true
        );
        assert_eq!(json["workgraph_summary"]["agent_tasks"], 1);
        assert_eq!(json["workgraph_summary"]["memory_candidates"], 1);
        assert_eq!(json["workgraph_summary"]["conflicts"], 1);
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
        store
            .append_runtime_event(&memory::RuntimeEvent::new(
                session_id,
                0,
                memory::RuntimeEventScope::Task,
                "task.started",
                serde_json::json!({"task_id": "task-health"}),
                10,
            ))
            .await
            .unwrap();
        store
            .append_runtime_event(&memory::RuntimeEvent::new(
                session_id,
                1,
                memory::RuntimeEventScope::Policy,
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
        store
            .append_runtime_event(&memory::RuntimeEvent::new(
                session_id,
                2,
                memory::RuntimeEventScope::Workgraph,
                "agent.workgraph.reviewed",
                serde_json::json!({
                    "value_verdict": {
                        "positive_lift": true,
                        "continue_multi_agent": true,
                        "value_score": 73,
                        "reasons": ["positive_multi_agent_lift"]
                    }
                }),
                12,
            ))
            .await
            .unwrap();
        store
            .append_runtime_event(&memory::RuntimeEvent::new(
                session_id,
                3,
                memory::RuntimeEventScope::Task,
                "task.completed",
                serde_json::json!({"task_id": "task-health"}),
                13,
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store);
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
        assert_eq!(json["health_summary"]["scope_counts"]["workgraph"], 1);
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
        assert_eq!(json["workgraph_summary"]["count"], 0);
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
            "collect_workgraph_review"
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
            json["control_policy"]["task"]["thresholds"]["critical_min"],
            80
        );
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
    protocol: "openai-compat"
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
        assert_eq!(json["providers"][0]["credential_present"], true);
        assert!(!json.to_string().contains("secret-local-key"));

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
        let root = test_temp_dir("slash-catalog");
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
    #[serial_test::serial(provider_registry)]
    async fn runtime_provider_reload_replaces_global_registry_from_config() {
        runtime::init_global_providers(model_protocol::provider_config::ProvidersConfig::default());
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
    protocol: "openai-compat"
"#,
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(workspace, config_home));
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
        let provider = runtime::resolve_global_provider("reload-model")
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

        let app = api_router(test_state_with_workspace(
            invalid_workspace,
            invalid_config_home,
        ));
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
        assert!(runtime::resolve_global_provider("broken-model").is_none());
        assert_eq!(
            runtime::resolve_global_provider("reload-model")
                .expect("existing provider should remain after failed reload")
                .name,
            "reload"
        );

        runtime::init_global_providers(model_protocol::provider_config::ProvidersConfig::default());
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(invalid_root);
    }

    #[tokio::test(flavor = "current_thread")]
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
        assert!(joined.contains("capability_count=31"));

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
        assert_eq!(packet.source, ResumeContextSource::TaskRegistry);
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
    async fn approval_routes_resolve_pending_gate_request() {
        let gate = test_approval_gate();
        let state = test_state_with_approval_gate(gate.clone());
        let app = api_router(state);

        let eval_gate = gate.clone();
        let eval = tokio::spawn(async move {
            eval_gate
                .evaluate("bash", r#"{"command":"rm -rf /tmp/cowd-approval-e2e"}"#)
                .await
        });

        let pending_json = loop {
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
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if json.as_array().is_some_and(|items| !items.is_empty()) {
                break json;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let request_id = pending_json[0]["id"].as_str().unwrap().to_string();
        assert!(pending_json[0]["command"]
            .as_str()
            .unwrap()
            .contains("rm -rf"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": request_id,
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

        let result = eval.await.unwrap();
        assert!(matches!(
            result,
            runtime::approval_gate::ApprovalGateResult::Approved { .. }
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        let history_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(history_response.status(), StatusCode::OK);
        let body = to_bytes(history_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let history: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(history[0]["request_id"], pending_json[0]["id"]);

        let export_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/audit/export?source=approval&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(export_response.status(), StatusCode::OK);
        let body = to_bytes(export_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let export: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(export["kind"], "audit_export");
        assert_eq!(export["source"], "approval");
        assert_eq!(export["totals"]["approval"], 1);
        assert_eq!(export["records"][0]["source"], "approval");
        assert_eq!(
            export["records"][0]["record"]["request_id"],
            pending_json[0]["id"]
        );
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
                .contains(&serde_json::json!("qr_login"))));
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
            .any(|item| item["capability_id"] == "service.mock.docs.read"
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
            .any(|item| item == "channel.feishu.delivery"));
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
    async fn mock_docs_service_connector_executes_through_cross_plane_receipt() {
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
                    .uri("/api/connectors/services/mock.docs/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tools.status(), StatusCode::OK);
        let body = to_bytes(tools.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_service_tools");
        assert_eq!(json["service"]["id"], "mock.docs");
        assert!(json["tools"].as_array().unwrap().iter().any(|tool| {
            tool["capability_id"] == "service.mock.docs.read" && tool["plane"] == "service"
        }));

        let key = format!("mock-docs-{}", uuid::Uuid::new_v4());
        let request = serde_json::json!({
            "actor_principal": format!("user:{key}"),
            "tool_id": "service.mock.docs.read",
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
                    .uri("/api/connectors/services/mock.docs/execute")
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
        assert_eq!(first_json["service"], "mock.docs");
        assert_eq!(first_json["replayed"], false);
        assert_eq!(first_json["resource_persisted"], true);
        assert_eq!(
            first_json["result"]["resource"]["reference"],
            "service://mock.docs/document/doc-1"
        );
        assert_eq!(
            first_json["receipt"]["action"]["requested_capability"],
            "service.mock.docs.read"
        );
        assert_eq!(
            first_json["receipt"]["action"]["resource_ref"],
            "service://mock.docs/document/doc-1"
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
                |resource| resource["reference"] == "service://mock.docs/document/doc-1"
                    && resource["title"] == "Architecture"
            ));

        let replay = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/mock.docs/execute")
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
            "tool_id": "service.mock.docs.read",
            "resource_id": "persisted-doc",
            "title": "Persisted Runtime Resource",
            "mode": "dry_run",
            "idempotency_key": format!("persisted-doc-{}", uuid::Uuid::new_v4())
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/mock.docs/execute")
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
            .any(
                |resource| resource["reference"] == "service://mock.docs/document/persisted-doc"
                    && resource["title"] == "Persisted Runtime Resource"
            ));
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
            "tool_id": "service.mock.docs.read",
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
                    .uri("/api/connectors/services/mock.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let revalidate = serde_json::json!({
            "reference": "service://mock.docs/document/revalidate-doc",
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
            "tool_id": "service.mock.docs.read",
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
                    .uri("/api/connectors/services/mock.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let promote = serde_json::json!({
            "reference": "service://mock.docs/document/memory-doc",
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
        assert!(content.contains("service://mock.docs/document/memory-doc"));
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
                    .contains("ref: service://mock.docs/document/memory-doc")
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
    async fn connector_service_commit_consumes_single_use_grant_and_audits() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:service-commit-{suffix}");
        let capability = "service.mock.docs.read";
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
                    .uri("/api/connectors/services/mock.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result"]["status"], "ok");
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
        let consumed = audit_json["records"]
            .as_array()
            .unwrap()
            .iter()
            .find(|record| {
                record["evidence"]["consumed_grant_id"].as_str() == Some(grant_id.as_str())
            })
            .expect("commit audit should include single-use grant consumption");
        assert_eq!(consumed["evidence"]["remaining_uses_after"], 0);

        let action = serde_json::json!({
            "actor_principal": principal,
            "requested_capability": capability,
            "provider_account": "mock.docs",
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
        assert_eq!(preflight_json["platform_readiness"]["status"], "ready");
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
        assert_eq!(json["executable"], true);
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
        assert_eq!(json["dispatch_status"], "adapter_not_bound");
        assert_eq!(json["executable"], false);
        assert_eq!(json["adapter_capability"]["live_supported"], true);
        assert_eq!(json["adapter_capability"]["adapter_bound"], false);
        assert!(json["blockers"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("dispatch:adapter_not_bound")));

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
    async fn cross_plane_adapter_registry_and_preflight_use_bound_runtime_snapshot() {
        let platform_runtime = test_platform_runtime_with_bound_adapter("feishu").await;
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
            Some(platform_runtime.clone()),
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = format!("user:bound-runtime-{suffix}");
        let capability = format!("channel.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-bound-runtime-{suffix}"),
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

        let adapters = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/action/adapters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let adapters_body = to_bytes(adapters.into_body(), usize::MAX).await.unwrap();
        let adapters_json: serde_json::Value = serde_json::from_slice(&adapters_body).unwrap();
        assert!(adapters_json["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["platform"] == "feishu"
                    && item["operation"] == "send_text"
                    && item["adapter_bound"] == true
            }));

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
        let preflight_body = to_bytes(preflight.into_body(), usize::MAX).await.unwrap();
        let preflight_json: serde_json::Value = serde_json::from_slice(&preflight_body).unwrap();
        assert_eq!(preflight_json["adapter_capability"]["adapter_bound"], true);
        assert_eq!(preflight_json["dispatch_target"]["ready"], false);
        assert!(preflight_json["dispatch_target"]["blockers"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("dispatch:target_ref_missing")));

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-bound-runtime-{suffix}"),
            "action": action
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
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();
        assert_eq!(
            executed_json["dispatch_status"],
            "dispatch_target_not_ready"
        );
        assert_eq!(executed_json["adapter_capability"]["adapter_bound"], true);
        assert!(executed_json["blockers"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("dispatch:target_ref_missing")));

        platform_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cross_plane_preflight_builds_dispatch_target_plan() {
        let platform_runtime = test_platform_runtime_with_bound_adapter("feishu").await;
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
            Some(platform_runtime.clone()),
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

        platform_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cross_plane_execute_persists_dispatch_target_snapshot() {
        let platform_runtime = test_platform_runtime_with_bound_adapter("feishu").await;
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
            Some(platform_runtime.clone()),
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

        platform_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_dispatches_ready_text_target() {
        let (platform_runtime, sent) = test_platform_runtime_with_sent_adapter("feishu").await;
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
            Some(platform_runtime.clone()),
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

        assert_eq!(executed_json["status"], "dispatched");
        assert_eq!(executed_json["dispatch_status"], "sent");
        assert_eq!(executed_json["dispatched"], true);
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_status"],
            "sent"
        );
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["session_key"],
            "feishu:live-chat"
        );
        assert_eq!(executed_json["dispatch_outcome"]["status"], "sent");
        assert_eq!(
            executed_json["dispatch_outcome"]["provider_message_id"],
            "mock-live-chat"
        );
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_outcome"]["session_key"],
            "feishu:live-chat"
        );

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].session_key.as_str(), "feishu:live-chat");
        assert_eq!(sent[0].text, "live payload");
        drop(sent);

        platform_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_dispatches_ready_image_target() {
        let (platform_runtime, media_sent) =
            test_platform_runtime_with_media_adapter("feishu").await;
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
            Some(platform_runtime.clone()),
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

        assert_eq!(executed_json["status"], "dispatched");
        assert_eq!(executed_json["dispatch_status"], "sent");
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_kind"],
            "image"
        );
        assert_eq!(executed_json["dispatch_outcome"]["operation"], "send_image");
        let media_sent = media_sent.lock().unwrap();
        assert_eq!(
            media_sent.as_slice(),
            ["image-url:live-chat:https://example.test/panel.png:"]
        );
        drop(media_sent);

        platform_runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_dispatches_workspace_file_target() {
        let root = test_temp_dir("cross-plane-file-dispatch");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(workspace.join("reports")).unwrap();
        let report_path = workspace.join("reports").join("panel.txt");
        std::fs::write(&report_path, "dispatchable report").unwrap();
        let (platform_runtime, media_sent) =
            test_platform_runtime_with_media_adapter("feishu").await;
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
            Some(platform_runtime.clone()),
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

        assert_eq!(executed_json["status"], "dispatched");
        assert_eq!(executed_json["dispatch_status"], "sent");
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_kind"],
            "file"
        );
        let media_sent = media_sent.lock().unwrap();
        assert_eq!(
            media_sent.as_slice(),
            [format!(
                "file:live-chat:{}:panel.txt:",
                report_path.canonicalize().unwrap().display()
            )]
        );
        drop(media_sent);

        platform_runtime.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_blocks_file_outside_workspace() {
        let root = test_temp_dir("cross-plane-file-block");
        let workspace = root.join("workspace");
        let outside = root.join("outside.txt");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(&outside, "must not send").unwrap();
        let (platform_runtime, media_sent) =
            test_platform_runtime_with_media_adapter("feishu").await;
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
            Some(platform_runtime.clone()),
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
        assert_eq!(executed_json["dispatch_status"], "dispatch_failed");
        assert!(executed_json["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| blocker
                .as_str()
                .unwrap_or_default()
                .contains("workspace_payload_outside_root")));
        assert!(media_sent.lock().unwrap().is_empty());

        platform_runtime.shutdown().await.unwrap();
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
            8
        );
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
            "tool_id": "service.mock.docs.read",
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
                    .uri("/api/connectors/services/mock.docs/execute")
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
            .find(|item| item["id"] == "service://mock.docs/document/context-doc")
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
            "service://mock.docs/document/context-doc"
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
            "tool_id": "service.mock.docs.read",
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
                    .uri("/api/connectors/services/mock.docs/execute")
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
                    .uri("/api/evidence/resolve?ref=service%3A%2F%2Fmock.docs%2Fdocument%2Fevidence-doc")
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
            .append_runtime_event(&memory::RuntimeEvent::new(
                session_id,
                3,
                memory::RuntimeEventScope::Policy,
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
        assert_eq!(timeline["events"][2]["kind"], "RuntimeRun");
        assert_eq!(timeline["events"][2]["status"], "completed");
        assert_eq!(timeline["events"][2]["refs"][0]["type"], "context_envelope");
        assert_eq!(
            timeline["events"][2]["refs"][0]["id"],
            "ctx-runtime-timeline"
        );
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
    async fn profile_and_workspace_routes_stay_protected_when_auth_token_set() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
            "/api/channels/wechat-ilink/accounts",
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
        let tools = Arc::new(GlobalToolRegistry::builtin());
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
            config_home: default_config_home(),
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
