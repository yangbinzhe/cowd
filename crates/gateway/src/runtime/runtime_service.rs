use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;
use tokio::task::JoinHandle;

use chrono::Utc;

use crate::gateway::ActiveSessions;
use crate::runtime_boundary::{
    RuntimeBoundaryClock, RuntimeBoundarySnapshot, RuntimeBoundaryStatus,
};
use crate::runtime_protocol::{RuntimeErrorKind, RuntimeRequest, RuntimeResponse};
use crate::session_kernel::SessionKernel;
use crate::session_lifecycle_kernel::SessionLifecycleKernel;
use harness_contract::{
    context::ContextTurnReport,
    projection::{ExecutionLiveState, ExecutionLiveStatus, SessionExecutionIndexProjection},
    task::{TaskId, TaskTurnBinding},
    turn::{
        InputRoutingDecision, SessionInputEnvelope, SessionInputId, SessionInputProjection,
        SessionInputReceipt, TurnEvent, TurnId, TurnInboxSnapshot, TurnInput, TurnJournalEnvelope,
        TurnJournalPhase, TurnReceipt, TurnStatus,
    },
};
use session::SessionLeaseRegistry;

#[async_trait::async_trait]
pub(crate) trait SessionActivationPort: Send + Sync {
    async fn activate(&self, session_id: &str) -> Result<(), String>;
}

use crate::services::{
    ActiveMessagesPage, SessionCompactResult, SessionMessageCounts, SessionStatsSnapshot,
    SessionTokenCounts,
};

#[derive(Debug)]
pub(crate) enum RuntimeTurnExecutionError {
    NotFound(String),
    Runtime(String),
    Join(String),
}

impl RuntimeTurnExecutionError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::NotFound(message) | Self::Runtime(message) | Self::Join(message) => {
                message.clone()
            }
        }
    }
}

/// Convert the Runtime's externally tagged event enum into the stable
/// browser/TUI event envelope. This is transport shaping only: it must never
/// manufacture lifecycle facts or terminal state.
fn runtime_event_stream_payload(event: runtime::CowdEvent) -> serde_json::Value {
    let execution_context = event.execution_context().cloned();
    let value = serde_json::to_value(event.domain_event()).unwrap_or_else(|error| {
        serde_json::json!({
            "type": "RuntimeEventEncodingError",
            "error": error.to_string(),
        })
    });
    let mut payload = match value {
        serde_json::Value::String(event_type) => serde_json::json!({"type": event_type}),
        serde_json::Value::Object(envelope) if envelope.len() == 1 => {
            let Some((event_type, payload)) = envelope.into_iter().next() else {
                return serde_json::json!({"type": "RuntimeEvent"});
            };
            match payload {
                serde_json::Value::Object(mut fields) => {
                    fields.insert("type".to_string(), serde_json::Value::String(event_type));
                    serde_json::Value::Object(fields)
                }
                payload => serde_json::json!({"type": event_type, "value": payload}),
            }
        }
        payload => serde_json::json!({"type": "RuntimeEvent", "value": payload}),
    };
    if let (Some(context), serde_json::Value::Object(fields)) = (execution_context, &mut payload) {
        fields.insert(
            "execution_id".to_string(),
            serde_json::Value::String(context.execution_id),
        );
        fields.insert(
            "turn_id".to_string(),
            serde_json::Value::String(context.turn_id),
        );
    }
    payload
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTurnExecution {
    pub(crate) summary: runtime::TurnSummary,
    pub(crate) receipt: TurnReceipt,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTurnOptions {
    pub(crate) profile: runtime::ContextProfile,
    pub(crate) pre_messages: Vec<runtime::ConversationMessage>,
}

/// Persisted verbatim in the Session ingress row.  The Session store treats it
/// as opaque JSON; only Runtime decodes the typed context/profile payload when
/// it becomes the exclusive execution owner.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct IngressRuntimeOptions {
    pub(crate) profile: runtime::ContextProfile,
    #[serde(default)]
    pub(crate) pre_messages: Vec<IngressPreMessage>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct IngressPreMessage {
    pub(crate) role: runtime::MessageRole,
    pub(crate) blocks: Vec<runtime::ContentBlock>,
}

impl From<runtime::ConversationMessage> for IngressPreMessage {
    fn from(message: runtime::ConversationMessage) -> Self {
        Self {
            role: message.role,
            blocks: message.blocks,
        }
    }
}

impl IngressPreMessage {
    fn into_conversation_message(self) -> runtime::ConversationMessage {
        runtime::ConversationMessage {
            role: self.role,
            blocks: self.blocks,
            usage: None,
        }
    }
}

impl Default for IngressRuntimeOptions {
    fn default() -> Self {
        Self {
            profile: runtime::ContextProfile::MainTurn,
            pre_messages: Vec::new(),
        }
    }
}

impl Default for RuntimeTurnOptions {
    fn default() -> Self {
        Self {
            profile: runtime::ContextProfile::MainTurn,
            pre_messages: Vec::new(),
        }
    }
}

fn extract_session_target_ref(content: &str) -> Option<&str> {
    let marker = "@session:";
    let start = content.find(marker)? + marker.len();
    let rest = content[start..].trim_start();
    let end = rest
        .find(|ch: char| ch.is_whitespace() || ch == ',' || ch == ';')
        .unwrap_or(rest.len());
    let target = rest[..end].trim();
    (!target.is_empty()).then_some(target)
}

fn is_live_terminal(status: ExecutionLiveStatus) -> bool {
    status.is_terminal()
}

/// Operational cancellation is deliberately separate from the durable live
/// projection.  RuntimeServices owns the lifecycle state; Gateway retains
/// only a short-lived handle required to signal an in-flight host.
#[derive(Clone)]
struct ActiveTurnControl {
    session_id: String,
    execution_id: Option<String>,
    cancellation_token: runtime::CancellationToken,
}

struct ActiveTurnControlGuard {
    turn_id: String,
    controls: Arc<Mutex<BTreeMap<String, ActiveTurnControl>>>,
}

impl Drop for ActiveTurnControlGuard {
    fn drop(&mut self) {
        self.controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.turn_id);
    }
}

/// Restores a moved Runtime host even if the request future is aborted.  The
/// normal path awaits `restore`; cancellation uses Drop to schedule the same
/// restoration before another turn can acquire the session.
struct RuntimeTurnOwner {
    entry: Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>,
    runtime:
        Option<runtime::StandardRuntimeHost<crate::gateway_tool_executor::GatewayToolExecutor>>,
}

impl RuntimeTurnOwner {
    fn new(
        entry: Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>,
        runtime: runtime::StandardRuntimeHost<crate::gateway_tool_executor::GatewayToolExecutor>,
    ) -> Self {
        Self {
            entry,
            runtime: Some(runtime),
        }
    }

    fn runtime_mut(
        &mut self,
    ) -> &mut runtime::StandardRuntimeHost<crate::gateway_tool_executor::GatewayToolExecutor> {
        self.runtime
            .as_mut()
            .expect("RuntimeTurnOwner always owns a host until it is restored")
    }

    async fn restore(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let mut entry = self.entry.lock().await;
        entry.restore_runtime_after_turn(runtime);
    }
}

impl Drop for RuntimeTurnOwner {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let entry = Arc::clone(&self.entry);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let mut entry = entry.lock().await;
                    if entry.turn_is_owned() {
                        entry.restore_runtime_after_turn(runtime);
                    } else {
                        tracing::error!(
                            "cancelled turn attempted to restore a Runtime host into an occupied session"
                        );
                    }
                });
            }
            Err(_) => tracing::error!(
                "cancelled turn dropped outside Tokio and could not restore its Runtime host"
            ),
        }
    }
}

fn session_execution_index_from_outbox(
    session_id: &str,
    records: &[memory::SessionRuntimeOutboxRecord],
) -> SessionExecutionIndexProjection {
    let mut ordered = records.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|record| {
        (
            record.updated_at_ms,
            record.sequence,
            record.request_id.as_str(),
        )
    });
    let latest = ordered.last().copied();
    let execution_for = |record: &memory::SessionRuntimeOutboxRecord| {
        runtime::session_ingress_graph_id(session_id, &record.request_id, &record.turn_id)
    };
    let status_for = |record: &memory::SessionRuntimeOutboxRecord| match record.status {
        memory::OutboxStatus::Pending | memory::OutboxStatus::RetryScheduled => {
            ExecutionLiveStatus::Queued
        }
        memory::OutboxStatus::Claimed => ExecutionLiveStatus::PreparingContext,
        memory::OutboxStatus::Materialized => ExecutionLiveStatus::Complete,
        memory::OutboxStatus::BlockedMaterialization => ExecutionLiveStatus::Error,
    };
    let latest_status = latest.map(status_for);
    SessionExecutionIndexProjection {
        session_id: session_id.to_string(),
        active_execution_ids: ordered
            .iter()
            .filter(|record| !is_live_terminal(status_for(record)))
            .map(|record| execution_for(record))
            .collect(),
        latest_execution_id: latest.map(execution_for),
        latest_status,
        latest_live_revision: None,
        last_progress_at_ms: latest.map(|record| record.updated_at_ms),
        terminal_ref: latest
            .filter(|record| record.status == memory::OutboxStatus::Materialized)
            .map(|record| format!("turn-terminal:{}", record.request_id)),
    }
}

#[derive(Clone)]
pub(crate) struct RuntimeService {
    sessions: Arc<ActiveSessions>,
    lease_registry: Arc<SessionLeaseRegistry>,
    session_kernel: Arc<SessionKernel>,
    lifecycle_kernel: Arc<SessionLifecycleKernel>,
    started_at: Instant,
    turns: Arc<Mutex<BTreeMap<String, TurnReceipt>>>,
    turn_bindings: Arc<Mutex<BTreeMap<String, TaskTurnBinding>>>,
    active_turn_controls: Arc<Mutex<BTreeMap<String, ActiveTurnControl>>>,
    session_inputs: Arc<Mutex<BTreeMap<String, runtime::SessionInputStream>>>,
    session_event_buses: Arc<Mutex<BTreeMap<String, runtime::CowdEventBus>>>,
    session_event_relays: Arc<Mutex<BTreeMap<String, JoinHandle<()>>>>,
    session_models: Arc<Mutex<BTreeMap<String, String>>>,
    approval_gate: Option<Arc<runtime::approval_gate::SmartApprovalGate>>,
    provider_registry: Arc<runtime::ProviderRegistry>,
    upgrade_coordinator: Arc<runtime::UpgradeCoordinator>,
    config_reload: Arc<crate::runtime_host::config_reload::ConfigReloadState>,
    tool_host: Arc<tools::ToolHost>,
    resource_capabilities: runtime::ResourceCapabilityIndex,
    runtime_services: Arc<runtime::RuntimeServices>,
    session_input_router: Arc<runtime::SessionInputRouter>,
    session_activator: Arc<OnceLock<Weak<dyn SessionActivationPort>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionInputAdmission {
    pub(crate) receipt: SessionInputReceipt,
    pub(crate) materialized: Option<serde_json::Value>,
    /// Server-issued execution identity for the accepted ingress. Surfaces
    /// attach to this canonical graph rather than inferring it from prose.
    pub(crate) execution_graph_id: String,
}

impl RuntimeService {
    #[must_use]
    pub(crate) fn new(
        sessions: Arc<ActiveSessions>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_kernel: Arc<SessionKernel>,
        lifecycle_kernel: Arc<SessionLifecycleKernel>,
        started_at: Instant,
        provider_registry: Arc<runtime::ProviderRegistry>,
        upgrade_coordinator: Arc<runtime::UpgradeCoordinator>,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> Result<Self, String> {
        let session_input_router = runtime_services
            .session_input_router()
            .cloned()
            .ok_or_else(|| "durable SessionInputRouter is required".to_string())?;
        let workspace_root = runtime_services.workspace_root().to_path_buf();
        let resource_capabilities = runtime::ResourceCapabilityIndex::default();
        let _ = resource_capabilities.refresh_from_environment();
        Ok(Self {
            sessions,
            lease_registry,
            session_kernel,
            lifecycle_kernel,
            started_at,
            turns: Arc::new(Mutex::new(BTreeMap::new())),
            turn_bindings: Arc::new(Mutex::new(BTreeMap::new())),
            active_turn_controls: Arc::new(Mutex::new(BTreeMap::new())),
            session_inputs: Arc::new(Mutex::new(BTreeMap::new())),
            session_event_buses: Arc::new(Mutex::new(BTreeMap::new())),
            session_event_relays: Arc::new(Mutex::new(BTreeMap::new())),
            session_models: Arc::new(Mutex::new(BTreeMap::new())),
            approval_gate: None,
            provider_registry,
            upgrade_coordinator,
            config_reload: Arc::new(crate::runtime_host::config_reload::ConfigReloadState::new()),
            tool_host: Arc::new(tools::ToolHost::builtin("gateway-runtime", workspace_root)),
            resource_capabilities,
            runtime_services,
            session_input_router,
            session_activator: Arc::new(OnceLock::new()),
        })
    }

    #[must_use]
    pub(crate) fn with_approval_gate(
        mut self,
        approval_gate: Arc<runtime::approval_gate::SmartApprovalGate>,
    ) -> Self {
        self.approval_gate = Some(approval_gate);
        self
    }

    pub(crate) fn session_input_router(&self) -> Arc<runtime::SessionInputRouter> {
        Arc::clone(&self.session_input_router)
    }

    pub(crate) fn install_session_activator(
        &self,
        activator: Weak<dyn SessionActivationPort>,
    ) -> Result<(), String> {
        self.session_activator
            .set(activator)
            .map_err(|_| "session activation port is already installed".to_string())
    }

    pub(crate) fn resource_capability_index(&self) -> runtime::ResourceCapabilityIndex {
        self.resource_capabilities.clone()
    }

    pub(crate) fn execution_live(&self, execution_id: &str) -> Option<ExecutionLiveState> {
        self.runtime_services.execution_live(execution_id)
    }

    pub(crate) fn session_execution_index(
        &self,
        session_id: &str,
    ) -> SessionExecutionIndexProjection {
        self.runtime_services.session_execution_index(session_id)
    }

    pub(crate) fn running_session_execution_indices(&self) -> Vec<SessionExecutionIndexProjection> {
        self.runtime_services.running_session_execution_indices()
    }

    /// Recover the discovery index after a Gateway restart from the durable
    /// Session ingress carrier.  It deliberately restores only identity and
    /// ingress lifecycle; detailed context/metrics/evidence are still read
    /// from the existing execution projection and terminal stores.
    pub(crate) async fn recoverable_session_execution_index(
        &self,
        session_id: &str,
    ) -> SessionExecutionIndexProjection {
        let volatile = self.session_execution_index(session_id);
        let Some(store) = self.session_kernel.unified_store() else {
            return volatile;
        };
        let Ok(records) = store
            .session_runtime_outbox_for_session(session_id, 100)
            .await
        else {
            return volatile;
        };
        let durable = session_execution_index_from_outbox(session_id, &records);
        match (volatile.last_progress_at_ms, durable.last_progress_at_ms) {
            (Some(live), Some(persisted)) if live >= persisted => volatile,
            (Some(_), None) => volatile,
            _ => durable,
        }
    }

    pub(crate) async fn recoverable_running_session_execution_indices(
        &self,
    ) -> Vec<SessionExecutionIndexProjection> {
        let mut session_ids = self
            .running_session_execution_indices()
            .into_iter()
            .map(|index| index.session_id)
            .collect::<BTreeSet<_>>();
        if let Some(store) = self.session_kernel.unified_store() {
            if let Ok(records) = store.active_session_runtime_outbox(500).await {
                session_ids.extend(records.into_iter().map(|record| record.session_id));
            }
        }
        let mut indices = Vec::with_capacity(session_ids.len());
        for session_id in session_ids {
            let index = self.recoverable_session_execution_index(&session_id).await;
            if !index.active_execution_ids.is_empty() {
                indices.push(index);
            }
        }
        indices
    }

    fn record_live_execution(&self, session_id: &str, execution_id: String, turn_id: String) {
        self.runtime_services
            .record_live_execution(session_id, execution_id, turn_id);
    }

    fn complete_live_execution(
        &self,
        execution_id: &str,
        report: &ContextTurnReport,
        terminal_ref: String,
    ) {
        self.runtime_services
            .complete_live_execution(execution_id, report, terminal_ref);
    }

    fn fail_live_execution(&self, execution_id: &str, error: String) {
        self.runtime_services
            .fail_live_execution(execution_id, error);
    }

    fn install_active_turn_control(
        &self,
        turn_id: &str,
        session_id: &str,
        execution_id: Option<String>,
    ) -> (runtime::CancellationToken, ActiveTurnControlGuard) {
        let cancellation_token = runtime::CancellationToken::new();
        self.active_turn_controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                turn_id.to_string(),
                ActiveTurnControl {
                    session_id: session_id.to_string(),
                    execution_id,
                    cancellation_token: cancellation_token.clone(),
                },
            );
        (
            cancellation_token,
            ActiveTurnControlGuard {
                turn_id: turn_id.to_string(),
                controls: Arc::clone(&self.active_turn_controls),
            },
        )
    }

    fn cancel_active_turn_control(&self, turn_id: &str, reason: &str) -> Option<String> {
        let control = self
            .active_turn_controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(turn_id)
            .cloned()?;
        control.cancellation_token.cancel();
        if let Some(execution_id) = &control.execution_id {
            self.runtime_services
                .cancel_live_execution(execution_id, reason.to_string());
        }
        Some(control.execution_id.unwrap_or_else(|| turn_id.to_string()))
    }

    fn cancel_active_session_turns(&self, session_id: &str, reason: &str) -> Vec<String> {
        let turn_ids = self
            .active_turn_controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(_, control)| control.session_id == session_id)
            .map(|(turn_id, _)| turn_id.clone())
            .collect::<Vec<_>>();
        turn_ids
            .into_iter()
            .filter_map(|turn_id| self.cancel_active_turn_control(&turn_id, reason))
            .collect()
    }

    pub(crate) fn refresh_resource_capabilities(&self) -> runtime::ResourceCapabilitySnapshot {
        self.resource_capabilities.refresh_from_environment()
    }

    pub(crate) async fn execute_ingress_record(
        &self,
        record: &memory::SessionRuntimeOutboxRecord,
        content: &str,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        let terminal_id = format!("turn-terminal:{}", record.request_id);
        let graph_id = runtime::session_ingress_graph_id(
            &record.session_id,
            &record.request_id,
            &record.turn_id,
        );
        if let Some(terminal) = self
            .runtime_services
            .session_terminal_delivery()
            .get(&terminal_id)
            .map_err(|error| error.to_string())?
        {
            if let Some(resolution) = self.session_input_router.record_target_terminal(
                record,
                &graph_id,
                terminal.commit_cursor,
            )? {
                self.runtime_services
                    .resolve_session_handoff_result(resolution)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            return Ok(runtime::SessionIngressExecutionReceipt {
                graph_id,
                commit_cursor: terminal.commit_cursor,
            });
        }
        if let Ok(projection) = self
            .runtime_services
            .graph_state_store()
            .projection(&graph_id)
        {
            if projection
                .nodes
                .iter()
                .all(|node| node.status.is_terminal())
            {
                return Err(format!(
                    "ingress graph {graph_id} is terminal without its durable session receipt"
                ));
            }
        }
        if !self.has_active_session(&record.session_id) {
            let activator = self
                .session_activator
                .get()
                .and_then(Weak::upgrade)
                .ok_or_else(|| {
                    format!(
                        "session {} requires UnifiedSessionManager activation, but no activation port is installed",
                        record.session_id
                    )
                })?;
            activator.activate(&record.session_id).await?;
        }
        let runtime_entry = self
            .sessions
            .get(&record.session_id)
            .ok_or_else(|| format!("session {} has no active runtime", record.session_id))?;
        let ingress = runtime::TurnIngressRef {
            request_id: record.request_id.clone(),
            turn_id: record.turn_id.clone(),
            message_id: record.message_id.clone(),
            session_id: record.session_id.clone(),
        };
        self.record_live_execution(&record.session_id, graph_id.clone(), record.turn_id.clone());
        let ingress_options = match record
            .runtime_options_json
            .as_deref()
            .map(serde_json::from_str::<IngressRuntimeOptions>)
            .transpose()
        {
            Ok(options) => options.unwrap_or_default(),
            Err(error) => {
                let error = format!("invalid persisted ingress runtime options: {error}");
                self.fail_live_execution(&graph_id, error.clone());
                return Err(error);
            }
        };
        let (cancellation_token, _turn_control) = self.install_active_turn_control(
            &record.turn_id,
            &record.session_id,
            Some(graph_id.clone()),
        );
        let mut owned_runtime = match async {
            let mut runtime = runtime_entry.lock().await;
            runtime.set_context_profile(ingress_options.profile);
            for message in ingress_options.pre_messages {
                runtime
                    .append_external_message(message.into_conversation_message())
                    .await
                    .map_err(|error| error.to_string())?;
            }
            runtime.install_turn_control(
                cancellation_token.clone(),
                runtime::HookAbortSignal::default(),
            );
            let host = runtime
                .take_runtime_for_turn()
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(RuntimeTurnOwner::new(Arc::clone(&runtime_entry), host))
        }
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                self.fail_live_execution(&graph_id, error.clone());
                return Err(error);
            }
        };
        let summary_result = owned_runtime
            .runtime_mut()
            .submit_ingress_turn(
                content,
                &runtime::permissions::SharedPrompter::none(),
                ingress,
            )
            .await;
        owned_runtime.restore().await;
        let summary = summary_result.map_err(|error| {
            if cancellation_token.is_cancelled() {
                self.runtime_services.cancel_live_execution(
                    &graph_id,
                    "cancelled while Runtime turn was running".to_string(),
                );
            } else {
                self.fail_live_execution(&graph_id, error.to_string());
            }
            error.to_string()
        })?;
        let terminal = self
            .runtime_services
            .session_terminal_delivery()
            .get(&terminal_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("runtime committed no terminal for {}", record.request_id))?;
        self.complete_live_execution(&graph_id, &summary.context_turn_report, terminal_id.clone());
        if let Some(resolution) = self.session_input_router.record_target_terminal(
            record,
            &graph_id,
            terminal.commit_cursor,
        )? {
            self.runtime_services
                .resolve_session_handoff_result(resolution)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(runtime::SessionIngressExecutionReceipt {
            graph_id,
            commit_cursor: terminal.commit_cursor,
        })
    }

    #[must_use]
    pub(crate) fn with_tool_host(mut self, tool_host: Arc<tools::ToolHost>) -> Self {
        self.tool_host = tool_host;
        self
    }

    #[must_use]
    pub(crate) fn status_value(&self) -> serde_json::Value {
        let status = self.status();
        serde_json::json!({
            "ok": true,
            "protocol_version": status.protocol_version,
            "runtime_host": status.runtime_host,
            "active_sessions": status.active_sessions,
            "uptime_secs": status.uptime_secs,
        })
    }

    #[must_use]
    pub(crate) fn session_kernel(&self) -> Arc<SessionKernel> {
        self.session_kernel.clone()
    }

    #[must_use]
    pub(crate) fn lifecycle_kernel(&self) -> Arc<SessionLifecycleKernel> {
        self.lifecycle_kernel.clone()
    }

    #[must_use]
    pub(crate) fn provider_registry(&self) -> Arc<runtime::ProviderRegistry> {
        Arc::clone(&self.provider_registry)
    }

    #[must_use]
    pub(crate) fn upgrade_coordinator(&self) -> Arc<runtime::UpgradeCoordinator> {
        Arc::clone(&self.upgrade_coordinator)
    }

    #[must_use]
    pub(crate) fn config_reload(
        &self,
    ) -> Arc<crate::runtime_host::config_reload::ConfigReloadState> {
        Arc::clone(&self.config_reload)
    }

    #[must_use]
    pub(crate) fn tool_host(&self) -> Arc<tools::ToolHost> {
        Arc::clone(&self.tool_host)
    }

    #[must_use]
    pub(crate) fn runtime_services(&self) -> Arc<runtime::RuntimeServices> {
        Arc::clone(&self.runtime_services)
    }

    #[must_use]
    pub(crate) fn status(&self) -> RuntimeBoundaryStatus {
        RuntimeBoundaryStatus {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: self.sessions.list().len(),
            uptime_secs: self.clock().uptime_secs(),
        }
    }

    pub(crate) async fn snapshot_value(&self) -> serde_json::Value {
        let snapshot = self.snapshot().await;
        let leases = self.lease_registry.list().await;
        let turns = self.turns_snapshot();
        let turn_bindings = self.turn_bindings_snapshot();
        serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "protocol_version": snapshot.protocol_version,
            "runtime_host": snapshot.runtime_host,
            "active_sessions": snapshot.active_sessions,
            "uptime_secs": snapshot.uptime_secs,
            "sessions": snapshot.sessions,
            "leases": {
                "total": leases.len(),
                "items": leases,
            },
            "lifecycle": self.lifecycle_kernel.snapshots().await,
            "turns": turns,
            "turn_bindings": turn_bindings,
            "transport": {
                "control": "gateway_http",
                "projection": "http_optional",
            },
        })
    }

    pub(crate) async fn submit_turn_value(
        &self,
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> serde_json::Value {
        if !self.upgrade_coordinator.accepts_new_work() {
            return serde_json::json!({
                "ok": false,
                "error": "runtime_maintenance",
                "message": "runtime is in upgrade maintenance mode and rejects new turns",
            });
        }
        if prompt.trim().is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "prompt is required",
            });
        }

        let input = Self::turn_input_for(session_id, task_id, prompt);
        let receipt = self.record_turn_from_input(&input, TurnStatus::Pending);
        let turn_id = input.turn_id.to_string();
        let journal_sequence = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Submitted, None)
            .await
            .transpose()
            .map_err(|error| {
                tracing::warn!(
                    turn_id = %turn_id,
                    error = %error,
                    "failed to persist submitted turn journal"
                );
                error
            })
            .ok()
            .flatten();

        serde_json::json!({
            "ok": true,
            "dispatch": "runtime_service",
            "accepted": true,
            "durable_journal": journal_sequence.is_some(),
            "journal_sequence": journal_sequence,
            "turn": receipt,
        })
    }

    pub(crate) fn upgrade_runtime_carriers(&self) -> Vec<runtime::UpgradeCarrierRecord> {
        let mut carriers = self
            .turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|receipt| {
                matches!(
                    receipt.status,
                    TurnStatus::Pending
                        | TurnStatus::Running
                        | TurnStatus::PendingApproval
                        | TurnStatus::Resuming
                )
            })
            .map(|receipt| {
                let payload = serde_json::to_vec(receipt).unwrap_or_default();
                runtime::UpgradeCarrierRecord {
                    carrier_kind: "active_turn".to_string(),
                    carrier_id: receipt.turn_id.to_string(),
                    status: match receipt.status {
                        TurnStatus::Pending => runtime::UpgradeCarrierStatus::Ready,
                        TurnStatus::Running | TurnStatus::Resuming => {
                            runtime::UpgradeCarrierStatus::Running
                        }
                        TurnStatus::PendingApproval => runtime::UpgradeCarrierStatus::Waiting,
                        _ => runtime::UpgradeCarrierStatus::Completed,
                    },
                    revision: receipt.events.len() as u64,
                    result_ref: receipt.context_report_id.clone(),
                    state_ref: receipt
                        .session_id
                        .as_ref()
                        .map(|session_id| format!("session://{session_id}")),
                    state_hash: format!(
                        "{:016x}",
                        model_protocol::prompt_cache::stable_hash_bytes(&payload)
                    ),
                }
            })
            .collect::<Vec<_>>();

        carriers.extend(
            self.runtime_services()
                .agent_runtime()
                .list()
                .into_iter()
                .map(|snapshot| {
                    let status = upgrade_agent_status(&snapshot.status);
                    upgrade_carrier_record(
                        "agent",
                        snapshot.agent_id.clone(),
                        status,
                        snapshot.revision,
                        snapshot.failure.clone(),
                        Some(format!(
                            "graph://{}/node/{}",
                            snapshot.graph_id, snapshot.node_id
                        )),
                        &snapshot,
                    )
                }),
        );
        carriers.extend(
            self.runtime_services()
                .team_runtime()
                .list()
                .unwrap_or_default()
                .into_iter()
                .map(|snapshot| {
                    let status = upgrade_team_status(snapshot.status.as_str());
                    upgrade_carrier_record(
                        "team",
                        snapshot.team_id.clone(),
                        status,
                        snapshot.graph_revision,
                        snapshot
                            .terminal_result
                            .as_ref()
                            .map(|result| result.result_ref.clone()),
                        Some(format!(
                            "mission://session/{}/team/{}",
                            snapshot.session_id, snapshot.team_id
                        )),
                        &snapshot,
                    )
                }),
        );
        carriers.extend(
            self.runtime_services
                .mission_runtime()
                .projection(
                    self.runtime_services.session_relations(),
                    self.runtime_services.agent_runtime(),
                    self.runtime_services.team_runtime(),
                    self.runtime_services.approval_queue(),
                    self.runtime_services.conflict_resolver(),
                    self.runtime_services.mission_evidence(),
                    self.runtime_services.mission_schedules().projection(),
                )
                .sessions
                .into_iter()
                .map(|snapshot| {
                    let status = upgrade_mission_status(&snapshot.status);
                    upgrade_carrier_record(
                        "mission_session",
                        snapshot.session_id.clone(),
                        status,
                        snapshot.updated_at_ms,
                        None,
                        Some(format!("mission://session/{}", snapshot.session_id)),
                        &snapshot,
                    )
                }),
        );
        carriers.sort_by(|left, right| {
            (&left.carrier_kind, &left.carrier_id).cmp(&(&right.carrier_kind, &right.carrier_id))
        });
        carriers
    }

    pub(crate) fn upgrade_turn_carriers(&self) -> Vec<runtime::UpgradeCarrierRecord> {
        self.upgrade_runtime_carriers()
    }

    pub(crate) fn turn_value(&self, turn_id: &str) -> serde_json::Value {
        let turns = self
            .turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match turns.get(turn_id) {
            Some(turn) => serde_json::json!({
                "ok": true,
                "turn": turn,
            }),
            None => serde_json::json!({
                "ok": false,
                "error": "turn not found",
            }),
        }
    }

    pub(crate) fn turns_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "turns": self.turns_snapshot(),
            "turn_bindings": self.turn_bindings_snapshot(),
        })
    }

    pub(crate) async fn cancel_turn_value(&self, turn_id: &str) -> serde_json::Value {
        let aborted_run_id = self.cancel_active_turn_control(turn_id, "cancelled by operator");
        let turn = {
            let mut turns = self
                .turns
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(turn) = turns.get_mut(turn_id) else {
                if let Some(aborted_run_id) = aborted_run_id {
                    return serde_json::json!({
                        "ok": true,
                        "cancelled": true,
                        "aborted_run_id": aborted_run_id,
                        "turn": null,
                    });
                }
                return serde_json::json!({
                    "ok": false,
                    "error": "turn not found",
                });
            };

            turn.status = TurnStatus::Cancelled;
            turn.events.push(TurnEvent::new(
                TurnId::from_string(turn_id.to_string()),
                TurnStatus::Cancelled,
            ));
            turn.clone()
        };
        let journal_sequence = self
            .persist_turn_receipt_journal(&turn, TurnJournalPhase::Cancelled, None)
            .await
            .transpose()
            .map_err(|error| {
                tracing::warn!(
                    turn_id = %turn_id,
                    error = %error,
                    "failed to persist cancelled turn journal"
                );
                error
            })
            .ok()
            .flatten();

        serde_json::json!({
            "ok": true,
            "cancelled": true,
            "aborted_run_id": aborted_run_id.unwrap_or_else(|| turn_id.to_string()),
            "journal_sequence": journal_sequence,
            "turn": turn,
        })
    }

    async fn persist_turn_input_journal(
        &self,
        input: &TurnInput,
        phase: TurnJournalPhase,
        message: Option<String>,
    ) -> Option<Result<Option<usize>, memory::MemoryError>> {
        let session_id = input
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())?;
        let envelope = TurnJournalEnvelope::new(
            session_id,
            input.turn_id.clone(),
            phase,
            "gateway.runtime_service",
            serde_json::json!({
                "status": phase.as_str(),
                "prompt": input.prompt.clone(),
                "prompt_preview": input.prompt.chars().take(240).collect::<String>(),
                "task_id": input.task_id.clone(),
                "message": message,
                "created_at": input.created_at,
            }),
        );
        Some(
            self.session_kernel
                .append_turn_journal_event(session_id, envelope)
                .await,
        )
    }

    async fn persist_turn_receipt_journal(
        &self,
        receipt: &TurnReceipt,
        phase: TurnJournalPhase,
        message: Option<String>,
    ) -> Option<Result<Option<usize>, memory::MemoryError>> {
        let session_id = receipt
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())?;
        let envelope = TurnJournalEnvelope::new(
            session_id,
            receipt.turn_id.clone(),
            phase,
            "gateway.runtime_service",
            serde_json::json!({
                "status": receipt.status.as_str(),
                "task_id": receipt.task_id.clone(),
                "context_report_id": receipt.context_report_id.clone(),
                "message": message,
                "completed_at": receipt.completed_at,
            }),
        );
        Some(
            self.session_kernel
                .append_turn_journal_event(session_id, envelope)
                .await,
        )
    }

    fn turn_input_for(
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> TurnInput {
        let mut input = TurnInput::new(prompt);
        input.session_id = session_id;
        input.task_id = task_id;
        input
    }

    fn record_turn_from_input(&self, input: &TurnInput, status: TurnStatus) -> TurnReceipt {
        let mut receipt = TurnReceipt::from_input(input, status.clone());
        receipt
            .events
            .push(TurnEvent::new(input.turn_id.clone(), status));
        self.record_turn_binding(input);
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), receipt.clone());
        receipt
    }

    fn turns_snapshot(&self) -> Vec<TurnReceipt> {
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn turn_bindings_snapshot(&self) -> Vec<TaskTurnBinding> {
        self.turn_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn has_active_session(&self, session_id: &str) -> bool {
        self.sessions.get(session_id).is_some()
    }

    /// Lazily activate a persisted session for a new ingress turn. Persisted
    /// session identity remains owned by UnifiedSessionStore; this only adds
    /// the process-local runtime required to execute work.
    pub(crate) async fn activate_persisted_session(
        &self,
        session_id: &str,
        model_hint: Option<&str>,
        system_prompt: Vec<String>,
    ) -> Result<(), String> {
        if self.has_active_session(session_id) {
            return Ok(());
        }
        let stored_model = self
            .session_kernel
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .and_then(|record| record.model)
            .filter(|model| !model.trim().is_empty());
        let model = model_hint
            .filter(|model| !model.trim().is_empty())
            .map(ToOwned::to_owned)
            .or(stored_model)
            .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string());
        let runtime = self.build_session_runtime_entry(session_id, &model, system_prompt)?;
        self.register_runtime(session_id.to_string(), runtime)?;
        Ok(())
    }

    pub(crate) fn register_runtime(
        &self,
        session_id: String,
        mut runtime: crate::runtime_entry::GatewayRuntimeEntry,
    ) -> Result<Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>>, String>
    {
        if let Some(approval_gate) = &self.approval_gate {
            runtime.install_approval_gate(Arc::clone(approval_gate));
        }
        let input_stream = runtime.session_input_stream();
        let cowd_bus = runtime.cowd_bus().cloned();
        let model = runtime
            .session()
            .model
            .filter(|model| !model.trim().is_empty());
        let result = self.sessions.register(session_id.clone(), runtime);
        if result.is_ok() {
            self.session_inputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(session_id.clone(), input_stream);
            if let Some(model) = model {
                self.session_models
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(session_id.clone(), model);
            } else {
                self.session_models
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&session_id);
            }
            if let Some(cowd_bus) = cowd_bus {
                self.session_event_buses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(session_id.clone(), cowd_bus.clone());
                self.install_session_event_relay(&session_id, cowd_bus);
            } else {
                self.session_event_buses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&session_id);
            }
        }
        result
    }

    pub(crate) fn remove_active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>> {
        self.session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.session_event_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        if let Some(handle) = self
            .session_event_relays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id)
        {
            handle.abort();
        }
        self.session_models
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.sessions.remove(session_id)
    }

    /// Runtime emits transient rendering/progress events on its own bus while
    /// Gateway owns the cross-surface transport. Relay them once per active
    /// session so every surface observes the same stream. Durable terminal
    /// settlement is deliberately emitted by `SessionRuntimeBridge` only
    /// after the transcript append succeeds.
    fn install_session_event_relay(&self, session_id: &str, bus: runtime::CowdEventBus) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // Unit-only construction outside Tokio has no live surface to
            // relay to; production runtime registration always has a handle.
            return;
        };
        if let Some(handle) = self
            .session_event_relays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id)
        {
            handle.abort();
        }
        let session_id = session_id.to_string();
        let relay_session_id = session_id.clone();
        let gateway_bus = self.session_kernel.event_bus();
        let runtime_services = Arc::clone(&self.runtime_services);
        let mut receiver = bus.subscribe();
        let handle = runtime.spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        runtime_services.observe_live_execution_event(&relay_session_id, &event);
                        gateway_bus
                            .broadcast(
                                &relay_session_id,
                                &runtime_event_stream_payload(event).to_string(),
                            )
                            .await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        gateway_bus
                            .broadcast(
                                &relay_session_id,
                                &serde_json::json!({
                                    "type": "RuntimeStreamLagged",
                                    "skipped": skipped,
                                })
                                .to_string(),
                            )
                            .await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        self.session_event_relays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id, handle);
    }

    pub(crate) fn remove_active_runtime_if_present(&self, session_id: &str) -> bool {
        self.remove_active_runtime(session_id).is_some()
    }

    pub(crate) async fn cowd_event_receiver(
        &self,
        session_id: &str,
    ) -> Option<tokio::sync::broadcast::Receiver<runtime::CowdEvent>> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        runtime_guard.cowd_bus().map(|bus| bus.subscribe())
    }

    pub(crate) async fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
    ) -> Result<SessionInputReceipt, RuntimeTurnExecutionError> {
        self.admit_session_input_with_materialized(envelope)
            .await
            .map(|admission| admission.receipt)
    }

    pub(crate) async fn route_pending_session_inputs(
        &self,
        limit: usize,
    ) -> Result<runtime::SessionInputRouteReport, RuntimeTurnExecutionError> {
        self.session_input_router
            .route_pending_with(self, limit.max(1))
            .await
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))
    }

    pub(crate) async fn admit_session_input_with_materialized(
        &self,
        envelope: SessionInputEnvelope,
    ) -> Result<SessionInputAdmission, RuntimeTurnExecutionError> {
        let session_id = envelope.session_id.clone();
        let content = envelope.content.clone();
        let request = memory::SessionRuntimeOutboxRequest {
            request_id: envelope.idempotency_key.clone(),
            turn_id: envelope
                .metadata
                .get("turn_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| envelope.input_id.to_string()),
            message_id: envelope
                .source_message_id
                .clone()
                .unwrap_or_else(|| envelope.input_id.to_string()),
            created_at_ms: envelope.created_at.timestamp_millis().max(0) as u64,
            runtime_options_json: envelope
                .metadata
                .get("runtime_options")
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?,
        };
        self.session_input_router
            .persist_input(&session_id, &content, &request)
            .await
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
        let stream = self.session_input_stream_for(&session_id).await?;
        let receipt = stream.admit(envelope, stream.runtime_state());
        let record_for_event = stream.record_snapshot(&receipt.input_id);
        self.emit_session_input_events(&session_id, &stream, Some(receipt.clone()));
        self.persist_session_input_domain_event(
            &session_id,
            "SessionInputReceived",
            Some(&receipt),
            record_for_event.as_ref(),
            &stream,
            None,
        )
        .await;
        let execution_graph_id =
            runtime::session_ingress_graph_id(&session_id, &request.request_id, &request.turn_id);
        self.record_live_execution(
            &session_id,
            execution_graph_id.clone(),
            request.turn_id.clone(),
        );
        Ok(SessionInputAdmission {
            execution_graph_id,
            receipt,
            materialized: None,
        })
    }

    pub(crate) async fn cancel_session_input(
        &self,
        session_id: &str,
        input_id: SessionInputId,
        reason: &str,
    ) -> Result<SessionInputReceipt, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        let record = stream
            .cancel_input(&input_id, reason)
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
        let receipt = record.to_receipt();
        self.emit_session_input_events(session_id, &stream, Some(receipt.clone()));
        self.persist_session_input_domain_event(
            session_id,
            "SessionInputCancelled",
            Some(&receipt),
            Some(&record),
            &stream,
            None,
        )
        .await;
        let cancelled_execution_ids = self.cancel_active_session_turns(session_id, reason);
        if !cancelled_execution_ids.is_empty() {
            tracing::info!(
                session_id,
                execution_ids = ?cancelled_execution_ids,
                "cancelled active Runtime execution(s) for session input"
            );
        }
        Ok(receipt)
    }

    pub(crate) async fn reclassify_session_input(
        &self,
        session_id: &str,
        input_id: SessionInputId,
        decision: InputRoutingDecision,
        reason: &str,
    ) -> Result<SessionInputReceipt, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        let record = stream
            .reclassify_input(&input_id, decision, reason)
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
        let receipt = record.to_receipt();
        let graph_materialized = Some(
            serde_json::to_value(
                self.session_input_router
                    .route_pending_with(self, 32)
                    .await
                    .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?,
            )
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?,
        );
        let materialized = graph_materialized;
        let materialized_for_event = materialized.clone();
        self.emit_session_input_events(session_id, &stream, Some(receipt.clone()));
        if let Some(materialized) = materialized {
            self.emit_session_input_materialized(session_id, materialized);
        }
        self.persist_session_input_domain_event(
            session_id,
            "SessionInputReclassified",
            Some(&receipt),
            Some(&record),
            &stream,
            materialized_for_event.as_ref(),
        )
        .await;
        Ok(receipt)
    }

    pub(crate) fn build_session_runtime_entry(
        &self,
        session_id: &str,
        model: &str,
        system_prompt: Vec<String>,
    ) -> Result<crate::runtime_entry::GatewayRuntimeEntry, String> {
        let session = runtime::Session::new();
        if let Some(store) = self.session_kernel.unified_store() {
            crate::runtime_factory::create_runtime_entry_with_session_store(
                store,
                self.runtime_services(),
                self.provider_registry(),
                self.tool_host(),
                session,
                session_id,
                model.to_string(),
                system_prompt,
                true,
                true,
                None,
                runtime::PermissionMode::WorkspaceWrite,
                None,
                None,
            )
            .map_err(|error| error.to_string())
        } else {
            crate::runtime_factory::create_runtime_entry(
                self.runtime_services(),
                self.provider_registry(),
                self.tool_host(),
                session,
                session_id,
                model.to_string(),
                system_prompt,
                true,
                true,
                None,
                runtime::PermissionMode::WorkspaceWrite,
                None,
                None,
            )
            .map_err(|error| error.to_string())
        }
    }

    fn emit_session_input_materialized(&self, session_id: &str, materialized: serde_json::Value) {
        let Some(bus) = self
            .session_event_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        else {
            return;
        };
        bus.emit(runtime::CowdEvent::Warning {
            message: format!("session input graph materialized: {materialized}"),
        });
    }

    pub(crate) async fn session_input_projection(
        &self,
        session_id: &str,
    ) -> Result<SessionInputProjection, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        Ok(stream.projection())
    }

    pub(crate) async fn active_turn_inbox(
        &self,
        session_id: &str,
        turn_id: Option<TurnId>,
    ) -> Result<TurnInboxSnapshot, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        Ok(stream.inbox_snapshot(turn_id))
    }

    async fn session_input_stream_for(
        &self,
        session_id: &str,
    ) -> Result<runtime::SessionInputStream, RuntimeTurnExecutionError> {
        if let Some(stream) = self
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        {
            return Ok(stream);
        }
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let runtime_guard = runtime_entry.lock().await;
        if runtime_guard.turn_is_owned() {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "session {session_id} is executing before its input stream was initialized"
            )));
        }
        let stream = runtime_guard.session_input_stream();
        self.session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_string(), stream.clone());
        if let Some(bus) = runtime_guard.cowd_bus().cloned() {
            self.session_event_buses
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(session_id.to_string(), bus);
        }
        Ok(stream)
    }

    fn emit_session_input_events(
        &self,
        session_id: &str,
        stream: &runtime::SessionInputStream,
        receipt: Option<SessionInputReceipt>,
    ) {
        let Some(bus) = self
            .session_event_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        else {
            return;
        };
        if let Some(receipt) = receipt {
            bus.emit(runtime::CowdEvent::SessionInputReceived { receipt });
        }
        bus.emit(runtime::CowdEvent::SessionInputProjection {
            projection: stream.projection(),
        });
        bus.emit(runtime::CowdEvent::TurnInboxUpdated {
            inbox: stream.inbox_snapshot(None),
        });
    }

    async fn persist_session_input_domain_event(
        &self,
        session_id: &str,
        kind: &str,
        receipt: Option<&SessionInputReceipt>,
        record: Option<&runtime::SessionInputRecord>,
        stream: &runtime::SessionInputStream,
        materialized: Option<&serde_json::Value>,
    ) {
        if let Err(error) = self.ensure_session_domain_record(session_id).await {
            tracing::warn!(
                %session_id,
                %kind,
                error = %error,
                "failed to ensure session before persisting session input runtime event"
            );
            return;
        }
        if let Err(error) = self
            .session_kernel
            .append_session_domain_event(
                session_id,
                memory::SessionDomainScope::Turn,
                kind,
                serde_json::json!({
                    "input": receipt,
                    "record": record,
                    "input_projection": stream.projection(),
                    "turn_inbox": stream.inbox_snapshot(None),
                    "materialized": materialized,
                }),
            )
            .await
        {
            tracing::warn!(
                %session_id,
                %kind,
                error = %error,
                "failed to persist session input runtime event"
            );
        }
    }

    async fn ensure_session_domain_record(
        &self,
        session_id: &str,
    ) -> Result<(), memory::MemoryError> {
        if self
            .session_kernel
            .stored_session(session_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        Err(memory::MemoryError::NotFound(format!(
            "session {session_id} must be created through UnifiedSessionManager before runtime events are persisted"
        )))
    }

    pub(crate) async fn configure_turn_context(
        &self,
        session_id: &str,
        profile: runtime::ContextProfile,
        resume_context: Option<runtime::ResumeContextPacket>,
        reality_context_items: Vec<runtime::ContextItem>,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.set_context_profile(profile);
        runtime_guard.replace_external_context_sources(
            &[
                runtime::ContextSourceKind::Fact,
                runtime::ContextSourceKind::Matrix,
            ],
            reality_context_items,
        );
        if let Some(packet) = resume_context {
            runtime_guard.inject_resume_context(packet);
        }
        Ok(())
    }

    pub(crate) async fn install_turn_control(
        &self,
        session_id: &str,
        cancellation_token: runtime::CancellationToken,
        hook_abort_signal: runtime::HookAbortSignal,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let mut runtime_guard = runtime_entry.lock().await;
        runtime_guard.install_turn_control(cancellation_token, hook_abort_signal);
        Ok(())
    }

    pub(crate) async fn run_turn(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        self.run_turn_with_options(session_id, task_id, content, RuntimeTurnOptions::default())
            .await
    }

    pub(crate) async fn run_turn_with_options(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
        options: RuntimeTurnOptions,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        let receipt = self
            .accept_turn_with_options(session_id, task_id, content.clone())
            .await?;
        self.run_accepted_turn_with_options(session_id, receipt.turn_id.clone(), content, options)
            .await
    }

    pub(crate) async fn accept_turn_with_options(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
    ) -> Result<TurnReceipt, RuntimeTurnExecutionError> {
        if self.sessions.get(session_id).is_none() {
            return Err(RuntimeTurnExecutionError::NotFound(format!(
                "session {session_id} not found"
            )));
        }
        let input = Self::turn_input_for(Some(session_id.to_string()), task_id, content);
        self.record_turn_from_input(&input, TurnStatus::Pending);
        if let Some(Err(error)) = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Submitted, None)
            .await
        {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "failed to persist submitted turn journal: {error}"
            )));
        }
        let receipt = self.record_turn_from_input(&input, TurnStatus::Running);
        if let Some(Err(error)) = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Running, None)
            .await
        {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "failed to persist running turn journal: {error}"
            )));
        }
        if let Ok(stream) = self.session_input_stream_for(session_id).await {
            stream.set_active_turn(Some(receipt.turn_id.clone()));
            self.emit_session_input_events(session_id, &stream, None);
        }
        Ok(receipt)
    }

    pub(crate) async fn run_accepted_turn_with_options(
        &self,
        session_id: &str,
        turn_id: TurnId,
        content: String,
        options: RuntimeTurnOptions,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let queued_next_options = options.clone();
        let (cancellation_token, _turn_control) =
            self.install_active_turn_control(&turn_id.to_string(), session_id, None);
        let mut owned_runtime = {
            let mut runtime_guard = runtime_entry.lock().await;
            runtime_guard.set_context_profile(options.profile);
            for message in options.pre_messages {
                runtime_guard
                    .append_external_message(message)
                    .await
                    .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
            }
            runtime_guard.install_turn_control(
                cancellation_token.clone(),
                runtime::HookAbortSignal::default(),
            );
            let host = runtime_guard
                .take_runtime_for_turn()
                .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
            RuntimeTurnOwner::new(Arc::clone(&runtime_entry), host)
        };
        // Do not hold `GatewayRuntimeEntry`'s mutex while a provider/tool turn
        // awaits.  The host returns to the entry before this method settles
        // the receipt, so the next turn still observes a single owner.
        let turn_result = owned_runtime
            .runtime_mut()
            .submit_turn(&content, &runtime::permissions::SharedPrompter::none())
            .await;
        owned_runtime.restore().await;

        match turn_result {
            Ok(summary) => {
                let mut receipt = self.finish_turn(&turn_id, TurnStatus::Completed, None);
                receipt.context_report_id = Some(summary.context_turn_report.turn_id.clone());
                self.turns
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(turn_id.to_string(), receipt.clone());
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(&receipt, TurnJournalPhase::Completed, None)
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist completed turn journal"
                    );
                }
                if let Some(stream) = self
                    .session_inputs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(session_id)
                    .cloned()
                {
                    let queued_next = stream.drain_queued_next_for_dispatch(4);
                    self.emit_session_input_events(session_id, &stream, None);
                    self.dispatch_queued_next_turns(
                        session_id.to_string(),
                        queued_next,
                        queued_next_options,
                    );
                }
                Ok(RuntimeTurnExecution { summary, receipt })
            }
            Err(error) => {
                let message = error.to_string();
                self.clear_session_input_turn_if_current(session_id, &turn_id);
                let receipt = self.finish_turn(&turn_id, TurnStatus::Failed, Some(message.clone()));
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(
                        &receipt,
                        TurnJournalPhase::Failed,
                        Some(message.clone()),
                    )
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist failed turn journal"
                    );
                }
                Err(RuntimeTurnExecutionError::Runtime(message))
            }
        }
    }

    fn clear_session_input_turn_if_current(&self, session_id: &str, turn_id: &TurnId) {
        let Some(stream) = self
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        else {
            return;
        };
        if stream.active_turn_id().as_ref() == Some(turn_id) {
            stream.set_active_turn(None);
            self.emit_session_input_events(session_id, &stream, None);
        }
    }

    fn dispatch_queued_next_turns(
        &self,
        session_id: String,
        records: Vec<runtime::SessionInputRecord>,
        options: RuntimeTurnOptions,
    ) {
        for record in records {
            let service = self.clone();
            let prompt = record.envelope.content.clone();
            let task_id = None;
            let session_id = session_id.clone();
            let options = options.clone();
            tokio::spawn(async move {
                let Ok(receipt) = service
                    .accept_turn_with_options(&session_id, task_id, prompt.clone())
                    .await
                else {
                    return;
                };
                if let Err(error) = service
                    .run_accepted_turn_with_options(
                        &session_id,
                        receipt.turn_id.clone(),
                        prompt,
                        options,
                    )
                    .await
                {
                    tracing::warn!(
                        %session_id,
                        turn_id = %receipt.turn_id,
                        error = %error.message(),
                        "queued next turn failed"
                    );
                }
            });
        }
    }

    fn start_running_turn(
        &self,
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> TurnReceipt {
        let mut input = TurnInput::new(prompt);
        input.session_id = session_id;
        input.task_id = task_id;
        let mut receipt = TurnReceipt::from_input(&input, TurnStatus::Running);
        receipt
            .events
            .push(TurnEvent::new(input.turn_id.clone(), TurnStatus::Running));
        self.record_turn_binding(&input);
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), receipt.clone());
        receipt
    }

    fn record_turn_binding(&self, input: &TurnInput) {
        let Some(task_id) = input
            .task_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        else {
            return;
        };
        let mut binding =
            TaskTurnBinding::new(TaskId::from_string(task_id.clone()), input.turn_id.clone());
        binding.session_id = input.session_id.clone();
        self.turn_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), binding);
    }

    fn finish_turn(
        &self,
        turn_id: &TurnId,
        status: TurnStatus,
        message: Option<String>,
    ) -> TurnReceipt {
        let mut turns = self
            .turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let turn = turns
            .entry(turn_id.to_string())
            .or_insert_with(|| TurnReceipt {
                turn_id: turn_id.clone(),
                status: status.clone(),
                session_id: None,
                task_id: None,
                events: Vec::new(),
                context_report_id: None,
                completed_at: None,
            });

        if turn.status != TurnStatus::Cancelled {
            turn.status = status.clone();
        }
        let mut event = TurnEvent::new(turn_id.clone(), status);
        event.message = message;
        turn.events.push(event);
        turn.completed_at = Some(Utc::now());
        turn.clone()
    }

    pub(crate) async fn session_snapshot(&self, session_id: &str) -> Option<runtime::Session> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        Some(runtime_guard.session_async().await)
    }

    pub(crate) async fn sync_session_snapshot(
        &self,
        session_id: &str,
        session: &runtime::Session,
    ) -> Result<(), memory::MemoryError> {
        self.session_kernel
            .sync_runtime_session_snapshot(session_id, session)
            .await
            .map(|_| ())
    }

    pub(crate) async fn compact_active_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionCompactResult>, memory::MemoryError> {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return Ok(None);
        };

        let mut runtime_guard = runtime_entry.lock().await;
        let (result, session_snapshot) = runtime_guard
            .compact_active_session()
            .await
            .map_err(|error| memory::MemoryError::Compression(error.to_string()))?;
        drop(runtime_guard);

        self.sync_session_snapshot(session_id, &session_snapshot)
            .await?;

        let removed_message_count = result
            .as_ref()
            .map_or(0, |event| event.removed_message_count);
        let summary = session_snapshot
            .compaction
            .as_ref()
            .map_or_else(String::new, |compaction| {
                runtime::format_compact_summary(&compaction.summary)
            });
        Ok(Some(SessionCompactResult {
            session_id: session_id.to_string(),
            compacted: removed_message_count > 0,
            removed_message_count,
            summary,
        }))
    }

    pub(crate) async fn active_session_stats(
        &self,
        session_id: &str,
    ) -> Option<SessionStatsSnapshot> {
        let runtime_entry = self.sessions.get(session_id)?;
        // A long provider/tool turn owns this mutex.  Stats are cumulative
        // history, so callers can fall back to the durable session snapshot
        // instead of queueing behind a running turn.
        let runtime_guard = runtime_entry.try_lock().ok()?;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        let session = runtime_guard.active_session_stats_session();
        let messages = &session.messages;

        let user_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::User)
            .count();
        let assistant_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::Assistant)
            .count();
        let tool_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::Tool)
            .count();

        let input: u32 = messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.input_tokens)
            .sum();
        let output: u32 = messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.output_tokens)
            .sum();

        let mut tool_usage = HashMap::new();
        for message in messages {
            if message.role == runtime::MessageRole::Assistant {
                for block in &message.blocks {
                    if let runtime::ContentBlock::ToolUse { name, .. } = block {
                        *tool_usage.entry(name.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        Some(SessionStatsSnapshot {
            session_id: session_id.to_string(),
            message_count: messages.len(),
            message_counts: SessionMessageCounts {
                user: user_count,
                assistant: assistant_count,
                tool: tool_count,
            },
            tokens: SessionTokenCounts {
                input,
                output,
                total: input + output,
            },
            tool_usage,
            duration_ms: session.updated_at_ms.saturating_sub(session.created_at_ms),
        })
    }

    pub(crate) fn last_context_envelope_nonblocking(
        &self,
        session_id: &str,
    ) -> Option<runtime::ContextEnvelope> {
        let runtime_entry = self.sessions.get(session_id)?;
        let envelope = match runtime_entry.try_lock() {
            Ok(runtime) if !runtime.turn_is_owned() => runtime.last_context_envelope(),
            Err(_) => {
                tracing::debug!(
                    %session_id,
                    "runtime context envelope skipped because active runtime is busy"
                );
                None
            }
            Ok(_) => None,
        };
        envelope
    }

    pub(crate) async fn active_messages_page(
        &self,
        session_id: &str,
        offset: usize,
        from_seq: Option<usize>,
        limit: usize,
    ) -> Option<ActiveMessagesPage> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        let session = runtime_guard.session_async().await;

        let all_messages: Vec<serde_json::Value> = session
            .messages
            .iter()
            .enumerate()
            .map(|(sequence, msg)| {
                let role = match msg.role {
                    runtime::MessageRole::System => "system",
                    runtime::MessageRole::User => "user",
                    runtime::MessageRole::Assistant => "assistant",
                    runtime::MessageRole::Tool => "tool",
                };
                let blocks: Vec<serde_json::Value> = msg
                    .blocks
                    .iter()
                    .map(|block| match block {
                        runtime::ContentBlock::Text { text } => {
                            serde_json::json!({"type": "text", "text": text})
                        }
                        runtime::ContentBlock::Image {
                            media_type,
                            data,
                            source_path,
                        } => {
                            serde_json::json!({
                                "type": "image",
                                "media_type": media_type,
                                "source_path": source_path,
                                "size_bytes": data.len() * 3 / 4,
                            })
                        }
                        runtime::ContentBlock::Thinking {
                            thinking,
                            signature,
                        } => {
                            let mut value =
                                serde_json::json!({"type": "thinking", "thinking": thinking});
                            if let Some(signature) = signature {
                                value["signature"] =
                                    serde_json::Value::String(signature.clone());
                            }
                            value
                        }
                        runtime::ContentBlock::ToolUse { id, name, input } => {
                            serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input})
                        }
                        runtime::ContentBlock::ToolResult {
                            tool_use_id,
                            tool_name,
                            output,
                            is_error,
                        } => {
                            serde_json::json!({"type": "tool_result", "tool_use_id": tool_use_id, "tool_name": tool_name, "output": output, "is_error": is_error})
                        }
                    })
                    .collect();

                let mut value = serde_json::json!({
                    "id": format!("runtime:{session_id}:{sequence}"),
                    "sequence": sequence,
                    "role": role,
                    "blocks": blocks,
                });
                if let Some(usage) = &msg.usage {
                    value["usage"] = serde_json::json!({
                        "input_tokens": usage.input_tokens,
                        "output_tokens": usage.output_tokens,
                        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                        "cache_read_input_tokens": usage.cache_read_input_tokens,
                    });
                }
                value
            })
            .collect();
        let total = all_messages.len();
        let start = from_seq.unwrap_or(offset);
        let messages: Vec<serde_json::Value> =
            all_messages.into_iter().skip(start).take(limit).collect();
        let next_seq = (!messages.is_empty()).then_some(start + messages.len());
        let has_more = next_seq.map(|seq| seq < total).unwrap_or(start < total);

        Some(ActiveMessagesPage {
            session_id: session_id.to_string(),
            messages,
            total,
            offset,
            from_seq,
            next_seq,
            limit,
            has_more,
        })
    }

    pub(crate) async fn update_active_session_model(
        &self,
        session_id: &str,
        model: Option<&str>,
    ) -> bool {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return false;
        };
        let Some(model) = model else {
            return true;
        };
        let mut runtime_guard = runtime_entry.lock().await;
        runtime_guard.update_session_model(model).await;
        self.session_models
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_string(), model.to_string());
        true
    }

    pub(crate) async fn last_context_envelope(
        &self,
        session_id: &str,
    ) -> Option<runtime::ContextEnvelope> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.last_context_envelope()
    }

    pub(crate) async fn last_context_turn_report(
        &self,
        session_id: &str,
    ) -> Option<harness_contract::context::ContextTurnReport> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.last_context_turn_report()
    }

    pub(crate) async fn snapshot(&self) -> RuntimeBoundarySnapshot {
        let mut session_ids = self.sessions.list();
        session_ids.sort();
        RuntimeBoundarySnapshot {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: session_ids.len(),
            uptime_secs: self.clock().uptime_secs(),
            sessions: session_ids,
        }
    }

    #[must_use]
    pub(crate) fn list_sessions_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "sessions": self.sessions.list(),
        })
    }

    pub(crate) async fn acquire_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
        mode: &str,
    ) -> serde_json::Value {
        self.lease_registry.acquire(session_id, owner, mode).await
    }

    pub(crate) async fn release_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
    ) -> serde_json::Value {
        self.lease_registry.release(session_id, owner).await
    }

    #[must_use]
    pub(crate) fn unsupported_protocol_value(request: &RuntimeRequest) -> serde_json::Value {
        let response = RuntimeResponse::unsupported_protocol(request);
        let message = response
            .error
            .as_ref()
            .map(|error| error.message.clone())
            .unwrap_or_else(|| "unsupported runtime protocol version".to_string());
        serde_json::json!({
            "ok": false,
            "protocol_version": crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            "request_id": response.request_id,
            "error": message,
            "error_kind": RuntimeErrorKind::UnsupportedProtocol,
            "retryable": false,
        })
    }

    fn clock(&self) -> RuntimeBoundaryClock {
        RuntimeBoundaryClock::from_uptime(self.started_at.elapsed())
    }
}

fn upgrade_carrier_record(
    carrier_kind: &str,
    carrier_id: String,
    status: runtime::UpgradeCarrierStatus,
    revision: u64,
    result_ref: Option<String>,
    state_ref: Option<String>,
    state: &impl serde::Serialize,
) -> runtime::UpgradeCarrierRecord {
    let payload = serde_json::to_vec(state).unwrap_or_default();
    runtime::UpgradeCarrierRecord {
        carrier_kind: carrier_kind.to_string(),
        carrier_id,
        status,
        revision,
        result_ref,
        state_ref,
        state_hash: format!(
            "{:016x}",
            model_protocol::prompt_cache::stable_hash_bytes(&payload)
        ),
    }
}

fn upgrade_agent_status(
    status: &harness_contract::agent::AgentStatus,
) -> runtime::UpgradeCarrierStatus {
    match status {
        harness_contract::agent::AgentStatus::Prepared
        | harness_contract::agent::AgentStatus::Starting => runtime::UpgradeCarrierStatus::Ready,
        harness_contract::agent::AgentStatus::Running => runtime::UpgradeCarrierStatus::Running,
        harness_contract::agent::AgentStatus::WaitingInput
        | harness_contract::agent::AgentStatus::WaitingApproval => {
            runtime::UpgradeCarrierStatus::Waiting
        }
        harness_contract::agent::AgentStatus::Paused => runtime::UpgradeCarrierStatus::Paused,
        harness_contract::agent::AgentStatus::Completed => runtime::UpgradeCarrierStatus::Completed,
        harness_contract::agent::AgentStatus::Failed => runtime::UpgradeCarrierStatus::Failed,
        harness_contract::agent::AgentStatus::Cancelled => runtime::UpgradeCarrierStatus::Cancelled,
        harness_contract::agent::AgentStatus::Blocked => runtime::UpgradeCarrierStatus::Blocked,
    }
}

fn upgrade_team_status(status: &str) -> runtime::UpgradeCarrierStatus {
    match status {
        "planned" => runtime::UpgradeCarrierStatus::Ready,
        "running" => runtime::UpgradeCarrierStatus::Running,
        "paused" => runtime::UpgradeCarrierStatus::Paused,
        "waiting" | "review_required" => runtime::UpgradeCarrierStatus::Waiting,
        "completed" => runtime::UpgradeCarrierStatus::Completed,
        "cancelled" => runtime::UpgradeCarrierStatus::Cancelled,
        "failed" => runtime::UpgradeCarrierStatus::Failed,
        "blocked" => runtime::UpgradeCarrierStatus::Blocked,
        _ => runtime::UpgradeCarrierStatus::Blocked,
    }
}

fn upgrade_mission_status(status: &runtime::MissionSessionStatus) -> runtime::UpgradeCarrierStatus {
    match status {
        runtime::MissionSessionStatus::Active => runtime::UpgradeCarrierStatus::Running,
        runtime::MissionSessionStatus::Background => runtime::UpgradeCarrierStatus::Waiting,
        runtime::MissionSessionStatus::Paused => runtime::UpgradeCarrierStatus::Paused,
        runtime::MissionSessionStatus::Closed => runtime::UpgradeCarrierStatus::Completed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime_service_with_services(
        active_sessions: Arc<ActiveSessions>,
        store: Arc<memory::UnifiedSessionStore>,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> RuntimeService {
        RuntimeService::new(
            active_sessions.clone(),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                active_sessions,
                Some(store),
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
            Arc::new(runtime::ProviderRegistry::empty()),
            Arc::new(runtime::UpgradeCoordinator::new()),
            runtime_services,
        )
        .expect("test runtime service")
    }

    fn test_runtime_service(
        active_sessions: Arc<ActiveSessions>,
        store: Option<Arc<memory::UnifiedSessionStore>>,
    ) -> RuntimeService {
        let store = store.unwrap_or_else(|| {
            Arc::new(memory::UnifiedSessionStore::open_in_memory().expect("test session store"))
        });
        let runtime_services =
            runtime::RuntimeServices::in_memory().expect("test runtime services");
        runtime_services
            .install_session_store(Arc::clone(&store))
            .expect("test session router");
        test_runtime_service_with_services(active_sessions, store, runtime_services)
    }

    #[tokio::test]
    async fn restart_reuses_terminal_receipt_before_provider_runtime_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let record = memory::SessionRuntimeOutboxRecord {
            request_id: "restart-request".into(),
            turn_id: "restart-turn".into(),
            message_id: "restart-message".into(),
            session_id: "restart-session".into(),
            sequence: 0,
            status: memory::OutboxStatus::Claimed,
            runtime_commit_cursor: None,
            attempts: 1,
            next_attempt_at_ms: 0,
            claim_owner: Some("worker-a".into()),
            claim_expires_at_ms: Some(u64::MAX),
            failure_class: None,
            last_error: None,
            revision: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
            runtime_options_json: None,
        };
        let services = runtime::RuntimeServices::builder(&home, &workspace)
            .session_store(Arc::clone(&store))
            .build()
            .unwrap();
        services
            .session_terminal_delivery()
            .enqueue(
                "turn-terminal:restart-request",
                "assistant-restart-message",
                "restart-session",
                41,
                "assistant_json:\"done\"",
            )
            .unwrap();
        let first = test_runtime_service_with_services(
            Arc::new(ActiveSessions::new()),
            Arc::clone(&store),
            services,
        );
        assert_eq!(
            first
                .execute_ingress_record(&record, "must not run")
                .await
                .unwrap()
                .commit_cursor,
            41
        );
        drop(first);

        let restarted_services = runtime::RuntimeServices::builder(&home, &workspace)
            .session_store(Arc::clone(&store))
            .build()
            .unwrap();
        let restarted = test_runtime_service_with_services(
            Arc::new(ActiveSessions::new()),
            store,
            restarted_services,
        );
        let receipt = restarted
            .execute_ingress_record(&record, "must still not run")
            .await
            .unwrap();
        assert_eq!(receipt.commit_cursor, 41);
        assert_eq!(
            receipt.graph_id,
            runtime::session_ingress_graph_id("restart-session", "restart-request", "restart-turn")
        );
    }

    #[test]
    fn upgrade_status_mapping_preserves_active_and_terminal_boundaries() {
        assert_eq!(
            upgrade_agent_status(&harness_contract::agent::AgentStatus::Running),
            runtime::UpgradeCarrierStatus::Running
        );
        assert_eq!(
            upgrade_agent_status(&harness_contract::agent::AgentStatus::Completed),
            runtime::UpgradeCarrierStatus::Completed
        );
        assert_eq!(
            upgrade_team_status("review_required"),
            runtime::UpgradeCarrierStatus::Waiting
        );
        assert_eq!(
            upgrade_mission_status(&runtime::MissionSessionStatus::Paused),
            runtime::UpgradeCarrierStatus::Paused
        );
    }

    #[test]
    fn upgrade_carrier_hash_is_stable_for_same_projection() {
        let state = serde_json::json!({"status": "running", "revision": 3});
        let first = upgrade_carrier_record(
            "agent",
            "agent-1".to_string(),
            runtime::UpgradeCarrierStatus::Running,
            3,
            None,
            None,
            &state,
        );
        let second = upgrade_carrier_record(
            "agent",
            "agent-1".to_string(),
            runtime::UpgradeCarrierStatus::Running,
            3,
            None,
            None,
            &state,
        );
        assert_eq!(first.state_hash, second.state_hash);
    }

    #[tokio::test]
    async fn runtime_service_status_does_not_initialize_model_provider() {
        let service = test_runtime_service(Arc::new(ActiveSessions::default()), None);

        let value = service.status_value();
        assert_eq!(value["ok"], true);
        assert_eq!(value["runtime_host"], "gateway-runtime-host");
        let removed_legacy_key = ["dae", "mon"].concat();
        assert!(value.get(&removed_legacy_key).is_none());
        assert_eq!(value["active_sessions"], 0);
    }

    #[tokio::test]
    async fn runtime_service_snapshot_reports_lease_projection() {
        let service = test_runtime_service(Arc::new(ActiveSessions::default()), None);

        let lease = service
            .acquire_session_lease_value("session-1", "tui:test", "collaborative")
            .await;
        assert_eq!(lease["ok"], true);

        let snapshot = service.snapshot_value().await;
        assert_eq!(snapshot["kind"], "gateway_runtime_snapshot");
        assert!(snapshot.get("legacy_kind").is_none());
        let removed_legacy_key = ["dae", "mon"].concat();
        assert!(snapshot.get(&removed_legacy_key).is_none());
        assert_eq!(snapshot["leases"]["total"], 1);
        assert_eq!(snapshot["transport"]["control"], "gateway_http");
    }

    #[tokio::test]
    async fn runtime_service_records_durable_turn_journal() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let service =
            test_runtime_service(Arc::new(ActiveSessions::default()), Some(store.clone()));

        let submitted = service
            .submit_turn_value(
                Some("journal-session".to_string()),
                Some("task-a".to_string()),
                "persist this turn".to_string(),
            )
            .await;

        assert_eq!(submitted["ok"], true);
        assert_eq!(submitted["durable_journal"], true);
        let events = store
            .get_events_by_type_limited("journal-session", "TurnJournal", 0, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&events[0].event_json).unwrap();
        assert_eq!(payload["event_type"], "turn.submitted");
        assert_eq!(payload["phase"], "submitted");
        assert_eq!(payload["payload"]["prompt"], "persist this turn");
        assert_eq!(payload["payload"]["task_id"], "task-a");
    }

    #[tokio::test]
    async fn runtime_service_persists_session_input_runtime_event() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&memory::SessionRecord {
                session_id: "input-session".to_string(),
                platform: "test".to_string(),
                chat_id: "input-session".to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let active_sessions = Arc::new(ActiveSessions::default());
        let service = test_runtime_service(active_sessions, Some(store.clone()));
        service
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                "input-session".to_string(),
                runtime::SessionInputStream::new("input-session"),
            );

        let receipt = service
            .admit_session_input(harness_contract::turn::SessionInputEnvelope::text(
                "input-session",
                harness_contract::turn::InputSourceKind::Api,
                "remember this during the current work",
            ))
            .await
            .expect("admit input");

        assert_eq!(receipt.session_id, "input-session");
        let page = store
            .session_domain_events_page("input-session", 0, 10)
            .await
            .expect("runtime events page");
        let event = page
            .events
            .iter()
            .find(|event| event.kind == "SessionInputReceived")
            .expect("session input event persisted");
        assert_eq!(
            event.payload["input"]["input_id"],
            receipt.input_id.to_string()
        );
        assert_eq!(
            event.payload["record"]["envelope"]["content"],
            "remember this during the current work"
        );
        assert_eq!(event.payload["input_projection"]["total"], 1);
    }

    #[test]
    fn runtime_service_rejects_unsupported_protocol_as_legacy_socket_error() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "protocol_version": 999,
            "request_id": "req-old",
            "cmd": "status",
        }))
        .expect("request parses");

        let value = RuntimeService::unsupported_protocol_value(&request);
        assert_eq!(value["ok"], false);
        assert_eq!(value["request_id"], "req-old");
        assert_eq!(value["error_kind"], "unsupported_protocol");
        assert_eq!(value["retryable"], false);
        assert!(value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("unsupported runtime protocol version"));
    }

    #[test]
    fn runtime_service_records_executing_turn_lifecycle() {
        let service = test_runtime_service(Arc::new(ActiveSessions::default()), None);

        let running = service.start_running_turn(
            Some("session-turn".to_string()),
            Some("task-turn".to_string()),
            "execute real turn".to_string(),
        );
        assert_eq!(running.status, TurnStatus::Running);
        assert_eq!(running.session_id.as_deref(), Some("session-turn"));
        assert_eq!(running.task_id.as_deref(), Some("task-turn"));

        let completed = service.finish_turn(&running.turn_id, TurnStatus::Completed, None);
        assert_eq!(completed.status, TurnStatus::Completed);
        assert!(completed.completed_at.is_some());
        assert_eq!(completed.events.len(), 2);
        assert_eq!(completed.events[0].status, TurnStatus::Running);
        assert_eq!(completed.events[1].status, TurnStatus::Completed);

        let snapshot = service.turns_value();
        assert_eq!(snapshot["turn_bindings"][0]["task_id"], "task-turn");
        assert_eq!(
            snapshot["turn_bindings"][0]["turn_id"],
            running.turn_id.to_string()
        );
        assert_eq!(snapshot["turn_bindings"][0]["session_id"], "session-turn");
    }

    #[test]
    fn runtime_event_relay_preserves_event_type_without_inventing_lifecycle() {
        let text = runtime_event_stream_payload(runtime::CowdEvent::TextDelta {
            text: "partial".to_string(),
        });
        assert_eq!(text["type"], "TextDelta");
        assert_eq!(text["text"], "partial");

        let completed = runtime_event_stream_payload(runtime::CowdEvent::TurnComplete {
            assistant_text: "draft".to_string(),
            iterations: 2,
        });
        assert_eq!(completed["type"], "TurnComplete");
        assert_eq!(completed["assistant_text"], "draft");
        assert!(completed.get("committed").is_none());

        let scoped = runtime_event_stream_payload(runtime::CowdEvent::ExecutionScoped {
            context: runtime::CowdExecutionContext {
                execution_id: "execution-1".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
            },
            event: Box::new(runtime::CowdEvent::ExecutionPhase {
                status: ExecutionLiveStatus::CallingModel,
                detail: Some("requesting model".to_string()),
            }),
        });
        assert_eq!(scoped["type"], "ExecutionPhase");
        assert_eq!(scoped["execution_id"], "execution-1");
        assert_eq!(scoped["turn_id"], "turn-1");
    }

    #[tokio::test]
    async fn runtime_event_relay_forwards_render_events_to_gateway_session_bus() {
        let service = test_runtime_service(Arc::new(ActiveSessions::default()), None);
        let gateway_bus = service.session_kernel.event_bus();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        gateway_bus.subscribe("relay-session", tx).await;
        let runtime_bus = runtime::CowdEventBus::new();
        service.install_session_event_relay("relay-session", runtime_bus.clone());
        runtime_bus.emit(runtime::CowdEvent::TextDelta {
            text: "streamed through gateway".to_string(),
        });
        let payload = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("relay should forward within bounded time")
            .expect("gateway subscriber remains open");
        let payload: serde_json::Value = serde_json::from_str(&payload).expect("relay JSON");
        assert_eq!(payload["type"], "TextDelta");
        assert_eq!(payload["text"], "streamed through gateway");
        service.remove_active_runtime("relay-session");
    }

    #[test]
    fn session_execution_index_exposes_running_only_and_retains_terminal_reference() {
        let service = test_runtime_service(Arc::new(ActiveSessions::default()), None);
        service.record_live_execution(
            "session-index",
            "execution-running".to_string(),
            "turn-running".to_string(),
        );
        service.record_live_execution(
            "session-index",
            "execution-finished".to_string(),
            "turn-finished".to_string(),
        );

        let report = ContextTurnReport::new(
            "turn-finished",
            harness_contract::context::ContextPressureState::new("default", 32_000, 8_000),
        );
        service.complete_live_execution(
            "execution-finished",
            &report,
            "terminal-finished".to_string(),
        );

        let index = service.session_execution_index("session-index");
        assert_eq!(index.active_execution_ids, vec!["execution-running"]);
        assert_eq!(
            index.latest_execution_id.as_deref(),
            Some("execution-finished")
        );
        assert_eq!(index.latest_status, Some(ExecutionLiveStatus::Complete));
        assert_eq!(index.terminal_ref.as_deref(), Some("terminal-finished"));
        assert!(service
            .running_session_execution_indices()
            .iter()
            .any(|entry| entry.session_id == "session-index"));
    }

    #[test]
    fn durable_ingress_index_recovers_execution_identity_without_mixing_cursors() {
        let records = vec![
            memory::SessionRuntimeOutboxRecord {
                request_id: "request-complete".to_string(),
                turn_id: "turn-complete".to_string(),
                message_id: "message-complete".to_string(),
                session_id: "session-recovery".to_string(),
                sequence: 1,
                status: memory::OutboxStatus::Materialized,
                runtime_commit_cursor: Some(44),
                attempts: 1,
                next_attempt_at_ms: 0,
                claim_owner: None,
                claim_expires_at_ms: None,
                failure_class: None,
                last_error: None,
                revision: 9,
                created_at_ms: 10,
                updated_at_ms: 20,
                runtime_options_json: None,
            },
            memory::SessionRuntimeOutboxRecord {
                request_id: "request-pending".to_string(),
                turn_id: "turn-pending".to_string(),
                message_id: "message-pending".to_string(),
                session_id: "session-recovery".to_string(),
                sequence: 2,
                status: memory::OutboxStatus::Pending,
                runtime_commit_cursor: None,
                attempts: 0,
                next_attempt_at_ms: 0,
                claim_owner: None,
                claim_expires_at_ms: None,
                failure_class: None,
                last_error: None,
                revision: 3,
                created_at_ms: 21,
                updated_at_ms: 30,
                runtime_options_json: None,
            },
        ];
        let index = session_execution_index_from_outbox("session-recovery", &records);
        assert_eq!(index.active_execution_ids.len(), 1);
        assert_eq!(index.latest_status, Some(ExecutionLiveStatus::Queued));
        assert_eq!(index.latest_live_revision, None);
        assert_eq!(index.last_progress_at_ms, Some(30));
        assert!(index.terminal_ref.is_none());
        assert_eq!(
            index.latest_execution_id,
            Some(runtime::session_ingress_graph_id(
                "session-recovery",
                "request-pending",
                "turn-pending"
            ))
        );
    }
}
