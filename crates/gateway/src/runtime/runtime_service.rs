use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::event_bus::{RuntimeStreamRange, SessionProjectionEvent, SessionProjectionHub};
use crate::gateway::HotSessionPool;
use crate::runtime_boundary::{
    RuntimeBoundaryClock, RuntimeBoundarySnapshot, RuntimeBoundaryStatus,
};
use crate::runtime_protocol::{RuntimeErrorKind, RuntimeRequest, RuntimeResponse};
use crate::session_runtime_data_port::SessionInputJournalKind;
use harness_contract::{
    context::ContextTurnReport,
    projection::{ExecutionLiveState, ExecutionLiveStatus, SessionExecutionIndexProjection},
    turn::{
        InputPayloadKind, InputRoutingDecision, InputRoutingReason, InputSourceKind,
        SessionInputEnvelope, SessionInputId, SessionInputProjection, SessionInputReceipt,
        SessionInputStatus, TurnEvent, TurnId, TurnInboxSnapshot, TurnInput, TurnJournalEnvelope,
        TurnJournalPhase, TurnReceipt, TurnStatus,
    },
};
use session::SessionLeaseRegistry;

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

fn tool_event_identity(event: &runtime::CowdEvent) -> Option<(&str, bool, bool)> {
    match event.domain_event() {
        runtime::CowdEvent::ToolStart { id, .. } => Some((id, true, false)),
        runtime::CowdEvent::ToolProgress { id, .. } => Some((id, false, false)),
        runtime::CowdEvent::ToolComplete { id, .. } => Some((id, false, true)),
        _ => None,
    }
}

fn rewrite_tool_event_identity(event: &mut runtime::CowdEvent, instance_id: &str) {
    match event {
        runtime::CowdEvent::ExecutionScoped { event, .. } => {
            rewrite_tool_event_identity(event, instance_id);
        }
        runtime::CowdEvent::ToolStart { id, .. }
        | runtime::CowdEvent::ToolProgress { id, .. }
        | runtime::CowdEvent::ToolComplete { id, .. } => {
            *id = instance_id.to_string();
        }
        _ => {}
    }
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

#[derive(serde::Serialize)]
struct SessionInputDomainEventPayload<'a> {
    input: Option<&'a SessionInputReceipt>,
    record: Option<&'a runtime::SessionInputRecord>,
    input_projection: SessionInputProjection,
    turn_inbox: TurnInboxSnapshot,
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

struct ActiveTurnRegistryState {
    accepting: bool,
    controls: BTreeMap<String, ActiveTurnControl>,
}

struct ActiveTurnRegistry {
    state: Mutex<ActiveTurnRegistryState>,
    changed: tokio::sync::Notify,
}

impl ActiveTurnRegistry {
    fn new() -> Self {
        Self {
            state: Mutex::new(ActiveTurnRegistryState {
                accepting: true,
                controls: BTreeMap::new(),
            }),
            changed: tokio::sync::Notify::new(),
        }
    }
}

struct ActiveTurnControlGuard {
    turn_id: String,
    registry: Arc<ActiveTurnRegistry>,
}

impl Drop for ActiveTurnControlGuard {
    fn drop(&mut self) {
        self.registry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .remove(&self.turn_id);
        self.registry.changed.notify_waiters();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ActiveTurnDrainReport {
    pub(crate) cancelled: usize,
    pub(crate) drained: usize,
    pub(crate) remaining_turn_ids: Vec<String>,
}

struct RuntimeTurnOwner {
    session_id: String,
    entry: Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>,
    gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
    runtime:
        Option<runtime::StandardRuntimeHost<crate::gateway_tool_executor::GatewayToolExecutor>>,
}

struct MeasuredRuntimeEntryGuard<'a> {
    guard: tokio::sync::MutexGuard<'a, crate::runtime_entry::GatewayRuntimeEntry>,
    acquired_at: Instant,
}

impl Deref for MeasuredRuntimeEntryGuard<'_> {
    type Target = crate::runtime_entry::GatewayRuntimeEntry;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for MeasuredRuntimeEntryGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for MeasuredRuntimeEntryGuard<'_> {
    fn drop(&mut self) {
        runtime::execution_core::performance::observe_duration(
            "runtime_lock_hold_ms",
            self.acquired_at.elapsed(),
        );
    }
}

async fn lock_runtime_entry(
    entry: &tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>,
) -> MeasuredRuntimeEntryGuard<'_> {
    let started = Instant::now();
    let guard = entry.lock().await;
    runtime::execution_core::performance::observe_duration(
        "runtime_lock_wait_ms",
        started.elapsed(),
    );
    MeasuredRuntimeEntryGuard {
        guard,
        acquired_at: Instant::now(),
    }
}

impl RuntimeTurnOwner {
    fn new(
        session_id: String,
        entry: Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>,
        gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
        runtime: runtime::StandardRuntimeHost<crate::gateway_tool_executor::GatewayToolExecutor>,
    ) -> Self {
        Self {
            session_id,
            entry,
            gateway_tasks,
            runtime: Some(runtime),
        }
    }

    fn runtime_mut(
        &mut self,
    ) -> Result<
        &mut runtime::StandardRuntimeHost<crate::gateway_tool_executor::GatewayToolExecutor>,
        String,
    > {
        self.runtime
            .as_mut()
            .ok_or_else(|| "Runtime host was already restored before turn execution".to_string())
    }

    async fn restore(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let mut entry = lock_runtime_entry(&self.entry).await;
        entry.restore_runtime_after_turn(runtime);
    }
}

impl Drop for RuntimeTurnOwner {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let entry = Arc::clone(&self.entry);
        let session_id = self.session_id.clone();
        if let Err(error) = self.gateway_tasks.spawn(
            crate::runtime_host::task_set::GatewayTaskKind::RuntimeRestoration,
            Some(session_id.clone()),
            move |_| async move {
                    let mut entry = lock_runtime_entry(&entry).await;
                    if entry.turn_is_owned() {
                        entry.restore_runtime_after_turn(runtime);
                    } else {
                        tracing::error!(
                            "cancelled turn attempted to restore a Runtime host into an occupied session"
                        );
                    }
                },
        ) {
            tracing::warn!(
                %session_id,
                %error,
                "cancelled turn restoration was rejected because Gateway lifecycle is closing"
            );
        }
    }
}

fn session_execution_index_from_outbox(
    session_id: &str,
    records: &[session::SessionRuntimeOutboxRecord],
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
    let execution_for = |record: &session::SessionRuntimeOutboxRecord| {
        runtime::session_ingress_graph_id(session_id, &record.request_id, &record.turn_id)
    };
    let status_for = |record: &session::SessionRuntimeOutboxRecord| match record.status {
        session::SessionRuntimeInputStatus::Accepted
        | session::SessionRuntimeInputStatus::Classified
        | session::SessionRuntimeInputStatus::Queued
        | session::SessionRuntimeInputStatus::Reclassified => ExecutionLiveStatus::Queued,
        session::SessionRuntimeInputStatus::Claimed => ExecutionLiveStatus::PreparingContext,
        session::SessionRuntimeInputStatus::Running => ExecutionLiveStatus::CallingModel,
        session::SessionRuntimeInputStatus::Completed
        | session::SessionRuntimeInputStatus::Supplemented => ExecutionLiveStatus::Complete,
        session::SessionRuntimeInputStatus::Cancelled => ExecutionLiveStatus::Cancelled,
        session::SessionRuntimeInputStatus::Failed
        | session::SessionRuntimeInputStatus::Blocked
        | session::SessionRuntimeInputStatus::Expired
        | session::SessionRuntimeInputStatus::RejectedDuplicate
        | session::SessionRuntimeInputStatus::RejectedPolicy => ExecutionLiveStatus::Error,
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
            .filter(|record| {
                matches!(
                    record.status,
                    session::SessionRuntimeInputStatus::Completed
                        | session::SessionRuntimeInputStatus::Supplemented
                )
            })
            .map(|record| format!("turn-terminal:{}", record.request_id)),
    }
}

fn reconcile_session_execution_indices(
    mut volatile: SessionExecutionIndexProjection,
    durable: SessionExecutionIndexProjection,
) -> SessionExecutionIndexProjection {
    let same_execution = volatile.latest_execution_id.is_some()
        && volatile.latest_execution_id == durable.latest_execution_id;
    let volatile_has_terminal_outcome = volatile.latest_status.is_some_and(is_live_terminal);
    if same_execution && volatile_has_terminal_outcome {
        // The outbox's Materialized state means only that the terminal message
        // reached the durable transcript. It is not an execution-success
        // verdict. A persisted Runtime terminal outcome for the same execution
        // is authoritative even when outbox bookkeeping has a later timestamp.
        if volatile.terminal_ref.is_none() {
            volatile.terminal_ref = durable.terminal_ref;
        }
        volatile.last_progress_at_ms =
            match (volatile.last_progress_at_ms, durable.last_progress_at_ms) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (left, right) => left.or(right),
            };
        return volatile;
    }
    match (volatile.last_progress_at_ms, durable.last_progress_at_ms) {
        (Some(live), Some(persisted)) if live >= persisted => volatile,
        (Some(_), None) => volatile,
        _ => durable,
    }
}

fn stored_message_bytes(message: &session::SessionMessage) -> usize {
    message
        .stable_message_id
        .len()
        .saturating_add(message.session_id.len())
        .saturating_add(message.role.len())
        .saturating_add(message.content_json.len())
        .saturating_add(
            message
                .token_usage_json
                .as_ref()
                .map_or(0, std::string::String::len),
        )
        .saturating_add(
            message
                .tool_use_id
                .as_ref()
                .map_or(0, std::string::String::len),
        )
        .saturating_add(
            message
                .tool_name
                .as_ref()
                .map_or(0, std::string::String::len),
        )
}

#[derive(Clone)]
pub(crate) struct RuntimeService {
    sessions: Arc<HotSessionPool>,
    lease_registry: Arc<SessionLeaseRegistry>,
    session_data: Arc<crate::session_runtime_data_port::GatewaySessionRuntimePort>,
    projection_hub: Arc<SessionProjectionHub>,
    started_at: Instant,
    turns: Arc<Mutex<BTreeMap<String, TurnReceipt>>>,
    active_turns: Arc<ActiveTurnRegistry>,
    session_inputs: Arc<Mutex<BTreeMap<String, runtime::SessionInputStream>>>,
    session_event_buses: Arc<Mutex<BTreeMap<String, runtime::CowdEventBus>>>,
    gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
    session_models: Arc<Mutex<BTreeMap<String, String>>>,
    hydration_attempts: Arc<AtomicU64>,
    hydration_body_reads: Arc<AtomicU64>,
    hydration_body_bytes: Arc<AtomicU64>,
    approval_gate: Option<Arc<runtime::approval_gate::SmartApprovalGate>>,
    provider_registry: Arc<runtime::ProviderRegistry>,
    upgrade_coordinator: Arc<runtime::UpgradeCoordinator>,
    config_reload: Arc<crate::runtime_host::config_reload::ConfigReloadState>,
    tool_host: Arc<tools::ToolHost>,
    resource_capabilities: runtime::ResourceCapabilityIndex,
    runtime_services: Arc<runtime::RuntimeServices>,
    session_input_router: Arc<runtime::SessionInputRouter>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct SessionHydrationStats {
    pub(crate) attempts: u64,
    pub(crate) body_reads: u64,
    pub(crate) body_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionInputAdmission {
    pub(crate) receipt: SessionInputReceipt,
    pub(crate) materialized: Option<serde_json::Value>,
    /// Server-issued execution identity for the accepted ingress. Surfaces
    /// attach to this canonical graph rather than inferring it from prose.
    pub(crate) execution_graph_id: String,
    /// Server-issued terminal identity for this exact accepted ingress. It is
    /// stable across retry and lets every Surface discard unrelated replay.
    pub(crate) terminal_id: String,
    pub(crate) turn_id: String,
}

impl RuntimeService {
    #[must_use]
    pub(crate) fn new(
        sessions: Arc<HotSessionPool>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_data: Arc<crate::session_runtime_data_port::GatewaySessionRuntimePort>,
        projection_hub: Arc<SessionProjectionHub>,
        started_at: Instant,
        provider_registry: Arc<runtime::ProviderRegistry>,
        upgrade_coordinator: Arc<runtime::UpgradeCoordinator>,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> Result<Self, String> {
        Self::new_with_gateway_tasks(
            sessions,
            lease_registry,
            session_data,
            projection_hub,
            started_at,
            provider_registry,
            upgrade_coordinator,
            runtime_services,
            crate::runtime_host::task_set::GatewayRuntimeTaskSet::new(Duration::from_secs(5)),
        )
    }

    #[must_use]
    pub(crate) fn new_with_gateway_tasks(
        sessions: Arc<HotSessionPool>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_data: Arc<crate::session_runtime_data_port::GatewaySessionRuntimePort>,
        projection_hub: Arc<SessionProjectionHub>,
        started_at: Instant,
        provider_registry: Arc<runtime::ProviderRegistry>,
        upgrade_coordinator: Arc<runtime::UpgradeCoordinator>,
        runtime_services: Arc<runtime::RuntimeServices>,
        gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
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
            session_data,
            projection_hub,
            started_at,
            turns: Arc::new(Mutex::new(BTreeMap::new())),
            active_turns: Arc::new(ActiveTurnRegistry::new()),
            session_inputs: Arc::new(Mutex::new(BTreeMap::new())),
            session_event_buses: Arc::new(Mutex::new(BTreeMap::new())),
            gateway_tasks,
            session_models: Arc::new(Mutex::new(BTreeMap::new())),
            hydration_attempts: Arc::new(AtomicU64::new(0)),
            hydration_body_reads: Arc::new(AtomicU64::new(0)),
            hydration_body_bytes: Arc::new(AtomicU64::new(0)),
            approval_gate: None,
            provider_registry,
            upgrade_coordinator,
            config_reload: Arc::new(crate::runtime_host::config_reload::ConfigReloadState::new()),
            tool_host: Arc::new(tools::ToolHost::builtin("gateway-runtime", workspace_root)),
            resource_capabilities,
            runtime_services,
            session_input_router,
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

    pub(crate) fn notify_session_input_scheduler(&self) {
        self.session_input_router.notify_pending();
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
        let Ok(records) = self.session_data.runtime_inputs(session_id, 100).await else {
            return volatile;
        };
        let durable = session_execution_index_from_outbox(session_id, &records);
        reconcile_session_execution_indices(volatile, durable)
    }

    pub(crate) async fn recoverable_running_session_execution_indices(
        &self,
    ) -> Vec<SessionExecutionIndexProjection> {
        let mut session_ids = self
            .running_session_execution_indices()
            .into_iter()
            .map(|index| index.session_id)
            .collect::<BTreeSet<_>>();
        if let Ok(records) = self.session_data.active_runtime_inputs(500).await {
            session_ids.extend(records.into_iter().map(|record| record.session_id));
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
        write_attempt_paths: &[String],
        terminal_ref: String,
    ) {
        self.runtime_services.complete_live_execution(
            execution_id,
            report,
            write_attempt_paths,
            terminal_ref,
        );
    }

    fn fail_live_execution(&self, execution_id: &str, error: String) {
        self.runtime_services
            .fail_live_execution(execution_id, error);
    }

    fn block_live_execution(
        &self,
        execution_id: &str,
        report: &ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
        reason: String,
    ) {
        self.runtime_services.block_live_execution(
            execution_id,
            report,
            write_attempt_paths,
            terminal_ref,
            reason,
        );
    }

    fn install_active_turn_control(
        &self,
        turn_id: &str,
        session_id: &str,
        execution_id: Option<String>,
    ) -> Result<(runtime::CancellationToken, ActiveTurnControlGuard), String> {
        let cancellation_token = runtime::CancellationToken::new();
        let mut state = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.accepting {
            return Err("Gateway Runtime turn admission is closing".to_string());
        }
        if state.controls.contains_key(turn_id) {
            return Err(format!("Runtime turn {turn_id} is already active"));
        }
        state.controls.insert(
            turn_id.to_string(),
            ActiveTurnControl {
                session_id: session_id.to_string(),
                execution_id,
                cancellation_token: cancellation_token.clone(),
            },
        );
        drop(state);
        Ok((
            cancellation_token,
            ActiveTurnControlGuard {
                turn_id: turn_id.to_string(),
                registry: Arc::clone(&self.active_turns),
            },
        ))
    }

    fn cancel_active_turn_control(&self, turn_id: &str, reason: &str) -> Option<String> {
        let control = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
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
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .iter()
            .filter(|(_, control)| control.session_id == session_id)
            .map(|(turn_id, _)| turn_id.clone())
            .collect::<Vec<_>>();
        turn_ids
            .into_iter()
            .filter_map(|turn_id| self.cancel_active_turn_control(&turn_id, reason))
            .collect()
    }

    /// Cancel every live turn owned by one session and propagate cancellation
    /// to the Runtime execution registry. The HTTP session-cancel endpoint
    /// uses this owner path; broadcasting a UI event alone is not cancellation.
    pub(crate) fn cancel_active_session(&self, session_id: &str, reason: &str) -> Vec<String> {
        self.cancel_active_session_turns(session_id, reason)
    }

    /// Atomically close Runtime turn admission and cancel every turn that was
    /// already accepted. The registry lock is the admission fence: no turn can
    /// be inserted between the snapshot and cancellation.
    pub(crate) fn stop_accepting_and_cancel_active_turns(&self, reason: &str) -> Vec<String> {
        let controls = {
            let mut state = self
                .active_turns
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.accepting = false;
            state
                .controls
                .iter()
                .map(|(turn_id, control)| (turn_id.clone(), control.clone()))
                .collect::<Vec<_>>()
        };
        for (_, control) in &controls {
            control.cancellation_token.cancel();
            if let Some(execution_id) = &control.execution_id {
                self.runtime_services
                    .cancel_live_execution(execution_id, reason.to_string());
            }
        }
        controls
            .into_iter()
            .map(|(turn_id, control)| control.execution_id.unwrap_or(turn_id))
            .collect()
    }

    pub(crate) async fn wait_for_active_turns(
        &self,
        cancelled: usize,
        timeout: Duration,
    ) -> ActiveTurnDrainReport {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.active_turns.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let remaining_turn_ids = self
                .active_turns
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .controls
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            if remaining_turn_ids.is_empty() {
                return ActiveTurnDrainReport {
                    cancelled,
                    drained: cancelled,
                    remaining_turn_ids,
                };
            }
            if tokio::time::timeout_at(deadline, &mut changed)
                .await
                .is_err()
            {
                let remaining_turn_ids = self
                    .active_turns
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .controls
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                return ActiveTurnDrainReport {
                    cancelled,
                    drained: cancelled.saturating_sub(remaining_turn_ids.len()),
                    remaining_turn_ids,
                };
            }
        }
    }

    #[cfg(test)]
    fn active_turn_count(&self) -> usize {
        self.active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .len()
    }

    pub(crate) fn session_input_runtime_state(
        &self,
        session_id: &str,
    ) -> runtime::RuntimeInputState {
        let active_turn_id = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .iter()
            .find_map(|(turn_id, control)| {
                (control.session_id == session_id).then(|| TurnId::from_string(turn_id.clone()))
            });
        runtime::RuntimeInputState {
            active_turn_id,
            waiting_for_approval: false,
            waiting_for_clarification: false,
        }
    }

    pub(crate) fn is_session_turn_active(&self, session_id: &str, turn_id: &str) -> bool {
        self.active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .get(turn_id)
            .is_some_and(|control| control.session_id == session_id)
    }

    pub(crate) async fn deliver_durable_session_input_view(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        content: String,
        status: SessionInputStatus,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let active_turn_id = record
            .target_turn_id
            .as_ref()
            .map(|turn_id| TurnId::from_string(turn_id.clone()));
        let created_at = chrono::DateTime::<Utc>::from_timestamp_millis(
            record.created_at_ms.min(i64::MAX as u64) as i64,
        )
        .unwrap_or_else(Utc::now);
        let envelope = SessionInputEnvelope {
            input_id: SessionInputId::from_string(record.input_id.clone()),
            session_id: record.session_id.clone(),
            source_kind: InputSourceKind::Runtime,
            payload_kind: InputPayloadKind::Text,
            content_preview: content.chars().take(160).collect(),
            content,
            source_ref: Some(format!("session-input:{}", record.input_id)),
            source_message_id: Some(record.message_id.clone()),
            idempotency_key: record.request_id.clone(),
            metadata: serde_json::json!({
                "durable_request_id": record.request_id,
                "session_generation": record.session_generation,
            }),
            created_at,
        };
        let receipt = SessionInputReceipt {
            input_id: envelope.input_id.clone(),
            session_id: record.session_id.clone(),
            status,
            decision: record.decision,
            relation_proposal: None,
            reason: Some(InputRoutingReason::new(
                "durable_delivery",
                "input delivered from the durable Session queue",
                10_000,
            )),
            active_turn_id,
            evidence_refs: vec![format!("session-input:{}", record.input_id)],
            created_at,
        };
        self.project_durable_session_input(envelope, receipt).await
    }

    /// Refresh the process-local turn inbox from a durable Session admission.
    /// Gateway/Memory retain lifecycle authority; Runtime only receives the
    /// content needed by active-turn checkpoints.
    pub(crate) async fn project_durable_session_input(
        &self,
        envelope: SessionInputEnvelope,
        receipt: SessionInputReceipt,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let session_id = envelope.session_id.clone();
        let stream = self.session_input_stream_for(&session_id).await?;
        stream.project_durable(envelope, receipt.clone());
        self.emit_session_input_events(&session_id, &stream, Some(receipt));
        Ok(())
    }

    pub(crate) fn project_durable_session_receipt(
        &self,
        session_id: &str,
        receipt: SessionInputReceipt,
    ) {
        let stream = self
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned();
        if let Some(stream) = stream {
            stream.project_durable_receipt(&receipt);
            self.emit_session_input_events(session_id, &stream, Some(receipt));
        }
    }

    pub(crate) async fn publish_user_message_committed(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        content: &str,
    ) {
        let execution_id = runtime::session_ingress_graph_id(
            &record.session_id,
            &record.request_id,
            &record.turn_id,
        );
        self.record_live_execution(
            &record.session_id,
            execution_id.clone(),
            record.turn_id.clone(),
        );
        self.projection_hub
            .publish(
                &record.session_id,
                SessionProjectionEvent::UserMessageCommitted {
                    session_id: record.session_id.clone(),
                    message_id: record.message_id.clone(),
                    sequence: record.sequence,
                    execution_id,
                    turn_id: record.turn_id.clone(),
                    content: content.to_string(),
                    created_at_ms: record.created_at_ms,
                },
            )
            .await;
    }

    pub(crate) fn refresh_resource_capabilities(&self) -> runtime::ResourceCapabilitySnapshot {
        self.resource_capabilities.refresh_from_environment()
    }

    pub(crate) async fn execute_ingress_record(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        content: &str,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        let invocation_id = uuid::Uuid::new_v4();
        let terminal_id = format!("turn-terminal:{}", record.request_id);
        let graph_id = runtime::session_ingress_graph_id(
            &record.session_id,
            &record.request_id,
            &record.turn_id,
        );
        tracing::debug!(
            %invocation_id,
            request_id = %record.request_id,
            graph_id,
            terminal_id,
            "entered canonical Session ingress execution"
        );
        if let Some(terminal) = self
            .runtime_services
            .session_terminal_delivery()
            .get(&terminal_id)
            .map_err(|error| error.to_string())?
        {
            let terminal = if terminal.status == "materialized" {
                terminal
            } else {
                self.adopt_existing_terminal_for_claim(record, terminal)
                    .await?;
                self.await_session_terminal_materialization(&terminal_id)
                    .await?
            };
            tracing::debug!(
                %invocation_id,
                request_id = %record.request_id,
                graph_id,
                terminal_id,
                terminal_status = %terminal.status,
                "restoring live execution from an existing durable terminal"
            );
            // A worker may be replaying an already-committed terminal after
            // process recovery. Re-establish the live record and mark it
            // terminal before best-effort Session projection journaling. The
            // durable terminal carrier, not that secondary journal, is the
            // completion authority.
            self.record_live_execution(
                &record.session_id,
                graph_id.clone(),
                record.turn_id.clone(),
            );
            self.runtime_services
                .complete_recovered_live_execution(&graph_id, terminal_id.clone());
            self.bind_primary_ingress_projection(record, &graph_id)
                .await;
            self.settle_primary_ingress_projection(record, &graph_id, &terminal_id)
                .await;
            let runtime_record =
                crate::session_runtime_data_port::to_runtime_input_record(record.clone());
            if let Some(resolution) = self.session_input_router.record_target_terminal(
                &runtime_record,
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
            return Err(format!(
                "session {} is not active; Gateway Session scheduler must activate it before Runtime execution",
                record.session_id
            ));
        }
        let runtime_entry = self
            .sessions
            .get(&record.session_id)
            .ok_or_else(|| format!("session {} has no active runtime", record.session_id))?;
        let ingress =
            runtime::TurnIngressRef {
                request_id: record.request_id.clone(),
                turn_id: record.turn_id.clone(),
                message_id: record.message_id.clone(),
                session_id: record.session_id.clone(),
                session_generation: record.session_generation,
                input_sequence: record.sequence as u64,
                claim_owner: record.claim_owner.clone().ok_or_else(|| {
                    format!("session input {} has no claim owner", record.request_id)
                })?,
                claim_token: record.claim_token.clone().ok_or_else(|| {
                    format!("session input {} has no claim token", record.request_id)
                })?,
                claim_revision: record.claim_fence_epoch.ok_or_else(|| {
                    format!(
                        "session input {} has no immutable claim fence epoch",
                        record.request_id
                    )
                })?,
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
        )?;
        let mut owned_runtime = match async {
            let mut runtime = lock_runtime_entry(&runtime_entry).await;
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
            Ok::<_, String>(RuntimeTurnOwner::new(
                record.session_id.clone(),
                Arc::clone(&runtime_entry),
                Arc::clone(&self.gateway_tasks),
                host,
            ))
        }
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                self.fail_live_execution(&graph_id, error.clone());
                return Err(error);
            }
        };
        self.bind_primary_ingress_projection(record, &graph_id)
            .await;
        tracing::debug!(
            %invocation_id,
            request_id = %record.request_id,
            graph_id,
            "starting fresh Runtime ingress turn"
        );
        // The complete Runtime turn state machine is intentionally large. Keep
        // it behind one heap allocation so Gateway worker stacks do not grow
        // with every execution capability added to Runtime.
        let summary_result = Box::pin(owned_runtime.runtime_mut()?.submit_ingress_turn(
            content,
            &runtime::permissions::SharedPrompter::none(),
            ingress,
        ))
        .await;
        owned_runtime.restore().await;
        let summary = match summary_result {
            Ok(summary) => summary,
            Err(error) => {
                let error = error.to_string();
                if cancellation_token.is_cancelled() {
                    self.runtime_services.cancel_live_execution(
                        &graph_id,
                        "cancelled while Runtime turn was running".to_string(),
                    );
                } else {
                    self.fail_live_execution(&graph_id, error.clone());
                }
                self.fail_primary_ingress_projection(record, &error).await;
                return Err(error);
            }
        };
        let terminal = match self
            .runtime_services
            .session_terminal_delivery()
            .get(&terminal_id)
        {
            Ok(Some(terminal)) => terminal,
            Ok(None) => {
                let error = format!("runtime committed no terminal for {}", record.request_id);
                self.fail_primary_ingress_projection(record, &error).await;
                self.fail_live_execution(&graph_id, error.clone());
                return Err(error);
            }
            Err(error) => {
                let error = error.to_string();
                self.fail_primary_ingress_projection(record, &error).await;
                self.fail_live_execution(&graph_id, error.clone());
                return Err(error);
            }
        };
        let terminal = if terminal.status == "materialized" {
            terminal
        } else {
            self.await_session_terminal_materialization(&terminal_id)
                .await?
        };
        tracing::debug!(
            %invocation_id,
            request_id = %record.request_id,
            graph_id,
            terminal_id,
            terminal_status = %terminal.status,
            completion = ?summary.terminal_completion,
            calibrated_input_tokens = ?summary
                .context_turn_report
                .ledger
                .as_ref()
                .and_then(|ledger| ledger.calibrated_input_tokens),
            "materialized fresh Runtime terminal; committing canonical live terminal"
        );
        // Once the durable terminal is materialized, every canonical live read
        // must observe a terminal state. Session input projection journaling is
        // useful evidence but is not allowed to hold completion behind storage
        // latency or a failed secondary write.
        match summary.terminal_completion {
            harness_contract::goal::GoalCompletion::Satisfied => self.complete_live_execution(
                &graph_id,
                &summary.context_turn_report,
                &summary.write_attempt_paths,
                terminal_id.clone(),
            ),
            harness_contract::goal::GoalCompletion::Blocked
            | harness_contract::goal::GoalCompletion::Open => {
                let reason = format!("Runtime turn blocked: {}", summary.final_answer);
                self.block_live_execution(
                    &graph_id,
                    &summary.context_turn_report,
                    &summary.write_attempt_paths,
                    terminal_id.clone(),
                    reason.clone(),
                );
                let event = runtime::CowdEvent::ExecutionScoped {
                    context: runtime::CowdExecutionContext {
                        execution_id: graph_id.clone(),
                        session_id: record.session_id.clone(),
                        turn_id: record.turn_id.clone(),
                    },
                    event: Box::new(runtime::CowdEvent::TurnError { error: reason }),
                };
                self.projection_hub
                    .publish(&record.session_id, SessionProjectionEvent::runtime(event))
                    .await;
            }
            harness_contract::goal::GoalCompletion::Cancelled => {
                self.runtime_services
                    .cancel_live_execution(&graph_id, "Runtime turn cancelled".to_string());
            }
        }
        self.settle_primary_ingress_projection(record, &graph_id, &terminal_id)
            .await;
        let runtime_record =
            crate::session_runtime_data_port::to_runtime_input_record(record.clone());
        if let Some(resolution) = self.session_input_router.record_target_terminal(
            &runtime_record,
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

    async fn await_session_terminal_materialization(
        &self,
        terminal_id: &str,
    ) -> Result<runtime::RuntimeSessionOutboxRecord, String> {
        const MATERIALIZATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        tokio::time::timeout(MATERIALIZATION_TIMEOUT, async {
            loop {
                let terminal = self
                    .runtime_services
                    .session_terminal_delivery()
                    .get(terminal_id)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("runtime terminal `{terminal_id}` disappeared"))?;
                match terminal.status.as_str() {
                    "materialized" => return Ok(terminal),
                    "blocked" => {
                        return Err(format!(
                            "runtime terminal `{terminal_id}` materialization blocked: {}",
                            terminal.last_error.as_deref().unwrap_or("unknown error")
                        ));
                    }
                    _ => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .map_err(|_| {
            format!(
                "runtime terminal `{terminal_id}` was not materialized within {}ms",
                MATERIALIZATION_TIMEOUT.as_millis()
            )
        })?
    }

    async fn adopt_existing_terminal_for_claim(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        mut terminal: runtime::RuntimeSessionOutboxRecord,
    ) -> Result<runtime::RuntimeSessionOutboxRecord, String> {
        let claim_owner = record.claim_owner.as_ref().ok_or_else(|| {
            format!(
                "running Session input `{}` has no claim owner",
                record.request_id
            )
        })?;
        let claim_token = record.claim_token.as_ref().ok_or_else(|| {
            format!(
                "running Session input `{}` has no claim token",
                record.request_id
            )
        })?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if terminal.status == "materialized"
                || (terminal.request_id.as_deref() == Some(record.request_id.as_str())
                    && terminal.session_id == record.session_id
                    && terminal.session_generation == Some(record.session_generation)
                    && terminal.input_claim_owner.as_deref() == Some(claim_owner.as_str())
                    && terminal.input_claim_token.as_deref() == Some(claim_token.as_str()))
            {
                return Ok(terminal);
            }
            if terminal.request_id.as_deref() != Some(record.request_id.as_str())
                || terminal.session_id != record.session_id
                || terminal.session_generation != Some(record.session_generation)
            {
                return Err(format!(
                    "runtime terminal `{}` cannot be adopted by Session input `{}` at generation {}",
                    terminal.terminal_id, record.request_id, record.session_generation
                ));
            }
            let now = Utc::now().timestamp_millis().max(0) as u64;
            if terminal.status == "claimed"
                && terminal
                    .claim_expires_at_ms
                    .is_some_and(|expires| expires > now)
            {
                if std::time::Instant::now() >= deadline {
                    return Err(format!(
                        "runtime terminal `{}` retained an active delivery claim during recovery",
                        terminal.terminal_id
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                terminal = self
                    .runtime_services
                    .session_terminal_delivery()
                    .get(&terminal.terminal_id)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| {
                        format!("runtime terminal `{}` disappeared", terminal.terminal_id)
                    })?;
                continue;
            }
            let adoption = runtime::RuntimeSessionTerminalFenceAdoption {
                terminal_id: terminal.terminal_id.clone(),
                expected_terminal_revision: terminal.revision,
                request_id: record.request_id.clone(),
                session_id: record.session_id.clone(),
                turn_id: record.turn_id.clone(),
                session_generation: record.session_generation,
                input_sequence: record.sequence as u64,
                claim_owner: claim_owner.clone(),
                claim_token: claim_token.clone(),
                claim_revision: record.claim_fence_epoch.ok_or_else(|| {
                    format!(
                        "Session input `{}` cannot adopt a terminal without a claim fence epoch",
                        record.input_id
                    )
                })?,
                claim_expires_at_ms: record.claim_expires_at_ms.ok_or_else(|| {
                    format!(
                        "Session input `{}` cannot adopt a terminal without an active claim deadline",
                        record.input_id
                    )
                })?,
                adopted_at_ms: now,
            };
            match self
                .runtime_services
                .session_terminal_delivery()
                .adopt_fence(&adoption)
            {
                Ok(adopted) => {
                    self.notify_session_input_scheduler();
                    return Ok(adopted);
                }
                Err(runtime::RuntimeEventStoreError::StaleRevision { .. })
                    if std::time::Instant::now() < deadline =>
                {
                    terminal = self
                        .runtime_services
                        .session_terminal_delivery()
                        .get(&terminal.terminal_id)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            format!("runtime terminal `{}` disappeared", terminal.terminal_id)
                        })?;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
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
            "execution": self.runtime_services.execution_health(),
        })
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
    pub(crate) fn gateway_tasks(
        &self,
    ) -> Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet> {
        Arc::clone(&self.gateway_tasks)
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
            "lifecycle": self.session_data.presence_snapshots().await,
            "turns": turns,
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
                        model_protocol::fingerprint::stable_hash_bytes(&payload)
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
        carriers.extend(self.sessions.list().into_iter().map(|session_id| {
            let snapshot = serde_json::json!({
                "session_id": session_id,
                "status": "active",
                "source": "gateway.hot_session_pool",
            });
            upgrade_carrier_record(
                "session",
                session_id.clone(),
                runtime::UpgradeCarrierStatus::Running,
                0,
                None,
                Some(format!("session://{session_id}")),
                &snapshot,
            )
        }));
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
    ) -> Option<Result<Option<usize>, session::SessionError>> {
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
            self.session_data
                .append_turn_journal(session_id, envelope)
                .await,
        )
    }

    async fn persist_turn_receipt_journal(
        &self,
        receipt: &TurnReceipt,
        phase: TurnJournalPhase,
        message: Option<String>,
    ) -> Option<Result<Option<usize>, session::SessionError>> {
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
            self.session_data
                .append_turn_journal(session_id, envelope)
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

    pub(crate) fn has_active_session(&self, session_id: &str) -> bool {
        self.sessions.get(session_id).is_some()
    }

    #[must_use]
    pub(crate) fn hydration_stats(&self) -> SessionHydrationStats {
        SessionHydrationStats {
            attempts: self.hydration_attempts.load(Ordering::Relaxed),
            body_reads: self.hydration_body_reads.load(Ordering::Relaxed),
            body_bytes: self.hydration_body_bytes.load(Ordering::Relaxed),
        }
    }

    /// Lazily activate a persisted session for a new ingress turn. Persisted
    /// session identity remains owned by UnifiedSessionStore; this only adds
    /// the process-local runtime required to execute work.
    pub(crate) async fn activate_persisted_session(
        &self,
        session_id: &str,
        model_hint: Option<&str>,
        system_prompt: Vec<String>,
        recovery: runtime::SessionRecoveryConfig,
    ) -> Result<(), String> {
        if self.has_active_session(session_id) {
            return Ok(());
        }
        let hydration_started = std::time::Instant::now();
        let stored_record = self
            .session_data
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?;
        let stored_model = stored_record
            .as_ref()
            .and_then(|record| record.model.clone())
            .filter(|model| !model.trim().is_empty());
        let model = model_hint
            .filter(|model| !model.trim().is_empty())
            .map(ToOwned::to_owned)
            .or(stored_model)
            .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string());
        let session = if let Some(record) = stored_record {
            self.hydration_attempts.fetch_add(1, Ordering::Relaxed);
            let mut messages = None;
            for attempt in 1..=recovery.stable_snapshot_attempts {
                let total_before = self
                    .session_data
                    .stored_message_count(session_id)
                    .await
                    .map_err(|error| error.to_string())?;
                let mut candidate = Vec::with_capacity(total_before);
                let mut hydrated_bytes = 0usize;
                while candidate.len() < total_before {
                    let remaining = total_before.saturating_sub(candidate.len());
                    let page_limit = recovery.hydrate_page_messages.min(remaining);
                    let page = self
                        .session_data
                        .stored_messages(session_id, candidate.len(), page_limit)
                        .await
                        .map_err(|error| error.to_string())?;
                    self.hydration_body_reads.fetch_add(1, Ordering::Relaxed);
                    if page.is_empty() {
                        break;
                    }
                    for message in &page {
                        hydrated_bytes = hydrated_bytes
                            .checked_add(stored_message_bytes(message))
                            .ok_or_else(|| {
                                format!("session {session_id} hydration byte accounting overflowed")
                            })?;
                    }
                    if hydrated_bytes > recovery.max_session_hydrate_bytes {
                        return Err(format!(
                            "session {session_id} durable payload exceeded configured gateway.recovery.max_session_hydrate_bytes={} during activation; raise the explicit limit or compact/checkpoint the session",
                            recovery.max_session_hydrate_bytes
                        ));
                    }
                    candidate.extend(page);
                }
                let total_after = self
                    .session_data
                    .stored_message_count(session_id)
                    .await
                    .map_err(|error| error.to_string())?;
                let sequences_are_contiguous = candidate
                    .iter()
                    .enumerate()
                    .all(|(sequence, message)| message.sequence == sequence);
                if total_before == total_after
                    && candidate.len() == total_before
                    && sequences_are_contiguous
                {
                    messages = Some((candidate, hydrated_bytes, attempt));
                    break;
                }
                if attempt < recovery.stable_snapshot_attempts {
                    tokio::task::yield_now().await;
                }
            }
            let (messages, hydrated_bytes, snapshot_attempts) = messages.ok_or_else(|| {
                format!(
                    "session {session_id} changed during all {} configured runtime hydration snapshot attempts; retry activation after ingress stabilizes",
                    recovery.stable_snapshot_attempts
                )
            })?;
            self.hydration_body_bytes
                .fetch_add(hydrated_bytes as u64, Ordering::Relaxed);
            tracing::info!(
                session_id,
                hydration_messages = messages.len(),
                hydration_bytes = hydrated_bytes,
                hydration_duration_ms = hydration_started.elapsed().as_millis(),
                hydration_snapshot_attempts = snapshot_attempts,
                "hydrated persisted session into Runtime carrier"
            );
            crate::entry::session_store_entry::hydrated_runtime_session(record, messages)?
        } else {
            let mut session = runtime::Session::new();
            session.session_id = session_id.to_string();
            session
        };
        if session.closed {
            return Err(format!(
                "session {session_id} is closed and cannot activate a new Runtime carrier"
            ));
        }
        let runtime =
            self.build_session_runtime_entry(session, session_id, &model, system_prompt)?;
        self.register_runtime(session_id.to_string(), runtime)
            .await?;
        runtime::execution_core::performance::observe_duration(
            "hydrate_ms",
            hydration_started.elapsed(),
        );
        Ok(())
    }

    pub(crate) async fn register_runtime(
        &self,
        session_id: String,
        mut runtime: crate::runtime_entry::GatewayRuntimeEntry,
    ) -> Result<Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>>, String>
    {
        if self.sessions.get(&session_id).is_some() {
            return Err(format!(
                "session {session_id} already has an active Runtime carrier; refusing replacement"
            ));
        }
        self.gateway_tasks
            .open_session(&session_id)
            .await
            .map_err(|error| format!("cannot activate Runtime carrier during shutdown: {error}"))?;
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
                if let Err(error) = self
                    .install_session_event_relay(&session_id, cowd_bus)
                    .await
                {
                    self.session_inputs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&session_id);
                    self.session_event_buses
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&session_id);
                    self.session_models
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&session_id);
                    self.sessions.remove(&session_id);
                    return Err(format!(
                        "failed to install Runtime event relay for session {session_id}: {error}"
                    ));
                }
            } else {
                self.session_event_buses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&session_id);
            }
        }
        result
    }

    pub(crate) async fn remove_active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>> {
        let task_report = self
            .gateway_tasks
            .close_session_and_drain(session_id, Duration::from_secs(5))
            .await;
        if task_report.forced_aborts > 0 || task_report.panicked > 0 {
            tracing::warn!(
                %session_id,
                joined = task_report.joined,
                panicked = task_report.panicked,
                forced_aborts = task_report.forced_aborts,
                "session-scoped Gateway background tasks required forced cleanup"
            );
        }
        self.session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.session_event_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.session_models
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.sessions.remove(session_id)
    }

    /// Runtime emits transient rendering/progress events on its own bus while
    /// Gateway owns the cross-surface transport. Relay them once per active
    /// session so every surface observes the same stream. Durable terminal
    /// settlement is deliberately emitted by `SessionWorkerSupervisor` only
    /// after the transcript append succeeds.
    async fn install_session_event_relay(
        &self,
        session_id: &str,
        bus: runtime::CowdEventBus,
    ) -> Result<(), crate::runtime_host::task_set::GatewayTaskSpawnError> {
        let session_id = session_id.to_string();
        let relay_session_id = session_id.clone();
        let gateway_bus = Arc::clone(&self.projection_hub);
        let runtime_services = Arc::clone(&self.runtime_services);
        let mut receiver = bus.subscribe();
        self.gateway_tasks
            .replace_session_task(
                crate::runtime_host::task_set::GatewayTaskKind::SessionEventRelay,
                &session_id,
                move |cancellation| async move {
                    let mut tool_ordinals = HashMap::<(String, String, String), u64>::new();
                    let mut active_tool_instances =
                        HashMap::<(String, String, String), String>::new();
                    let mut text_stream_offsets = HashMap::<(String, String, String), usize>::new();
                    loop {
                        let received = tokio::select! {
                            _ = cancellation.cancelled() => break,
                            received = receiver.recv() => received,
                        };
                        match received {
                            Ok(mut event) => {
                                if let (Some(context), Some((provider_id, started, completed))) = (
                                    event.execution_context().cloned(),
                                    tool_event_identity(&event),
                                ) {
                                    let provider_id = provider_id.to_string();
                                    let key = (
                                        context.execution_id,
                                        context.turn_id,
                                        provider_id.clone(),
                                    );
                                    let instance_id = if started {
                                        let ordinal = tool_ordinals.entry(key.clone()).or_default();
                                        let instance_id = format!("{provider_id}#cowd-{ordinal}");
                                        *ordinal = ordinal.saturating_add(1);
                                        active_tool_instances
                                            .insert(key.clone(), instance_id.clone());
                                        instance_id
                                    } else {
                                        active_tool_instances.get(&key).cloned().unwrap_or_else(
                                            || {
                                                let ordinal =
                                                    tool_ordinals.entry(key.clone()).or_default();
                                                let instance_id =
                                                    format!("{provider_id}#cowd-{ordinal}");
                                                *ordinal = ordinal.saturating_add(1);
                                                instance_id
                                            },
                                        )
                                    };
                                    rewrite_tool_event_identity(&mut event, &instance_id);
                                    if completed {
                                        active_tool_instances.remove(&key);
                                    }
                                }
                                runtime_services
                                    .observe_live_execution_event(&relay_session_id, &event);
                                let stream_range =
                                    match (event.execution_context(), event.domain_event()) {
                                        (Some(context), runtime::CowdEvent::TextDelta { text }) => {
                                            let stream_key = (
                                                context.execution_id.clone(),
                                                context.turn_id.clone(),
                                                "assistant_text".to_string(),
                                            );
                                            let start_bytes = *text_stream_offsets
                                                .entry(stream_key.clone())
                                                .or_default();
                                            let end_bytes = start_bytes.saturating_add(text.len());
                                            text_stream_offsets.insert(stream_key, end_bytes);
                                            Some(RuntimeStreamRange {
                                                start_bytes,
                                                end_bytes,
                                                stream_revision: end_bytes,
                                            })
                                        }
                                        _ => None,
                                    };
                                gateway_bus
                                    .publish(
                                        &relay_session_id,
                                        SessionProjectionEvent::Runtime {
                                            event,
                                            tool_instance_id: None,
                                            stream_range,
                                        },
                                    )
                                    .await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                gateway_bus
                                    .publish(
                                        &relay_session_id,
                                        SessionProjectionEvent::RuntimeStreamLagged { skipped },
                                    )
                                    .await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                },
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn remove_active_runtime_if_present(&self, session_id: &str) -> bool {
        self.remove_active_runtime(session_id).await.is_some()
    }

    pub(crate) async fn cowd_event_receiver(
        &self,
        session_id: &str,
    ) -> Option<tokio::sync::broadcast::Receiver<runtime::CowdEvent>> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        runtime_guard.cowd_bus().map(|bus| bus.subscribe())
    }

    #[cfg(test)]
    pub(crate) async fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
    ) -> Result<SessionInputReceipt, RuntimeTurnExecutionError> {
        self.admit_session_input_with_materialized(envelope)
            .await
            .map(|admission| admission.receipt)
    }

    #[cfg(test)]
    pub(crate) async fn admit_session_input_with_materialized(
        &self,
        envelope: SessionInputEnvelope,
    ) -> Result<SessionInputAdmission, RuntimeTurnExecutionError> {
        let session_id = envelope.session_id.clone();
        let content = envelope.content.clone();
        let request = runtime::RuntimeSessionIngressCommand {
            input_id: envelope.input_id.to_string(),
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
            session_generation: self
                .session_data
                .session_input_admission(&session_id)
                .await
                .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?
                .ok_or_else(|| {
                    RuntimeTurnExecutionError::NotFound(format!(
                        "session `{session_id}` does not exist"
                    ))
                })?
                .generation,
            decision: InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            created_at_ms: envelope.created_at.timestamp_millis().max(0) as u64,
            runtime_options_json: envelope
                .metadata
                .get("runtime_options")
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?,
        };
        let persisted = self
            .session_input_router
            .persist_input(&session_id, &content, &request)
            .await
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
        let stream = self.session_input_stream_for(&session_id).await?;
        let receipt = stream.admit(envelope, stream.runtime_state());
        let record_for_event = stream.record_snapshot(&receipt.input_id);
        self.emit_session_input_events(&session_id, &stream, Some(receipt.clone()));
        self.persist_session_input_domain_event(
            &session_id,
            SessionInputJournalKind::Received,
            Some(&receipt),
            record_for_event.as_ref(),
            &stream,
        )
        .await;
        let execution_graph_id =
            runtime::session_ingress_graph_id(&session_id, &request.request_id, &request.turn_id);
        let terminal_id = format!("turn-terminal:{}", request.request_id);
        self.record_live_execution(
            &session_id,
            execution_graph_id.clone(),
            request.turn_id.clone(),
        );
        self.projection_hub
            .publish(
                &session_id,
                SessionProjectionEvent::UserMessageCommitted {
                    session_id: session_id.clone(),
                    message_id: request.message_id.clone(),
                    sequence: persisted.sequence,
                    execution_id: execution_graph_id.clone(),
                    turn_id: request.turn_id.clone(),
                    content: content.clone(),
                    created_at_ms: request.created_at_ms,
                },
            )
            .await;
        Ok(SessionInputAdmission {
            execution_graph_id,
            receipt,
            materialized: None,
            terminal_id,
            turn_id: request.turn_id,
        })
    }

    /// Mirror the durable ingress worker's exact request-to-turn ownership in
    /// the in-process input projection. Projection absence after a restart is
    /// deliberately non-fatal: the durable outbox remains the execution
    /// source of truth.
    async fn bind_primary_ingress_projection(
        &self,
        outbox: &session::SessionRuntimeOutboxRecord,
        execution_id: &str,
    ) {
        let stream = match self.session_input_stream_for(&outbox.session_id).await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::debug!(
                    session_id = %outbox.session_id,
                    request_id = %outbox.request_id,
                    ?error,
                    "ingress execution has no in-process input projection to bind"
                );
                return;
            }
        };
        let turn_id = TurnId::from_string(outbox.turn_id.clone());
        match stream.bind_primary_ingress(&outbox.request_id, turn_id, execution_id) {
            Ok(Some(record)) => {
                let receipt = record.to_receipt();
                self.emit_session_input_events(&outbox.session_id, &stream, Some(receipt.clone()));
                self.persist_session_input_domain_event(
                    &outbox.session_id,
                    SessionInputJournalKind::IngressBound,
                    Some(&receipt),
                    Some(&record),
                    &stream,
                )
                .await;
            }
            Ok(None) => tracing::debug!(
                session_id = %outbox.session_id,
                request_id = %outbox.request_id,
                "durable ingress recovered without an in-process input projection"
            ),
            Err(error) => tracing::warn!(
                session_id = %outbox.session_id,
                request_id = %outbox.request_id,
                %error,
                "refused to bind a non-primary session input to ingress execution"
            ),
        }
    }

    async fn settle_primary_ingress_projection(
        &self,
        outbox: &session::SessionRuntimeOutboxRecord,
        execution_id: &str,
        terminal_id: &str,
    ) {
        let stream = match self.session_input_stream_for(&outbox.session_id).await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::debug!(
                    session_id = %outbox.session_id,
                    request_id = %outbox.request_id,
                    ?error,
                    "terminal settled without an in-process input projection"
                );
                return;
            }
        };
        let turn_id = TurnId::from_string(outbox.turn_id.clone());
        match stream.settle_primary_ingress(&outbox.request_id, &turn_id, execution_id, terminal_id)
        {
            Ok(Some(record)) => {
                let receipt = record.to_receipt();
                self.emit_session_input_events(&outbox.session_id, &stream, Some(receipt.clone()));
                self.persist_session_input_domain_event(
                    &outbox.session_id,
                    SessionInputJournalKind::IngressSettled,
                    Some(&receipt),
                    Some(&record),
                    &stream,
                )
                .await;
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(
                session_id = %outbox.session_id,
                request_id = %outbox.request_id,
                %error,
                "refused to settle an unrelated session input"
            ),
        }
    }

    async fn fail_primary_ingress_projection(
        &self,
        outbox: &session::SessionRuntimeOutboxRecord,
        error: &str,
    ) {
        let stream = match self.session_input_stream_for(&outbox.session_id).await {
            Ok(stream) => stream,
            Err(_) => return,
        };
        let turn_id = TurnId::from_string(outbox.turn_id.clone());
        match stream.fail_primary_ingress(&outbox.request_id, &turn_id, error) {
            Ok(Some(record)) => {
                let receipt = record.to_receipt();
                self.emit_session_input_events(&outbox.session_id, &stream, Some(receipt.clone()));
                self.persist_session_input_domain_event(
                    &outbox.session_id,
                    SessionInputJournalKind::IngressFailed,
                    Some(&receipt),
                    Some(&record),
                    &stream,
                )
                .await;
            }
            Ok(None) => {}
            Err(mutation_error) => tracing::warn!(
                session_id = %outbox.session_id,
                request_id = %outbox.request_id,
                %mutation_error,
                "refused to fail an unrelated session input"
            ),
        }
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
            SessionInputJournalKind::Cancelled,
            Some(&receipt),
            Some(&record),
            &stream,
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
        self.emit_session_input_events(session_id, &stream, Some(receipt.clone()));
        self.persist_session_input_domain_event(
            session_id,
            SessionInputJournalKind::Reclassified,
            Some(&receipt),
            Some(&record),
            &stream,
        )
        .await;
        Ok(receipt)
    }

    pub(crate) fn build_session_runtime_entry(
        &self,
        session: runtime::Session,
        session_id: &str,
        model: &str,
        system_prompt: Vec<String>,
    ) -> Result<crate::runtime_entry::GatewayRuntimeEntry, String> {
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
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
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
        kind: SessionInputJournalKind,
        receipt: Option<&SessionInputReceipt>,
        record: Option<&runtime::SessionInputRecord>,
        stream: &runtime::SessionInputStream,
    ) {
        if let Err(error) = self.ensure_session_domain_record(session_id).await {
            tracing::warn!(
                %session_id,
                kind = kind.as_str(),
                error = %error,
                "failed to ensure session before persisting session input runtime event"
            );
            return;
        }
        let payload = match serde_json::to_value(SessionInputDomainEventPayload {
            input: receipt,
            record,
            input_projection: stream.projection(),
            turn_inbox: stream.inbox_snapshot(None),
        }) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!(
                    %session_id,
                    kind = kind.as_str(),
                    error = %error,
                    "failed to encode typed Session input domain event"
                );
                return;
            }
        };
        if let Err(error) = self
            .session_data
            .append_session_input_journal(
                session_id,
                kind,
                payload,
                Utc::now().timestamp_millis().max(0) as u64,
            )
            .await
        {
            tracing::warn!(
                %session_id,
                kind = kind.as_str(),
                error = %error,
                "failed to persist session input runtime event"
            );
        }
    }

    async fn ensure_session_domain_record(
        &self,
        session_id: &str,
    ) -> Result<(), session::SessionError> {
        if self
            .session_data
            .stored_session(session_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        Err(session::SessionError::InvalidArgument(format!(
            "session {session_id} must be created through SessionActivationCoordinator before runtime events are persisted"
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
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
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
        let mut runtime_guard = lock_runtime_entry(&runtime_entry).await;
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
        let (cancellation_token, _turn_control) = self
            .install_active_turn_control(&turn_id.to_string(), session_id, None)
            .map_err(RuntimeTurnExecutionError::Runtime)?;
        let mut owned_runtime = {
            let mut runtime_guard = lock_runtime_entry(&runtime_entry).await;
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
            RuntimeTurnOwner::new(
                session_id.to_string(),
                Arc::clone(&runtime_entry),
                Arc::clone(&self.gateway_tasks),
                host,
            )
        };
        // Do not hold `GatewayRuntimeEntry`'s mutex while a provider/tool turn
        // awaits.  The host returns to the entry before this method settles
        // the receipt, so the next turn still observes a single owner.
        let turn_result = owned_runtime
            .runtime_mut()
            .map_err(RuntimeTurnExecutionError::Runtime)?
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
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), receipt.clone());
        receipt
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
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        Some(runtime_guard.session_async().await)
    }

    pub(crate) async fn compact_active_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionCompactResult>, session::SessionError> {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return Ok(None);
        };

        let mut runtime_guard = lock_runtime_entry(&runtime_entry).await;
        let (result, session_snapshot) = runtime_guard
            .compact_active_session()
            .await
            .map_err(|error| session::SessionError::Other(error.to_string()))?;
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

        let user_count = session
            .messages()
            .filter(|message| message.role == runtime::MessageRole::User)
            .count();
        let assistant_count = session
            .messages()
            .filter(|message| message.role == runtime::MessageRole::Assistant)
            .count();
        let tool_count = session
            .messages()
            .filter(|message| message.role == runtime::MessageRole::Tool)
            .count();

        let input: u32 = session
            .messages()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.input_tokens)
            .sum();
        let output: u32 = session
            .messages()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.output_tokens)
            .sum();

        let mut tool_usage = HashMap::new();
        for message in session.messages() {
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
            message_count: session.message_count(),
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
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        let session = runtime_guard.session_async().await;

        let total = session.message_count();
        let start = from_seq.unwrap_or(offset).min(total);
        let page = session.messages_page(start, limit);
        let messages: Vec<serde_json::Value> = page
            .iter()
            .enumerate()
            .map(|(page_index, msg)| {
                let sequence = start + page_index;
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
        let mut runtime_guard = lock_runtime_entry(&runtime_entry).await;
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
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        runtime_guard.last_context_envelope()
    }

    pub(crate) async fn last_context_turn_report(
        &self,
        session_id: &str,
    ) -> Option<harness_contract::context::ContextTurnReport> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
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

    pub(crate) async fn active_lease_session_ids(&self) -> Vec<String> {
        self.lease_registry.active_session_ids().await
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
            model_protocol::fingerprint::stable_hash_bytes(&payload)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::session_service::{
        presence::SessionPresenceLedger, repository::SessionRepository,
    };

    fn test_runtime_service_with_services(
        active_sessions: Arc<HotSessionPool>,
        store: Arc<session::UnifiedSessionStore>,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> RuntimeService {
        let projection_hub = crate::event_bus::SessionProjectionHub::new();
        let repository = Arc::new(SessionRepository::new(
            active_sessions.clone(),
            Some(Arc::clone(&store)),
            Arc::clone(&projection_hub),
        ));
        let presence = Arc::new(SessionPresenceLedger::new());
        let session_runtime_port =
            crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
                repository, presence,
            );
        runtime_services
            .install_session_ports(
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
            )
            .expect("test Session runtime port");
        RuntimeService::new(
            active_sessions.clone(),
            Arc::new(SessionLeaseRegistry::default()),
            session_runtime_port,
            projection_hub,
            Instant::now(),
            Arc::new(runtime::ProviderRegistry::empty()),
            Arc::new(runtime::UpgradeCoordinator::new()),
            runtime_services,
        )
        .expect("test runtime service")
    }

    fn test_runtime_service(
        active_sessions: Arc<HotSessionPool>,
        store: Option<Arc<session::UnifiedSessionStore>>,
    ) -> RuntimeService {
        let store = store.unwrap_or_else(|| {
            Arc::new(session::UnifiedSessionStore::open_in_memory().expect("test session store"))
        });
        let runtime_services =
            runtime::RuntimeServices::in_memory().expect("test runtime services");
        test_runtime_service_with_services(active_sessions, store, runtime_services)
    }

    #[tokio::test]
    async fn remove_active_runtime_keeps_other_session_restorations_isolated() {
        let service = test_runtime_service(Arc::new(HotSessionPool::default()), None);
        service
            .gateway_tasks
            .open_session("session-a")
            .await
            .unwrap();
        service
            .gateway_tasks
            .open_session("session-b")
            .await
            .unwrap();
        service
            .gateway_tasks
            .spawn(
                crate::runtime_host::task_set::GatewayTaskKind::RuntimeRestoration,
                Some("session-a".to_string()),
                |cancellation| async move {
                    cancellation.cancelled().await;
                },
            )
            .unwrap();
        service
            .gateway_tasks
            .spawn(
                crate::runtime_host::task_set::GatewayTaskKind::RuntimeRestoration,
                Some("session-b".to_string()),
                |cancellation| async move {
                    cancellation.cancelled().await;
                },
            )
            .unwrap();

        assert!(service.remove_active_runtime("session-a").await.is_none());

        assert_eq!(service.gateway_tasks.tracked_task_count(), 1);
        service
            .gateway_tasks
            .close_session_and_drain("session-b", Duration::from_secs(1))
            .await;
        assert_eq!(service.gateway_tasks.tracked_task_count(), 0);
        service.gateway_tasks.shutdown().await;
    }

    #[tokio::test]
    async fn restart_reuses_terminal_receipt_before_provider_runtime_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&session::SessionRecord {
                session_id: "restart-session".to_string(),
                platform: "test".to_string(),
                chat_id: "restart-session".to_string(),
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
        store
            .append_ingress_with_runtime_outbox(
                "restart-session",
                "user",
                Some(r#"[{"type":"text","text":"must not run"}]"#),
                1,
                &session::SessionRuntimeOutboxRequest {
                    input_id: "restart-input".to_string(),
                    request_id: "restart-request".to_string(),
                    turn_id: "restart-turn".to_string(),
                    message_id: "restart-message".to_string(),
                    session_generation: 1,
                    decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                    target_turn_id: None,
                    classification_json: None,
                    created_at_ms: 1,
                    runtime_options_json: None,
                },
            )
            .await
            .unwrap();
        let claim_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let claimed = store
            .claim_session_runtime_outbox("worker-a", claim_at, 30_000, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let claim_token = claimed.claim_token.clone().unwrap();
        let record = store
            .mark_session_runtime_outbox_running(
                "restart-request",
                "worker-a",
                claimed.session_generation,
                &claim_token,
                claimed.revision,
                claim_at,
            )
            .await
            .unwrap();
        let event_store_path = temp.path().join("runtime-events.sqlite");
        let runtime_event_store =
            Arc::new(runtime::RuntimeEventStore::try_open(&event_store_path).unwrap());
        let services = runtime::RuntimeServices::builder(&home, &workspace)
            .runtime_event_store(Arc::clone(&runtime_event_store))
            .build()
            .unwrap();
        let terminal_receipt = runtime_event_store
            .append_transaction_with_terminal(
                runtime::AppendTransactionRequest {
                    transaction_id: "restart-terminal-transaction".to_string(),
                    expected_streams: vec![runtime::ExpectedStreamRevision {
                        stream_id: "turn:restart-turn".to_string(),
                        expected_revision: 0,
                    }],
                    events: vec![runtime::RuntimeTransactionEventInput {
                        event: runtime::RuntimeEventInput {
                            stream_id: "turn:restart-turn".to_string(),
                            scope: runtime::RuntimeEventScope::SessionInput,
                            kind: "turn.terminal_committed".to_string(),
                            status: Some("completed".to_string()),
                            actor: Some("restart-test".to_string()),
                            refs: Vec::new(),
                            payload: serde_json::json!({"result": "done"}),
                        },
                        idempotency_key: Some("restart-terminal-event".to_string()),
                        schema_version: 1,
                    }],
                },
                runtime::SessionTerminalInput {
                    terminal_id: "turn-terminal:restart-request".to_string(),
                    message_id: "assistant-restart-message".to_string(),
                    session_id: "restart-session".to_string(),
                    execution_id: Some(runtime::session_ingress_graph_id(
                        "restart-session",
                        "restart-request",
                        "restart-turn",
                    )),
                    turn_id: Some("restart-turn".to_string()),
                    request_id: Some("restart-request".to_string()),
                    session_generation: Some(record.session_generation),
                    input_sequence: Some(record.sequence as u64),
                    input_claim_owner: record.claim_owner.clone(),
                    input_claim_token: record.claim_token.clone(),
                    input_claim_revision: record.claim_fence_epoch,
                    payload_ref: "assistant_json:\"done\"".to_string(),
                },
            )
            .unwrap();
        let terminal_port = services.session_terminal_delivery();
        let claimed_terminal = terminal_port
            .claim("delivery-worker", claim_at, 30_000, 1)
            .unwrap()
            .pop()
            .unwrap();
        store
            .commit_terminal_transcript_if_fenced(&session::SessionTerminalTranscriptCommit {
                terminal_message_id: "assistant-restart-message".to_string(),
                ingress_message_id: "restart-message".to_string(),
                session_id: "restart-session".to_string(),
                turn_id: "restart-turn".to_string(),
                messages: vec![session::SessionMessage {
                    stable_message_id: "assistant-restart-message".to_string(),
                    session_id: "restart-session".to_string(),
                    sequence: 0,
                    role: "assistant".to_string(),
                    content_json: r#"[{"type":"text","text":"done"}]"#.to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: claim_at,
                }],
                runtime_commit_cursor: terminal_receipt.commit_cursor,
                created_at_ms: claim_at,
                fence: session::SessionTerminalExecutionFence {
                    request_id: record.request_id.clone(),
                    input_sequence: record.sequence,
                    session_generation: record.session_generation,
                    claim_owner: record.claim_owner.clone().unwrap(),
                    claim_token: record.claim_token.clone().unwrap(),
                    claim_fence_epoch: record
                        .claim_fence_epoch
                        .expect("running input owns an immutable claim fence"),
                },
            })
            .await
            .unwrap();
        terminal_port
            .acknowledge(
                &claimed_terminal.terminal_id,
                "delivery-worker",
                claimed_terminal.revision,
                claim_at,
            )
            .unwrap();
        let first = test_runtime_service_with_services(
            Arc::new(HotSessionPool::new()),
            Arc::clone(&store),
            services,
        );
        assert_eq!(
            first
                .execute_ingress_record(&record, "must not run")
                .await
                .unwrap()
                .commit_cursor,
            terminal_receipt.commit_cursor
        );
        drop(first);
        drop(runtime_event_store);

        let restarted_event_store =
            Arc::new(runtime::RuntimeEventStore::try_open(&event_store_path).unwrap());
        let restarted_services = runtime::RuntimeServices::builder(&home, &workspace)
            .runtime_event_store(restarted_event_store)
            .build()
            .unwrap();
        let restarted = test_runtime_service_with_services(
            Arc::new(HotSessionPool::new()),
            store,
            restarted_services,
        );
        let receipt = restarted
            .execute_ingress_record(&record, "must still not run")
            .await
            .unwrap();
        assert_eq!(receipt.commit_cursor, terminal_receipt.commit_cursor);
        assert_eq!(
            receipt.graph_id,
            runtime::session_ingress_graph_id("restart-session", "restart-request", "restart-turn")
        );
    }

    #[tokio::test]
    async fn recovered_terminal_settles_the_exact_primary_input_projection() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let runtime_event_store =
            Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap());
        let runtime_services = runtime::RuntimeServices::builder(&home, &workspace)
            .runtime_event_store(Arc::clone(&runtime_event_store))
            .build()
            .unwrap();
        let service = test_runtime_service_with_services(
            Arc::new(HotSessionPool::default()),
            Arc::clone(&store),
            runtime_services,
        );
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&session::SessionRecord {
                session_id: "projection-session".to_string(),
                platform: "test".to_string(),
                chat_id: "projection-session".to_string(),
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
            .expect("test session");
        service
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                "projection-session".to_string(),
                runtime::SessionInputStream::new("projection-session"),
            );
        let admission = service
            .admit_session_input_with_materialized(
                SessionInputEnvelope::text(
                    "projection-session",
                    harness_contract::turn::InputSourceKind::Webui,
                    "already supplied to ingress",
                )
                .with_idempotency_key("projection-primary"),
            )
            .await
            .expect("admission");
        let queued = store
            .get_session_runtime_outbox("projection-primary")
            .await
            .expect("outbox lookup")
            .expect("persisted ingress");
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let claimed = store
            .claim_session_runtime_outbox("projection-worker", now_ms, 30_000, 1)
            .await
            .expect("claim persisted ingress")
            .into_iter()
            .next()
            .expect("claim result");
        assert_eq!(claimed.request_id, queued.request_id);
        let claim_token = claimed
            .claim_token
            .clone()
            .expect("claim token is part of the execution fence");
        let record = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "projection-worker",
                claimed.session_generation,
                &claim_token,
                claimed.revision,
                now_ms,
            )
            .await
            .expect("mark claimed ingress running");
        let terminal_commit = runtime_event_store
            .append_transaction_with_terminal(
                runtime::AppendTransactionRequest {
                    transaction_id: "projection-terminal-transaction".to_string(),
                    expected_streams: vec![runtime::ExpectedStreamRevision {
                        stream_id: "turn:projection-primary".to_string(),
                        expected_revision: 0,
                    }],
                    events: vec![runtime::RuntimeTransactionEventInput {
                        event: runtime::RuntimeEventInput {
                            stream_id: "turn:projection-primary".to_string(),
                            scope: runtime::RuntimeEventScope::SessionInput,
                            kind: "turn.terminal_committed".to_string(),
                            status: Some("completed".to_string()),
                            actor: Some("projection-test".to_string()),
                            refs: Vec::new(),
                            payload: serde_json::json!({"result": "done"}),
                        },
                        idempotency_key: Some("projection-terminal-event".to_string()),
                        schema_version: 1,
                    }],
                },
                runtime::SessionTerminalInput {
                    terminal_id: admission.terminal_id.clone(),
                    message_id: "assistant-projection-primary".to_string(),
                    session_id: record.session_id.clone(),
                    execution_id: Some(admission.execution_graph_id.clone()),
                    turn_id: Some(record.turn_id.clone()),
                    request_id: Some(record.request_id.clone()),
                    session_generation: Some(record.session_generation),
                    input_sequence: Some(record.sequence as u64),
                    input_claim_owner: record.claim_owner.clone(),
                    input_claim_token: record.claim_token.clone(),
                    input_claim_revision: record.claim_fence_epoch,
                    payload_ref: "assistant_json:\"done\"".to_string(),
                },
            )
            .expect("terminal and its exact Session fence commit atomically");
        let persisted_terminal = service
            .runtime_services()
            .session_terminal_delivery()
            .get(&admission.terminal_id)
            .expect("terminal lookup")
            .expect("terminal persisted");
        assert_eq!(
            persisted_terminal.request_id.as_deref(),
            Some(record.request_id.as_str())
        );
        assert_eq!(
            persisted_terminal.input_claim_revision,
            record.claim_fence_epoch
        );
        assert_eq!(
            persisted_terminal.commit_cursor,
            terminal_commit.commit_cursor
        );
        assert_eq!(persisted_terminal.input_claim_owner, record.claim_owner);
        assert_eq!(persisted_terminal.input_claim_token, record.claim_token);
        assert_eq!(
            persisted_terminal.session_generation,
            Some(record.session_generation)
        );
        assert_eq!(
            persisted_terminal.turn_id.as_deref(),
            Some(record.turn_id.as_str())
        );
        assert_eq!(
            persisted_terminal.execution_id.as_deref(),
            Some(admission.execution_graph_id.as_str())
        );
        assert_eq!(persisted_terminal.session_id, record.session_id);
        assert_eq!(persisted_terminal.terminal_id, admission.terminal_id);
        assert_eq!(
            persisted_terminal.message_id,
            "assistant-projection-primary"
        );
        assert_eq!(persisted_terminal.payload_ref, "assistant_json:\"done\"");
        assert_eq!(persisted_terminal.status, "pending");
        assert_eq!(persisted_terminal.revision, 0);
        assert_eq!(persisted_terminal.attempts, 0);
        assert_eq!(persisted_terminal.next_attempt_at_ms, None);
        assert_eq!(persisted_terminal.claim_owner, None);
        assert_eq!(persisted_terminal.claim_expires_at_ms, None);
        assert_eq!(persisted_terminal.failure_class, None);
        assert_eq!(persisted_terminal.last_error, None);
        assert_eq!(persisted_terminal.materialized_at_ms, None);
        let terminal_claim = service
            .runtime_services()
            .session_terminal_delivery()
            .claim("projection-delivery", now_ms.saturating_add(1), 30_000, 1)
            .expect("claim terminal delivery")
            .into_iter()
            .find(|terminal| terminal.terminal_id == admission.terminal_id)
            .expect("terminal delivery claim");
        assert_eq!(
            terminal_claim.input_claim_revision,
            record.claim_fence_epoch
        );
        assert!(terminal_claim.revision > persisted_terminal.revision);
        service
            .runtime_services()
            .session_terminal_delivery()
            .acknowledge(
                &terminal_claim.terminal_id,
                "projection-delivery",
                terminal_claim.revision,
                now_ms.saturating_add(2),
            )
            .expect("materialize recovered terminal");

        service
            .execute_ingress_record(&record, "must not call provider")
            .await
            .expect("recovered terminal is delivered");

        let projection = service
            .session_input_projection("projection-session")
            .await
            .expect("input projection");
        assert_eq!(projection.pending_count, 0);
        assert_eq!(projection.consumed_count, 1);
        let stream = service
            .session_inputs
            .lock()
            .unwrap()
            .get("projection-session")
            .cloned()
            .expect("in-process stream");
        let primary = stream
            .record_snapshot(&admission.receipt.input_id)
            .expect("primary record");
        assert_eq!(
            primary.status,
            harness_contract::turn::SessionInputStatus::Consumed
        );
        assert_eq!(
            primary.checkpoint,
            Some(harness_contract::turn::TurnInputCheckpoint::IngressDispatched)
        );
        assert!(primary
            .evidence_refs
            .iter()
            .any(|reference| reference
                == &format!("execution_graph:{}", admission.execution_graph_id)));
        assert!(primary
            .evidence_refs
            .iter()
            .any(|reference| reference == &format!("terminal:{}", admission.terminal_id)));
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
        let service = test_runtime_service(Arc::new(HotSessionPool::default()), None);

        let value = service.status_value();
        assert_eq!(value["ok"], true);
        assert_eq!(value["runtime_host"], "gateway-runtime-host");
        let removed_legacy_key = ["dae", "mon"].concat();
        assert!(value.get(&removed_legacy_key).is_none());
        assert_eq!(value["active_sessions"], 0);
    }

    #[tokio::test]
    async fn runtime_service_snapshot_reports_lease_projection() {
        let service = test_runtime_service(Arc::new(HotSessionPool::default()), None);

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
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let service =
            test_runtime_service(Arc::new(HotSessionPool::default()), Some(store.clone()));

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
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&session::SessionRecord {
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
        let active_sessions = Arc::new(HotSessionPool::default());
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
        let kinds = page
            .events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "session.input.accepted.v1",
                "session.input.classified.v1",
                "session.input.queued.v1",
            ],
            "durable admission owns one canonical accepted/classified/queued timeline"
        );
        let event = &page.events[0];
        assert_eq!(event.payload["input_id"], receipt.input_id.to_string());
        assert_eq!(event.payload["message_id"], receipt.input_id.to_string());
        assert_eq!(event.status.as_deref(), Some("accepted"));
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
        let service = test_runtime_service(Arc::new(HotSessionPool::default()), None);

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
        assert_eq!(snapshot["turns"][0]["task_id"], "task-turn");
        assert_eq!(snapshot["turns"][0]["turn_id"], running.turn_id.to_string());
        assert_eq!(snapshot["turns"][0]["session_id"], "session-turn");
    }

    #[test]
    fn runtime_event_relay_preserves_event_type_without_inventing_lifecycle() {
        let text = SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
            text: "partial".to_string(),
        })
        .to_transport_value();
        assert_eq!(text["type"], "TextDelta");
        assert_eq!(text["text"], "partial");

        let completed = SessionProjectionEvent::runtime(runtime::CowdEvent::TurnComplete {
            assistant_text: "draft".to_string(),
            iterations: 2,
        })
        .to_transport_value();
        assert_eq!(completed["type"], "TurnComplete");
        assert_eq!(completed["assistant_text"], "draft");
        assert!(completed.get("committed").is_none());

        let scoped = SessionProjectionEvent::runtime(runtime::CowdEvent::ExecutionScoped {
            context: runtime::CowdExecutionContext {
                execution_id: "execution-1".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
            },
            event: Box::new(runtime::CowdEvent::ExecutionPhase {
                status: ExecutionLiveStatus::CallingModel,
                detail: Some("requesting model".to_string()),
            }),
        })
        .to_transport_value();
        assert_eq!(scoped["type"], "ExecutionPhase");
        assert_eq!(scoped["execution_id"], "execution-1");
        assert_eq!(scoped["turn_id"], "turn-1");
    }

    #[tokio::test]
    async fn runtime_event_relay_forwards_render_events_to_gateway_session_bus() {
        let service = test_runtime_service(Arc::new(HotSessionPool::default()), None);
        let gateway_bus = Arc::clone(&service.projection_hub);
        let mut rx = gateway_bus.subscribe("relay-session", 8).await;
        let old_runtime_bus = runtime::CowdEventBus::new();
        service
            .install_session_event_relay("relay-session", old_runtime_bus.clone())
            .await
            .unwrap();
        old_runtime_bus.emit(runtime::CowdEvent::TextDelta {
            text: "before replacement".to_string(),
        });
        let payload = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("relay should forward within bounded time")
            .expect("gateway subscriber remains open");
        let payload = payload.to_transport_value();
        assert_eq!(payload["type"], "TextDelta");
        assert_eq!(payload["text"], "before replacement");

        let current_runtime_bus = runtime::CowdEventBus::new();
        service
            .install_session_event_relay("relay-session", current_runtime_bus.clone())
            .await
            .unwrap();
        old_runtime_bus.emit(runtime::CowdEvent::TextDelta {
            text: "stale relay".to_string(),
        });
        current_runtime_bus.emit(runtime::CowdEvent::TextDelta {
            text: "current relay".to_string(),
        });
        let payload = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("replacement relay should forward within bounded time")
            .expect("gateway subscriber remains open")
            .to_transport_value();
        assert_eq!(payload["text"], "current relay");
        assert_eq!(service.gateway_tasks.tracked_task_count(), 1);

        service.remove_active_runtime("relay-session").await;
        assert_eq!(service.gateway_tasks.tracked_task_count(), 0);
        current_runtime_bus.emit(runtime::CowdEvent::TextDelta {
            text: "after removal".to_string(),
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "removed relay must not forward additional events"
        );
        service.gateway_tasks.shutdown().await;
    }

    #[test]
    fn session_execution_index_exposes_running_only_and_retains_terminal_reference() {
        let service = test_runtime_service(Arc::new(HotSessionPool::default()), None);
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
            &[],
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
    fn session_cancel_reaches_the_runtime_turn_control_instead_of_only_emitting_ui_state() {
        let service = test_runtime_service(Arc::new(HotSessionPool::default()), None);
        let (cancellation, _guard) = service
            .install_active_turn_control(
                "turn-cancel",
                "session-cancel",
                Some("execution-cancel".to_string()),
            )
            .unwrap();

        let cancelled =
            service.cancel_active_session("session-cancel", "evaluator timeout isolation");

        assert_eq!(cancelled, vec!["execution-cancel"]);
        assert!(cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn process_shutdown_rejects_new_turns_and_waits_for_active_turn_guard() {
        let service = Arc::new(test_runtime_service(
            Arc::new(HotSessionPool::default()),
            None,
        ));
        let (cancellation, guard) = service
            .install_active_turn_control(
                "turn-shutdown",
                "session-shutdown",
                Some("execution-shutdown".to_string()),
            )
            .unwrap();

        let cancelled =
            service.stop_accepting_and_cancel_active_turns("Gateway process shutdown test");
        assert_eq!(cancelled, vec!["execution-shutdown"]);
        assert!(cancellation.is_cancelled());
        assert_eq!(service.active_turn_count(), 1);
        assert!(service
            .install_active_turn_control("turn-late", "session-shutdown", None)
            .is_err());

        let waiter_service = Arc::clone(&service);
        let waiter = tokio::spawn(async move {
            waiter_service
                .wait_for_active_turns(cancelled.len(), Duration::from_secs(1))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        drop(guard);
        let report = waiter.await.unwrap();
        assert_eq!(report.cancelled, 1);
        assert_eq!(report.drained, 1);
        assert!(report.remaining_turn_ids.is_empty());
        assert_eq!(service.active_turn_count(), 0);
        service.gateway_tasks.shutdown().await;
    }

    #[test]
    fn durable_ingress_index_recovers_execution_identity_without_mixing_cursors() {
        let records = vec![
            session::SessionRuntimeOutboxRecord {
                input_id: "input-complete".to_string(),
                request_id: "request-complete".to_string(),
                turn_id: "turn-complete".to_string(),
                message_id: "message-complete".to_string(),
                session_id: "session-recovery".to_string(),
                sequence: 1,
                session_generation: 1,
                decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: None,
                status: session::SessionRuntimeInputStatus::Completed,
                runtime_commit_cursor: Some(44),
                attempts: 1,
                next_attempt_at_ms: 0,
                claim_owner: None,
                claim_token: None,
                claim_fence_epoch: None,
                claim_expires_at_ms: None,
                failure_class: None,
                last_error: None,
                revision: 9,
                created_at_ms: 10,
                updated_at_ms: 20,
                terminal_at_ms: Some(20),
                runtime_options_json: None,
            },
            session::SessionRuntimeOutboxRecord {
                input_id: "input-pending".to_string(),
                request_id: "request-pending".to_string(),
                turn_id: "turn-pending".to_string(),
                message_id: "message-pending".to_string(),
                session_id: "session-recovery".to_string(),
                sequence: 2,
                session_generation: 1,
                decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: None,
                status: session::SessionRuntimeInputStatus::Queued,
                runtime_commit_cursor: None,
                attempts: 0,
                next_attempt_at_ms: 0,
                claim_owner: None,
                claim_token: None,
                claim_fence_epoch: None,
                claim_expires_at_ms: None,
                failure_class: None,
                last_error: None,
                revision: 3,
                created_at_ms: 21,
                updated_at_ms: 30,
                terminal_at_ms: None,
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

    #[test]
    fn durable_materialization_cannot_reclassify_blocked_live_outcome_as_complete() {
        let execution_id = "session-ingress-graph:blocked".to_string();
        let volatile = SessionExecutionIndexProjection {
            session_id: "session-blocked".to_string(),
            active_execution_ids: Vec::new(),
            latest_execution_id: Some(execution_id.clone()),
            latest_status: Some(ExecutionLiveStatus::Error),
            latest_live_revision: Some(7),
            last_progress_at_ms: Some(100),
            terminal_ref: Some("turn-terminal:blocked".to_string()),
        };
        let durable = SessionExecutionIndexProjection {
            session_id: "session-blocked".to_string(),
            active_execution_ids: Vec::new(),
            latest_execution_id: Some(execution_id),
            latest_status: Some(ExecutionLiveStatus::Complete),
            latest_live_revision: None,
            last_progress_at_ms: Some(110),
            terminal_ref: Some("turn-terminal:blocked".to_string()),
        };

        let reconciled = reconcile_session_execution_indices(volatile, durable);

        assert_eq!(reconciled.latest_status, Some(ExecutionLiveStatus::Error));
        assert_eq!(reconciled.latest_live_revision, Some(7));
        assert_eq!(reconciled.last_progress_at_ms, Some(110));
        assert_eq!(
            reconciled.terminal_ref.as_deref(),
            Some("turn-terminal:blocked")
        );
    }
}
