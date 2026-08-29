use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::active_session::{ActiveSessionDirectory, PreparedActiveSession, SessionRelayLease};
use crate::event_bus::{RuntimeStreamRange, SessionProjectionEvent, SessionProjectionHub};
use crate::runtime_boundary::{
    RuntimeBoundaryClock, RuntimeBoundarySnapshot, RuntimeBoundaryStatus,
};
use crate::runtime_protocol::{RuntimeErrorKind, RuntimeRequest, RuntimeResponse};
use crate::session_runtime_data_port::SessionInputJournalKind;
use harness_contract::{
    context::ContextTurnReport,
    projection::{
        ExecutionLiveState, ExecutionLiveStatus, SessionExecutionEntryProjection,
        SessionExecutionIndexProjection,
    },
    turn::{
        InputPayloadKind, InputRelationKind, InputRelationProposal, InputRoutingDecision,
        InputRoutingReason, InputSourceKind, SessionInputEnvelope, SessionInputId,
        SessionInputProjection, SessionInputReceipt, SessionInputStatus, TurnEvent, TurnId,
        TurnInboxSnapshot, TurnInput, TurnJournalEnvelope, TurnJournalPhase, TurnReceipt,
        TurnStatus,
    },
};
use session::SessionLeaseRegistry;

use crate::services::{
    ActiveMessagesPage, SessionCompactResult, SessionMessageCounts, SessionStatsSnapshot,
    SessionTokenCounts,
};

pub(crate) const SESSION_RUNTIME_BUSY_ERROR: &str = "session runtime is already executing a turn";

fn normalize_configured_model(model: Option<String>) -> Option<String> {
    model
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

#[path = "session_materializer.rs"]
mod session_materializer;

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

fn turn_status_is_terminal(status: &TurnStatus) -> bool {
    matches!(
        status,
        TurnStatus::Completed
            | TurnStatus::Failed
            | TurnStatus::Denied
            | TurnStatus::Fallback
            | TurnStatus::Cancelled
    )
}

/// Operational cancellation is deliberately separate from the durable live
/// projection.  RuntimeServices owns the lifecycle state; Gateway retains
/// only a short-lived handle required to signal an in-flight host.
#[derive(Clone)]
struct ActiveTurnControl {
    session_id: String,
    execution_id: Option<String>,
    policy_revision: u64,
    requested_sandbox_posture: harness_contract::policy::SandboxPosture,
    effective_sandbox_posture: harness_contract::policy::SandboxPosture,
    cancellation_token: runtime::CancellationToken,
}

#[derive(Clone)]
struct FrozenPolicyTransition {
    transition_id: String,
    effective_revision: u64,
    desired_revision: u64,
}

struct ActiveTurnRegistryState {
    accepting: bool,
    controls: BTreeMap<String, ActiveTurnControl>,
    frozen_sessions: BTreeMap<String, FrozenPolicyTransition>,
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
                frozen_sessions: BTreeMap::new(),
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
    // The durable Session outbox carries both independent turn executions and
    // messages attached to an existing turn. Supplements/control messages do
    // not own an ingress graph or terminal and must never replace the target
    // execution in the discovery index.
    let mut ordered = records
        .iter()
        .filter(|record| {
            record.target_turn_id.is_none()
                && !matches!(
                    record.decision,
                    harness_contract::turn::InputRoutingDecision::SupplementCurrentTurn
                        | harness_contract::turn::InputRoutingDecision::ControlOrApproval
                        | harness_contract::turn::InputRoutingDecision::RejectDuplicate
                        | harness_contract::turn::InputRoutingDecision::RejectPolicy
                )
        })
        .collect::<Vec<_>>();
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
        session::SessionRuntimeInputStatus::Attached => ExecutionLiveStatus::Thinking,
        session::SessionRuntimeInputStatus::Completed => ExecutionLiveStatus::Complete,
        // Filtered above. Keep a defensive mapping so malformed historical
        // records cannot manufacture a non-terminal execution.
        session::SessionRuntimeInputStatus::Supplemented => ExecutionLiveStatus::Complete,
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
        executions: ordered
            .iter()
            .map(|record| SessionExecutionEntryProjection {
                execution_id: execution_for(record),
                graph_id: Some(execution_for(record)),
                turn_id: Some(record.turn_id.clone()),
                status: status_for(record),
                live_revision: None,
                started_at_ms: Some(record.created_at_ms),
                updated_at_ms: record.updated_at_ms,
                terminal_ref: matches!(
                    record.status,
                    session::SessionRuntimeInputStatus::Completed
                )
                .then(|| format!("turn-terminal:{}", record.request_id)),
            })
            .collect(),
        active_execution_ids: ordered
            .iter()
            .filter(|record| !is_live_terminal(status_for(record)))
            .map(|record| execution_for(record))
            .collect(),
        latest_execution_id: latest.map(execution_for),
        latest_graph_id: latest.map(execution_for),
        latest_status,
        latest_live_revision: None,
        last_progress_at_ms: latest.map(|record| record.updated_at_ms),
        terminal_ref: latest
            .filter(|record| matches!(record.status, session::SessionRuntimeInputStatus::Completed))
            .map(|record| format!("turn-terminal:{}", record.request_id)),
    }
}

fn reconcile_session_execution_indices(
    volatile: SessionExecutionIndexProjection,
    mut durable: SessionExecutionIndexProjection,
) -> SessionExecutionIndexProjection {
    if durable.latest_execution_id.is_none() {
        return volatile;
    }
    let mut durable_execution_ids = durable
        .executions
        .iter()
        .map(|entry| entry.execution_id.clone())
        .collect::<BTreeSet<_>>();
    if let Some(latest_execution_id) = durable.latest_execution_id.as_ref() {
        durable_execution_ids.insert(latest_execution_id.clone());
    }
    let executions = reconcile_session_execution_entries(&volatile.executions, &durable.executions)
        .into_iter()
        .filter(|entry| durable_execution_ids.contains(entry.execution_id.as_str()))
        .collect::<Vec<_>>();
    durable.executions = executions;
    let same_execution = volatile.latest_execution_id.is_some()
        && volatile.latest_execution_id == durable.latest_execution_id;
    let volatile_has_terminal_outcome = volatile.latest_status.is_some_and(is_live_terminal);
    if same_execution && volatile_has_terminal_outcome {
        // The outbox's Materialized state means only that the terminal message
        // reached the durable transcript. It is not an execution-success
        // verdict. A persisted Runtime terminal outcome for the same execution
        // is authoritative even when outbox bookkeeping has a later timestamp.
        let mut selected = volatile;
        selected.executions = durable.executions;
        selected.active_execution_ids = durable.active_execution_ids;
        selected.terminal_ref = selected.terminal_ref.or(durable.terminal_ref);
        selected.last_progress_at_ms =
            match (selected.last_progress_at_ms, durable.last_progress_at_ms) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (left, right) => left.or(right),
            };
        return selected;
    }

    // Session discovery is a Turn-root index. A newer child Agent live record
    // must never replace the durable Session ingress identity. For the same
    // root, Runtime live state may enrich the outbox; for different identities
    // the durable root is authoritative regardless of timestamps.
    let mut selected = durable;
    if same_execution {
        selected.latest_graph_id = volatile.latest_graph_id.or(selected.latest_graph_id);
        if !selected.latest_status.is_some_and(is_live_terminal) {
            selected.latest_status = volatile.latest_status.or(selected.latest_status);
            selected.latest_live_revision = volatile
                .latest_live_revision
                .or(selected.latest_live_revision);
        }
    } else if let Some(root_entry) = selected
        .latest_execution_id
        .as_ref()
        .and_then(|execution_id| {
            selected
                .executions
                .iter()
                .find(|entry| &entry.execution_id == execution_id)
        })
        .cloned()
    {
        selected.latest_graph_id = root_entry.graph_id.clone();
        selected.latest_status = Some(root_entry.status);
        selected.latest_live_revision = root_entry.live_revision;
        selected.last_progress_at_ms = Some(
            selected
                .last_progress_at_ms
                .map_or(root_entry.updated_at_ms, |value| {
                    value.max(root_entry.updated_at_ms)
                }),
        );
        selected.terminal_ref = root_entry.terminal_ref.clone().or(selected.terminal_ref);
    }
    let statuses = selected
        .executions
        .iter()
        .map(|entry| (entry.execution_id.as_str(), entry.status))
        .collect::<BTreeMap<_, _>>();
    selected.active_execution_ids.retain(|execution_id| {
        statuses
            .get(execution_id.as_str())
            .is_some_and(|status| !is_live_terminal(*status))
    });
    selected
}

fn reconcile_session_execution_entries(
    volatile: &[SessionExecutionEntryProjection],
    durable: &[SessionExecutionEntryProjection],
) -> Vec<SessionExecutionEntryProjection> {
    let mut entries = BTreeMap::<String, SessionExecutionEntryProjection>::new();
    for entry in durable {
        entries.insert(entry.execution_id.clone(), entry.clone());
    }
    for entry in volatile {
        entries
            .entry(entry.execution_id.clone())
            .and_modify(|persisted| {
                persisted.graph_id = entry
                    .graph_id
                    .clone()
                    .or_else(|| persisted.graph_id.clone());
                persisted.turn_id = entry.turn_id.clone().or_else(|| persisted.turn_id.clone());
                // Runtime owns an exact terminal verdict (including blocked or
                // failed), while the durable ingress owns terminal completion
                // when an old live checkpoint still reports an active phase.
                // Never let a stale active status reopen a durable terminal.
                if is_live_terminal(entry.status) || !is_live_terminal(persisted.status) {
                    persisted.status = entry.status;
                }
                persisted.live_revision = entry.live_revision.or(persisted.live_revision);
                persisted.started_at_ms = entry.started_at_ms.or(persisted.started_at_ms);
                persisted.updated_at_ms = persisted.updated_at_ms.max(entry.updated_at_ms);
                persisted.terminal_ref = entry
                    .terminal_ref
                    .clone()
                    .or_else(|| persisted.terminal_ref.clone());
            })
            .or_insert_with(|| entry.clone());
    }
    let mut entries = entries.into_values().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.started_at_ms
            .unwrap_or(left.updated_at_ms)
            .cmp(&right.started_at_ms.unwrap_or(right.updated_at_ms))
            .then_with(|| left.updated_at_ms.cmp(&right.updated_at_ms))
            .then_with(|| left.execution_id.cmp(&right.execution_id))
    });
    entries
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

fn session_execution_policy_from_record(
    record: &session::SessionRecord,
    default_policy: &runtime::SessionExecutionPolicy,
) -> runtime::SessionExecutionPolicy {
    stored_session_execution_policy(record).unwrap_or_else(|| default_policy.clone())
}

fn stored_session_execution_policy(
    record: &session::SessionRecord,
) -> Option<runtime::SessionExecutionPolicy> {
    if let Some(state) = stored_session_execution_policy_state(record) {
        return Some(state.effective);
    }
    let value = record
        .metadata_json
        .as_deref()
        .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
        .and_then(|metadata| metadata.pointer("/execution_policy").cloned())?;
    serde_json::from_value::<runtime::SessionExecutionPolicy>(value).ok()
}

fn stored_session_execution_policy_state(
    record: &session::SessionRecord,
) -> Option<harness_contract::policy::SessionExecutionPolicyState> {
    let value = record
        .metadata_json
        .as_deref()
        .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
        .and_then(|metadata| metadata.pointer("/execution_policy_state").cloned())?;
    serde_json::from_value(value).ok()
}

fn execution_policy_defaults_match(
    left: &runtime::SessionExecutionPolicy,
    right: &runtime::SessionExecutionPolicy,
) -> bool {
    left.autonomy_profile == right.autonomy_profile
        && left.permission_mode == right.permission_mode
        && left.sandbox_posture == right.sandbox_posture
        && left.approval_profile == right.approval_profile
        && left.interruption_policy == right.interruption_policy
}

#[derive(Clone)]
pub(crate) struct RuntimeService {
    sessions: Arc<ActiveSessionDirectory>,
    lease_registry: Arc<SessionLeaseRegistry>,
    session_data: Arc<crate::session_runtime_data_port::GatewaySessionRuntimePort>,
    projection_hub: Arc<SessionProjectionHub>,
    started_at: Instant,
    turns: Arc<Mutex<BTreeMap<String, TurnReceipt>>>,
    active_turns: Arc<ActiveTurnRegistry>,
    gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
    policy_transition_stripes: Arc<[Arc<tokio::sync::Mutex<()>>]>,
    hydration_attempts: Arc<AtomicU64>,
    hydration_body_reads: Arc<AtomicU64>,
    hydration_body_bytes: Arc<AtomicU64>,
    provider_registry: Arc<runtime::ProviderRegistry>,
    configured_model: Arc<RwLock<Option<String>>>,
    upgrade_coordinator: Arc<runtime::UpgradeCoordinator>,
    config_reload: Arc<crate::runtime_host::config_reload::ConfigReloadState>,
    tool_host: Arc<tools::ToolHost>,
    session_bootstrap: Arc<RwLock<crate::runtime_bootstrap::RuntimeSessionBootstrapSnapshot>>,
    resource_capabilities: runtime::ResourceCapabilityIndex,
    runtime_services: Arc<runtime::RuntimeServices>,
    session_input_router: Arc<runtime::SessionInputRouter>,
    execution_policy_default: Arc<RwLock<runtime::SessionExecutionPolicy>>,
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
    pub(crate) message_id: String,
    pub(crate) message_sequence: usize,
}

impl RuntimeService {
    #[cfg(test)]
    fn install_test_session_policy(
        &self,
        session_id: &str,
        policy: runtime::SessionExecutionPolicy,
    ) {
        let control = runtime::permissions::SessionExecutionPolicyControl::from_policy(policy);
        self.sessions
            .install_policy_fixture(session_id, control.clone());
        self.runtime_services
            .publish_session_execution_policy(session_id.to_string(), control);
    }

    #[cfg(test)]
    fn install_test_session_input(&self, session_id: &str, input: runtime::SessionInputStream) {
        self.sessions.install_input_fixture(session_id, input);
    }

    #[cfg(test)]
    fn test_session_input(&self, session_id: &str) -> Option<runtime::SessionInputStream> {
        self.sessions
            .session(session_id)
            .and_then(|session| session.input())
    }

    #[must_use]
    pub(crate) fn new(
        sessions: Arc<ActiveSessionDirectory>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_data: Arc<crate::session_runtime_data_port::GatewaySessionRuntimePort>,
        projection_hub: Arc<SessionProjectionHub>,
        started_at: Instant,
        configured_model: Option<String>,
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
            configured_model,
            provider_registry,
            upgrade_coordinator,
            runtime_services,
            crate::runtime_host::task_set::GatewayRuntimeTaskSet::new(Duration::from_secs(5)),
        )
    }

    #[must_use]
    pub(crate) fn new_with_gateway_tasks(
        sessions: Arc<ActiveSessionDirectory>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_data: Arc<crate::session_runtime_data_port::GatewaySessionRuntimePort>,
        projection_hub: Arc<SessionProjectionHub>,
        started_at: Instant,
        configured_model: Option<String>,
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
        let configured_model = normalize_configured_model(configured_model);
        if let Some(model) = configured_model.as_deref() {
            if provider_registry.pin().resolve(model).is_none() {
                return Err(format!(
                    "configured default model '{model}' is not declared by any configured provider"
                ));
            }
        }
        Ok(Self {
            sessions,
            lease_registry,
            session_data,
            projection_hub,
            started_at,
            turns: Arc::new(Mutex::new(BTreeMap::new())),
            active_turns: Arc::new(ActiveTurnRegistry::new()),
            gateway_tasks,
            policy_transition_stripes: (0..64)
                .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                .collect::<Vec<_>>()
                .into(),
            hydration_attempts: Arc::new(AtomicU64::new(0)),
            hydration_body_reads: Arc::new(AtomicU64::new(0)),
            hydration_body_bytes: Arc::new(AtomicU64::new(0)),
            provider_registry,
            configured_model: Arc::new(RwLock::new(configured_model)),
            upgrade_coordinator,
            config_reload: Arc::new(crate::runtime_host::config_reload::ConfigReloadState::new()),
            tool_host: Arc::new(
                tools::ToolHost::builtin("gateway-runtime", workspace_root)
                    .with_authorization_lease_verifier(Arc::new(
                        runtime::AuthorizationNegotiator::verify_lease_signature,
                    )),
            ),
            session_bootstrap: Arc::new(RwLock::new(
                crate::runtime_bootstrap::RuntimeSessionBootstrapSnapshot {
                    feature_config: runtime::RuntimeFeatureConfig::default(),
                    tool_registry: crate::runtime_bootstrap::GatewayToolRegistry::builtin(),
                    plugin_registry: plugins::PluginRegistry::default(),
                },
            )),
            resource_capabilities,
            runtime_services,
            session_input_router,
            execution_policy_default: Arc::new(RwLock::new(
                runtime::SessionExecutionPolicy::from_defaults(
                    runtime::PermissionMode::WorkspaceWrite,
                    runtime::ApprovalProfile::Balanced,
                ),
            )),
        })
    }

    #[must_use]
    pub(crate) fn with_permission_mode(mut self, permission_mode: runtime::PermissionMode) -> Self {
        let approval_profile = self.default_execution_policy().approval_profile;
        self.execution_policy_default = Arc::new(RwLock::new(
            runtime::SessionExecutionPolicy::from_defaults(permission_mode, approval_profile),
        ));
        self
    }

    #[must_use]
    pub(crate) fn with_approval_profile(
        mut self,
        approval_profile: runtime::ApprovalProfile,
    ) -> Self {
        let permission_mode = self.default_execution_policy().permission_mode;
        self.execution_policy_default = Arc::new(RwLock::new(
            runtime::SessionExecutionPolicy::from_defaults(permission_mode, approval_profile),
        ));
        self
    }

    fn default_execution_policy(&self) -> runtime::SessionExecutionPolicy {
        self.execution_policy_default
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Public accessor for the current global execution policy default (P0).
    pub(crate) fn execution_policy_default_value(&self) -> runtime::SessionExecutionPolicy {
        self.default_execution_policy()
    }

    fn resolved_session_execution_policy(
        &self,
        record: &session::SessionRecord,
    ) -> runtime::SessionExecutionPolicy {
        let default_policy = self.default_execution_policy();
        match stored_session_execution_policy(record) {
            Some(policy)
                if policy.origin == runtime::SessionExecutionPolicyOrigin::ConfigDefault
                    && !execution_policy_defaults_match(&policy, &default_policy) =>
            {
                runtime::SessionExecutionPolicy::from_profile(
                    default_policy.autonomy_profile,
                    policy.revision.saturating_add(1),
                    runtime::SessionExecutionPolicyOrigin::ConfigDefault,
                )
                .with_approval_profile(default_policy.approval_profile)
            }
            Some(policy) => policy,
            None => default_policy,
        }
    }

    /// Materialize the effective execution policy while the activation
    /// coordinator already owns the Session's exclusive gate.
    ///
    /// The caller persists the returned record together with any other
    /// activation-time changes. Going through `SessionService::update_session`
    /// here would attempt to acquire the same non-reentrant gate again.
    pub(crate) fn materialize_execution_policy_for_activation(
        &self,
        record: &mut session::SessionRecord,
    ) -> Result<bool, String> {
        let stored = stored_session_execution_policy(record);
        let resolved = self.resolved_session_execution_policy(record);
        if stored.as_ref() == Some(&resolved)
            && stored_session_execution_policy_state(record).is_some_and(|state| {
                state.effective == resolved
                    && state.desired.is_none()
                    && state.pending_transition.is_none()
            })
        {
            return Ok(false);
        }
        let mut metadata = record
            .metadata_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| serde_json::json!({}));
        metadata["execution_policy"] = serde_json::to_value(resolved)
            .map_err(|error| format!("cannot serialize Session execution policy: {error}"))?;
        metadata["execution_policy_state"] =
            serde_json::to_value(harness_contract::policy::SessionExecutionPolicyState {
                effective: self.resolved_session_execution_policy(record),
                desired: None,
                pending_transition: None,
            })
            .map_err(|error| format!("cannot serialize Session execution policy state: {error}"))?;
        record.metadata_json = Some(
            serde_json::to_string(&metadata)
                .map_err(|error| format!("cannot encode Session metadata: {error}"))?,
        );
        Ok(true)
    }

    async fn persist_session_execution_policy(
        &self,
        record: &session::SessionRecord,
        policy: &runtime::SessionExecutionPolicy,
    ) -> Result<(), String> {
        self.persist_session_execution_policy_state(
            record,
            &harness_contract::policy::SessionExecutionPolicyState {
                effective: policy.clone(),
                desired: None,
                pending_transition: None,
            },
        )
        .await
    }

    async fn persist_session_execution_policy_state(
        &self,
        record: &session::SessionRecord,
        state: &harness_contract::policy::SessionExecutionPolicyState,
    ) -> Result<(), String> {
        let mut metadata = record
            .metadata_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| serde_json::json!({}));
        // Keep the historical field as an effective-only compatibility
        // projection. Desired policy is never advertised as active before the
        // freeze/drain/rebind transaction reaches Stable.
        metadata["execution_policy"] = serde_json::to_value(&state.effective)
            .map_err(|error| format!("cannot serialize Session execution policy: {error}"))?;
        metadata["execution_policy_state"] = serde_json::to_value(state)
            .map_err(|error| format!("cannot serialize Session execution policy state: {error}"))?;
        let stored = self
            .session_data
            .update_session_metadata(&record.session_id, metadata)
            .await
            .map_err(|error| error.to_string())?;
        stored
            .then_some(())
            .ok_or_else(|| format!("session {} not found", record.session_id))
    }

    async fn resolve_stored_session_execution_policy(
        &self,
        record: &session::SessionRecord,
    ) -> Result<runtime::SessionExecutionPolicy, String> {
        if let Some(state) = stored_session_execution_policy_state(record) {
            return Ok(state.effective);
        }
        let stored = stored_session_execution_policy(record);
        let resolved = self.resolved_session_execution_policy(record);
        if stored.as_ref() != Some(&resolved)
            || stored_session_execution_policy_state(record).is_none()
        {
            self.persist_session_execution_policy(record, &resolved)
                .await?;
        }
        Ok(resolved)
    }

    pub(crate) fn session_input_router(&self) -> Arc<runtime::SessionInputRouter> {
        Arc::clone(&self.session_input_router)
    }

    pub(crate) fn registered_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        let bootstrap = self
            .session_bootstrap
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let permission = bootstrap.tool_registry.required_permission(tool_name)?;
        Some(tools::tool_orchestrator::resolve_registered_tool_effect(
            &bootstrap.tool_registry.effect_resolver(tool_name),
            tool_name,
            input,
            permission,
        ))
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

    /// Build a bounded, read-only response for control-plane inputs that can
    /// be answered without waiting for the active Provider step. The durable
    /// Session input still owns receipt and ordering; this projection is only
    /// an immediate Surface view over canonical Mission/Execution state.
    pub(crate) fn responsive_input_projection(
        &self,
        session_id: &str,
        proposal: Option<&InputRelationProposal>,
    ) -> Option<serde_json::Value> {
        let proposal = proposal?;
        if proposal.candidate != InputRelationKind::Progress {
            return None;
        }
        let mission_port = runtime::MissionRuntimePort::new(self.runtime_services());
        if mission_port.ensure_default_mission().is_err() {
            return None;
        }
        let mission = mission_port.projection();
        let execution = self.session_execution_index(session_id);
        Some(serde_json::json!({
            "kind": "session_input.progress",
            "session_id": session_id,
            "mission": mission.aggregate,
            "execution": execution,
            "teams": mission.team_projection,
            "agents": mission.agent_projection,
            "approvals": mission.approval_projection,
            "health": mission.health_projection,
        }))
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
        self.recoverable_session_execution_indices(&[session_id.to_string()])
            .await
            .remove(session_id)
            .unwrap_or_else(|| self.session_execution_index(session_id))
    }

    /// Recover several Session discovery indices with one durable query.
    ///
    /// The durable outbox owns Turn-root discovery identity. Runtime memory
    /// enriches that same root with current graph/live state and exact terminal
    /// outcomes. The bounded batch avoids issuing N database queries.
    pub(crate) async fn recoverable_session_execution_indices(
        &self,
        session_ids: &[String],
    ) -> BTreeMap<String, SessionExecutionIndexProjection> {
        const DURABLE_EXECUTIONS_PER_SESSION: usize = 128;
        let mut grouped = BTreeMap::<String, Vec<session::SessionRuntimeOutboxRecord>>::new();
        if !session_ids.is_empty() {
            match self
                .session_data
                .runtime_inputs_for_sessions(session_ids, DURABLE_EXECUTIONS_PER_SESSION)
                .await
            {
                Ok(records) => {
                    for record in records {
                        grouped
                            .entry(record.session_id.clone())
                            .or_default()
                            .push(record);
                    }
                }
                Err(error) => tracing::warn!(
                    session_count = session_ids.len(),
                    %error,
                    "durable Session root execution recovery query failed; serving volatile index"
                ),
            }
        }
        session_ids
            .iter()
            .map(|session_id| {
                let volatile = self.session_execution_index(session_id);
                let index = grouped
                    .remove(session_id)
                    .map(|records| {
                        reconcile_session_execution_indices(
                            volatile.clone(),
                            session_execution_index_from_outbox(session_id, &records),
                        )
                    })
                    .unwrap_or(volatile);
                (session_id.clone(), index)
            })
            .collect()
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
        let session_ids = session_ids.into_iter().collect::<Vec<_>>();
        let recovered = self
            .recoverable_session_execution_indices(&session_ids)
            .await;
        let mut indices = Vec::with_capacity(recovered.len());
        for (_, index) in recovered {
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
        if let Some(transition) = state.frozen_sessions.get(session_id) {
            return Err(format!(
                "session_policy_transition_in_progress: session {session_id} is frozen while policy revision {} drains for desired revision {} ({})",
                transition.effective_revision,
                transition.desired_revision,
                transition.transition_id
            ));
        }
        // Read the effective snapshot while holding the same registry lock as
        // the per-Session freeze fence. Transition finalization updates the
        // policy before unfreezing, so admission can observe only old+counted
        // or new+stable, never an uncounted stale revision.
        let effective_policy = self.effective_session_execution_policy(session_id);
        if state.controls.contains_key(turn_id) {
            return Err(format!("Runtime turn {turn_id} is already active"));
        }
        state.controls.insert(
            turn_id.to_string(),
            ActiveTurnControl {
                session_id: session_id.to_string(),
                execution_id,
                policy_revision: effective_policy.revision,
                requested_sandbox_posture: effective_policy.sandbox_posture,
                effective_sandbox_posture: effective_policy.sandbox_posture,
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
        if let Some(execution_id) = &control.execution_id {
            match self
                .runtime_services
                .try_cancel_live_execution(execution_id, reason.to_string())
            {
                Ok(true) => {
                    control.cancellation_token.cancel();
                    return Some(execution_id.clone());
                }
                Ok(false) => return None,
                Err(error) => {
                    tracing::error!(execution_id, %error, "durable cancellation winner could not be persisted");
                    return None;
                }
            }
        }
        control.cancellation_token.cancel();
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

    fn freeze_session_policy_transition(
        &self,
        session_id: &str,
        transition: &harness_contract::policy::PolicyTransitionReceipt,
    ) -> (u64, bool) {
        let mut state = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = state
            .frozen_sessions
            .get(session_id)
            .is_none_or(|current| current.transition_id != transition.transition_id);
        state.frozen_sessions.insert(
            session_id.to_string(),
            FrozenPolicyTransition {
                transition_id: transition.transition_id.clone(),
                effective_revision: transition.effective_revision,
                desired_revision: transition.desired_revision,
            },
        );
        self.runtime_services
            .freeze_session_execution_policy_admission(
                session_id.to_string(),
                transition.transition_id.clone(),
            );
        let active = state
            .controls
            .values()
            .filter(|control| {
                control.session_id == session_id
                    && control.policy_revision == transition.effective_revision
            })
            .count() as u64;
        drop(state);
        self.active_turns.changed.notify_waiters();
        (active, changed)
    }

    fn active_turns_for_policy_revision(&self, session_id: &str, revision: u64) -> u64 {
        self.active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .values()
            .filter(|control| {
                control.session_id == session_id && control.policy_revision == revision
            })
            .count() as u64
    }

    async fn active_attempts_for_policy_revision(
        &self,
        session_id: &str,
        revision: u64,
    ) -> Result<u64, String> {
        let turns = self.active_turns_for_policy_revision(session_id, revision);
        let tasks = self
            .runtime_services
            .active_tasks_for_session_policy_revision(session_id, revision)
            .await
            .map_err(|error| error.to_string())?;
        // Graphs are counted independently from Tasks. A graph can be
        // registered after its Task was observed/cancelled, so Task state alone
        // is not a sufficient zero fence for Stable.
        let graphs = self
            .runtime_services
            .active_graphs_for_session_policy_revision(session_id, revision)
            .await
            .map_err(|error| error.to_string())?;
        Ok(turns
            .saturating_add(tasks.len() as u64)
            .saturating_add(graphs.len() as u64))
    }

    fn active_turn_ids_for_policy_revision(&self, session_id: &str, revision: u64) -> Vec<String> {
        self.active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .iter()
            .filter(|(_, control)| {
                control.session_id == session_id && control.policy_revision == revision
            })
            .map(|(turn_id, _)| turn_id.clone())
            .collect()
    }

    async fn record_policy_transition_blocker(
        &self,
        session_id: &str,
        transition_id: &str,
        blocker: String,
    ) -> Result<(), String> {
        let update_lock = self.session_policy_update_lock(session_id);
        let _guard = update_lock.lock().await;
        let record = self
            .session_data
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session {session_id} not found"))?;
        let Some(mut state) = stored_session_execution_policy_state(&record) else {
            return Ok(());
        };
        let Some(receipt) = state.pending_transition.as_mut() else {
            return Ok(());
        };
        if receipt.transition_id != transition_id {
            return Ok(());
        }
        receipt.phase = harness_contract::policy::PolicyTransitionPhase::Draining;
        receipt.old_revision_active_attempts = self
            .active_attempts_for_policy_revision(session_id, receipt.effective_revision)
            .await?;
        receipt.blocker = Some(blocker);
        self.persist_session_execution_policy_state(&record, &state)
            .await
    }

    fn unfreeze_session_policy_transition(&self, session_id: &str, transition_id: &str) -> bool {
        let mut state = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .frozen_sessions
            .get(session_id)
            .is_none_or(|transition| transition.transition_id != transition_id)
        {
            return false;
        }
        self.runtime_services
            .unfreeze_session_execution_policy_admission(session_id, transition_id);
        state.frozen_sessions.remove(session_id);
        drop(state);
        self.active_turns.changed.notify_waiters();
        true
    }

    async fn wait_for_policy_revision_to_drain(
        &self,
        session_id: &str,
        revision: u64,
        transition_id: &str,
        cancellation: &runtime::CancellationToken,
    ) -> Result<(), String> {
        let mut poll = tokio::time::interval(Duration::from_millis(50));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_active = None;
        loop {
            let changed = self.active_turns.changed.notified();
            let active = self
                .active_attempts_for_policy_revision(session_id, revision)
                .await?;
            if active != last_active.unwrap_or(u64::MAX) {
                last_active = Some(active);
                self.record_policy_transition_blocker(
                    session_id,
                    transition_id,
                    if active == 0 {
                        "old policy revision drained; activating the requested revision"
                            .to_string()
                    } else {
                        format!(
                            "waiting for {active} attempt(s) bound to old policy revision {revision} to finish; the running turn keeps its bound policy and is never cancelled"
                        )
                    },
                )
                .await?;
            }
            if active == 0 {
                return Ok(());
            }
            tokio::select! {
                () = changed => {}
                _ = poll.tick() => {}
                () = cancellation.cancelled() => {
                    return Err("policy transition supervisor was superseded or stopped".to_string());
                }
            }
        }
    }

    /// Cancel every live turn owned by one session and propagate cancellation
    /// to the Runtime execution registry. The HTTP session-cancel endpoint
    /// uses this owner path; broadcasting a UI event alone is not cancellation.
    pub(crate) fn cancel_active_session(&self, session_id: &str, reason: &str) -> Vec<String> {
        self.cancel_active_session_turns(session_id, reason)
    }

    pub(crate) fn cancel_active_execution(
        &self,
        session_id: &str,
        turn_id: &str,
        execution_id: &str,
        reason: &str,
    ) -> bool {
        let matches_target = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .get(turn_id)
            .is_some_and(|control| {
                control.session_id == session_id
                    && control.execution_id.as_deref() == Some(execution_id)
            });
        matches_target && self.cancel_active_turn_control(turn_id, reason).is_some()
    }

    #[cfg(test)]
    pub(crate) fn spawn_test_active_session_execution(
        &self,
        session_id: &str,
        turn_id: &str,
        execution_id: &str,
    ) -> tokio::task::JoinHandle<()> {
        let (cancellation, guard) = self
            .install_active_turn_control(turn_id, session_id, Some(execution_id.to_string()))
            .expect("install test active turn");
        self.record_live_execution(session_id, execution_id.to_string(), turn_id.to_string());
        tokio::spawn(async move {
            cancellation.cancelled().await;
            drop(guard);
        })
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
        let mut cancellation = self
            .runtime_services
            .latest_cancellation_receipt_for_execution(
                &record.session_id,
                &graph_id,
                &record.turn_id,
            )
            .map_err(|error| error.to_string())?;
        if let Some(requested) = cancellation.as_ref().filter(|receipt| {
            receipt.status == harness_contract::turn::CancellationStatus::Requested
        }) {
            cancellation = self
                .runtime_services
                .resolve_requested_cancellation(&requested.cancellation_id)
                .map_err(|error| error.to_string())?;
        }
        let durable_cancelled = cancellation.as_ref().is_some_and(|receipt| {
            receipt.status == harness_contract::turn::CancellationStatus::Cancelled
                || (receipt.status == harness_contract::turn::CancellationStatus::Requested
                    && self
                        .runtime_services
                        .execution_live(&graph_id)
                        .is_some_and(|live| {
                            live.status
                                == harness_contract::projection::ExecutionLiveStatus::Cancelled
                        }))
        });
        if durable_cancelled {
            let cancelled = cancellation.expect("checked cancellation receipt");
            self.bind_primary_ingress_projection(record, &graph_id)
                .await;
            self.cancel_primary_ingress_projection(
                record,
                cancelled.reason.as_deref().unwrap_or("user requested"),
            )
            .await;
            return Ok(runtime::SessionIngressExecutionReceipt {
                graph_id,
                commit_cursor: cancelled.journal_sequence,
                status: runtime::SessionIngressExecutionStatus::Cancelled,
            });
        }
        if let Some(terminal) = self
            .runtime_services
            .session_terminal_delivery()
            .get(&terminal_id)
            .map_err(|error| error.to_string())?
        {
            if terminal.status == "suppressed" {
                return self
                    .settle_suppressed_terminal_as_cancelled(record, &graph_id, &terminal_id)
                    .await;
            }
            let terminal = if terminal.status == "materialized" {
                terminal
            } else {
                self.adopt_existing_terminal_for_claim(record, terminal)
                    .await?;
                self.await_session_terminal_materialization(&terminal_id)
                    .await?
            };
            if terminal.status == "suppressed" {
                return self
                    .settle_suppressed_terminal_as_cancelled(record, &graph_id, &terminal_id)
                    .await;
            }
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
                status: runtime::SessionIngressExecutionStatus::Completed,
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
        let route_model = self
            .sessions
            .session(&record.session_id)
            .and_then(|session| session.model());
        let task_route = runtime::materialize_session_task_route(
            &self.runtime_services,
            &runtime::TaskRouter,
            &record.request_id,
            &record.input_id,
            &record.session_id,
            &record.turn_id,
            content,
            self.runtime_services.mission_runtime().default_mission_id(),
            record.task_route_hint.clone(),
            harness_contract::task::TaskOrigin::User,
            route_model.as_deref(),
            None,
        )
        .await?;
        self.session_data
            .append_session_input_journal(
                &record.session_id,
                crate::session_runtime_data_port::SessionInputJournalKind::TaskRouted,
                serde_json::json!({
                    "request_id": record.request_id,
                    "turn_id": record.turn_id,
                    "route_receipt": task_route.receipt,
                    "bindings": task_route.bindings,
                }),
                chrono::Utc::now().timestamp_millis().max(0) as u64,
                &format!(
                    "session-input:{}:{}:{}:{}",
                    crate::session_runtime_data_port::SessionInputJournalKind::TaskRouted.as_str(),
                    record.session_id,
                    record.request_id,
                    record.turn_id
                ),
            )
            .await
            .map_err(|error| error.to_string())?;
        let organizer = runtime::MissionOrganizer::new(Arc::clone(&self.runtime_services));
        for binding in &task_route.bindings {
            if let Some(task) = self
                .runtime_services
                .task_aggregate_service()
                .get(&binding.task_id)?
            {
                if let Err(error) = organizer.enqueue_root(&task) {
                    // Mission organization is a recoverable projection of the
                    // canonical Root Task. The supervised organizer scans
                    // pending roots, so this side effect must never abort the
                    // foreground Session turn.
                    tracing::warn!(
                        task_id = %task.task_id,
                        session_id = %record.session_id,
                        %error,
                        "deferred Mission organization after foreground enqueue failure"
                    );
                }
            }
        }
        let ingress =
            runtime::TurnIngressRef {
                request_id: record.request_id.clone(),
                turn_id: record.turn_id.clone(),
                message_id: record.message_id.clone(),
                session_id: record.session_id.clone(),
                primary_task_id: task_route.primary_task.task_id.clone(),
                root_task_id: task_route.root_task.task_id.clone(),
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
                handoff_id: record
                    .task_route_hint
                    .as_ref()
                    .and_then(|hint| hint.handoff_id.clone()),
            };
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
        let mut cancellation_after_control = self
            .runtime_services
            .latest_cancellation_receipt_for_execution(
                &record.session_id,
                &graph_id,
                &record.turn_id,
            )
            .map_err(|error| error.to_string())?;
        if let Some(requested) = cancellation_after_control.as_ref().filter(|receipt| {
            receipt.status == harness_contract::turn::CancellationStatus::Requested
        }) {
            cancellation_after_control = self
                .runtime_services
                .resolve_requested_cancellation(&requested.cancellation_id)
                .map_err(|error| error.to_string())?;
        }
        if cancellation_after_control.as_ref().is_some_and(|receipt| {
            receipt.status == harness_contract::turn::CancellationStatus::Cancelled
                || (receipt.status == harness_contract::turn::CancellationStatus::Requested
                    && self
                        .runtime_services
                        .execution_live(&graph_id)
                        .is_some_and(|live| {
                            live.status
                                == harness_contract::projection::ExecutionLiveStatus::Cancelled
                        }))
        }) {
            cancellation_token.cancel();
            let receipt = cancellation_after_control.expect("checked cancellation receipt");
            self.bind_primary_ingress_projection(record, &graph_id)
                .await;
            self.cancel_primary_ingress_projection(
                record,
                receipt.reason.as_deref().unwrap_or("user requested"),
            )
            .await;
            return Ok(runtime::SessionIngressExecutionReceipt {
                graph_id,
                commit_cursor: receipt.journal_sequence,
                status: runtime::SessionIngressExecutionStatus::Cancelled,
            });
        }
        let execution_policy = self.effective_session_execution_policy(&record.session_id);
        let active_model = self
            .sessions
            .session(&record.session_id)
            .and_then(|session| session.model());
        let mut owned_runtime = match async {
            let mut runtime = lock_runtime_entry(&runtime_entry).await;
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
                return Err(format!("{SESSION_RUNTIME_BUSY_ERROR}: {error}"));
            }
        };
        let prepare_result = async {
            let runtime = owned_runtime.runtime_mut()?;
            runtime.set_execution_policy(execution_policy.clone())?;
            if let Some(model) = active_model.as_deref() {
                runtime.update_session_model(model).await;
            }
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
            Ok::<_, String>(())
        }
        .await;
        if let Err(error) = prepare_result {
            owned_runtime.restore().await;
            self.fail_live_execution(&graph_id, error.clone());
            return Err(error);
        }
        self.record_live_execution(&record.session_id, graph_id.clone(), record.turn_id.clone());
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
                    let mut receipt = self
                        .runtime_services
                        .latest_cancellation_receipt_for_execution(
                            &record.session_id,
                            &graph_id,
                            &record.turn_id,
                        )
                        .map_err(|lookup_error| lookup_error.to_string())?;
                    if let Some(requested) = receipt.as_ref().filter(|receipt| {
                        receipt.status == harness_contract::turn::CancellationStatus::Requested
                    }) {
                        receipt = self
                            .runtime_services
                            .resolve_requested_cancellation(&requested.cancellation_id)
                            .map_err(|resolve_error| resolve_error.to_string())?;
                    }
                    if let Some(receipt) = receipt.filter(|receipt| {
                        receipt.status == harness_contract::turn::CancellationStatus::Cancelled
                    }) {
                        self.cancel_primary_ingress_projection(
                            record,
                            receipt.reason.as_deref().unwrap_or("user requested"),
                        )
                        .await;
                        return Ok(runtime::SessionIngressExecutionReceipt {
                            graph_id,
                            commit_cursor: receipt.journal_sequence,
                            status: runtime::SessionIngressExecutionStatus::Cancelled,
                        });
                    }
                }
                // A graph transition and its terminal outbox event are one
                // durable commit. A post-commit hook may still fail (for
                // example while promoting an artifact pin). In that case the
                // committed terminal must win over the transient Runtime
                // error; the supervised outbox bridge remains responsible for
                // exactly-once materialization.
                if let Some(terminal) = self
                    .runtime_services
                    .session_terminal_delivery()
                    .get(&terminal_id)
                    .map_err(|lookup_error| lookup_error.to_string())?
                {
                    if terminal.status == "suppressed" {
                        return self
                            .settle_suppressed_terminal_as_cancelled(
                                record,
                                &graph_id,
                                &terminal_id,
                            )
                            .await;
                    }
                    let terminal = if terminal.status == "materialized" {
                        terminal
                    } else {
                        self.adopt_existing_terminal_for_claim(record, terminal)
                            .await?;
                        self.await_session_terminal_materialization(&terminal_id)
                            .await?
                    };
                    if terminal.status == "suppressed" {
                        return self
                            .settle_suppressed_terminal_as_cancelled(
                                record,
                                &graph_id,
                                &terminal_id,
                            )
                            .await;
                    }
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
                            .map_err(|resolution_error| resolution_error.to_string())?;
                    }
                    tracing::warn!(
                        request_id = %record.request_id,
                        %graph_id,
                        %terminal_id,
                        %error,
                        "recovered a durable terminal after a post-commit Runtime error"
                    );
                    return Ok(runtime::SessionIngressExecutionReceipt {
                        graph_id,
                        commit_cursor: terminal.commit_cursor,
                        status: runtime::SessionIngressExecutionStatus::Completed,
                    });
                }
                self.fail_live_execution(&graph_id, error.clone());
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
        if terminal.status == "suppressed" {
            return self
                .settle_suppressed_terminal_as_cancelled(record, &graph_id, &terminal_id)
                .await;
        }
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
            harness_contract::goal::GoalCompletion::Partial
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
                    activity_binding: None,
                    event: Box::new(runtime::CowdEvent::TurnError { error: reason }),
                };
                self.projection_hub
                    .publish(&record.session_id, SessionProjectionEvent::runtime(event))
                    .await;
            }
            harness_contract::goal::GoalCompletion::WaitingExternalDecision => {
                let reason = format!(
                    "Runtime turn waiting for external decision: {}",
                    summary.final_answer
                );
                self.block_live_execution(
                    &graph_id,
                    &summary.context_turn_report,
                    &summary.write_attempt_paths,
                    terminal_id.clone(),
                    reason.clone(),
                );
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
            status: runtime::SessionIngressExecutionStatus::Completed,
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
                    "suppressed" => return Ok(terminal),
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

    async fn settle_suppressed_terminal_as_cancelled(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        graph_id: &str,
        terminal_id: &str,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        let receipt = self
            .runtime_services
            .latest_cancellation_receipt_for_execution(
                &record.session_id,
                graph_id,
                &record.turn_id,
            )
            .map_err(|error| error.to_string())?
            .filter(|receipt| {
                receipt.status == harness_contract::turn::CancellationStatus::Cancelled
            })
            .ok_or_else(|| {
                format!(
                    "terminal `{terminal_id}` was suppressed without a durable cancellation winner"
                )
            })?;
        self.bind_primary_ingress_projection(record, graph_id).await;
        self.cancel_primary_ingress_projection(
            record,
            receipt.reason.as_deref().unwrap_or("user requested"),
        )
        .await;
        Ok(runtime::SessionIngressExecutionReceipt {
            graph_id: graph_id.to_string(),
            commit_cursor: receipt.journal_sequence,
            status: runtime::SessionIngressExecutionStatus::Cancelled,
        })
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
    pub(crate) fn with_session_bootstrap(
        mut self,
        snapshot: crate::runtime_bootstrap::RuntimeSessionBootstrapSnapshot,
    ) -> Self {
        self.session_bootstrap = Arc::new(RwLock::new(snapshot));
        self
    }

    pub(crate) fn replace_session_bootstrap(
        &self,
        snapshot: crate::runtime_bootstrap::RuntimeSessionBootstrapSnapshot,
    ) {
        *self
            .session_bootstrap
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot;
    }

    #[must_use]
    pub(crate) fn status_value(&self) -> serde_json::Value {
        let status = self.status();
        let execution_policy_default = self.default_execution_policy();
        serde_json::json!({
            "ok": true,
            "protocol_version": status.protocol_version,
            "runtime_host": status.runtime_host,
            "active_sessions": status.active_sessions,
            "uptime_secs": status.uptime_secs,
            "permission_mode": execution_policy_default.permission_mode.as_str(),
            "approval_profile": execution_policy_default.approval_profile.as_str(),
            "execution": self.runtime_services.execution_health(),
            "hot_state": self.runtime_services.hot_state_health(),
        })
    }

    #[must_use]
    pub(crate) fn provider_registry(&self) -> Arc<runtime::ProviderRegistry> {
        Arc::clone(&self.provider_registry)
    }

    #[must_use]
    pub(crate) fn configured_model(&self) -> Option<String> {
        self.configured_model
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn replace_configured_model(&self, model: Option<String>) -> Result<(), String> {
        let model = normalize_configured_model(model);
        if let Some(model) = model.as_deref() {
            if self.provider_registry.pin().resolve(model).is_none() {
                return Err(format!(
                    "configured default model '{model}' is not declared by any configured provider"
                ));
            }
        }
        *self
            .configured_model
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = model;
        Ok(())
    }

    pub(crate) fn resolve_session_model(&self, requested: Option<&str>) -> Result<String, String> {
        let model = requested
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| self.configured_model())
            .ok_or_else(|| {
                "no default model is configured; set `model` and declare it under `providers.*.models`"
                    .to_string()
            })?;
        if self.provider_registry.pin().resolve(&model).is_none() {
            return Err(format!(
                "model '{model}' is not declared by any configured provider; update the Session model or provider configuration"
            ));
        }
        Ok(model)
    }

    pub(crate) fn resolve_persisted_session_model(
        &self,
        persisted: Option<&str>,
    ) -> Result<String, String> {
        let persisted = persisted.map(str::trim).filter(|model| !model.is_empty());
        if let Some(model) = persisted {
            if self.provider_registry.pin().resolve(model).is_some() {
                return Ok(model.to_string());
            }
            tracing::warn!(
                model,
                configured_model = self.configured_model().as_deref().unwrap_or(""),
                "persisted Session model is no longer configured; rebinding to the configured default"
            );
        }
        self.resolve_session_model(None)
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
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(turn_id);

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
                "task_id": input.primary_task_id.clone(),
                "task_bindings": input.task_bindings.clone(),
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
                "task_id": receipt.primary_task_id.clone(),
                "task_bindings": receipt.task_bindings.clone(),
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
        input.primary_task_id = task_id;
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

    /// Narrow security lookup used by Gateway approval projection. Session
    /// identity is not a bearer token; ordinary principals may observe or
    /// decide an approval only when this durable owner matches.
    pub(crate) async fn session_owner_principal_id(&self, session_id: &str) -> Option<String> {
        let record = self.session_data.stored_session(session_id).await.ok()??;
        record
            .metadata_json
            .as_deref()
            .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
            .and_then(|metadata| {
                metadata
                    .get("owner_principal_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
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
        let execution_policy = match stored_record.as_ref() {
            Some(record)
                if stored_session_execution_policy_state(record)
                    .is_some_and(|state| state.pending_transition.is_some()) =>
            {
                // Recovery owns a durable desired/effective pair. Resolve or
                // re-supervise it before creating a carrier; ingress remains
                // fenced while the state is non-Stable.
                let _ = self.session_execution_policy_value(session_id).await?;
                let latest = self
                    .session_data
                    .stored_session(session_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("session {session_id} not found"))?;
                self.resolve_stored_session_execution_policy(&latest)
                    .await?
            }
            Some(record) => self.resolve_stored_session_execution_policy(record).await?,
            None => self.default_execution_policy(),
        };
        let stored_model = stored_record
            .as_ref()
            .and_then(|record| record.model.clone())
            .filter(|model| !model.trim().is_empty());
        let model = match model_hint.filter(|model| !model.trim().is_empty()) {
            Some(model) => self.resolve_session_model(Some(model))?,
            None => self.resolve_persisted_session_model(stored_model.as_deref())?,
        };
        let (session, resume_context) = if let Some(record) = stored_record {
            self.hydration_attempts.fetch_add(1, Ordering::Relaxed);
            let history = self
                .runtime_services()
                .session_history_reader()
                .ok_or_else(|| {
                    format!(
                        "session {session_id} cannot activate because the canonical history reader is unavailable"
                    )
                })?;
            let mut stable_projection = None;
            let mut manifest_rebuilt = false;
            for attempt in 1..=recovery.stable_snapshot_attempts {
                let manifest_before = match history
                    .activation_manifest(session_id)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    Some(manifest) => manifest,
                    None => {
                        manifest_rebuilt = true;
                        history
                            .rebuild_activation_manifest(
                                session_id,
                                Utc::now().timestamp_millis().max(0) as u64,
                            )
                            .await
                            .map_err(|error| error.to_string())?
                            .ok_or_else(|| {
                                format!(
                                    "session {session_id} disappeared while rebuilding its activation manifest"
                                )
                            })?
                    }
                };
                if manifest_before.schema_version
                    != session::SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION
                {
                    return Err(format!(
                        "session {session_id} activation manifest schema {} is unsupported by {}",
                        manifest_before.schema_version,
                        session::SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION
                    ));
                }
                let latest_checkpoint = history
                    .latest_domain_event_by_kind(session_id, "memory.semantic_checkpoint.created")
                    .await
                    .map_err(|error| error.to_string())?;
                let checkpoint = latest_checkpoint
                    .as_ref()
                    .and_then(|event| crate::semantic_checkpoint_from_event(event, session_id));
                let total_messages = manifest_before.recovery.transcript_messages as usize;
                let checkpoint_cursor = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.resume_cursor.message_index)
                    .unwrap_or_default()
                    .min(total_messages);
                let tail_start = checkpoint_cursor
                    .max(total_messages.saturating_sub(recovery.activation_tail_messages));
                let post_checkpoint_tail = history
                    .messages_after_sequence(
                        session_id,
                        tail_start,
                        recovery.activation_tail_messages,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                self.hydration_body_reads.fetch_add(1, Ordering::Relaxed);
                let metadata_start =
                    total_messages.saturating_sub(recovery.activation_metadata_messages);
                let recent_metadata = history
                    .message_metadata_page(
                        session_id,
                        metadata_start,
                        recovery.activation_metadata_messages,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let context_cards = history
                    .context_index_cards(session_id, recovery.context_card_cache_entries)
                    .await
                    .map_err(|error| error.to_string())?;
                let manifest_after = history
                    .activation_manifest(session_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| {
                        format!("session {session_id} activation manifest disappeared")
                    })?;
                if manifest_before.projection_generation == manifest_after.projection_generation
                    && manifest_before.recovery.durable_cursor
                        == manifest_after.recovery.durable_cursor
                    && manifest_before.recovery.event_cursor == manifest_after.recovery.event_cursor
                {
                    let recovery_state = if manifest_rebuilt {
                        session::SessionProjectionRecoveryState::ManifestRebuilt
                    } else if manifest_after.recovery.latest_checkpoint_sequence.is_some()
                        && latest_checkpoint.is_none()
                    {
                        session::SessionProjectionRecoveryState::CheckpointMissing
                    } else if latest_checkpoint.is_some() && checkpoint.is_none() {
                        session::SessionProjectionRecoveryState::CheckpointMalformed
                    } else if !manifest_after.index_complete {
                        session::SessionProjectionRecoveryState::IndexPending
                    } else {
                        session::SessionProjectionRecoveryState::Ready
                    };
                    stable_projection = Some((
                        session::ActiveSessionProjection {
                            manifest: manifest_after,
                            latest_checkpoint,
                            post_checkpoint_tail,
                            recent_metadata,
                            context_cards,
                            recovery_state,
                        },
                        attempt,
                    ));
                    break;
                }
                if attempt < recovery.stable_snapshot_attempts {
                    tokio::task::yield_now().await;
                }
            }
            let (projection, snapshot_attempts) = stable_projection.ok_or_else(|| {
                format!(
                    "session {session_id} changed during all {} configured activation projection attempts; retry after ingress stabilizes",
                    recovery.stable_snapshot_attempts
                )
            })?;
            let hydrated_bytes = projection
                .post_checkpoint_tail
                .iter()
                .try_fold(0usize, |total, message| {
                    total.checked_add(stored_message_bytes(message))
                })
                .ok_or_else(|| {
                    format!("session {session_id} activation byte accounting overflowed")
                })?;
            self.hydration_body_bytes
                .fetch_add(hydrated_bytes as u64, Ordering::Relaxed);
            if !projection.manifest.index_complete {
                let history = Arc::clone(&history);
                let index_session_id = session_id.to_string();
                tokio::spawn(async move {
                    if let Err(error) = history
                        .reconcile_context_index(
                            &index_session_id,
                            recovery.context_index_card_span,
                            recovery.context_index_parent_span,
                            Utc::now().timestamp_millis().max(0) as u64,
                        )
                        .await
                    {
                        tracing::warn!(
                            session_id = %index_session_id,
                            error = %error,
                            "background Session context index reconciliation failed"
                        );
                    }
                });
            }
            let resume_context = projection.latest_checkpoint.as_ref().and_then(|event| {
                crate::semantic_checkpoint_resume_context_packet(event, session_id)
            });
            tracing::info!(
                session_id,
                activation_tail_messages = projection.post_checkpoint_tail.len(),
                activation_tail_bytes = hydrated_bytes,
                activation_metadata = projection.recent_metadata.len(),
                activation_cards = projection.context_cards.len(),
                activation_generation = projection.manifest.projection_generation,
                activation_duration_ms = hydration_started.elapsed().as_millis(),
                activation_snapshot_attempts = snapshot_attempts,
                recovery_state = ?projection.recovery_state,
                "activated persisted Session from checkpoint-first projection"
            );
            (
                crate::entry::session_store_entry::hydrated_runtime_session(
                    record,
                    projection.post_checkpoint_tail,
                )?,
                resume_context,
            )
        } else {
            let mut session = runtime::Session::new();
            session.session_id = session_id.to_string();
            (session, None)
        };
        if session.closed {
            return Err(format!(
                "session {session_id} is closed and cannot activate a new Runtime carrier"
            ));
        }
        let runtime = self.build_session_runtime_entry_with_execution_policy(
            session,
            session_id,
            &model,
            system_prompt,
            &execution_policy,
            resume_context,
        )?;
        self.register_runtime(session_id.to_string(), runtime)
            .await?;
        let activation_elapsed = hydration_started.elapsed();
        runtime::execution_core::performance::observe_duration("activation_ms", activation_elapsed);
        runtime::execution_core::performance::record_session_activation_latency(
            session_id,
            activation_elapsed,
        );
        Ok(())
    }

    pub(crate) async fn register_runtime(
        &self,
        session_id: String,
        runtime: crate::runtime_entry::GatewayRuntimeEntry,
    ) -> Result<Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>>, String>
    {
        let transition_lock = self.sessions.transition_lock(&session_id);
        let _transition = transition_lock.lock().await;
        if self.sessions.get(&session_id).is_some() {
            return Err(format!(
                "session {session_id} already has an active Runtime carrier; refusing replacement"
            ));
        }
        self.gateway_tasks
            .open_session(&session_id)
            .await
            .map_err(|error| format!("cannot activate Runtime carrier during shutdown: {error}"))?;
        let input_stream = runtime.session_input_stream();
        let cowd_bus = runtime.cowd_bus().cloned();
        let policy_control = runtime.execution_policy_control();
        let model = runtime
            .session_head()
            .await
            .model
            .filter(|model| !model.trim().is_empty());
        let relay = match cowd_bus.clone() {
            Some(bus) => Some(
                self.install_session_event_relay(&session_id, bus)
                    .await
                    .map_err(|error| {
                        format!(
                            "failed to install Runtime event relay for session {session_id}: {error}"
                        )
                    })?,
            ),
            None => None,
        };
        let prepared = PreparedActiveSession::complete(
            runtime,
            input_stream,
            cowd_bus,
            model,
            policy_control.clone(),
            relay,
        );
        let transition = match self.sessions.publish(session_id.clone(), prepared) {
            Ok(transition) => transition,
            Err(error) => {
                let _ = self
                    .gateway_tasks
                    .close_session_and_drain(&session_id, Duration::from_secs(5))
                    .await;
                return Err(error);
            }
        };
        self.runtime_services
            .publish_session_execution_policy(session_id, policy_control);
        Ok(transition.replaced_carrier())
    }

    pub(crate) async fn remove_active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>> {
        let transition_lock = self.sessions.transition_lock(session_id);
        let _transition = transition_lock.lock().await;
        let removed = self.sessions.remove_aggregate(session_id);
        let carrier = removed.as_ref().map(|session| session.carrier());
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
        self.active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frozen_sessions
            .remove(session_id);
        self.runtime_services
            .remove_session_execution_policy(session_id);
        carrier
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
    ) -> Result<SessionRelayLease, crate::runtime_host::task_set::GatewayTaskSpawnError> {
        let session_id = session_id.to_string();
        let relay_session_id = session_id.clone();
        let gateway_bus = Arc::clone(&self.projection_hub);
        let runtime_services = Arc::clone(&self.runtime_services);
        let mut receiver = bus.subscribe();
        let task_id = self
            .gateway_tasks
            .replace_session_task(
                crate::runtime_host::task_set::GatewayTaskKind::SessionEventRelay,
                &session_id,
                move |cancellation| async move {
                    let mut tool_ordinals = HashMap::<(String, String, String), u64>::new();
                    let mut active_tool_instances =
                        HashMap::<(String, String, String), String>::new();
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
                                            // observe_live_execution_event above has already
                                            // committed this delta into Runtime's canonical live
                                            // projection. Derive the range from that state so a
                                            // replaced relay resumes at the real byte offset
                                            // instead of restarting from a task-local zero.
                                            let end_bytes = runtime_services
                                                .execution_live(&context.execution_id)
                                                .and_then(|live| {
                                                    let part_id = event
                                                        .causal_identity()?
                                                        .segment_id
                                                        .as_str();
                                                    live.output_parts
                                                        .iter()
                                                        .find(|part| part.part_id == part_id)
                                                        .and_then(|part| {
                                                            usize::try_from(part.bytes).ok()
                                                        })
                                                })
                                                .unwrap_or(text.len());
                                            let start_bytes = end_bytes.saturating_sub(text.len());
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
        Ok(SessionRelayLease::new(task_id))
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
            task_route_hint: None,
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
        self.runtime_services.record_hot_session_durable_ingress(
            &session_id,
            u64::try_from(persisted.sequence).unwrap_or(u64::MAX),
        );
        let stream = self.session_input_stream_for(&session_id).await?;
        let mut receipt = stream.admit(envelope, stream.runtime_state());
        // The production SessionService immediately projects the durable
        // admission cursor back into Runtime. Keep this test-only ingress
        // helper faithful to that boundary so terminal watermark ACKs can
        // prove exactly which hot record the Session commit covered.
        receipt.cursor = Some(harness_contract::turn::SessionInputCursor::new(
            persisted.session_generation,
            u64::try_from(persisted.sequence).unwrap_or(u64::MAX),
        ));
        stream.project_durable_receipt(&receipt);
        let record_for_event = stream.record_snapshot(&receipt.input_id);
        self.emit_session_input_events(&session_id, &stream, Some(receipt.clone()));
        self.persist_session_input_domain_event(
            &session_id,
            SessionInputJournalKind::Received,
            Some(&receipt),
            record_for_event.as_ref(),
            &stream,
            &request.request_id,
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
                    input_turn_id: request.turn_id.clone(),
                    supplemental: false,
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
            message_id: request.message_id,
            message_sequence: persisted.sequence,
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
            Ok(runtime::session_input::PrimaryIngressBindOutcome::Bound(record)) => {
                let receipt = record.to_receipt();
                self.emit_session_input_events(&outbox.session_id, &stream, Some(receipt.clone()));
                self.persist_session_input_domain_event(
                    &outbox.session_id,
                    SessionInputJournalKind::IngressBound,
                    Some(&receipt),
                    Some(&record),
                    &stream,
                    &format!("{}:{}:{}", outbox.request_id, outbox.turn_id, execution_id),
                )
                .await;
            }
            Ok(runtime::session_input::PrimaryIngressBindOutcome::AlreadyBoundSame(_)) => {
                // Idempotent replay of the exact same tuple: no new live
                // event, no audit journal entry, no warning, no revision.
            }
            Ok(runtime::session_input::PrimaryIngressBindOutcome::ProjectionMissing) => {
                tracing::debug!(
                    session_id = %outbox.session_id,
                    request_id = %outbox.request_id,
                    "durable ingress recovered without an in-process input projection"
                )
            }
            Ok(runtime::session_input::PrimaryIngressBindOutcome::Conflict {
                existing,
                reason,
            }) => tracing::warn!(
                session_id = %outbox.session_id,
                request_id = %outbox.request_id,
                incoming_turn = %outbox.turn_id,
                incoming_execution = %execution_id,
                existing_turn = ?existing.active_turn_id,
                existing_status = ?existing.status,
                %reason,
                "refused primary ingress bind: incoming tuple conflicts with the existing binding"
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
                    &format!(
                        "{}:{}:{}:{}",
                        outbox.request_id, outbox.turn_id, execution_id, terminal_id
                    ),
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
        // Every caller reaches this boundary only after the terminal outbox
        // has been materialized into the durable Session transcript. The
        // SessionService wrapper may have acknowledged the cursor before this
        // process-local projection became Consumed, so repeat the exact
        // watermark ACK after settling. It is idempotent and releases the hot
        // primary record while retaining durable replay watermarks.
        self.acknowledge_durable_session_inputs_through(
            &outbox.session_id,
            &outbox.turn_id,
            outbox.session_generation,
            outbox.sequence,
        );
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
                    &format!("{}:{}", outbox.request_id, outbox.turn_id),
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

    async fn cancel_primary_ingress_projection(
        &self,
        outbox: &session::SessionRuntimeOutboxRecord,
        reason: &str,
    ) {
        let stream = match self.session_input_stream_for(&outbox.session_id).await {
            Ok(stream) => stream,
            Err(_) => return,
        };
        let input_id = SessionInputId::from_string(outbox.input_id.clone());
        match stream.cancel_input(&input_id, reason) {
            Ok(record) => {
                let receipt = record.to_receipt();
                self.emit_session_input_events(&outbox.session_id, &stream, Some(receipt.clone()));
                self.persist_session_input_domain_event(
                    &outbox.session_id,
                    SessionInputJournalKind::Cancelled,
                    Some(&receipt),
                    Some(&record),
                    &stream,
                    &format!("{}:{}", outbox.request_id, outbox.input_id),
                )
                .await;
            }
            Err(error) => tracing::debug!(
                session_id = %outbox.session_id,
                request_id = %outbox.request_id,
                %error,
                "cancelled Runtime turn had no mutable primary ingress projection"
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
            receipt.input_id.as_str(),
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
            &format!("{}:{:?}", receipt.input_id, decision),
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
        let policy = self.effective_session_execution_policy(session_id);
        self.build_session_runtime_entry_with_execution_policy(
            session,
            session_id,
            model,
            system_prompt,
            &policy,
            None,
        )
    }

    fn build_session_runtime_entry_with_execution_policy(
        &self,
        session: runtime::Session,
        session_id: &str,
        model: &str,
        system_prompt: Vec<String>,
        policy: &runtime::SessionExecutionPolicy,
        resume_context: Option<runtime::ResumeContextPacket>,
    ) -> Result<crate::runtime_entry::GatewayRuntimeEntry, String> {
        let entry = crate::runtime_factory::create_runtime_entry(
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
            policy.clone(),
            None,
            None,
            self.session_bootstrap
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            resume_context,
        )
        .map_err(|error| error.to_string())?;
        Ok(entry)
    }

    fn effective_session_execution_policy(
        &self,
        session_id: &str,
    ) -> runtime::SessionExecutionPolicy {
        self.sessions
            .session(session_id)
            .and_then(|session| session.policy())
            .or_else(|| {
                self.runtime_services
                    .session_execution_policy_control(session_id)
                    .map(|control| control.snapshot())
            })
            .unwrap_or_else(|| self.default_execution_policy())
    }

    pub(crate) async fn update_execution_policy_defaults(
        &self,
        permission_mode: runtime::PermissionMode,
        approval_profile: runtime::ApprovalProfile,
    ) -> serde_json::Value {
        let current_default = self.default_execution_policy();
        let mut next_default =
            runtime::SessionExecutionPolicy::from_defaults(permission_mode, approval_profile);
        let default_changed = !execution_policy_defaults_match(&current_default, &next_default);
        if default_changed {
            next_default.revision = current_default.revision.saturating_add(1);
            *self
                .execution_policy_default
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = next_default.clone();
        } else {
            next_default = current_default;
        }

        let session_ids = self
            .sessions
            .list()
            .into_iter()
            .filter(|session_id| {
                let policy = self.effective_session_execution_policy(session_id);
                policy.origin == runtime::SessionExecutionPolicyOrigin::ConfigDefault
                    && !execution_policy_defaults_match(&policy, &next_default)
            })
            .collect::<Vec<_>>();
        let mut updated = Vec::new();
        let mut warnings = Vec::new();
        for session_id in session_ids {
            let current = match self.session_execution_policy_value(&session_id).await {
                Ok(response)
                    if response.policy.origin
                        == runtime::SessionExecutionPolicyOrigin::ConfigDefault =>
                {
                    response
                }
                Ok(_) => continue,
                Err(error) => {
                    warnings.push(format!(
                        "Session {session_id} retained its prior policy because it could not be read: {error}"
                    ));
                    continue;
                }
            };
            let response = match self
                .set_session_execution_policy_with_approval(
                    &session_id,
                    next_default.autonomy_profile,
                    current.policy.revision,
                    runtime::SessionExecutionPolicyOrigin::ConfigDefault,
                    Some(next_default.approval_profile),
                )
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    warnings.push(format!(
                        "Session {session_id} retained its prior policy because its transition failed: {error}"
                    ));
                    continue;
                }
            };
            if response.persisted != Some(true) {
                continue;
            }
            updated.push(serde_json::json!({
                "session_id": session_id,
                "policy_revision": response.policy.revision,
                "effective_revision": response.permission_revision,
                "transition": response.transition,
            }));
        }

        serde_json::json!({
            "status": if !warnings.is_empty() {
                "attention"
            } else if default_changed || !updated.is_empty() {
                "applied"
            } else {
                "unchanged"
            },
            "policy": next_default,
            "default_changed": default_changed,
            "updated_active_sessions": updated.len(),
            "sessions": updated,
            "warnings": warnings,
        })
    }

    async fn session_execution_policy_state_from_record(
        &self,
        record: &session::SessionRecord,
    ) -> Result<harness_contract::policy::SessionExecutionPolicyState, String> {
        if let Some(state) = stored_session_execution_policy_state(record) {
            return Ok(state);
        }
        Ok(harness_contract::policy::SessionExecutionPolicyState {
            effective: self.resolve_stored_session_execution_policy(record).await?,
            desired: None,
            pending_transition: None,
        })
    }

    async fn persist_policy_transition_phase(
        &self,
        session_id: &str,
        state: &harness_contract::policy::SessionExecutionPolicyState,
    ) -> Result<(), String> {
        let record = self
            .session_data
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session {session_id} not found"))?;
        self.persist_session_execution_policy_state(&record, state)
            .await
    }

    async fn finalize_policy_transition_under_lock(
        &self,
        session_id: &str,
        transition_id: &str,
    ) -> Result<Option<harness_contract::policy::PolicyTransitionReceipt>, String> {
        let record = self
            .session_data
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session {session_id} not found"))?;
        let mut state = self
            .session_execution_policy_state_from_record(&record)
            .await?;
        let Some(mut receipt) = state.pending_transition.clone() else {
            return Ok(None);
        };
        if receipt.transition_id != transition_id {
            // A newer desired revision owns this Session. The replaced task
            // must never publish its stale snapshot.
            return Ok(None);
        }
        let desired = state.desired.clone().ok_or_else(|| {
            format!("policy transition {transition_id} has no durable desired Session policy")
        })?;
        let active = self
            .active_attempts_for_policy_revision(session_id, receipt.effective_revision)
            .await?;
        if active > 0 {
            return Ok(Some(receipt));
        }

        receipt.phase = harness_contract::policy::PolicyTransitionPhase::Rebinding;
        receipt.old_revision_active_attempts = 0;
        state.pending_transition = Some(receipt.clone());
        self.persist_session_execution_policy_state(&record, &state)
            .await?;

        let mut applied_revision = None;
        if let Some(control) = self
            .runtime_services
            .session_execution_policy_control(session_id)
        {
            applied_revision = Some(control.replace(desired.clone())?);
        } else if let Some(runtime_entry) = self.sessions.get(session_id) {
            let runtime_guard = lock_runtime_entry(&runtime_entry).await;
            let control = runtime_guard.execution_policy_control();
            applied_revision = Some(control.replace(desired.clone())?);
            self.runtime_services
                .publish_session_execution_policy(session_id.to_string(), control);
        }
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        receipt.phase = harness_contract::policy::PolicyTransitionPhase::Stable;
        receipt.effective_revision = desired.revision;
        receipt.effective_at_ms = Some(now);
        receipt.blocker = None;
        receipt.failure = None;
        let stable_state = harness_contract::policy::SessionExecutionPolicyState {
            effective: desired.clone(),
            desired: None,
            pending_transition: None,
        };
        self.persist_policy_transition_phase(session_id, &stable_state)
            .await?;
        self.unfreeze_session_policy_transition(session_id, transition_id);

        if let Some(bus) = self
            .sessions
            .session(session_id)
            .and_then(|session| session.event_bus())
        {
            bus.emit(runtime::CowdEvent::PermissionRevisionChanged {
                permission_mode: desired.permission_mode.as_str().to_string(),
                revision: applied_revision.unwrap_or(desired.revision),
                applies_to_active_turn: applied_revision.is_some(),
            });
        }
        let mut domain_event = session::SessionDomainEvent::new(
            session_id,
            0,
            session::SessionDomainScope::Session,
            "session.permission_revision.changed",
            serde_json::json!({
                "policy": desired,
                "policy_revision": receipt.effective_revision,
                "revision": applied_revision.unwrap_or(0),
                "transition": receipt,
                "applies_to_active_turn": applied_revision.is_some(),
                "safe_replay": "started attempts retained their bound revision; new admission resumed only after Stable",
            }),
            now,
        );
        domain_event.event_id = format!("session-execution-policy:{session_id}:{transition_id}");
        domain_event.correlation_id = Some(format!("session-execution-policy:{session_id}"));
        if let Err(error) = self
            .session_data
            .append_control_domain_event_if_absent(&domain_event)
            .await
        {
            // Stable policy state is the commit authority. A secondary event
            // projection failure must not report the already-applied policy
            // transition as failed; durable metadata recovery can re-emit it.
            tracing::warn!(session_id, transition_id, %error, "stable policy transition event projection failed");
        }
        Ok(Some(receipt))
    }

    async fn run_policy_transition(
        self,
        session_id: String,
        transition_id: String,
        effective_revision: u64,
        cancellation: runtime::CancellationToken,
    ) {
        if self
            .wait_for_policy_revision_to_drain(
                &session_id,
                effective_revision,
                &transition_id,
                &cancellation,
            )
            .await
            .is_err()
        {
            return;
        }
        let update_lock = self.session_policy_update_lock(&session_id);
        let _guard = update_lock.lock().await;
        if let Err(error) = self
            .finalize_policy_transition_under_lock(&session_id, &transition_id)
            .await
        {
            tracing::error!(session_id, transition_id, %error, "policy transition finalization failed");
        }
    }

    fn session_policy_update_lock(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(session) = self.sessions.session(session_id) {
            return session.policy_transition_lock();
        }
        let stripe = session_id.bytes().fold(0_usize, |hash, byte| {
            hash.wrapping_mul(31).wrapping_add(usize::from(byte))
        }) % self.policy_transition_stripes.len();
        Arc::clone(&self.policy_transition_stripes[stripe])
    }

    async fn supervise_policy_transition(
        &self,
        session_id: &str,
        receipt: &harness_contract::policy::PolicyTransitionReceipt,
    ) -> Result<(), String> {
        let service = self.clone();
        let owned_session = session_id.to_string();
        let transition_id = receipt.transition_id.clone();
        let effective_revision = receipt.effective_revision;
        self.gateway_tasks
            .replace_session_task(
                crate::runtime_host::task_set::GatewayTaskKind::PolicyTransition,
                session_id,
                move |cancellation| async move {
                    service
                        .run_policy_transition(
                            owned_session,
                            transition_id,
                            effective_revision,
                            cancellation,
                        )
                        .await;
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("cannot supervise Session policy transition: {error}"))
    }

    pub(crate) async fn set_session_execution_policy(
        &self,
        session_id: &str,
        profile: runtime::AutonomyProfileId,
        expected_revision: u64,
        origin: runtime::SessionExecutionPolicyOrigin,
    ) -> Result<harness_contract::policy::SessionExecutionPolicyResponse, String> {
        self.set_session_execution_policy_with_approval(
            session_id,
            profile,
            expected_revision,
            origin,
            None,
        )
        .await
    }

    async fn set_session_execution_policy_with_approval(
        &self,
        session_id: &str,
        profile: runtime::AutonomyProfileId,
        expected_revision: u64,
        origin: runtime::SessionExecutionPolicyOrigin,
        approval_profile: Option<runtime::ApprovalProfile>,
    ) -> Result<harness_contract::policy::SessionExecutionPolicyResponse, String> {
        let update_lock = self.session_policy_update_lock(session_id);
        let update_guard = update_lock.lock().await;
        let record = self
            .session_data
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session {session_id} not found"))?;
        let mut state = self
            .session_execution_policy_state_from_record(&record)
            .await?;
        let exposed_revision = state
            .desired
            .as_ref()
            .map_or(state.effective.revision, |policy| policy.revision);
        if exposed_revision != expected_revision {
            return Err(format!(
                "session_execution_policy_revision_conflict: expected {expected_revision}, current {}",
                exposed_revision
            ));
        }
        let mut next_policy = runtime::SessionExecutionPolicy::from_profile(
            profile,
            exposed_revision.saturating_add(1),
            origin,
        );
        if let Some(approval_profile) = approval_profile {
            next_policy = next_policy.with_approval_profile(approval_profile);
        }
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let mut receipt = harness_contract::policy::PolicyTransitionReceipt {
            transition_id: format!("policy-transition:{}:{}", session_id, uuid::Uuid::new_v4()),
            phase: harness_contract::policy::PolicyTransitionPhase::Persisted,
            desired_revision: next_policy.revision,
            effective_revision: state.effective.revision,
            old_revision_active_attempts: 0,
            requested_at_ms: now,
            effective_at_ms: None,
            blocker: None,
            failure: None,
        };
        state.desired = Some(next_policy.clone());
        state.pending_transition = Some(receipt.clone());
        self.persist_session_execution_policy_state(&record, &state)
            .await?;

        receipt.phase = harness_contract::policy::PolicyTransitionPhase::Freezing;
        let _ = self.freeze_session_policy_transition(session_id, &receipt);
        let active = self
            .active_attempts_for_policy_revision(session_id, receipt.effective_revision)
            .await?;
        receipt.old_revision_active_attempts = active;
        receipt.phase = if active == 0 {
            harness_contract::policy::PolicyTransitionPhase::Rebinding
        } else {
            harness_contract::policy::PolicyTransitionPhase::Draining
        };
        receipt.blocker = (active > 0).then(|| {
            format!(
                "waiting for {active} attempt(s) bound to policy revision {}",
                receipt.effective_revision
            )
        });
        state.pending_transition = Some(receipt.clone());
        self.persist_policy_transition_phase(session_id, &state)
            .await?;

        let transition = if active == 0 {
            self.finalize_policy_transition_under_lock(session_id, &receipt.transition_id)
                .await?
                .unwrap_or(receipt)
        } else {
            receipt
        };
        drop(update_guard);
        if transition.phase != harness_contract::policy::PolicyTransitionPhase::Stable {
            self.supervise_policy_transition(session_id, &transition)
                .await?;
        }
        let applied_to_active_runtime = transition.phase
            == harness_contract::policy::PolicyTransitionPhase::Stable
            && self.sessions.get(session_id).is_some();
        let response_state =
            if transition.phase == harness_contract::policy::PolicyTransitionPhase::Stable {
                harness_contract::policy::SessionExecutionPolicyState {
                    effective: next_policy.clone(),
                    desired: None,
                    pending_transition: None,
                }
            } else {
                harness_contract::policy::SessionExecutionPolicyState {
                    effective: state.effective.clone(),
                    desired: Some(next_policy.clone()),
                    pending_transition: Some(transition.clone()),
                }
            };
        Ok(harness_contract::policy::SessionExecutionPolicyResponse {
            session_id: session_id.to_string(),
            state: response_state,
            matched_preset: next_policy.matched_preset(),
            active_turn: harness_contract::policy::SessionExecutionPolicyActiveTurn {
                state: if transition.phase
                    == harness_contract::policy::PolicyTransitionPhase::Stable
                    && applied_to_active_runtime
                {
                    "applied".to_string()
                } else if transition.phase
                    == harness_contract::policy::PolicyTransitionPhase::Stable
                {
                    "applies_on_activation".to_string()
                } else {
                    "draining_previous_revision".to_string()
                },
                applied_revision: Some(transition.effective_revision),
            },
            policy: next_policy,
            permission_revision: Some(transition.effective_revision),
            persisted: Some(true),
            applied_to_active_runtime: Some(applied_to_active_runtime),
            applies_after_active_turn: Some(
                transition.phase != harness_contract::policy::PolicyTransitionPhase::Stable,
            ),
            safe_replay: Some(
                "started attempts retain their exact policy revision; queued admission resumes only after the newest desired revision is Stable"
                    .to_string(),
            ),
            transition: Some(transition),
        })
    }

    pub(crate) async fn session_execution_policy_value(
        &self,
        session_id: &str,
    ) -> Result<harness_contract::policy::SessionExecutionPolicyResponse, String> {
        let update_lock = self.session_policy_update_lock(session_id);
        let update_guard = update_lock.lock().await;
        let record = self
            .session_data
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session {session_id} not found"))?;
        let mut state = self
            .session_execution_policy_state_from_record(&record)
            .await?;
        let mut transition = state.pending_transition.clone();
        let mut needs_supervisor = false;
        if let Some(receipt) = transition.as_mut() {
            let (_, changed) = self.freeze_session_policy_transition(session_id, receipt);
            let active = self
                .active_attempts_for_policy_revision(session_id, receipt.effective_revision)
                .await?;
            receipt.old_revision_active_attempts = active;
            if active == 0 {
                if let Some(stable) = self
                    .finalize_policy_transition_under_lock(session_id, &receipt.transition_id)
                    .await?
                {
                    state.effective = state
                        .desired
                        .clone()
                        .unwrap_or_else(|| state.effective.clone());
                    state.desired = None;
                    transition = Some(stable);
                }
            } else {
                needs_supervisor = changed;
            }
        }
        let policy = state
            .desired
            .clone()
            .unwrap_or_else(|| state.effective.clone());
        drop(update_guard);
        if needs_supervisor {
            if let Some(receipt) = transition.as_ref() {
                self.supervise_policy_transition(session_id, receipt)
                    .await?;
            }
        }
        let stable = transition.as_ref().is_none_or(|receipt| {
            receipt.phase == harness_contract::policy::PolicyTransitionPhase::Stable
        });
        let active = self.sessions.get(session_id).is_some() && stable;
        let effective_revision = transition
            .as_ref()
            .map_or(state.effective.revision, |receipt| {
                receipt.effective_revision
            });
        if stable {
            state.pending_transition = None;
            state.desired = None;
        } else {
            state.pending_transition = transition.clone();
        }
        Ok(harness_contract::policy::SessionExecutionPolicyResponse {
            session_id: session_id.to_string(),
            state,
            matched_preset: policy.matched_preset(),
            active_turn: harness_contract::policy::SessionExecutionPolicyActiveTurn {
                state: if !stable {
                    "draining_previous_revision".to_string()
                } else if active {
                    "applied".to_string()
                } else {
                    "applies_on_activation".to_string()
                },
                applied_revision: Some(effective_revision),
            },
            policy,
            permission_revision: Some(effective_revision),
            persisted: None,
            applied_to_active_runtime: Some(active),
            applies_after_active_turn: Some(!stable),
            safe_replay: Some(
                "started attempts retain their exact policy revision; new admission resumes only after Stable"
                    .to_string(),
            ),
            transition,
        })
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
        if runtime_guard.turn_is_owned() {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "session {session_id} is already executing; context changes apply to the next admitted turn"
            )));
        }
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
        if runtime_guard.turn_is_owned() {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "session {session_id} is already executing a turn"
            )));
        }
        runtime_guard.install_turn_control(cancellation_token, hook_abort_signal);
        Ok(())
    }

    fn clear_session_input_turn_if_current(&self, session_id: &str, turn_id: &TurnId) {
        let Some(stream) = self
            .sessions
            .session(session_id)
            .and_then(|session| session.input())
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
        input.primary_task_id = task_id;
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
                primary_task_id: None,
                task_bindings: Vec::new(),
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
        let terminal = turn.clone();
        if turn_status_is_terminal(&terminal.status) {
            turns.remove(&turn_id.to_string());
        }
        terminal
    }

    pub(crate) async fn session_snapshot(&self, session_id: &str) -> Option<runtime::Session> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        Some(runtime_guard.session_snapshot().await)
    }

    pub(crate) async fn compact_active_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionCompactResult>, session::SessionError> {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return Ok(None);
        };

        let mut runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return Err(session::SessionError::Other(format!(
                "session {session_id} is executing; compaction requires an idle Runtime carrier"
            )));
        }
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
        tail: bool,
    ) -> Option<ActiveMessagesPage> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        let session = runtime_guard.session_snapshot().await;

        let total = session.message_count();
        let start = from_seq
            .unwrap_or_else(|| {
                if tail {
                    total.saturating_sub(limit)
                } else {
                    offset
                }
            })
            .min(total);
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
                    .filter_map(|block| match block {
                        runtime::ContentBlock::Text { text } => {
                            Some(serde_json::json!({"type": "text", "text": text}))
                        }
                        runtime::ContentBlock::ReasoningSummary { text } => {
                            Some(serde_json::json!({"type": "reasoning_summary", "text": text}))
                        }
                        runtime::ContentBlock::Image {
                            media_type,
                            data,
                            source_path,
                        } => {
                            Some(serde_json::json!({
                                "type": "image",
                                "media_type": media_type,
                                "source_path": source_path,
                                "size_bytes": data.len() * 3 / 4,
                            }))
                        }
                        // Private Provider transcript state is required only
                        // for the next Provider request and is never projected
                        // through the Gateway history API.
                        runtime::ContentBlock::Thinking { .. } => None,
                        runtime::ContentBlock::ToolUse { id, name, input } => {
                            Some(serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input}))
                        }
                        runtime::ContentBlock::ToolResult {
                            tool_use_id,
                            tool_name,
                            output,
                            is_error,
                        } => {
                            Some(serde_json::json!({"type": "tool_result", "tool_use_id": tool_use_id, "tool_name": tool_name, "output": output, "is_error": is_error}))
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
    ) -> Result<bool, String> {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return Ok(false);
        };
        let Some(model) = model else {
            return Ok(true);
        };
        let model = self.resolve_session_model(Some(model))?;
        if let Some(session) = self.sessions.session(session_id) {
            session.set_model(Some(model.clone()));
        }
        let mut runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return Ok(true);
        }
        runtime_guard.update_session_model(&model).await;
        Ok(true)
    }

    pub(crate) async fn last_context_envelope(
        &self,
        session_id: &str,
    ) -> Option<runtime::ContextEnvelope> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
        runtime_guard.last_context_envelope()
    }

    pub(crate) async fn last_context_turn_report(
        &self,
        session_id: &str,
    ) -> Option<harness_contract::context::ContextTurnReport> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = lock_runtime_entry(&runtime_entry).await;
        if runtime_guard.turn_is_owned() {
            return None;
        }
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

struct SessionApprovalControl {
    approval_id: Option<String>,
    approved: bool,
    skip: bool,
    scope: runtime::ApprovalGrantScope,
}

fn parse_session_approval_control(content: &str) -> Option<SessionApprovalControl> {
    let tokens = content.split_whitespace().collect::<Vec<_>>();
    let command = tokens.first()?.to_ascii_lowercase();
    let (approved, skip) = match command.as_str() {
        "/approve" | "approve" | "批准" | "同意" => (true, false),
        "/deny" | "deny" | "拒绝" => (false, false),
        "/skip" | "skip" | "跳过" => (false, true),
        _ => return None,
    };
    let mut approval_id = None;
    let mut scope = runtime::ApprovalGrantScope::Once;
    for token in tokens.iter().skip(1) {
        let normalized = token.to_ascii_lowercase();
        let parsed_scope = match normalized.as_str() {
            "once" | "本次" => Some(runtime::ApprovalGrantScope::Once),
            "turn" | "本轮" | "回合" => Some(runtime::ApprovalGrantScope::Turn),
            "task" | "任务" => Some(runtime::ApprovalGrantScope::Task),
            "session" | "会话" => Some(runtime::ApprovalGrantScope::Session),
            "global" | "全局" => Some(runtime::ApprovalGrantScope::Global),
            _ => None,
        };
        if let Some(parsed_scope) = parsed_scope {
            scope = parsed_scope;
        } else if approval_id.is_none() {
            approval_id = Some((*token).to_string());
        }
    }
    Some(SessionApprovalControl {
        approval_id,
        approved,
        skip,
        scope,
    })
}

fn surface_actor_from_classification(classification_json: Option<&str>) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(classification_json?).ok()?;
    let surface = value
        .pointer("/metadata/surface")
        .and_then(serde_json::Value::as_str)?;
    let user = value
        .pointer("/metadata/user_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("bound-user");
    Some(format!("surface:{surface}:{user}"))
}
