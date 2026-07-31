//! Workspace-owned runtime service graph.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use harness_contract::agent::{
    AgentEvaluationBinding, AgentReleaseBinding, AgentTaskIntent, AgentTaskPacket,
    AgentTerminalStatus, ReleaseChannel, RevisionSelector,
};
use harness_contract::context::ContextBudgetLeaseRef;
use harness_contract::evaluation::{EvaluationScenarioObservation, EvaluationScenarioSpec};
use harness_contract::execution::ExecutionIdentity;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeStatus,
};
use harness_contract::team::{
    TeamInstantiationRequest, TeamSelectionMode, TeamTemplateRevisionRef, TeamTemplateSelector,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::cross_plane::{CrossPlaneRuntimeError, CrossPlaneRuntimeService};
use super::goal::GoalStore;
use super::graph::{
    executors::{
        AgentTaskExecutor, ApprovalNodeExecutor, CompileTargetGuardExecutor, ScopedNodeExecutor,
        SynthesizeNodeExecutor, VerifyNodeExecutor,
    },
    ExecutionCommitService, ExecutionGraphRunner, ExecutionGraphStateStore, ExecutionRecoveryError,
    ExecutionResourceKind, ExecutionResourceManager, ExecutionRunnerError,
    ExecutionStateStoreError, NodeExecutor, NodeExecutorError, NodeExecutorRegistry,
    ResourceAdmissionObservationStatus, ResourceQuota, ScopeLockError, ScopeLockManager,
    WorktreeLeaseError, WorktreeLeaseManager,
};
use super::protocols::ProtocolResultReducer;
use crate::agent::binding::request_for_intent;
use crate::agent::definition::ExplicitTomlAgentImport;
use crate::managed_agent::ManagedAgentRestartDisposition;
use crate::runtime_event_store::RuntimeEventStoreError;
use crate::{
    AgentBindingCompiler, AgentBindingRequest, AgentDefinitionDraftReceipt, AgentRuntime,
    AgentRuntimeResolver, ApprovalQueue, CompiledAgentBinding, ConflictArbiter,
    DefinitionRegistryError, DurableRuntimeEvent, ExecutionGraphHost, InProcessAgentWorker,
    ManagedAgentRuntimeDispatchReport, MissionEvidenceBus, MissionRuntime, MissionScheduleStore,
    ProcessJsonlAdapter, RealityRecallPort, RuntimeDefinitionRegistry, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventReplayer, RuntimeEventScope, RuntimeEventStore,
    RuntimeSessionOutboxFailureClass, RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord,
    SessionInputRouter, SessionRelationGraph, TeamResultReducer, TeamRuntime,
};

#[derive(Debug, Error)]
pub enum RuntimeServicesError {
    #[error(transparent)]
    Storage(#[from] storage::StorageError),
    #[error(transparent)]
    DefinitionRegistry(#[from] DefinitionRegistryError),
    #[error(transparent)]
    EventStore(#[from] RuntimeEventStoreError),
    #[error(transparent)]
    Artifact(#[from] crate::ArtifactError),
    #[error(transparent)]
    WorktreeLease(#[from] WorktreeLeaseError),
    #[error(transparent)]
    ScopeLock(#[from] ScopeLockError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    CrossPlane(#[from] CrossPlaneRuntimeError),
    #[error(transparent)]
    NodeExecutor(#[from] NodeExecutorError),
    #[error(transparent)]
    GraphState(#[from] ExecutionStateStoreError),
    #[error(transparent)]
    GraphRecovery(#[from] ExecutionRecoveryError),
    #[error(transparent)]
    GraphRunner(#[from] ExecutionRunnerError),
    #[error("runtime service root cannot be empty")]
    EmptyRoot,
    #[error("mission runtime initialization failed: {0}")]
    Mission(String),
    #[error("task runtime initialization failed: {0}")]
    Task(String),
    #[error("agent runtime initialization failed: {0}")]
    AgentRuntime(String),
    #[error("session input router was concurrently installed")]
    DuplicateSessionRouter,
    #[error("session integration requires query, ingress and journal ports together")]
    IncompleteSessionPorts,
    #[error("workspace mutation is blocked because upgrade recovery is required")]
    UpgradeRecoveryRequired,
    #[error("durable session handoff recovery failed: {0}")]
    SessionHandoffRecovery(String),
    #[error("execution projection access is denied by the current scoped authorization")]
    ProjectionAccessDenied,
    #[error("execution projection invariant failed: {0}")]
    Invariant(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStartupRecoveryReport {
    pub examined_graphs: usize,
    pub recovered_graphs: usize,
    pub notified_graphs: usize,
    pub advanced_graphs: usize,
    pub terminal_graphs: usize,
    pub waiting_graphs: usize,
    pub blocked_graphs: usize,
    pub resolved_handoff_results: usize,
    pub errors: Vec<ExecutionStartupRecoveryError>,
    pub records: Vec<ExecutionStartupRecoveryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStartupRecoveryRecord {
    pub graph_id: String,
    pub objective: String,
    pub before_revision: u64,
    pub after_revision: u64,
    pub before_status: String,
    pub after_status: String,
    pub action: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStartupRecoveryError {
    pub graph_id: String,
    pub error: String,
}

pub struct RuntimeServicesBuilder {
    cowd_home: PathBuf,
    workspace_root: PathBuf,
    runtime_event_store: Option<Arc<RuntimeEventStore>>,
    task_aggregate_service: Option<Arc<crate::TaskAggregateService>>,
    builtin_definitions_root: Option<PathBuf>,
    resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
    provider_registry: Arc<crate::ProviderRegistry>,
    provider_fallbacks: Vec<String>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    session_query_port: Option<Arc<dyn crate::SessionRuntimeQueryPort>>,
    session_ingress_port: Option<Arc<dyn crate::SessionRuntimeIngressPort>>,
    session_journal_port: Option<Arc<dyn crate::SessionRuntimeJournalPort>>,
    artifact_store: Option<Arc<crate::ArtifactStore>>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    reality_recall_port: Option<Arc<RealityRecallPort>>,
    knowledge_activation: Option<crate::knowledge_activation::KnowledgeActivationRuntime>,
    evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
    skill_catalog: crate::RuntimeSkillCatalog,
    mission_schedule_policy: crate::MissionSchedulePolicy,
}

/// Runtime-owned supervisor for non-critical-path maintenance.
///
/// Tasks are serialized per logical owner, retained until completion, and
/// drained explicitly during process shutdown. This keeps post-turn work off
/// the response path without creating detached tasks.
type MaintenanceWork = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaintenanceLifecycle {
    Open,
    Closing,
    Closed,
}

struct MaintenanceOwner {
    generation: u64,
    queued: VecDeque<MaintenanceWork>,
    handle: tokio::task::JoinHandle<()>,
}

struct MaintenanceState {
    lifecycle: MaintenanceLifecycle,
    next_generation: u64,
    owners: HashMap<String, MaintenanceOwner>,
    reaping: usize,
}

struct MaintenanceCompletion {
    owner: MaintenanceOwner,
}

pub(crate) struct RuntimeMaintenanceSupervisor {
    state: Arc<Mutex<MaintenanceState>>,
    changed: Arc<tokio::sync::Notify>,
    completion_tx: tokio::sync::mpsc::UnboundedSender<MaintenanceCompletion>,
    completion_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<MaintenanceCompletion>>>,
    reaper: Mutex<Option<tokio::task::JoinHandle<()>>>,
    reaper_cancellation: crate::CancellationToken,
    shutdown_lock: tokio::sync::Mutex<()>,
    shutdown_timeout: Duration,
}

impl RuntimeMaintenanceSupervisor {
    fn new() -> Self {
        Self::with_shutdown_timeout(Duration::from_secs(10))
    }

    fn with_shutdown_timeout(shutdown_timeout: Duration) -> Self {
        let (completion_tx, completion_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            state: Arc::new(Mutex::new(MaintenanceState {
                lifecycle: MaintenanceLifecycle::Open,
                next_generation: 0,
                owners: HashMap::new(),
                reaping: 0,
            })),
            changed: Arc::new(tokio::sync::Notify::new()),
            completion_tx,
            completion_rx: Mutex::new(Some(completion_rx)),
            reaper: Mutex::new(None),
            reaper_cancellation: crate::CancellationToken::new(),
            shutdown_lock: tokio::sync::Mutex::new(()),
            shutdown_timeout,
        }
    }

    pub(crate) async fn submit<F>(&self, owner: String, work: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if !self.ensure_reaper() {
            return false;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.lifecycle != MaintenanceLifecycle::Open {
            return false;
        }

        if let Some(existing) = state.owners.get_mut(&owner) {
            existing.queued.push_back(Box::pin(work));
            return true;
        }

        state.next_generation = state.next_generation.saturating_add(1);
        let generation = state.next_generation;
        let worker_state = Arc::downgrade(&self.state);
        let worker_changed = Arc::clone(&self.changed);
        let completion_tx = self.completion_tx.clone();
        let worker_owner = owner.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_rx.await.is_err() {
                return;
            }
            let mut current: MaintenanceWork = Box::pin(work);
            loop {
                let _ = std::panic::AssertUnwindSafe(current).catch_unwind().await;
                let Some(state) = worker_state.upgrade() else {
                    return;
                };
                let next = {
                    let mut state = state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = state.owners.get_mut(&worker_owner) else {
                        return;
                    };
                    if entry.generation != generation {
                        return;
                    }
                    if let Some(next) = entry.queued.pop_front() {
                        Some(next)
                    } else {
                        let completed = state
                            .owners
                            .remove(&worker_owner)
                            .expect("maintenance owner exists while its worker is running");
                        state.reaping = state.reaping.saturating_add(1);
                        let _ = completion_tx.send(MaintenanceCompletion { owner: completed });
                        None
                    }
                };
                worker_changed.notify_waiters();
                match next {
                    Some(next) => current = next,
                    None => return,
                }
            }
        });
        state.owners.insert(
            owner,
            MaintenanceOwner {
                generation,
                queued: VecDeque::new(),
                handle,
            },
        );
        let _ = start_tx.send(());
        true
    }

    fn ensure_reaper(&self) -> bool {
        let mut reaper = self
            .reaper
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reaper.is_some() {
            return true;
        }
        let Some(mut receiver) = self
            .completion_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return false;
        };
        let state = Arc::downgrade(&self.state);
        let changed = Arc::clone(&self.changed);
        let cancellation = self.reaper_cancellation.clone();
        *reaper = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    completion = receiver.recv() => {
                        let Some(mut completion) = completion else {
                            break;
                        };
                        let _ = (&mut completion.owner.handle).await;
                        let Some(state) = state.upgrade() else {
                            break;
                        };
                        let mut state = state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        state.reaping = state.reaping.saturating_sub(1);
                        drop(state);
                        changed.notify_waiters();
                    }
                }
            }
        }));
        true
    }

    async fn shutdown_and_drain(&self) {
        let _shutdown = self.shutdown_lock.lock().await;
        let wait_for_existing_shutdown = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match state.lifecycle {
                MaintenanceLifecycle::Open => {
                    state.lifecycle = MaintenanceLifecycle::Closing;
                    false
                }
                MaintenanceLifecycle::Closing => true,
                MaintenanceLifecycle::Closed => return,
            }
        };

        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        loop {
            let notified = self.changed.notified();
            let lifecycle = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.owners.is_empty() && state.reaping == 0 {
                    Some(state.lifecycle)
                } else {
                    None
                }
            };
            if matches!(lifecycle, Some(MaintenanceLifecycle::Closed)) {
                return;
            }
            if lifecycle.is_some() {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.lifecycle = MaintenanceLifecycle::Closed;
                self.changed.notify_waiters();
                break;
            }
            if wait_for_existing_shutdown && tokio::time::Instant::now() >= deadline {
                break;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }

        let owners = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.lifecycle = MaintenanceLifecycle::Closed;
            std::mem::take(&mut state.owners)
        };
        for (_, owner) in owners {
            owner.handle.abort();
            let _ = owner.handle.await;
        }
        self.reaper_cancellation.cancel();
        let reaper = self
            .reaper
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(mut reaper) = reaper {
            if tokio::time::timeout(self.shutdown_timeout, &mut reaper)
                .await
                .is_err()
            {
                reaper.abort();
                let _ = reaper.await;
            }
        }
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    fn tracked_task_count(&self) -> usize {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.owners.len().saturating_add(state.reaping)
    }
}

/// Read-only projection port for the durable runtime ledger.
///
/// Gateway and surfaces receive this port rather than the SQLite-backed event
/// store.  Keeping the store private prevents a projection consumer from
/// silently becoming another lifecycle event writer.
#[derive(Clone)]
pub struct RuntimeEventReader {
    store: Arc<RuntimeEventStore>,
}

impl RuntimeEventReader {
    #[must_use]
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.store.subscribe_commits()
    }

    pub fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> Result<Vec<crate::CommittedEventBatch>, String> {
        self.store
            .events_after_cursor(cursor, max_commits)
            .map_err(|error| error.to_string())
    }

    pub fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.store.list_stream(stream_id)
    }

    pub fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.store.list_scope(scope, limit)
    }

    pub fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.store.all_events(limit)
    }

    pub fn replay_report(&self, limit: usize) -> Result<crate::RuntimeReplayReport, String> {
        RuntimeEventReplayer::report(&self.store, limit)
    }

    pub fn session_timeline_events(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut events = self
            .store
            .list_stream(session_id)?
            .into_iter()
            .filter(|event| {
                after_position.is_none_or(|position| {
                    (event.commit_cursor, event.transaction_index) > position
                })
            })
            .collect::<Vec<_>>();
        events.extend(self.store.execution_events_for_session(
            session_id,
            after_position,
            limit,
        )?);
        events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        events.dedup_by(|left, right| left.event_id == right.event_id);
        events.truncate(limit);
        Ok(events)
    }
}

/// Fixture-only ledger writer used by downstream unit tests to seed durable
/// historical state. Production Gateway paths never receive this capability.
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
#[derive(Clone)]
pub struct RuntimeFixtureEventPort {
    store: Arc<RuntimeEventStore>,
}

#[cfg(feature = "test-fixtures")]
impl RuntimeFixtureEventPort {
    pub fn append_for_test(&self, event: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        self.store.append(event)
    }
}

/// Narrow delivery port for the Gateway-owned surface delivery worker.
///
/// It deliberately exposes only the terminal-outbox state machine.  It cannot
/// append arbitrary runtime events or access graph/approval transactions.
#[derive(Clone)]
pub struct SessionTerminalDeliveryPort {
    store: Arc<RuntimeEventStore>,
}

impl SessionTerminalDeliveryPort {
    pub fn enqueue(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.enqueue_session_terminal(
            terminal_id,
            message_id,
            session_id,
            commit_cursor,
            payload_ref,
        )
    }

    pub fn get(
        &self,
        terminal_id: &str,
    ) -> Result<Option<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store.session_terminal(terminal_id)
    }

    pub fn claim(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store
            .claim_session_terminals(worker_id, now_ms, lease_ms, limit)
    }

    pub fn acknowledge(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store
            .ack_session_terminal(terminal_id, worker_id, expected_revision, now_ms)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fail(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.fail_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            class,
            error,
            retry_at_ms,
            max_attempts,
            now_ms,
        )
    }

    pub fn health(&self) -> Result<RuntimeSessionOutboxHealth, RuntimeEventStoreError> {
        self.store.session_terminal_health()
    }

    pub fn has_unsettled_for_session(
        &self,
        session_id: &str,
    ) -> Result<bool, RuntimeEventStoreError> {
        self.store.has_unsettled_session_terminals(session_id)
    }

    pub fn blocked(
        &self,
        limit: usize,
    ) -> Result<Vec<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store.blocked_session_terminals(limit)
    }

    pub fn materialized_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> Result<Vec<RuntimeSessionOutboxRecord>, RuntimeEventStoreError> {
        self.store
            .materialized_session_terminals_after(session_id, after_commit_cursor, limit)
    }

    pub fn retry_blocked(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store
            .retry_session_terminal(terminal_id, actor, reason, now_ms)
    }

    pub fn adopt_fence(
        &self,
        request: &crate::RuntimeSessionTerminalFenceAdoption,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.adopt_session_terminal_fence(request)
    }
}

impl RuntimeServicesBuilder {
    #[must_use]
    pub fn resource_quotas(
        mut self,
        quotas: impl IntoIterator<Item = (ExecutionResourceKind, ResourceQuota)>,
    ) -> Self {
        self.resource_quotas = quotas.into_iter().collect();
        self
    }

    #[must_use]
    pub fn provider_registry(mut self, registry: Arc<crate::ProviderRegistry>) -> Self {
        self.provider_registry = registry;
        self
    }

    /// Install the ordered fallback policy shared by every conversation in
    /// this RuntimeServices instance.
    #[must_use]
    pub fn provider_fallbacks(mut self, fallbacks: impl IntoIterator<Item = String>) -> Self {
        self.provider_fallbacks = normalize_provider_fallbacks(fallbacks);
        self
    }

    #[must_use]
    pub fn tool_execution_host(mut self, host: Arc<dyn crate::RuntimeExecutionHost>) -> Self {
        self.tool_execution_host = Some(host);
        self
    }

    #[must_use]
    pub fn session_query_port(mut self, port: Arc<dyn crate::SessionRuntimeQueryPort>) -> Self {
        self.session_query_port = Some(port);
        self
    }

    #[must_use]
    pub fn session_ingress_port(mut self, port: Arc<dyn crate::SessionRuntimeIngressPort>) -> Self {
        self.session_ingress_port = Some(port);
        self
    }

    #[must_use]
    pub fn session_journal_port(mut self, port: Arc<dyn crate::SessionRuntimeJournalPort>) -> Self {
        self.session_journal_port = Some(port);
        self
    }

    #[must_use]
    pub fn artifact_store(mut self, store: Arc<crate::ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self
    }

    /// Install the only Memory kernel that Runtime-owned conversation hosts may
    /// use. Gateway may construct and monitor this component, but it must not
    /// assemble Memory context on behalf of a turn.
    #[must_use]
    pub fn memory_manager(mut self, manager: Arc<memory::CognitiveContextManager>) -> Self {
        self.memory_manager = Some(manager);
        self
    }

    /// Install the process-selected Fact/Matrix recall port. Runtime owns its
    /// use during prompt assembly but never chooses the physical backend.
    #[must_use]
    pub fn reality_recall_port(mut self, port: Arc<RealityRecallPort>) -> Self {
        self.reality_recall_port = Some(port);
        self
    }

    /// Install the selected durable Knowledge fabric once at startup. Turn
    /// construction clones this adapter and never reopens a database.
    #[must_use]
    pub fn knowledge_activation(
        mut self,
        activation: crate::knowledge_activation::KnowledgeActivationRuntime,
    ) -> Self {
        self.knowledge_activation = Some(activation);
        self
    }

    /// Inject a trusted evaluator at the composition root. Runtime owns the
    /// immutable comparison contract; evaluator implementations belong to
    /// `harness-eval` or another explicitly trusted adapter.
    #[must_use]
    pub fn evolution_eval_runner(mut self, runner: Arc<dyn crate::EvolutionEvalRunner>) -> Self {
        self.evolution_eval_runner = Some(runner);
        self
    }

    /// Install the inspected Skill snapshot at the Runtime composition root.
    /// Workers can activate these profiles but never discover packages.
    #[must_use]
    pub fn skill_catalog(mut self, catalog: crate::RuntimeSkillCatalog) -> Self {
        self.skill_catalog = catalog;
        self
    }

    /// Bind builtin Definitions to the installation bundle selected by the
    /// launcher. User and workspace Definitions are never inferred from this
    /// path; it is only the trusted builtin scope root.
    #[must_use]
    pub fn builtin_definitions_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.builtin_definitions_root = Some(root.into());
        self
    }

    #[must_use]
    pub fn mission_schedule_policy(mut self, policy: crate::MissionSchedulePolicy) -> Self {
        self.mission_schedule_policy = policy;
        self
    }

    /// Compose a verified durable event backend at the Runtime host boundary.
    /// This is explicit injection, not a process-wide backend switch; business
    /// callers continue to depend only on Runtime event semantics.
    #[must_use]
    pub fn runtime_event_store(mut self, store: Arc<RuntimeEventStore>) -> Self {
        self.runtime_event_store = Some(store);
        self
    }

    /// Install the selected durable Task aggregate backend. Runtime owns Task
    /// lifecycle semantics; the launcher may only select its physical store.
    #[must_use]
    pub fn task_aggregate_service(mut self, service: Arc<crate::TaskAggregateService>) -> Self {
        self.task_aggregate_service = Some(service);
        self
    }

    pub fn build(self) -> Result<Arc<RuntimeServices>, RuntimeServicesError> {
        if self.cowd_home.as_os_str().is_empty() || self.workspace_root.as_os_str().is_empty() {
            return Err(RuntimeServicesError::EmptyRoot);
        }
        let session_ports = match (
            self.session_query_port,
            self.session_ingress_port,
            self.session_journal_port,
        ) {
            (Some(query), Some(ingress), Some(journal)) => Some((query, ingress, journal)),
            (None, None, None) => None,
            _ => return Err(RuntimeServicesError::IncompleteSessionPorts),
        };
        let legacy_team_state_path = self
            .cowd_home
            .join("agents")
            .join("team-runtime")
            .join("state.json");
        let legacy_team_profile_path = self.cowd_home.join("agents").join("team-profiles.json");
        let legacy_team_profile_archive_root = self.cowd_home.join("migrations").join("teams");
        let workspace_root = canonical_workspace_root(&self.workspace_root)?;
        let workspace_key = workspace_key(&workspace_root);
        let storage_registry = storage::StorageRegistry::default_for_config_home(&self.cowd_home)
            .with_workspace(&workspace_root)?;
        let builtin_definitions_root = self.builtin_definitions_root.unwrap_or_else(|| {
            // An unconfigured installation has no runnable builtin Definitions
            // yet. This explicit empty bundle root preserves scope separation;
            // the launcher supplies the verified release-bundle root before
            // builtin bootstrap is enabled.
            self.cowd_home.join("runtime").join("builtin-definitions")
        });
        let definition_registry = Arc::new(RuntimeDefinitionRegistry::from_storage_registry(
            &storage_registry,
            builtin_definitions_root,
            &workspace_root,
        )?);
        let event_store = if let Some(store) = self.runtime_event_store {
            store
        } else {
            let event_scope = storage::StorageScope::workspace_for_root(&workspace_root);
            let runtime_event_handle = storage_registry
                .endpoint_in_scope(&storage::StorageDomainId::RuntimeEvents, &event_scope)?
                .as_handle();
            Arc::new(RuntimeEventStore::try_open(runtime_event_handle.path)?)
        };
        let artifact_store = self.artifact_store.unwrap_or_else(|| {
            Arc::new(crate::ArtifactStore::sqlite_default(
                storage_registry.layout.blobs.clone(),
            ))
        });
        let task_aggregate_service = match self.task_aggregate_service {
            Some(service) => service,
            None => {
                let task_scope = storage::StorageScope::workspace_for_root(&workspace_root);
                let task_handle = storage_registry
                    .endpoint_in_scope(&storage::StorageDomainId::Tasks, &task_scope)?
                    .as_handle();
                Arc::new(
                    crate::TaskAggregateService::open_storage_handle(&task_handle)
                        .map_err(RuntimeServicesError::Task)?,
                )
            }
        };
        let resource_state_root = std::env::temp_dir()
            .join("cowd-runtime-resource-locks")
            .join(&workspace_key);
        // Resource managers are owned by this RuntimeServices instance. Their
        // persistent file locks and lease store coordinate same-workspace
        // instances without retaining a process-global mutable registry.
        let scope_locks = Arc::new(ScopeLockManager::persistent(
            resource_state_root.join("scope-locks"),
        )?);
        let worktree_leases = Arc::new(WorktreeLeaseManager::open(
            resource_state_root.join("worktree-leases.json"),
        )?);
        let services = Arc::new(RuntimeServices::assemble(
            self.cowd_home.clone(),
            workspace_root,
            workspace_key,
            event_store,
            worktree_leases,
            scope_locks,
            self.resource_quotas,
            self.provider_registry,
            self.provider_fallbacks,
            self.tool_execution_host,
            artifact_store,
            self.memory_manager,
            self.reality_recall_port,
            self.knowledge_activation,
            self.evolution_eval_runner,
            self.skill_catalog,
            self.mission_schedule_policy,
            definition_registry,
            task_aggregate_service,
            None,
        )?);
        services
            .task_runtime_port()
            .recover()
            .map_err(RuntimeServicesError::Task)?;
        services.agent_runtime.bind_services(Arc::clone(&services));
        services
            .agent_runtime
            .register_backend(Arc::new(InProcessAgentWorker::new(Arc::downgrade(
                &services,
            ))));
        services
            .agent_runtime
            .register_backend(Arc::new(ProcessJsonlAdapter::for_workspace(
                services.workspace_root(),
            )));
        services
            .agent_runtime
            .block_unrecoverable_replayed_runs()
            .map_err(RuntimeServicesError::AgentRuntime)?;
        services.materialize_evolution_release_assignments()?;
        services.knowledge_candidate_projector.start();
        services.outcome_projector.start();
        services.evolution_signal_projector.start();
        services
            .team_runtime()
            .import_legacy_state_file(&legacy_team_state_path)
            .map_err(RuntimeServicesError::Mission)?;
        services
            .team_runtime()
            .archive_legacy_profile_file(
                &legacy_team_profile_path,
                &legacy_team_profile_archive_root,
            )
            .map_err(RuntimeServicesError::Mission)?;
        if let Some((query, ingress, journal)) = session_ports {
            services.install_session_ports(query, ingress, journal)?;
        }
        Ok(services)
    }
}

fn normalize_provider_fallbacks(fallbacks: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut normalized = Vec::new();
    for fallback in fallbacks {
        let fallback = fallback.trim().to_string();
        if !fallback.is_empty() && !normalized.contains(&fallback) {
            normalized.push(fallback);
        }
    }
    normalized
}

pub struct RuntimeServices {
    workspace_root: PathBuf,
    workspace_key: String,
    event_store: Arc<RuntimeEventStore>,
    live_execution_store: Arc<crate::execution_live::ExecutionLiveStore>,
    executor_registry: Arc<NodeExecutorRegistry>,
    model_step_executor: Arc<ScopedNodeExecutor>,
    tool_batch_executor: Arc<ScopedNodeExecutor>,
    cross_plane_connector_executor: Arc<ScopedNodeExecutor>,
    agent_task_executor: Arc<AgentTaskExecutor>,
    agent_runtime: Arc<AgentRuntime>,
    team_runtime: Arc<TeamRuntime>,
    l4_promotion_service: Arc<crate::L4PromotionService>,
    knowledge_candidate_projector: Arc<crate::KnowledgeCandidateProjector>,
    outcome_service: Arc<crate::execution_core::OutcomeService>,
    outcome_projector: Arc<crate::OutcomeProjector>,
    verify_executor: Arc<VerifyNodeExecutor>,
    synthesize_executor: Arc<SynthesizeNodeExecutor>,
    graph_state_store: ExecutionGraphStateStore,
    commit_service: ExecutionCommitService,
    execution_supervisor: Arc<crate::RuntimeExecutionSupervisor>,
    approval_queue: Arc<ApprovalQueue>,
    evolution_governance: Arc<crate::EvolutionGovernanceService>,
    evolution_discovery: Arc<crate::evolution::EvolutionDiscoveryService>,
    evolution_signal_projector: Arc<crate::evolution::EvolutionSignalProjector>,
    mission_evidence: Arc<MissionEvidenceBus>,
    conflict_resolver: Arc<ConflictArbiter>,
    resource_manager: Arc<ExecutionResourceManager>,
    tool_execution_plane: Arc<crate::ToolExecutionPlane>,
    scope_locks: Arc<ScopeLockManager>,
    worktree_leases: Arc<WorktreeLeaseManager>,
    definition_registry: Arc<RuntimeDefinitionRegistry>,
    cross_plane: Arc<CrossPlaneRuntimeService>,
    mission_runtime: Arc<MissionRuntime>,
    task_aggregate_service: Arc<crate::TaskAggregateService>,
    mission_schedules: Arc<MissionScheduleStore>,
    managed_agents: Arc<crate::ManagedAgentDispatcher>,
    mission_schedule_policy: Arc<RwLock<crate::MissionSchedulePolicy>>,
    session_relations: Arc<SessionRelationGraph>,
    goal_store: Arc<GoalStore>,
    provider_registry: Arc<crate::ProviderRegistry>,
    provider_fallbacks: Arc<RwLock<Vec<String>>>,
    provider_transport_pool: Arc<crate::ProviderTransportPool>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    artifact_store: Arc<crate::ArtifactStore>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
    skill_catalog: Arc<RwLock<crate::RuntimeSkillCatalog>>,
    reality_recall_port: Arc<RealityRecallPort>,
    knowledge_activation: crate::knowledge_activation::KnowledgeActivationRuntime,
    session_dispatch_executor: Arc<crate::session_execution::SessionDispatchNodeExecutor>,
    session_input_router: OnceLock<Arc<SessionInputRouter>>,
    session_query_port: OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>,
    session_ingress_port: OnceLock<Arc<dyn crate::SessionRuntimeIngressPort>>,
    session_journal_port: OnceLock<Arc<dyn crate::SessionRuntimeJournalPort>>,
    active_execution_buses: Arc<Mutex<BTreeMap<String, ActiveExecutionBus>>>,
    next_execution_bus_generation: AtomicU64,
    maintenance_supervisor: Arc<RuntimeMaintenanceSupervisor>,
    // Keep this field last so filesystem-backed components are dropped before
    // the temporary root removes their files.
    _ephemeral_root: Option<tempfile::TempDir>,
}

#[derive(Clone)]
struct ActiveExecutionBus {
    generation: u64,
    bus: crate::CowdEventBus,
}

/// Removes one request-local root event bus without allowing an older turn to
/// tear down a newer binding for the same deterministic execution identity.
pub(crate) struct ActiveExecutionBusLease {
    execution_id: String,
    generation: u64,
    buses: Arc<Mutex<BTreeMap<String, ActiveExecutionBus>>>,
}

impl Drop for ActiveExecutionBusLease {
    fn drop(&mut self) {
        let mut buses = self
            .buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buses
            .get(&self.execution_id)
            .is_some_and(|active| active.generation == self.generation)
        {
            buses.remove(&self.execution_id);
        }
    }
}

impl RuntimeServices {
    #[must_use]
    pub fn builder(
        cowd_home: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> RuntimeServicesBuilder {
        RuntimeServicesBuilder {
            cowd_home: cowd_home.into(),
            workspace_root: workspace_root.into(),
            runtime_event_store: None,
            task_aggregate_service: None,
            builtin_definitions_root: None,
            resource_quotas: default_resource_quotas(),
            provider_registry: Arc::new(crate::ProviderRegistry::empty()),
            provider_fallbacks: Vec::new(),
            tool_execution_host: None,
            session_query_port: None,
            session_ingress_port: None,
            session_journal_port: None,
            artifact_store: None,
            memory_manager: None,
            reality_recall_port: None,
            knowledge_activation: None,
            evolution_eval_runner: None,
            skill_catalog: crate::RuntimeSkillCatalog::default(),
            mission_schedule_policy: crate::MissionSchedulePolicy::default(),
        }
    }

    pub fn in_memory() -> Result<Arc<Self>, RuntimeServicesError> {
        let workspace_key = format!("in-memory-{}", uuid::Uuid::new_v4());
        let workspace_root = PathBuf::from(format!("/{workspace_key}"));
        let ephemeral_root = tempfile::Builder::new()
            .prefix("cowd-runtime-services-")
            .tempdir()?;
        let definition_root = ephemeral_root.path().join("definitions");
        let config_home = definition_root.join("config-home");
        let storage_registry = storage::StorageRegistry::default_for_config_home(&config_home)
            .with_workspace(&workspace_root)?;
        let definition_registry = Arc::new(RuntimeDefinitionRegistry::from_storage_registry(
            &storage_registry,
            definition_root.join("builtin"),
            &workspace_root,
        )?);
        let task_scope = storage::StorageScope::workspace_for_root(&workspace_root);
        let task_handle = storage_registry
            .endpoint_in_scope(&storage::StorageDomainId::Tasks, &task_scope)?
            .as_handle();
        let task_aggregate_service = Arc::new(
            crate::TaskAggregateService::open_storage_handle(&task_handle)
                .map_err(RuntimeServicesError::Task)?,
        );
        let services = Arc::new(Self::assemble(
            config_home,
            workspace_root,
            workspace_key.clone(),
            Arc::new(RuntimeEventStore::try_open_in_memory()?),
            Arc::new(WorktreeLeaseManager::open(
                ephemeral_root.path().join("worktree-leases.json"),
            )?),
            Arc::new(ScopeLockManager::new()),
            default_resource_quotas(),
            Arc::new(crate::ProviderRegistry::empty()),
            Vec::new(),
            None,
            Arc::new(crate::ArtifactStore::sqlite_default(
                storage_registry
                    .endpoint(&storage::StorageDomainId::Blobs)?
                    .path
                    .clone(),
            )),
            None,
            None,
            None,
            None,
            crate::RuntimeSkillCatalog::default(),
            crate::MissionSchedulePolicy::default(),
            definition_registry,
            task_aggregate_service,
            Some(ephemeral_root),
        )?);
        services.agent_runtime.bind_services(Arc::clone(&services));
        services
            .agent_runtime
            .register_backend(Arc::new(InProcessAgentWorker::new(Arc::downgrade(
                &services,
            ))));
        services
            .agent_runtime
            .register_backend(Arc::new(ProcessJsonlAdapter::new()));
        services
            .agent_runtime
            .block_unrecoverable_replayed_runs()
            .map_err(RuntimeServicesError::AgentRuntime)?;
        services.materialize_evolution_release_assignments()?;
        services.knowledge_candidate_projector.start();
        services.outcome_projector.start();
        services.evolution_signal_projector.start();
        Ok(services)
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        cowd_home: PathBuf,
        workspace_root: PathBuf,
        workspace_key: String,
        event_store: Arc<RuntimeEventStore>,
        worktree_leases: Arc<WorktreeLeaseManager>,
        scope_locks: Arc<ScopeLockManager>,
        resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
        provider_registry: Arc<crate::ProviderRegistry>,
        provider_fallbacks: Vec<String>,
        tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
        artifact_store: Arc<crate::ArtifactStore>,
        memory_manager: Option<Arc<memory::CognitiveContextManager>>,
        reality_recall_port: Option<Arc<RealityRecallPort>>,
        knowledge_activation: Option<crate::knowledge_activation::KnowledgeActivationRuntime>,
        evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
        skill_catalog: crate::RuntimeSkillCatalog,
        mission_schedule_policy: crate::MissionSchedulePolicy,
        definition_registry: Arc<RuntimeDefinitionRegistry>,
        task_aggregate_service: Arc<crate::TaskAggregateService>,
        ephemeral_root: Option<tempfile::TempDir>,
    ) -> Result<Self, RuntimeServicesError> {
        let executor_registry = Arc::new(NodeExecutorRegistry::new());
        let provider_transport_pool = Arc::new(crate::ProviderTransportPool::default());
        let graph_state_store = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let model_step_executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
        let tool_batch_executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
        let cross_plane_connector_executor =
            Arc::new(ScopedNodeExecutor::new("cross_plane_connector"));
        let agent_task_executor =
            Arc::new(AgentTaskExecutor::new().with_state_store(graph_state_store.clone()));
        let agent_runtime = Arc::new(AgentRuntime::new(
            Arc::clone(&event_store),
            Arc::clone(&provider_registry),
        ));
        agent_runtime
            .catalog()
            .replace_all(definition_registry.runnable_agent_catalog()?);
        agent_task_executor.install_resolver(Arc::new(AgentRuntimeResolver::new(Arc::clone(
            &agent_runtime,
        ))));
        let verify_executor = Arc::new(VerifyNodeExecutor::new(graph_state_store.clone()));
        let synthesize_executor = Arc::new(SynthesizeNodeExecutor::new());
        synthesize_executor.install_resolver(Arc::new(TeamResultReducer::new(
            graph_state_store.clone(),
            Arc::clone(&agent_runtime),
        )));
        synthesize_executor.install_resolver(Arc::new(ProtocolResultReducer::new(
            graph_state_store.clone(),
            Arc::clone(&agent_runtime),
        )));
        let session_dispatch_executor =
            Arc::new(crate::session_execution::SessionDispatchNodeExecutor::new());
        let approval_queue = Arc::new(ApprovalQueue::new(Arc::clone(&event_store)));
        let evolution_governance = Arc::new(crate::EvolutionGovernanceService::new(
            Arc::clone(&event_store),
            Arc::clone(&approval_queue),
        ));
        let evolution_discovery = Arc::new(crate::evolution::EvolutionDiscoveryService::new(
            Arc::clone(&event_store),
        ));
        let evolution_signal_projector = Arc::new(crate::evolution::EvolutionSignalProjector::new(
            Arc::clone(&event_store),
            Arc::clone(&evolution_discovery),
        ));
        install_builtin_executors(
            &executor_registry,
            vec![
                Arc::clone(&model_step_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&tool_batch_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&cross_plane_connector_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&agent_task_executor) as Arc<dyn NodeExecutor>,
                Arc::new(CompileTargetGuardExecutor) as Arc<dyn NodeExecutor>,
                Arc::new(ApprovalNodeExecutor::new(Arc::clone(&approval_queue))),
                Arc::clone(&verify_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&synthesize_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&session_dispatch_executor) as Arc<dyn NodeExecutor>,
            ],
        )?;
        let commit_service = ExecutionCommitService::new(Arc::clone(&event_store));
        let resource_manager = Arc::new(ExecutionResourceManager::new(resource_quotas));
        let resource_event_store = Arc::clone(&event_store);
        resource_manager
            .install_admission_observer(move |observation| {
                let mut refs = vec![RuntimeEventRef {
                    kind: "resource_request".to_string(),
                    id: observation.request_id.to_string(),
                }];
                if let Some(execution_id) = observation.fairness_key.strip_prefix("graph:") {
                    refs.push(RuntimeEventRef {
                        kind: "execution_graph".to_string(),
                        id: execution_id.to_string(),
                    });
                } else if let Some(session_id) = observation.fairness_key.strip_prefix("session:") {
                    refs.push(RuntimeEventRef {
                        kind: "session".to_string(),
                        id: session_id.to_string(),
                    });
                }
                let state = match observation.status {
                    ResourceAdmissionObservationStatus::Queued => "queued",
                    ResourceAdmissionObservationStatus::Waiting => "waiting",
                    ResourceAdmissionObservationStatus::Granted => "granted",
                    ResourceAdmissionObservationStatus::Deferred => "deferred",
                    ResourceAdmissionObservationStatus::Overloaded => "overloaded",
                };
                if let Err(error) = resource_event_store.append(RuntimeEventInput {
                    stream_id: format!("resource-admission:{}", observation.request_id),
                    scope: RuntimeEventScope::Schedule,
                    kind: format!("resource.admission.{state}"),
                    status: Some(state.to_string()),
                    actor: Some("execution_resource_manager".to_string()),
                    refs,
                    payload: serde_json::to_value(observation).unwrap_or_else(
                        |error| serde_json::json!({ "serialization_error": error.to_string() }),
                    ),
                }) {
                    tracing::warn!(
                        error = %error,
                        request_id = %observation.request_id,
                        state,
                        "resource admission transition evidence could not be persisted"
                    );
                }
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let tool_execution_plane = Arc::new(crate::ToolExecutionPlane::new(
            Arc::clone(&resource_manager),
            Arc::clone(&scope_locks),
        ));
        let graph_runner = Arc::new(ExecutionGraphRunner::new(
            Arc::clone(&executor_registry),
            graph_state_store.clone(),
            commit_service.clone(),
            Arc::clone(&resource_manager),
            Arc::clone(&scope_locks),
            Arc::clone(&worktree_leases),
            workspace_key.clone(),
            workspace_root.clone(),
        ));
        let execution_supervisor = Arc::new(crate::RuntimeExecutionSupervisor::new(graph_runner));
        tool_execution_plane.bind_supervisor(&execution_supervisor);
        let mission_runtime = Arc::new(
            MissionRuntime::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        let task_runtime_port = crate::TaskRuntimePort::from_components(
            Arc::clone(&task_aggregate_service),
            Arc::clone(&mission_runtime),
            Arc::clone(&event_store),
            graph_state_store.clone(),
            commit_service.clone(),
        );
        let team_runtime = Arc::new(TeamRuntime::new(
            Arc::clone(&execution_supervisor),
            graph_state_store.clone(),
            Arc::clone(&agent_runtime),
            Arc::clone(&event_store),
            Arc::clone(&definition_registry),
            Arc::clone(&evolution_governance),
            workspace_key.clone(),
            task_runtime_port,
            Arc::clone(&mission_runtime),
        ));
        let l4_promotion_service = Arc::new(crate::L4PromotionService::new(
            Arc::clone(&event_store),
            Arc::clone(&approval_queue),
            memory_manager.clone(),
        ));
        let knowledge_candidate_projector = Arc::new(crate::KnowledgeCandidateProjector::new(
            Arc::clone(&event_store),
            Arc::clone(&l4_promotion_service),
        ));
        let outcome_service = Arc::new(crate::execution_core::OutcomeService::new(Arc::clone(
            &event_store,
        )));
        let outcome_projector = Arc::new(crate::OutcomeProjector::new(Arc::clone(&event_store)));
        let gate_store = Arc::clone(&event_store);
        let gate_workspace = workspace_key.clone();
        let gate_root = workspace_root.clone();
        execution_supervisor.install_mutation_gate(move || {
            let importer = crate::upgrade::LegacyExecutionImporter::new(
                Arc::clone(&gate_store),
                &gate_workspace,
                &gate_root,
                "",
            );
            importer
                .mutation_allowed()
                .map_err(|error| error.to_string())?
                .then_some(())
                .ok_or_else(|| "upgrade_recovery_required".to_string())
        });
        let mission_evidence = Arc::new(MissionEvidenceBus::new(Arc::clone(&event_store)));
        let goal_store = Arc::new(GoalStore::new(Arc::clone(&event_store)));
        let conflict_resolver = Arc::new(ConflictArbiter::new(
            Arc::clone(&mission_evidence),
            Arc::clone(&event_store),
        ));
        let mission_schedules = Arc::new(
            MissionScheduleStore::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        let managed_agents = Arc::new(
            crate::ManagedAgentDispatcher::event_sourced(
                Arc::clone(&event_store),
                workspace_key.clone(),
            )
            .map_err(RuntimeServicesError::Mission)?,
        );
        let managed_projection_store = graph_state_store.clone();
        let managed_projection_dispatcher = Arc::clone(&managed_agents);
        let outcome_projection_store = graph_state_store.clone();
        let settled_outcome_service = Arc::clone(&outcome_service);
        let settled_outcome_projector = Arc::clone(&outcome_projector);
        execution_supervisor
            .install_graph_settled_observer(move |graph_id| {
                let graph_id = graph_id.to_string();
                let graph_store = managed_projection_store.clone();
                let dispatcher = Arc::clone(&managed_projection_dispatcher);
                let outcome_store = outcome_projection_store.clone();
                let outcome_service = Arc::clone(&settled_outcome_service);
                let outcome_projector = Arc::clone(&settled_outcome_projector);
                tokio::spawn(async move {
                    if let Err(error) =
                        project_managed_invocation_terminal(graph_store, dispatcher, &graph_id)
                            .await
                    {
                        tracing::warn!(
                            graph_id,
                            error,
                            "managed Agent terminal projector could not reduce graph state"
                        );
                    }
                    if let Err(error) = project_team_terminal_outcome(
                        outcome_store,
                        outcome_service,
                        outcome_projector,
                        &graph_id,
                    )
                    .await
                    {
                        tracing::warn!(
                            graph_id,
                            error,
                            "Team terminal Outcome projector could not reduce graph state"
                        );
                    }
                });
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let session_relations = Arc::new(
            SessionRelationGraph::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        Ok(Self {
            workspace_root,
            workspace_key: workspace_key.clone(),
            event_store: Arc::clone(&event_store),
            live_execution_store: Arc::new(crate::execution_live::ExecutionLiveStore::new(
                Arc::clone(&event_store),
            )),
            executor_registry,
            model_step_executor,
            tool_batch_executor,
            cross_plane_connector_executor,
            agent_task_executor,
            agent_runtime,
            team_runtime,
            l4_promotion_service,
            knowledge_candidate_projector,
            outcome_service,
            outcome_projector,
            verify_executor,
            synthesize_executor,
            graph_state_store,
            commit_service,
            execution_supervisor,
            approval_queue,
            evolution_governance,
            evolution_discovery,
            evolution_signal_projector,
            mission_evidence,
            conflict_resolver,
            resource_manager,
            tool_execution_plane,
            scope_locks,
            worktree_leases,
            definition_registry,
            cross_plane: Arc::new(CrossPlaneRuntimeService::open(Arc::clone(&event_store))?),
            mission_runtime,
            task_aggregate_service,
            mission_schedules,
            managed_agents,
            mission_schedule_policy: Arc::new(RwLock::new(mission_schedule_policy)),
            session_relations,
            goal_store,
            provider_registry,
            provider_fallbacks: Arc::new(RwLock::new(normalize_provider_fallbacks(
                provider_fallbacks,
            ))),
            provider_transport_pool,
            tool_execution_host,
            artifact_store,
            memory_manager,
            evolution_eval_runner,
            skill_catalog: Arc::new(RwLock::new(skill_catalog)),
            reality_recall_port: reality_recall_port
                .unwrap_or_else(|| Arc::new(RealityRecallPort::for_config_home(&cowd_home))),
            knowledge_activation: knowledge_activation.unwrap_or_else(|| {
                crate::knowledge_activation::KnowledgeActivationRuntime::for_config_home(&cowd_home)
                    .unwrap_or_else(|_| {
                        crate::knowledge_activation::KnowledgeActivationRuntime::new()
                    })
            }),
            session_dispatch_executor,
            session_input_router: OnceLock::new(),
            session_query_port: OnceLock::new(),
            session_ingress_port: OnceLock::new(),
            session_journal_port: OnceLock::new(),
            active_execution_buses: Arc::new(Mutex::new(BTreeMap::new())),
            next_execution_bus_generation: AtomicU64::new(0),
            maintenance_supervisor: Arc::new(RuntimeMaintenanceSupervisor::new()),
            _ephemeral_root: ephemeral_root,
        })
    }

    pub fn install_session_ports(
        self: &Arc<Self>,
        query: Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress: Arc<dyn crate::SessionRuntimeIngressPort>,
        journal: Arc<dyn crate::SessionRuntimeJournalPort>,
    ) -> Result<Arc<SessionInputRouter>, RuntimeServicesError> {
        if let Some(router) = self.session_input_router.get() {
            return Ok(Arc::clone(router));
        }
        let router = SessionInputRouter::install(
            Arc::clone(&query),
            Arc::clone(&ingress),
            &self.workspace_key,
            Arc::clone(&self.event_store),
        )?;
        self.session_dispatch_executor
            .install_router(Arc::clone(&router))
            .map_err(RuntimeServicesError::Mission)?;
        self.session_query_port
            .set(query)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_ingress_port
            .set(ingress)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_journal_port
            .set(journal)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_input_router
            .set(Arc::clone(&router))
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        Ok(router)
    }

    #[cfg(test)]
    pub(crate) fn install_test_session_store(
        self: &Arc<Self>,
        store: Arc<session::UnifiedSessionStore>,
    ) -> Result<Arc<SessionInputRouter>, RuntimeServicesError> {
        if let Some(router) = self.session_input_router.get() {
            return Ok(Arc::clone(router));
        }
        let port = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
        let query: Arc<dyn crate::SessionRuntimeQueryPort> = port.clone();
        let ingress: Arc<dyn crate::SessionRuntimeIngressPort> = port.clone();
        let journal: Arc<dyn crate::SessionRuntimeJournalPort> = port;
        let router = SessionInputRouter::install_for_test(
            Arc::clone(&query),
            Arc::clone(&ingress),
            store,
            &self.workspace_key,
            Arc::clone(&self.event_store),
        )?;
        self.session_dispatch_executor
            .install_router(Arc::clone(&router))
            .map_err(RuntimeServicesError::Mission)?;
        self.session_query_port
            .set(query)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_ingress_port
            .set(ingress)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_journal_port
            .set(journal)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_input_router
            .set(Arc::clone(&router))
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        Ok(router)
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }
    pub fn workspace_key(&self) -> &str {
        &self.workspace_key
    }

    /// Runtime-owned access to the shared cognitive manager. Callers can use
    /// it only through Runtime host construction; Gateway does not receive a
    /// prompt-assembly capability from this accessor.
    #[must_use]
    pub fn memory_manager(&self) -> Option<Arc<memory::CognitiveContextManager>> {
        self.memory_manager.clone()
    }

    pub(crate) fn maintenance_supervisor(&self) -> Arc<RuntimeMaintenanceSupervisor> {
        Arc::clone(&self.maintenance_supervisor)
    }

    /// Stop accepting detached maintenance and await every retained task.
    pub async fn shutdown_maintenance(&self) {
        self.evolution_signal_projector.shutdown().await;
        self.outcome_projector.shutdown().await;
        self.knowledge_candidate_projector.shutdown().await;
        self.maintenance_supervisor.shutdown_and_drain().await;
    }

    /// Stop the one Runtime execution owner and return auditable per-owner
    /// queue/abort/error evidence to the process host.
    pub async fn shutdown_execution(&self) -> crate::RuntimeExecutionShutdownReport {
        self.execution_supervisor.shutdown().await
    }

    #[must_use]
    pub fn execution_health(&self) -> crate::RuntimeExecutionHealth {
        self.execution_supervisor.health()
    }

    /// Return a stable copy for primary or delegated turn construction.
    /// Replacing the catalog later cannot alter an already-created turn.
    #[must_use]
    pub fn skill_catalog(&self) -> crate::RuntimeSkillCatalog {
        self.skill_catalog
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Gateway's composition boundary may refresh an inspected package
    /// snapshot. Runtime retains sole ownership of activation and injection.
    pub fn replace_skill_catalog(&self, catalog: crate::RuntimeSkillCatalog) {
        *self
            .skill_catalog
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = catalog;
    }

    #[must_use]
    pub fn l4_promotion_service(&self) -> &Arc<crate::L4PromotionService> {
        &self.l4_promotion_service
    }

    #[must_use]
    pub fn knowledge_candidate_projector(&self) -> &Arc<crate::KnowledgeCandidateProjector> {
        &self.knowledge_candidate_projector
    }

    #[must_use]
    pub fn outcome_service(&self) -> &Arc<crate::execution_core::OutcomeService> {
        &self.outcome_service
    }

    #[must_use]
    pub fn outcome_projector(&self) -> &Arc<crate::OutcomeProjector> {
        &self.outcome_projector
    }

    pub fn import_legacy_strategy_outcomes(
        &self,
        path: &Path,
    ) -> Result<crate::execution_core::LegacyOutcomeImportReceipt, RuntimeServicesError> {
        self.outcome_service
            .import_legacy_strategy_file(path)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn import_calibration_outcomes(
        &self,
        path: &Path,
    ) -> Result<crate::execution_core::CalibrationOutcomeImportReceipt, RuntimeServicesError> {
        let receipt = self
            .outcome_service
            .import_calibration_file(path)
            .map_err(RuntimeServicesError::Invariant)?;
        self.outcome_projector
            .project_available(128)
            .map_err(RuntimeServicesError::Invariant)?;
        Ok(receipt)
    }

    /// Runtime-owned read port for Fact and Matrix context. Each call requires
    /// a Binding and verifies its data lease before exposing model context.
    #[must_use]
    pub fn reality_recall_port(&self) -> &Arc<RealityRecallPort> {
        &self.reality_recall_port
    }

    #[must_use]
    pub fn knowledge_activation(&self) -> crate::knowledge_activation::KnowledgeActivationRuntime {
        self.knowledge_activation.clone()
    }

    /// Runtime-owned, scope-qualified Agent and Team Definition registry.
    /// Gateway and surfaces consume this projection; they do not scan
    /// arbitrary workspace directories to construct runnable identities.
    #[must_use]
    pub fn definition_registry(&self) -> &Arc<RuntimeDefinitionRegistry> {
        &self.definition_registry
    }

    /// Rebuild the executable Agent index after a Definition release state
    /// changes. The operation replaces, rather than merges, cached entries so
    /// stopped or revoked Definitions cannot remain selectable.
    pub fn refresh_definition_catalog(&self) -> Result<(), RuntimeServicesError> {
        self.agent_runtime
            .catalog()
            .replace_all(self.definition_registry.runnable_agent_catalog()?);
        Ok(())
    }

    /// Import one caller-selected external Agent TOML document as a local
    /// Draft through the Runtime ownership boundary.
    ///
    /// This command deliberately does not release the Definition, update a
    /// default pointer, or refresh the runnable catalog. Those are separate
    /// human-authorized Runtime commands, so an import can never become an
    /// executable identity merely by reaching the Gateway.
    pub fn import_agent_toml_draft(
        &self,
        import: ExplicitTomlAgentImport,
    ) -> Result<AgentDefinitionDraftReceipt, RuntimeServicesError> {
        self.definition_registry
            .import_agent_toml_draft(import)
            .map_err(RuntimeServicesError::from)
    }

    /// Compile one exact Agent Definition execution Binding through the
    /// Runtime composition root. Gateway, Teams and Surfaces submit only a
    /// restricted request; they never resolve Definition paths or construct
    /// executable snapshots themselves.
    pub fn compile_agent_binding(
        &self,
        request: AgentBindingRequest,
    ) -> Result<CompiledAgentBinding, RuntimeServicesError> {
        let routing_identity = format!(
            "{}|{}|{}|{}",
            request.session_id,
            request.task_id,
            request.instance_id,
            request.team_id.as_deref().unwrap_or("direct")
        );
        let compiler = AgentBindingCompiler::new(Arc::clone(&self.definition_registry));
        let selected_canary = self
            .evolution_governance
            .select_agent_canary_assignment(
                &request.definition_id,
                &request.selector,
                &routing_identity,
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        if let Some(assignment) = selected_canary {
            let crate::EvolutionCandidateSubject::AgentDefinition { revision_ref } =
                &assignment.subject
            else {
                return Err(RuntimeServicesError::Invariant(
                    "agent Canary routing selected a non-Agent Definition subject".to_string(),
                ));
            };
            let resolved = self
                .definition_registry
                .resolve_agent_canary(revision_ref)
                .map_err(RuntimeServicesError::from)?;
            return compiler
                .compile_resolved(
                    request,
                    resolved,
                    Some(AgentReleaseBinding {
                        assignment_id: assignment.assignment_id,
                        generation: assignment.generation,
                        channel: ReleaseChannel::Canary,
                    }),
                )
                .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()));
        }
        compiler
            .compile(request)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))
    }

    /// Turn one non-executable planning intent into the immutable packet that
    /// a graph runner may persist.  This is the sole Runtime-owned boundary
    /// where a catalog choice becomes an instance identity and data lease.
    pub fn compile_agent_task_intent(
        &self,
        intent: AgentTaskIntent,
    ) -> Result<AgentTaskPacket, RuntimeServicesError> {
        let execution_identity = self.prepare_agent_task_intent(&intent)?;
        let selected = intent
            .selected_agent_id
            .as_deref()
            .and_then(|agent_id| self.agent_runtime.catalog().get(agent_id));
        let request = request_for_intent(&intent, selected)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
        let compiled = self.compile_agent_binding(request)?;
        compiled
            .snapshot
            .compile_task_packet(intent, execution_identity)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))
    }

    fn prepare_agent_task_intent(
        &self,
        intent: &AgentTaskIntent,
    ) -> Result<ExecutionIdentity, RuntimeServicesError> {
        let task_port = crate::TaskRuntimePort::new(self);
        match task_port
            .get(&intent.task_id)
            .map_err(RuntimeServicesError::Task)?
        {
            Some(task)
                if task.mission_id == intent.mission_id
                    && task.source_session_id == intent.session_id
                    && task.source_turn_id == intent.source_turn_id => {}
            Some(_) => {
                return Err(RuntimeServicesError::Invariant(format!(
                    "Agent task `{}` conflicts with its canonical Task aggregate lineage",
                    intent.task_id
                )));
            }
            None => {
                let mut spec = harness_contract::task::TaskSpec::new(intent.objective.clone());
                spec.execution_policy.max_failures_before_block = 3;
                task_port
                    .create(harness_contract::task::TaskCreateCommand {
                        task_id: intent.task_id.clone(),
                        mission_id: intent.mission_id.clone(),
                        source_session_id: intent.session_id.clone(),
                        source_turn_id: intent.source_turn_id.clone(),
                        spec,
                        evidence_refs: Vec::new(),
                    })
                    .map_err(RuntimeServicesError::Task)?;
            }
        }
        let graph_identity = ExecutionIdentity::for_task_graph(
            intent.principal_id.clone(),
            self.workspace_key.clone(),
            intent.mission_id.clone(),
            intent.task_id.clone(),
            intent.session_id.clone(),
            intent.source_turn_id.clone(),
            intent.graph_id.clone(),
        )
        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let parent_identity = if let Some(team_id) = &intent.team_id {
            ExecutionIdentity::for_team_node(&graph_identity, team_id, &intent.node_id)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
        } else {
            graph_identity
        };
        let execution_identity =
            ExecutionIdentity::for_agent_node(&parent_identity, &intent.run_id, &intent.node_id)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        Ok(execution_identity)
    }

    /// Validate release provenance immediately before a compiled Agent packet
    /// is admitted. Canary packets are deliberately rechecked because a
    /// human StopCanary decision may occur after planning but before worker
    /// start; Stable packets continue through the normal Definition resolver.
    pub(crate) fn validate_agent_binding_release(
        &self,
        binding: &harness_contract::agent::AgentBindingSnapshot,
    ) -> Result<(), RuntimeServicesError> {
        let Some(release) = &binding.release else {
            return Ok(());
        };
        match release.channel {
            ReleaseChannel::Canary => self
                .evolution_governance
                .validate_agent_canary_binding(
                    &binding.definition_ref,
                    &release.assignment_id,
                    release.generation,
                )
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string())),
            ReleaseChannel::Stable | ReleaseChannel::Shadow => {
                Err(RuntimeServicesError::Invariant(
                    "Runtime only accepts explicit release provenance for active Canary Bindings"
                        .to_string(),
                ))
            }
        }
    }

    /// Compile every Agent intent before graph registration.  A graph that
    /// already contains an executable packet is rejected instead of being
    /// silently re-bound, preventing recovery or a surface from replacing an
    /// immutable Binding after planning.
    pub fn compile_graph_agent_intents(
        &self,
        mut graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, RuntimeServicesError> {
        for node in &mut graph.nodes {
            if node.kind != ExecutionNodeKind::AgentTask {
                continue;
            }
            let intent: AgentTaskIntent = serde_json::from_str(&node.payload_ref).map_err(|_| {
                RuntimeServicesError::AgentRuntime(format!(
                    "AgentTask node `{}` must contain an unbound AgentTaskIntent before registration",
                    node.id
                ))
            })?;
            if intent.node_id != node.id {
                return Err(RuntimeServicesError::AgentRuntime(format!(
                    "AgentTask intent node identity `{}` does not match graph node `{}`",
                    intent.node_id, node.id
                )));
            }
            let packet = self.compile_agent_task_intent(intent)?;
            node.payload_ref = serde_json::to_string(&packet).map_err(|error| {
                RuntimeServicesError::AgentRuntime(format!(
                    "encode Runtime-bound AgentTask node `{}`: {error}",
                    node.id
                ))
            })?;
        }
        Ok(graph)
    }
    pub(crate) fn event_store(&self) -> &Arc<RuntimeEventStore> {
        &self.event_store
    }

    /// Bind the live bus owned by one root Session execution. Nested Agents
    /// resolve this request-local registry through their immutable graph
    /// parent binding; no event transport handles enter serialized contracts.
    pub(crate) fn bind_active_execution_bus(
        &self,
        execution_id: impl Into<String>,
        bus: crate::CowdEventBus,
    ) -> ActiveExecutionBusLease {
        let execution_id = execution_id.into();
        let generation = self
            .next_execution_bus_generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.active_execution_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(execution_id.clone(), ActiveExecutionBus { generation, bus });
        ActiveExecutionBusLease {
            execution_id,
            generation,
            buses: Arc::clone(&self.active_execution_buses),
        }
    }

    #[must_use]
    pub(crate) fn active_execution_bus(&self, execution_id: &str) -> Option<crate::CowdEventBus> {
        self.active_execution_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(execution_id)
            .map(|active| active.bus.clone())
    }

    /// Read-only durable event projection for Gateway and Surface consumers.
    #[must_use]
    pub fn event_reader(&self) -> RuntimeEventReader {
        RuntimeEventReader {
            store: Arc::clone(&self.event_store),
        }
    }

    /// Register the durable SessionIngress identity before a provider-backed
    /// turn starts. Runtime owns every subsequent lifecycle transition.
    pub fn record_live_execution(&self, session_id: &str, execution_id: String, turn_id: String) {
        self.live_execution_store
            .record_queued(session_id, execution_id, turn_id);
    }

    /// Reduce an execution-scoped Runtime event. Events without an execution
    /// context are intentionally ignored instead of being assigned to the
    /// session's most recently active turn.
    pub fn observe_live_execution_event(&self, session_id: &str, event: &crate::CowdEvent) {
        self.live_execution_store.observe_event(session_id, event);
    }

    pub fn complete_live_execution(
        &self,
        execution_id: &str,
        report: &harness_contract::context::ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
    ) {
        self.live_execution_store
            .complete(execution_id, report, write_attempt_paths, terminal_ref);
    }

    /// Re-establish the terminal live projection from an already materialized
    /// Session terminal during durable replay. Detailed metrics remain those
    /// captured by the last live checkpoint; the terminal carrier is the
    /// authority for completion.
    pub fn complete_recovered_live_execution(&self, execution_id: &str, terminal_ref: String) {
        self.live_execution_store
            .complete_recovered(execution_id, terminal_ref);
    }

    pub fn fail_live_execution(&self, execution_id: &str, error: String) {
        self.live_execution_store.fail(execution_id, error);
    }

    pub fn block_live_execution(
        &self,
        execution_id: &str,
        report: &harness_contract::context::ContextTurnReport,
        write_attempt_paths: &[String],
        terminal_ref: String,
        reason: String,
    ) {
        self.live_execution_store.block(
            execution_id,
            report,
            write_attempt_paths,
            terminal_ref,
            reason,
        );
    }

    pub fn cancel_live_execution(&self, execution_id: &str, detail: String) {
        self.live_execution_store.cancel(execution_id, detail);
    }

    #[must_use]
    pub fn execution_live(
        &self,
        execution_id: &str,
    ) -> Option<harness_contract::projection::ExecutionLiveState> {
        self.live_execution_store.execution_live(execution_id)
    }

    #[must_use]
    pub fn session_execution_index(
        &self,
        session_id: &str,
    ) -> harness_contract::projection::SessionExecutionIndexProjection {
        self.live_execution_store
            .session_execution_index(session_id)
    }

    #[must_use]
    pub fn running_session_execution_indices(
        &self,
    ) -> Vec<harness_contract::projection::SessionExecutionIndexProjection> {
        self.live_execution_store
            .running_session_execution_indices()
    }

    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    #[must_use]
    pub fn fixture_event_port(&self) -> RuntimeFixtureEventPort {
        RuntimeFixtureEventPort {
            store: Arc::clone(&self.event_store),
        }
    }

    /// Delivery-only terminal outbox port for the Gateway session bridge.
    #[must_use]
    pub fn session_terminal_delivery(&self) -> SessionTerminalDeliveryPort {
        SessionTerminalDeliveryPort {
            store: Arc::clone(&self.event_store),
        }
    }

    /// Consume an already verified human decision lease exactly once.  The
    /// signed lease is verified before it reaches this method; this Runtime
    /// command persists the replay fence alongside the workspace ledger so a
    /// process restart cannot resurrect a release decision.
    pub fn consume_verified_decision_lease(
        &self,
        lease: crate::VerifiedDecisionLease,
    ) -> Result<(), String> {
        let consumed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        self.event_store
            .consume_verified_decision_lease(
                lease.lease_id(),
                lease.principal_id(),
                lease.review_id(),
                lease.action(),
                lease.scope(),
                lease.evidence_digest(),
                lease.credential_epoch(),
                consumed_at_ms,
            )
            .map_err(|error| error.to_string())
    }

    /// Runtime-owned startup migration command.  Gateway may trigger the
    /// command at startup but never obtains the raw event-store committer.
    pub fn import_legacy_execution_receipt(
        &self,
        receipt_path: &Path,
        version: &str,
    ) -> Result<(), String> {
        crate::upgrade::LegacyExecutionImporter::new(
            Arc::clone(&self.event_store),
            &self.workspace_key,
            &self.workspace_root,
            version,
        )
        .import_receipt_file(receipt_path)
        .map(|_| ())
        .map_err(|error| error.to_string())
    }
    pub fn executor_registry(&self) -> &Arc<NodeExecutorRegistry> {
        &self.executor_registry
    }
    pub fn model_step_executor(&self) -> &Arc<ScopedNodeExecutor> {
        &self.model_step_executor
    }
    pub fn tool_batch_executor(&self) -> &Arc<ScopedNodeExecutor> {
        &self.tool_batch_executor
    }
    pub fn cross_plane_connector_executor(&self) -> &Arc<ScopedNodeExecutor> {
        &self.cross_plane_connector_executor
    }
    pub fn agent_task_executor(&self) -> &Arc<AgentTaskExecutor> {
        &self.agent_task_executor
    }
    pub fn agent_runtime(&self) -> &Arc<AgentRuntime> {
        &self.agent_runtime
    }

    /// Runtime-owned, immutable terminal run evidence. Consumers receive
    /// projections only and cannot amend a Definition's self-model directly.
    #[must_use]
    pub fn agent_run_evaluations(&self) -> Vec<crate::AgentRunEvaluation> {
        self.agent_runtime.evaluations()
    }

    #[must_use]
    pub fn agent_self_models(&self) -> Vec<crate::AgentSelfModel> {
        self.agent_runtime.self_models()
    }

    /// Recompute all active Canary observations from the canonical terminal
    /// Agent evidence. The method is intentionally idempotent and does not
    /// authorize Stable promotion; it only refreshes the evidence consumed by
    /// the separate typed Stable-review gate.
    pub fn refresh_evolution_canary_observations(
        &self,
    ) -> Result<Vec<crate::EvolutionGovernanceCandidate>, RuntimeServicesError> {
        self.evolution_governance
            .refresh_canary_observations_from_agent_runs(&self.agent_runtime.evaluations())
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }
    pub fn team_runtime(&self) -> &Arc<TeamRuntime> {
        &self.team_runtime
    }
    pub fn verify_executor(&self) -> &Arc<VerifyNodeExecutor> {
        &self.verify_executor
    }
    pub fn synthesize_executor(&self) -> &Arc<SynthesizeNodeExecutor> {
        &self.synthesize_executor
    }
    pub fn graph_state_store(&self) -> &ExecutionGraphStateStore {
        &self.graph_state_store
    }
    pub fn commit_service(&self) -> &ExecutionCommitService {
        &self.commit_service
    }
    pub fn execution_supervisor(&self) -> &Arc<crate::RuntimeExecutionSupervisor> {
        &self.execution_supervisor
    }
    pub fn approval_queue(&self) -> &Arc<ApprovalQueue> {
        &self.approval_queue
    }
    /// Runtime is the single owner of evolution candidates, evaluation
    /// eligibility and release-change review projections. Gateway and
    /// surfaces consume this service rather than keeping a second registry.
    pub fn record_evolution_signal(
        &self,
        signal: crate::EvolutionSignal,
    ) -> Result<crate::EvolutionSignal, RuntimeServicesError> {
        self.evolution_discovery
            .record_signal(signal)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_signals(&self) -> Result<Vec<crate::EvolutionSignal>, RuntimeServicesError> {
        self.evolution_discovery
            .list_signals()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn create_evolution_diagnosis(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<crate::EvolutionDiagnosis, RuntimeServicesError> {
        self.evolution_discovery
            .create_diagnosis(signal_ids)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_diagnoses(
        &self,
    ) -> Result<Vec<crate::EvolutionDiagnosis>, RuntimeServicesError> {
        self.evolution_discovery
            .list_diagnoses()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_diagnosis(
        &self,
        diagnosis_id: &str,
    ) -> Result<Option<crate::EvolutionDiagnosis>, RuntimeServicesError> {
        self.evolution_discovery
            .diagnosis(diagnosis_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn create_evolution_lifecycle(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<crate::EvolutionLifecycleDraft, RuntimeServicesError> {
        self.evolution_discovery
            .create_lifecycle(signal_ids)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_missions(&self) -> Result<Vec<crate::EvolutionMission>, RuntimeServicesError> {
        self.evolution_discovery
            .list_missions()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_mission(
        &self,
        mission_id: &str,
    ) -> Result<Option<crate::EvolutionMission>, RuntimeServicesError> {
        self.evolution_discovery
            .mission(mission_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_proposals(
        &self,
    ) -> Result<Vec<crate::EvolutionProposal>, RuntimeServicesError> {
        self.evolution_discovery
            .list_proposals()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_proposal(
        &self,
        proposal_id: &str,
    ) -> Result<Option<crate::EvolutionProposal>, RuntimeServicesError> {
        self.evolution_discovery
            .proposal(proposal_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_proposal_decision_digest(
        &self,
        proposal_id: &str,
        decision: &str,
    ) -> Result<String, RuntimeServicesError> {
        self.evolution_discovery
            .proposal_decision_digest(proposal_id, decision)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn decide_evolution_proposal(
        &self,
        principal: &crate::VerifiedPrincipal,
        lease: &crate::VerifiedDecisionLease,
        proposal_id: &str,
        decision: &str,
    ) -> Result<crate::EvolutionProposal, RuntimeServicesError> {
        self.evolution_discovery
            .decide_proposal(principal, lease, proposal_id, decision)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_projector_health(
        &self,
    ) -> Result<crate::EvolutionProjectorHealth, RuntimeServicesError> {
        self.evolution_signal_projector
            .health()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn outcome_projection_health(
        &self,
    ) -> Result<crate::OutcomeProjectionHealth, RuntimeServicesError> {
        self.outcome_projector
            .health()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_candidate(
        &self,
        candidate_id: &str,
    ) -> Result<crate::EvolutionGovernanceCandidate, RuntimeServicesError> {
        self.evolution_governance
            .candidate(candidate_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn evolution_candidates(
        &self,
    ) -> Result<Vec<crate::EvolutionGovernanceCandidate>, RuntimeServicesError> {
        self.evolution_governance
            .list_candidates()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn evolution_release_reviews(
        &self,
    ) -> Result<Vec<crate::ReleaseChangeReview>, RuntimeServicesError> {
        self.evolution_governance
            .list_reviews()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn evolution_release_review(
        &self,
        review_id: &str,
    ) -> Result<crate::ReleaseChangeReview, RuntimeServicesError> {
        self.evolution_governance
            .review(review_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    /// The active floor is a Runtime event projection. Gateway may display it
    /// but cannot supply a looser policy while registering or releasing a
    /// candidate.
    #[must_use]
    pub fn evolution_evaluation_policy_floor(
        &self,
    ) -> harness_contract::evaluation::EvaluationPolicyFloor {
        self.evolution_governance.evaluation_policy_floor()
    }

    pub fn evolution_evaluation_policy_reviews(
        &self,
    ) -> Result<Vec<crate::EvaluationPolicyChangeReview>, RuntimeServicesError> {
        self.evolution_governance
            .list_evaluation_policy_reviews()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn request_evolution_evaluation_policy_change(
        &self,
        intent: crate::EvaluationPolicyChangeIntent,
    ) -> Result<crate::EvaluationPolicyChangeReview, RuntimeServicesError> {
        self.evolution_governance
            .request_evaluation_policy_change(intent)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn decide_evolution_evaluation_policy_change(
        &self,
        principal: &crate::VerifiedPrincipal,
        lease: &crate::VerifiedDecisionLease,
        review_id: &str,
        decision: crate::ReleaseChangeReviewDecision,
        reason: String,
    ) -> Result<Option<harness_contract::evaluation::EvaluationPolicyFloor>, RuntimeServicesError>
    {
        self.evolution_governance
            .decide_evaluation_policy_change(principal, lease, review_id, decision, reason)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn request_evolution_canary_review(
        &self,
        candidate_id: &str,
    ) -> Result<crate::ReleaseChangeReview, RuntimeServicesError> {
        self.evolution_governance
            .request_canary_review(candidate_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn request_evolution_stable_review(
        &self,
        candidate_id: &str,
    ) -> Result<crate::ReleaseChangeReview, RuntimeServicesError> {
        self.refresh_evolution_canary_observations()?;
        self.evolution_governance
            .request_stable_review(candidate_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    /// Queue a non-candidate release/pointer change behind the same immutable
    /// Runtime review and human-decision boundary used by Canary and Stable.
    /// The referenced revision is validated before a pending review exists,
    /// preventing a surface from creating a pointer request for a missing
    /// Definition or Template.
    pub fn request_evolution_release_change(
        &self,
        request: crate::ReleaseChangeRequest,
    ) -> Result<crate::ReleaseChangeReview, RuntimeServicesError> {
        match &request.subject {
            crate::EvolutionCandidateSubject::AgentDefinition { revision_ref } => {
                self.definition_registry
                    .agents()
                    .read_revision(revision_ref)
                    .map_err(DefinitionRegistryError::Agent)?;
                if let Some(harness_contract::agent::RevisionSelector::ExactApprovedRevision {
                    revision,
                }) = request.selector.as_ref()
                {
                    let target = harness_contract::agent::AgentDefinitionRevisionRef::new(
                        revision_ref.definition_id.clone(),
                        *revision,
                    )
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
                    self.definition_registry
                        .agents()
                        .read_revision(&target)
                        .map_err(DefinitionRegistryError::Agent)?;
                }
            }
            crate::EvolutionCandidateSubject::TeamTemplate { revision_ref } => {
                self.definition_registry
                    .teams()
                    .read_revision(revision_ref)
                    .map_err(DefinitionRegistryError::Team)?;
                if let Some(harness_contract::agent::RevisionSelector::ExactApprovedRevision {
                    revision,
                }) = request.selector.as_ref()
                {
                    let target = harness_contract::team::TeamTemplateRevisionRef::new(
                        revision_ref.template_id.clone(),
                        *revision,
                    )
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
                    self.definition_registry
                        .teams()
                        .read_revision(&target)
                        .map_err(DefinitionRegistryError::Team)?;
                }
            }
        }
        self.evolution_governance
            .request_release_change(request)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    /// Accept an immutable Canary observation from a trusted Runtime-side
    /// evaluator. There is deliberately no Gateway HTTP route for raw
    /// observation payloads: untrusted clients cannot manufacture the
    /// evidence required for Stable promotion.
    pub fn record_evolution_canary_observation(
        &self,
        observation: crate::CanaryObservationReport,
    ) -> Result<crate::EvolutionGovernanceCandidate, RuntimeServicesError> {
        self.evolution_governance
            .record_canary_observation(observation)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    /// Register a Draft evolution candidate only after both the baseline and
    /// proposed Definition revisions are present in the registered Runtime
    /// stores. Gateway never receives direct Definition-store write access.
    pub fn register_evolution_candidate(
        &self,
        intent: crate::EvolutionCandidateIntent,
    ) -> Result<crate::EvolutionGovernanceCandidate, RuntimeServicesError> {
        let proposal = self
            .evolution_discovery
            .proposal(&intent.proposal_id)
            .map_err(RuntimeServicesError::Invariant)?
            .ok_or_else(|| {
                RuntimeServicesError::Invariant("evolution proposal not found".to_string())
            })?;
        if proposal.status != "approved" {
            return Err(RuntimeServicesError::Invariant(
                "evolution proposal must be approved before candidate registration".to_string(),
            ));
        }
        let evaluation_contract = match &intent.subject {
            crate::EvolutionCandidateSubject::AgentDefinition { revision_ref } => {
                if intent.baseline_revision >= revision_ref.revision {
                    return Err(RuntimeServicesError::Invariant(
                        "evolution candidate revision must be newer than its baseline".to_string(),
                    ));
                }
                let candidate = self
                    .definition_registry
                    .agents()
                    .read_revision(revision_ref)
                    .map_err(DefinitionRegistryError::Agent)?;
                let baseline = harness_contract::agent::AgentDefinitionRevisionRef::new(
                    revision_ref.definition_id.clone(),
                    intent.baseline_revision,
                )
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
                let baseline = self
                    .definition_registry
                    .agents()
                    .read_revision(&baseline)
                    .map_err(DefinitionRegistryError::Agent)?;
                if !candidate
                    .revision
                    .manifest
                    .evaluation
                    .is_noninferior_to(&baseline.revision.manifest.evaluation)
                {
                    return Err(RuntimeServicesError::Invariant(
                        "candidate Agent Definition weakens the baseline evaluation contract; submit a separate policy review"
                            .to_string(),
                    ));
                }
                baseline.revision.manifest.evaluation.clone()
            }
            crate::EvolutionCandidateSubject::TeamTemplate { revision_ref } => {
                if intent.baseline_revision >= revision_ref.revision {
                    return Err(RuntimeServicesError::Invariant(
                        "evolution candidate revision must be newer than its baseline".to_string(),
                    ));
                }
                let candidate = self
                    .definition_registry
                    .teams()
                    .read_revision(revision_ref)
                    .map_err(DefinitionRegistryError::Team)?;
                let baseline = harness_contract::team::TeamTemplateRevisionRef::new(
                    revision_ref.template_id.clone(),
                    intent.baseline_revision,
                )
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
                let baseline = self
                    .definition_registry
                    .teams()
                    .read_revision(&baseline)
                    .map_err(DefinitionRegistryError::Team)?;
                ensure_team_evaluation_contract_noninferior(
                    &baseline.revision.manifest,
                    &candidate.revision.manifest,
                )?;
                baseline.revision.manifest.evaluation.clone()
            }
        };
        let proposal_id = intent.proposal_id;
        let candidate = self
            .evolution_governance
            .register_candidate(crate::EvolutionCandidateRegistration {
                candidate_id: intent.candidate_id,
                proposal_id: proposal_id.clone(),
                subject: intent.subject,
                baseline_revision: intent.baseline_revision,
                evaluation_contract,
                source_evidence_refs: intent.source_evidence_refs,
                canary_policy: intent.canary_policy,
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        self.evolution_discovery
            .link_candidate(&proposal_id, &candidate.candidate_id)
            .map_err(RuntimeServicesError::Invariant)?;
        Ok(candidate)
    }

    /// Run a registered candidate through the composition-root evaluator and
    /// record only its immutable Runtime comparison report. An absent runner
    /// is an explicit configuration error, never a permissive fallback or a
    /// Gateway-calculated verdict.
    pub async fn evaluate_evolution_candidate(
        &self,
        candidate_id: &str,
    ) -> Result<crate::EvolutionGovernanceCandidate, RuntimeServicesError> {
        let candidate = self
            .evolution_governance
            .candidate(candidate_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let proposal = self
            .evolution_discovery
            .proposal(&candidate.proposal_id)
            .map_err(RuntimeServicesError::Invariant)?
            .ok_or_else(|| {
                RuntimeServicesError::Invariant(
                    "evolution candidate proposal was not found".to_string(),
                )
            })?;
        if proposal.status != "approved"
            || !proposal
                .candidate_ids
                .iter()
                .any(|linked| linked == candidate_id)
        {
            return Err(RuntimeServicesError::Invariant(
                "evolution candidate must be linked to its approved proposal before evaluation"
                    .to_string(),
            ));
        }
        let Some(runner) = self.evolution_eval_runner.as_ref() else {
            return self
                .evolution_governance
                .record_evaluation_blocked(candidate_id, "evolution_evaluator_not_configured")
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()));
        };
        let report = match runner.evaluate(&candidate).await {
            Ok(report) => report,
            Err(error) => {
                return self
                    .evolution_governance
                    .record_evaluation_blocked(
                        candidate_id,
                        &format!("evolution_evaluator_failed:{error}"),
                    )
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()));
            }
        };
        if report.candidate_id != candidate.candidate_id
            || report.evaluation_contract_digest != candidate.evaluation_contract_digest()
        {
            return Err(RuntimeServicesError::Invariant(
                "evolution_evaluator_report_binding_mismatch".to_string(),
            ));
        }
        self.evolution_governance
            .record_comparison(report)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    /// Execute the correct concrete Runtime path for one immutable paired
    /// evaluation scenario. The evaluator receives only this port; it cannot
    /// choose an Agent shortcut for a Team candidate or obtain release
    /// authority from an execution result.
    pub async fn execute_evolution_scenario(
        &self,
        candidate_id: &str,
        scenario: &EvaluationScenarioSpec,
        sample_index: u32,
    ) -> Result<(EvaluationScenarioObservation, EvaluationScenarioObservation), RuntimeServicesError>
    {
        let candidate = self.evolution_candidate(candidate_id)?;
        match &candidate.subject {
            crate::EvolutionCandidateSubject::AgentDefinition { .. } => {
                self.execute_evolution_agent_scenario(candidate_id, scenario, sample_index)
                    .await
            }
            crate::EvolutionCandidateSubject::TeamTemplate { .. } => {
                self.execute_evolution_team_scenario(candidate_id, scenario, sample_index)
                    .await
            }
        }
    }

    /// Execute one real paired Agent scenario through Runtime. Both packets
    /// use normal AgentRuntime/provider/tool lifecycle; only the candidate
    /// packet carries the narrow evaluation provenance that permits a
    /// published-but-not-released revision to be resolved. This operation
    /// returns observations, never an eligibility or rollout decision.
    pub async fn execute_evolution_agent_scenario(
        &self,
        candidate_id: &str,
        scenario: &EvaluationScenarioSpec,
        sample_index: u32,
    ) -> Result<(EvaluationScenarioObservation, EvaluationScenarioObservation), RuntimeServicesError>
    {
        scenario
            .validate()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        validate_evolution_scenario_isolation(scenario, self.tool_execution_host.as_deref())?;
        let candidate = self.evolution_candidate(candidate_id)?;
        let crate::EvolutionCandidateSubject::AgentDefinition { revision_ref } = &candidate.subject
        else {
            return Err(RuntimeServicesError::Invariant(
                "paired Agent scenario execution requires an Agent Definition candidate"
                    .to_string(),
            ));
        };
        if !candidate
            .evaluation_contract
            .scenario_refs
            .iter()
            .any(|configured| configured == &scenario.scenario_ref)
        {
            return Err(RuntimeServicesError::Invariant(
                "scenario is absent from the candidate's immutable evaluation contract".to_string(),
            ));
        }
        let baseline_ref = harness_contract::agent::AgentDefinitionRevisionRef::new(
            revision_ref.definition_id.clone(),
            candidate.baseline_revision,
        )
        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let baseline = self
            .definition_registry
            .resolve_agent(
                &baseline_ref.definition_id,
                RevisionSelector::ExactApprovedRevision {
                    revision: baseline_ref.revision,
                },
            )
            .map_err(RuntimeServicesError::from)?;
        let proposed = self
            .definition_registry
            .resolve_agent_canary(revision_ref)
            .map_err(RuntimeServicesError::from)?;
        let baseline_packet = self.compile_evolution_scenario_packet(
            &candidate,
            scenario,
            baseline,
            None,
            "baseline",
            sample_index,
        )?;
        let candidate_packet = self.compile_evolution_scenario_packet(
            &candidate,
            scenario,
            proposed,
            Some(AgentEvaluationBinding {
                candidate_id: candidate.candidate_id.clone(),
                scenario_ref: scenario.scenario_ref.clone(),
            }),
            "candidate",
            sample_index,
        )?;
        let started = Instant::now();
        let baseline_return = self
            .agent_runtime
            .execute_task(baseline_packet.clone())
            .await
            .map_err(RuntimeServicesError::AgentRuntime)?;
        let baseline_elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let started = Instant::now();
        let candidate_return = self
            .agent_runtime
            .execute_task(candidate_packet.clone())
            .await
            .map_err(RuntimeServicesError::AgentRuntime)?;
        let candidate_elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        Ok((
            scenario_observation(
                &baseline_packet,
                &baseline_return,
                scenario,
                baseline_elapsed_ms,
            ),
            scenario_observation(
                &candidate_packet,
                &candidate_return,
                scenario,
                candidate_elapsed_ms,
            ),
        ))
    }

    /// Execute baseline and candidate Team Template revisions through the
    /// canonical Team graph compiler. Candidate selection is evaluation-only
    /// and never creates a rollout assignment, while every role still uses
    /// its pinned approved Agent revision and normal graph lifecycle.
    async fn execute_evolution_team_scenario(
        &self,
        candidate_id: &str,
        scenario: &EvaluationScenarioSpec,
        sample_index: u32,
    ) -> Result<(EvaluationScenarioObservation, EvaluationScenarioObservation), RuntimeServicesError>
    {
        scenario
            .validate()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        validate_evolution_scenario_isolation(scenario, self.tool_execution_host.as_deref())?;
        let candidate = self.evolution_candidate(candidate_id)?;
        let crate::EvolutionCandidateSubject::TeamTemplate { revision_ref } = &candidate.subject
        else {
            return Err(RuntimeServicesError::Invariant(
                "paired Team scenario execution requires a Team Template candidate".to_string(),
            ));
        };
        if !candidate
            .evaluation_contract
            .scenario_refs
            .iter()
            .any(|configured| configured == &scenario.scenario_ref)
        {
            return Err(RuntimeServicesError::Invariant(
                "scenario is absent from the candidate's immutable evaluation contract".to_string(),
            ));
        }
        let baseline_ref = TeamTemplateRevisionRef::new(
            revision_ref.template_id.clone(),
            candidate.baseline_revision,
        )
        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let baseline_request = evolution_team_request(
            &candidate,
            scenario,
            &baseline_ref,
            "baseline",
            sample_index,
            self.mission_runtime.default_mission_id(),
        );
        let candidate_request = evolution_team_request(
            &candidate,
            scenario,
            revision_ref,
            "candidate",
            sample_index,
            self.mission_runtime.default_mission_id(),
        );
        let started = Instant::now();
        let baseline = self
            .team_runtime
            .instantiate_evaluation(baseline_request, None, &scenario.allowed_tools)
            .await
            .map_err(RuntimeServicesError::Invariant)?;
        let baseline_elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let started = Instant::now();
        let proposed = self
            .team_runtime
            .instantiate_evaluation(
                candidate_request,
                Some(revision_ref),
                &scenario.allowed_tools,
            )
            .await
            .map_err(RuntimeServicesError::Invariant)?;
        let candidate_elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        Ok((
            team_scenario_observation(
                &baseline,
                &self.agent_runtime.evaluations(),
                scenario,
                baseline_ref.revision,
                baseline_elapsed_ms,
            ),
            team_scenario_observation(
                &proposed,
                &self.agent_runtime.evaluations(),
                scenario,
                revision_ref.revision,
                candidate_elapsed_ms,
            ),
        ))
    }

    fn compile_evolution_scenario_packet(
        &self,
        candidate: &crate::EvolutionGovernanceCandidate,
        scenario: &EvaluationScenarioSpec,
        resolved: crate::agent::definition::ResolvedAgentDefinition,
        evaluation: Option<AgentEvaluationBinding>,
        side: &str,
        sample_index: u32,
    ) -> Result<AgentTaskPacket, RuntimeServicesError> {
        let revision_ref = resolved.revision.revision_ref.clone();
        let run_id = format!(
            "evolution-eval:{}:{}:{}:{}:{}",
            candidate.candidate_id,
            scenario.scenario_ref,
            side,
            revision_ref.revision,
            sample_index
        );
        let task_id = format!("{run_id}:task");
        let session_id = format!("evolution-eval:{}", candidate.candidate_id);
        let mut request = AgentBindingRequest::new(
            revision_ref.definition_id.clone(),
            RevisionSelector::ExactApprovedRevision {
                revision: revision_ref.revision,
            },
            format!("instance:{run_id}"),
            session_id.clone(),
            task_id.clone(),
        );
        request.granted_capabilities = resolved
            .revision
            .manifest
            .capability_contract
            .capability_ceiling
            .clone();
        request.allowed_tool_contract_refs = scenario.allowed_tools.clone();
        request.allowed_skill_refs = scenario.allowed_skills.clone();
        let compiler = AgentBindingCompiler::new(Arc::clone(&self.definition_registry));
        let compiled = match evaluation {
            Some(evaluation) => compiler.compile_evaluation_resolved(request, resolved, evaluation),
            None => compiler.compile_resolved(request, resolved, None),
        }
        .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
        let intent = AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: Some(revision_ref),
            granted_capabilities: Vec::new(),
            principal_id: "runtime.evolution_eval".to_string(),
            source_turn_id: format!("{}:{side}:{sample_index}", scenario.scenario_ref),
            run_id: run_id.clone(),
            task_id: task_id.clone(),
            session_id,
            mission_id: self.mission_runtime.default_mission_id().to_string(),
            team_id: None,
            graph_id: format!("evolution-eval-graph:{}", candidate.candidate_id),
            node_id: format!("{}:{}", scenario.scenario_ref, side),
            attempt: 1,
            expected_graph_revision: 0,
            objective: scenario.objective.clone(),
            acceptance: scenario.acceptance.clone(),
            constraints: vec![
                "evolution_evaluation:isolation_required".to_string(),
                format!("evaluation_scenario:{}", scenario.scenario_ref),
            ],
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: scenario.allowed_tools.clone(),
            allowed_skills: scenario.allowed_skills.clone(),
            permission_lease: scenario.permission_lease.clone(),
            model_lease: scenario.model_lease.clone(),
            budget_lease: ContextBudgetLeaseRef::new(
                format!("evolution-eval-budget:{run_id}"),
                run_id.clone(),
                "evolution_evaluation",
                65_536,
                1,
            ),
            managed_invocation: None,
            idempotency_key: format!("evolution-eval:{}", run_id),
        };
        let execution_identity = self.prepare_agent_task_intent(&intent)?;
        compiled
            .snapshot
            .compile_task_packet(intent, execution_identity)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))
    }

    /// Converge file-backed Definition release projections from the Runtime
    /// authorization ledger. This is deliberately idempotent: a crash after
    /// the authorized event commit can delay availability, but can never make
    /// an unapproved revision runnable.
    pub fn materialize_evolution_release_assignments(&self) -> Result<(), RuntimeServicesError> {
        for assignment in self
            .evolution_governance
            .release_assignments()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
        {
            self.definition_registry
                .materialize_evolution_release(&assignment)?;
        }
        self.refresh_definition_catalog()
    }

    pub fn decide_evolution_release_review(
        &self,
        principal: &crate::VerifiedPrincipal,
        lease: &crate::VerifiedDecisionLease,
        review_id: &str,
        decision: crate::ReleaseChangeReviewDecision,
        reason: String,
    ) -> Result<Option<crate::EvolutionReleaseAssignment>, RuntimeServicesError> {
        let assignment = self
            .evolution_governance
            .decide_review(principal, lease, review_id, decision, reason)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        self.materialize_evolution_release_assignments()?;
        Ok(assignment)
    }
    pub fn mission_evidence(&self) -> &Arc<MissionEvidenceBus> {
        &self.mission_evidence
    }
    pub fn goal_store(&self) -> &Arc<GoalStore> {
        &self.goal_store
    }
    pub fn conflict_resolver(&self) -> &Arc<ConflictArbiter> {
        &self.conflict_resolver
    }
    pub fn resource_manager(&self) -> &Arc<ExecutionResourceManager> {
        &self.resource_manager
    }
    pub fn tool_execution_plane(&self) -> &Arc<crate::ToolExecutionPlane> {
        &self.tool_execution_plane
    }
    pub fn scope_locks(&self) -> &Arc<ScopeLockManager> {
        &self.scope_locks
    }
    pub fn worktree_leases(&self) -> &Arc<WorktreeLeaseManager> {
        &self.worktree_leases
    }
    pub fn ensure_mutation_allowed(&self) -> Result<(), RuntimeServicesError> {
        let importer = crate::upgrade::LegacyExecutionImporter::new(
            Arc::clone(&self.event_store),
            &self.workspace_key,
            &self.workspace_root,
            "",
        );
        if importer
            .mutation_allowed()
            .map_err(|error| RuntimeServicesError::Mission(error.to_string()))?
        {
            Ok(())
        } else {
            Err(RuntimeServicesError::UpgradeRecoveryRequired)
        }
    }

    pub async fn recover_execution_graphs_on_startup(
        &self,
    ) -> Result<ExecutionStartupRecoveryReport, RuntimeServicesError> {
        self.ensure_mutation_allowed()?;
        let mut managed_dispositions = BTreeMap::new();
        for invocation in self
            .managed_agents
            .invocations()
            .map_err(RuntimeServicesError::Mission)?
        {
            if invocation.status != crate::ManagedAgentInvocationStatus::Running {
                continue;
            }
            let disposition = match invocation.execution_ref.as_deref() {
                Some(graph_id) => match self.graph_state_store.load_async(graph_id).await {
                    Ok(graph)
                        if graph.node_statuses.values().all(|status| {
                            matches!(
                                status,
                                ExecutionNodeStatus::Planned | ExecutionNodeStatus::Ready
                            )
                        }) =>
                    {
                        ManagedAgentRestartDisposition::RetrySafe
                    }
                    Ok(graph)
                        if graph
                            .node_statuses
                            .values()
                            .any(|status| *status == ExecutionNodeStatus::Running) =>
                    {
                        ManagedAgentRestartDisposition::ReconciliationRequired(
                            format!(
                                "Runtime restarted while Managed Agent graph `{graph_id}` had a running node; external completion is uncertain"
                            ),
                        )
                    }
                    Ok(_) => ManagedAgentRestartDisposition::PreserveRunning,
                    Err(error) => ManagedAgentRestartDisposition::ReconciliationRequired(
                        format!(
                            "Runtime restarted but Managed Agent graph `{graph_id}` cannot be loaded: {error}"
                        ),
                    ),
                },
                None => ManagedAgentRestartDisposition::ReconciliationRequired(
                    "Runtime restarted with a running Managed Agent invocation that has no execution graph"
                        .to_string(),
                ),
            };
            managed_dispositions.insert(invocation.invocation_id, disposition);
        }
        self.managed_agents
            .recover_with_dispositions(now_ms(), &managed_dispositions)
            .map_err(RuntimeServicesError::Mission)?;
        let managed_invocations = self
            .managed_agents
            .invocations()
            .map_err(RuntimeServicesError::Mission)?
            .into_iter()
            .map(|invocation| (invocation.invocation_id.clone(), invocation))
            .collect::<BTreeMap<_, _>>();
        let resolved_handoff_results = self.resolve_durable_handoff_results().await?;
        let graph_ids = self.graph_state_store.nonterminal_graph_ids_async().await?;
        let mut report = ExecutionStartupRecoveryReport {
            examined_graphs: graph_ids.len(),
            resolved_handoff_results,
            ..ExecutionStartupRecoveryReport::default()
        };
        for graph_id in graph_ids {
            let before = self.graph_state_store.load_async(&graph_id).await?;
            let before_revision = before.revision;
            let before_status = graph_status_label(&before);
            let objective = before.objective.clone();
            let had_running = graph_has_status(&before, ExecutionNodeStatus::Running);
            let mut action = "observed".to_string();
            let mut error = None;
            let managed_fences = managed_invocation_fences(&before);
            let managed_runnable = managed_fences.iter().all(|fence| {
                managed_invocations
                    .get(&fence.invocation_id)
                    .is_some_and(|invocation| {
                        invocation.status == crate::ManagedAgentInvocationStatus::Running
                            && invocation.execution_ref.as_deref() == Some(before.id.as_str())
                            && invocation.fence_generation == fence.fence_generation
                            && invocation.claimed_by.as_deref()
                                == Some(fence.dispatcher_id.as_str())
                    })
            });

            if !managed_fences.is_empty() && !managed_runnable {
                if before.node_statuses.values().all(|status| {
                    matches!(
                        status,
                        ExecutionNodeStatus::Planned | ExecutionNodeStatus::Ready
                    )
                }) {
                    match self
                        .execution_supervisor
                        .command_graph(
                            &graph_id,
                            ExecutionGraphCommand::Cancel {
                                expected_revision: before.revision,
                                reason:
                                    "Managed Agent execution fence is no longer runnable after restart"
                                        .to_string(),
                            },
                        )
                        .await
                    {
                        Ok(_) => action = "cancelled_stale_managed_graph".to_string(),
                        Err(cancel_error) => {
                            let message = cancel_error.to_string();
                            report.errors.push(ExecutionStartupRecoveryError {
                                graph_id: graph_id.clone(),
                                error: message.clone(),
                            });
                            error = Some(message);
                        }
                    }
                } else {
                    action = "managed_reconciliation_required".to_string();
                    report.blocked_graphs += 1;
                }
            } else if had_running {
                match self.execution_supervisor.recover_graph(&graph_id).await {
                    Ok(recovered) => {
                        if recovered.revision != before_revision {
                            report.recovered_graphs += 1;
                            action = "recovered_running".to_string();
                        }
                    }
                    Err(recovery_error) => {
                        let message = recovery_error.to_string();
                        report.errors.push(ExecutionStartupRecoveryError {
                            graph_id: graph_id.clone(),
                            error: message.clone(),
                        });
                        error = Some(message);
                    }
                }
            }

            if error.is_none() && (managed_fences.is_empty() || managed_runnable) {
                let current = self.graph_state_store.load_async(&graph_id).await?;
                if graph_can_advance(&current) {
                    match self.execution_supervisor.notify_graph(&graph_id).await {
                        Ok(()) => {
                            report.notified_graphs += 1;
                            action = if had_running {
                                "recovered_and_notified".to_string()
                            } else {
                                "notified_ready".to_string()
                            };
                        }
                        Err(run_error) => {
                            let message = run_error.to_string();
                            report.errors.push(ExecutionStartupRecoveryError {
                                graph_id: graph_id.clone(),
                                error: message.clone(),
                            });
                            error = Some(message);
                        }
                    }
                }
            }

            let final_graph = self.graph_state_store.load_async(&graph_id).await?;
            if graph_is_terminal(&final_graph) {
                report.terminal_graphs += 1;
            }
            if graph_is_waiting(&final_graph) {
                report.waiting_graphs += 1;
            }
            if graph_has_status(&final_graph, ExecutionNodeStatus::Blocked) {
                report.blocked_graphs += 1;
            }
            report.records.push(ExecutionStartupRecoveryRecord {
                graph_id,
                objective,
                before_revision,
                after_revision: final_graph.revision,
                before_status,
                after_status: graph_status_label(&final_graph),
                action,
                error,
            });
        }

        Ok(report)
    }

    /// Resolve source graph nodes for target results that were durably
    /// committed before an adapter process stopped. Graph ownership stays in
    /// Runtime; Gateway only delivers target turns and never owns recovery.
    pub async fn resolve_durable_handoff_results(&self) -> Result<usize, RuntimeServicesError> {
        let Some(router) = self.session_input_router() else {
            return Ok(0);
        };
        let mut resolved = 0;
        for resolution in router
            .completed_handoff_resolutions()
            .map_err(RuntimeServicesError::SessionHandoffRecovery)?
        {
            if self.resolve_handoff_source(resolution).await? {
                resolved += 1;
            }
        }
        Ok(resolved)
    }

    pub async fn resolve_session_handoff_result(
        &self,
        resolution: crate::SessionHandoffResolution,
    ) -> Result<bool, RuntimeServicesError> {
        self.resolve_handoff_source(resolution).await
    }

    async fn resolve_handoff_source(
        &self,
        resolution: crate::SessionHandoffResolution,
    ) -> Result<bool, RuntimeServicesError> {
        for _ in 0..3 {
            let graph = self
                .graph_state_store
                .load(&resolution.source_graph_id)
                .map_err(|error| RuntimeServicesError::SessionHandoffRecovery(error.to_string()))?;
            let node = graph
                .nodes
                .iter()
                .find(|node| node.id == resolution.source_node_id)
                .ok_or_else(|| {
                    RuntimeServicesError::SessionHandoffRecovery(format!(
                        "handoff source node `{}` is absent from graph `{}`",
                        resolution.source_node_id, resolution.source_graph_id
                    ))
                })?;
            let status = graph
                .node_statuses
                .get(&resolution.source_node_id)
                .copied()
                .ok_or_else(|| {
                    RuntimeServicesError::SessionHandoffRecovery(format!(
                        "handoff source node `{}` has no graph status",
                        resolution.source_node_id
                    ))
                })?;
            if status == ExecutionNodeStatus::Completed {
                return Ok(false);
            }
            if status != ExecutionNodeStatus::WaitingExternal {
                return Err(RuntimeServicesError::SessionHandoffRecovery(format!(
                    "handoff source node `{}` is not waiting for a result ({status:?})",
                    resolution.source_node_id
                )));
            }
            let payload = node
                .payload_ref
                .strip_prefix("session_handoff:")
                .ok_or_else(|| {
                    RuntimeServicesError::SessionHandoffRecovery(format!(
                        "handoff source node `{}` does not carry a SessionHandoff payload",
                        resolution.source_node_id
                    ))
                })?;
            let command: harness_contract::turn::SessionDispatchCommand =
                serde_json::from_str(payload).map_err(|error| {
                    RuntimeServicesError::SessionHandoffRecovery(format!(
                        "invalid durable SessionHandoff source payload: {error}"
                    ))
                })?;
            if command.handoff.correlation_id != resolution.packet.correlation_id {
                return Err(RuntimeServicesError::SessionHandoffRecovery(format!(
                    "handoff result correlation does not match source node `{}`",
                    resolution.source_node_id
                )));
            }
            let result_ref = resolution.packet.result_ref.clone().ok_or_else(|| {
                RuntimeServicesError::SessionHandoffRecovery(
                    "handoff result packet is missing its durable result reference".to_string(),
                )
            })?;
            match self
                .execution_supervisor
                .command_graph(
                    &resolution.source_graph_id,
                    ExecutionGraphCommand::ResolveExternal {
                        expected_revision: graph.revision,
                        node_id: resolution.source_node_id.clone(),
                        result_ref,
                        correlation_id: resolution.packet.correlation_id.clone(),
                    },
                )
                .await
            {
                Ok(_) => return Ok(true),
                Err(error) if error.to_string().contains("revision mismatch") => continue,
                Err(error) => {
                    return Err(RuntimeServicesError::SessionHandoffRecovery(
                        error.to_string(),
                    ));
                }
            }
        }
        Err(RuntimeServicesError::SessionHandoffRecovery(format!(
            "handoff source graph `{}` changed concurrently while resolving `{}`",
            resolution.source_graph_id, resolution.packet.correlation_id
        )))
    }

    pub fn cross_plane(&self) -> &Arc<CrossPlaneRuntimeService> {
        &self.cross_plane
    }

    /// Timer event source for durable Mission schedules. It claims due
    /// occurrences first, then submits one stable SessionDispatch graph per
    /// fire. The source never advances a graph itself and therefore cannot
    /// become a second scheduler or execution owner.
    pub async fn dispatch_due_mission_schedules(
        &self,
        now_ms: u64,
    ) -> Result<crate::MissionScheduleDispatchReport, String> {
        let policy = self.mission_schedule_policy();
        if !policy.enabled {
            return Ok(crate::MissionScheduleDispatchReport {
                kind: "runtime.mission_schedule_dispatch".to_string(),
                tick: crate::MissionScheduleTickReport {
                    kind: "runtime.mission_schedule_tick".to_string(),
                    now_ms,
                    claimed: Vec::new(),
                    missed: Vec::new(),
                },
                submitted: Vec::new(),
                failed: Vec::new(),
            });
        }
        let tick = self.mission_schedules.claim_due(now_ms, policy.grace_ms)?;
        let mut submitted = Vec::new();
        let mut failed = Vec::new();
        for fire in self.mission_schedules.pending_fires() {
            if self.session_input_router().is_none() {
                failed.push(
                    self.mission_schedules.mark_failed(
                        &fire.fire_id,
                        "SessionInputRouter is not installed; schedule cannot submit a graph"
                            .to_string(),
                    )?,
                );
                continue;
            }
            let handoff = harness_contract::turn::SessionHandoff {
                handoff_id: format!("schedule-handoff:{}", fire.fire_id),
                source_session_id: format!("mission:{}", fire.schedule_id),
                target_session_id: fire.target_session_id.clone(),
                objective: fire.objective.clone(),
                acceptance: Vec::new(),
                scope: vec![format!("mission-schedule:{}", fire.schedule_id)],
                context_lens: Vec::new(),
                evidence_refs: vec![harness_contract::turn::opaque_session_evidence_ref(
                    &format!("mission:{}", fire.schedule_id),
                    format!("schedule-fire:{}", fire.fire_id),
                )],
                context_budget_lease: None,
                permission_lease: fire.permission_lease.clone(),
                deadline_at_ms: None,
                priority: fire.priority,
                correlation_id: fire.correlation_id.clone(),
                result_contract: "return evidence-backed scheduled result".to_string(),
            };
            let interpretation =
                crate::MissionCommandInterpreter::interpret_session_handoff_with_graph_id(
                    handoff,
                    format!("mission-schedule-dispatch:{}", fire.fire_id),
                );
            match interpretation.command {
                crate::MissionInterpretedCommand::SubmitExecutionGraph { mut graph, .. } => {
                    graph.service_class =
                        harness_contract::execution_graph::ExecutionServiceClass::Background;
                    let graph_id = graph.id.clone();
                    let graph = match self.compile_graph_agent_intents(graph) {
                        Ok(graph) => graph,
                        Err(error) => {
                            failed.push(self.mission_schedules.mark_failed(
                                &fire.fire_id,
                                format!(
                                    "SessionDispatch Agent Binding compilation failed: {error}"
                                ),
                            )?);
                            continue;
                        }
                    };
                    match self
                        .execution_supervisor
                        .submit_graph(
                            graph,
                            ExecutionGraphCommand::Start {
                                expected_revision: 0,
                            },
                        )
                        .await
                    {
                        Ok(_) => submitted.push(
                            self.mission_schedules
                                .mark_submitted(&fire.fire_id, graph_id)?,
                        ),
                        Err(error) => failed.push(self.mission_schedules.mark_failed(
                            &fire.fire_id,
                            format!("SessionDispatch graph submission failed: {error}"),
                        )?),
                    }
                }
                crate::MissionInterpretedCommand::Blocked { reason } => {
                    failed.push(self.mission_schedules.mark_failed(&fire.fire_id, reason)?);
                }
            }
        }
        Ok(crate::MissionScheduleDispatchReport {
            kind: "runtime.mission_schedule_dispatch".to_string(),
            tick,
            submitted,
            failed,
        })
    }

    pub async fn wake_due_mission_schedules(
        self: &Arc<Self>,
        now_ms: u64,
    ) -> Result<crate::RuntimeWorkAdmissionReceipt, RuntimeServicesError> {
        let services = Arc::clone(self);
        self.execution_supervisor
            .admit_owned("mission_schedule_dispatch", async move {
                services
                    .dispatch_due_mission_schedules(now_ms)
                    .await
                    .map(|_| ())
            })
            .await
            .map_err(RuntimeServicesError::GraphRunner)
    }

    pub fn mission_runtime(&self) -> &Arc<MissionRuntime> {
        &self.mission_runtime
    }

    /// Canonical durable Task aggregate owned by Runtime.
    #[must_use]
    pub fn task_aggregate_service(&self) -> &Arc<crate::TaskAggregateService> {
        &self.task_aggregate_service
    }

    #[must_use]
    pub fn task_runtime_port(&self) -> crate::TaskRuntimePort {
        crate::TaskRuntimePort::new(self)
    }

    pub fn mission_schedules(&self) -> &Arc<MissionScheduleStore> {
        &self.mission_schedules
    }
    /// Runtime-owned Managed Agent registry and dispatcher. Gateway and Edge
    /// can submit trigger intents, but they cannot claim or mutate its
    /// invocation fence directly.
    pub fn managed_agents(&self) -> &Arc<crate::ManagedAgentDispatcher> {
        &self.managed_agents
    }

    pub fn register_managed_agent(
        &self,
        definition: harness_contract::managed_agent::ManagedAgentDefinition,
    ) -> Result<harness_contract::managed_agent::ManagedAgentDefinition, RuntimeServicesError> {
        self.managed_agents
            .register_definition(definition, now_ms())
            .map_err(RuntimeServicesError::Mission)
    }

    pub fn deactivate_managed_agent(
        &self,
        managed_agent_id: &str,
    ) -> Result<harness_contract::managed_agent::ManagedAgentDefinition, RuntimeServicesError> {
        self.managed_agents
            .deactivate_definition(managed_agent_id, now_ms())
            .map_err(RuntimeServicesError::Mission)
    }

    pub fn trigger_managed_agent_manual(
        &self,
        managed_agent_id: &str,
        request_id: &str,
    ) -> Result<crate::ManagedAgentInvocation, RuntimeServicesError> {
        self.managed_agents
            .trigger_manual(managed_agent_id, request_id, now_ms())
            .map_err(RuntimeServicesError::Mission)
    }

    pub fn accept_managed_agent_event(
        &self,
        event: harness_contract::managed_agent::ManagedAgentTriggerEvent,
    ) -> Result<crate::ManagedAgentDispatchReport, RuntimeServicesError> {
        self.managed_agents
            .accept_event(event, now_ms())
            .map_err(RuntimeServicesError::Mission)
    }

    pub fn reset_managed_agent_health(
        &self,
        managed_agent_id: &str,
    ) -> Result<crate::ManagedAgentHealth, RuntimeServicesError> {
        self.managed_agents
            .reset_health(managed_agent_id)
            .map_err(RuntimeServicesError::Mission)
    }

    /// Enter Runtime's durable fenced-effect boundary.  Gateway owns the
    /// adapter invocation, but it cannot execute a Managed Agent side effect
    /// until this Runtime-owned ledger has persisted and claimed the intent.
    pub fn begin_managed_agent_effect(
        &self,
        fence: &harness_contract::managed_agent::ManagedAgentInvocationFence,
        effect_id: &str,
        effect_kind: String,
        idempotency_key: String,
        request_ref: String,
    ) -> Result<crate::ManagedAgentEffectPermit, RuntimeServicesError> {
        fence
            .validate()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let queued = self
            .managed_agents
            .enqueue_effect(
                &fence.invocation_id,
                &fence.dispatcher_id,
                fence.fence_generation,
                effect_id,
                effect_kind,
                idempotency_key,
                request_ref,
                now_ms(),
            )
            .map_err(RuntimeServicesError::Mission)?;
        match queued.status {
            crate::FencedEffectStatus::Pending => self
                .managed_agents
                .claim_effect(
                    &fence.invocation_id,
                    effect_id,
                    fence.fence_generation,
                    &fence.dispatcher_id,
                )
                .map(|record| crate::ManagedAgentEffectPermit::Execute { record })
                .map_err(RuntimeServicesError::Mission),
            crate::FencedEffectStatus::Completed => {
                Ok(crate::ManagedAgentEffectPermit::AlreadyCompleted { record: queued })
            }
            crate::FencedEffectStatus::Claimed
            | crate::FencedEffectStatus::ReconciliationRequired
            | crate::FencedEffectStatus::Cancelled => {
                Err(RuntimeServicesError::Invariant(format!(
                    "managed effect `{effect_id}` is not safe to execute from state {:?}",
                    queued.status
                )))
            }
        }
    }

    pub fn complete_managed_agent_effect(
        &self,
        fence: &harness_contract::managed_agent::ManagedAgentInvocationFence,
        effect_id: &str,
        receipt_ref: String,
    ) -> Result<crate::FencedEffectOutboxRecord, RuntimeServicesError> {
        self.managed_agents
            .complete_effect(
                &fence.invocation_id,
                effect_id,
                fence.fence_generation,
                &fence.dispatcher_id,
                receipt_ref,
            )
            .map_err(RuntimeServicesError::Mission)
    }

    pub fn reconcile_managed_agent_effect(
        &self,
        fence: &harness_contract::managed_agent::ManagedAgentInvocationFence,
        effect_id: &str,
        error: String,
    ) -> Result<crate::FencedEffectOutboxRecord, RuntimeServicesError> {
        self.managed_agents
            .mark_effect_reconciliation_required(
                &fence.invocation_id,
                effect_id,
                fence.fence_generation,
                &fence.dispatcher_id,
                error,
            )
            .map_err(RuntimeServicesError::Mission)
    }

    /// Accept due schedule occurrences, then compile and run each claimed
    /// Managed Agent invocation through the same Agent/Team Runtime paths
    /// used by interactive work. The report contains durable invocation
    /// records, not a second Gateway scheduler state.
    pub async fn dispatch_managed_agents(
        &self,
        dispatcher_id: &str,
        limit: usize,
    ) -> Result<ManagedAgentRuntimeDispatchReport, RuntimeServicesError> {
        let (completed, mut failed) = self.reconcile_managed_agent_invocations().await?;
        let mut health_affected = self
            .managed_agents
            .reclaim_expired_claims(now_ms())
            .map_err(RuntimeServicesError::Mission)?;
        health_affected.extend(
            self.managed_agents
                .enforce_run_health(now_ms())
                .map_err(RuntimeServicesError::Mission)?,
        );
        let scheduled = self
            .managed_agents
            .accept_due_schedules(now_ms())
            .map_err(RuntimeServicesError::Mission)?;
        let available_submission_slots = self
            .execution_supervisor
            .submission_capacity_snapshot()
            .available_slots
            .min(limit);
        let claimed = if available_submission_slots == 0 {
            Vec::new()
        } else {
            self.managed_agents
                .claim_ready(dispatcher_id, now_ms(), 30_000, available_submission_slots)
                .map_err(RuntimeServicesError::Mission)?
        };
        let mut submitted = Vec::new();
        for invocation in &claimed {
            match self
                .submit_managed_agent_invocation(dispatcher_id, invocation.clone())
                .await
            {
                Ok(invocation) => submitted.push(invocation),
                Err(error) => {
                    let current = self
                        .managed_agents
                        .invocations()
                        .map_err(RuntimeServicesError::Mission)?
                        .into_iter()
                        .find(|current| current.invocation_id == invocation.invocation_id)
                        .ok_or_else(|| {
                            RuntimeServicesError::Invariant(format!(
                                "claimed Managed Agent invocation `{}` disappeared",
                                invocation.invocation_id
                            ))
                        })?;
                    let completed_invocation = match current.status {
                        crate::ManagedAgentInvocationStatus::Claimed => self
                            .managed_agents
                            .fail_claimed_invocation(
                                &invocation.invocation_id,
                                dispatcher_id,
                                invocation.fence_generation,
                                now_ms(),
                                error.to_string(),
                            )
                            .map_err(RuntimeServicesError::Mission)?,
                        crate::ManagedAgentInvocationStatus::Running => self
                            .managed_agents
                            .complete_invocation(
                                &invocation.invocation_id,
                                dispatcher_id,
                                invocation.fence_generation,
                                false,
                                now_ms(),
                                None,
                                Vec::new(),
                                Some(error.to_string()),
                            )
                            .map_err(RuntimeServicesError::Mission)?,
                        crate::ManagedAgentInvocationStatus::Materialized => self
                            .managed_agents
                            .mark_invocation_reconciliation_required(
                                &invocation.invocation_id,
                                dispatcher_id,
                                invocation.fence_generation,
                                current.claim_token.as_deref().ok_or_else(|| {
                                    RuntimeServicesError::Invariant(
                                        "materialized Managed Agent invocation lost its claim token"
                                            .to_string(),
                                    )
                                })?,
                                format!(
                                    "graph was materialized but Runtime could not start it: {error}"
                                ),
                            )
                            .map_err(RuntimeServicesError::Mission)?,
                        _ => current,
                    };
                    failed.push(completed_invocation);
                }
            }
        }
        Ok(ManagedAgentRuntimeDispatchReport {
            health_affected,
            scheduled,
            claimed,
            submitted,
            completed,
            failed,
        })
    }

    pub async fn wake_managed_agents(
        self: &Arc<Self>,
        dispatcher_id: String,
        limit: usize,
    ) -> Result<crate::RuntimeWorkAdmissionReceipt, RuntimeServicesError> {
        let services = Arc::clone(self);
        self.execution_supervisor
            .admit_owned("managed_agent_dispatch", async move {
                services
                    .dispatch_managed_agents(&dispatcher_id, limit)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(RuntimeServicesError::GraphRunner)
    }

    async fn reconcile_managed_agent_invocations(
        &self,
    ) -> Result<
        (
            Vec<crate::ManagedAgentInvocation>,
            Vec<crate::ManagedAgentInvocation>,
        ),
        RuntimeServicesError,
    > {
        let running = self
            .managed_agents
            .invocations()
            .map_err(RuntimeServicesError::Mission)?
            .into_iter()
            .filter(|invocation| invocation.status == crate::ManagedAgentInvocationStatus::Running)
            .take(256)
            .collect::<Vec<_>>();
        let mut completed = Vec::new();
        let mut failed = Vec::new();
        for invocation in running {
            let Some(graph_id) = invocation.execution_ref.as_deref() else {
                continue;
            };
            let graph = match self.graph_state_store.load_async(graph_id).await {
                Ok(graph) => graph,
                Err(ExecutionStateStoreError::NotFound(_)) => continue,
                Err(error) => return Err(RuntimeServicesError::GraphState(error)),
            };
            if graph
                .node_statuses
                .values()
                .any(|status| !status.is_terminal())
            {
                continue;
            }
            let succeeded = !graph.node_statuses.is_empty()
                && graph
                    .node_statuses
                    .values()
                    .all(|status| *status == ExecutionNodeStatus::Completed);
            let dispatcher_id = invocation.claimed_by.as_deref().ok_or_else(|| {
                RuntimeServicesError::Invariant(format!(
                    "running managed invocation `{}` has no dispatcher fence owner",
                    invocation.invocation_id
                ))
            })?;
            let mut evidence_refs = graph
                .node_results
                .values()
                .flat_map(|result| result.evidence_refs.iter())
                .map(|reference| reference.evidence_ref.id.clone())
                .collect::<Vec<_>>();
            evidence_refs.push(format!("execution-graph:{graph_id}@{}", graph.revision));
            evidence_refs.sort();
            evidence_refs.dedup();
            let terminal = match self.managed_agents.complete_invocation(
                &invocation.invocation_id,
                dispatcher_id,
                invocation.fence_generation,
                succeeded,
                now_ms(),
                Some(graph_id.to_string()),
                evidence_refs,
                (!succeeded).then(|| {
                    format!(
                        "managed execution graph reached non-success terminal state at revision {}",
                        graph.revision
                    )
                }),
            ) {
                Ok(terminal) => terminal,
                Err(error) => {
                    let current = self
                        .managed_agents
                        .invocations()
                        .map_err(RuntimeServicesError::Mission)?
                        .into_iter()
                        .find(|current| current.invocation_id == invocation.invocation_id);
                    match current {
                        Some(current) if !current.status.is_active() => current,
                        _ => return Err(RuntimeServicesError::Mission(error)),
                    }
                }
            };
            if terminal.status == crate::ManagedAgentInvocationStatus::Completed {
                completed.push(terminal);
            } else {
                failed.push(terminal);
            }
        }
        Ok((completed, failed))
    }

    async fn submit_managed_agent_invocation(
        &self,
        dispatcher_id: &str,
        invocation: crate::ManagedAgentInvocation,
    ) -> Result<crate::ManagedAgentInvocation, RuntimeServicesError> {
        let definition = self
            .managed_agents
            .definition(
                &invocation.definition_id,
                Some(invocation.definition_revision),
            )
            .map_err(RuntimeServicesError::Mission)?;
        match &definition.target {
            harness_contract::managed_agent::ManagedAgentTarget::Agent {
                definition_id,
                selector,
            } => {
                let resolved = self
                    .definition_registry
                    .resolve_agent(definition_id, selector.clone())
                    .map_err(RuntimeServicesError::DefinitionRegistry)?;
                if !matches!(
                    resolved.revision.manifest.executor,
                    harness_contract::agent::AgentExecutorPolicy::CowdNative
                ) {
                    return Err(RuntimeServicesError::Invariant(
                        "Managed Agent execution requires the Runtime-fenced CowdNative executor; ProcessJsonl and MCP-backed definitions cannot bypass the effect outbox"
                            .to_string(),
                    ));
                }
                let run_id = format!(
                    "managed-run:{}:{}:fence:{}",
                    invocation.invocation_id, invocation.attempt_no, invocation.fence_generation
                );
                let task_id = format!("{run_id}:task");
                let mut request = AgentBindingRequest::new(
                    definition_id.clone(),
                    selector.clone(),
                    format!("managed-instance:{run_id}"),
                    definition.session_id.clone(),
                    task_id.clone(),
                );
                request.granted_capabilities = definition.granted_capabilities.clone();
                request.allowed_tool_contract_refs = definition.allowed_tool_contract_refs.clone();
                request.allowed_skill_refs = definition.allowed_skill_refs.clone();
                let compiled = AgentBindingCompiler::new(Arc::clone(&self.definition_registry))
                    .compile(request)
                    .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
                let intent = AgentTaskIntent {
                    selected_agent_id: None,
                    definition_ref: Some(compiled.snapshot.definition_ref.clone()),
                    granted_capabilities: Vec::new(),
                    principal_id: dispatcher_id.to_string(),
                    source_turn_id: invocation.invocation_id.clone(),
                    run_id: run_id.clone(),
                    task_id,
                    session_id: definition.session_id.clone(),
                    mission_id: self
                        .mission_runtime
                        .mission_id_for_session(&definition.session_id)
                        .unwrap_or_else(|| self.mission_runtime.default_mission_id().to_string()),
                    team_id: None,
                    graph_id: format!(
                        "managed-agent:{}:fence:{}",
                        invocation.invocation_id, invocation.fence_generation
                    ),
                    node_id: format!(
                        "managed-agent:{}:attempt:{}",
                        invocation.invocation_id, invocation.attempt_no
                    ),
                    attempt: u32::from(invocation.attempt_no),
                    expected_graph_revision: 0,
                    objective: definition.objective.clone(),
                    acceptance: definition.acceptance.clone(),
                    constraints: vec![
                        format!(
                            "managed_agent:{}@{}",
                            definition.managed_agent_id, definition.revision
                        ),
                        format!("managed_invocation:{}", invocation.invocation_id),
                        format!("managed_fence:{}", invocation.fence_generation),
                    ],
                    context_refs: Vec::new(),
                    evidence_refs: Vec::new(),
                    resource_scopes: definition.resource_scopes.clone(),
                    allowed_tools: definition.allowed_tool_contract_refs.clone(),
                    allowed_skills: definition.allowed_skill_refs.clone(),
                    permission_lease: definition.permission_lease.clone(),
                    model_lease: definition.model_lease.clone(),
                    budget_lease: ContextBudgetLeaseRef::new(
                        format!("managed-budget:{run_id}"),
                        run_id.clone(),
                        "managed_agent",
                        65_536,
                        1,
                    ),
                    managed_invocation: Some(
                        harness_contract::managed_agent::ManagedAgentInvocationFence {
                            managed_agent_id: definition.managed_agent_id.clone(),
                            definition_revision: definition.revision,
                            invocation_id: invocation.invocation_id.clone(),
                            attempt_no: invocation.attempt_no,
                            fence_generation: invocation.fence_generation,
                            dispatcher_id: dispatcher_id.to_string(),
                        },
                    ),
                    idempotency_key: format!(
                        "managed-agent:{}:{}:fence:{}",
                        invocation.invocation_id,
                        invocation.attempt_no,
                        invocation.fence_generation
                    ),
                };
                let execution_identity = self.prepare_agent_task_intent(&intent)?;
                let packet = compiled
                    .snapshot
                    .compile_task_packet(intent, execution_identity)
                    .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
                let mut graph = ExecutionGraph::new(definition.objective.clone());
                graph.id = packet.graph_id().to_string();
                graph.service_class =
                    harness_contract::execution_graph::ExecutionServiceClass::Background;
                let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
                    ExecutionNodeKind::AgentTask,
                    AgentTaskExecutor::KIND,
                    serde_json::to_string(&packet)
                        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?,
                );
                node.id = packet.node_id().to_string();
                node.idempotency_key = packet.idempotency_key.clone();
                node.acceptance.criteria = packet.acceptance.clone();
                graph.nodes.push(node);
                let claim_token = invocation.claim_token.as_deref().ok_or_else(|| {
                    RuntimeServicesError::Invariant(
                        "claimed Managed Agent invocation has no claim token".to_string(),
                    )
                })?;
                self.managed_agents
                    .begin_graph_registration(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        claim_token,
                        graph.id.clone(),
                    )
                    .map_err(RuntimeServicesError::Mission)?;
                let graph = self
                    .execution_supervisor
                    .register_graph(graph)
                    .await
                    .map_err(RuntimeServicesError::GraphRunner)?;
                self.managed_agents
                    .materialize_invocation(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        claim_token,
                        graph.id.clone(),
                        format!("graph-registration-receipt:{}@{}", graph.id, graph.revision),
                    )
                    .map_err(RuntimeServicesError::Mission)?;
                let running = self
                    .managed_agents
                    .start_invocation(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        claim_token,
                        graph.id.clone(),
                        now_ms(),
                    )
                    .map_err(RuntimeServicesError::Mission)?;
                self.execution_supervisor
                    .admit_registered(&graph.id)
                    .await
                    .map_err(RuntimeServicesError::GraphRunner)?;
                Ok(running)
            }
            harness_contract::managed_agent::ManagedAgentTarget::Team {
                template_id,
                selector,
            } => {
                let execution_ref = format!(
                    "managed-team:{}:{}:fence:{}",
                    invocation.invocation_id, invocation.attempt_no, invocation.fence_generation
                );
                let selector_template_id = match selector {
                    harness_contract::team::TeamTemplateSelector::Exact { revision_ref } => {
                        &revision_ref.template_id
                    }
                    harness_contract::team::TeamTemplateSelector::LatestStable { template_id }
                    | harness_contract::team::TeamTemplateSelector::Default { template_id } => {
                        template_id
                    }
                    harness_contract::team::TeamTemplateSelector::Automatic => {
                        return Err(RuntimeServicesError::Invariant(
                            "managed Team target cannot use automatic template selection"
                                .to_string(),
                        ));
                    }
                };
                if selector_template_id != template_id {
                    return Err(RuntimeServicesError::Invariant(
                        "managed Team target template_id must match its selector".to_string(),
                    ));
                }
                let request = TeamInstantiationRequest {
                    request_id: format!(
                        "managed-team-request:{}:{}",
                        invocation.invocation_id, invocation.attempt_no
                    ),
                    team_id: execution_ref.clone(),
                    session_id: definition.session_id.clone(),
                    mission_id: self
                        .mission_runtime
                        .mission_id_for_session(&definition.session_id)
                        .unwrap_or_else(|| self.mission_runtime.default_mission_id().to_string()),
                    parent_execution: None,
                    selection_mode: TeamSelectionMode::Explicit,
                    strategy_binding: None,
                    template_selector: selector.clone(),
                    objective: definition.objective.clone(),
                    acceptance: definition.acceptance.clone(),
                    risk: None,
                    role_binding_overrides: Vec::new(),
                    cardinality_overrides: Vec::new(),
                    focus_partition_plans: Vec::new(),
                    permission_lease: definition.permission_lease.clone(),
                    model_lease: definition.model_lease.clone(),
                    budget_lease: None,
                    managed_invocation: Some(
                        harness_contract::managed_agent::ManagedAgentInvocationFence {
                            managed_agent_id: definition.managed_agent_id.clone(),
                            definition_revision: definition.revision,
                            invocation_id: invocation.invocation_id.clone(),
                            attempt_no: invocation.attempt_no,
                            fence_generation: invocation.fence_generation,
                            dispatcher_id: dispatcher_id.to_string(),
                        },
                    ),
                    resource_scopes: definition.resource_scopes.clone(),
                };
                let mission_id = request.mission_id.clone();
                let team_id = request.team_id.clone();
                let instantiated = self
                    .team_runtime
                    .plan(request)
                    .map_err(RuntimeServicesError::Mission)?;
                let graph_id = instantiated.graph.id.clone();
                let claim_token = invocation.claim_token.as_deref().ok_or_else(|| {
                    RuntimeServicesError::Invariant(
                        "claimed Managed Team invocation has no claim token".to_string(),
                    )
                })?;
                self.managed_agents
                    .begin_graph_registration(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        claim_token,
                        graph_id.clone(),
                    )
                    .map_err(RuntimeServicesError::Mission)?;
                let graph_id = self
                    .team_runtime
                    .prepare_planned(&mission_id, &team_id, instantiated)
                    .await
                    .map_err(RuntimeServicesError::Mission)?;
                self.managed_agents
                    .materialize_invocation(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        claim_token,
                        graph_id.clone(),
                        format!("graph-registration-receipt:{graph_id}"),
                    )
                    .map_err(RuntimeServicesError::Mission)?;
                let running = self
                    .managed_agents
                    .start_invocation(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        claim_token,
                        graph_id.clone(),
                        now_ms(),
                    )
                    .map_err(RuntimeServicesError::Mission)?;
                self.execution_supervisor
                    .admit_registered(&graph_id)
                    .await
                    .map_err(RuntimeServicesError::GraphRunner)?;
                Ok(running)
            }
        }
    }
    #[must_use]
    pub fn mission_schedule_policy(&self) -> crate::MissionSchedulePolicy {
        self.mission_schedule_policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub fn update_mission_schedule_policy(
        &self,
        policy: crate::MissionSchedulePolicy,
    ) -> Result<(), String> {
        policy.validate()?;
        *self
            .mission_schedule_policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = policy;
        Ok(())
    }

    /// Validate the Runtime-only candidate/scenario provenance carried by an
    /// isolated evaluation Binding immediately before execution.
    pub(crate) fn validate_agent_evaluation_binding(
        &self,
        binding: &harness_contract::agent::AgentBindingSnapshot,
    ) -> Result<(), RuntimeServicesError> {
        let Some(evaluation) = &binding.evaluation else {
            return Ok(());
        };
        self.evolution_governance
            .validate_agent_evaluation_binding(
                &binding.definition_ref,
                &evaluation.candidate_id,
                &evaluation.scenario_ref,
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }
    pub fn session_relations(&self) -> &Arc<SessionRelationGraph> {
        &self.session_relations
    }
    pub fn provider_registry(&self) -> &Arc<crate::ProviderRegistry> {
        &self.provider_registry
    }
    /// Return the current ordered fallback policy. Each model request reads
    /// this snapshot so Gateway config reloads affect already-open Sessions
    /// and their child Agents without mutating an in-flight request.
    #[must_use]
    pub fn provider_fallbacks(&self) -> Vec<String> {
        self.provider_fallbacks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    #[must_use]
    pub(crate) fn provider_fallback_policy(&self) -> Arc<RwLock<Vec<String>>> {
        Arc::clone(&self.provider_fallbacks)
    }
    pub fn replace_provider_fallbacks(
        &self,
        fallbacks: impl IntoIterator<Item = String>,
    ) -> Vec<String> {
        let normalized = normalize_provider_fallbacks(fallbacks);
        *self
            .provider_fallbacks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = normalized.clone();
        normalized
    }
    pub fn provider_transport_pool(&self) -> &Arc<crate::ProviderTransportPool> {
        &self.provider_transport_pool
    }
    pub fn artifact_store(&self) -> &Arc<crate::ArtifactStore> {
        &self.artifact_store
    }
    pub fn tool_execution_host(&self) -> Option<&Arc<dyn crate::RuntimeExecutionHost>> {
        self.tool_execution_host.as_ref()
    }
    pub fn session_input_router(&self) -> Option<&Arc<SessionInputRouter>> {
        self.session_input_router.get()
    }

    #[must_use]
    pub(crate) fn session_query_port(&self) -> Option<Arc<dyn crate::SessionRuntimeQueryPort>> {
        self.session_query_port.get().cloned()
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn session_ingress_port(&self) -> Option<Arc<dyn crate::SessionRuntimeIngressPort>> {
        self.session_ingress_port.get().cloned()
    }

    #[must_use]
    pub(crate) fn session_journal_port(&self) -> Option<Arc<dyn crate::SessionRuntimeJournalPort>> {
        self.session_journal_port.get().cloned()
    }

    #[must_use]
    pub fn session_history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        self.session_query_port
            .get()
            .and_then(|port| port.history_reader())
    }
}

fn ensure_team_evaluation_contract_noninferior(
    baseline: &harness_contract::team::TeamTemplateManifest,
    candidate: &harness_contract::team::TeamTemplateManifest,
) -> Result<(), RuntimeServicesError> {
    let baseline_result = &baseline.result_contract;
    let candidate_result = &candidate.result_contract;
    if (baseline.topology.require_synthesis && !candidate.topology.require_synthesis)
        || (baseline.topology.require_review && !candidate.topology.require_review)
        || (baseline_result.evidence_required && !candidate_result.evidence_required)
        || (baseline_result.synthesis_required && !candidate_result.synthesis_required)
        || baseline_result
            .required_fields
            .iter()
            .any(|field| !candidate_result.required_fields.contains(field))
        || !candidate.evaluation.is_noninferior_to(&baseline.evaluation)
    {
        return Err(RuntimeServicesError::Invariant(
            "candidate Team Template weakens the baseline evaluation/result contract; submit a separate policy review"
                .to_string(),
        ));
    }
    Ok(())
}

/// Evaluation evidence must never create an untracked external side effect.
/// Runtime has a dedicated mutation-sandbox design for future code-change
/// scenarios; until that executor exists, paired Definition evaluation is
/// deliberately read-only.  Skills are excluded because a Skill may contain
/// arbitrary multi-language executable assets and has no per-invocation
/// effect receipt yet.
fn validate_evolution_scenario_isolation(
    scenario: &EvaluationScenarioSpec,
    tool_host: Option<&dyn crate::RuntimeExecutionHost>,
) -> Result<(), RuntimeServicesError> {
    if !scenario.allowed_skills.is_empty() {
        return Err(RuntimeServicesError::Invariant(
            "paired evolution evaluation cannot execute Skills until a fenced Skill executor is installed"
                .to_string(),
        ));
    }
    let unsafe_tools = scenario
        .allowed_tools
        .iter()
        .filter(|tool| {
            tool_host
                .and_then(|host| {
                    host.delegated_tool_effect_descriptor(tool, &serde_json::json!({}))
                })
                .is_none_or(|effect| {
                    crate::ToolSafetyCategory::from_effect(&effect)
                        != crate::ToolSafetyCategory::ReadOnly
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    if !unsafe_tools.is_empty() {
        return Err(RuntimeServicesError::Invariant(format!(
            "paired evolution evaluation permits only read-only tools; unsafe tools: {}",
            unsafe_tools.join(", ")
        )));
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

async fn project_managed_invocation_terminal(
    graph_state_store: ExecutionGraphStateStore,
    managed_agents: Arc<crate::ManagedAgentDispatcher>,
    graph_id: &str,
) -> Result<(), String> {
    let Some(invocation) = managed_agents
        .invocations()?
        .into_iter()
        .find(|invocation| {
            invocation.status == crate::ManagedAgentInvocationStatus::Running
                && invocation.execution_ref.as_deref() == Some(graph_id)
        })
    else {
        return Ok(());
    };
    let graph = match graph_state_store.load_async(graph_id).await {
        Ok(graph) => graph,
        Err(ExecutionStateStoreError::NotFound(_)) => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    if graph
        .node_statuses
        .values()
        .any(|status| !status.is_terminal())
    {
        return Ok(());
    }
    let succeeded = !graph.node_statuses.is_empty()
        && graph
            .node_statuses
            .values()
            .all(|status| *status == ExecutionNodeStatus::Completed);
    let dispatcher_id = invocation.claimed_by.as_deref().ok_or_else(|| {
        format!(
            "running managed invocation `{}` has no dispatcher fence owner",
            invocation.invocation_id
        )
    })?;
    let mut evidence_refs = graph
        .node_results
        .values()
        .flat_map(|result| result.evidence_refs.iter())
        .map(|reference| reference.evidence_ref.id.clone())
        .collect::<Vec<_>>();
    evidence_refs.push(format!("execution-graph:{graph_id}@{}", graph.revision));
    evidence_refs.sort();
    evidence_refs.dedup();
    match managed_agents.complete_invocation(
        &invocation.invocation_id,
        dispatcher_id,
        invocation.fence_generation,
        succeeded,
        now_ms(),
        Some(graph_id.to_string()),
        evidence_refs,
        (!succeeded).then(|| {
            format!(
                "managed execution graph reached non-success terminal state at revision {}",
                graph.revision
            )
        }),
    ) {
        Ok(_) => Ok(()),
        Err(error) => {
            let already_terminal = managed_agents
                .invocations()?
                .into_iter()
                .find(|current| current.invocation_id == invocation.invocation_id)
                .is_some_and(|current| !current.status.is_active());
            if already_terminal {
                Ok(())
            } else {
                Err(error)
            }
        }
    }
}

async fn project_team_terminal_outcome(
    graph_state_store: ExecutionGraphStateStore,
    outcome_service: Arc<crate::execution_core::OutcomeService>,
    outcome_projector: Arc<crate::OutcomeProjector>,
    graph_id: &str,
) -> Result<(), String> {
    let graph = match graph_state_store.load_async(graph_id).await {
        Ok(graph) => graph,
        Err(ExecutionStateStoreError::NotFound(_)) => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    if graph.node_statuses.is_empty()
        || graph
            .node_statuses
            .values()
            .any(|status| !status.is_terminal())
    {
        return Ok(());
    }
    let Some(packet) = graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .filter_map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok())
        .find(|packet| packet.team_id().is_some())
    else {
        // Direct and Tool-only graphs are owned by the parent Turn Outcome;
        // standalone Agent nodes emit their own Agent terminal Outcome.
        return Ok(());
    };
    let team_id = packet
        .team_id()
        .ok_or_else(|| "Team graph has no team identity".to_string())?;
    let identity = &packet.assignment.execution_identity;
    let turn_id = identity
        .turn_id()
        .ok_or_else(|| "Team outcome has no canonical turn identity".to_string())?;
    let has_failed = graph_has_status(&graph, ExecutionNodeStatus::Failed);
    let has_blocked = graph_has_status(&graph, ExecutionNodeStatus::Blocked);
    let has_cancelled = graph_has_status(&graph, ExecutionNodeStatus::Cancelled);
    let has_completed = graph_has_status(&graph, ExecutionNodeStatus::Completed);
    let terminal = if has_failed && has_completed {
        harness_contract::outcome::OutcomeTerminalClass::PartialFailure(
            "Team graph contains completed and failed nodes".to_string(),
        )
    } else if has_failed {
        harness_contract::outcome::OutcomeTerminalClass::Failed(
            "Team graph contains failed nodes".to_string(),
        )
    } else if has_blocked {
        harness_contract::outcome::OutcomeTerminalClass::Blocked(
            "Team graph contains blocked nodes".to_string(),
        )
    } else if has_cancelled {
        harness_contract::outcome::OutcomeTerminalClass::Cancelled(
            "Team graph contains cancelled nodes".to_string(),
        )
    } else {
        harness_contract::outcome::OutcomeTerminalClass::Succeeded(
            "Team graph completed".to_string(),
        )
    };
    let completed_at_ms = graph
        .node_results
        .values()
        .map(|result| result.finished_at_ms)
        .max()
        .unwrap_or_else(now_ms);
    let duration_ms = graph.node_results.values().fold(0_u64, |total, result| {
        total.saturating_add(result.usage.duration_ms)
    });
    let usage = graph.node_results.values().fold(
        harness_contract::outcome::OutcomeUsage::default(),
        |mut usage, result| {
            usage.input_tokens = Some(
                usage
                    .input_tokens
                    .unwrap_or_default()
                    .saturating_add(result.usage.input_tokens),
            );
            usage.output_tokens = Some(
                usage
                    .output_tokens
                    .unwrap_or_default()
                    .saturating_add(result.usage.output_tokens),
            );
            usage.cached_tokens = Some(
                usage
                    .cached_tokens
                    .unwrap_or_default()
                    .saturating_add(result.usage.cached_tokens),
            );
            usage.tool_calls = usage.tool_calls.saturating_add(result.usage.tool_calls);
            usage.duplicate_tool_calls = usage
                .duplicate_tool_calls
                .saturating_add(result.usage.duplicate_tool_calls);
            usage
        },
    );
    let mut evidence_refs = graph
        .node_results
        .values()
        .flat_map(|result| result.evidence_refs.iter())
        .map(|reference| reference.evidence_ref.clone())
        .collect::<Vec<_>>();
    dedupe_evolution_evidence(&mut evidence_refs);
    let outcome = harness_contract::outcome::ExecutionOutcome {
        identity: harness_contract::outcome::OutcomeIdentity {
            execution_id: format!("team:{team_id}:{}", graph.id),
            session_id: packet.session_id().to_string(),
            turn_id: turn_id.to_string(),
            terminal_generation: graph.revision,
            paired_sample_id: None,
            task_id: Some(packet.task_id().to_string()),
            mission_id: Some(packet.mission_id().to_string()),
            agent_id: None,
            team_id: Some(team_id.to_string()),
            execution_graph_ref: Some(graph.id.clone()),
        },
        runtime: harness_contract::outcome::RuntimeIdentity {
            workspace_key: identity.workspace_id().to_string(),
            runtime_revision: env!("CARGO_PKG_VERSION").to_string(),
            config_revision: packet.binding.as_ref().map_or_else(
                || "team-graph:unknown-binding".to_string(),
                |binding| format!("team-binding:{}", binding.binding_digest),
            ),
        },
        provider: None,
        strategy: harness_contract::outcome::StrategyIdentity {
            decision_id: format!("team-graph:{}", graph.id),
            policy_revision: "runtime.team_graph.v1".to_string(),
            decision_source: "runtime.execution_supervisor".to_string(),
            selected_candidate: harness_contract::strategy::ExecutionCandidateKind::Team,
            selected_pattern: "team".to_string(),
        },
        timing: harness_contract::outcome::OutcomeTiming {
            started_at_ms: completed_at_ms.saturating_sub(duration_ms),
            completed_at_ms,
            duration_ms,
        },
        usage,
        terminal,
        quality: harness_contract::outcome::OutcomeQuality::Unknown,
        observation: harness_contract::outcome::OutcomeObservation {
            source: "runtime.team_terminal".to_string(),
            observed_at_ms: completed_at_ms,
            freshness_ms: 0,
        },
        evidence_completeness: if evidence_refs.is_empty() {
            harness_contract::reality::EvidenceCompleteness::None
        } else {
            harness_contract::reality::EvidenceCompleteness::Partial
        },
        evidence_refs,
        schema_revision: harness_contract::outcome::OUTCOME_SCHEMA_REVISION,
    };
    outcome_service.record_terminal(&outcome)?;
    outcome_projector.project_available(128)?;
    Ok(())
}

fn scenario_observation(
    packet: &AgentTaskPacket,
    returned: &harness_contract::agent::AgentReturnPacket,
    scenario: &EvaluationScenarioSpec,
    elapsed_ms: u64,
) -> EvaluationScenarioObservation {
    let acceptance_satisfied = scenario
        .acceptance
        .iter()
        .filter(|criterion| returned.acceptance.contains(*criterion))
        .count()
        .min(u32::MAX as usize) as u32;
    let mut evidence_refs = returned
        .evidence_refs
        .iter()
        .filter(|evidence| evidence.is_durable())
        .map(|evidence| evidence.evidence_ref.clone())
        .collect::<Vec<_>>();
    dedupe_evolution_evidence(&mut evidence_refs);
    EvaluationScenarioObservation {
        scenario_ref: scenario.scenario_ref.clone(),
        definition_revision: packet
            .binding
            .as_ref()
            .map(|binding| binding.definition_ref.revision)
            .unwrap_or_default(),
        run_ref: format!("agent-run:{}", packet.run_id()),
        succeeded: returned.status == AgentTerminalStatus::Completed,
        acceptance_total: scenario.acceptance.len().min(u32::MAX as usize) as u32,
        acceptance_satisfied,
        evidence_refs,
        input_tokens: returned.input_tokens,
        output_tokens: returned.output_tokens,
        tool_calls: returned.tool_calls,
        elapsed_ms,
    }
}

fn evolution_team_request(
    candidate: &crate::EvolutionGovernanceCandidate,
    scenario: &EvaluationScenarioSpec,
    revision_ref: &TeamTemplateRevisionRef,
    side: &str,
    sample_index: u32,
    mission_id: &str,
) -> TeamInstantiationRequest {
    let identity = format!(
        "evolution-eval:{}:{}:{}:{}:{}",
        candidate.candidate_id, scenario.scenario_ref, side, revision_ref.revision, sample_index
    );
    TeamInstantiationRequest {
        request_id: format!("{identity}:request"),
        team_id: format!("{identity}:team"),
        session_id: format!("evolution-eval:{}", candidate.candidate_id),
        mission_id: mission_id.to_string(),
        parent_execution: None,
        selection_mode: TeamSelectionMode::Explicit,
        strategy_binding: None,
        template_selector: TeamTemplateSelector::Exact {
            revision_ref: revision_ref.clone(),
        },
        objective: scenario.objective.clone(),
        acceptance: scenario.acceptance.clone(),
        risk: None,
        role_binding_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        permission_lease: scenario.permission_lease.clone(),
        model_lease: scenario.model_lease.clone(),
        budget_lease: Some(ContextBudgetLeaseRef::new(
            format!("evolution-eval-budget:{identity}"),
            identity.clone(),
            "evolution_evaluation",
            65_536,
            1,
        )),
        managed_invocation: None,
        resource_scopes: scenario.resource_scopes.clone(),
    }
}

fn team_scenario_observation(
    projection: &crate::TeamProjection,
    evaluations: &[crate::AgentRunEvaluation],
    scenario: &EvaluationScenarioSpec,
    definition_revision: u64,
    elapsed_ms: u64,
) -> EvaluationScenarioObservation {
    let run_ids = projection
        .tasks
        .iter()
        .map(|task| task.run_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let matched = evaluations
        .iter()
        .filter(|evaluation| run_ids.contains(evaluation.run_id.as_str()))
        .collect::<Vec<_>>();
    let mut evidence_refs = projection
        .terminal_result
        .as_ref()
        .map(|result| {
            result
                .evidence_refs
                .iter()
                .map(|evidence| evidence.evidence_ref.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    evidence_refs.extend(matched.iter().flat_map(|evaluation| {
        evaluation.evidence_refs.iter().map(|reference| {
            harness_contract::reality::EvidenceRef::observed(
                "agent_run_evidence",
                reference.clone(),
            )
            .with_source(evaluation.evaluation_id.clone())
        })
    }));
    dedupe_evolution_evidence(&mut evidence_refs);
    let succeeded = projection.status == "completed" && projection.terminal_result.is_some();
    EvaluationScenarioObservation {
        scenario_ref: scenario.scenario_ref.clone(),
        definition_revision,
        run_ref: format!("team-graph:{}", projection.graph_id),
        succeeded,
        acceptance_total: scenario.acceptance.len().min(u32::MAX as usize) as u32,
        acceptance_satisfied: succeeded
            .then_some(scenario.acceptance.len().min(u32::MAX as usize) as u32)
            .unwrap_or_default(),
        evidence_refs,
        input_tokens: matched
            .iter()
            .map(|evaluation| evaluation.input_tokens)
            .sum(),
        output_tokens: matched
            .iter()
            .map(|evaluation| evaluation.output_tokens)
            .sum(),
        tool_calls: matched.iter().map(|evaluation| evaluation.tool_calls).sum(),
        elapsed_ms,
    }
}

fn dedupe_evolution_evidence(evidence_refs: &mut Vec<harness_contract::reality::EvidenceRef>) {
    evidence_refs
        .sort_by(|left, right| (&left.ref_type, &left.id).cmp(&(&right.ref_type, &right.id)));
    evidence_refs.dedup_by(|left, right| left.ref_type == right.ref_type && left.id == right.id);
}

fn install_builtin_executors(
    registry: &NodeExecutorRegistry,
    executors: Vec<Arc<dyn NodeExecutor>>,
) -> Result<(), NodeExecutorError> {
    for executor in executors {
        registry.register(executor)?;
    }
    Ok(())
}

fn default_resource_quotas() -> Vec<(ExecutionResourceKind, ResourceQuota)> {
    vec![
        (
            ExecutionResourceKind::SessionTurn,
            ResourceQuota {
                minimum: 1,
                target: 16,
                maximum: 128,
            },
        ),
        (
            ExecutionResourceKind::Provider,
            ResourceQuota {
                minimum: 1,
                target: 4,
                maximum: 16,
            },
        ),
        (
            ExecutionResourceKind::Agent,
            ResourceQuota {
                minimum: 1,
                target: 4,
                maximum: 32,
            },
        ),
        (
            ExecutionResourceKind::Tool,
            ResourceQuota {
                minimum: 1,
                target: 8,
                maximum: 64,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.process".to_string()),
            ResourceQuota {
                minimum: 1,
                target: 4,
                maximum: 16,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.network".to_string()),
            ResourceQuota {
                minimum: 1,
                target: 8,
                maximum: 32,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.cpu".to_string()),
            ResourceQuota {
                minimum: 1,
                target: 16,
                maximum: 64,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.memory_mib".to_string()),
            ResourceQuota {
                minimum: 64,
                target: 2_048,
                maximum: 16_384,
            },
        ),
    ]
}

fn graph_is_terminal(graph: &ExecutionGraph) -> bool {
    !graph.node_statuses.is_empty()
        && graph
            .node_statuses
            .values()
            .copied()
            .all(ExecutionNodeStatus::is_terminal)
}

fn managed_invocation_fences(
    graph: &ExecutionGraph,
) -> Vec<harness_contract::managed_agent::ManagedAgentInvocationFence> {
    graph
        .nodes
        .iter()
        .filter_map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok())
        .filter_map(|packet| packet.managed_invocation)
        .collect()
}

fn graph_can_advance(graph: &ExecutionGraph) -> bool {
    graph.node_statuses.values().any(|status| {
        matches!(
            status,
            ExecutionNodeStatus::Planned | ExecutionNodeStatus::Ready
        )
    })
}

fn graph_is_waiting(graph: &ExecutionGraph) -> bool {
    graph.node_statuses.values().any(|status| {
        matches!(
            status,
            ExecutionNodeStatus::WaitingInput
                | ExecutionNodeStatus::WaitingApproval
                | ExecutionNodeStatus::WaitingExternal
                | ExecutionNodeStatus::Paused
        )
    })
}

fn graph_has_status(graph: &ExecutionGraph, target: ExecutionNodeStatus) -> bool {
    graph
        .node_statuses
        .values()
        .copied()
        .any(|status| status == target)
}

fn graph_status_label(graph: &ExecutionGraph) -> String {
    if graph.node_statuses.is_empty() {
        return "planned".to_string();
    }
    if graph_is_terminal(graph) {
        if graph_has_status(graph, ExecutionNodeStatus::Failed) {
            "failed"
        } else if graph_has_status(graph, ExecutionNodeStatus::Blocked) {
            "blocked"
        } else if graph_has_status(graph, ExecutionNodeStatus::Cancelled) {
            "cancelled"
        } else {
            "completed"
        }
    } else if graph_is_waiting(graph) {
        "waiting"
    } else if graph_has_status(graph, ExecutionNodeStatus::Running) {
        "running"
    } else {
        "ready"
    }
    .to_string()
}

fn workspace_key(workspace_root: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(workspace_root.as_os_str().as_encoded_bytes());
    let digest = format!("{:x}", hasher.finalize());
    digest[..24].to_string()
}

pub fn canonical_workspace_root(workspace_root: &Path) -> Result<PathBuf, RuntimeServicesError> {
    if workspace_root.as_os_str().is_empty() {
        return Err(RuntimeServicesError::EmptyRoot);
    }
    std::fs::canonicalize(workspace_root).map_err(|error| {
        RuntimeServicesError::Mission(format!(
            "failed to canonicalize workspace `{}`: {error}",
            workspace_root.display()
        ))
    })
}

pub fn canonical_workspace_identity(workspace_root: &Path) -> Result<String, RuntimeServicesError> {
    canonical_workspace_root(workspace_root).map(|root| workspace_key(&root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use harness_contract::agent::{
        AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
        AgentDefinitionManifest, AgentEvaluationContract, AgentExecutorPolicy, AgentModelPolicy,
        AgentOutputContract, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
        CognitiveReadScope, CognitiveWriteMode, DefinitionScope, ReleaseAssignment,
        ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
        RevisionSelector,
    };
    use harness_contract::context::ContextBudgetLeaseRef;
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    };
    use harness_contract::mission::ScheduleTrigger;
    use harness_contract::skill::{
        SkillAdapterKind, SkillCapabilityProfile, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
    };
    use harness_contract::team::{
        RoleCardinalityPolicy, TeamInstantiationRequest, TeamRoleCardinalityOverride,
        TeamSelectionMode, TeamTemplateDefinitionId, TeamTemplateSelector,
    };
    use session::SessionRecord;

    #[test]
    fn in_memory_services_reclaim_their_filesystem_state() {
        let root = {
            let services = RuntimeServices::in_memory().expect("in-memory runtime services");
            let root = services
                ._ephemeral_root
                .as_ref()
                .expect("in-memory services own a temporary root")
                .path()
                .to_path_buf();
            assert!(root.exists());
            root
        };

        assert!(
            !root.exists(),
            "dropping the final RuntimeServices owner must remove {root:?}"
        );
    }

    #[test]
    fn startup_recovers_task_outbox_and_mission_membership_after_commit_crash() {
        let root = tempfile::tempdir().expect("runtime root");
        let home = root.path().join("home");
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let first = RuntimeServices::builder(&home, &workspace)
            .build()
            .expect("first runtime");
        let mission_id = first.mission_runtime().default_mission_id().to_string();
        first
            .task_aggregate_service()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: "task-startup-recovery".to_string(),
                mission_id: mission_id.clone(),
                source_session_id: "session-startup-recovery".to_string(),
                source_turn_id: "turn-startup-recovery".to_string(),
                spec: harness_contract::task::TaskSpec::new("recover committed task side effects"),
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "test_fixture",
                    "test://task/startup-recovery",
                )],
            })
            .expect("commit Task without running its Runtime port");
        assert!(first
            .event_reader()
            .list_stream("task:task-startup-recovery")
            .expect("task event stream")
            .is_empty());
        assert_eq!(
            first
                .task_aggregate_service()
                .pending_outbox(None, 10)
                .expect("pending outbox")
                .len(),
            1
        );
        drop(first);

        let recovered = RuntimeServices::builder(&home, &workspace)
            .build()
            .expect("recovered runtime");
        assert_eq!(
            recovered
                .task_aggregate_service()
                .pending_outbox(None, 10)
                .expect("drained outbox")
                .len(),
            0
        );
        assert_eq!(
            recovered
                .event_reader()
                .list_stream("task:task-startup-recovery")
                .expect("projected Task event")
                .len(),
            1
        );
        let mission = recovered
            .mission_runtime()
            .aggregate(&mission_id)
            .expect("recovered Mission");
        assert!(mission
            .session_refs
            .iter()
            .any(|reference| reference.id == "session-startup-recovery"));
        assert!(mission
            .task_refs
            .iter()
            .any(|reference| reference.id == "task-startup-recovery"));
    }

    #[tokio::test]
    async fn maintenance_supervisor_serializes_owner_tasks_and_drains_them() {
        let supervisor = Arc::new(RuntimeMaintenanceSupervisor::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let first_order = Arc::clone(&order);
        assert!(
            supervisor
                .submit("session-a".to_string(), async move {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    first_order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(1);
                })
                .await
        );
        let second_order = Arc::clone(&order);
        assert!(
            supervisor
                .submit("session-a".to_string(), async move {
                    second_order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(2);
                })
                .await
        );

        assert_eq!(supervisor.tracked_task_count(), 1);
        supervisor.shutdown_and_drain().await;
        assert_eq!(supervisor.tracked_task_count(), 0);
        assert_eq!(
            *order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn maintenance_supervisor_serializes_concurrent_submissions_per_owner() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let supervisor = Arc::new(RuntimeMaintenanceSupervisor::new());
        let start = Arc::new(tokio::sync::Barrier::new(9));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let mut submitters = Vec::new();
        for _ in 0..8 {
            let supervisor = Arc::clone(&supervisor);
            let start = Arc::clone(&start);
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            let completed = Arc::clone(&completed);
            submitters.push(tokio::spawn(async move {
                start.wait().await;
                assert!(
                    supervisor
                        .submit("session-a".to_string(), async move {
                            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                            maximum.fetch_max(now, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            completed.fetch_add(1, Ordering::SeqCst);
                        })
                        .await
                );
            }));
        }
        start.wait().await;
        for submitter in submitters {
            submitter.await.expect("submitter joins");
        }
        supervisor.shutdown_and_drain().await;
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        assert_eq!(completed.load(Ordering::SeqCst), 8);
        assert_eq!(supervisor.tracked_task_count(), 0);
    }

    #[tokio::test]
    async fn maintenance_supervisor_rejects_work_after_shutdown_starts() {
        let supervisor = Arc::new(RuntimeMaintenanceSupervisor::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let release_work = Arc::clone(&release);
        assert!(
            supervisor
                .submit("session-a".to_string(), async move {
                    release_work.notified().await;
                })
                .await
        );

        let draining = {
            let supervisor = Arc::clone(&supervisor);
            tokio::spawn(async move { supervisor.shutdown_and_drain().await })
        };
        tokio::task::yield_now().await;
        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_work = Arc::clone(&executed);
        assert!(
            !supervisor
                .submit("session-b".to_string(), async move {
                    executed_work.store(true, std::sync::atomic::Ordering::SeqCst);
                })
                .await
        );
        release.notify_waiters();
        draining.await.expect("shutdown joins");
        assert!(!executed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn maintenance_supervisor_contains_panics_and_reclaims_owner() {
        let supervisor = RuntimeMaintenanceSupervisor::new();
        assert!(
            supervisor
                .submit("session-a".to_string(), async move {
                    panic!("maintenance failure");
                })
                .await
        );
        supervisor.shutdown_and_drain().await;
        assert_eq!(supervisor.tracked_task_count(), 0);
    }

    #[tokio::test]
    async fn maintenance_supervisor_reclaims_idle_owner_without_shutdown() {
        let supervisor = RuntimeMaintenanceSupervisor::new();
        let completed = Arc::new(tokio::sync::Notify::new());
        let completed_work = Arc::clone(&completed);
        assert!(
            supervisor
                .submit("session-a".to_string(), async move {
                    completed_work.notify_one();
                })
                .await
        );
        completed.notified().await;
        tokio::time::timeout(Duration::from_millis(100), async {
            while supervisor.tracked_task_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("idle owner is reclaimed");
        supervisor.shutdown_and_drain().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn maintenance_supervisor_never_strands_immediate_owner_completion() {
        let supervisor = RuntimeMaintenanceSupervisor::new();
        for index in 0..128 {
            assert!(
                supervisor
                    .submit(format!("immediate-{index}"), async {})
                    .await
            );
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while supervisor.tracked_task_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("every immediate owner is reaped");
        supervisor.shutdown_and_drain().await;
    }

    #[tokio::test]
    async fn maintenance_supervisor_aborts_timed_out_work() {
        let supervisor =
            RuntimeMaintenanceSupervisor::with_shutdown_timeout(Duration::from_millis(10));
        assert!(
            supervisor
                .submit("session-a".to_string(), std::future::pending())
                .await
        );
        tokio::time::timeout(Duration::from_millis(100), supervisor.shutdown_and_drain())
            .await
            .expect("bounded shutdown");
        assert_eq!(supervisor.tracked_task_count(), 0);
    }

    #[test]
    fn task_terminal_observation_is_idempotent_without_becoming_a_task_writer() {
        let services = RuntimeServices::in_memory().expect("in-memory runtime services");
        let first = services
            .task_runtime_port()
            .record_assignment_terminal_observation(
                "task-completion-1",
                "completed",
                "runtime-event://source",
                "correlation-1",
            )
            .expect("first observation");
        let replay = services
            .task_runtime_port()
            .record_assignment_terminal_observation(
                "task-completion-1",
                "completed",
                "runtime-event://source",
                "correlation-1",
            )
            .expect("idempotent observation replay");
        assert_eq!(first.event_id, replay.event_id);
        assert_eq!(first.commit_cursor, replay.commit_cursor);
        assert_eq!(first.scope, RuntimeEventScope::Relation);
        assert_eq!(
            first.kind,
            "application.assignment.task_terminal_observed.v1"
        );
        assert_eq!(
            services
                .event_store
                .list_stream("task-observation:task-completion-1")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn concurrent_task_terminal_observation_replays_the_committed_receipt() {
        let services =
            std::sync::Arc::new(RuntimeServices::in_memory().expect("in-memory runtime services"));
        let workers = (0..16)
            .map(|_| {
                let services = std::sync::Arc::clone(&services);
                std::thread::spawn(move || {
                    services
                        .task_runtime_port()
                        .record_assignment_terminal_observation(
                            "task-completion-race",
                            "completed",
                            "runtime-event://source-race",
                            "correlation-race",
                        )
                })
            })
            .collect::<Vec<_>>();

        let receipts = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker join").expect("observation"))
            .collect::<Vec<_>>();
        let first_event_id = receipts[0].event_id.clone();
        assert!(receipts
            .iter()
            .all(|receipt| receipt.event_id == first_event_id));
        assert_eq!(
            services
                .event_store
                .list_stream("task-observation:task-completion-race")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn runtime_skill_catalog_is_available_to_delegated_execution_services() {
        let services = RuntimeServices::in_memory().expect("in-memory runtime services");
        let profile = SkillCapabilityProfile {
            skill_id: "delegated-review".to_string(),
            name: "Delegated Review".to_string(),
            version: Some("1.0.0".to_string()),
            source_root: "skill://delegated-review".to_string(),
            package_fingerprint: "sha256:delegated-review".to_string(),
            kind: SkillKind::Workflow,
            lifecycle_status: SkillLifecycleStatus::UsablePrompt,
            adapters: vec![SkillAdapterKind::PromptOnly],
            risk_level: SkillRiskLevel::Low,
            entrypoints: Vec::new(),
            inspection_summary: vec!["review source changes".to_string()],
            structured_dependencies: Vec::new(),
        };
        services.replace_skill_catalog(crate::RuntimeSkillCatalog::new(
            vec![profile],
            vec![crate::RuntimeSkillPromptAsset {
                skill_id: "delegated-review".to_string(),
                version: Some("1.0.0".to_string()),
                content: "Review evidence before returning.".to_string(),
                source_ref: "skill://delegated-review/SKILL.md".to_string(),
                tool_refs: Vec::new(),
            }],
        ));

        let catalog = services.skill_catalog();
        assert_eq!(catalog.profiles()[0].skill_id, "delegated-review");
        assert_eq!(
            catalog.prompt_assets()[0].source_ref,
            "skill://delegated-review/SKILL.md"
        );
    }

    struct TestExecutionHost;

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for TestExecutionHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some("ok".to_string()),
                error: None,
                evidence_ref: format!("evidence:{}", request.tool_use_id),
            }
        }
    }

    struct ServiceScopedBackend {
        calls: Arc<AtomicUsize>,
    }

    struct CompletedAgentBackend;

    struct ParallelTrackingAgentBackend {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::AgentRuntimeBackend for CompletedAgentBackend {
        fn kind(&self) -> crate::AgentBackendKind {
            crate::AgentBackendKind::InProcess
        }

        fn capabilities(&self) -> crate::AgentBackendCapabilities {
            crate::AgentBackendCapabilities::in_process()
        }

        async fn execute(
            &self,
            packet: AgentTaskPacket,
            selection: crate::AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            let mut evidence_refs = packet.evidence_refs.clone();
            evidence_refs.push(harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::observed(
                    "tool",
                    format!("materialized:{}", packet.node_id()),
                ),
                "a".repeat(64),
                1,
                "application/json",
                "artifact://art_runtime_services_packet",
                format!("session:{}", packet.session_id()),
            ));
            Ok(AgentReturnPacket {
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                session_id: packet.session_id().to_string(),
                mission_id: packet.mission_id().to_string(),
                team_id: packet.team_id().map(ToString::to_string),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: serde_json::json!({
                    "summary": "verified agent result",
                    "evidence": "materialized durable tool evidence",
                    "completed": "verified"
                })
                .to_string(),
                acceptance: packet.acceptance,
                evidence_refs,
                changes: Vec::new(),
                runtime_change_receipts: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 5,
                output_tokens: 3,
                cached_tokens: 0,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 1,
                duplicate_tool_calls: 0,
                runtime_write_attempt_paths: Vec::new(),
                runtime_observed_resource_scopes: Vec::new(),
                failure: None,
            })
        }
    }

    #[async_trait::async_trait]
    impl crate::AgentRuntimeBackend for ParallelTrackingAgentBackend {
        fn kind(&self) -> crate::AgentBackendKind {
            crate::AgentBackendKind::InProcess
        }

        fn capabilities(&self) -> crate::AgentBackendCapabilities {
            crate::AgentBackendCapabilities::in_process()
        }

        async fn execute(
            &self,
            packet: AgentTaskPacket,
            selection: crate::AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            let mut evidence_refs = packet.evidence_refs.clone();
            evidence_refs.push(harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::observed(
                    "tool",
                    format!("materialized:{}", packet.node_id()),
                ),
                "a".repeat(64),
                1,
                "application/json",
                "artifact://art_runtime_services_shared",
                format!("session:{}", packet.session_id()),
            ));
            Ok(AgentReturnPacket {
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                session_id: packet.session_id().to_string(),
                mission_id: packet.mission_id().to_string(),
                team_id: packet.team_id().map(ToString::to_string),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: serde_json::json!({
                    "summary": "parallel agent result",
                    "evidence": "materialized durable tool evidence",
                    "completed": "verified"
                })
                .to_string(),
                acceptance: packet.acceptance,
                evidence_refs,
                changes: Vec::new(),
                runtime_change_receipts: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 5,
                output_tokens: 3,
                cached_tokens: 0,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 1,
                duplicate_tool_calls: 0,
                runtime_write_attempt_paths: Vec::new(),
                runtime_observed_resource_scopes: Vec::new(),
                failure: None,
            })
        }
    }

    #[async_trait::async_trait]
    impl super::super::graph::ScopedNodeBackend for ServiceScopedBackend {
        async fn execute(
            &self,
            ticket: &super::super::graph::NodeExecutionTicket,
        ) -> Result<super::super::graph::NodeExecutionOutcome, super::super::graph::NodeExecutorError>
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(super::super::graph::NodeExecutionOutcome::new(
                harness_contract::execution_graph::ExecutionNodeResult {
                    status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                    result_ref: Some(format!("service-result:{}", ticket.node_id)),
                    summary: Some("service backend completed".to_string()),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }

    struct ServiceScopedResolver {
        payload_ref: String,
        backend: Arc<ServiceScopedBackend>,
    }

    impl super::super::graph::executors::ScopedNodeBackendResolver for ServiceScopedResolver {
        fn resolve(
            &self,
            ticket: &super::super::graph::NodeExecutionTicket,
        ) -> Option<Arc<dyn super::super::graph::ScopedNodeBackend>> {
            (ticket.payload_ref == self.payload_ref).then(|| {
                Arc::clone(&self.backend) as Arc<dyn super::super::graph::ScopedNodeBackend>
            })
        }
    }

    #[test]
    fn workspace_instances_isolate_provider_tool_and_registry_ownership() {
        let left = RuntimeServices::in_memory().unwrap();
        let right = RuntimeServices::in_memory().unwrap();
        assert_ne!(left.workspace_key(), right.workspace_key());
        assert!(!Arc::ptr_eq(
            left.provider_registry(),
            right.provider_registry()
        ));
        assert!(!Arc::ptr_eq(
            left.executor_registry(),
            right.executor_registry()
        ));
        assert_eq!(
            left.executor_registry().available_kinds(),
            right.executor_registry().available_kinds()
        );
        assert!(left
            .executor_registry()
            .available_kinds()
            .contains("inline_model"));
        assert!(left
            .executor_registry()
            .available_kinds()
            .contains("tool_batch"));
        assert!(left
            .executor_registry()
            .available_kinds()
            .contains("session_dispatch"));
        assert!(left
            .executor_registry()
            .available_kinds()
            .contains("cross_plane_connector"));
        assert!(!Arc::ptr_eq(
            left.cross_plane_connector_executor(),
            right.cross_plane_connector_executor()
        ));
        assert!(!Arc::ptr_eq(left.scope_locks(), right.scope_locks()));
        assert!(!Arc::ptr_eq(
            left.worktree_leases(),
            right.worktree_leases()
        ));
    }

    #[test]
    fn definition_catalog_refresh_only_exposes_active_stable_revisions() {
        let temp = tempfile::tempdir().expect("temporary root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
            .build()
            .expect("runtime services");
        let definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/reviewer")
            .expect("definition id");
        let instructions = "# Reviewer\n\nReview evidence.\n";
        let digest = format!("{:x}", Sha256::digest(instructions.as_bytes()));
        let stored = services
            .definition_registry()
            .agents()
            .store_revision(
                AgentDefinitionManifest {
                    api_version: "cowd.agent/v1".to_string(),
                    definition_id: definition_id.clone(),
                    revision: 1,
                    name: "Reviewer".to_string(),
                    description: "Reviews implementation evidence".to_string(),
                    lifecycle: RevisionLifecycle::Published,
                    executor: AgentExecutorPolicy::CowdNative,
                    model_policy: AgentModelPolicy {
                        profile: "coding".to_string(),
                        allowed_models: vec!["test-model".to_string()],
                        fallback_allowed: true,
                    },
                    cognitive_policy: AgentCognitivePolicy {
                        context_profile: "team".to_string(),
                        read_scopes: vec![CognitiveReadScope::Session],
                        write_mode: CognitiveWriteMode::CandidateOnly,
                        team_working_state_visible: true,
                    },
                    capability_contract: AgentCapabilityContract {
                        capability_ceiling: vec![AgentCapability::Read],
                        skill_refs: Vec::new(),
                        approval_required_for: Vec::new(),
                    },
                    output_contract: AgentOutputContract::reviewable(),
                    evaluation: AgentEvaluationContract::single_release_gate("review", "evidence"),
                    instructions_digest: digest,
                },
                instructions,
            )
            .expect("stored revision");
        services
            .definition_registry()
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/reviewer-v1".to_string(),
                },
                content_digest: stored.revision.content_digest.clone(),
            })
            .expect("active stable");
        services
            .refresh_definition_catalog()
            .expect("catalog refresh");
        let entry = services
            .agent_runtime()
            .catalog()
            .get(definition_id.as_str())
            .expect("runnable entry");
        assert_eq!(entry.definition_ref.revision, 1);
        assert_eq!(entry.capabilities, vec!["read"]);

        services
            .definition_registry()
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref,
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Stopped,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/reviewer-v1".to_string(),
                },
                content_digest: stored.revision.content_digest,
            })
            .expect("stopped stable");
        services
            .refresh_definition_catalog()
            .expect("catalog refresh");
        assert!(services
            .agent_runtime()
            .catalog()
            .get(definition_id.as_str())
            .is_none());
    }

    #[test]
    fn active_canary_routes_new_bindings_and_stop_reverts_to_stable() {
        let temp = tempfile::tempdir().expect("temporary root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
            .build()
            .expect("runtime services");
        let definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/canary")
            .expect("definition id");
        let instructions = "# Canary\n\nReturn evidence-backed review output.\n";
        let evaluation = AgentEvaluationContract {
            scenario_refs: vec!["canary/review".to_string()],
            metrics: vec![
                harness_contract::agent::EvaluationMetricSpec::release_gate(
                    "canary/review",
                    "contract",
                    true,
                    true,
                ),
                harness_contract::agent::EvaluationMetricSpec::release_gate(
                    "canary/review",
                    "evidence",
                    true,
                    false,
                ),
            ],
        };
        let manifest = |revision| AgentDefinitionManifest {
            api_version: "cowd.agent/v1".to_string(),
            definition_id: definition_id.clone(),
            revision,
            name: format!("Canary {revision}"),
            description: "Canary routing fixture".to_string(),
            lifecycle: RevisionLifecycle::Published,
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "test".to_string(),
                allowed_models: Vec::new(),
                fallback_allowed: true,
            },
            cognitive_policy: AgentCognitivePolicy {
                context_profile: "sub_agent".to_string(),
                read_scopes: vec![CognitiveReadScope::Session],
                write_mode: CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
            },
            capability_contract: AgentCapabilityContract {
                capability_ceiling: vec![AgentCapability::Read],
                skill_refs: Vec::new(),
                approval_required_for: Vec::new(),
            },
            output_contract: AgentOutputContract::reviewable(),
            evaluation: evaluation.clone(),
            instructions_digest: format!("{:x}", Sha256::digest(instructions.as_bytes())),
        };
        let baseline = services
            .definition_registry()
            .agents()
            .store_revision(manifest(1), instructions)
            .expect("baseline revision");
        let candidate_revision = services
            .definition_registry()
            .agents()
            .store_revision(manifest(2), instructions)
            .expect("candidate revision");
        services
            .definition_registry()
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: baseline.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/canary-baseline".to_string(),
                },
                content_digest: baseline.revision.content_digest.clone(),
            })
            .expect("baseline stable");
        let signal = services
            .record_evolution_signal(crate::EvolutionSignal::eval_failure(
                "canary-fixture",
                vec![harness_contract::reality::EvidenceRef::observed(
                    "agent_run",
                    "baseline",
                )],
            ))
            .expect("signal");
        let proposal = services
            .create_evolution_lifecycle(vec![signal.signal_id])
            .expect("proposal")
            .proposal;
        let principal = crate::security::test_human_interactive_principal();
        let digest = services
            .evolution_proposal_decision_digest(&proposal.proposal_id, "approved")
            .expect("proposal digest");
        let proposal_lease = crate::security::test_verified_decision_lease(
            &format!("evolution-proposal:{}", proposal.proposal_id),
            "proposal.decision.approved",
            &format!("evolution.proposal:{}", proposal.proposal_id),
            &digest,
        );
        services
            .decide_evolution_proposal(
                &principal,
                &proposal_lease,
                &proposal.proposal_id,
                "approved",
            )
            .expect("proposal approved");
        let candidate = services
            .register_evolution_candidate(crate::EvolutionCandidateIntent {
                candidate_id: "candidate-canary-v2".to_string(),
                proposal_id: proposal.proposal_id,
                subject: crate::EvolutionCandidateSubject::AgentDefinition {
                    revision_ref: candidate_revision.revision.revision_ref.clone(),
                },
                baseline_revision: 1,
                source_evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "agent_run",
                    "baseline",
                )],
                canary_policy: crate::CanaryRolloutPolicy {
                    traffic_basis_points: 10_000,
                    minimum_samples: 1,
                    minimum_duration_ms: 1,
                    maximum_duration_ms: 60_000,
                },
            })
            .expect("candidate registered");
        services
            .evolution_governance
            .record_comparison(crate::EvolutionComparisonReportV2 {
                report_id: "canary-fixture-report".to_string(),
                candidate_id: candidate.candidate_id.clone(),
                evaluation_contract_digest: candidate.evaluation_contract_digest(),
                dimensions: vec![
                    crate::EvolutionComparisonDimension {
                        metric_id: "evidence".to_string(),
                        direction: crate::EvaluationDirection::HigherIsBetter,
                        baseline: 1.0,
                        candidate: 1.0,
                        non_inferiority_margin: 0.0,
                        sample_count: 10,
                        minimum_samples: 10,
                        confidence: 1.0,
                        minimum_confidence: 0.9,
                        hard_gate: true,
                        protected: true,
                        target_improvement: false,
                    },
                    crate::EvolutionComparisonDimension {
                        metric_id: "contract".to_string(),
                        direction: crate::EvaluationDirection::HigherIsBetter,
                        baseline: 1.0,
                        // The immutable contract marks this as a target-improvement
                        // metric, so equality is intentionally not eligible for a
                        // Canary review.
                        candidate: 1.01,
                        non_inferiority_margin: 0.0,
                        sample_count: 10,
                        minimum_samples: 10,
                        confidence: 1.0,
                        minimum_confidence: 0.9,
                        hard_gate: true,
                        protected: true,
                        target_improvement: true,
                    },
                ],
                source_run_refs: vec!["eval:paired".to_string()],
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "evaluation",
                    "paired",
                )],
                created_at_ms: 1,
            })
            .expect("eligible comparison");
        let review = services
            .request_evolution_canary_review(&candidate.candidate_id)
            .expect("canary review");
        let lease = crate::security::test_verified_decision_lease(
            &review.review_id,
            review.action_key(),
            review.subject.scope_ref(),
            review.evidence_digest(),
        );
        services
            .decide_evolution_release_review(
                &principal,
                &lease,
                &review.review_id,
                crate::ReleaseChangeReviewDecision::Approve,
                "approve canary fixture".to_string(),
            )
            .expect("canary approved");

        let mut request = AgentBindingRequest::new(
            definition_id.clone(),
            RevisionSelector::LatestApprovedStable,
            "instance:canary-a",
            "session:canary",
            "task:canary",
        );
        request.granted_capabilities = vec![AgentCapability::Read];
        let routed = services
            .compile_agent_binding(request)
            .expect("canary binding");
        assert_eq!(routed.snapshot.definition_ref.revision, 2);
        assert_eq!(
            routed
                .snapshot
                .release
                .as_ref()
                .map(|release| release.channel),
            Some(ReleaseChannel::Canary)
        );

        let stop = services
            .request_evolution_release_change(crate::ReleaseChangeRequest {
                request_id: "stop-canary-fixture".to_string(),
                subject: candidate.subject.clone(),
                action: crate::ReleaseChangeAction::StopCanary,
                selector: None,
                candidate_id: Some(candidate.candidate_id.clone()),
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "incident", "fixture",
                )],
            })
            .expect("stop review");
        let stop_lease = crate::security::test_verified_decision_lease(
            &stop.review_id,
            stop.action_key(),
            stop.subject.scope_ref(),
            stop.evidence_digest(),
        );
        services
            .decide_evolution_release_review(
                &principal,
                &stop_lease,
                &stop.review_id,
                crate::ReleaseChangeReviewDecision::Approve,
                "stop canary fixture".to_string(),
            )
            .expect("stopped canary");

        let mut request = AgentBindingRequest::new(
            definition_id,
            RevisionSelector::LatestApprovedStable,
            "instance:canary-b",
            "session:canary",
            "task:stable",
        );
        request.granted_capabilities = vec![AgentCapability::Read];
        let stable = services
            .compile_agent_binding(request)
            .expect("stable binding");
        assert_eq!(stable.snapshot.definition_ref.revision, 1);
        assert!(stable.snapshot.release.is_none());
    }

    #[test]
    fn explicit_toml_import_is_runtime_owned_and_never_enters_runnable_catalog() {
        let temp = tempfile::tempdir().expect("temporary root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
            .build()
            .expect("runtime services");
        let definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, "external/reviewer")
            .expect("definition id");

        let receipt = services
            .import_agent_toml_draft(crate::agent::definition::ExplicitTomlAgentImport {
                definition_id: definition_id.clone(),
                revision: 1,
                source_label: "manual:/tmp/external-reviewer.toml".to_string(),
                toml: "name = 'External reviewer'\nmodel = 'review-model'\n".to_string(),
            })
            .expect("runtime import");

        assert_eq!(receipt.revision_ref.definition_id, definition_id);
        assert_eq!(receipt.revision_ref.revision, 1);
        assert!(!receipt.content_digest.is_empty());
        services
            .refresh_definition_catalog()
            .expect("catalog refresh");
        assert!(services
            .agent_runtime()
            .catalog()
            .get("workspace/external/reviewer")
            .is_none());
    }

    #[test]
    fn builder_rejects_partial_session_port_sets() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
        let result = RuntimeServices::builder(temp.path(), temp.path().join("partial"))
            .session_query_port(ports)
            .build();

        assert!(matches!(
            result,
            Err(RuntimeServicesError::IncompleteSessionPorts)
        ));
    }

    #[test]
    fn workspace_builders_isolate_provider_tool_host_and_session_router() {
        let temp = tempfile::tempdir().unwrap();
        let left_provider = Arc::new(crate::ProviderRegistry::empty());
        let right_provider = Arc::new(crate::ProviderRegistry::empty());
        let left_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
        let right_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
        let left_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let right_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        std::fs::create_dir_all(temp.path().join("left")).unwrap();
        std::fs::create_dir_all(temp.path().join("right")).unwrap();

        let left_ports =
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&left_store));
        let right_ports =
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&right_store));
        let left = RuntimeServices::builder(temp.path(), temp.path().join("left"))
            .provider_registry(Arc::clone(&left_provider))
            .tool_execution_host(Arc::clone(&left_tool))
            .session_query_port(left_ports.clone())
            .session_ingress_port(left_ports.clone())
            .session_journal_port(left_ports)
            .build()
            .unwrap();
        let right = RuntimeServices::builder(temp.path(), temp.path().join("right"))
            .provider_registry(Arc::clone(&right_provider))
            .tool_execution_host(Arc::clone(&right_tool))
            .session_query_port(right_ports.clone())
            .session_ingress_port(right_ports.clone())
            .session_journal_port(right_ports)
            .build()
            .unwrap();

        assert!(Arc::ptr_eq(left.provider_registry(), &left_provider));
        assert!(Arc::ptr_eq(right.provider_registry(), &right_provider));
        assert!(!Arc::ptr_eq(
            left.provider_registry(),
            right.provider_registry()
        ));
        assert!(Arc::ptr_eq(left.tool_execution_host().unwrap(), &left_tool));
        assert!(Arc::ptr_eq(
            right.tool_execution_host().unwrap(),
            &right_tool
        ));
        assert!(!Arc::ptr_eq(
            left.tool_execution_host().unwrap(),
            right.tool_execution_host().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            left.session_input_router().unwrap(),
            right.session_input_router().unwrap()
        ));
        assert!(left.session_query_port().is_some());
        assert!(left.session_ingress_port().is_some());
        assert!(left.session_journal_port().is_some());
        assert!(right.session_query_port().is_some());
        assert!(right.session_ingress_port().is_some());
        assert!(right.session_journal_port().is_some());
    }

    #[tokio::test]
    async fn due_schedule_submits_one_durable_handoff_graph_and_never_duplicates_it() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let timestamp = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "scheduled-target".to_string(),
                platform: "test".to_string(),
                chat_id: "scheduled-chat".to_string(),
                user_id: None,
                model: None,
                created_at: timestamp.clone(),
                last_activity: timestamp,
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
        let services = RuntimeServices::in_memory().unwrap();
        services
            .install_test_session_store(Arc::clone(&store))
            .unwrap();
        let due_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        services
            .mission_schedules()
            .create(
                crate::CreateMissionScheduleRequest {
                    mission_id: "schedule-mission".to_string(),
                    target_session_id: "scheduled-target".to_string(),
                    objective: "check the durable schedule path".to_string(),
                    trigger: ScheduleTrigger::At { at_ms: due_at_ms },
                    autonomy_profile: "assisted".to_string(),
                    permission_lease: "read_only".to_string(),
                    priority: 64,
                },
                due_at_ms,
            )
            .unwrap();

        let first = services
            .dispatch_due_mission_schedules(due_at_ms)
            .await
            .unwrap();
        assert_eq!(first.tick.claimed.len(), 1);
        assert_eq!(first.submitted.len(), 1);
        assert!(first.failed.is_empty());
        let graph_id = first.submitted[0]
            .graph_id
            .clone()
            .expect("stable graph id");
        services
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await
            .unwrap();
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        assert!(graph
            .node_statuses
            .values()
            .all(|status| *status == ExecutionNodeStatus::WaitingExternal));

        let target_outbox = store
            .claim_session_runtime_outbox(
                "schedule-test",
                due_at_ms.saturating_add(5_000),
                1_000,
                8,
            )
            .await
            .unwrap();
        assert_eq!(target_outbox.len(), 1);
        assert_eq!(target_outbox[0].session_id, "scheduled-target");

        let second = services
            .dispatch_due_mission_schedules(due_at_ms.saturating_add(1))
            .await
            .unwrap();
        assert!(second.tick.claimed.is_empty());
        assert!(second.submitted.is_empty());
        assert!(second.failed.is_empty());
    }

    #[tokio::test]
    async fn same_workspace_services_coordinate_with_persistent_resources() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(temp.path().join("other")).unwrap();
        let first = RuntimeServices::builder(temp.path(), &workspace)
            .build()
            .unwrap();
        let second = RuntimeServices::builder(temp.path(), &workspace)
            .build()
            .unwrap();
        let isolated = RuntimeServices::builder(temp.path(), temp.path().join("other"))
            .build()
            .unwrap();

        assert!(!Arc::ptr_eq(first.scope_locks(), second.scope_locks()));
        assert!(!Arc::ptr_eq(
            first.worktree_leases(),
            second.worktree_leases()
        ));
        assert!(!Arc::ptr_eq(first.scope_locks(), isolated.scope_locks()));
        assert!(!Arc::ptr_eq(
            first.worktree_leases(),
            isolated.worktree_leases()
        ));

        let held = first
            .scope_locks()
            .acquire(
                [super::super::graph::ScopeLockRequest {
                    scope: super::super::graph::ScopedResource::workspace(first.workspace_key())
                        .unwrap(),
                    mode: super::super::graph::ScopeLockMode::Write,
                }],
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            second
                .scope_locks()
                .acquire(
                    [super::super::graph::ScopeLockRequest {
                        scope: super::super::graph::ScopedResource::workspace(
                            second.workspace_key(),
                        )
                        .unwrap(),
                        mode: super::super::graph::ScopeLockMode::Write,
                    }],
                    Some(std::time::Duration::from_millis(25)),
                )
                .await,
            Err(super::super::graph::ScopeLockError::TimedOut { .. })
        ));
        drop(held);
    }

    #[test]
    fn canonical_workspace_identity_shares_resources_across_home_and_symlink_aliases() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let alias = temp.path().join("workspace-alias");
        std::fs::create_dir_all(&workspace).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&workspace, &alias).unwrap();
        #[cfg(not(unix))]
        std::fs::create_dir_all(&alias).unwrap();

        let first = RuntimeServices::builder(temp.path().join("home-a"), &workspace)
            .build()
            .unwrap();
        let second = RuntimeServices::builder(temp.path().join("home-b"), &alias)
            .build()
            .unwrap();

        #[cfg(unix)]
        {
            assert_eq!(first.workspace_key(), second.workspace_key());
            assert!(!Arc::ptr_eq(first.scope_locks(), second.scope_locks()));
            assert!(!Arc::ptr_eq(
                first.worktree_leases(),
                second.worktree_leases()
            ));
        }
    }

    #[tokio::test]
    async fn recovery_marker_blocks_runner_start_run_and_command_entries() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .build()
            .unwrap();
        let importer = crate::upgrade::LegacyExecutionImporter::new(
            Arc::clone(services.event_store()),
            services.workspace_key(),
            services.workspace_root(),
            "0.9.472",
        );
        assert!(importer
            .import_receipt_file(temp.path().join("missing-receipt.json"))
            .is_err());

        let graph = harness_contract::execution_graph::ExecutionGraph::new("blocked");
        let graph_id = graph.id.clone();
        assert!(matches!(
            services
                .execution_supervisor()
                .submit_and_wait(
                    graph,
                    harness_contract::execution_graph::ExecutionGraphCommand::Start {
                        expected_revision: 0,
                    },
                )
                .await,
            Err(super::super::graph::ExecutionRunnerError::MutationBlocked(
                _
            ))
        ));
        assert!(matches!(
            services
                .execution_supervisor()
                .notify_graph(&graph_id)
                .await,
            Ok(())
        ));
        assert!(matches!(
            services
                .execution_supervisor()
                .wait_for_quiescence(&graph_id)
                .await,
            Err(super::super::graph::ExecutionRunnerError::Driver(_))
        ));
        assert!(matches!(
            services.recover_execution_graphs_on_startup().await,
            Err(RuntimeServicesError::UpgradeRecoveryRequired)
        ));
        assert!(matches!(
            services
                .execution_supervisor()
                .command_graph(
                    &graph_id,
                    harness_contract::execution_graph::ExecutionGraphCommand::Advance {
                        expected_revision: 0,
                    },
                )
                .await,
            Err(super::super::graph::ExecutionRunnerError::MutationBlocked(
                _
            ))
        ));
    }

    #[tokio::test]
    async fn startup_recovery_rehydrates_and_advances_persistent_execution_graphs() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let cowd_home = temp.path().join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&cowd_home).unwrap();

        let graph_id = {
            let services = RuntimeServices::builder(&cowd_home, &workspace)
                .build()
                .unwrap();
            let mut graph = harness_contract::execution_graph::ExecutionGraph::new(
                "startup recovery production path",
            );
            let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
                harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
                "tool_batch",
                "payload:startup-recovery",
            );
            node.id = "startup-node".to_string();
            node.idempotency_key = "idempotency:startup-node".to_string();
            graph.nodes.push(node);
            let graph = services
                .commit_service()
                .register_graph(graph)
                .unwrap()
                .graph;
            let graph = services
                .commit_service()
                .transition_node(
                    &graph,
                    "startup-node",
                    ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            let graph = services
                .commit_service()
                .transition_node(
                    &graph,
                    "startup-node",
                    ExecutionNodeStatus::Running,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            graph.id
        };

        let restarted = RuntimeServices::builder(&cowd_home, &workspace)
            .build()
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        restarted
            .tool_batch_executor()
            .install_resolver(Arc::new(ServiceScopedResolver {
                payload_ref: "payload:startup-recovery".to_string(),
                backend: Arc::new(ServiceScopedBackend {
                    calls: Arc::clone(&calls),
                }),
            }));

        let report = restarted
            .recover_execution_graphs_on_startup()
            .await
            .expect("startup recovery");
        assert_eq!(report.examined_graphs, 1);
        assert_eq!(report.recovered_graphs, 1);
        assert_eq!(report.notified_graphs, 1);
        assert!(report.errors.is_empty());
        restarted
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await
            .expect("recovered graph reaches quiescence");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let graph = restarted.graph_state_store().load(&graph_id).unwrap();
        assert_eq!(
            graph.node_statuses["startup-node"],
            ExecutionNodeStatus::Completed
        );
    }

    #[tokio::test]
    async fn canonical_agent_task_flows_through_runner_and_commits_once() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let providers = crate::config::ProvidersConfig {
            providers: std::collections::HashMap::from([(
                "test".into(),
                crate::config::ProviderConfig {
                    name: "test".into(),
                    base_url: "https://example.test/v1".into(),
                    api_key: "test".into(),
                    models: vec!["fast".into()],
                    protocol: Some("responses".into()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        };
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
            .build()
            .unwrap();
        services
            .agent_runtime()
            .register_backend(Arc::new(CompletedAgentBackend));

        let mut graph = ExecutionGraph::new("agent graph integration");
        graph.id = "agent-runtime-graph".into();
        let intent = AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: None,
            granted_capabilities: Vec::new(),
            principal_id: "test".to_string(),
            source_turn_id: "agent-runtime-turn".to_string(),
            run_id: "agent-runtime-run".into(),
            task_id: "agent-runtime-task".into(),
            session_id: "agent-runtime-session".into(),
            mission_id: services.mission_runtime().default_mission_id().to_string(),
            team_id: None,
            graph_id: graph.id.clone(),
            node_id: "agent-runtime-node".into(),
            attempt: 1,
            expected_graph_revision: 0,
            objective: "complete one graph-owned agent task".into(),
            acceptance: vec!["completed".into()],
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".into(),
            model_lease: "fast".into(),
            budget_lease: ContextBudgetLeaseRef::new(
                "agent-runtime-budget",
                "agent-runtime-agent",
                "agent",
                1000,
                1,
            ),
            managed_invocation: None,
            idempotency_key: "agent-runtime-idempotency".into(),
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
            serde_json::to_string(&intent).unwrap(),
        );
        node.id = intent.node_id.clone();
        node.idempotency_key = intent.idempotency_key.clone();
        node.acceptance.criteria = intent.acceptance.clone();
        graph.nodes.push(node);

        let graph = services.compile_graph_agent_intents(graph).unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();

        let (_, report) = services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("run graph");
        assert_eq!(report.completed, 1);
        let graph = services.graph_state_store().load(&report.graph_id).unwrap();
        assert_eq!(
            graph.node_statuses.get(packet.node_id()),
            Some(&ExecutionNodeStatus::Completed)
        );
        let agent = services
            .agent_runtime()
            .get(packet.agent_id())
            .expect("agent projection");
        assert_eq!(
            agent.status,
            harness_contract::agent::AgentStatus::Completed
        );
        let binding = agent.binding.expect("prepared Agent Binding is durable");
        assert_eq!(
            binding.definition_ref.definition_id.as_str(),
            "builtin/cowd/direct"
        );
        assert_eq!(binding.data_lease.session_id, packet.session_id());
        assert_eq!(binding.data_lease.task_id, packet.task_id());
        assert_eq!(services.agent_runtime().events(packet.agent_id()).len(), 3);
    }

    #[tokio::test]
    async fn one_definition_can_drive_eight_isolated_runtime_instances() {
        let temp = tempfile::tempdir().expect("temporary root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let providers = crate::config::ProvidersConfig {
            providers: std::collections::HashMap::from([(
                "test".into(),
                crate::config::ProviderConfig {
                    name: "test".into(),
                    base_url: "https://example.test/v1".into(),
                    api_key: "test".into(),
                    models: vec!["fast".into()],
                    protocol: Some("responses".into()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        };
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .provider_registry(Arc::new(
                crate::ProviderRegistry::new(providers).expect("provider"),
            ))
            .build()
            .expect("runtime services");
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        services
            .agent_runtime()
            .register_backend(Arc::new(ParallelTrackingAgentBackend {
                active: Arc::clone(&active),
                max_active: Arc::clone(&max_active),
            }));

        let mut graph = ExecutionGraph::new("eight independent evidence reads");
        graph.id = "binding-eight-instances".to_string();
        for index in 0..8_u8 {
            let agent_id = format!("researcher-slot-{index}");
            let node_id = format!("binding-agent-node-{index}");
            let intent = AgentTaskIntent {
                selected_agent_id: Some("builtin/cowd/direct".to_string()),
                definition_ref: None,
                granted_capabilities: Vec::new(),
                principal_id: "test".to_string(),
                source_turn_id: format!("binding-turn-{index}"),
                run_id: format!("binding-run-{index}"),
                task_id: format!("binding-task-{index}"),
                session_id: "binding-session".to_string(),
                mission_id: services.mission_runtime().default_mission_id().to_string(),
                team_id: Some("binding-team".to_string()),
                graph_id: graph.id.clone(),
                node_id: node_id.clone(),
                attempt: 1,
                expected_graph_revision: 0,
                objective: format!("research isolated domain {index}"),
                acceptance: vec!["evidence".to_string()],
                constraints: vec![
                    format!("role_slot:researcher-{index}"),
                    format!(
                        "team_acceptance_contract:{}",
                        serde_json::to_string(&vec![
                            harness_contract::team::TeamAcceptanceRequirement {
                                criterion: "evidence".to_string(),
                                check:
                                    harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                                        scopes: vec![format!("read:binding-domain-{index}")],
                                    },
                            },
                        ])
                        .expect("team acceptance contract")
                    ),
                ],
                context_refs: Vec::new(),
                evidence_refs: Vec::new(),
                resource_scopes: vec![format!("read:binding-domain-{index}")],
                allowed_tools: vec!["read_file".to_string()],
                allowed_skills: Vec::new(),
                permission_lease: "read_only".to_string(),
                model_lease: "fast".to_string(),
                budget_lease: ContextBudgetLeaseRef::new(
                    format!("binding-budget-{index}"),
                    agent_id,
                    "agent",
                    2_000,
                    1,
                ),
                managed_invocation: None,
                idempotency_key: format!("binding-agent-{index}"),
            };
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::AgentTask,
                crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
                serde_json::to_string(&intent).expect("intent"),
            );
            node.id = node_id;
            node.idempotency_key = intent.idempotency_key;
            node.acceptance.criteria = intent.acceptance;
            graph.nodes.push(node);
        }

        let graph = services
            .compile_graph_agent_intents(graph)
            .expect("bind graph");
        assert!(graph.nodes.iter().all(|node| {
            serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                .ok()
                .is_some_and(|packet| {
                    packet
                        .constraints
                        .iter()
                        .any(|constraint| constraint.starts_with("team_acceptance_contract:"))
                })
        }));

        let (_, report) = services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("run graph");
        assert_eq!(report.completed, 8, "parallel agent report: {report:?}");
        let snapshots = services
            .agent_runtime()
            .list()
            .into_iter()
            .filter(|snapshot| snapshot.session_id == "binding-session")
            .collect::<Vec<_>>();
        assert_eq!(snapshots.len(), 8);
        let bindings = snapshots
            .iter()
            .map(|snapshot| snapshot.binding.as_ref().expect("durable binding"))
            .collect::<Vec<_>>();
        assert!(bindings.iter().all(|binding| {
            binding.definition_ref.definition_id.as_str() == "builtin/cowd/direct"
                && binding.definition_ref.revision == 1
                && binding.data_lease.team_id.as_deref() == Some("binding-team")
        }));
        let instances = bindings
            .iter()
            .map(|binding| binding.instance.instance_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(instances.len(), 8);
        assert!(max_active.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn team_runtime_compiles_parallel_agents_and_emits_one_verified_terminal_result() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let providers = crate::config::ProvidersConfig {
            providers: std::collections::HashMap::from([(
                "test".into(),
                crate::config::ProviderConfig {
                    name: "test".into(),
                    base_url: "https://example.test/v1".into(),
                    api_key: "test".into(),
                    models: vec!["fast".into()],
                    protocol: Some("responses".into()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        };
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
            .build()
            .unwrap();
        services
            .agent_runtime()
            .register_backend(Arc::new(CompletedAgentBackend));

        let projection = services
            .team_runtime()
            .instantiate(team_request(
                "team-runtime-integration",
                "team-runtime-session",
                "cowd/execute-review",
                "independently analyse and review the runtime boundary",
                "fast",
                services.mission_runtime().default_mission_id(),
            ))
            .await
            .expect("team execution");

        assert_eq!(projection.status, "completed");
        assert_eq!(projection.tasks.len(), 2);
        let terminal = projection.terminal_result.expect("one terminal result");
        let encoded = terminal
            .result_ref
            .strip_prefix("assistant_json:")
            .expect("terminal team result carries the synthesized answer");
        let final_answer = serde_json::from_str::<String>(encoded).unwrap();
        assert!(final_answer.contains("verified agent result"));
        let graph = services
            .graph_state_store()
            .load(&projection.graph_id)
            .expect("canonical graph");
        assert!(graph
            .node_statuses
            .values()
            .all(|status| *status == ExecutionNodeStatus::Completed));
        let team_bindings = graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .map(|node| {
                serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                    .expect("canonical AgentTaskPacket")
                    .binding
                    .expect("Team graph payload contains exact Binding")
            })
            .collect::<Vec<_>>();
        assert_eq!(team_bindings.len(), 2);
        assert_eq!(
            team_bindings
                .iter()
                .map(|binding| binding.definition_ref.definition_id.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["builtin/cowd/direct", "builtin/cowd/execute"])
        );
        assert!(team_bindings.iter().all(|binding| {
            binding.data_lease.team_id.as_deref() == Some("team-runtime-integration")
        }));
        assert!(services.team_runtime().projection_json()["teams"]
            .as_array()
            .is_some_and(|teams| teams.len() == 1));
    }

    #[tokio::test]
    async fn fanout_team_uses_runner_parallelism_without_a_team_scheduler() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let providers = crate::config::ProvidersConfig {
            providers: std::collections::HashMap::from([(
                "test".into(),
                crate::config::ProviderConfig {
                    name: "test".into(),
                    base_url: "https://example.test/v1".into(),
                    api_key: "test".into(),
                    models: vec!["fast".into()],
                    protocol: Some("responses".into()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        };
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
            .build()
            .unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        services
            .agent_runtime()
            .register_backend(Arc::new(ParallelTrackingAgentBackend {
                active: Arc::clone(&active),
                max_active: Arc::clone(&max_active),
            }));
        let projection = services
            .team_runtime()
            .instantiate(TeamInstantiationRequest {
                cardinality_overrides: vec![TeamRoleCardinalityOverride {
                    role_id: "researcher".to_string(),
                    cardinality: RoleCardinalityPolicy::Fixed { count: 3 },
                }],
                focus_partition_plans: vec![harness_contract::team::FocusPartitionPlan {
                    role_id: "researcher".to_string(),
                    shared_baseline: vec!["compare the same architecture constraints".to_string()],
                    slots: vec![
                        harness_contract::team::FocusPartitionSlot {
                            focus_id: "architecture-a".to_string(),
                            boundary: "only architecture-a".to_string(),
                            evidence_responsibility: "source evidence for architecture-a"
                                .to_string(),
                            capability_cropped_refs: vec!["read:architecture-a".to_string()],
                            scope_hash: harness_contract::team::focus_scope_hash(
                                "researcher",
                                "only architecture-a",
                                &["read:architecture-a".to_string()],
                            ),
                            overlap_budget_bp: 0,
                            novelty_target_bp: 2_500,
                            output_contract: vec!["findings".to_string(), "evidence".to_string()],
                            output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
                        },
                        harness_contract::team::FocusPartitionSlot {
                            focus_id: "architecture-b".to_string(),
                            boundary: "only architecture-b".to_string(),
                            evidence_responsibility: "source evidence for architecture-b"
                                .to_string(),
                            capability_cropped_refs: vec!["read:architecture-b".to_string()],
                            scope_hash: harness_contract::team::focus_scope_hash(
                                "researcher",
                                "only architecture-b",
                                &["read:architecture-b".to_string()],
                            ),
                            overlap_budget_bp: 0,
                            novelty_target_bp: 2_500,
                            output_contract: vec!["findings".to_string(), "evidence".to_string()],
                            output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
                        },
                        harness_contract::team::FocusPartitionSlot {
                            focus_id: "architecture-c".to_string(),
                            boundary: "only architecture-c".to_string(),
                            evidence_responsibility: "source evidence for architecture-c"
                                .to_string(),
                            capability_cropped_refs: vec!["read:architecture-c".to_string()],
                            scope_hash: harness_contract::team::focus_scope_hash(
                                "researcher",
                                "only architecture-c",
                                &["read:architecture-c".to_string()],
                            ),
                            overlap_budget_bp: 0,
                            novelty_target_bp: 2_500,
                            output_contract: vec!["findings".to_string(), "evidence".to_string()],
                            output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
                        },
                    ],
                }],
                resource_scopes: vec![
                    "read:architecture-a".to_string(),
                    "read:architecture-b".to_string(),
                    "read:architecture-c".to_string(),
                ],
                ..team_request(
                    "team-runtime-fanout",
                    "team-runtime-session",
                    "cowd/parallel-research-synthesis",
                    "compare three independent architecture choices",
                    "fast",
                    services.mission_runtime().default_mission_id(),
                )
            })
            .await
            .expect("fanout team execution");
        assert_eq!(projection.status, "completed");
        assert!(max_active.load(Ordering::SeqCst) >= 2);
        assert!(max_active.load(Ordering::SeqCst) <= 3);
    }

    fn team_request(
        team_id: &str,
        session_id: &str,
        template_id: &str,
        objective: &str,
        model_lease: &str,
        mission_id: &str,
    ) -> TeamInstantiationRequest {
        TeamInstantiationRequest {
            request_id: format!("test-request-{team_id}"),
            team_id: team_id.to_string(),
            session_id: session_id.to_string(),
            mission_id: mission_id.to_string(),
            parent_execution: None,
            selection_mode: TeamSelectionMode::Explicit,
            strategy_binding: None,
            template_selector: TeamTemplateSelector::LatestStable {
                template_id: TeamTemplateDefinitionId::new(DefinitionScope::Builtin, template_id)
                    .expect("builtin Team template id"),
            },
            objective: objective.to_string(),
            acceptance: vec!["summary".to_string(), "evidence".to_string()],
            risk: None,
            role_binding_overrides: Vec::new(),
            cardinality_overrides: Vec::new(),
            focus_partition_plans: Vec::new(),
            permission_lease: if template_id == "cowd/execute-review" {
                "workspace-write".to_string()
            } else {
                "read_only".to_string()
            },
            model_lease: model_lease.to_string(),
            budget_lease: None,
            managed_invocation: None,
            resource_scopes: vec![if template_id == "cowd/execute-review" {
                "write:crates/runtime".to_string()
            } else {
                "read:crates/runtime".to_string()
            }],
        }
    }

    #[test]
    fn services_builder_imports_and_retires_unbound_legacy_team_state() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let legacy_path = temp
            .path()
            .join("agents")
            .join("team-runtime")
            .join("state.json");
        std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(
            &legacy_path,
            r#"{"runs":{"legacy":{"snapshot":{"team_id":"legacy","status":"running"}}}}"#,
        )
        .unwrap();

        let services = RuntimeServices::builder(temp.path(), &workspace)
            .build()
            .unwrap();
        assert!(!legacy_path.exists());
        let imported = services
            .event_store()
            .all_events(20)
            .unwrap()
            .into_iter()
            .find(|event| event.kind == "team.legacy_imported")
            .expect("legacy team audit event");
        assert_eq!(imported.status.as_deref(), Some("blocked"));
        assert_eq!(imported.payload["team_id"], "legacy");
        assert_eq!(imported.payload["disposition"], "blocked_unbound");
    }

    #[test]
    fn runtime_timeline_position_never_skips_events_inside_one_transaction() {
        let store =
            Arc::new(crate::RuntimeEventStore::try_open_in_memory().expect("runtime event store"));
        store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: "timeline-transaction".to_string(),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: "timeline-session".to_string(),
                    expected_revision: 0,
                }],
                events: vec![
                    crate::RuntimeEventInput {
                        stream_id: "timeline-session".to_string(),
                        scope: crate::RuntimeEventScope::SessionInput,
                        kind: "timeline.first".to_string(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::Value::Null,
                    }
                    .into(),
                    crate::RuntimeEventInput {
                        stream_id: "timeline-session".to_string(),
                        scope: crate::RuntimeEventScope::SessionInput,
                        kind: "timeline.second".to_string(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::Value::Null,
                    }
                    .into(),
                ],
            })
            .expect("transaction commits");
        let reader = super::RuntimeEventReader { store };

        let first = reader
            .session_timeline_events("timeline-session", None, 1)
            .expect("first page");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].transaction_index, 0);
        let second = reader
            .session_timeline_events(
                "timeline-session",
                Some((first[0].commit_cursor, first[0].transaction_index)),
                1,
            )
            .expect("second page");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].transaction_index, 1);
        assert_eq!(second[0].kind, "timeline.second");
    }
}
