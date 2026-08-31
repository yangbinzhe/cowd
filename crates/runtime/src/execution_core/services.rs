//! Workspace-owned runtime service graph.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{FutureExt, StreamExt};
use harness_contract::agent::{
    AgentEvaluationBinding, AgentReleaseBinding, AgentTaskIntent, AgentTaskPacket,
    AgentTerminalStatus, ReleaseChannel, RevisionSelector,
};
use harness_contract::context::ChildExecutionBudgetReservation;
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
    aggregate_team_leaf_usage,
    executors::{
        AgentTaskExecutor, ApprovalNodeExecutor, CompileTargetGuardExecutor,
        MaterializeNodeExecutor, ScopedNodeExecutor, SynthesizeNodeExecutor, TeamSubgraphExecutor,
        VerifyNodeExecutor,
    },
    ExecutionCommitService, ExecutionGraphRunner, ExecutionGraphStateStore, ExecutionRecoveryError,
    ExecutionResourceKind, ExecutionResourceManager, ExecutionRunnerError, ExecutionServiceClass,
    ExecutionStateStoreError, NodeExecutor, NodeExecutorError, NodeExecutorRegistry,
    ResourceAdmissionDecision, ResourceAdmissionRequest, ResourceObservation, ResourceQuota,
    ResourceResultClass, ScopeLockError, ScopeLockManager, WorktreeLeaseError,
    WorktreeLeaseManager,
};
use super::protocols::ProtocolResultReducer;
use crate::agent::binding::request_for_intent;
use crate::agent::definition::ExplicitTomlAgentImport;
use crate::managed_agent::ManagedAgentRestartDisposition;
use crate::runtime_event_store::RuntimeEventStoreError;
#[cfg(feature = "test-fixtures")]
use crate::RuntimeEventInput;
use crate::{
    AgentBindingCompiler, AgentBindingRequest, AgentDefinitionDraftReceipt, AgentRuntime,
    AgentRuntimeResolver, ApprovalConfig, ApprovalCoordinator, ApprovalQueue, CompiledAgentBinding,
    ConflictArbiter, DefinitionRegistryError, DurableRuntimeEvent, ExecutionGraphHost,
    InProcessAgentWorker, ManagedAgentRuntimeDispatchReport, MissionEvidenceBus, MissionRuntime,
    MissionScheduleStore, ProcessJsonlAdapter, RealityRecallPort, RuntimeDefinitionRegistry,
    RuntimeEventReplayer, RuntimeEventScope, RuntimeEventStore, RuntimeSessionOutboxFailureClass,
    RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord, SessionInputRouter,
    SessionRelationGraph, TeamResultReducer, TeamRuntime,
};

#[path = "composition.rs"]
mod composition;
use composition::normalize_provider_fallbacks;

#[path = "lifecycle_services.rs"]
mod lifecycle_services;

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
    #[error("session integration requires query, ingress, journal and application ports together")]
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

#[cfg(test)]
#[path = "tests/services.rs"]
mod tests;

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
    runtime_build_identity: harness_contract::outcome::RuntimeBuildIdentity,
    runtime_event_store: Option<Arc<RuntimeEventStore>>,
    task_aggregate_service: Option<Arc<crate::TaskAggregateService>>,
    builtin_definitions_root: Option<PathBuf>,
    resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
    provider_resource_config: crate::ProviderResourceConfig,
    provider_registry: Arc<crate::ProviderRegistry>,
    provider_fallbacks: Vec<String>,
    provider_transport_pool: Arc<crate::ProviderTransportPool>,
    provider_template_cache: Arc<crate::ProviderClientTemplateCache>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    session_query_port: Option<Arc<dyn crate::SessionRuntimeQueryPort>>,
    session_ingress_port: Option<Arc<dyn crate::SessionRuntimeIngressPort>>,
    session_journal_port: Option<Arc<dyn crate::SessionRuntimeJournalPort>>,
    session_application_port: Option<Arc<dyn crate::SessionRuntimeApplicationPort>>,
    artifact_store: Option<Arc<crate::ArtifactStore>>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    reality_recall_port: Option<Arc<RealityRecallPort>>,
    knowledge_activation: Option<crate::knowledge_activation::KnowledgeActivationRuntime>,
    evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
    skill_catalog: crate::RuntimeSkillCatalog,
    skill_revision_pointer_cache: Option<Arc<crate::SkillRevisionPointerCache>>,
    mission_schedule_policy: crate::MissionSchedulePolicy,
    hot_state_config: crate::execution_core::hot_state::HotStateConfig,
    approval_config: ApprovalConfig,
    collaboration_capacity: crate::CollaborationCapacityPolicy,
    collaboration_max_parallel_agents: usize,
    projection_lanes: Vec<crate::RuntimeProjectionLane>,
}

/// Runtime-owned supervisor for non-critical-path maintenance.
///
/// Tasks are serialized per logical owner, retained until completion, and
/// drained explicitly during process shutdown. This keeps post-turn work off
/// the response path without creating detached tasks.
type MaintenanceWork = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

// Maintenance is intentionally off the response-critical path, but it still
// needs a hard memory bound. Backpressure preserves every completed turn's
// work instead of dropping or coalescing semantically distinct memory updates.
const MAX_MAINTENANCE_OWNERS: usize = 1_024;
const MAX_QUEUED_MAINTENANCE_PER_OWNER: usize = 64;

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
    completion_tx: tokio::sync::mpsc::Sender<MaintenanceCompletion>,
    completion_rx: Mutex<Option<tokio::sync::mpsc::Receiver<MaintenanceCompletion>>>,
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
        let (completion_tx, completion_rx) = tokio::sync::mpsc::channel(MAX_MAINTENANCE_OWNERS);
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
        let mut pending_work: Option<MaintenanceWork> = Some(Box::pin(work));
        loop {
            let changed = self.changed.notified();
            let should_wait = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.lifecycle != MaintenanceLifecycle::Open {
                    return false;
                }

                if let Some(existing) = state.owners.get_mut(&owner) {
                    if existing.queued.len() < MAX_QUEUED_MAINTENANCE_PER_OWNER {
                        existing
                            .queued
                            .push_back(pending_work.take().expect("maintenance work is pending"));
                        return true;
                    }
                    true
                } else if state.owners.len() >= MAX_MAINTENANCE_OWNERS {
                    true
                } else {
                    state.next_generation = state.next_generation.saturating_add(1);
                    let generation = state.next_generation;
                    let worker_state = Arc::downgrade(&self.state);
                    let worker_changed = Arc::clone(&self.changed);
                    let completion_tx = self.completion_tx.clone();
                    let worker_owner = owner.clone();
                    let initial_work = pending_work.take().expect("maintenance work is pending");
                    let (start_tx, start_rx) = tokio::sync::oneshot::channel();
                    let handle = tokio::spawn(async move {
                        if start_rx.await.is_err() {
                            return;
                        }
                        let mut current = initial_work;
                        loop {
                            let _ = std::panic::AssertUnwindSafe(current).catch_unwind().await;
                            let Some(state) = worker_state.upgrade() else {
                                return;
                            };
                            let (next, completed) = {
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
                                    (Some(next), None)
                                } else {
                                    let completed = state.owners.remove(&worker_owner).expect(
                                        "maintenance owner exists while its worker is running",
                                    );
                                    state.reaping = state.reaping.saturating_add(1);
                                    (None, Some(completed))
                                }
                            };
                            worker_changed.notify_waiters();
                            if let Some(completed) = completed {
                                if completion_tx
                                    .send(MaintenanceCompletion { owner: completed })
                                    .await
                                    .is_err()
                                {
                                    tracing::debug!(
                                        owner = %worker_owner,
                                        "maintenance completion reaper is closed"
                                    );
                                }
                                return;
                            }
                            match next {
                                Some(next) => current = next,
                                None => return,
                            }
                        }
                    });
                    state.owners.insert(
                        owner.clone(),
                        MaintenanceOwner {
                            generation,
                            queued: VecDeque::new(),
                            handle,
                        },
                    );
                    let _ = start_tx.send(());
                    return true;
                }
            };
            if should_wait {
                changed.await;
            }
        }
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
        // Wake submissions currently applying bounded-queue backpressure so
        // they observe Closing and return instead of waiting through shutdown.
        self.changed.notify_waiters();

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
    #[must_use]
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.store.subscribe_commits()
    }

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

    pub fn suppress(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        reason: &str,
        now_ms: u64,
    ) -> Result<RuntimeSessionOutboxRecord, RuntimeEventStoreError> {
        self.store.suppress_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            reason,
            now_ms,
        )
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

pub struct RuntimeServices {
    workspace_root: PathBuf,
    workspace_key: String,
    path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    event_store: Arc<RuntimeEventStore>,
    live_execution_store: Arc<crate::execution_live::ExecutionLiveStore>,
    hot_state: Arc<crate::execution_core::hot_state::RuntimeHotStatePlane>,
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
    approval_coordinator: Arc<ApprovalCoordinator>,
    execution_capacity_profile: Arc<crate::ExecutionCapacityProfile>,
    evolution_governance: Arc<crate::EvolutionGovernanceService>,
    evolution_discovery: Arc<crate::evolution::EvolutionDiscoveryService>,
    evolution_analyst: Arc<crate::evolution::analyst::EvolutionAnalystService>,
    evolution_signal_projector: Arc<crate::evolution::EvolutionSignalProjector>,
    skill_maintenance_projector: Arc<crate::SkillMaintenanceProjector>,
    event_reactor: Arc<crate::RuntimeEventReactor>,
    skill_revision_governance: Arc<crate::SkillRevisionGovernanceService>,
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
    provider_resource_config: Arc<RwLock<crate::ProviderResourceConfig>>,
    provider_fallbacks: Arc<RwLock<Vec<String>>>,
    provider_transport_pool: Arc<crate::ProviderTransportPool>,
    provider_template_cache: Arc<crate::ProviderClientTemplateCache>,
    evaluation_provider_token_leases:
        Arc<crate::conversation::EvaluationProviderTokenLeaseRegistry>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    artifact_store: Arc<crate::ArtifactStore>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
    evolution_evaluation_flights: Arc<Mutex<BTreeSet<String>>>,
    skill_catalog: Arc<RwLock<crate::RuntimeSkillCatalog>>,
    reality_recall_port: Arc<RealityRecallPort>,
    knowledge_activation: crate::knowledge_activation::KnowledgeActivationRuntime,
    session_dispatch_executor: Arc<crate::session_execution::SessionDispatchNodeExecutor>,
    session_input_router: OnceLock<Arc<SessionInputRouter>>,
    session_query_port: OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>,
    session_ingress_port: OnceLock<Arc<dyn crate::SessionRuntimeIngressPort>>,
    session_journal_port: OnceLock<Arc<dyn crate::SessionRuntimeJournalPort>>,
    session_application_port: OnceLock<Arc<dyn crate::SessionRuntimeApplicationPort>>,
    active_execution_buses: Arc<Mutex<BTreeMap<String, ActiveExecutionBus>>>,
    session_execution_policy_controls:
        Arc<RwLock<BTreeMap<String, crate::permissions::SessionExecutionPolicyControl>>>,
    session_execution_policy_admission_blocks: Arc<RwLock<BTreeMap<String, String>>>,
    next_execution_bus_generation: AtomicU64,
    maintenance_supervisor: Arc<RuntimeMaintenanceSupervisor>,
    resource_evidence_writer: Arc<super::evidence_writer::ResourceEvidenceWriter>,
    execution_projection_cache: Mutex<crate::execution_projection::ExecutionProjectionCache>,
    // Keep this field last so filesystem-backed components are dropped before
    // the temporary root removes their files.
    _ephemeral_root: Option<tempfile::TempDir>,
}

#[derive(Clone)]
struct ActiveExecutionBus {
    generation: u64,
    bus: crate::CowdEventBus,
}

/// Process-local fail-fast single-flight for paid candidate evaluation. It
/// never queues behind another evaluation, so duplicate UI/API requests do
/// not consume another provider budget or delay foreground execution.
struct EvolutionEvaluationFlight {
    candidate_id: String,
    active: Arc<Mutex<BTreeSet<String>>>,
}

impl EvolutionEvaluationFlight {
    fn try_acquire(
        active: Arc<Mutex<BTreeSet<String>>>,
        candidate_id: &str,
    ) -> Result<Self, RuntimeServicesError> {
        let inserted = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(candidate_id.to_string());
        if !inserted {
            return Err(RuntimeServicesError::Invariant(
                "evolution_evaluation_in_progress".to_string(),
            ));
        }
        Ok(Self {
            candidate_id: candidate_id.to_string(),
            active,
        })
    }
}

impl Drop for EvolutionEvaluationFlight {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.candidate_id);
    }
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
            runtime_build_identity:
                harness_contract::outcome::RuntimeBuildIdentity::unresolved_development(env!(
                    "CARGO_PKG_VERSION"
                )),
            runtime_event_store: None,
            task_aggregate_service: None,
            builtin_definitions_root: None,
            resource_quotas: default_resource_quotas(),
            provider_resource_config: crate::ProviderResourceConfig::default(),
            provider_registry: Arc::new(crate::ProviderRegistry::empty()),
            provider_fallbacks: Vec::new(),
            provider_transport_pool: Arc::new(crate::ProviderTransportPool::default()),
            provider_template_cache: Arc::new(crate::ProviderClientTemplateCache::default()),
            tool_execution_host: None,
            session_query_port: None,
            session_ingress_port: None,
            session_journal_port: None,
            session_application_port: None,
            artifact_store: None,
            memory_manager: None,
            reality_recall_port: None,
            knowledge_activation: None,
            evolution_eval_runner: None,
            skill_catalog: crate::RuntimeSkillCatalog::default(),
            skill_revision_pointer_cache: None,
            mission_schedule_policy: crate::MissionSchedulePolicy::default(),
            hot_state_config: crate::execution_core::hot_state::HotStateConfig::default(),
            approval_config: ApprovalConfig::default(),
            collaboration_capacity: crate::CollaborationCapacityPolicy::default(),
            collaboration_max_parallel_agents: crate::AgentControlPolicy::default()
                .max_parallel_agents,
            projection_lanes: Vec::new(),
        }
    }

    pub fn in_memory() -> Result<Arc<Self>, RuntimeServicesError> {
        let workspace_key = format!("in-memory-{}", uuid::Uuid::new_v4());
        let ephemeral_root = tempfile::Builder::new()
            .prefix("cowd-runtime-services-")
            .tempdir()?;
        let workspace_root = ephemeral_root.path().join(&workspace_key);
        std::fs::create_dir_all(&workspace_root)?;
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
            harness_contract::outcome::RuntimeBuildIdentity::unresolved_development(env!(
                "CARGO_PKG_VERSION"
            )),
            Arc::new(WorktreeLeaseManager::open(
                ephemeral_root.path().join("worktree-leases.json"),
            )?),
            Arc::new(ScopeLockManager::new()),
            default_resource_quotas(),
            crate::ProviderResourceConfig::default(),
            Arc::new(crate::ProviderRegistry::empty()),
            Vec::new(),
            Arc::new(crate::ProviderTransportPool::default()),
            Arc::new(crate::ProviderClientTemplateCache::default()),
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
            None,
            crate::MissionSchedulePolicy::default(),
            crate::execution_core::hot_state::HotStateConfig::default(),
            ApprovalConfig::default(),
            crate::CollaborationCapacityPolicy::default(),
            crate::AgentControlPolicy::default().max_parallel_agents,
            definition_registry,
            task_aggregate_service,
            Vec::new(),
            Some(ephemeral_root),
        )?);
        services.agent_runtime.bind_services(Arc::clone(&services));
        services
            .agent_runtime
            .register_observation_authority_backend(Arc::new(InProcessAgentWorker::new(
                Arc::downgrade(&services),
            )));
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
        services
            .event_reactor
            .start()
            .map_err(RuntimeServicesError::Invariant)?;
        Ok(services)
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        cowd_home: PathBuf,
        workspace_root: PathBuf,
        workspace_key: String,
        event_store: Arc<RuntimeEventStore>,
        runtime_build_identity: harness_contract::outcome::RuntimeBuildIdentity,
        worktree_leases: Arc<WorktreeLeaseManager>,
        scope_locks: Arc<ScopeLockManager>,
        mut resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
        provider_resource_config: crate::ProviderResourceConfig,
        provider_registry: Arc<crate::ProviderRegistry>,
        provider_fallbacks: Vec<String>,
        provider_transport_pool: Arc<crate::ProviderTransportPool>,
        provider_template_cache: Arc<crate::ProviderClientTemplateCache>,
        tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
        artifact_store: Arc<crate::ArtifactStore>,
        memory_manager: Option<Arc<memory::CognitiveContextManager>>,
        reality_recall_port: Option<Arc<RealityRecallPort>>,
        knowledge_activation: Option<crate::knowledge_activation::KnowledgeActivationRuntime>,
        evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
        skill_catalog: crate::RuntimeSkillCatalog,
        skill_revision_pointer_cache: Option<Arc<crate::SkillRevisionPointerCache>>,
        mission_schedule_policy: crate::MissionSchedulePolicy,
        hot_state_config: crate::execution_core::hot_state::HotStateConfig,
        approval_config: ApprovalConfig,
        collaboration_capacity: crate::CollaborationCapacityPolicy,
        collaboration_max_parallel_agents: usize,
        definition_registry: Arc<RuntimeDefinitionRegistry>,
        task_aggregate_service: Arc<crate::TaskAggregateService>,
        mut projection_lanes: Vec<crate::RuntimeProjectionLane>,
        ephemeral_root: Option<tempfile::TempDir>,
    ) -> Result<Self, RuntimeServicesError> {
        let assembly_started_at = Instant::now();
        let path_identity_resolver = Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(&workspace_root)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?,
        );
        let executor_registry = Arc::new(NodeExecutorRegistry::new());
        let hot_state = Arc::new(crate::execution_core::hot_state::RuntimeHotStatePlane::new(
            hot_state_config,
        ));
        let graph_state_store = ExecutionGraphStateStore::with_hot_state(
            Arc::clone(&event_store),
            Arc::clone(&hot_state),
        );
        let model_step_executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
        let tool_batch_executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
        let cross_plane_connector_executor =
            Arc::new(ScopedNodeExecutor::new("cross_plane_connector"));
        let agent_task_executor =
            Arc::new(AgentTaskExecutor::new().with_state_store(graph_state_store.clone()));
        agent_task_executor
            .bind_path_identity_resolver(Arc::clone(&path_identity_resolver))
            .map_err(RuntimeServicesError::Invariant)?;
        let agent_runtime = Arc::new(AgentRuntime::new(
            Arc::clone(&event_store),
            Arc::clone(&provider_registry),
        ));
        agent_runtime
            .catalog()
            .replace_all(definition_registry.runnable_agent_catalog()?);
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime Agent graph assembled"
        );
        agent_task_executor.install_resolver(Arc::new(AgentRuntimeResolver::new(Arc::clone(
            &agent_runtime,
        ))));
        let verify_executor = Arc::new(VerifyNodeExecutor::new(graph_state_store.clone()));
        let materialize_executor = Arc::new(MaterializeNodeExecutor::new(
            graph_state_store.clone(),
            workspace_root.clone(),
        ));
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
        let session_execution_policy_controls = Arc::new(RwLock::new(BTreeMap::<
            String,
            crate::permissions::SessionExecutionPolicyControl,
        >::new()));
        let session_execution_policy_admission_blocks =
            Arc::new(RwLock::new(BTreeMap::<String, String>::new()));
        let approval_queue = Arc::new(ApprovalQueue::new(Arc::clone(&event_store)));
        let approval_coordinator = Arc::new(ApprovalCoordinator::new(
            Arc::clone(&approval_queue),
            approval_config,
        ));
        let evolution_governance = Arc::new(crate::EvolutionGovernanceService::new(
            Arc::clone(&event_store),
            Arc::clone(&approval_queue),
        ));
        let evolution_discovery = Arc::new(crate::evolution::EvolutionDiscoveryService::new(
            Arc::clone(&event_store),
        ));
        let evolution_analyst = Arc::new(crate::evolution::analyst::EvolutionAnalystService::new(
            Arc::clone(&event_store),
            Arc::clone(&evolution_discovery),
        ));
        let evolution_signal_projector = Arc::new(crate::evolution::EvolutionSignalProjector::new(
            Arc::clone(&event_store),
            Arc::clone(&evolution_discovery),
        ));
        let skill_maintenance_projector = Arc::new(crate::SkillMaintenanceProjector::new(
            Arc::clone(&event_store),
        ));
        let skill_revision_governance =
            Arc::new(crate::SkillRevisionGovernanceService::with_pointer_cache(
                Arc::clone(&event_store),
                Arc::clone(&approval_queue),
                skill_revision_pointer_cache.unwrap_or_default(),
            ));
        let approval_policy_controls = Arc::clone(&session_execution_policy_controls);
        install_builtin_executors(
            &executor_registry,
            vec![
                Arc::clone(&model_step_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&tool_batch_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&cross_plane_connector_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&agent_task_executor) as Arc<dyn NodeExecutor>,
                Arc::new(CompileTargetGuardExecutor) as Arc<dyn NodeExecutor>,
                Arc::new(ApprovalNodeExecutor::with_session_policy_lookup(
                    Arc::clone(&approval_queue),
                    Arc::new(move |session_id| {
                        approval_policy_controls
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get(session_id)
                            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
                    }),
                )),
                Arc::clone(&verify_executor) as Arc<dyn NodeExecutor>,
                materialize_executor as Arc<dyn NodeExecutor>,
                Arc::clone(&synthesize_executor) as Arc<dyn NodeExecutor>,
                Arc::clone(&session_dispatch_executor) as Arc<dyn NodeExecutor>,
            ],
        )?;
        let commit_service = ExecutionCommitService::with_hot_state(
            Arc::clone(&event_store),
            Arc::clone(&hot_state),
        );
        let provider_generation = provider_resource_config.materialize(&provider_registry.pin());
        resource_quotas.retain(|(kind, _)| {
            !matches!(
                kind,
                ExecutionResourceKind::Provider
                    | ExecutionResourceKind::ProviderAccount(_)
                    | ExecutionResourceKind::ProviderModel(_)
                    | ExecutionResourceKind::ProviderTokenPool(_)
            )
        });
        resource_quotas.extend(provider_generation.quotas.iter().cloned());
        let execution_capacity_profile = crate::ExecutionCapacityProfile::resolve(
            &collaboration_capacity,
            collaboration_max_parallel_agents,
        )
        .map_err(RuntimeServicesError::Invariant)?;
        let resource_manager = Arc::new(
            ExecutionResourceManager::with_admission_policy(
                resource_quotas.clone(),
                crate::execution_core::graph::ExecutionAdmissionPolicy {
                    revision: execution_capacity_profile.revision,
                    max_pending_instance: execution_capacity_profile.max_pending_instance,
                    max_pending_per_class: execution_capacity_profile.max_pending_per_class,
                    max_pending_per_key: execution_capacity_profile.max_pending_per_key,
                    aging_interval: std::time::Duration::from_millis(
                        execution_capacity_profile.admission_aging_interval_ms,
                    ),
                },
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?,
        );
        resource_manager
            .reconcile_quotas(resource_quotas, provider_generation.reserves)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let resource_evidence_writer =
            super::evidence_writer::ResourceEvidenceWriter::start(Arc::clone(&event_store));
        let observer_writer = Arc::clone(&resource_evidence_writer);
        resource_manager
            .install_admission_observer(move |observation| {
                observer_writer.try_publish(observation);
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let tool_execution_plane = Arc::new(crate::ToolExecutionPlane::new(
            Arc::clone(&resource_manager),
            Arc::clone(&scope_locks),
        ));
        let graph_runner = Arc::new(ExecutionGraphRunner::new_with_path_identity_resolver(
            Arc::clone(&executor_registry),
            graph_state_store.clone(),
            commit_service.clone(),
            Arc::clone(&resource_manager),
            Arc::clone(&scope_locks),
            Arc::clone(&worktree_leases),
            workspace_key.clone(),
            workspace_root.clone(),
            Arc::clone(&path_identity_resolver),
        ));
        let execution_supervisor = Arc::new(crate::RuntimeExecutionSupervisor::new(graph_runner));
        tool_execution_plane.bind_supervisor(&execution_supervisor);
        let deadline_supervisor = Arc::clone(&execution_supervisor);
        let deadline_approval_coordinator = Arc::clone(&approval_coordinator);
        approval_queue.install_deadline_scheduler(Arc::new(move |approval_id| {
            let supervisor = Arc::clone(&deadline_supervisor);
            let approval_coordinator = Arc::clone(&deadline_approval_coordinator);
            Box::pin(async move {
                approval_coordinator.notify_decision(&approval_id);
                if let Some((graph_id, _)) =
                    crate::execution_core::graph::executors::parse_graph_approval_id(&approval_id)
                {
                    if let Err(error) = supervisor.notify_graph(&graph_id).await {
                        tracing::warn!(graph_id, %error, "approval deadline could not wake graph");
                    }
                }
            })
        }));
        let mission_runtime = Arc::new(
            MissionRuntime::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        let task_runtime_port = crate::TaskRuntimePort::from_components(
            Arc::clone(&task_aggregate_service),
            Arc::clone(&mission_runtime),
            Arc::clone(&event_store),
            graph_state_store.clone(),
            Arc::clone(&session_execution_policy_controls),
            Arc::clone(&session_execution_policy_admission_blocks),
        );
        let team_runtime = Arc::new(TeamRuntime::new(
            Arc::clone(&execution_supervisor),
            graph_state_store.clone(),
            Arc::clone(&agent_runtime),
            Arc::clone(&event_store),
            Arc::clone(&definition_registry),
            Arc::clone(&evolution_governance),
            workspace_key.clone(),
            Arc::clone(&path_identity_resolver),
            task_runtime_port,
            Arc::clone(&mission_runtime),
        ));
        executor_registry.register(Arc::new(TeamSubgraphExecutor::new(
            Arc::downgrade(&team_runtime),
            Arc::downgrade(&execution_supervisor),
        )))?;
        let l4_session_policy_lookup = {
            let controls = Arc::clone(&session_execution_policy_controls);
            Arc::new(move |session_id: &str| {
                controls
                    .read()
                    .ok()
                    .and_then(|map| map.get(session_id).cloned())
                    .map(|control| control.snapshot())
            })
                as Arc<
                    dyn Fn(&str) -> Option<harness_contract::policy::SessionExecutionPolicy>
                        + Send
                        + Sync,
                >
        };
        let l4_promotion_service = Arc::new(crate::L4PromotionService::new(
            Arc::clone(&event_store),
            Arc::clone(&approval_queue),
            memory_manager.clone(),
            Some(l4_session_policy_lookup),
        ));
        let knowledge_candidate_projector = Arc::new(crate::KnowledgeCandidateProjector::new(
            Arc::clone(&event_store),
            Arc::clone(&l4_promotion_service),
        ));
        let outcome_service = Arc::new(crate::execution_core::OutcomeService::with_build_identity(
            Arc::clone(&event_store),
            runtime_build_identity,
        ));
        let outcome_projector = Arc::new(crate::OutcomeProjector::new(Arc::clone(&event_store)));
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime outcome projection assembled"
        );
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
        projection_lanes.extend([
            knowledge_candidate_projector.projection_lane(),
            outcome_projector.projection_lane(),
            mission_evidence.projection_lane(),
            evolution_signal_projector.projection_lane(),
            skill_maintenance_projector.projection_lane(),
        ]);
        projection_lanes.push(child_execution_resolution_lane(
            Arc::clone(&event_store),
            graph_state_store.clone(),
            Arc::clone(&execution_supervisor),
        ));
        let event_reactor = Arc::new(
            crate::RuntimeEventReactor::sealed(Arc::clone(&event_store), projection_lanes)
                .map_err(RuntimeServicesError::Invariant)?,
        );
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
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime mission and Managed Agent projections assembled"
        );
        let managed_projection_store = graph_state_store.clone();
        let managed_projection_dispatcher = Arc::clone(&managed_agents);
        let outcome_projection_store = graph_state_store.clone();
        let settled_outcome_service = Arc::clone(&outcome_service);
        let settled_lineage_supervisor = Arc::clone(&execution_supervisor);
        let settled_team_runtime = Arc::clone(&team_runtime);
        execution_supervisor
            .install_graph_settled_observer(move |graph_id| {
                let graph_id = graph_id.to_string();
                let graph_store = managed_projection_store.clone();
                let dispatcher = Arc::clone(&managed_projection_dispatcher);
                let outcome_store = outcome_projection_store.clone();
                let outcome_service = Arc::clone(&settled_outcome_service);
                let lineage_supervisor = Arc::clone(&settled_lineage_supervisor);
                let coordinator_store = graph_store.clone();
                let coordinator_supervisor = Arc::clone(&lineage_supervisor);
                let coordinator_teams = Arc::clone(&settled_team_runtime);
                tokio::spawn(async move {
                    if let Err(error) = crate::orchestration::collaboration_coordinator::reconcile_program_wait_state_with(
                        &graph_id,
                        coordinator_supervisor.as_ref(),
                        &coordinator_store,
                        coordinator_teams.as_ref(),
                    )
                    .await
                    {
                        tracing::warn!(
                            graph_id,
                            %error,
                            "settled graph could not reconcile CollaborationProgram wait truth"
                        );
                    }
                    if let Err(error) = lineage_supervisor
                        .wake_parent_for_settled_child(&graph_id)
                        .await
                    {
                        tracing::warn!(
                            graph_id,
                            %error,
                            "settled child graph could not wake its durable parent join"
                        );
                    }
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
                    if let Err(error) =
                        project_team_terminal_outcome(outcome_store, outcome_service, &graph_id)
                            .await
                    {
                        tracing::warn!(
                            graph_id,
                            error,
                            "Team terminal Outcome projector could not reduce graph state"
                        );
                    }
                    if let Err(error) = crate::orchestration::collaboration_coordinator::reconcile_terminal_program_with(
                        &graph_id,
                        coordinator_supervisor.as_ref(),
                        &coordinator_store,
                    )
                    .await
                    {
                        tracing::warn!(
                            graph_id,
                            %error,
                            "settled graph could not reconcile CollaborationProgram terminal truth"
                        );
                    }
                });
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let session_relations = Arc::new(
            SessionRelationGraph::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        tracing::info!(
            elapsed_ms = assembly_started_at.elapsed().as_millis() as u64,
            "Runtime Session relation projection assembled"
        );
        let reality_recall_port = reality_recall_port.unwrap_or_else(|| {
            Arc::new(RealityRecallPort::for_config_home_and_workspace(
                &cowd_home,
                &workspace_root,
            ))
        });
        Ok(Self {
            workspace_root,
            workspace_key: workspace_key.clone(),
            path_identity_resolver,
            event_store: Arc::clone(&event_store),
            live_execution_store: Arc::new(
                crate::execution_live::ExecutionLiveStore::with_hot_state(
                    Arc::clone(&event_store),
                    Arc::clone(&hot_state),
                ),
            ),
            hot_state,
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
            approval_coordinator,
            execution_capacity_profile: Arc::new(execution_capacity_profile),
            evolution_governance,
            evolution_discovery,
            evolution_analyst,
            evolution_signal_projector,
            skill_maintenance_projector,
            event_reactor,
            skill_revision_governance,
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
            provider_resource_config: Arc::new(RwLock::new(provider_resource_config)),
            provider_fallbacks: Arc::new(RwLock::new(normalize_provider_fallbacks(
                provider_fallbacks,
            ))),
            provider_transport_pool,
            provider_template_cache,
            evaluation_provider_token_leases: Arc::new(
                crate::conversation::EvaluationProviderTokenLeaseRegistry::default(),
            ),
            tool_execution_host,
            artifact_store,
            memory_manager,
            evolution_eval_runner,
            evolution_evaluation_flights: Arc::new(Mutex::new(BTreeSet::new())),
            skill_catalog: Arc::new(RwLock::new(skill_catalog)),
            reality_recall_port,
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
            session_application_port: OnceLock::new(),
            active_execution_buses: Arc::new(Mutex::new(BTreeMap::new())),
            session_execution_policy_controls,
            session_execution_policy_admission_blocks,
            next_execution_bus_generation: AtomicU64::new(0),
            maintenance_supervisor: Arc::new(RuntimeMaintenanceSupervisor::new()),
            resource_evidence_writer,
            execution_projection_cache: Mutex::new(
                crate::execution_projection::ExecutionProjectionCache::default(),
            ),
            _ephemeral_root: ephemeral_root,
        })
    }

    pub fn install_session_ports(
        self: &Arc<Self>,
        query: Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress: Arc<dyn crate::SessionRuntimeIngressPort>,
        journal: Arc<dyn crate::SessionRuntimeJournalPort>,
        application: Arc<dyn crate::SessionRuntimeApplicationPort>,
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
        self.session_application_port
            .set(application)
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
        let application: Arc<dyn crate::SessionRuntimeApplicationPort> =
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
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
        self.session_application_port
            .set(application)
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        self.session_input_router
            .set(Arc::clone(&router))
            .map_err(|_| RuntimeServicesError::DuplicateSessionRouter)?;
        Ok(router)
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }
    pub fn path_identity_resolver(
        &self,
    ) -> &Arc<crate::path_identity::WorkspacePathIdentityResolver> {
        &self.path_identity_resolver
    }
    pub fn workspace_key(&self) -> &str {
        &self.workspace_key
    }

    pub(crate) fn cached_execution_projection(
        &self,
        key: &crate::execution_projection::ExecutionProjectionCacheKey,
    ) -> Option<harness_contract::projection::ExecutionProjection> {
        self.execution_projection_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
    }

    pub(crate) fn cache_execution_projection(
        &self,
        key: crate::execution_projection::ExecutionProjectionCacheKey,
        projection: harness_contract::projection::ExecutionProjection,
    ) {
        self.execution_projection_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .put(key, projection);
    }

    #[cfg(test)]
    pub(crate) fn execution_projection_cache_stats(&self) -> (u64, u64, usize) {
        self.execution_projection_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stats()
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
        self.approval_queue.shutdown_deadline_scheduler().await;
        let report = self.event_reactor.shutdown().await;
        if !report.timed_out_lanes.is_empty() || !report.join_errors.is_empty() {
            tracing::warn!(
                ?report,
                "Runtime event reactor shutdown was not fully drained"
            );
        }
        self.maintenance_supervisor.shutdown_and_drain().await;
        self.resource_evidence_writer.shutdown_and_drain();
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

    /// Unified operational view for every sealed Runtime event projection.
    pub fn event_reactor_health(
        &self,
    ) -> Result<crate::RuntimeEventReactorHealth, RuntimeServicesError> {
        self.event_reactor
            .health()
            .map_err(RuntimeServicesError::Invariant)
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

    /// Publishes an approved AI-authored Team template candidate. The
    /// candidate is read from the durable approval-bound event stream, so a
    /// human approval can be completed long after the proposing turn ended.
    /// Publishing is idempotent through the definition store's revision
    /// semantics.
    pub fn publish_approved_template_candidate(
        &self,
        approval_id: &str,
    ) -> Result<serde_json::Value, String> {
        let request = self
            .approval_queue()
            .get(approval_id)
            .ok_or_else(|| format!("approval_not_found: {approval_id}"))?;
        if request.action != "definition.template.publish" {
            return Err(format!(
                "approval `{approval_id}` is not a template publish approval"
            ));
        }
        if request.status != harness_contract::policy::ApprovalStatus::Approved {
            return Err(format!("approval `{approval_id}` is not approved"));
        }
        let stream_id = format!("definition-template-candidate:{approval_id}");
        let candidate_event = self
            .event_store()
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|event| event.kind == "definition.template.candidate.v1")
            .ok_or_else(|| format!("template candidate missing for approval {approval_id}"))?;
        let mut manifest = serde_json::from_value::<harness_contract::team::TeamTemplateManifest>(
            candidate_event
                .payload
                .get("manifest")
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|error| format!("decode template candidate manifest: {error}"))?;
        // The candidate is compiled as Draft and becomes runnable only after
        // this approval completes; the store refuses to resolve non-published
        // revisions even with an active stable release assignment.
        manifest.lifecycle = harness_contract::agent::RevisionLifecycle::Published;
        let instructions = candidate_event
            .payload
            .get("instructions")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let stored = self
            .definition_registry()
            .teams()
            .store_revision(manifest, &instructions)
            .map_err(|error| format!("template_publish_failed: {error}"))?;
        let scope = stored.revision.revision_ref.template_id.scope();
        let authorization = harness_contract::agent::ReleaseAuthorization::HumanApproval {
            approval_ref: format!("approval/{approval_id}"),
        };
        self.definition_registry()
            .teams()
            .record_release_assignment(&crate::team_definition::TeamReleaseAssignment {
                scope,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: harness_contract::agent::ReleaseChannel::Stable,
                status: harness_contract::agent::ReleaseAssignmentStatus::Active,
                authorization: authorization.clone(),
                content_digest: stored.revision.content_digest.clone(),
            })
            .map_err(|error| format!("template_publish_failed: {error}"))?;
        self.definition_registry()
            .teams()
            .set_default_pointer(&crate::team_definition::TeamDefaultPointer::latest(
                scope,
                stored.revision.revision_ref.template_id.clone(),
                authorization,
            ))
            .map_err(|error| format!("template_publish_failed: {error}"))?;
        let _ = self.event_store().append(crate::RuntimeEventInput {
            stream_id,
            scope: crate::RuntimeEventScope::Mission,
            kind: "definition.template.published.v1".to_string(),
            status: Some("published".to_string()),
            actor: Some(approval_id.to_string()),
            refs: vec![crate::RuntimeEventRef {
                kind: "team_template".to_string(),
                id: stored
                    .revision
                    .revision_ref
                    .template_id
                    .as_str()
                    .to_string(),
            }],
            payload: serde_json::json!({
                "approval_id": approval_id,
                "template_id": stored.revision.revision_ref.template_id.as_str(),
                "revision": stored.revision.revision_ref.revision,
                "digest": stored.revision.content_digest,
            }),
        });
        Ok(serde_json::json!({
            "approval_id": approval_id,
            "template_id": stored.revision.revision_ref.template_id.as_str(),
            "revision": stored.revision.revision_ref.revision,
            "content_digest": stored.revision.content_digest,
        }))
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
        let policy_revision = self.canonical_task_policy_revision(&intent.task_id)?;
        let selected = intent
            .selected_agent_id
            .as_deref()
            .and_then(|agent_id| self.agent_runtime.catalog().get(agent_id));
        let request = request_for_intent(&intent, selected)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
        let compiled = self.compile_agent_binding(request)?;
        let mut packet = compiled
            .snapshot
            .compile_task_packet(intent, execution_identity)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
        packet.policy_revision = policy_revision;
        Ok(packet)
    }

    fn canonical_task_policy_revision(&self, task_id: &str) -> Result<u64, RuntimeServicesError> {
        let task = crate::TaskRuntimePort::new(self)
            .get(task_id)
            .map_err(RuntimeServicesError::Task)?
            .ok_or_else(|| {
                RuntimeServicesError::Invariant(format!(
                    "Agent task `{task_id}` has no canonical Task aggregate"
                ))
            })?;
        let binding = task.execution_policy.binding.as_ref().ok_or_else(|| {
            RuntimeServicesError::Invariant(format!(
                "Agent task `{task_id}` has no canonical execution-policy binding"
            ))
        })?;
        binding
            .validate()
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        Ok(binding.execution.policy_revision)
    }

    fn ensure_evolution_execution_policy(
        &self,
        session_id: &str,
    ) -> Result<(), RuntimeServicesError> {
        let expected = harness_contract::policy::SessionExecutionPolicy::from_profile(
            harness_contract::policy::AutonomyProfileId::Cautious,
            1,
            harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
        );
        match self.session_execution_policy(session_id) {
            Some(current) if current == expected => Ok(()),
            Some(_) => Err(RuntimeServicesError::Invariant(format!(
                "evolution evaluation Session `{session_id}` has a non-canonical execution policy"
            ))),
            None => {
                self.publish_session_execution_policy(
                    session_id,
                    crate::permissions::SessionExecutionPolicyControl::from_policy(expected),
                );
                Ok(())
            }
        }
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
                    && task.origin_session_id == intent.session_id
                    && task.origin_turn_id == intent.source_turn_id
                    && task.root_task_id == intent.root_task_id
                    && task.parent_task_id == intent.parent_task_id => {}
            Some(_) => {
                return Err(RuntimeServicesError::Invariant(format!(
                    "Agent task `{}` conflicts with its canonical Task aggregate lineage",
                    intent.task_id
                )));
            }
            None => {
                let mut spec = harness_contract::task::TaskSpec::new(intent.objective.clone());
                spec.execution_policy.max_failures_before_block = 3;
                let spec = if let Some(parent_task_id) = intent.parent_task_id.as_deref() {
                    task_port
                        .bind_inherited_task_spec(parent_task_id, intent.permission_ceiling, spec)
                        .map_err(RuntimeServicesError::Task)?
                } else {
                    task_port
                        .bind_task_spec(&intent.session_id, None, spec)
                        .map_err(RuntimeServicesError::Task)?
                };
                let command = harness_contract::task::TaskCreateCommand {
                    task_id: intent.task_id.clone(),
                    mission_id: intent.mission_id.clone(),
                    kind: if intent.task_id == intent.root_task_id {
                        harness_contract::task::TaskKind::Root
                    } else {
                        harness_contract::task::TaskKind::Delegated
                    },
                    origin: if intent.task_id == intent.root_task_id {
                        harness_contract::task::TaskOrigin::User
                    } else {
                        harness_contract::task::TaskOrigin::Delegated
                    },
                    origin_session_id: intent.session_id.clone(),
                    origin_turn_id: intent.source_turn_id.clone(),
                    root_task_id: intent.root_task_id.clone(),
                    parent_task_id: intent.parent_task_id.clone(),
                    predecessor_task_id: None,
                    mission_assignment: harness_contract::task::TaskMissionAssignment::Automatic,
                    mission_assigned_by: "runtime.agent".to_string(),
                    spec,
                    evidence_refs: Vec::new(),
                };
                if command.parent_task_id.is_some() {
                    task_port
                        .create_inherited(command)
                        .map_err(RuntimeServicesError::Task)?;
                } else {
                    task_port
                        .create(command)
                        .map_err(RuntimeServicesError::Task)?;
                }
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
        self.compile_agent_task_nodes(&mut graph.nodes)?;
        Ok(graph)
    }

    /// Resolve only newly-added semantic Agent nodes during a graph revision.
    /// Existing immutable packets are deliberately not rebound.
    pub fn compile_agent_task_nodes(
        &self,
        nodes: &mut [harness_contract::execution_graph::ExecutionNodeSpec],
    ) -> Result<(), RuntimeServicesError> {
        for node in nodes {
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
            if let Some(work) = node.work.as_mut() {
                work.model_profile = packet
                    .binding
                    .as_ref()
                    .map(|binding| binding.model_policy.profile.clone())
                    .or_else(|| Some(packet.model_lease.clone()));
                work.context_view_ref = Some(packet.budget_lease.lease_id.clone());
            }
            node.payload_ref = serde_json::to_string(&packet).map_err(|error| {
                RuntimeServicesError::AgentRuntime(format!(
                    "encode Runtime-bound AgentTask node `{}`: {error}",
                    node.id
                ))
            })?;
        }
        Ok(())
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

    /// Resolve the root Session event bus for any nested execution graph.
    ///
    /// Team graphs are normally two levels below SessionIngress
    /// (Mission -> Team), while Agent graphs add another level. The live bus
    /// registry intentionally contains only root Session executions, so a
    /// direct lookup is insufficient for nested Agents.
    #[must_use]
    pub(crate) fn resolve_active_execution_bus(
        &self,
        execution_id: &str,
    ) -> Option<(String, crate::CowdEventBus)> {
        const MAX_LINEAGE_DEPTH: usize = 32;
        let mut candidate = execution_id.to_string();
        let mut visited = std::collections::BTreeSet::new();
        for _ in 0..MAX_LINEAGE_DEPTH {
            if !visited.insert(candidate.clone()) {
                return None;
            }
            if let Some(bus) = self.active_execution_bus(&candidate) {
                return Some((candidate, bus));
            }
            candidate = self
                .graph_state_store
                .load(&candidate)
                .ok()?
                .parent_execution?
                .execution_id;
        }
        None
    }

    /// Read-only durable event projection for Gateway and Surface consumers.
    #[must_use]
    pub fn event_reader(&self) -> RuntimeEventReader {
        RuntimeEventReader {
            store: Arc::clone(&self.event_store),
        }
    }

    /// Commit the one canonical cancellation activity receipt. The receipt is
    /// keyed by `cancellation_id`, so HTTP retries and live delivery reuse the
    /// same durable fact; a changed payload under the same id is rejected by
    /// the event-store transaction hash.
    pub fn commit_cancellation_receipt(
        &self,
        mut receipt: harness_contract::turn::CancellationReceipt,
    ) -> Result<harness_contract::turn::CancellationReceipt, RuntimeServicesError> {
        if receipt.cancellation_id.trim().is_empty()
            || receipt.session_id.trim().is_empty()
            || receipt.requested_at_ms == 0
        {
            return Err(RuntimeServicesError::Invariant(
                "cancellation receipt requires id, session and requested_at_ms".to_string(),
            ));
        }
        let stream_id = format!("cancellation:{}", receipt.cancellation_id);
        let expected_revision = self.event_store.stream_revision(&stream_id)?;
        // Commit metadata is assigned by the ledger, never trusted from an
        // HTTP caller. Normalization keeps an idempotent retry byte-identical.
        receipt.journal_sequence = 0;
        receipt.projection_revision = 0;
        if let Some(existing) = self
            .event_store
            .list_stream(&stream_id)
            .map_err(RuntimeServicesError::Invariant)?
            .last()
        {
            let mut persisted =
                serde_json::from_value::<harness_contract::turn::CancellationReceipt>(
                    existing.payload.clone(),
                )
                .map_err(|error| {
                    RuntimeServicesError::Invariant(format!(
                        "decode durable cancellation receipt: {error}"
                    ))
                })?;
            if persisted == receipt {
                persisted.journal_sequence = existing.commit_cursor;
                persisted.projection_revision = existing.sequence;
                return Ok(persisted);
            }
            let same_identity = persisted.cancellation_id == receipt.cancellation_id
                && persisted.session_id == receipt.session_id
                && persisted.turn_id == receipt.turn_id
                && persisted.execution_id == receipt.execution_id
                && persisted.actor_id == receipt.actor_id
                && persisted.cause == receipt.cause
                && persisted.reason == receipt.reason
                && persisted.requested_at_ms == receipt.requested_at_ms;
            // HTTP, the ingress worker, and the recovery reconciler can all
            // finalize the same Requested intent. Once a final receipt exists,
            // its status and effective timestamp are the durable winner; a
            // concurrent writer with the same immutable request identity must
            // return it rather than treating timestamp drift as ID reuse.
            if same_identity
                && persisted.status != harness_contract::turn::CancellationStatus::Requested
            {
                persisted.journal_sequence = existing.commit_cursor;
                persisted.projection_revision = existing.sequence;
                return Ok(persisted);
            }
            let valid_requested_transition = persisted.status
                == harness_contract::turn::CancellationStatus::Requested
                && receipt.status != harness_contract::turn::CancellationStatus::Requested
                && same_identity;
            if !valid_requested_transition {
                return Err(RuntimeServicesError::Invariant(format!(
                    "cancellation id `{}` was reused with a different receipt",
                    receipt.cancellation_id
                )));
            }
        }
        let status_key = match receipt.status {
            harness_contract::turn::CancellationStatus::Requested => "requested",
            harness_contract::turn::CancellationStatus::Cancelled => "cancelled",
            harness_contract::turn::CancellationStatus::AlreadyTerminal => "already-terminal",
        };
        let transaction_id = format!(
            "session-cancellation:{}:{status_key}",
            receipt.cancellation_id
        );
        let event = crate::RuntimeEventInput {
            stream_id: stream_id.clone(),
            scope: crate::RuntimeEventScope::Session,
            kind: "session.cancellation_committed".to_string(),
            status: Some(
                match receipt.status {
                    harness_contract::turn::CancellationStatus::Requested => "requested",
                    harness_contract::turn::CancellationStatus::Cancelled => "cancelled",
                    harness_contract::turn::CancellationStatus::AlreadyTerminal => {
                        "already_terminal"
                    }
                }
                .to_string(),
            ),
            actor: Some(receipt.actor_id.clone()),
            refs: [
                (!receipt.session_id.is_empty()).then(|| crate::RuntimeEventRef {
                    kind: "session".to_string(),
                    id: receipt.session_id.clone(),
                }),
                (!receipt.execution_id.is_empty()).then(|| crate::RuntimeEventRef {
                    kind: "execution".to_string(),
                    id: receipt.execution_id.clone(),
                }),
                (!receipt.turn_id.is_empty()).then(|| crate::RuntimeEventRef {
                    kind: "turn".to_string(),
                    id: receipt.turn_id.clone(),
                }),
            ]
            .into_iter()
            .flatten()
            .collect(),
            payload: serde_json::to_value(&receipt).map_err(|error| {
                RuntimeServicesError::Invariant(format!("encode cancellation receipt: {error}"))
            })?,
        };
        let committed = match self
            .event_store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id,
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                }],
                events: vec![crate::RuntimeTransactionEventInput {
                    event,
                    idempotency_key: Some(format!("{}:{status_key}", receipt.cancellation_id)),
                    schema_version: 1,
                }],
            }) {
            Ok(committed) => committed,
            Err(error) => {
                // HTTP, ingress execution, and the restart reconciler may all
                // race to finalize the same durable Requested intent. The CAS
                // loser must observe the already committed winner rather than
                // turn a successful user cancellation into an execution
                // failure merely because its effective timestamp differed.
                if let Some(winner) = self.cancellation_receipt(&receipt.cancellation_id)? {
                    let same_identity = winner.cancellation_id == receipt.cancellation_id
                        && winner.session_id == receipt.session_id
                        && winner.turn_id == receipt.turn_id
                        && winner.execution_id == receipt.execution_id
                        && winner.actor_id == receipt.actor_id
                        && winner.cause == receipt.cause
                        && winner.reason == receipt.reason
                        && winner.requested_at_ms == receipt.requested_at_ms;
                    if same_identity {
                        if receipt.status == harness_contract::turn::CancellationStatus::Requested
                            || winner.status
                                != harness_contract::turn::CancellationStatus::Requested
                        {
                            return Ok(winner);
                        }
                        // The CAS lost to the first identical Requested
                        // append, not to a finalizer. Retry against the stable
                        // new revision; if a finalizer wins meanwhile, the
                        // next read returns that durable winner.
                        return self.commit_cancellation_receipt(receipt);
                    }
                }
                return Err(error.into());
            }
        };
        receipt.journal_sequence = committed.commit_cursor;
        receipt.projection_revision = committed
            .stream_revisions
            .first()
            .map_or(expected_revision.saturating_add(1), |revision| {
                revision.committed_revision
            });
        Ok(receipt)
    }

    pub fn cancellation_receipt(
        &self,
        cancellation_id: &str,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let stream_id = format!("cancellation:{cancellation_id}");
        let Some(event) = self
            .event_store
            .list_stream(&stream_id)
            .map_err(RuntimeServicesError::Invariant)?
            .last()
            .cloned()
        else {
            return Ok(None);
        };
        let mut receipt =
            serde_json::from_value::<harness_contract::turn::CancellationReceipt>(event.payload)
                .map_err(|error| {
                    RuntimeServicesError::Invariant(format!(
                        "decode durable cancellation receipt: {error}"
                    ))
                })?;
        receipt.journal_sequence = event.commit_cursor;
        receipt.projection_revision = event.sequence;
        Ok(Some(receipt))
    }

    /// Resolve durable cancellation intents left behind by a process crash.
    /// Missing executions remain Requested; absence of a process-local token
    /// is never treated as proof that work is terminal.
    pub fn reconcile_requested_cancellations(
        &self,
        limit: usize,
    ) -> Result<Vec<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let mut finalized = Vec::new();
        for receipt in self.pending_cancellation_receipts(limit)? {
            if let Some(receipt) = self.resolve_requested_cancellation(&receipt.cancellation_id)? {
                finalized.push(receipt);
            }
        }
        Ok(finalized)
    }

    pub fn resolve_requested_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        let Some(mut receipt) = self.cancellation_receipt(cancellation_id)? else {
            return Ok(None);
        };
        if receipt.status != harness_contract::turn::CancellationStatus::Requested {
            return Ok(Some(receipt));
        }
        let Some(live) = self.execution_live(&receipt.execution_id) else {
            // A concurrent finalizer releases the live winner checkpoint only
            // after committing the final receipt. Re-read that durable stream
            // before declaring the intent unresolved.
            let winner = self.cancellation_receipt(cancellation_id)?;
            return Ok(winner.filter(|winner| {
                winner.status != harness_contract::turn::CancellationStatus::Requested
            }));
        };
        let status = if live.status == harness_contract::projection::ExecutionLiveStatus::Cancelled
        {
            harness_contract::turn::CancellationStatus::Cancelled
        } else if live.status.is_terminal() {
            harness_contract::turn::CancellationStatus::AlreadyTerminal
        } else if self
            .try_cancel_live_execution(
                &receipt.execution_id,
                receipt
                    .reason
                    .clone()
                    .unwrap_or_else(|| "user_requested".to_string()),
            )
            .map_err(RuntimeServicesError::Invariant)?
        {
            harness_contract::turn::CancellationStatus::Cancelled
        } else {
            // Another finalizer can win the live CAS just before this call.
            // Re-read the winner projection: either writer may now commit the
            // same final receipt, and the event-stream CAS below makes that
            // commit exactly once.
            match self.execution_live(&receipt.execution_id) {
                Some(winner)
                    if winner.status
                        == harness_contract::projection::ExecutionLiveStatus::Cancelled =>
                {
                    harness_contract::turn::CancellationStatus::Cancelled
                }
                Some(winner) if winner.status.is_terminal() => {
                    harness_contract::turn::CancellationStatus::AlreadyTerminal
                }
                _ => {
                    let winner = self.cancellation_receipt(cancellation_id)?;
                    return Ok(winner.filter(|winner| {
                        winner.status != harness_contract::turn::CancellationStatus::Requested
                    }));
                }
            }
        };
        receipt.status = status;
        receipt.effective_at_ms = Some(now_ms());
        receipt.journal_sequence = 0;
        receipt.projection_revision = 0;
        let receipt = self.commit_cancellation_receipt(receipt)?;
        self.release_live_terminal_fence(&receipt.execution_id);
        Ok(Some(receipt))
    }

    pub fn pending_cancellation_receipts(
        &self,
        limit: usize,
    ) -> Result<Vec<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        const PAGE_SIZE: usize = 256;
        let mut latest =
            std::collections::BTreeMap::<String, harness_contract::turn::CancellationReceipt>::new(
            );
        let mut after = None;
        loop {
            let page = self
                .event_store
                .list_scope_kind_page_asc(
                    crate::RuntimeEventScope::Session,
                    "session.cancellation_committed",
                    after,
                    PAGE_SIZE,
                )
                .map_err(RuntimeServicesError::Invariant)?;
            if page.is_empty() {
                break;
            }
            for event in &page {
                let Ok(mut receipt) = serde_json::from_value::<
                    harness_contract::turn::CancellationReceipt,
                >(event.payload.clone()) else {
                    continue;
                };
                receipt.journal_sequence = event.commit_cursor;
                receipt.projection_revision = event.sequence;
                latest.insert(receipt.cancellation_id.clone(), receipt);
            }
            after = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            if page.len() < PAGE_SIZE {
                break;
            }
        }
        Ok(latest
            .into_values()
            .filter(|receipt| {
                receipt.status == harness_contract::turn::CancellationStatus::Requested
                    && !receipt.execution_id.is_empty()
            })
            .take(limit)
            .collect())
    }

    pub fn latest_cancellation_receipt_for_execution(
        &self,
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> Result<Option<harness_contract::turn::CancellationReceipt>, RuntimeServicesError> {
        const PAGE_SIZE: usize = 256;
        let mut after = None;
        let mut found = None;
        loop {
            let page = self
                .event_store
                .list_scope_kind_page_asc(
                    crate::RuntimeEventScope::Session,
                    "session.cancellation_committed",
                    after,
                    PAGE_SIZE,
                )
                .map_err(RuntimeServicesError::Invariant)?;
            if page.is_empty() {
                break;
            }
            for event in &page {
                let Ok(mut receipt) = serde_json::from_value::<
                    harness_contract::turn::CancellationReceipt,
                >(event.payload.clone()) else {
                    continue;
                };
                if receipt.session_id == session_id
                    && receipt.execution_id == execution_id
                    && receipt.turn_id == turn_id
                {
                    receipt.journal_sequence = event.commit_cursor;
                    receipt.projection_revision = event.sequence;
                    found = Some(receipt);
                }
            }
            after = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            if page.len() < PAGE_SIZE {
                break;
            }
        }
        Ok(found)
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
        let _ = self.try_complete_recovered_live_execution(execution_id, terminal_ref);
    }

    pub fn try_complete_recovered_live_execution(
        &self,
        execution_id: &str,
        terminal_ref: String,
    ) -> bool {
        self.live_execution_store
            .complete_recovered(execution_id, terminal_ref)
    }

    pub fn claim_live_terminal_fence(
        &self,
        execution_id: &str,
        terminal_ref: String,
        status: harness_contract::projection::ExecutionLiveStatus,
    ) -> Result<crate::execution_live::TerminalFenceClaim, String> {
        self.live_execution_store
            .claim_terminal(execution_id, terminal_ref, status)
    }

    /// Release the temporary durable live winner only after its canonical
    /// terminal carrier (Session transcript/outbox or cancellation receipt)
    /// has committed. Before that boundary it is the crash-recovery fence.
    pub fn release_live_terminal_fence(&self, execution_id: &str) {
        if !execution_id.trim().is_empty() {
            self.live_execution_store
                .release_terminal_checkpoint(execution_id);
        }
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

    /// Atomically claim cancellation as the live terminal winner. A concurrent
    /// normal terminal transition uses the same sharded record lock; exactly
    /// one transition can leave a non-terminal state.
    pub fn try_cancel_live_execution(
        &self,
        execution_id: &str,
        detail: String,
    ) -> Result<bool, String> {
        self.live_execution_store.cancel(execution_id, detail)
    }

    pub fn cancel_live_execution(&self, execution_id: &str, detail: String) {
        let _ = self.try_cancel_live_execution(execution_id, detail);
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
    pub fn hot_state(&self) -> &Arc<crate::execution_core::hot_state::RuntimeHotStatePlane> {
        &self.hot_state
    }

    #[must_use]
    pub fn hot_session_snapshot(
        &self,
        session_id: &str,
    ) -> Option<Arc<crate::execution_core::hot_state::HotSessionSnapshot>> {
        let snapshot = self.hot_state.sessions().get(session_id)?;
        let mut snapshot = (*snapshot).clone();
        snapshot.pending_approvals = self
            .approval_queue
            .pending()
            .into_iter()
            .filter(|request| request.source.session_id.as_deref() == Some(session_id))
            .count();
        Some(Arc::new(snapshot))
    }

    #[must_use]
    pub fn hot_session_snapshots(
        &self,
        session_ids: &[String],
    ) -> Vec<Arc<crate::execution_core::hot_state::HotSessionSnapshot>> {
        let mut pending_approvals = HashMap::<String, usize>::new();
        for request in self.approval_queue.pending() {
            if let Some(session_id) = request.source.session_id {
                *pending_approvals.entry(session_id).or_default() += 1;
            }
        }
        self.hot_state
            .sessions()
            .get_many(session_ids)
            .into_iter()
            .map(|snapshot| {
                let mut snapshot = (*snapshot).clone();
                snapshot.pending_approvals = pending_approvals
                    .get(&snapshot.session_id)
                    .copied()
                    .unwrap_or_default();
                Arc::new(snapshot)
            })
            .collect()
    }

    pub fn update_hot_session_input(
        &self,
        projection: &harness_contract::turn::SessionInputProjection,
        inbox: &harness_contract::turn::TurnInboxSnapshot,
    ) {
        if projection.session_id != inbox.session_id {
            tracing::warn!(
                projection_session_id = %projection.session_id,
                inbox_session_id = %inbox.session_id,
                "refused mismatched hot Session input projection"
            );
            return;
        }
        self.hot_state
            .sessions()
            .update(&projection.session_id, |snapshot| {
                if let Some(cursor) = projection.admitted_cursor {
                    snapshot.generation = snapshot.generation.max(cursor.generation);
                    snapshot.accepted_cursor = snapshot.accepted_cursor.max(cursor.sequence);
                    snapshot.durable_cursor = Some(
                        snapshot
                            .durable_cursor
                            .map_or(cursor.sequence, |current| current.max(cursor.sequence)),
                    );
                }
                if let Some(cursor) = projection.consumed_cursor {
                    snapshot.generation = snapshot.generation.max(cursor.generation);
                    snapshot.runtime_cursor = snapshot.runtime_cursor.max(cursor.sequence);
                }
                snapshot.current_turn_id =
                    projection.active_turn_id.as_ref().map(ToString::to_string);
                snapshot.pending_inputs = projection.pending_count;
                snapshot.inbox_refs = inbox
                    .items
                    .iter()
                    .filter(|item| item.consumed_at.is_none())
                    .map(|item| format!("session-input:{}", item.input_id))
                    .collect();
            });
    }

    pub fn record_hot_session_durable_ingress(&self, session_id: &str, durable_cursor: u64) {
        self.hot_state.sessions().update(session_id, |snapshot| {
            snapshot.durable_cursor = Some(
                snapshot
                    .durable_cursor
                    .map_or(durable_cursor, |current| current.max(durable_cursor)),
            );
        });
    }

    #[must_use]
    pub fn hot_state_health(&self) -> crate::execution_core::hot_state::HotStateHealth {
        self.hot_state.health()
    }

    pub fn update_hot_state_config(
        &self,
        config: &crate::execution_core::hot_state::HotStateConfig,
    ) -> Result<crate::execution_core::hot_state::HotStateHealth, String> {
        self.hot_state.reconfigure(config)?;
        Ok(self.hot_state.health())
    }
    pub fn commit_service(&self) -> &ExecutionCommitService {
        &self.commit_service
    }
    pub fn execution_supervisor(&self) -> &Arc<crate::RuntimeExecutionSupervisor> {
        &self.execution_supervisor
    }

    /// Durably cancel an execution graph and every graph registered beneath it.
    ///
    /// Session cancellation cannot stop at the process-local provider token or
    /// the live projection: Team/Subgraph work may already have been admitted
    /// into independent supervisor slots. The lineage stream is the canonical
    /// ownership relation, so walk it breadth-first and terminalize every
    /// non-terminal descendant. A graph that is already terminal is still
    /// traversed because its children may outlive a failed or cancelled parent.
    pub async fn cancel_execution_tree(
        &self,
        root_execution_id: &str,
        reason: &str,
    ) -> Result<Vec<String>, RuntimeServicesError> {
        if root_execution_id.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut pending = VecDeque::from([root_execution_id.to_string()]);
        let mut seen = BTreeSet::new();
        let mut cancelled = Vec::new();
        while let Some(graph_id) = pending.pop_front() {
            if !seen.insert(graph_id.clone()) {
                continue;
            }

            // Discover children even when the parent is already terminal. This
            // is the exact crash/race shape that previously left Team
            // synthesizers running after a Session cancellation won.
            for link in self
                .graph_state_store
                .child_links_async(graph_id.clone())
                .await?
            {
                pending.push_back(link.child_execution_id);
            }

            let mut terminalized = false;
            for _ in 0..4 {
                let graph = match self.graph_state_store.load_async(&graph_id).await {
                    Ok(graph) => graph,
                    Err(ExecutionStateStoreError::NotFound(_)) => break,
                    Err(error) => return Err(error.into()),
                };
                if graph
                    .node_statuses
                    .values()
                    .all(|status| status.is_terminal())
                {
                    break;
                }
                match self
                    .execution_supervisor
                    .command_graph(
                        &graph_id,
                        ExecutionGraphCommand::Cancel {
                            expected_revision: graph.revision,
                            reason: reason.to_string(),
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        terminalized = true;
                        break;
                    }
                    Err(ExecutionRunnerError::Commit(
                        super::graph::ExecutionCommitError::StaleRevision { .. },
                    )) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            if terminalized {
                cancelled.push(graph_id.clone());
            }

            // A running parent can register a child concurrently with the
            // first observation. Re-read after cancellation and enqueue only
            // newly committed lineage; `seen` keeps replay idempotent.
            for link in self.graph_state_store.child_links_async(graph_id).await? {
                if !seen.contains(&link.child_execution_id) {
                    pending.push_back(link.child_execution_id);
                }
            }
        }
        Ok(cancelled)
    }

    pub fn approval_queue(&self) -> &Arc<ApprovalQueue> {
        &self.approval_queue
    }
    pub fn approval_coordinator(&self) -> &Arc<ApprovalCoordinator> {
        &self.approval_coordinator
    }
    pub fn execution_capacity_profile(&self) -> &Arc<crate::ExecutionCapacityProfile> {
        &self.execution_capacity_profile
    }

    pub fn publish_session_execution_policy(
        &self,
        session_id: impl Into<String>,
        control: crate::permissions::SessionExecutionPolicyControl,
    ) {
        let session_id = session_id.into();
        self.session_execution_policy_controls
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id, control);
    }

    #[must_use]
    pub fn session_execution_policy(
        &self,
        session_id: &str,
    ) -> Option<harness_contract::policy::SessionExecutionPolicy> {
        self.session_execution_policy_controls
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
    }

    #[must_use]
    pub fn session_execution_policy_control(
        &self,
        session_id: &str,
    ) -> Option<crate::permissions::SessionExecutionPolicyControl> {
        self.session_execution_policy_controls
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
    }

    pub(crate) fn session_execution_policy_controls_handle(
        &self,
    ) -> Arc<RwLock<BTreeMap<String, crate::permissions::SessionExecutionPolicyControl>>> {
        Arc::clone(&self.session_execution_policy_controls)
    }

    pub(crate) fn session_execution_policy_admission_blocks_handle(
        &self,
    ) -> Arc<RwLock<BTreeMap<String, String>>> {
        Arc::clone(&self.session_execution_policy_admission_blocks)
    }

    /// Fence every new Task admission for one Session while Gateway drains a
    /// policy revision. Existing Tasks retain their immutable binding.
    pub fn freeze_session_execution_policy_admission(
        &self,
        session_id: impl Into<String>,
        transition_id: impl Into<String>,
    ) {
        self.session_execution_policy_admission_blocks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.into(), transition_id.into());
    }

    pub fn unfreeze_session_execution_policy_admission(
        &self,
        session_id: &str,
        transition_id: &str,
    ) -> bool {
        let mut blocks = self
            .session_execution_policy_admission_blocks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if blocks.get(session_id).map(String::as_str) != Some(transition_id) {
            return false;
        }
        blocks.remove(session_id);
        true
    }

    /// Returns every non-terminal durable graph whose canonical Task remains
    /// bound to one exact Session policy revision. This is the Runtime-owned
    /// drain truth for background/Mission work that has no Gateway turn guard.
    pub async fn active_graphs_for_session_policy_revision(
        &self,
        session_id: &str,
        policy_revision: u64,
    ) -> Result<Vec<(String, u64)>, RuntimeServicesError> {
        let mut active = Vec::new();
        for graph_id in self.graph_state_store.nonterminal_graph_ids_async().await? {
            let graph = self.graph_state_store.load_async(&graph_id).await?;
            let Some(task_id) = graph
                .lineage
                .as_ref()
                .map(|lineage| lineage.task_id.as_str())
            else {
                continue;
            };
            let Some(task) = self
                .task_aggregate_service
                .get(task_id)
                .map_err(RuntimeServicesError::Invariant)?
            else {
                continue;
            };
            let Some(binding) = task.execution_policy.binding.as_ref() else {
                continue;
            };
            if binding.execution.session_id == session_id
                && binding.execution.policy_revision == policy_revision
            {
                active.push((graph.id, graph.revision));
            }
        }
        active.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(active)
    }

    /// Returns already-admitted Task attempts even during the narrow window
    /// before their graph is submitted. Counting the Task binding closes the
    /// freeze-versus-submit race that a graph-only drain cannot observe.
    pub async fn active_tasks_for_session_policy_revision(
        &self,
        session_id: &str,
        policy_revision: u64,
    ) -> Result<Vec<(String, u64)>, RuntimeServicesError> {
        let candidates = self
            .task_aggregate_service
            .list()
            .map_err(RuntimeServicesError::Invariant)?
            .into_iter()
            .filter(|task| !task.status.is_terminal())
            .filter(|task| {
                task.execution_policy
                    .binding
                    .as_ref()
                    .is_some_and(|binding| {
                        binding.execution.session_id == session_id
                            && binding.execution.policy_revision == policy_revision
                    })
            })
            .collect::<Vec<_>>();
        let mut active = Vec::new();
        for task in candidates {
            let mut has_live_graph = task.graph_refs.is_empty();
            for graph_ref in &task.graph_refs {
                let graph = self
                    .graph_state_store
                    .load_async(&graph_ref.graph_id)
                    .await?;
                if graph
                    .node_statuses
                    .values()
                    .any(|status| !status.is_terminal())
                {
                    has_live_graph = true;
                    break;
                }
            }
            if has_live_graph {
                active.push((task.task_id, task.revision));
            }
        }
        active.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(active)
    }

    /// Requests exact durable cancellation for old-revision graphs and their
    /// admitted Tasks after the transition drain grace expires.
    /// Revision races are harmless: the next drain observation reloads
    /// canonical state and retries only live attempts.
    pub async fn cancel_attempts_for_session_policy_revision(
        &self,
        session_id: &str,
        policy_revision: u64,
        reason: &str,
    ) -> Result<usize, RuntimeServicesError> {
        // Freeze the Task set before graph cancellation. Once a graph becomes
        // terminal it no longer appears in the active-task projection, but its
        // owning Task must still be terminalized under the old policy revision.
        let tasks = self
            .active_tasks_for_session_policy_revision(session_id, policy_revision)
            .await?;
        let graphs = self
            .active_graphs_for_session_policy_revision(session_id, policy_revision)
            .await?;
        let mut cancelled = 0usize;
        for (graph_id, revision) in graphs {
            match self
                .execution_supervisor
                .command_graph(
                    &graph_id,
                    harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                        expected_revision: revision,
                        reason: reason.to_string(),
                    },
                )
                .await
            {
                Ok(_) => cancelled = cancelled.saturating_add(1),
                Err(crate::execution_core::ExecutionRunnerError::Commit(
                    crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
                )) => {}
                Err(error) => return Err(RuntimeServicesError::GraphRunner(error)),
            }
        }
        for (task_id, _) in tasks {
            let Some(task) = self
                .task_aggregate_service
                .get(&task_id)
                .map_err(RuntimeServicesError::Invariant)?
            else {
                continue;
            };
            let still_bound_to_old_revision =
                task.execution_policy
                    .binding
                    .as_ref()
                    .is_some_and(|binding| {
                        binding.execution.session_id == session_id
                            && binding.execution.policy_revision == policy_revision
                    });
            if task.status.is_terminal() || !still_bound_to_old_revision {
                continue;
            }
            match self.task_runtime_port().transition(
                &task.task_id,
                task.revision,
                harness_contract::task::TaskStatus::Cancelled,
                vec![harness_contract::reality::EvidenceRef::observed(
                    "session_policy_transition",
                    format!("{session_id}:{policy_revision}"),
                )],
                reason,
            ) {
                Ok(_) => cancelled = cancelled.saturating_add(1),
                Err(error) if error.contains("stale") || error.contains("terminal") => {}
                Err(error) => return Err(RuntimeServicesError::Invariant(error)),
            }
        }
        Ok(cancelled)
    }

    /// Attach the current Session policy revision to an approval context.
    /// Non-Session governance domains remain independent and are returned
    /// unchanged.
    #[must_use]
    pub fn bind_session_policy_to_approval_context(
        &self,
        context: harness_contract::policy::ApprovalContext,
    ) -> harness_contract::policy::ApprovalContext {
        let Some(session_id) = context.session_id.as_deref() else {
            return context;
        };
        match self.session_execution_policy(session_id) {
            Some(policy) => context.with_execution_policy(&policy),
            None => context,
        }
    }

    pub fn remove_session_execution_policy(&self, session_id: &str) {
        self.session_execution_policy_controls
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.session_execution_policy_admission_blocks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
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

    pub fn evolution_signal(
        &self,
        signal_id: &str,
    ) -> Result<Option<crate::EvolutionSignal>, RuntimeServicesError> {
        self.evolution_discovery
            .signal(signal_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_cases(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::EvolutionCase>, RuntimeServicesError> {
        self.evolution_discovery
            .list_cases(limit)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_case(
        &self,
        case_id: &str,
    ) -> Result<Option<crate::EvolutionCase>, RuntimeServicesError> {
        self.evolution_discovery
            .case(case_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_case_page(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<crate::EvolutionCasePage, RuntimeServicesError> {
        self.evolution_discovery
            .case_page(cursor, limit)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_case_index(&self) -> Result<crate::EvolutionCaseIndex, RuntimeServicesError> {
        self.evolution_discovery
            .case_index()
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn evolution_analysis(
        &self,
        case_id: &str,
    ) -> Result<Option<harness_contract::evolution::EvolutionAnalysisDraft>, RuntimeServicesError>
    {
        self.evolution_analyst
            .draft_for_case(case_id)
            .map_err(RuntimeServicesError::Invariant)
    }

    pub fn skill_maintenance_drafts(
        &self,
        limit: usize,
    ) -> Result<Vec<harness_contract::skill::SkillMaintenanceDraft>, RuntimeServicesError> {
        Ok(self.skill_maintenance_projector.drafts(limit))
    }

    pub fn skill_maintenance_draft(
        &self,
        draft_id: &str,
    ) -> Result<Option<harness_contract::skill::SkillMaintenanceDraft>, RuntimeServicesError> {
        Ok(self.skill_maintenance_projector.draft(draft_id))
    }

    pub fn skill_maintenance_health(&self) -> crate::SkillMaintenanceProjectionHealth {
        let worker_running = self
            .event_reactor
            .lane_health(crate::skill::maintenance::PROJECTOR_ID)
            .ok()
            .flatten()
            .is_some_and(|health| health.worker_running);
        self.skill_maintenance_projector
            .health_with_worker(worker_running)
    }

    pub fn request_skill_revision_activation(
        &self,
        principal: &crate::VerifiedPrincipal,
        request_id: &str,
        draft_id: &str,
        target_revision: &str,
        validation_digest: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        let draft = self
            .skill_maintenance_draft(draft_id)?
            .ok_or_else(|| RuntimeServicesError::Invariant("skill Draft not found".to_string()))?;
        self.skill_revision_governance
            .request_activation(
                principal,
                request_id,
                &draft,
                target_revision,
                validation_digest,
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn request_skill_revision_rollback(
        &self,
        principal: &crate::VerifiedPrincipal,
        request_id: &str,
        skill_id: &str,
        target_revision: &str,
        evidence_digest: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        self.skill_revision_governance
            .request_rollback(
                principal,
                request_id,
                skill_id,
                target_revision,
                evidence_digest,
            )
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn skill_revision_review(
        &self,
        review_id: &str,
    ) -> Result<harness_contract::skill::SkillRevisionReview, RuntimeServicesError> {
        self.skill_revision_governance
            .review(review_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn decide_skill_revision_review(
        &self,
        principal: &crate::VerifiedPrincipal,
        lease: &crate::VerifiedDecisionLease,
        review_id: &str,
        decision: harness_contract::skill::SkillRevisionReviewDecision,
        reason: &str,
    ) -> Result<Option<harness_contract::skill::SkillActivePointer>, RuntimeServicesError> {
        self.skill_revision_governance
            .decide_review(principal, lease, review_id, decision, reason)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    }

    pub fn skill_active_pointer(
        &self,
        skill_id: &str,
    ) -> Result<Option<harness_contract::skill::SkillActivePointer>, RuntimeServicesError> {
        self.skill_revision_governance
            .pointer(skill_id)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
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
    pub(crate) fn evaluation_provider_token_leases(
        &self,
    ) -> &Arc<crate::conversation::EvaluationProviderTokenLeaseRegistry> {
        &self.evaluation_provider_token_leases
    }
    pub fn session_turn_admission(&self) -> crate::SessionTurnAdmissionPort {
        crate::SessionTurnAdmissionPort::new(Arc::clone(&self.resource_manager))
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
    #[must_use]
    pub fn resource_evidence_writer_health(
        &self,
    ) -> crate::execution_core::ResourceEvidenceWriterHealth {
        self.resource_evidence_writer.health()
    }
    #[must_use]
    pub fn provider_resource_config(&self) -> Arc<RwLock<crate::ProviderResourceConfig>> {
        Arc::clone(&self.provider_resource_config)
    }
    pub fn replace_provider_resource_config(
        &self,
        config: crate::ProviderResourceConfig,
    ) -> Result<Vec<crate::execution_core::graph::ExecutionResourceSnapshot>, String> {
        config.validate()?;
        let generation = config.materialize(&self.provider_registry.pin());
        let mut quotas = default_resource_quotas();
        quotas.retain(|(kind, _)| {
            !matches!(
                kind,
                ExecutionResourceKind::Provider
                    | ExecutionResourceKind::ProviderAccount(_)
                    | ExecutionResourceKind::ProviderModel(_)
                    | ExecutionResourceKind::ProviderTokenPool(_)
            )
        });
        quotas.extend(generation.quotas);
        let snapshots = self
            .resource_manager
            .reconcile_quotas(quotas, generation.reserves)
            .map_err(|error| error.to_string())?;
        *self
            .provider_resource_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = config;
        Ok(snapshots)
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
    pub fn provider_template_cache(&self) -> &Arc<crate::ProviderClientTemplateCache> {
        &self.provider_template_cache
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
    pub(crate) fn session_application_port(
        &self,
    ) -> Option<Arc<dyn crate::SessionRuntimeApplicationPort>> {
        self.session_application_port.get().cloned()
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

const CHILD_EXECUTION_RESOLVER_PROJECTION_ID: &str = "runtime:child-execution-resolver:v1";

fn child_execution_resolution_lane(
    event_store: Arc<RuntimeEventStore>,
    graph_store: ExecutionGraphStateStore,
    supervisor: Arc<crate::RuntimeExecutionSupervisor>,
) -> crate::RuntimeProjectionLane {
    let descriptor = crate::RuntimeProjectionDescriptor::new(
        CHILD_EXECUTION_RESOLVER_PROJECTION_ID,
        crate::RuntimeProjectionInterest::new([crate::RuntimeProjectionEventInterest::new(
            RuntimeEventScope::ExecutionNode,
            "execution_node.transitioned",
        )]),
        256,
        Duration::from_secs(5),
    )
    .expect("child execution resolver descriptor is static and valid");
    crate::RuntimeProjectionLane::asynchronous(descriptor, move |batch_size| {
        let event_store = Arc::clone(&event_store);
        let graph_store = graph_store.clone();
        let supervisor = Arc::clone(&supervisor);
        Box::pin(async move {
            let checkpoint = event_store
                .projection_checkpoint(CHILD_EXECUTION_RESOLVER_PROJECTION_ID)
                .map_err(|error| error.to_string())?;
            let source_cursor = checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.source_cursor);
            let interest = crate::RuntimeProjectionInterest::new([
                crate::RuntimeProjectionEventInterest::new(
                    RuntimeEventScope::ExecutionNode,
                    "execution_node.transitioned",
                ),
            ]);
            let page = event_store
                .projection_scan_page(
                    source_cursor,
                    &interest,
                    batch_size.max(1),
                    10_000,
                    16 * 1024 * 1024,
                )
                .map_err(|error| error.to_string())?;
            if page.scanned_commits == 0 {
                return Ok(crate::RuntimeProjectionPass::default());
            }
            let mut touched_graphs = BTreeSet::new();
            for batch in &page.batches {
                for event in &batch.events {
                    if let Some(graph_id) = event
                        .payload
                        .get("graph_id")
                        .and_then(serde_json::Value::as_str)
                    {
                        touched_graphs.insert(graph_id.to_string());
                    }
                }
            }
            // Resolve from either side of the durable join. A fast child may
            // terminal before the parent commits WaitingExternal; the later
            // parent transition then discovers the same child via lineage.
            for graph_id in &touched_graphs {
                supervisor
                    .wake_parent_for_settled_child(graph_id)
                    .await
                    .map_err(|error| error.to_string())?;
                for link in graph_store
                    .child_links(graph_id)
                    .map_err(|error| error.to_string())?
                {
                    supervisor
                        .wake_parent_for_settled_child(&link.child_execution_id)
                        .await
                        .map_err(|error| error.to_string())?;
                }
            }
            // The checkpoint advances only after every matching join was
            // either atomically resolved or proven inapplicable/terminal.
            let expected_revision = checkpoint.as_ref().map_or(0, |value| value.revision);
            event_store
                .compare_and_put_projection_checkpoint(
                    CHILD_EXECUTION_RESOLVER_PROJECTION_ID,
                    page.scanned_through_cursor,
                    expected_revision,
                    &serde_json::json!({
                        "source_cursor": page.scanned_through_cursor,
                        "resolved_graphs": touched_graphs.len(),
                    }),
                    now_ms(),
                )
                .map_err(|error| error.to_string())?;
            Ok(
                crate::RuntimeProjectionPass::scanned(page.scanned_commits, batch_size)
                    .with_matches(page.matched_events),
            )
        })
    })
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
        harness_contract::outcome::OutcomeTerminalClass::PartialFailure(
            "Team graph contains failed nodes; committed evidence is retained".to_string(),
        )
    } else if has_blocked {
        harness_contract::outcome::OutcomeTerminalClass::PartialFailure(
            "Team graph contains blocked nodes; unresolved work is retained".to_string(),
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
    let leaf_usage = aggregate_team_leaf_usage(&graph);
    let duration_ms = leaf_usage.duration_ms;
    let usage = harness_contract::outcome::OutcomeUsage {
        input_tokens: Some(leaf_usage.input_tokens),
        output_tokens: Some(leaf_usage.output_tokens),
        cached_tokens: Some(leaf_usage.cached_tokens),
        evaluation_tokens: None,
        tool_calls: leaf_usage.tool_calls,
        duplicate_tool_calls: leaf_usage.duplicate_tool_calls,
        retries: 0,
        max_observed_concurrency: leaf_usage.max_tool_concurrency_observed,
    };
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
            build: Default::default(),
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
        strategy_feedback: harness_contract::outcome::OutcomeStrategyFeedback {
            evaluation_environment: if packet.session_id().starts_with("evolution-eval:") {
                "evolution_evaluation".to_string()
            } else {
                "production".to_string()
            },
            ..Default::default()
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
    let provider_model_refs = (!returned.provider.trim().is_empty()
        || !returned.model.trim().is_empty())
    .then(|| format!("{}/{}", returned.provider, returned.model))
    .into_iter()
    .collect::<Vec<_>>();
    let environment_fingerprint = format!(
        "sha256:{:x}",
        Sha256::digest(
            serde_json::to_vec(&serde_json::json!({
                "provider": returned.provider,
                "model": returned.model,
                "permission_ceiling": packet.permission_ceiling,
                "allowed_tools": scenario.allowed_tools,
                "allowed_skills": scenario.allowed_skills,
                "resource_scopes": scenario.resource_scopes,
            }))
            .unwrap_or_default()
        )
    );
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
        provider_model_refs,
        environment_fingerprint,
    }
}

fn evolution_team_request(
    candidate: &crate::EvolutionGovernanceCandidate,
    scenario: &EvaluationScenarioSpec,
    revision_ref: &TeamTemplateRevisionRef,
    side: &str,
    sample_index: u32,
    mission_id: &str,
    execution_capacity: harness_contract::team::TeamExecutionCapacitySnapshot,
) -> TeamInstantiationRequest {
    let identity = format!(
        "evolution-eval:{}:{}:{}:{}:{}",
        candidate.candidate_id, scenario.scenario_ref, side, revision_ref.revision, sample_index
    );
    let deadline_at_ms =
        now_ms().saturating_add(harness_contract::agent::DEFAULT_DELEGATED_EXECUTION_TIMEOUT_MS);
    TeamInstantiationRequest {
        request_id: format!("{identity}:request"),
        team_id: format!("{identity}:team"),
        mission_id: mission_id.to_string(),
        lineage: harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: format!("evolution-eval:{}", candidate.candidate_id),
            turn_id: format!("{identity}:turn"),
            root_task_id: format!("{identity}:root-task"),
            task_id: format!("{identity}:root-task"),
            generation: 1,
        },
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
        display_name: None,
        role_display_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        requires_managed_collaboration_escalation: false,
        permission_ceiling: scenario.permission_ceiling.clone(),
        model_lease: scenario.model_lease.clone(),
        execution_budget: crate::team_instantiation::bounded_parent_execution_budget(
            format!("evolution-eval-budget:{identity}"),
            65_536,
            deadline_at_ms,
            32,
        ),
        deadline_at_ms,
        managed_invocation: None,
        resource_scopes: scenario.resource_scopes.clone(),
        allow_whole_workspace_scope: false,
        upstream_evidence_refs: Vec::new(),
        upstream_artifact_refs: Vec::new(),
        upstream_result_context: Vec::new(),
        execution_capacity: Some(execution_capacity),
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
    let mut provider_model_refs = matched
        .iter()
        .filter(|evaluation| {
            !evaluation.provider.trim().is_empty() || !evaluation.model.trim().is_empty()
        })
        .map(|evaluation| format!("{}/{}", evaluation.provider, evaluation.model))
        .collect::<Vec<_>>();
    provider_model_refs.sort();
    provider_model_refs.dedup();
    let mut environment_inputs = matched
        .iter()
        .map(|evaluation| evaluation.environment_fingerprint.clone())
        .collect::<Vec<_>>();
    environment_inputs.sort();
    environment_inputs.dedup();
    let environment_fingerprint = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&environment_inputs).unwrap_or_default())
    );
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
        provider_model_refs,
        environment_fingerprint,
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
                minimum: 2,
                target: 32,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Provider,
            ResourceQuota {
                minimum: 8,
                target: 64,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Agent,
            ResourceQuota {
                minimum: 4,
                target: 64,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Tool,
            ResourceQuota {
                minimum: 4,
                target: 64,
                maximum: 256,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.process".to_string()),
            ResourceQuota {
                minimum: 2,
                target: 16,
                maximum: 64,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.network".to_string()),
            ResourceQuota {
                minimum: 2,
                target: 32,
                maximum: 128,
            },
        ),
        (
            ExecutionResourceKind::Custom("tool.cpu".to_string()),
            ResourceQuota {
                minimum: 2,
                target: 64,
                maximum: 256,
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
            "partial"
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
