//! Workspace-owned runtime service graph.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use harness_contract::agent::{
    AgentEvaluationBinding, AgentReleaseBinding, AgentTaskIntent, AgentTaskPacket,
    AgentTerminalStatus, ReleaseChannel, RevisionSelector,
};
use harness_contract::context::ContextBudgetLeaseRef;
use harness_contract::evaluation::{EvaluationScenarioObservation, EvaluationScenarioSpec};
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
    ExecutionStateStoreError, NodeExecutor, NodeExecutorError, NodeExecutorRegistry, ResourceQuota,
    ScopeLockError, ScopeLockManager, WorktreeLeaseError, WorktreeLeaseManager,
};
use super::protocols::ProtocolResultReducer;
use crate::agent::binding::request_for_intent;
use crate::agent::definition::ExplicitTomlAgentImport;
use crate::runtime_event_store::RuntimeEventStoreError;
use crate::{
    AgentBindingCompiler, AgentBindingRequest, AgentDefinitionDraftReceipt, AgentRuntime,
    AgentRuntimeResolver, ApprovalQueue, CompiledAgentBinding, ConflictArbiter,
    DefinitionRegistryError, DurableRuntimeEvent, InProcessAgentWorker,
    ManagedAgentRuntimeDispatchReport, MissionEvidenceBus, MissionRuntime, MissionScheduleStore,
    ProcessJsonlAdapter, RealityRecallPort, RuntimeDefinitionRegistry, RuntimeEventInput,
    RuntimeEventReplayer, RuntimeEventScope, RuntimeEventStore, RuntimeSessionOutboxFailureClass,
    RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord, SessionInputRouter,
    SessionRelationGraph, TeamResultReducer, TeamRuntime,
};

#[derive(Debug, Error)]
pub enum RuntimeServicesError {
    #[error(transparent)]
    DefinitionRegistry(#[from] DefinitionRegistryError),
    #[error(transparent)]
    EventStore(#[from] RuntimeEventStoreError),
    #[error(transparent)]
    WorktreeLease(#[from] WorktreeLeaseError),
    #[error(transparent)]
    ScopeLock(#[from] ScopeLockError),
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
    #[error("agent runtime initialization failed: {0}")]
    AgentRuntime(String),
    #[error("session input router was concurrently installed")]
    DuplicateSessionRouter,
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
    builtin_definitions_root: Option<PathBuf>,
    resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
    provider_registry: Arc<crate::ProviderRegistry>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    session_store: Option<Arc<memory::UnifiedSessionStore>>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
    skill_catalog: crate::RuntimeSkillCatalog,
    mission_schedule_policy: crate::MissionSchedulePolicy,
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
        from_sequence: u64,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut events = self
            .store
            .list_stream(session_id)?
            .into_iter()
            .filter(|event| event.sequence >= from_sequence)
            .collect::<Vec<_>>();
        events.extend(
            self.store
                .execution_events_for_session(session_id, 0, limit)?,
        );
        events.sort_by_key(|event| (event.created_at_ms, event.sequence));
        events.dedup_by(|left, right| {
            left.event_id == right.event_id
                || (left.stream_id == right.stream_id
                    && left.sequence == right.sequence
                    && left.kind == right.kind)
        });
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
}

/// The only task-lifecycle families that the Gateway task projection may
/// persist.  Arbitrary caller supplied runtime event kinds are intentionally
/// not accepted at this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskLifecycleKind {
    Started,
    PhaseStarted,
    PhaseArtifactRecorded,
    PhaseReviewed,
    Cancelled,
    Completed,
    FailureRecorded,
    Blocked,
}

impl TaskLifecycleKind {
    #[must_use]
    pub const fn event_kind(self) -> &'static str {
        match self {
            Self::Started => "task.started",
            Self::PhaseStarted => "task.phase.started",
            Self::PhaseArtifactRecorded => "task.phase.artifact.recorded",
            Self::PhaseReviewed => "task.phase.reviewed",
            Self::Cancelled => "task.cancelled",
            Self::Completed => "task.completed",
            Self::FailureRecorded => "task.failure.recorded",
            Self::Blocked => "task.blocked",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskLifecycleEvent {
    pub task_id: String,
    pub kind: TaskLifecycleKind,
    pub payload: serde_json::Value,
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

    #[must_use]
    pub fn tool_execution_host(mut self, host: Arc<dyn crate::RuntimeExecutionHost>) -> Self {
        self.tool_execution_host = Some(host);
        self
    }

    #[must_use]
    pub fn session_store(mut self, store: Arc<memory::UnifiedSessionStore>) -> Self {
        self.session_store = Some(store);
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

    pub fn build(self) -> Result<Arc<RuntimeServices>, RuntimeServicesError> {
        if self.cowd_home.as_os_str().is_empty() || self.workspace_root.as_os_str().is_empty() {
            return Err(RuntimeServicesError::EmptyRoot);
        }
        let legacy_team_state_path = self
            .cowd_home
            .join("agents")
            .join("team-runtime")
            .join("state.json");
        let legacy_team_profile_path = self.cowd_home.join("agents").join("team-profiles.json");
        let legacy_team_profile_archive_root = self.cowd_home.join("migrations").join("teams");
        let workspace_root = canonical_workspace_root(&self.workspace_root)?;
        let workspace_key = workspace_key(&workspace_root);
        let storage_layout = storage::StorageLayout::default_for_config_home(&self.cowd_home);
        let builtin_definitions_root = self.builtin_definitions_root.unwrap_or_else(|| {
            // An unconfigured installation has no runnable builtin Definitions
            // yet. This explicit empty bundle root preserves scope separation;
            // the launcher supplies the verified release-bundle root before
            // builtin bootstrap is enabled.
            self.cowd_home.join("runtime").join("builtin-definitions")
        });
        let definition_registry = Arc::new(RuntimeDefinitionRegistry::from_storage_layout(
            &storage_layout,
            builtin_definitions_root,
            &workspace_root,
        )?);
        let state_root = self
            .cowd_home
            .join("runtime")
            .join("workspaces")
            .join(&workspace_key);
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
            Arc::new(RuntimeEventStore::try_open(
                state_root.join("runtime-events.sqlite"),
            )?),
            worktree_leases,
            scope_locks,
            self.resource_quotas,
            self.provider_registry,
            self.tool_execution_host,
            self.memory_manager,
            self.evolution_eval_runner,
            self.skill_catalog,
            self.mission_schedule_policy,
            definition_registry,
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
        if let Some(store) = self.session_store {
            services.install_session_store(store)?;
        }
        Ok(services)
    }
}

pub struct RuntimeServices {
    workspace_root: PathBuf,
    workspace_key: String,
    event_store: Arc<RuntimeEventStore>,
    executor_registry: Arc<NodeExecutorRegistry>,
    model_step_executor: Arc<ScopedNodeExecutor>,
    tool_batch_executor: Arc<ScopedNodeExecutor>,
    cross_plane_connector_executor: Arc<ScopedNodeExecutor>,
    agent_task_executor: Arc<AgentTaskExecutor>,
    agent_runtime: Arc<AgentRuntime>,
    team_runtime: Arc<TeamRuntime>,
    l4_promotion_service: Arc<crate::L4PromotionService>,
    verify_executor: Arc<VerifyNodeExecutor>,
    synthesize_executor: Arc<SynthesizeNodeExecutor>,
    graph_state_store: ExecutionGraphStateStore,
    commit_service: ExecutionCommitService,
    graph_runner: Arc<ExecutionGraphRunner>,
    approval_queue: Arc<ApprovalQueue>,
    evolution_governance: Arc<crate::EvolutionGovernanceService>,
    mission_evidence: Arc<MissionEvidenceBus>,
    conflict_resolver: Arc<ConflictArbiter>,
    resource_manager: Arc<ExecutionResourceManager>,
    scope_locks: Arc<ScopeLockManager>,
    worktree_leases: Arc<WorktreeLeaseManager>,
    definition_registry: Arc<RuntimeDefinitionRegistry>,
    cross_plane: Arc<CrossPlaneRuntimeService>,
    mission_runtime: Arc<MissionRuntime>,
    mission_schedules: Arc<MissionScheduleStore>,
    managed_agents: Arc<crate::ManagedAgentDispatcher>,
    mission_schedule_policy: Arc<RwLock<crate::MissionSchedulePolicy>>,
    session_relations: Arc<SessionRelationGraph>,
    goal_store: Arc<GoalStore>,
    provider_registry: Arc<crate::ProviderRegistry>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
    skill_catalog: Arc<RwLock<crate::RuntimeSkillCatalog>>,
    reality_recall_port: Arc<RealityRecallPort>,
    session_dispatch_executor: Arc<crate::session_execution::SessionDispatchNodeExecutor>,
    session_input_router: OnceLock<Arc<SessionInputRouter>>,
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
            builtin_definitions_root: None,
            resource_quotas: default_resource_quotas(),
            provider_registry: Arc::new(crate::ProviderRegistry::empty()),
            tool_execution_host: None,
            session_store: None,
            memory_manager: None,
            evolution_eval_runner: None,
            skill_catalog: crate::RuntimeSkillCatalog::default(),
            mission_schedule_policy: crate::MissionSchedulePolicy::default(),
        }
    }

    pub fn in_memory() -> Result<Arc<Self>, RuntimeServicesError> {
        let workspace_key = format!("in-memory-{}", uuid::Uuid::new_v4());
        let workspace_root = PathBuf::from(format!("/{workspace_key}"));
        let definition_root = std::env::temp_dir()
            .join("cowd-runtime-services")
            .join(&workspace_key)
            .join("definitions");
        let config_home = definition_root.join("config-home");
        let definition_registry = Arc::new(RuntimeDefinitionRegistry::from_storage_layout(
            &storage::StorageLayout::default_for_config_home(&config_home),
            definition_root.join("builtin"),
            &workspace_root,
        )?);
        let services = Arc::new(Self::assemble(
            config_home,
            workspace_root,
            workspace_key.clone(),
            Arc::new(RuntimeEventStore::try_open_in_memory()?),
            Arc::new(WorktreeLeaseManager::open(
                std::env::temp_dir()
                    .join("cowd-runtime-services")
                    .join(&workspace_key)
                    .join("worktree-leases.json"),
            )?),
            Arc::new(ScopeLockManager::new()),
            default_resource_quotas(),
            Arc::new(crate::ProviderRegistry::empty()),
            None,
            None,
            None,
            crate::RuntimeSkillCatalog::default(),
            crate::MissionSchedulePolicy::default(),
            definition_registry,
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
        tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
        memory_manager: Option<Arc<memory::CognitiveContextManager>>,
        evolution_eval_runner: Option<Arc<dyn crate::EvolutionEvalRunner>>,
        skill_catalog: crate::RuntimeSkillCatalog,
        mission_schedule_policy: crate::MissionSchedulePolicy,
        definition_registry: Arc<RuntimeDefinitionRegistry>,
    ) -> Result<Self, RuntimeServicesError> {
        let executor_registry = Arc::new(NodeExecutorRegistry::new());
        let graph_state_store = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let model_step_executor = Arc::new(ScopedNodeExecutor::new("inline_model"));
        let tool_batch_executor = Arc::new(ScopedNodeExecutor::new("tool_batch"));
        let cross_plane_connector_executor =
            Arc::new(ScopedNodeExecutor::new("cross_plane_connector"));
        let agent_task_executor = Arc::new(AgentTaskExecutor::new());
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
        let team_runtime = Arc::new(TeamRuntime::new(
            Arc::clone(&graph_runner),
            graph_state_store.clone(),
            Arc::clone(&agent_runtime),
            Arc::clone(&event_store),
            Arc::clone(&definition_registry),
            Arc::clone(&resource_manager),
            Arc::clone(&evolution_governance),
        ));
        let l4_promotion_service = Arc::new(crate::L4PromotionService::new(
            Arc::clone(&event_store),
            memory_manager.clone(),
        ));
        let gate_store = Arc::clone(&event_store);
        let gate_workspace = workspace_key.clone();
        let gate_root = workspace_root.clone();
        graph_runner.install_mutation_gate(move || {
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
        let mission_runtime =
            MissionRuntime::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?;
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
        managed_agents
            .recover(now_ms())
            .map_err(RuntimeServicesError::Mission)?;
        let session_relations = Arc::new(
            SessionRelationGraph::event_sourced(Arc::clone(&event_store), workspace_key.clone())
                .map_err(RuntimeServicesError::Mission)?,
        );
        Ok(Self {
            workspace_root,
            workspace_key: workspace_key.clone(),
            event_store: Arc::clone(&event_store),
            executor_registry,
            model_step_executor,
            tool_batch_executor,
            cross_plane_connector_executor,
            agent_task_executor,
            agent_runtime,
            team_runtime,
            l4_promotion_service,
            verify_executor,
            synthesize_executor,
            graph_state_store,
            commit_service,
            graph_runner,
            approval_queue,
            evolution_governance,
            mission_evidence,
            conflict_resolver,
            resource_manager,
            scope_locks,
            worktree_leases,
            definition_registry,
            cross_plane: Arc::new(CrossPlaneRuntimeService::open(Arc::clone(&event_store))?),
            mission_runtime: Arc::new(mission_runtime),
            mission_schedules,
            managed_agents,
            mission_schedule_policy: Arc::new(RwLock::new(mission_schedule_policy)),
            session_relations,
            goal_store,
            provider_registry,
            tool_execution_host,
            memory_manager,
            evolution_eval_runner,
            skill_catalog: Arc::new(RwLock::new(skill_catalog)),
            reality_recall_port: Arc::new(RealityRecallPort::for_config_home(cowd_home)),
            session_dispatch_executor,
            session_input_router: OnceLock::new(),
        })
    }

    pub fn install_session_store(
        self: &Arc<Self>,
        store: Arc<memory::UnifiedSessionStore>,
    ) -> Result<Arc<SessionInputRouter>, RuntimeServicesError> {
        if let Some(router) = self.session_input_router.get() {
            return Ok(Arc::clone(router));
        }
        let router =
            SessionInputRouter::install(store, &self.workspace_key, Arc::clone(&self.event_store))?;
        self.session_dispatch_executor
            .install_router(Arc::clone(&router))
            .map_err(RuntimeServicesError::Mission)?;
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

    /// Runtime-owned read port for Fact and Matrix context. Each call requires
    /// a Binding and verifies its data lease before exposing model context.
    #[must_use]
    pub fn reality_recall_port(&self) -> &Arc<RealityRecallPort> {
        &self.reality_recall_port
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
        let selected = intent
            .selected_agent_id
            .as_deref()
            .and_then(|agent_id| self.agent_runtime.catalog().get(agent_id));
        let request = request_for_intent(&intent, selected)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
        let compiled = self.compile_agent_binding(request)?;
        compiled
            .snapshot
            .compile_task_packet(intent)
            .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))
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

    /// Read-only durable event projection for Gateway and Surface consumers.
    #[must_use]
    pub fn event_reader(&self) -> RuntimeEventReader {
        RuntimeEventReader {
            store: Arc::clone(&self.event_store),
        }
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

    /// Persist a task lifecycle transition through the Runtime-owned task
    /// writer.  Gateway supplies task projection data but cannot choose an
    /// arbitrary ledger scope, actor or event family.
    pub fn record_task_lifecycle(
        &self,
        event: TaskLifecycleEvent,
    ) -> Result<DurableRuntimeEvent, String> {
        if event.task_id.trim().is_empty() {
            return Err("task lifecycle event requires task_id".to_string());
        }
        self.event_store.append(RuntimeEventInput {
            stream_id: event.task_id,
            scope: RuntimeEventScope::Task,
            kind: event.kind.event_kind().to_string(),
            status: None,
            actor: Some("gateway-task-command".to_string()),
            refs: Vec::new(),
            payload: event.payload,
        })
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
    pub fn graph_runner(&self) -> &Arc<ExecutionGraphRunner> {
        &self.graph_runner
    }
    pub fn approval_queue(&self) -> &Arc<ApprovalQueue> {
        &self.approval_queue
    }
    /// Runtime is the single owner of evolution candidates, evaluation
    /// eligibility and release-change review projections. Gateway and
    /// surfaces consume this service rather than keeping a second registry.
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
                    .read_revision(&revision_ref)
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
                    .read_revision(&revision_ref)
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
                    .read_revision(&revision_ref)
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
                    .read_revision(&revision_ref)
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
        self.evolution_governance
            .register_candidate(crate::EvolutionCandidateRegistration {
                candidate_id: intent.candidate_id,
                subject: intent.subject,
                baseline_revision: intent.baseline_revision,
                evaluation_contract,
                source_evidence_refs: intent.source_evidence_refs,
                canary_policy: intent.canary_policy,
            })
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
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
        let runner = self.evolution_eval_runner.as_ref().ok_or_else(|| {
            RuntimeServicesError::Invariant("evolution_evaluator_not_configured".to_string())
        })?;
        let report = runner
            .evaluate(&candidate)
            .await
            .map_err(RuntimeServicesError::Invariant)?;
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
        validate_evolution_scenario_isolation(scenario)?;
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
        validate_evolution_scenario_isolation(scenario)?;
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
        );
        let candidate_request = evolution_team_request(
            &candidate,
            scenario,
            revision_ref,
            "candidate",
            sample_index,
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
        compiled
            .snapshot
            .compile_task_packet(AgentTaskIntent {
                selected_agent_id: None,
                definition_ref: Some(revision_ref),
                granted_capabilities: Vec::new(),
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                session_id,
                mission_id: None,
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
            })
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
        let resolved_handoff_results = self.resolve_durable_handoff_results().await?;
        let graph_ids = self.graph_state_store.nonterminal_graph_ids_async().await?;
        let mut report = ExecutionStartupRecoveryReport {
            examined_graphs: graph_ids.len(),
            resolved_handoff_results,
            ..ExecutionStartupRecoveryReport::default()
        };
        let recovery = super::graph::ExecutionGraphRecovery::new(
            &self.graph_state_store,
            &self.commit_service,
            &self.executor_registry,
        );

        for graph_id in graph_ids {
            let before = self.graph_state_store.load_async(&graph_id).await?;
            let before_revision = before.revision;
            let before_status = graph_status_label(&before);
            let objective = before.objective.clone();
            let had_running = graph_has_status(&before, ExecutionNodeStatus::Running);
            let mut action = "observed".to_string();
            let mut error = None;

            if had_running {
                match recovery.recover(&graph_id).await {
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

            if error.is_none() {
                let current = self.graph_state_store.load_async(&graph_id).await?;
                if graph_can_advance(&current) {
                    match self.graph_runner.run_until_quiescent(&graph_id).await {
                        Ok(_) => {
                            let advanced = self.graph_state_store.load_async(&graph_id).await?;
                            if advanced.revision != current.revision {
                                report.advanced_graphs += 1;
                                action = if had_running {
                                    "recovered_and_advanced".to_string()
                                } else {
                                    "advanced_ready".to_string()
                                };
                            }
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
        use crate::ExecutionGraphHost;

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
                .graph_runner
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
                crate::MissionInterpretedCommand::SubmitExecutionGraph { graph, .. } => {
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
                    match self.graph_runner.start(graph).await {
                        Ok(report) if report.failed == 0 => {
                            submitted.push(
                                self.mission_schedules
                                    .mark_submitted(&fire.fire_id, graph_id)?,
                            );
                        }
                        Ok(report) => failed.push(self.mission_schedules.mark_failed(
                            &fire.fire_id,
                            format!(
                                "SessionDispatch graph completed with {} failed nodes",
                                report.failed
                            ),
                        )?),
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

    pub fn mission_runtime(&self) -> &Arc<MissionRuntime> {
        &self.mission_runtime
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
        let health_affected = self
            .managed_agents
            .enforce_run_health(now_ms())
            .map_err(RuntimeServicesError::Mission)?;
        let scheduled = self
            .managed_agents
            .accept_due_schedules(now_ms())
            .map_err(RuntimeServicesError::Mission)?;
        let claimed = self
            .managed_agents
            .claim_ready(dispatcher_id, now_ms(), limit)
            .map_err(RuntimeServicesError::Mission)?;
        let mut completed = Vec::new();
        let mut failed = Vec::new();
        for invocation in &claimed {
            match self
                .execute_managed_agent_invocation(dispatcher_id, invocation.clone())
                .await
            {
                Ok(invocation)
                    if invocation.status == crate::ManagedAgentInvocationStatus::Completed =>
                {
                    completed.push(invocation);
                }
                Ok(invocation) => failed.push(invocation),
                Err(error) => {
                    let completed_invocation = self
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
                        .map_err(RuntimeServicesError::Mission)?;
                    failed.push(completed_invocation);
                }
            }
        }
        Ok(ManagedAgentRuntimeDispatchReport {
            health_affected,
            scheduled,
            claimed,
            completed,
            failed,
        })
    }

    async fn execute_managed_agent_invocation(
        &self,
        dispatcher_id: &str,
        invocation: crate::ManagedAgentInvocation,
    ) -> Result<crate::ManagedAgentInvocation, RuntimeServicesError> {
        // Move the durable invocation to Running before resolving the target.
        // Any later definition/binding/executor failure is then completed by
        // `dispatch_managed_agents` through the one retry/failure transition,
        // rather than stranding a claimed reservation after a bad revision.
        self.managed_agents
            .start_invocation(
                &invocation.invocation_id,
                dispatcher_id,
                invocation.fence_generation,
                format!(
                    "managed-dispatch:{}:{}",
                    invocation.invocation_id, invocation.attempt_no
                ),
                now_ms(),
            )
            .map_err(RuntimeServicesError::Mission)?;
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
                    "managed-run:{}:{}",
                    invocation.invocation_id, invocation.attempt_no
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
                let execution_ref = run_id.clone();
                let packet = compiled
                    .snapshot
                    .compile_task_packet(AgentTaskIntent {
                        selected_agent_id: None,
                        definition_ref: Some(compiled.snapshot.definition_ref.clone()),
                        granted_capabilities: Vec::new(),
                        run_id: run_id.clone(),
                        task_id,
                        session_id: definition.session_id.clone(),
                        mission_id: None,
                        team_id: None,
                        graph_id: format!("managed-agent:{}", invocation.invocation_id),
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
                            "managed-agent:{}:{}",
                            invocation.invocation_id, invocation.attempt_no
                        ),
                    })
                    .map_err(|error| RuntimeServicesError::AgentRuntime(error.to_string()))?;
                let returned = self
                    .agent_runtime
                    .execute_task(packet)
                    .await
                    .map_err(RuntimeServicesError::AgentRuntime)?;
                self.managed_agents
                    .complete_invocation(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        returned.status == AgentTerminalStatus::Completed
                            && returned.failure.is_none(),
                        now_ms(),
                        Some(execution_ref),
                        returned
                            .evidence_refs
                            .iter()
                            .map(|reference| reference.evidence_ref.0.id.clone())
                            .collect(),
                        returned.failure,
                    )
                    .map_err(RuntimeServicesError::Mission)
            }
            harness_contract::managed_agent::ManagedAgentTarget::Team {
                template_id,
                selector,
            } => {
                let execution_ref = format!(
                    "managed-team:{}:{}",
                    invocation.invocation_id, invocation.attempt_no
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
                let projection = self
                    .team_runtime
                    .instantiate(TeamInstantiationRequest {
                        request_id: format!(
                            "managed-team-request:{}:{}",
                            invocation.invocation_id, invocation.attempt_no
                        ),
                        team_id: execution_ref.clone(),
                        session_id: definition.session_id.clone(),
                        mission_id: None,
                        parent_execution: None,
                        selection_mode: TeamSelectionMode::Explicit,
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
                    })
                    .await
                    .map_err(RuntimeServicesError::Mission)?;
                let succeeded = projection.status == "completed";
                self.managed_agents
                    .complete_invocation(
                        &invocation.invocation_id,
                        dispatcher_id,
                        invocation.fence_generation,
                        succeeded,
                        now_ms(),
                        Some(execution_ref),
                        vec![format!("team-graph:{}", projection.graph_id)],
                        (!succeeded)
                            .then(|| format!("Team graph terminal status: {}", projection.status)),
                    )
                    .map_err(RuntimeServicesError::Mission)
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
    pub fn tool_execution_host(&self) -> Option<&Arc<dyn crate::RuntimeExecutionHost>> {
        self.tool_execution_host.as_ref()
    }
    pub fn session_input_router(&self) -> Option<&Arc<SessionInputRouter>> {
        self.session_input_router.get()
    }

    /// Return the workspace's canonical Session authority when it is
    /// installed. In-process agents use this exact store for durable raw
    /// evidence; they never create a second session database.
    #[must_use]
    pub fn session_store(&self) -> Option<Arc<memory::UnifiedSessionStore>> {
        self.session_input_router
            .get()
            .map(|router| router.session_store())
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
            crate::ToolSafetyCategory::from_tool_name(tool) != crate::ToolSafetyCategory::ReadOnly
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
        .map(|evidence| evidence.evidence_ref.id().to_string())
        .collect::<Vec<_>>();
    evidence_refs.sort();
    evidence_refs.dedup();
    EvaluationScenarioObservation {
        scenario_ref: scenario.scenario_ref.clone(),
        definition_revision: packet
            .binding
            .as_ref()
            .map(|binding| binding.definition_ref.revision)
            .unwrap_or_default(),
        run_ref: format!("agent-run:{}", packet.run_id),
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
) -> TeamInstantiationRequest {
    let identity = format!(
        "evolution-eval:{}:{}:{}:{}:{}",
        candidate.candidate_id, scenario.scenario_ref, side, revision_ref.revision, sample_index
    );
    TeamInstantiationRequest {
        request_id: format!("{identity}:request"),
        team_id: format!("{identity}:team"),
        session_id: format!("evolution-eval:{}", candidate.candidate_id),
        mission_id: None,
        parent_execution: None,
        selection_mode: TeamSelectionMode::Explicit,
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
        resource_scopes: Vec::new(),
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
                .map(|evidence| evidence.evidence_ref.id().to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    evidence_refs.extend(
        matched
            .iter()
            .flat_map(|evaluation| evaluation.evidence_refs.clone()),
    );
    evidence_refs.sort();
    evidence_refs.dedup();
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
    use memory::SessionRecord;

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

    impl crate::RuntimeExecutionHost for TestExecutionHost {
        fn execute_runtime_tool(
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
            Ok(AgentReturnPacket {
                run_id: packet.run_id,
                agent_id: packet.agent_id,
                task_id: packet.task_id,
                session_id: packet.session_id,
                mission_id: packet.mission_id,
                team_id: packet.team_id,
                graph_id: packet.graph_id,
                node_id: packet.node_id,
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: "verified agent result".into(),
                acceptance: vec!["completed".into()],
                evidence_refs: Vec::new(),
                changes: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 5,
                output_tokens: 3,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 0,
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
            Ok(AgentReturnPacket {
                run_id: packet.run_id,
                agent_id: packet.agent_id,
                task_id: packet.task_id,
                session_id: packet.session_id,
                mission_id: packet.mission_id,
                team_id: packet.team_id,
                graph_id: packet.graph_id,
                node_id: packet.node_id,
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: "parallel agent result".into(),
                acceptance: vec!["completed".into()],
                evidence_refs: Vec::new(),
                changes: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 5,
                output_tokens: 3,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 0,
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
        let candidate = services
            .register_evolution_candidate(crate::EvolutionCandidateIntent {
                candidate_id: "candidate-canary-v2".to_string(),
                subject: crate::EvolutionCandidateSubject::AgentDefinition {
                    revision_ref: candidate_revision.revision.revision_ref.clone(),
                },
                baseline_revision: 1,
                source_evidence_refs: vec!["agent-run:baseline".to_string()],
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
                evidence_refs: vec!["evidence:paired".to_string()],
                created_at_ms: 1,
            })
            .expect("eligible comparison");
        let review = services
            .request_evolution_canary_review(&candidate.candidate_id)
            .expect("canary review");
        let principal = crate::security::test_human_interactive_principal();
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
                evidence_refs: vec!["incident:fixture".to_string()],
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
    fn workspace_builders_isolate_provider_tool_host_and_session_router() {
        let temp = tempfile::tempdir().unwrap();
        let left_provider = Arc::new(crate::ProviderRegistry::empty());
        let right_provider = Arc::new(crate::ProviderRegistry::empty());
        let left_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
        let right_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
        let left_store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let right_store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        std::fs::create_dir_all(temp.path().join("left")).unwrap();
        std::fs::create_dir_all(temp.path().join("right")).unwrap();

        let left = RuntimeServices::builder(temp.path(), temp.path().join("left"))
            .provider_registry(Arc::clone(&left_provider))
            .tool_execution_host(Arc::clone(&left_tool))
            .session_store(Arc::clone(&left_store))
            .build()
            .unwrap();
        let right = RuntimeServices::builder(temp.path(), temp.path().join("right"))
            .provider_registry(Arc::clone(&right_provider))
            .tool_execution_host(Arc::clone(&right_tool))
            .session_store(Arc::clone(&right_store))
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
        assert!(Arc::ptr_eq(
            &left.session_store().expect("left session store"),
            &left_store
        ));
        assert!(Arc::ptr_eq(
            &right.session_store().expect("right session store"),
            &right_store
        ));
    }

    #[tokio::test]
    async fn due_schedule_submits_one_durable_handoff_graph_and_never_duplicates_it() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
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
        services.install_session_store(Arc::clone(&store)).unwrap();
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
            services.graph_runner().start(graph).await,
            Err(super::super::graph::ExecutionRunnerError::MutationBlocked(
                _
            ))
        ));
        assert!(matches!(
            services.graph_runner().run_until_quiescent(&graph_id).await,
            Err(super::super::graph::ExecutionRunnerError::MutationBlocked(
                _
            ))
        ));
        assert!(matches!(
            services.recover_execution_graphs_on_startup().await,
            Err(RuntimeServicesError::UpgradeRecoveryRequired)
        ));
        assert!(matches!(
            services
                .graph_runner()
                .command(
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
        assert_eq!(report.advanced_graphs, 1);
        assert_eq!(report.terminal_graphs, 1);
        assert!(report.errors.is_empty());
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
            run_id: "agent-runtime-run".into(),
            task_id: "agent-runtime-task".into(),
            session_id: "agent-runtime-session".into(),
            mission_id: None,
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

        let report = services
            .graph_runner()
            .start(graph)
            .await
            .expect("run graph");
        assert_eq!(report.completed, 1);
        let graph = services.graph_state_store().load(&report.graph_id).unwrap();
        assert_eq!(
            graph.node_statuses.get(&packet.node_id),
            Some(&ExecutionNodeStatus::Completed)
        );
        let agent = services
            .agent_runtime()
            .get(&packet.agent_id)
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
        assert_eq!(binding.data_lease.session_id, packet.session_id);
        assert_eq!(binding.data_lease.task_id, packet.task_id);
        assert_eq!(services.agent_runtime().events(&packet.agent_id).len(), 3);
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
                selected_agent_id: None,
                definition_ref: None,
                granted_capabilities: Vec::new(),
                run_id: format!("binding-run-{index}"),
                task_id: format!("binding-task-{index}"),
                session_id: "binding-session".to_string(),
                mission_id: None,
                team_id: Some("binding-team".to_string()),
                graph_id: graph.id.clone(),
                node_id: node_id.clone(),
                attempt: 1,
                expected_graph_revision: 0,
                objective: format!("research isolated domain {index}"),
                acceptance: vec!["evidence".to_string()],
                constraints: vec![format!("role_slot:researcher-{index}")],
                context_refs: Vec::new(),
                evidence_refs: Vec::new(),
                allowed_tools: Vec::new(),
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

        let report = services
            .graph_runner()
            .start(graph)
            .await
            .expect("run graph");
        assert_eq!(report.completed, 8);
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
                            output_contract: vec!["findings".to_string(), "evidence".to_string()],
                        },
                        harness_contract::team::FocusPartitionSlot {
                            focus_id: "architecture-b".to_string(),
                            boundary: "only architecture-b".to_string(),
                            evidence_responsibility: "source evidence for architecture-b"
                                .to_string(),
                            output_contract: vec!["findings".to_string(), "evidence".to_string()],
                        },
                        harness_contract::team::FocusPartitionSlot {
                            focus_id: "architecture-c".to_string(),
                            boundary: "only architecture-c".to_string(),
                            evidence_responsibility: "source evidence for architecture-c"
                                .to_string(),
                            output_contract: vec!["findings".to_string(), "evidence".to_string()],
                        },
                    ],
                }],
                ..team_request(
                    "team-runtime-fanout",
                    "team-runtime-session",
                    "cowd/parallel-research-synthesis",
                    "compare three independent architecture choices",
                    "fast",
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
    ) -> TeamInstantiationRequest {
        TeamInstantiationRequest {
            request_id: format!("test-request-{team_id}"),
            team_id: team_id.to_string(),
            session_id: session_id.to_string(),
            mission_id: None,
            parent_execution: None,
            selection_mode: TeamSelectionMode::Explicit,
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
            permission_lease: "read_only".to_string(),
            model_lease: model_lease.to_string(),
            budget_lease: None,
            managed_invocation: None,
            resource_scopes: Vec::new(),
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
}
