//! Workspace-owned runtime service graph.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeStatus,
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
use crate::runtime_event_store::RuntimeEventStoreError;
use crate::{
    AgentRuntime, AgentRuntimeResolver, ApprovalQueue, ConflictArbiter, InProcessAgentWorker,
    MissionEvidenceBus, MissionRuntime, MissionScheduleStore, ProcessJsonlAdapter,
    RuntimeEventStore, SessionInputRouter, SessionRelationGraph, TeamResultReducer, TeamRuntime,
};

#[derive(Debug, Error)]
pub enum RuntimeServicesError {
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
    resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
    provider_registry: Arc<crate::ProviderRegistry>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
    session_store: Option<Arc<memory::UnifiedSessionStore>>,
    mission_schedule_policy: crate::MissionSchedulePolicy,
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
        let workspace_root = canonical_workspace_root(&self.workspace_root)?;
        let workspace_key = workspace_key(&workspace_root);
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
            self.mission_schedule_policy,
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
        services
            .team_runtime()
            .import_legacy_state_file(&legacy_team_state_path)
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
    verify_executor: Arc<VerifyNodeExecutor>,
    synthesize_executor: Arc<SynthesizeNodeExecutor>,
    graph_state_store: ExecutionGraphStateStore,
    commit_service: ExecutionCommitService,
    graph_runner: Arc<ExecutionGraphRunner>,
    approval_queue: Arc<ApprovalQueue>,
    mission_evidence: Arc<MissionEvidenceBus>,
    conflict_resolver: Arc<ConflictArbiter>,
    resource_manager: Arc<ExecutionResourceManager>,
    scope_locks: Arc<ScopeLockManager>,
    worktree_leases: Arc<WorktreeLeaseManager>,
    cross_plane: Arc<CrossPlaneRuntimeService>,
    mission_runtime: Arc<MissionRuntime>,
    mission_schedules: Arc<MissionScheduleStore>,
    mission_schedule_policy: Arc<RwLock<crate::MissionSchedulePolicy>>,
    session_relations: Arc<SessionRelationGraph>,
    goal_store: Arc<GoalStore>,
    provider_registry: Arc<crate::ProviderRegistry>,
    tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
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
            resource_quotas: default_resource_quotas(),
            provider_registry: Arc::new(crate::ProviderRegistry::empty()),
            tool_execution_host: None,
            session_store: None,
            mission_schedule_policy: crate::MissionSchedulePolicy::default(),
        }
    }

    pub fn in_memory() -> Result<Arc<Self>, RuntimeServicesError> {
        let workspace_key = format!("in-memory-{}", uuid::Uuid::new_v4());
        let workspace_root = PathBuf::from(format!("/{workspace_key}"));
        let services = Arc::new(Self::assemble(
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
            crate::MissionSchedulePolicy::default(),
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
        Ok(services)
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        workspace_root: PathBuf,
        workspace_key: String,
        event_store: Arc<RuntimeEventStore>,
        worktree_leases: Arc<WorktreeLeaseManager>,
        scope_locks: Arc<ScopeLockManager>,
        resource_quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
        provider_registry: Arc<crate::ProviderRegistry>,
        tool_execution_host: Option<Arc<dyn crate::RuntimeExecutionHost>>,
        mission_schedule_policy: crate::MissionSchedulePolicy,
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
            verify_executor,
            synthesize_executor,
            graph_state_store,
            commit_service,
            graph_runner,
            approval_queue,
            mission_evidence,
            conflict_resolver,
            resource_manager,
            scope_locks,
            worktree_leases,
            cross_plane: Arc::new(CrossPlaneRuntimeService::open(Arc::clone(&event_store))?),
            mission_runtime: Arc::new(mission_runtime),
            mission_schedules,
            mission_schedule_policy: Arc::new(RwLock::new(mission_schedule_policy)),
            session_relations,
            goal_store,
            provider_registry,
            tool_execution_host,
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
    pub fn event_store(&self) -> &Arc<RuntimeEventStore> {
        &self.event_store
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
                evidence_refs: vec![format!("schedule-fire:{}", fire.fire_id)],
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
                    match self.graph_runner.start(graph.clone()).await {
                        Ok(report) if report.failed == 0 => {
                            submitted.push(
                                self.mission_schedules
                                    .mark_submitted(&fire.fire_id, graph.id)?,
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

    use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};
    use harness_contract::context::ContextBudgetLeaseRef;
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    };
    use harness_contract::mission::ScheduleTrigger;
    use memory::SessionRecord;

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
        let packet = AgentTaskPacket {
            run_id: "agent-runtime-run".into(),
            agent_id: "agent-runtime-agent".into(),
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
            idempotency_key: "agent-runtime-idempotency".into(),
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
            serde_json::to_string(&packet).unwrap(),
        );
        node.id = packet.node_id.clone();
        node.idempotency_key = packet.idempotency_key.clone();
        node.acceptance.criteria = packet.acceptance.clone();
        graph.nodes.push(node);

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
        assert_eq!(services.agent_runtime().events(&packet.agent_id).len(), 3);
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
            .start(crate::StartTeamRequest {
                team_id: "team-runtime-integration".into(),
                session_id: "team-runtime-session".into(),
                objective: "independently analyse and review the runtime boundary".into(),
                template_id: harness_contract::team::TeamTemplateId::ExecuteReview,
                roles: vec![
                    harness_contract::team::TeamRoleSpec {
                        role_id: "analysis".into(),
                        responsibility: "analyse the boundary".into(),
                        required_capabilities: vec!["analysis".into()],
                        allowed_tools: vec!["read_file".into()],
                        acceptance: vec!["evidence".into()],
                        evidence_duties: vec!["source".into()],
                    },
                    harness_contract::team::TeamRoleSpec {
                        role_id: "review".into(),
                        responsibility: "review the conclusion".into(),
                        required_capabilities: vec!["review".into()],
                        allowed_tools: vec!["read_file".into()],
                        acceptance: vec!["risks".into()],
                        evidence_duties: vec!["review".into()],
                    },
                ],
                role_dependencies: vec![crate::TeamRoleDependency {
                    from_role_id: "analysis".into(),
                    to_role_id: "review".into(),
                }],
                lift_input: crate::CollaborationLiftInput {
                    independent_work_items: 2,
                    domain_count: 2,
                    shared_write_scope: false,
                    review_required: true,
                    provider_healthy: true,
                    budget_allows_parallelism: true,
                    requested_parallelism: 2,
                },
                permission_lease: "read_only".into(),
                model_lease: "fast".into(),
                backend_constraint: None,
            })
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
        let role = |role_id: &str| harness_contract::team::TeamRoleSpec {
            role_id: role_id.into(),
            responsibility: format!("independent {role_id} analysis"),
            required_capabilities: vec!["analysis".into()],
            allowed_tools: vec!["read_file".into()],
            acceptance: vec!["evidence".into()],
            evidence_duties: vec!["source".into()],
        };
        let projection = services
            .team_runtime()
            .start(crate::StartTeamRequest {
                team_id: "team-runtime-fanout".into(),
                session_id: "team-runtime-session".into(),
                objective: "compare three independent architecture choices".into(),
                template_id: harness_contract::team::TeamTemplateId::FanoutResearchSynthesis,
                roles: vec![role("a"), role("b"), role("c")],
                role_dependencies: Vec::new(),
                lift_input: crate::CollaborationLiftInput {
                    independent_work_items: 3,
                    domain_count: 3,
                    shared_write_scope: false,
                    review_required: false,
                    provider_healthy: true,
                    budget_allows_parallelism: true,
                    requested_parallelism: 2,
                },
                permission_lease: "read_only".into(),
                model_lease: "fast".into(),
                backend_constraint: None,
            })
            .await
            .expect("fanout team execution");
        assert_eq!(projection.status, "completed");
        assert!(max_active.load(Ordering::SeqCst) >= 2);
        assert!(max_active.load(Ordering::SeqCst) <= 2);
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
