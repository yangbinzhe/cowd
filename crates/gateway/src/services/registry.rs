use std::sync::Arc;

use super::*;
use crate::runtime_service::RuntimeService;
#[cfg(test)]
use crate::services::session_service::repository::SessionRepository;

pub(crate) type ServiceRegistry = GatewayServices;

impl GatewayServices {
    pub(crate) fn new(
        runtime: Arc<RuntimeService>,
        session_activation: Arc<
            crate::services::session_service::activation::SessionActivationCoordinator,
        >,
        session_supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
    ) -> Self {
        Self::new_with_config_home(
            runtime,
            session_activation,
            session_supervisor,
            surface_host,
            memory_manager,
            ::runtime::cowd_dirs::config_home_dir(),
        )
    }

    pub(crate) fn new_with_config_home(
        runtime: Arc<RuntimeService>,
        session_activation: Arc<
            crate::services::session_service::activation::SessionActivationCoordinator,
        >,
        session_supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        config_home: impl AsRef<std::path::Path>,
    ) -> Self {
        Self::new_with_session_activation(
            runtime,
            surface_host,
            memory_manager,
            session_activation,
            session_supervisor,
            config_home,
            runtime::GatewayCapacityConfig::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_session_activation(
        runtime: Arc<RuntimeService>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        session_activation: Arc<
            crate::services::session_service::activation::SessionActivationCoordinator,
        >,
        session_supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
        config_home: impl AsRef<std::path::Path>,
        capacity_config: runtime::GatewayCapacityConfig,
    ) -> Self {
        Self::new_with_session_activation_inner(
            runtime,
            surface_host,
            memory_manager,
            session_activation,
            session_supervisor,
            config_home,
            capacity_config,
            None,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_session_activation_and_storage(
        runtime: Arc<RuntimeService>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        session_activation: Arc<
            crate::services::session_service::activation::SessionActivationCoordinator,
        >,
        session_supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
        config_home: impl AsRef<std::path::Path>,
        capacity_config: runtime::GatewayCapacityConfig,
        selected_storage: Arc<crate::selected_storage::SelectedStorageTopology>,
    ) -> Self {
        Self::new_with_session_activation_inner(
            runtime,
            surface_host,
            memory_manager,
            session_activation,
            session_supervisor,
            config_home,
            capacity_config,
            Some(selected_storage),
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_bound_session_and_storage(
        runtime: Arc<RuntimeService>,
        session: Arc<SessionService>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        session_activation: Arc<
            crate::services::session_service::activation::SessionActivationCoordinator,
        >,
        session_supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
        config_home: impl AsRef<std::path::Path>,
        capacity_config: runtime::GatewayCapacityConfig,
        selected_storage: Arc<crate::selected_storage::SelectedStorageTopology>,
        growth_projection_services: super::GrowthProjectionServices,
    ) -> Self {
        Self::new_with_session_activation_inner(
            runtime,
            surface_host,
            memory_manager,
            session_activation,
            session_supervisor,
            config_home,
            capacity_config,
            Some(selected_storage),
            Some(session),
            Some(growth_projection_services),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::expect_used,
        reason = "SelectedStorageTopology is constructed only after the Matrix endpoint inventory has been validated"
    )]
    fn new_with_session_activation_inner(
        runtime: Arc<RuntimeService>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        session_activation: Arc<
            crate::services::session_service::activation::SessionActivationCoordinator,
        >,
        session_supervisor: Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
        config_home: impl AsRef<std::path::Path>,
        capacity_config: runtime::GatewayCapacityConfig,
        selected_storage: Option<Arc<crate::selected_storage::SelectedStorageTopology>>,
        session_service: Option<Arc<SessionService>>,
        growth_projection_services: Option<super::GrowthProjectionServices>,
    ) -> Self {
        let config_home = config_home.as_ref().to_path_buf();
        let command_host_runtime = Arc::clone(&runtime);
        let runtime_services = runtime.runtime_services();
        let runtime_events = RuntimeEventService::from_runtime_services(runtime_services.as_ref());
        let task = TaskService::with_runtime(Arc::clone(&runtime_services));
        let session = session_service.unwrap_or_else(|| {
            Arc::new(SessionService::new(
                Arc::clone(&runtime),
                session_activation,
                session_supervisor,
            ))
        });
        let mission = MissionService::new().with_dependencies(
            runtime::MissionRuntimePort::new(Arc::clone(&runtime_services)),
            Arc::clone(&session),
            runtime_events.clone(),
        );
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let warm = mission.clone();
                handle.spawn(async move {
                    if let Err(error) = warm.warm_projection_cache().await {
                        tracing::warn!(%error, "mission projection cache warm-up failed; first request may pay cold-start cost");
                    }
                });
            }
        }
        let capacity = crate::gateway_capacity::GatewayCapacityController::new(
            crate::gateway_capacity::GatewayCapacityConfig::resolve(&capacity_config),
            Arc::clone(runtime_services.resource_manager()),
        );
        let (mut memory, connector, mut growth, mut matrix) =
            if let Some(topology) = selected_storage.as_ref() {
                let matrix_endpoint = topology
                    .registry
                    .endpoint(&storage::StorageDomainId::Matrix)
                    .expect("selected Matrix endpoint")
                    .clone();
                (
                    MemoryService::with_manager_and_knowledge(
                        memory_manager,
                        topology.knowledge_fabric.clone(),
                    ),
                    ConnectorService::with_resource_directory_factory(
                        Arc::clone(&topology.connector_factory),
                        topology.connector_handle.clone(),
                    ),
                    GrowthService::with_ledger(Arc::clone(&topology.fact_ledger)),
                    MatrixService::with_store(Arc::clone(&topology.matrix_store), matrix_endpoint),
                )
            } else {
                (
                    MemoryService::with_manager(memory_manager),
                    ConnectorService::new(),
                    GrowthService::new_for_config_home(&config_home),
                    MatrixService::new(),
                )
            };
        if let Some(shared) = growth_projection_services {
            memory = shared.memory;
            growth = shared.growth;
            matrix = shared.matrix;
        }
        Self {
            selected_storage,
            app_platform: None,
            core_platform_bindings: Default::default(),
            runtime: Some(Arc::clone(&runtime)),
            runtime_events,
            surface: SurfaceService::with_host(surface_host),
            slash: SlashController::new(Some(command_host_runtime), task.clone()),
            session,
            task,
            memory,
            approval: ApprovalService::new()
                .with_runtime(Arc::clone(&runtime))
                .with_runtime_services(Arc::clone(&runtime_services)),
            context: ContextService::new()
                .with_artifact_store(Arc::clone(runtime_services.artifact_store())),
            connector,
            cross_plane: CrossPlaneService::new(Arc::clone(&runtime_services)),
            tool: ToolService::new(),
            system: SystemService::new(),
            audit: AuditService::new(),
            harness_eval: HarnessEvalService::with_gateway_tasks(runtime.gateway_tasks()),
            evolution: EvolutionService::new(),
            provider: ProviderService::new(),
            reality: RealityService::new(),
            growth,
            workspace: WorkspaceService::new(),
            skill: SkillService::new(),
            agent: AgentService::new(),
            matrix,
            mission,
            capacity,
            owner: "0.9.581 GatewayServices selected composition",
            boundary_status: "0620_final_boundary",
        }
    }

    #[cfg(test)]
    pub(crate) fn baseline() -> Self {
        Self::baseline_with_config_home(::runtime::cowd_dirs::config_home_dir())
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        reason = "the in-memory cross-plane baseline is a deterministic local dependency for static command projections"
    )]
    pub(crate) fn baseline_with_config_home(config_home: impl AsRef<std::path::Path>) -> Self {
        let config_home = config_home.as_ref();
        let baseline_runtime =
            runtime::RuntimeServices::in_memory().expect("baseline runtime event projection");
        let capacity = crate::gateway_capacity::GatewayCapacityController::defaults(Arc::clone(
            baseline_runtime.resource_manager(),
        ));
        let task = TaskService::new();
        let sessions = Arc::new(crate::active_session::ActiveSessionDirectory::default());
        let session_repository = Arc::new(SessionRepository::new(
            Arc::clone(&sessions),
            None,
            crate::event_bus::SessionProjectionHub::new(),
        ));
        let presence_ledger =
            Arc::new(crate::services::session_service::presence::SessionPresenceLedger::new());
        Self {
            selected_storage: None,
            app_platform: None,
            core_platform_bindings: Default::default(),
            runtime: None,
            runtime_events: RuntimeEventService::from_runtime_services(baseline_runtime.as_ref()),
            surface: SurfaceService::new(),
            slash: SlashController::new(None, task.clone()),
            session: Arc::new(SessionService::for_tests(
                session_repository,
                presence_ledger,
            )),
            task,
            approval: ApprovalService::new(),
            memory: MemoryService::new(),
            context: ContextService::new()
                .with_artifact_store(Arc::clone(baseline_runtime.artifact_store())),
            connector: ConnectorService::new(),
            cross_plane: CrossPlaneService::new(baseline_runtime),
            tool: ToolService::new(),
            system: SystemService::new(),
            audit: AuditService::new(),
            harness_eval: HarnessEvalService::new(),
            evolution: EvolutionService::new(),
            provider: ProviderService::new(),
            reality: RealityService::new(),
            growth: GrowthService::new_for_config_home(config_home),
            workspace: WorkspaceService::new(),
            skill: SkillService::new(),
            agent: AgentService::new(),
            matrix: MatrixService::new(),
            mission: MissionService::new(),
            capacity,
            owner: "0.9.380 GatewayServices",
            boundary_status: "0620_final_boundary",
        }
    }

    #[must_use]
    pub(crate) fn with_app_platform(
        mut self,
        app_platform: Arc<crate::app_platform::GatewayAppPlatform>,
    ) -> Self {
        self.app_platform = Some(app_platform);
        self
    }

    /// Gateway only projects the Runtime-owned capability snapshot. Baseline
    /// services used by static command/tests intentionally expose core-only
    /// capabilities and never perform environment discovery.
    pub(crate) fn resource_capability_index(&self) -> runtime::ResourceCapabilityIndex {
        self.runtime
            .as_ref()
            .map_or_else(runtime::ResourceCapabilityIndex::default, |runtime| {
                runtime.resource_capability_index()
            })
    }

    pub(crate) fn refresh_resource_capabilities(&self) -> runtime::ResourceCapabilitySnapshot {
        self.runtime
            .as_ref()
            .map_or_else(runtime::ResourceCapabilitySnapshot::default, |runtime| {
                runtime.refresh_resource_capabilities()
            })
    }

    #[cfg(test)]
    pub(crate) fn with_approval_for_tests(runtime_services: Arc<runtime::RuntimeServices>) -> Self {
        Self {
            approval: ApprovalService::new().with_runtime_services(runtime_services),
            ..Self::baseline()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_memory_for_tests(memory_manager: Arc<GatewayMemoryManager>) -> Self {
        Self {
            memory: MemoryService::with_manager(Some(memory_manager)),
            ..Self::baseline()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_session_repository_for_tests(
        session_repository: Arc<SessionRepository>,
    ) -> Self {
        let presence_ledger =
            Arc::new(crate::services::session_service::presence::SessionPresenceLedger::new());
        Self {
            session: Arc::new(SessionService::for_tests(
                session_repository,
                presence_ledger,
            )),
            ..Self::baseline()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_for_tests(
        session_repository: Arc<SessionRepository>,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> Self {
        let presence_ledger =
            Arc::new(crate::services::session_service::presence::SessionPresenceLedger::new());
        Self {
            session: Arc::new(SessionService::for_tests(
                session_repository,
                presence_ledger,
            )),
            task: TaskService::with_runtime(runtime_services),
            ..Self::baseline()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_task_runtime_for_tests(
        mut self,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> Self {
        self.task = TaskService::with_runtime(runtime_services);
        self
    }

    pub(crate) fn service_labels(&self) -> Vec<&'static str> {
        vec![
            "runtime",
            self.surface.label(),
            self.slash.label(),
            self.session.label,
            self.task.label,
            self.approval.label,
            self.memory.label,
            self.context.label,
            self.connector.label,
            self.cross_plane.label,
            self.tool.label,
            self.system.label,
            self.audit.label,
            self.harness_eval.label,
            self.evolution.label,
            self.provider.label,
            self.reality.label,
            self.growth.label,
            self.workspace.label,
            self.skill.label,
            self.agent.label,
            self.matrix.label,
            self.mission.label,
        ]
    }

    pub(crate) fn service_contracts(&self) -> Vec<ServiceEnvelope> {
        let mut contracts = Vec::new();
        contracts.extend(self.slash.contracts());
        contracts.extend(self.session.contracts());
        contracts.extend(self.task.contracts());
        contracts.extend(self.approval.contracts());
        contracts.extend(self.memory.contracts());
        contracts.extend(self.context.contracts());
        contracts.extend(self.connector.contracts());
        contracts.extend(self.cross_plane.contracts());
        contracts.extend(self.tool.contracts());
        contracts.extend(self.system.contracts());
        contracts.extend(self.audit.contracts());
        contracts.extend(self.harness_eval.contracts());
        contracts.extend(self.evolution.contracts());
        contracts.extend(self.provider.contracts());
        contracts.extend(self.reality.contracts());
        contracts.extend(self.growth.contracts());
        contracts.extend(self.workspace.contracts());
        contracts.extend(self.skill.contracts());
        contracts.extend(self.agent.contracts());
        contracts.extend(self.matrix.contracts());
        contracts.extend(self.mission.contracts());
        contracts
    }

    pub(crate) fn has_minimum_service_contract(&self) -> bool {
        let contracts = self.service_contracts();
        let has = |service: &str, operation: &str| {
            contracts
                .iter()
                .any(|item| item.service == service && item.operation == operation)
        };

        [
            ("session", "chat"),
            ("slash", "catalog"),
            ("slash", "projection"),
            ("slash", "resolve"),
            ("slash", "dispatch"),
            ("session", "create"),
            ("session", "list"),
            ("session", "replay"),
            ("task", "list"),
            ("task", "start"),
            ("task", "cancel"),
            ("task", "complete"),
            ("approval", "pending"),
            ("approval", "respond"),
            ("memory", "status"),
            ("memory", "list"),
            ("memory", "query"),
            ("context", "snapshot"),
            ("context", "status"),
            ("connector", "resource_list"),
            ("connector", "resource_revalidate"),
            ("connector", "resource_promote_memory"),
            ("cross_plane", "summary"),
            ("cross_plane", "grants"),
            ("cross_plane", "identities"),
            ("cross_plane", "audit"),
            ("cross_plane", "execute"),
            ("tool", "approve"),
            ("tool", "deny"),
            ("system", "health"),
            ("system", "config_summary"),
            ("system", "storage_summary"),
            ("system", "runtime_summary"),
            ("audit", "approval_projection"),
            ("audit", "audit_projection"),
            ("audit", "risk_gate_projection"),
            ("harness_eval", "reports"),
            ("harness_eval", "latest_report"),
            ("harness_eval", "runs"),
            ("harness_eval", "run_start"),
            ("evolution", "signals"),
            ("evolution", "proposals"),
            ("evolution", "signal_create"),
            ("evolution", "diagnoses"),
            ("evolution", "diagnosis_create"),
            ("evolution", "missions_summary"),
            ("evolution", "mission_detail"),
            ("evolution", "proposal_create"),
            ("evolution", "proposal_detail"),
            ("evolution", "chain"),
            ("evolution", "proposal_decision"),
            ("evolution", "skill_draft"),
            ("provider", "config_projection"),
            ("provider", "model_routing"),
            ("reality", "status"),
            ("reality", "static"),
            ("reality", "flow"),
            ("reality", "promotions"),
            ("reality", "boundaries"),
            ("growth", "risk_gate_event"),
            ("growth", "memory_candidates"),
            ("growth", "matrix_signals"),
            ("growth", "event_log"),
            ("workspace", "overview"),
            ("skill", "catalog"),
            ("skill", "projection"),
            ("agent", "list"),
            ("agent", "task_projection"),
            ("matrix", "health"),
            ("mission", "projection"),
            ("mission", "session_control"),
            ("mission", "approval_projection"),
            ("mission", "relation_projection"),
        ]
        .into_iter()
        .all(|(service, operation)| has(service, operation))
    }
}
