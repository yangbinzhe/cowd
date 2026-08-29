//! Runtime service composition and builder assembly.

use super::*;

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
    pub fn collaboration_capacity(
        mut self,
        policy: crate::CollaborationCapacityPolicy,
        max_parallel_agents: usize,
    ) -> Self {
        self.collaboration_capacity = policy;
        self.collaboration_max_parallel_agents = max_parallel_agents;
        self
    }

    #[must_use]
    pub fn provider_resource_config(mut self, config: crate::ProviderResourceConfig) -> Self {
        self.provider_resource_config = config;
        self
    }

    #[must_use]
    pub fn provider_registry(mut self, registry: Arc<crate::ProviderRegistry>) -> Self {
        self.provider_registry = registry;
        self
    }

    #[must_use]
    pub fn provider_transport_pool(mut self, pool: Arc<crate::ProviderTransportPool>) -> Self {
        self.provider_transport_pool = pool;
        self
    }

    #[must_use]
    pub fn provider_template_cache(
        mut self,
        cache: Arc<crate::ProviderClientTemplateCache>,
    ) -> Self {
        self.provider_template_cache = cache;
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

    /// Install the complete Session integration boundary as one atomic builder
    /// operation. Keeping the four capabilities together prevents a launcher
    /// from compiling with a partially wired Session control plane.
    #[must_use]
    pub fn session_ports(
        mut self,
        query: Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress: Arc<dyn crate::SessionRuntimeIngressPort>,
        journal: Arc<dyn crate::SessionRuntimeJournalPort>,
        application: Arc<dyn crate::SessionRuntimeApplicationPort>,
    ) -> Self {
        self.session_query_port = Some(query);
        self.session_ingress_port = Some(ingress);
        self.session_journal_port = Some(journal);
        self.session_application_port = Some(application);
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

    /// Share the approved Skill pointer cache with the package page-in
    /// adapter. This keeps durable pointer reads off the normal turn path.
    #[must_use]
    pub fn skill_revision_pointer_cache(
        mut self,
        cache: Arc<crate::SkillRevisionPointerCache>,
    ) -> Self {
        self.skill_revision_pointer_cache = Some(cache);
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

    #[must_use]
    pub fn hot_state_config(
        mut self,
        config: crate::execution_core::hot_state::HotStateConfig,
    ) -> Self {
        self.hot_state_config = config;
        self
    }

    #[must_use]
    pub fn approval_config(mut self, config: ApprovalConfig) -> Self {
        self.approval_config = config;
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

    /// Bind every durable root/Agent/Team Outcome to the exact executable
    /// selected by the process composition root.
    #[must_use]
    pub fn runtime_build_identity(
        mut self,
        identity: harness_contract::outcome::RuntimeBuildIdentity,
    ) -> Self {
        self.runtime_build_identity = identity;
        self
    }

    /// Register a sealed Runtime projection lane before the service graph is
    /// built. App-owned projections use this composition boundary instead of
    /// spawning detached workers after startup.
    #[must_use]
    pub fn projection_lane(mut self, lane: crate::RuntimeProjectionLane) -> Self {
        self.projection_lanes.push(lane);
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
        self.runtime_build_identity
            .validate_for_recording()
            .map_err(RuntimeServicesError::Invariant)?;
        let session_ports = match (
            self.session_query_port,
            self.session_ingress_port,
            self.session_journal_port,
            self.session_application_port,
        ) {
            (Some(query), Some(ingress), Some(journal), Some(application)) => {
                Some((query, ingress, journal, application))
            }
            (None, None, None, None) => None,
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
        let assemble_started_at = Instant::now();
        let services = Arc::new(RuntimeServices::assemble(
            self.cowd_home.clone(),
            workspace_root,
            workspace_key,
            event_store,
            self.runtime_build_identity,
            worktree_leases,
            scope_locks,
            self.resource_quotas,
            self.provider_resource_config,
            self.provider_registry,
            self.provider_fallbacks,
            self.provider_transport_pool,
            self.provider_template_cache,
            self.tool_execution_host,
            artifact_store,
            self.memory_manager,
            self.reality_recall_port,
            self.knowledge_activation,
            self.evolution_eval_runner,
            self.skill_catalog,
            self.skill_revision_pointer_cache,
            self.mission_schedule_policy,
            self.hot_state_config,
            self.approval_config,
            self.collaboration_capacity,
            self.collaboration_max_parallel_agents,
            definition_registry,
            task_aggregate_service,
            self.projection_lanes,
            None,
        )?);
        tracing::info!(
            elapsed_ms = assemble_started_at.elapsed().as_millis() as u64,
            "Runtime service graph assembly completed"
        );
        let task_recovery_started_at = Instant::now();
        services
            .task_runtime_port()
            .recover()
            .map_err(RuntimeServicesError::Task)?;
        tracing::info!(
            elapsed_ms = task_recovery_started_at.elapsed().as_millis() as u64,
            "Runtime task recovery completed"
        );
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
        let agent_recovery_started_at = Instant::now();
        services
            .agent_runtime
            .block_unrecoverable_replayed_runs()
            .map_err(RuntimeServicesError::AgentRuntime)?;
        tracing::info!(
            elapsed_ms = agent_recovery_started_at.elapsed().as_millis() as u64,
            "Runtime Agent recovery completed"
        );
        let evolution_projection_started_at = Instant::now();
        services.materialize_evolution_release_assignments()?;
        tracing::info!(
            elapsed_ms = evolution_projection_started_at.elapsed().as_millis() as u64,
            "Runtime evolution release projection completed"
        );
        services
            .event_reactor
            .start()
            .map_err(RuntimeServicesError::Invariant)?;
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
        if let Some((query, ingress, journal, application)) = session_ports {
            services.install_session_ports(query, ingress, journal, application)?;
        }
        Ok(services)
    }
}

pub(super) fn normalize_provider_fallbacks(
    fallbacks: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut normalized = Vec::new();
    for fallback in fallbacks {
        let fallback = fallback.trim().to_string();
        if !fallback.is_empty() && !normalized.contains(&fallback) {
            normalized.push(fallback);
        }
    }
    normalized
}
