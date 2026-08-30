//! Conversation turn construction, policy, session, and compaction control.

use super::*;

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    #[must_use]
    pub fn new(
        session: Session,
        api_client: C,
        tool_executor: T,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
    ) -> Self {
        Self::new_with_features(
            session,
            api_client,
            Arc::new(tool_executor),
            permission_policy,
            system_prompt,
            &RuntimeFeatureConfig::default(),
        )
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_with_features(
        session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
    ) -> Self {
        Self::new_with_features_and_memory_composition(
            session,
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            feature_config,
            MemoryManagerComposition::Automatic,
        )
    }

    /// Construct a conversation from the Memory owner already selected by the
    /// embedding host. `None` is an explicit unavailable selection and never
    /// falls back to the standalone SQLite constructor.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn new_with_features_and_selected_memory(
        session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
        memory_manager: Option<Arc<CognitiveContextManager>>,
    ) -> Self {
        Self::new_with_features_and_memory_composition(
            session,
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            feature_config,
            MemoryManagerComposition::HostSelected(memory_manager),
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn new_with_features_and_memory_composition(
        mut session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
        memory_composition: MemoryManagerComposition,
    ) -> Self {
        session.configure_history(feature_config.session_history());
        let usage_tracker = UsageTracker::from_session(&session);
        let permission_fingerprint = model_protocol::fingerprint::stable_hash_bytes(
            format!("{permission_policy:?}").as_bytes(),
        );
        let subsystem_budget_ratio_bp = feature_config.context_budget().subsystem_budget_ratio_bp;
        let initial_model = feature_config.resolved_model();
        let initial_window_resolution = initial_model.as_deref().map_or(
            provider::ModelContextWindowResolution {
                tokens: 128_000,
                source: provider::ModelContextWindowSource::Assumed,
            },
            |model| {
                provider::model_context_window_resolution(
                    model,
                    Some(feature_config.model_context_windows()),
                )
            },
        );
        let initial_model_context_window = initial_window_resolution.tokens;
        let initial_model_max_output = initial_model.as_deref().map_or(0, |model| {
            provider_output_budget_hint(
                model,
                initial_model_context_window,
                feature_config
                    .provider_resources()
                    .max_output_tokens_override(),
            )
        });
        let initial_budget_plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: initial_model_context_window,
            model_max_output_tokens: initial_model_max_output,
            subsystem_budget_ratio_bp,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
            expected_parallel_branches: 1,
            expected_verification_passes: 0,
        });
        let (memory_manager, memory_status) = match memory_composition {
            MemoryManagerComposition::HostSelected(manager) => {
                let status = (feature_config.memory().enabled && manager.is_none()).then(|| {
                    "Memory system unavailable: the composition root selected no Memory owner. \
                     Runtime will not infer or open a fallback backend."
                        .to_string()
                });
                (manager, status)
            }
            MemoryManagerComposition::Automatic if feature_config.memory().enabled => {
                initialize_automatic_memory_manager(feature_config, &initial_budget_plan)
            }
            MemoryManagerComposition::Automatic => (None, None),
        };
        let session_id = session.session_id.clone();
        let session = Arc::new(RwLock::new(session));
        let mut runtime_control_policy = feature_config.runtime_control().policy.clone();
        apply_runtime_budget_to_control_policy(&mut runtime_control_policy, &initial_budget_plan);
        Self {
            session_id: session_id.clone(),
            session,
            session_input_stream: crate::session_input::SessionInputStream::new(session_id),
            consumed_session_inputs: std::sync::Mutex::new(Vec::new()),
            api_client,
            tool_executor,
            permission_policy,
            permission_fingerprint,
            system_prompt,
            usage_tracker,
            hook_runner: HookRunner::from_feature_config(feature_config),
            cowd_bus: None,
            turn_callback: None,
            profiler: crate::context_profiler::ContextProfiler::new(),
            subsystem_budget_ratio_bp,
            session_compaction_config: feature_config.compression().session.clone(),
            semantic_checkpoint_enabled: feature_config
                .memory()
                .runtime
                .semantic_checkpoint_enabled,
            model_context_window: initial_model_context_window,
            model_context_window_source: initial_window_resolution.source,
            model_context_windows: feature_config.model_context_windows().clone(),
            provider_max_output_override: feature_config
                .provider_resources()
                .max_output_tokens_override(),
            evaluation_provider_token_lease: None,
            delegated_provider_budget: None,
            calibrated_model_context_windows: std::sync::Mutex::new(BTreeMap::new()),
            hook_abort_signal: HookAbortSignal::default(),
            hook_progress_reporter: Arc::new(std::sync::Mutex::new(None)),
            session_tracer: None,
            memory_manager,
            checkpoint_workspace_id: "runtime-workspace".to_string(),
            execution_identity: None,
            maintenance_supervisor: None,
            memory_status,
            reality_recall: None,
            knowledge_activation: None,
            last_reality_recall_report: std::sync::Mutex::new(None),
            tool_callback: None,
            session_journal_port: None,
            session_history_reader: None,
            hot_state: None,
            session_context_projection_cache: std::sync::Mutex::new(None),
            session_memory_projection: tokio::sync::Mutex::new(SessionMemoryProjection::default()),
            memory_context_revision: AtomicU64::new(0),
            current_context_cache_hit: AtomicBool::new(false),
            current_context_source_latency_ms: std::sync::Mutex::new(BTreeMap::new()),
            artifact_store: None,
            runtime_event_store: None,
            outcome_service: None,
            outcome_projector: None,
            routing_mode: feature_config.routing_mode(),
            runtime_config_revision: format!(
                "{:016x}",
                model_protocol::fingerprint::stable_hash_bytes(
                    format!("{feature_config:?}").as_bytes()
                )
            ),
            active_provider_identity: std::sync::Mutex::new(None),
            provider_selection_receipt: std::sync::Mutex::new(None),
            event_log: None,
            tool_output_sandbox: memory::ToolOutputSandbox::new()
                .map(|sandbox| Arc::new(std::sync::Mutex::new(sandbox)))
                .map_err(|error| {
                    tracing::warn!(%error, "tool output sandbox unavailable");
                    error
                })
                .ok(),
            sse_callback: None,
            memory_callback: None,
            approval_coordinator: None,
            skill_profiles: Vec::new(),
            agent_skill_profile: AgentSkillProfile::default(),
            skill_prompt_assets: Vec::new(),
            skill_instruction_source: None,
            memory_agent_id: "primary".to_string(),
            memory_definition_lineage_id: None,
            memory_team_id: None,
            memory_read_scopes: vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Team,
                harness_contract::agent::CognitiveReadScope::WorkspaceKnowledge,
                harness_contract::agent::CognitiveReadScope::Project,
                harness_contract::agent::CognitiveReadScope::DefinitionLineage,
            ],
            project_phase: "Discovery".to_string(),
            model: initial_model,
            fallbacks: Arc::new(std::sync::RwLock::new(feature_config.fallbacks().to_vec())),
            cancellation_token: CancellationToken::new(),
            last_context_envelope: std::sync::Mutex::new(None),
            context_profile: std::sync::Mutex::new(ContextProfile::MainTurn),
            runtime_control_policy,
            external_context_items: std::sync::Mutex::new(Vec::new()),
            next_model_context_items: std::sync::Mutex::new(Vec::new()),
            next_model_text_only: AtomicBool::new(false),
            next_model_tool_allowlist: std::sync::Mutex::new(None),
            next_model_tool_required: AtomicBool::new(false),
            next_model_required_tool_name: std::sync::Mutex::new(None),
            next_model_tool_activation_notice: std::sync::Mutex::new(None),
            next_model_reasoning_effort: std::sync::Mutex::new(None),
            tool_trace_context_items: std::sync::Mutex::new(Vec::new()),
            turn_tool_observations: std::sync::Mutex::new(Vec::new()),
            turn_governed_tool_plans: std::sync::Mutex::new(Vec::new()),
            active_turn_strategy: std::sync::Mutex::new(None),
            tool_exposure_state: std::sync::Mutex::new(None),
            turn_tool_exposure_metrics: std::sync::Mutex::new(TurnProviderState::default()),
            active_skill_tool_refs: std::sync::Mutex::new(BTreeSet::new()),
            tool_exposure_revision: AtomicU64::new(0),
            request_compiler: crate::PreparedRequestCompiler::new(
                feature_config.session_history().request_cache_entries,
            ),
            turn_stable_prefix_metrics: std::sync::Mutex::new(TurnStablePrefixMetrics::default()),
            turn_evidence_audits: std::sync::Mutex::new(Vec::new()),
            turn_generated_model_receipts: std::sync::Mutex::new(Vec::new()),
            turn_model_observations: std::sync::Mutex::new(Vec::new()),
            turn_context_ledger: std::sync::Mutex::new(crate::context_ledger::ContextLedger::new(
                initial_budget_plan.subsystem_budget_tokens,
                initial_budget_plan.tool_result_budget.max_total_tokens as u64,
            )),
            last_context_turn_report: std::sync::Mutex::new(None),
            turn_preflight_compaction: std::sync::Mutex::new(None),
            turn_knowledge_report: std::sync::Mutex::new(None),
            tool_execution_plane: Arc::new(crate::ToolExecutionPlane::new(
                Arc::new(crate::execution_core::graph::ExecutionResourceManager::new(
                    [
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Tool,
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 4,
                                target: 64,
                                maximum: 256,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.process".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 2,
                                target: 16,
                                maximum: 64,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.network".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 2,
                                target: 32,
                                maximum:
                                    crate::governed_tool_plan::default_parallel_tool_concurrency(),
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.cpu".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 2,
                                target: 64,
                                maximum: 256,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.memory_mib".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 64,
                                target: 2_048,
                                maximum: 16_384,
                            },
                        ),
                    ],
                )),
                Arc::new(crate::execution_core::graph::ScopeLockManager::new()),
            )),
            authorization_negotiator: crate::AuthorizationNegotiator::new(),
            provider_admission: None,
            provider_resource_config: Arc::new(std::sync::RwLock::new(
                crate::ProviderResourceConfig::default(),
            )),
            execution_service_class:
                crate::execution_core::graph::ExecutionServiceClass::Interactive,
            tool_timeout: Some(Duration::from_secs(120)),
            explicit_team_escalation: true,
            model_step_limit_override: AtomicUsize::new(0),
            delegated_focus_novelty_target_bp: AtomicU64::new(0),
            delegated_focus_acceptance_scopes: std::sync::Mutex::new(Vec::new()),
            delegated_focus_required_output_fields: std::sync::Mutex::new(Vec::new()),
            session_execution_fence: None,
        }
    }

    #[must_use]
    pub fn with_session_execution_fence(mut self, fence: crate::SessionExecutionFence) -> Self {
        self.session_execution_fence = Some(fence);
        self
    }

    pub(crate) async fn verify_session_execution_fence(
        &self,
        phase: crate::SessionExecutionFencePhase,
    ) -> Result<(), RuntimeError> {
        match self.session_execution_fence.as_ref() {
            Some(fence) => fence
                .verify(phase)
                .await
                .map(|_| ())
                .map_err(RuntimeError::new),
            None => Ok(()),
        }
    }

    pub(crate) async fn capture_session_execution_fence(
        &self,
        phase: crate::SessionExecutionFencePhase,
    ) -> Result<Option<crate::SessionExecutionFenceSnapshot>, RuntimeError> {
        match self.session_execution_fence.as_ref() {
            Some(fence) => fence
                .verify(phase)
                .await
                .map(Some)
                .map_err(RuntimeError::new),
            None => Ok(None),
        }
    }

    #[must_use]
    pub fn with_tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn with_provider_admission(
        mut self,
        manager: Arc<crate::execution_core::graph::ExecutionResourceManager>,
    ) -> Self {
        self.provider_admission = Some(manager);
        self
    }

    #[must_use]
    pub fn with_provider_resource_config(
        mut self,
        config: Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
    ) -> Self {
        self.provider_resource_config = config;
        self
    }

    #[must_use]
    pub(crate) fn with_evaluation_provider_token_lease(
        mut self,
        lease: Arc<EvaluationProviderTokenLease>,
    ) -> Self {
        self.evaluation_provider_token_lease = Some(lease);
        self
    }

    #[cfg(test)]
    pub(crate) fn uses_evaluation_provider_token_lease(
        &self,
        lease: &Arc<EvaluationProviderTokenLease>,
    ) -> bool {
        self.evaluation_provider_token_lease
            .as_ref()
            .is_some_and(|bound| Arc::ptr_eq(bound, lease))
    }

    /// Install the immutable child share of a durable parent execution
    /// budget. Missing Runtime persistence fails closed rather than silently
    /// falling back to a process-local counter.
    pub(crate) fn set_delegated_provider_budget(
        &mut self,
        reservation: harness_contract::context::ChildExecutionBudgetReservation,
    ) -> Result<(), RuntimeError> {
        reservation.validate().map_err(RuntimeError::new)?;
        let store = self.runtime_event_store.clone().ok_or_else(|| {
            RuntimeError::new("delegated provider budget requires Runtime event persistence")
        })?;
        let ledger = crate::execution_core::budget::ParentExecutionBudgetLedger::new(
            store,
            reservation.parent_budget.clone(),
        )
        .map_err(RuntimeError::new)?;
        self.delegated_provider_budget = Some((ledger, reservation));
        Ok(())
    }

    #[must_use]
    pub(crate) fn with_provider_fallback_policy(
        mut self,
        policy: Arc<std::sync::RwLock<Vec<String>>>,
    ) -> Self {
        self.fallbacks = policy;
        self
    }

    #[must_use]
    pub fn with_execution_service_class(
        mut self,
        service_class: crate::execution_core::graph::ExecutionServiceClass,
    ) -> Self {
        self.execution_service_class = service_class;
        self
    }

    pub fn set_execution_service_class(
        &mut self,
        service_class: crate::execution_core::graph::ExecutionServiceClass,
    ) {
        self.execution_service_class = service_class;
    }

    #[must_use]
    pub fn with_tool_execution_plane(mut self, plane: Arc<crate::ToolExecutionPlane>) -> Self {
        self.tool_execution_plane = plane;
        self
    }

    #[cfg(test)]
    pub(crate) fn uses_tool_execution_plane(&self, plane: &Arc<crate::ToolExecutionPlane>) -> bool {
        Arc::ptr_eq(&self.tool_execution_plane, plane)
    }

    #[cfg(test)]
    pub(crate) fn uses_artifact_store(&self, store: &Arc<crate::ArtifactStore>) -> bool {
        self.artifact_store
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, store))
    }

    #[must_use]
    pub fn with_explicit_team_escalation(mut self, enabled: bool) -> Self {
        self.explicit_team_escalation = enabled;
        self
    }

    /// Provider context capacity bound to this runtime instance. Execution
    /// safety derives its lease from this value rather than Gateway prompt
    /// classes or a fixed whole-turn iteration limit.
    #[must_use]
    pub const fn model_context_window(&self) -> u32 {
        self.model_context_window
    }

    #[must_use]
    pub(crate) fn current_model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn with_model_context_window(mut self, ctx_window: u32) -> Self {
        if ctx_window >= 1_024 {
            self.model_context_window = ctx_window;
            // Hosts often pass the same registry/config resolution explicitly
            // for workspace sizing. Preserve its real provenance instead of
            // falsely reporting every host value as a user override.
            self.model_context_window_source = self
                .model
                .as_deref()
                .map(|model| {
                    provider::model_context_window_resolution(
                        model,
                        Some(&self.model_context_windows),
                    )
                })
                .filter(|resolution| resolution.tokens == ctx_window)
                .map_or(
                    provider::ModelContextWindowSource::Configured,
                    |resolution| resolution.source,
                );
        }
        let plan = self.runtime_budget_plan();
        apply_runtime_budget_to_control_policy(&mut self.runtime_control_policy, &plan);
        self
    }

    pub fn set_active_model(&mut self, model: impl Into<String>) {
        let model = model.into();
        if !model.trim().is_empty() {
            // A session model switch must not inherit the previous model's
            // window. Resolve this model independently so explicit per-model
            // configuration remains authoritative across a live session.
            if self.model.as_deref() != Some(model.as_str()) {
                let resolution = provider::model_context_window_resolution(
                    &model,
                    Some(&self.model_context_windows),
                );
                self.model_context_window = resolution.tokens;
                self.model_context_window_source = resolution.source;
            }
            self.model = Some(model);
        }
    }

    #[must_use]
    pub(crate) fn active_model_lease(&self) -> String {
        self.model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
            .unwrap_or("default")
            .to_string()
    }

    /// Set a tool callback for real-time execution visualization (P0-2).
    ///
    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_tool_callback(mut self, callback: Arc<dyn ToolCallback>) -> Self {
        self.tool_callback = Some(callback);
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_sse_callback(mut self, callback: Arc<dyn Fn(String) + Send + Sync>) -> Self {
        self.sse_callback = Some(callback);
        self
    }

    /// Set the SSE callback on an already-constructed runtime instance.
    pub fn set_sse_callback(&mut self, callback: Arc<dyn Fn(String) + Send + Sync>) {
        self.sse_callback = Some(callback);
    }

    /// Clear the SSE callback from this runtime instance.
    pub fn clear_sse_callback(&mut self) {
        self.sse_callback = None;
    }

    #[must_use]
    pub fn with_session_journal_port(
        mut self,
        port: Arc<dyn crate::SessionRuntimeJournalPort>,
    ) -> Self {
        self.session_journal_port = Some(port);
        self.refresh_provider_wire_evidence_writer();
        self
    }

    #[must_use]
    pub fn with_session_history_reader(
        mut self,
        reader: Arc<session::SessionHistoryReader>,
    ) -> Self {
        self.session_history_reader = Some(reader);
        self
    }

    #[must_use]
    pub fn with_hot_state(
        mut self,
        hot_state: Arc<crate::execution_core::hot_state::RuntimeHotStatePlane>,
    ) -> Self {
        self.hot_state = Some(hot_state);
        self
    }

    #[must_use]
    pub fn with_artifact_store(mut self, store: Arc<crate::ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self.refresh_provider_wire_evidence_writer();
        self
    }

    pub(super) fn refresh_provider_wire_evidence_writer(&mut self) {
        let writer = self
            .artifact_store
            .as_ref()
            .zip(self.session_journal_port.as_ref())
            .map(|(artifacts, session_port)| {
                Arc::new(SessionProviderWireEvidenceWriter {
                    artifacts: Arc::clone(artifacts),
                    session_port: Arc::clone(session_port),
                }) as Arc<dyn crate::ProviderWireEvidenceWriter>
            });
        self.api_client.configure_provider_wire_evidence(writer);
    }

    /// Attach the durable store that owns tool, graph, agent, and task execution state.
    #[must_use]
    pub(crate) fn with_runtime_event_store(mut self, store: Arc<RuntimeEventStore>) -> Self {
        self.outcome_service = Some(Arc::new(crate::execution_core::OutcomeService::new(
            Arc::clone(&store),
        )));
        self.outcome_projector = Some(Arc::new(crate::OutcomeProjector::new(Arc::clone(&store))));
        self.runtime_event_store = Some(store);
        self
    }

    #[must_use]
    pub(crate) fn with_outcome_runtime(
        mut self,
        service: Arc<crate::execution_core::OutcomeService>,
        projector: Arc<crate::OutcomeProjector>,
    ) -> Self {
        self.outcome_service = Some(service);
        self.outcome_projector = Some(projector);
        self
    }

    /// Attach a [`SessionEventLog`] for time-travel debugging and session rebuild.
    #[must_use]
    pub fn with_event_log(mut self, log: SessionEventLog) -> Self {
        self.event_log = Some(std::sync::Mutex::new(log));
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_memory_callback(mut self, callback: Arc<dyn MemoryCallback>) -> Self {
        self.memory_callback = Some(callback);
        self
    }

    pub fn set_memory_callback(&mut self, callback: Arc<dyn MemoryCallback>) {
        self.memory_callback = Some(callback);
    }

    /// Install the Runtime-owned approval coordinator.
    #[must_use]
    pub fn with_approval_coordinator(
        mut self,
        coordinator: Arc<crate::ApprovalCoordinator>,
    ) -> Self {
        self.approval_coordinator = Some(coordinator);
        self
    }

    /// Provide Skill capability profiles already inspected by the Skill asset
    /// layer. Runtime consumes these profiles during activation, but does not
    /// inspect packages or own the registry.
    #[must_use]
    pub fn with_skill_profiles(mut self, profiles: Vec<SkillCapabilityProfile>) -> Self {
        self.skill_profiles = profiles;
        self
    }

    /// Configure the agent-scoped Skill visibility and adapter ceiling used by
    /// runtime activation.
    #[must_use]
    pub fn with_agent_skill_profile(mut self, profile: AgentSkillProfile) -> Self {
        self.agent_skill_profile = profile;
        self
    }

    /// Provide prompt assets already inspected by the Skill layer. Only an
    /// asset selected by Runtime is injected for a single model request.
    #[must_use]
    pub fn with_skill_prompt_assets(mut self, assets: Vec<RuntimeSkillPromptAsset>) -> Self {
        self.skill_prompt_assets = assets;
        self
    }

    /// Attach the Gateway-owned lazy instruction source pinned to this
    /// Runtime catalog generation.
    #[must_use]
    pub fn with_skill_instruction_source(
        mut self,
        source: Option<Arc<dyn crate::RuntimeSkillInstructionSource>>,
    ) -> Self {
        self.skill_instruction_source = source;
        self
    }

    /// Bind the Runtime's immutable Agent instance identity to memory
    /// operations for this conversation. This is set only by Runtime-owned
    /// child execution, never by a Surface request field.
    #[must_use]
    pub fn with_memory_identity(
        mut self,
        agent_id: impl Into<String>,
        definition_lineage_id: Option<String>,
        team_id: Option<String>,
        read_scopes: Vec<harness_contract::agent::CognitiveReadScope>,
    ) -> Self {
        self.memory_agent_id = agent_id.into();
        self.memory_definition_lineage_id = definition_lineage_id;
        self.memory_team_id = team_id;
        self.memory_read_scopes = read_scopes;
        self
    }

    /// Bind semantic checkpoints to the same canonical execution identity as
    /// the active Agent node. Root surface turns provide only the workspace
    /// basis and receive a session-turn identity when compaction is planned.
    #[must_use]
    pub fn with_checkpoint_identity(
        mut self,
        workspace_id: impl Into<String>,
        execution_identity: Option<harness_contract::execution::ExecutionIdentity>,
    ) -> Self {
        self.checkpoint_workspace_id = workspace_id.into();
        self.execution_identity = execution_identity;
        self
    }

    #[must_use]
    pub fn with_runtime_control_policy(mut self, policy: RuntimeControlPolicy) -> Self {
        self.runtime_control_policy = policy;
        self
    }

    /// T35: Set a cancellation token for graceful shutdown.
    #[must_use]
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }

    #[must_use]
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub(super) fn governed_workspace_root(&self) -> Result<PathBuf, RuntimeError> {
        if let Some(root) = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf))
        {
            return Ok(root);
        }
        #[cfg(test)]
        {
            return std::env::current_dir().map_err(|error| {
                RuntimeError::new(format!("test workspace unavailable: {error}"))
            });
        }
        #[cfg(not(test))]
        {
            Err(RuntimeError::new(
                "governed Runtime execution requires an explicit Session workspace",
            ))
        }
    }

    /// Attach a CowdEventBus for domain event emission.
    #[must_use]
    pub fn with_cowd_event_bus(mut self, bus: crate::cowd_event::CowdEventBus) -> Self {
        self.cowd_bus = Some(Arc::new(bus.clone()));
        self
    }

    /// Get a reference to the attached CowdEventBus, if any.
    pub fn cowd_bus(&self) -> Option<&crate::cowd_event::CowdEventBus> {
        self.cowd_bus.as_deref()
    }

    pub fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
        state: crate::input_classifier::RuntimeInputState,
    ) -> SessionInputReceipt {
        let mut state = state;
        if state.active_turn_id.is_none() {
            state.active_turn_id = self.session_input_stream.active_turn_id();
        }
        let receipt = self.session_input_stream.admit(envelope, state);
        self.emit_session_input_projection(Some(receipt.clone()));
        receipt
    }

    #[must_use]
    pub fn session_input_projection(&self) -> SessionInputProjection {
        self.session_input_stream.projection()
    }

    #[must_use]
    pub fn active_turn_inbox(&self, turn_id: Option<TurnId>) -> TurnInboxSnapshot {
        self.session_input_stream.inbox_snapshot(turn_id)
    }

    #[must_use]
    pub fn session_input_stream(&self) -> crate::session_input::SessionInputStream {
        self.session_input_stream.clone()
    }

    pub(super) fn emit_session_input_projection(&self, receipt: Option<SessionInputReceipt>) {
        if let Some(ref cowd) = self.cowd_bus {
            if let Some(receipt) = receipt {
                cowd.emit(crate::cowd_event::CowdEvent::SessionInputReceived { receipt });
            }
            cowd.emit(crate::cowd_event::CowdEvent::SessionInputProjection {
                projection: self.session_input_stream.projection(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::TurnInboxUpdated {
                inbox: self.session_input_stream.inbox_snapshot(None),
            });
        }
    }

    pub(super) fn consume_runtime_input_records(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let consumed = self
            .session_input_stream
            .consume_for_checkpoint(turn_id, checkpoint, 32);
        if !consumed.is_empty() {
            if let Ok(mut pending) = self.consumed_session_inputs.lock() {
                pending.extend(consumed.iter().cloned());
            }
        }
        if let Some(ref cowd) = self.cowd_bus {
            if !consumed.is_empty() {
                cowd.emit(crate::cowd_event::CowdEvent::TurnInputCheckpointConsumed {
                    checkpoint,
                    consumed: consumed
                        .iter()
                        .map(crate::session_input::SessionInputRecord::to_inbox_item)
                        .collect(),
                });
            }
            cowd.emit(crate::cowd_event::CowdEvent::SessionInputProjection {
                projection: self.session_input_stream.projection(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::TurnInboxUpdated {
                inbox: self
                    .session_input_stream
                    .inbox_snapshot(Some(turn_id.clone())),
            });
        }
        consumed
    }

    pub(super) fn consume_runtime_inputs_at_checkpoint(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
        prompt: &mut PromptAssembly,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let consumed = self.consume_runtime_input_records(turn_id, checkpoint);
        if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(checkpoint, &consumed) {
            prompt.push_trusted_system(guidance);
        }
        for item in crate::turn_inbox::checkpoint_context_items(checkpoint, &consumed) {
            prompt.push_context_item(&item);
        }
        consumed
    }

    /// Consume active-turn input after a Provider/tool boundary and place its
    /// typed context on the next Provider request.
    pub(crate) fn consume_active_runtime_inputs_for_next_step(
        &self,
        checkpoint: TurnInputCheckpoint,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let Some(turn_id) = self.session_input_stream.active_turn_id() else {
            return Vec::new();
        };
        let consumed = self.consume_runtime_input_records(&turn_id, checkpoint);
        if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(checkpoint, &consumed) {
            let mut item = ContextItem::new(
                format!("turn-input-guidance:{}", checkpoint.as_str()),
                ContextSourceKind::Task,
                ContextRole::Instruction,
                guidance,
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            self.push_next_model_context_item(item);
        }
        for item in crate::turn_inbox::checkpoint_context_items(checkpoint, &consumed) {
            self.push_next_model_context_item(item);
        }
        consumed
    }

    #[must_use]
    pub(crate) fn consumed_session_input_cursor(
        &self,
    ) -> Option<harness_contract::turn::SessionInputCursor> {
        self.session_input_stream
            .active_turn_id()
            .as_ref()
            .and_then(|turn_id| self.session_input_stream.highest_consumed_cursor(turn_id))
    }

    /// Drain compact receipts for inputs consumed during the current provider
    /// step. This does not mutate routing or create tasks; the graph host is
    /// the only caller allowed to decide whether a correction revises a Goal.
    pub fn take_consumed_session_inputs(&self) -> Vec<crate::session_input::SessionInputRecord> {
        self.consumed_session_inputs
            .lock()
            .map_or_else(|_| Vec::new(), |mut pending| std::mem::take(&mut *pending))
    }

    /// P1-05: Register a TurnCallback for generator-style injection after tool results.
    #[must_use]
    pub fn with_turn_callback(mut self, cb: TurnCallback) -> Self {
        self.turn_callback = Some(Arc::new(cb));
        self
    }

    #[must_use]
    pub fn with_hook_abort_signal(mut self, hook_abort_signal: HookAbortSignal) -> Self {
        self.hook_abort_signal = hook_abort_signal;
        self
    }

    #[must_use]
    pub fn with_hook_progress_reporter(
        self,
        hook_progress_reporter: Box<dyn HookProgressReporter + Send>,
    ) -> Self {
        *self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(hook_progress_reporter);
        self
    }

    #[must_use]
    pub fn with_session_tracer(mut self, session_tracer: SessionTracer) -> Self {
        self.session_tracer = Some(session_tracer);
        self
    }

    /// Override the memory manager with a pre-constructed instance.
    ///
    /// This is primarily useful in tests or when the caller wants full control
    /// over the [`CognitiveContextManager`] lifecycle.
    #[must_use]
    pub fn with_memory_manager(mut self, manager: Arc<CognitiveContextManager>) -> Self {
        self.memory_manager = Some(manager);
        self
    }

    #[must_use]
    pub(crate) fn with_maintenance_supervisor(
        mut self,
        supervisor: Arc<crate::execution_core::services::RuntimeMaintenanceSupervisor>,
    ) -> Self {
        self.maintenance_supervisor = Some(supervisor);
        self
    }

    /// Attach the Runtime-owned Fact/Matrix recall port to this conversation.
    /// The Binding is immutable for the host lifetime, so each turn evaluates
    /// the same data lease rather than re-resolving a surface default.
    #[must_use]
    pub fn with_reality_binding(
        mut self,
        port: crate::RealityRecallPort,
        binding: harness_contract::agent::AgentBindingSnapshot,
    ) -> Self {
        self.reality_recall = Some((port, binding));
        self
    }

    #[must_use]
    pub fn with_knowledge_activation(mut self, activation: KnowledgeActivationRuntime) -> Self {
        self.knowledge_activation = Some(activation);
        self
    }

    /// Return the source-level report for the most recently assembled model
    /// context.  The report proves a lease was applied even when it selected
    /// no Fact or Matrix evidence.
    #[must_use]
    pub fn last_reality_recall_report(&self) -> Option<crate::RealityRecallReport> {
        self.last_reality_recall_report
            .lock()
            .ok()
            .and_then(|report| report.clone())
    }

    /// Explicitly disable the memory subsystem, regardless of feature config.
    #[must_use]
    pub fn without_memory(mut self) -> Self {
        self.memory_manager = None;
        self
    }

    /// Access the cognitive memory manager, if memory is enabled.
    ///
    /// Returns `None` when memory is disabled or failed to initialise.
    #[must_use]
    pub fn memory_manager(&self) -> Option<&Arc<CognitiveContextManager>> {
        self.memory_manager.as_ref()
    }

    /// Record a compact runtime event for later context and memory governance.
    pub(super) fn record_context_event(
        &mut self,
        event_type: &str,
        category: &str,
        summary: &str,
        priority: u8,
    ) {
        let project_dir = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf))
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.profiler
            .record_dedup(crate::context_profiler::SessionEvent {
                event_type: event_type.into(),
                category: category.into(),
                data_summary: summary.into(),
                priority,
                data_hash: 0, // computed by record_dedup
                timestamp,
                project_dir,
                attribution_confidence: 0.9,
            });
    }

    pub(super) fn run_pre_tool_use_hook(&self, tool_name: &str, input: &str) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    pub(super) fn run_post_tool_use_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    pub(super) fn run_post_tool_use_failure_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    /// Compact the active transcript through the sole semantic checkpoint
    /// pipeline. Both automatic preflight compaction and operator-triggered
    /// compaction use this path so a session never receives a second,
    /// timeline-only summary representation.
    pub async fn compact_active_session(
        &mut self,
    ) -> Result<Option<AutoCompactionEvent>, RuntimeError> {
        // Operator-triggered compaction shares the configured preservation and
        // checkpoint limits with request-preflight compaction. `1` makes the
        // operation explicit without introducing a second threshold policy.
        self.compact_session_with_checkpoint(self.compaction_config_for_session(1))
            .await
    }

    #[must_use]
    pub fn estimated_tokens(&self) -> usize {
        estimate_session_tokens(&self.session.blocking_read())
    }

    pub(super) fn model_candidates_for_turn(&self, _user_input: &str) -> Vec<String> {
        let primary = self
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(ToString::to_string);
        let fallback_snapshot = self
            .fallbacks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut fallback_models: Vec<String> = fallback_snapshot
            .iter()
            .map(|model| model.trim())
            .filter(|model| !model.is_empty())
            .map(ToString::to_string)
            .collect();
        fallback_models.dedup();
        if let Some(primary) = primary.as_ref() {
            fallback_models.retain(|model| model != primary);
        }

        let mut routed = Vec::with_capacity(fallback_models.len() + usize::from(primary.is_some()));
        if let Some(primary) = primary {
            routed.push(primary);
        }
        for model in fallback_models {
            if !routed.iter().any(|known| known == &model) {
                routed.push(model);
            }
        }
        if routed.is_empty() {
            // An empty model delegates selection to the configured provider. This keeps
            // embedded runtimes valid when they intentionally rely on a provider default.
            routed.push(String::new());
        }
        let strategy_segment = self
            .active_turn_strategy()
            .map(|state| (state.policy_version.clone(), state.selected_candidate));
        let receipt = if let Some(projector) = self.outcome_projector.as_ref() {
            let (selected, receipt) = crate::select_provider_from_outcome_snapshot(
                self.routing_mode,
                &routed,
                &self.runtime_config_revision,
                strategy_segment
                    .as_ref()
                    .map(|(policy_revision, _)| policy_revision.as_str()),
                strategy_segment
                    .as_ref()
                    .map(|(_, selected_candidate)| *selected_candidate),
                &projector.snapshot(),
                now_ms(),
            );
            routed = selected;
            receipt
        } else {
            crate::ProviderSelectionReceipt {
                requested_mode: self.routing_mode,
                effective_mode: crate::RoutingMode::Pinned,
                snapshot_revision: 0,
                selected_model: routed.first().cloned().unwrap_or_default(),
                fallback_reason: (self.routing_mode == crate::RoutingMode::Auto)
                    .then(|| "outcome projection is unavailable".to_string()),
                candidates: Vec::new(),
            }
        };
        *self
            .provider_selection_receipt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(receipt);
        routed
    }

    #[must_use]
    pub fn usage(&self) -> &UsageTracker {
        &self.usage_tracker
    }

    #[must_use]
    pub fn tool_executor(&self) -> &Arc<T> {
        &self.tool_executor
    }

    #[must_use]
    pub fn permission_policy(&self) -> &PermissionPolicy {
        &self.permission_policy
    }

    #[must_use]
    pub(crate) fn authorization_negotiator(&self) -> crate::AuthorizationNegotiator {
        self.authorization_negotiator.clone()
    }

    pub(super) fn authorization_request(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        execution_policy: &harness_contract::policy::SessionExecutionPolicy,
    ) -> crate::AuthorizationRequest {
        let execution_context = self
            .cowd_bus()
            .and_then(crate::CowdEventBus::current_execution_context);
        let delegated = self.memory_agent_id != "primary";
        let recovery_scope = execution_context.as_ref().map_or_else(
            || format!("session:{}", self.session_id()),
            |context| format!("turn:{}", context.turn_id),
        );
        let safe_alternatives = match descriptor.effect_kind {
            harness_contract::tool::ToolEffectKind::Read => Vec::new(),
            harness_contract::tool::ToolEffectKind::Write => {
                vec!["return a patch or proposed change without applying it".to_string()]
            }
            harness_contract::tool::ToolEffectKind::Network => {
                vec!["use already-authorized local or cached evidence".to_string()]
            }
            harness_contract::tool::ToolEffectKind::Process
            | harness_contract::tool::ToolEffectKind::Package => {
                vec!["inspect and report the required operation without executing it".to_string()]
            }
            harness_contract::tool::ToolEffectKind::System
            | harness_contract::tool::ToolEffectKind::Destructive
            | harness_contract::tool::ToolEffectKind::Unknown => Vec::new(),
        };
        crate::AuthorizationRequest {
            principal_id: if delegated {
                format!("agent:{}", self.memory_agent_id)
            } else {
                format!("session:{}", self.session_id())
            },
            capability: descriptor.tool_id.clone(),
            input: input.to_string(),
            idempotency_key,
            effect: descriptor.clone(),
            parent_ceiling: if delegated {
                self.permission_policy.active_mode()
            } else {
                crate::PermissionMode::DangerFullAccess
            },
            parent_lease_id: delegated.then(|| format!("binding:{}", self.memory_agent_id)),
            policy_revision: execution_policy.revision,
            recovery_scope,
            context: permission_context,
            safe_alternatives,
        }
    }

    pub(super) fn record_capability_assessment(
        &self,
        assessment: &harness_contract::policy::CapabilityAssessment,
    ) {
        if let Some(cowd) = self.cowd_bus() {
            cowd.emit(crate::cowd_event::CowdEvent::CapabilityAssessed {
                assessment: assessment.clone(),
            });
        }
        let mut refs = vec![RuntimeEventRef {
            kind: "capability".to_string(),
            id: assessment.capability.clone(),
        }];
        if let Some(lease) = assessment.lease.as_ref() {
            refs.push(RuntimeEventRef {
                kind: "authorization_lease".to_string(),
                id: lease.lease_id.clone(),
            });
        }
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "authorization.capability_assessed",
            Some(format!("{:?}", assessment.path).to_ascii_lowercase()),
            refs,
            serde_json::to_value(assessment).unwrap_or_else(|_| serde_json::json!({})),
        );
        let Some(store) = self.runtime_event_store.as_ref() else {
            for transition in self.authorization_negotiator.drain_transitions() {
                if let Some(cowd) = self.cowd_bus() {
                    cowd.emit(crate::cowd_event::CowdEvent::AuthorizationLeaseTransition {
                        transition,
                    });
                }
            }
            return;
        };
        let _ = self
            .authorization_negotiator
            .take_transitions_for_persistence();
        for transition in self
            .authorization_negotiator
            .transitions_awaiting_persistence()
        {
            // Authorization leases live on their own stream so parallel Team
            // agents and early-tool grants never contend with session model
            // events on the shared `session:<id>` stream.
            let stream_id = format!("authorization-lease:{}", transition.lease.lease_id);
            if let Err(error) = crate::authorization_negotiator::persist_authorization_transition(
                store,
                &stream_id,
                "conversation_runtime",
                &transition,
            ) {
                tracing::warn!(
                    %error,
                    transition_id = transition.transition_id,
                    "authorization transition remains hot because durable append failed"
                );
                break;
            }
            if self
                .authorization_negotiator
                .acknowledge_persisted_transitions(std::slice::from_ref(&transition.transition_id))
                == 1
            {
                if let Some(cowd) = self.cowd_bus() {
                    cowd.emit(crate::cowd_event::CowdEvent::AuthorizationLeaseTransition {
                        transition,
                    });
                }
            }
        }
    }

    pub(super) fn assess_tool_authorization_at(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        timeout_secs: u64,
        execution_policy: &harness_contract::policy::SessionExecutionPolicy,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        let request = self.authorization_request(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context,
            execution_policy,
        );
        let bound_policy = self.permission_policy.bound_to_snapshot(execution_policy);
        let evaluated = self
            .authorization_negotiator
            .assess_effective(&bound_policy, &request);
        let assessment = evaluated.assessment;
        if let Some(lease) = assessment.lease.clone() {
            self.record_capability_assessment(&assessment);
            return crate::ToolPolicy
                .authorize(
                    &evaluated.effective,
                    &assessment,
                    idempotency_key,
                    lease,
                    timeout_secs,
                )
                .map(ToolAuthorizationDecision::Authorized)
                .map_err(|error| RuntimeError::new(error.to_string()));
        }
        Ok(ToolAuthorizationDecision::Gap {
            assessment,
            effective: evaluated.effective,
        })
    }

    pub(crate) async fn negotiate_tool_authorization(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        timeout_secs: u64,
        _prompter: &crate::permissions::SharedPrompter,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        let execution_policy = self.permission_policy.execution_policy_control().snapshot();
        let initial = self.assess_tool_authorization_at(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context.clone(),
            timeout_secs,
            &execution_policy,
        )?;
        let ToolAuthorizationDecision::Gap {
            assessment,
            effective,
        } = initial
        else {
            return Ok(initial);
        };
        if assessment.path != harness_contract::policy::AuthorizationPath::HumanApproval {
            let assessment = self.govern_capability_gap(assessment);
            self.record_capability_assessment(&assessment);
            return Ok(ToolAuthorizationDecision::Gap {
                assessment,
                effective,
            });
        }

        let explicit_ask = permission_context.override_decision()
            == Some(crate::permissions::PermissionOverride::Ask);
        let request = self.authorization_request(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context,
            &execution_policy,
        );
        let Some(coordinator) = &self.approval_coordinator else {
            let assessment = self.govern_capability_gap(assessment);
            self.record_capability_assessment(&assessment);
            return Ok(ToolAuthorizationDecision::Gap {
                assessment,
                effective,
            });
        };
        let approved_grant = {
            let execution_context = self
                .cowd_bus()
                .and_then(crate::CowdEventBus::current_execution_context);
            let activity_binding = self
                .cowd_bus()
                .and_then(crate::CowdEventBus::current_activity_binding);
            let source = harness_contract::policy::ApprovalSource {
                kind: if self.memory_agent_id != "primary" {
                    harness_contract::policy::ApprovalSourceKind::Agent
                } else {
                    harness_contract::policy::ApprovalSourceKind::Session
                },
                session_id: Some(self.session_id().to_string()),
                agent_id: (self.memory_agent_id != "primary").then(|| self.memory_agent_id.clone()),
                team_id: self.memory_team_id.clone(),
                mission_id: None,
                resource_ref: Some(self.checkpoint_workspace_id.clone()),
                review_ref: None,
                application: None,
            };
            let context = harness_contract::policy::ApprovalContext {
                principal_id: request.principal_id.clone(),
                profile_id: execution_policy.autonomy_profile.as_str().to_string(),
                approval_profile: Some(execution_policy.approval_profile),
                workspace_key: self.checkpoint_workspace_id.clone(),
                session_id: Some(self.session_id().to_string()),
                turn_id: execution_context
                    .as_ref()
                    .map(|value| value.turn_id.clone()),
                task_id: activity_binding
                    .as_ref()
                    .map(|binding| binding.task_id.clone()),
                capability: descriptor.tool_id.clone(),
                invocation_id: Some(idempotency_key.clone()),
                execution_id: execution_context
                    .as_ref()
                    .map(|value| value.execution_id.clone()),
                strategy_decision_ref: None,
                source_surface: Some("gateway_session".to_string()),
                resource_targets: descriptor
                    .scopes
                    .iter()
                    .filter_map(|scope| scope.target.clone())
                    .collect(),
                effect: Some(effective.descriptor.clone()),
                explicit_ask,
                policy_revision: execution_policy.revision,
                requested_sandbox_posture: Some(execution_policy.sandbox_posture),
                effective_sandbox_posture: Some(execution_policy.sandbox_posture),
            };
            let pending_hook = self.cowd_bus.clone().map(|cowd| {
                let tool = descriptor.tool_id.clone();
                Arc::new(move |request: &harness_contract::policy::ApprovalRequest| {
                    cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
                        status: harness_contract::projection::ExecutionLiveStatus::WaitingApproval,
                        detail: Some(tool.clone()),
                    });
                    cowd.emit(crate::cowd_event::CowdEvent::ApprovalRequested {
                        request_id: request.approval_id.clone(),
                        tool: tool.clone(),
                    });
                }) as crate::ApprovalPendingHook
            });
            let approval_result = coordinator
                .resolve_tool(
                    source,
                    context,
                    Some(execution_policy.autonomy_profile),
                    &effective.descriptor,
                    input,
                    self.cancellation_token(),
                    Some(self.session_input_stream.input_notifier()),
                    pending_hook,
                    Duration::from_secs(timeout_secs.max(1)),
                )
                .await;
            emit_approval_resolution_event(self.cowd_bus(), coordinator.queue(), &approval_result);
            match approval_result {
                Ok(crate::ApprovalResolution::Approved { grant, .. }) => grant,
                Ok(crate::ApprovalResolution::Denied {
                    reason,
                    approval_id,
                })
                | Ok(crate::ApprovalResolution::Cancelled {
                    reason,
                    approval_id,
                }) => {
                    let denied = denied_capability_assessment(assessment, &reason, &approval_id);
                    self.record_capability_assessment(&denied);
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                    });
                }
                Ok(crate::ApprovalResolution::ControlRequested {
                    reason,
                    approval_id,
                }) => {
                    self.consume_active_runtime_inputs_for_next_step(
                        TurnInputCheckpoint::AfterToolResult,
                    );
                    let denied = denied_capability_assessment(assessment, &reason, &approval_id);
                    self.record_capability_assessment(&denied);
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                    });
                }
                Err(error) => {
                    let denied = denied_capability_assessment(
                        assessment,
                        &error,
                        &format!("tool-approval:{idempotency_key}"),
                    );
                    self.record_capability_assessment(&denied);
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                    });
                }
            }
        };

        let current_revision = self.permission_policy.execution_policy_control().revision();
        if current_revision != execution_policy.revision {
            let denied = denied_capability_assessment(
                assessment,
                "session policy changed while approval was pending; replan the effect",
                &approved_grant.grant_id,
            );
            self.record_capability_assessment(&denied);
            return Ok(ToolAuthorizationDecision::Gap {
                assessment: denied,
                effective,
            });
        }
        let bound_policy = self.permission_policy.bound_to_snapshot(&execution_policy);
        let approved = self.authorization_negotiator.approve_effective(
            &bound_policy,
            &request,
            &effective,
            &approved_grant,
        );
        self.record_capability_assessment(&approved);
        let Some(lease) = approved.lease.clone() else {
            return Ok(ToolAuthorizationDecision::Gap {
                assessment: approved,
                effective,
            });
        };
        let authorized = crate::ToolPolicy
            .authorize(&effective, &approved, idempotency_key, lease, timeout_secs)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if approved_grant.scope == harness_contract::policy::ApprovalGrantScope::Once {
            coordinator
                .queue()
                .consume_once_grant(&approved_grant.grant_id)
                .map_err(RuntimeError::new)?;
        }
        Ok(ToolAuthorizationDecision::Authorized(authorized))
    }

    pub(super) fn govern_capability_gap(
        &self,
        mut assessment: harness_contract::policy::CapabilityAssessment,
    ) -> harness_contract::policy::CapabilityAssessment {
        let Some(fingerprint) = assessment
            .gap
            .as_ref()
            .filter(|gap| gap.recoverable)
            .map(|gap| gap.fingerprint.clone())
        else {
            return assessment;
        };
        let Some(context) = self
            .cowd_bus()
            .and_then(crate::CowdEventBus::current_execution_context)
        else {
            close_controlled_recovery_gap(
                &mut assessment,
                "controlled recovery requires an exact durable turn identity",
                "authorization.recovery_missing_turn_identity",
            );
            return assessment;
        };
        let recovery_scope = format!("turn:{}", context.turn_id);
        if !self
            .authorization_negotiator
            .claim_controlled_recovery(&assessment, &recovery_scope)
        {
            close_controlled_recovery_gap(
                &mut assessment,
                "the same capability gap already consumed its single controlled recovery",
                "authorization.recovery_circuit_open",
            );
            return assessment;
        }
        let claim = crate::authorization_negotiator::ControlledRecoveryClaimRecord {
            fingerprint: fingerprint.clone(),
            recovery_scope,
            session_id: context.session_id,
            turn_id: context.turn_id,
            execution_id: context.execution_id,
            capability: assessment.capability.clone(),
        };
        let persist_result = self
            .runtime_event_store
            .as_ref()
            .ok_or_else(|| {
                "controlled recovery requires the durable Runtime event store".to_string()
            })
            .and_then(|store| {
                crate::authorization_negotiator::persist_controlled_recovery_claim(store, &claim)
            });
        if let Err(error) = persist_result {
            self.authorization_negotiator
                .rollback_unpersisted_controlled_recovery_claim(&fingerprint);
            close_controlled_recovery_gap(
                &mut assessment,
                &format!("controlled recovery claim was not durably recorded: {error}"),
                "authorization.recovery_persistence_failed",
            );
        } else {
            assessment.evidence_refs.push(format!(
                "authorization.controlled_recovery_claim:{fingerprint}"
            ));
        }
        assessment
    }

    pub(crate) fn restore_controlled_recovery_claims_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        execution_id: &str,
    ) -> Result<usize, RuntimeError> {
        if self.session_id().to_string() != session_id
            || turn_id.trim().is_empty()
            || execution_id.trim().is_empty()
        {
            return Err(RuntimeError::new(
                "controlled recovery restore identity does not match the active Session turn",
            ));
        }
        let store = self.runtime_event_store.as_ref().ok_or_else(|| {
            RuntimeError::new("controlled recovery restore requires the Runtime event store")
        })?;
        let claims = crate::authorization_negotiator::load_open_controlled_recovery_claims(
            store,
            session_id,
            turn_id,
            execution_id,
        )
        .map_err(RuntimeError::new)?;
        let mut restored = 0;
        for claim in claims {
            if self
                .authorization_negotiator
                .restore_controlled_recovery_claim(&claim.fingerprint, &claim.recovery_scope)
            {
                restored += 1;
            }
        }
        Ok(restored)
    }
}
