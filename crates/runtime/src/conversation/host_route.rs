//! Stable host construction and route composition.

use super::*;

impl<T> StandardRuntimeHost<T>
where
    T: ToolExecutor,
{
    pub fn new(config: StandardRuntimeHostConfig<T>) -> Result<Self, String> {
        let services = Arc::clone(&config.runtime_services);
        let recovered_tool_receipt_count = config.recovered_tool_receipt_count;
        let root_provider_owner = config.execution_role.owns_root_presentation();
        let execution_service_class = if config
            .reality_binding
            .as_ref()
            .is_some_and(|binding| binding.evaluation.is_some())
        {
            harness_contract::execution_graph::ExecutionServiceClass::Maintenance
        } else if config.execution_parent.is_some() {
            harness_contract::execution_graph::ExecutionServiceClass::Foreground
        } else {
            harness_contract::execution_graph::ExecutionServiceClass::Interactive
        };
        let active_model = config.model.clone();
        let model_context_window = config.model_context_window.unwrap_or_else(|| {
            let overrides = config.feature_config.model_context_windows();
            model_context_window_with_overrides(&active_model, Some(overrides))
        });
        let evaluation_provider_token_lease = services
            .evaluation_provider_token_leases()
            .get(&config.session.session_id)
            .map_err(|error| error.to_string())?;
        let system_prompt = canonical_host_system_prompt(config.system_prompt);
        let selected_memory_manager = services.memory_manager();
        let mut runtime = crate::ConversationRuntime::new_with_features_and_selected_memory(
            config.session,
            ProviderRuntimeClient::new_with_transport_and_template_cache(
                Arc::clone(&config.provider_registry),
                Arc::clone(services.provider_transport_pool()),
                Arc::clone(services.provider_template_cache()),
                active_model.clone(),
                config.tool_definitions,
            )?
            .with_execution_supervisor(services.execution_supervisor())
            .with_emit_output(config.emit_output)
            .with_stream_callback(config.stream_callback.clone()),
            config.tool_executor.clone(),
            config.permission_policy,
            system_prompt,
            &config.feature_config,
            selected_memory_manager,
        )
        .with_model_context_window(model_context_window)
        .with_knowledge_activation(services.knowledge_activation())
        .with_explicit_team_escalation(root_provider_owner)
        .with_runtime_event_store(Arc::clone(services.event_store()))
        .with_outcome_runtime(
            Arc::clone(services.outcome_service()),
            Arc::clone(services.outcome_projector()),
        )
        .with_artifact_store(Arc::clone(services.artifact_store()))
        .with_skill_profiles(config.skill_profiles)
        .with_agent_skill_profile(config.agent_skill_profile)
        .with_skill_prompt_assets(config.skill_prompt_assets)
        .with_skill_instruction_source(config.skill_instruction_source)
        .with_memory_identity(
            config.memory_agent_id,
            config.memory_definition_lineage_id,
            config.memory_team_id,
            config.memory_read_scopes,
        )
        .with_checkpoint_identity(services.workspace_key(), config.execution_identity)
        .with_maintenance_supervisor(services.maintenance_supervisor())
        .with_tool_execution_plane(Arc::clone(services.tool_execution_plane()))
        .with_execution_service_class(execution_service_class)
        .with_provider_admission(Arc::clone(services.resource_manager()))
        .with_provider_resource_config(services.provider_resource_config())
        .with_provider_fallback_policy(services.provider_fallback_policy())
        .with_approval_coordinator(Arc::clone(services.approval_coordinator()));
        if let Some(lease) = evaluation_provider_token_lease {
            runtime = runtime.with_evaluation_provider_token_lease(lease);
        }
        if let Some(binding) = config.reality_binding {
            runtime = runtime
                .with_reality_binding(services.reality_recall_port().as_ref().clone(), binding);
        }
        runtime.set_active_model(active_model);
        if recovered_tool_receipt_count > 0 {
            runtime.require_next_model_final_response();
        }

        if let Some(journal) = services.session_journal_port() {
            runtime = runtime.with_session_journal_port(journal);
        }
        if let Some(history) = services.session_history_reader() {
            runtime = runtime.with_session_history_reader(history);
        }
        runtime = runtime.with_hot_state(Arc::clone(services.hot_state()));
        if let Some(callback) = config.tool_callback {
            runtime = runtime.with_tool_callback(callback);
        }
        if let Some(reporter) = config.hook_progress_reporter {
            runtime = runtime.with_hook_progress_reporter(reporter);
        }
        runtime = runtime.with_cowd_event_bus(CowdEventBus::new());
        for item in config.external_context_items {
            runtime.push_external_context_item(item);
        }

        Ok(Self {
            runtime: Some(runtime),
            inflight_turn: None,
            services,
            execution_parent: config.execution_parent,
            execution_lineage: config.execution_lineage,
            execution_role: config.execution_role,
            recovered_tool_receipt_count,
        })
    }

    pub(crate) fn set_delegated_provider_budget(
        &mut self,
        reservation: harness_contract::context::ChildExecutionBudgetReservation,
    ) -> Result<(), crate::RuntimeError> {
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.set_delegated_provider_budget(reservation)?;
        }
        Ok(())
    }

    #[allow(
        clippy::expect_used,
        reason = "the private runtime slot is only empty while an exclusive &mut submit call owns it"
    )]
    pub fn with_hook_abort_signal(mut self, hook_abort_signal: HookAbortSignal) -> Self {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing hook abort signal");
        self.runtime = Some(runtime.with_hook_abort_signal(hook_abort_signal));
        self
    }

    #[allow(
        clippy::expect_used,
        reason = "the private runtime slot is only empty while an exclusive &mut submit call owns it"
    )]
    pub fn install_turn_control(
        &mut self,
        cancellation_token: crate::CancellationToken,
        hook_abort_signal: HookAbortSignal,
    ) {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing turn control");
        self.runtime = Some(
            runtime
                .with_cancellation_token(cancellation_token)
                .with_hook_abort_signal(hook_abort_signal),
        );
    }

    pub fn cowd_bus(&self) -> Option<&CowdEventBus> {
        self.runtime_ref().cowd_bus()
    }

    pub fn services(&self) -> &Arc<crate::RuntimeServices> {
        &self.services
    }

    pub fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
        state: crate::RuntimeInputState,
    ) -> SessionInputReceipt {
        self.runtime_ref().admit_session_input(envelope, state)
    }

    pub fn session_input_projection(&self) -> SessionInputProjection {
        self.runtime_ref().session_input_projection()
    }

    pub fn active_turn_inbox(&self, turn_id: Option<TurnId>) -> TurnInboxSnapshot {
        self.runtime_ref().active_turn_inbox(turn_id)
    }

    pub fn session_input_stream(&self) -> crate::SessionInputStream {
        self.runtime_ref().session_input_stream()
    }

    pub fn set_context_profile(&self, profile: ContextProfile) {
        self.runtime_ref().set_context_profile(profile);
    }

    pub fn set_execution_policy(
        &self,
        policy: harness_contract::policy::SessionExecutionPolicy,
    ) -> Result<u64, String> {
        self.runtime_ref().set_execution_policy(policy)
    }

    #[must_use]
    pub fn approval_profile(&self) -> harness_contract::policy::ApprovalProfile {
        self.runtime_ref().approval_profile()
    }

    #[must_use]
    pub fn autonomy_profile(&self) -> crate::AutonomyProfileId {
        self.runtime_ref().autonomy_profile()
    }

    pub fn execution_policy_control(&self) -> crate::permissions::SessionExecutionPolicyControl {
        self.runtime_ref()
            .permission_policy()
            .execution_policy_control()
    }

    pub fn set_execution_service_class(
        &mut self,
        service_class: harness_contract::execution_graph::ExecutionServiceClass,
    ) {
        self.runtime_mut()
            .set_execution_service_class(service_class);
    }

    pub fn set_model_step_limit_override(&self, limit: usize) {
        self.runtime_ref().set_model_step_limit_override(limit);
    }

    pub fn set_delegated_focus_policy(
        &self,
        novelty_target_bp: u16,
        acceptance_scopes: Vec<String>,
        required_output_fields: Vec<String>,
    ) {
        self.runtime_ref().set_delegated_focus_policy(
            novelty_target_bp,
            acceptance_scopes,
            required_output_fields,
        );
    }

    pub fn inject_resume_context(&self, packet: ResumeContextPacket) {
        self.runtime_ref().inject_resume_context(packet);
    }

    pub fn replace_external_context_sources(
        &self,
        sources: &[ContextSourceKind],
        items: Vec<ContextItem>,
    ) {
        let runtime = self.runtime_ref();
        for source in sources {
            runtime.clear_external_context_source(*source);
        }
        for item in items {
            runtime.push_external_context_item(item);
        }
    }

    /// Submit a user turn through the canonical ExecutionGraph runner.
    ///
    /// This is the only production entry point that may start provider-backed
    /// turn work. Gateway and Agent backends receive a terminal result emitted
    /// by the synthesize node instead of inspecting the session transcript.
    pub async fn submit_turn(
        &mut self,
        content: &str,
        prompter: &SharedPrompter,
    ) -> Result<TurnSummary, RuntimeError> {
        self.restore_inflight_turn().await?;
        let Some(runtime) = self.runtime.take() else {
            return Err(RuntimeError::new(
                "Runtime host has no conversation available for this turn",
            ));
        };
        self.start_turn(runtime, content, prompter, None).await?;
        self.await_started_turn().await
    }

    pub async fn submit_ingress_turn(
        &mut self,
        content: &str,
        prompter: &SharedPrompter,
        ingress: TurnIngressRef,
    ) -> Result<TurnSummary, RuntimeError> {
        self.restore_inflight_turn().await?;
        let Some(runtime) = self.runtime.take() else {
            return Err(RuntimeError::new(
                "Runtime host has no conversation available for ingress execution",
            ));
        };
        // Gateway ingress owns the user row and the terminal outbox atomically
        // commits the complete Runtime transcript.
        let execution_id = crate::session_execution::session_ingress_graph_id(
            &ingress.session_id,
            &ingress.request_id,
            &ingress.turn_id,
        );
        self.start_turn(runtime, content, prompter, Some((ingress, execution_id)))
            .await?;
        self.await_started_turn().await
    }

    pub async fn append_external_message(
        &self,
        message: ConversationMessage,
    ) -> Result<(), RuntimeError> {
        self.runtime_ref().append_external_message(message).await
    }

    pub async fn session_snapshot(&self) -> Session {
        self.runtime_ref().session_snapshot().await
    }

    pub async fn session_head(&self) -> SessionReadHead {
        self.runtime_ref().session_head().await
    }

    pub async fn compact_active_session(
        &mut self,
    ) -> Result<(Option<AutoCompactionEvent>, Session), RuntimeError> {
        let result = self.runtime_mut().compact_active_session().await?;
        let session = self.runtime_ref().session_snapshot().await;
        Ok((result, session))
    }

    pub fn active_session_stats_session(&self) -> Session {
        self.runtime_ref().session_snapshot_blocking()
    }

    pub async fn update_session_model(&mut self, model: &str) {
        let runtime = self.runtime_mut();
        runtime.set_active_model(model.to_string());
        let mut session = runtime.session_mut_async().await;
        session.model = Some(model.to_string());
    }

    pub fn last_context_envelope(&self) -> Option<ContextEnvelope> {
        self.runtime_ref().last_context_envelope()
    }

    pub fn last_context_turn_report(&self) -> Option<harness_contract::context::ContextTurnReport> {
        self.runtime_ref().last_context_turn_report()
    }

    /// Start a graph-owned turn without making the caller the owner of the
    /// conversation runtime.  This is the cancellation boundary: dropping a
    /// request future leaves the receiver in `self`, while the task keeps
    /// running long enough to send the runtime back.
    pub(super) async fn start_turn(
        &mut self,
        runtime: crate::ConversationRuntime<ProviderRuntimeClient, T>,
        content: &str,
        prompter: &SharedPrompter,
        ingress: Option<(TurnIngressRef, String)>,
    ) -> Result<(), RuntimeError> {
        debug_assert!(self.inflight_turn.is_none());
        let services = Arc::clone(&self.services);
        let content = content.to_string();
        let prompter = prompter.clone();
        let execution_parent = self.execution_parent.clone();
        let execution_lineage = self.execution_lineage.clone();
        let execution_role = self.execution_role;
        let recovered_tool_receipt_count = self.recovered_tool_receipt_count;
        let (runtime_sender, runtime_receiver) =
            tokio::sync::oneshot::channel::<crate::ConversationRuntime<ProviderRuntimeClient, T>>();
        let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
        let execution_supervisor = Arc::clone(services.execution_supervisor());
        if let Err(error) = execution_supervisor
            .spawn_owned("conversation_turn", Box::pin(async move {
                let Ok(runtime) = runtime_receiver.await else {
                    return;
                };
                let (runtime, result) = match ingress {
                    Some((ingress, execution_id)) => {
                        // Scope every provider/tool/approval event to the
                        // deterministic SessionIngress execution. The guard lives
                        // in the owning task, so it also clears if the caller has
                        // already been cancelled.
                        let execution_bus = runtime.cowd_bus().cloned();
                        let execution_bus_lease = execution_bus.as_ref().map(|bus| {
                            services.bind_active_execution_bus(execution_id.clone(), bus.clone())
                        });
                        let execution_scope = execution_bus.map(|bus| {
                            let activity_id =
                                format!("activity:execution:{execution_id}");
                            bus.enter_execution_with_activity(
                                crate::CowdExecutionContext {
                                    execution_id: execution_id.clone(),
                                    session_id: ingress.session_id.clone(),
                                    turn_id: ingress.turn_id.clone(),
                                },
                                Some(
                                    harness_contract::projection::RuntimeActivityBinding {
                                        root_execution_id: execution_id,
                                        session_id: ingress.session_id.clone(),
                                        turn_id: ingress.turn_id.clone(),
                                        root_task_id: ingress.root_task_id.clone(),
                                        task_id: ingress.primary_task_id.clone(),
                                        activity_id,
                                        node_id: None,
                                        parent_activity_id: None,
                                        initiator_activity_id: None,
                                        team_run_id: None,
                                        agent_instance_id: None,
                                        agent_run_id: None,
                                        skill_id: None,
                                        skill_revision: None,
                                        skill_activation_id: None,
                                        tool_contract_id: None,
                                        tool_call_id: None,
                                        approval_id: None,
                                        parallel_group_id: None,
                                        revision: ingress.claim_revision.max(1),
                                        fence: ingress.session_generation.max(1),
                                        generation: ingress.session_generation.max(1),
                                    },
                                ),
                            )
                        });
                        let runtime = runtime;
                        let fence = match usize::try_from(ingress.input_sequence) {
                            Ok(input_sequence) => match services.session_query_port() {
                                Some(query) => crate::SessionExecutionFence::from_claim(
                                    query,
                                    ingress.request_id.clone(),
                                    ingress.session_id.clone(),
                                    ingress.session_generation,
                                    input_sequence,
                                    ingress.claim_owner.clone(),
                                    ingress.claim_token.clone(),
                                ),
                                None => {
                                    Err("Session ingress requires a durable execution fence store"
                                        .to_string())
                                }
                            },
                            Err(_) => Err(format!(
                                "Session ingress sequence {} exceeds this platform's durable index range",
                                ingress.input_sequence
                            )),
                        };
                        let completed = match fence {
                            Ok(fence) => {
                                submit_owned_conversation_turn_with_ingress(
                                    runtime.with_session_execution_fence(fence),
                                    services,
                                    &content,
                                    &prompter,
                                    Some(ingress),
                                    execution_parent,
                                    execution_lineage,
                                    execution_role,
                                    recovered_tool_receipt_count,
                                )
                                .await
                            }
                            Err(error) => (runtime, Err(RuntimeError::new(error))),
                        };
                        drop(execution_scope);
                        drop(execution_bus_lease);
                        completed
                    }
                    None => {
                        submit_owned_conversation_turn_with_ingress(
                            runtime,
                            services,
                            &content,
                            &prompter,
                            None,
                            execution_parent,
                            execution_lineage,
                            execution_role,
                            recovered_tool_receipt_count,
                        )
                        .await
                    }
                };
                let _ = completion_sender.send((runtime, result));
            }))
            .await
        {
            self.runtime = Some(runtime);
            return Err(RuntimeError::new(error.to_string()));
        }
        match runtime_sender.send(runtime) {
            Ok(()) => {
                self.inflight_turn = Some(completion_receiver);
                Ok(())
            }
            Err(runtime) => {
                self.runtime = Some(runtime);
                Err(RuntimeError::new(
                    "Runtime execution supervisor stopped before accepting the conversation turn",
                ))
            }
        }
    }

    pub(super) async fn await_started_turn(&mut self) -> Result<TurnSummary, RuntimeError> {
        let completion = {
            let Some(receiver) = self.inflight_turn.as_mut() else {
                return Err(RuntimeError::new(
                    "Runtime host has no submitted turn to await",
                ));
            };
            receiver.await
        };
        self.inflight_turn = None;
        let (runtime, result) = completion.map_err(|error| {
            RuntimeError::new(format!(
                "submitted Runtime turn ended before recovery: {error}"
            ))
        })?;
        self.runtime = Some(runtime);
        result
    }

    /// Reclaim an interrupted caller's completed graph before beginning a new
    /// one. Its old result is intentionally not replayed to a new caller; the
    /// graph/event stores are the durable record for that prior turn.
    pub(super) async fn restore_inflight_turn(&mut self) -> Result<(), RuntimeError> {
        if self.inflight_turn.is_none() {
            return Ok(());
        }
        let completion = {
            let Some(receiver) = self.inflight_turn.as_mut() else {
                return Ok(());
            };
            receiver.await
        };
        self.inflight_turn = None;
        let (runtime, _result) = completion.map_err(|error| {
            RuntimeError::new(format!(
                "interrupted Runtime turn ended before recovery: {error}"
            ))
        })?;
        self.runtime = Some(runtime);
        Ok(())
    }

    #[allow(
        clippy::expect_used,
        reason = "the slot can only be empty during an exclusive mutable submit operation"
    )]
    pub(super) fn runtime_ref(&self) -> &crate::ConversationRuntime<ProviderRuntimeClient, T> {
        self.runtime
            .as_ref()
            .expect("runtime should exist while standard runtime host is alive")
    }

    #[allow(
        clippy::expect_used,
        reason = "the slot can only be empty during an exclusive mutable submit operation"
    )]
    pub(super) fn runtime_mut(
        &mut self,
    ) -> &mut crate::ConversationRuntime<ProviderRuntimeClient, T> {
        self.runtime
            .as_mut()
            .expect("runtime should exist while standard runtime host is alive")
    }
}
