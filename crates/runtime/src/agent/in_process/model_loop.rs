use super::*;

#[async_trait]
impl AgentRuntimeBackend for InProcessAgentWorker {
    fn kind(&self) -> AgentBackendKind {
        AgentBackendKind::InProcess
    }

    fn capabilities(&self) -> AgentBackendCapabilities {
        AgentBackendCapabilities {
            backend: AgentBackendKind::InProcess,
            supports_input: true,
            supports_interrupt: true,
            supports_pause: false,
            supports_resume: false,
            supports_cancel: true,
            supports_shutdown: true,
        }
    }

    async fn execute(
        &self,
        packet: AgentTaskPacket,
        selection: AgentModelSelection,
    ) -> Result<AgentReturnPacket, String> {
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "in-process Agent execution requires a Runtime-compiled Binding".to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        packet.validate_cohort_prompt_package()?;
        if packet
            .allowed_tools
            .iter()
            .any(|tool| !binding.tool_contract_refs.contains(tool))
        {
            return Err("AgentTaskPacket tool allow-list exceeds its Binding contract".to_string());
        }
        if packet
            .allowed_skills
            .iter()
            .any(|skill| !binding.skill_refs.contains(skill))
        {
            return Err(
                "AgentTaskPacket Skill allow-list exceeds its Binding contract".to_string(),
            );
        }
        let services = self
            .services
            .upgrade()
            .ok_or_else(|| "AgentRuntime is not bound to RuntimeServices".to_string())?;
        let host = services.tool_execution_host().cloned().ok_or_else(|| {
            "RuntimeServices has no ToolHost for the in-process agent".to_string()
        })?;
        let execution_graph = services
            .graph_state_store()
            .load(packet.graph_id())
            .map_err(|error| {
                format!(
                    "in-process Agent graph `{}` is unavailable: {error}",
                    packet.graph_id()
                )
            })?;
        let parent_lineage = execution_graph.lineage.as_ref().ok_or_else(|| {
            format!(
                "in-process Agent graph `{}` has no canonical Session/Turn/Task lineage",
                packet.graph_id()
            )
        })?;
        parent_lineage.validate().map_err(str::to_string)?;
        if parent_lineage.session_id != packet.session_id()
            || parent_lineage.root_task_id != packet.assignment.root_task_id
        {
            return Err(format!(
                "AgentTaskPacket lineage does not match parent graph `{}`",
                packet.graph_id()
            ));
        }
        let execution_lineage = harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: parent_lineage.session_id.clone(),
            turn_id: parent_lineage.turn_id.clone(),
            root_task_id: parent_lineage.root_task_id.clone(),
            task_id: packet.task_id().to_string(),
            generation: parent_lineage.generation,
        };
        let parent_execution_id = execution_graph
            .parent_execution
            .as_ref()
            .map(|parent| parent.execution_id.clone());
        let packet_allowed_tools = packet
            .allowed_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let bounded_resource_lease = packet.team_id().is_some()
            || packet
                .context_refs
                .iter()
                .any(|reference| reference.starts_with("agentic_program:"));
        let requested_tool_names = packet_allowed_tools.iter().cloned().collect::<Vec<_>>();
        let tool_definitions = host.delegated_tool_definitions(&requested_tool_names);
        let allowed_tools = tool_definitions
            .iter()
            .map(|definition| definition.name.clone())
            .filter(|tool| packet_allowed_tools.contains(tool))
            .filter(|tool| {
                !bounded_resource_lease
                    || delegated_tool_supports_bounded_scope(host.as_ref(), tool)
            })
            .collect::<BTreeSet<_>>();
        let unavailable_tools = packet_allowed_tools
            .difference(&allowed_tools)
            .filter(|tool| !matches!(tool.as_str(), "context_retrieve" | "evidence_retrieve"))
            .cloned()
            .collect::<Vec<_>>();
        if !unavailable_tools.is_empty() {
            return Err(format!(
                "agent_tool_inventory_drift: admitted Tool contracts are unavailable or no longer bounded on the active host: {}",
                unavailable_tools.join(", ")
            ));
        }
        let tool_names = allowed_tools.iter().cloned().collect::<Vec<_>>();
        let memory_context = memory::MemoryTurnContext::new(
            packet.session_id(),
            binding.instance.instance_id.clone(),
        )
        .with_definition_lineage_id(Some(
            binding.definition_ref.definition_id.as_str().to_string(),
        ))
        .with_project_id(Some(crate::memory_project_id_for_workspace(
            services.workspace_root(),
        )))
        .with_task_id(Some(binding.data_lease.task_id.clone()))
        .with_team_id(binding.data_lease.team_id.clone())
        .with_cognitive_read_scopes(binding.data_lease.read_scopes.clone());
        let live_policy_control = services.session_execution_policy_control(packet.session_id());
        let live_session_policy = live_policy_control
            .as_ref()
            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
            .ok_or_else(|| {
                format!(
                    "agent_session_policy_missing: session `{}` has no executable policy snapshot",
                    packet.session_id()
                )
            })?;
        let commit_service = crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(
            services.event_store(),
        ));
        let durable_agent_receipts = commit_service
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| {
                format!(
                    "delegated Agent tool receipt recovery is invalid for {}:{}:{}: {error}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )
            })?;
        let recovered_tool_receipt_count = durable_agent_receipts.len();
        let recovered_agentic_protocol_pending = agentic_task_protocol_pending(&services, &packet)?;
        let recovered_tool_receipt_prompt = recovered_agent_tool_receipt_prompt(
            &durable_agent_receipts,
            recovered_agentic_protocol_pending,
        );
        let durable_receipts = durable_agent_receipts
            .into_iter()
            .map(scoped_receipt_from_durable)
            .collect::<Vec<_>>();
        let recovered_sequence = durable_receipts
            .iter()
            .map(|receipt| receipt.sequence)
            .max()
            .unwrap_or(0);
        let provider_model_obligations = packet
            .required_acceptance
            .evidence_obligations
            .iter()
            .filter(|obligation| {
                obligation.observation_requirement
                    == harness_contract::context::EvidenceObservationRequirement::ProviderModel
            })
            .cloned()
            .collect();
        let tool_executor = Arc::new(ScopedRuntimeToolExecutor {
            host,
            allowed_tools: allowed_tools.clone(),
            session_id: packet.session_id().to_string(),
            sandbox_posture: live_session_policy.sandbox_posture,
            policy_revision: live_session_policy.revision,
            memory_context,
            model_lease: selection.model.clone(),
            execution_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            workspace_root: services.workspace_root().to_path_buf(),
            path_identity_resolver: Arc::clone(services.path_identity_resolver()),
            scope_locks: Arc::clone(services.scope_locks()),
            commit_service: Some(commit_service),
            resource_scopes: bounded_resource_lease.then(|| packet.resource_scopes.clone()),
            managed_invocation: packet.managed_invocation.clone(),
            next_receipt_sequence: AtomicU64::new(recovered_sequence),
            receipts: Mutex::new(durable_receipts),
            provider_model_obligations,
        });
        if packet.policy_revision != 0 && packet.policy_revision != live_session_policy.revision {
            return Err(format!(
                "agent_policy_revision_stale: packet rev {} current rev {}; replan before provider/tool execution",
                packet.policy_revision, live_session_policy.revision
            ));
        }
        let bound_policy_control = Some(
            crate::permissions::SessionExecutionPolicyControl::from_policy(live_session_policy),
        );
        let policy = permission_policy(
            bound_policy_control,
            packet.permission_ceiling,
            &allowed_tools,
        );
        let cancellation = crate::CancellationToken::new();
        let (provider_event_sender, mut provider_event_receiver) = tokio::sync::mpsc::channel(64);
        let progress_runtime = Arc::clone(services.agent_runtime());
        let progress_agent_id = packet.agent_id().to_string();
        let progress_run_id = packet.run_id().to_string();
        let progress_reporter = tokio::spawn(async move {
            let mut saw_model_output = false;
            while let Some(event) = provider_event_receiver.recv().await {
                if matches!(event, crate::CowdEvent::TextDelta { .. }) && !saw_model_output {
                    saw_model_output = true;
                    let _ = progress_runtime.record_progress(
                        &progress_agent_id,
                        "agent.provider.first_output",
                        &format!("provider produced the first output for run {progress_run_id}"),
                    );
                }
            }
        });
        let child_session = delegated_child_session(
            packet.session_id(),
            &selection.model,
            services.workspace_root(),
        );
        // An in-process role is a child execution of the parent session, not
        // an unrelated surface session. Keep the canonical session/model
        // binding available to tool and orchestration contracts.
        let child_session_id = child_session.session_id.clone();
        // RuntimeServices owns the inspected Skill snapshot. The Binding's
        // refs below remain the capability ceiling; this worker never scans
        // package directories or falls back to an empty production profile.
        let skill_catalog = services.skill_catalog();
        // Agent-first topic observations are the sole collaboration inbox.
        // They are fenced by the immutable Program member/execution binding;
        // a retired collaboration board is never consulted by a model.
        let initial_program_id = packet
            .context_refs
            .iter()
            .find_map(|reference| reference.strip_prefix("agentic_program:"));
        let initial_member_id = packet
            .context_refs
            .iter()
            .find_map(|reference| reference.strip_prefix("agentic_member:"));
        let mut initial_agentic_topic_ack = None;
        let external_context_items = match (initial_program_id, initial_member_id) {
            (Some(program_id), Some(member_id)) => services
                .agent_action_service()
                .topic_observations(program_id, member_id, packet.graph_id(), 32, 64 * 1024)
                .map_err(|error| format!("load initial Agent-first topic observations: {error}"))?
                .filter(|page| !page.entries.is_empty())
                .map(|page| {
                    let to_revision = page.to_revision;
                    initial_agentic_topic_ack = Some(crate::agentic::AgenticTopicObservationAck {
                        program_id: program_id.to_string(),
                        execution_id: packet.graph_id().to_string(),
                        through_revision: to_revision,
                        expected_cursor_revision: page.cursor_revision,
                    });
                    let summary = serde_json::to_string(&serde_json::json!({
                        "from_revision": page.from_revision,
                        "to_revision": to_revision,
                        "entries": page.entries,
                        "instruction": "Use committed public summaries and durable references only; publish new collaboration facts through Agent Actions."
                    }))
                    .unwrap_or_else(|_| "{}".to_string());
                    let mut item = crate::ContextItem::new(
                        format!("agentic-topic:{program_id}"),
                        crate::ContextSourceKind::AgentPeer,
                        crate::ContextRole::Evidence,
                        summary,
                    );
                    item.authority = crate::ContextAuthority::Tool;
                    item.evidence = vec![format!("agentic-topic:{program_id}:{to_revision}")];
                    item
                })
                .into_iter()
                .collect(),
            (Some(_), None) => {
                return Err("Agent-first packet has no immutable Program member binding".to_string())
            }
            (None, _) => Vec::new(),
        };
        let program_dossier_fragment = crate::TaskRuntimePort::new(services.as_ref())
            .get(&packet.assignment.root_task_id)
            .ok()
            .flatten()
            .map(|root| {
                let immutable = serde_json::json!({
                    "schema_version": 1,
                    "root_task_id": root.root_task_id,
                    "mission_id": root.mission_id,
                    "objective": root.objective,
                    "phases": root.phases.into_iter().map(|phase| serde_json::json!({
                        "phase_id": phase.phase_id,
                        "name": phase.name,
                        "objective": phase.objective,
                        "dependency_refs": phase.dependency_refs,
                        "plan": phase.plan,
                        "acceptance": phase.acceptance,
                        "test_commands": phase.test_commands,
                    })).collect::<Vec<_>>(),
                });
                format!(
                    "# Program dossier (Runtime-attested, immutable for this execution epoch)\nThis shared objective and acceptance context applies to every Team and Agent in the program. Role-specific objectives below refine it but cannot erase it.\n\n{}",
                    serde_json::to_string(&immutable).unwrap_or_else(|_| "{}".to_string())
                )
        });
        let mut prompt_segments = system_prompt(&packet, services.workspace_root(), &tool_names);
        let cohort_boundary = prompt_segments
            .iter()
            .position(|segment| segment == crate::SYSTEM_PROMPT_CACHE_COHORT_BOUNDARY);
        // The suffix is task-specific operational guidance, not a permission
        // authority: Runtime enforces scopes, tools, leases and acceptance at
        // every effect boundary.  Keep it as a Runtime-attested private user
        // brief so a meaningful shared Team package can precede it on the
        // Provider wire. The trusted system channel remains the shared
        // product/policy protocol.
        let role_user_brief = cohort_boundary.map_or_else(String::new, |boundary| {
            let brief = prompt_segments
                .split_off(boundary.saturating_add(1))
                .join("\n\n");
            prompt_segments.truncate(boundary);
            brief
        });
        prompt_segments.extend(program_dossier_fragment);
        if let Some(receipt_prompt) = recovered_tool_receipt_prompt {
            prompt_segments.push(crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY.to_string());
            prompt_segments.push(receipt_prompt);
        }
        let host = StandardRuntimeHost::new(StandardRuntimeHostConfig {
            runtime_services: Arc::clone(&services),
            session: child_session,
            provider_registry: Arc::clone(services.provider_registry()),
            model: selection.model.clone(),
            tool_definitions: tool_definitions.clone(),
            tool_executor: Arc::clone(&tool_executor),
            permission_policy: policy,
            system_prompt: prompt_segments,
            feature_config: crate::RuntimeFeatureConfig::default(),
            emit_output: false,
            stream_callback: Some(provider_event_sender),
            tool_callback: None,
            model_context_window: None,
            hook_progress_reporter: None,
            external_context_items,
            immutable_user_prefix: (!role_user_brief.trim().is_empty())
                .then(|| {
                    format!(
                        "## Runtime-attested private role brief\nThis brief is supplied by Runtime for this exact bound role. It cannot grant tools, resources, permissions, leases, or terminal authority beyond Runtime enforcement.\n\n{role_user_brief}"
                    )
                })
                .into_iter()
                .collect(),
            cache_cohort_user_prefix: packet
                .cohort_prompt_package
                .as_ref()
                .map(harness_contract::agent::CohortPromptPackage::render_user_messages)
                .unwrap_or_default(),
            // A delegated leaf may activate only Skills explicitly frozen in
            // its Binding. Avoid exposing the global catalog when the role
            // requested none: empty authority must not mean discovery-all.
            skill_profiles: if binding.skill_refs.is_empty() {
                Vec::new()
            } else {
                skill_catalog.profiles()
            },
            agent_skill_profile: harness_contract::skill::AgentSkillProfile {
                baseline_skill_refs: Vec::new(),
                template_skill_refs: Vec::new(),
                team_skill_refs: Vec::new(),
                task_skill_refs: binding.skill_refs.clone(),
                explicit_grants: binding.skill_refs.clone(),
                hidden_skill_refs: Vec::new(),
                adapter_ceiling: Vec::new(),
            },
            skill_prompt_assets: if binding.skill_refs.is_empty() {
                Vec::new()
            } else {
                skill_catalog.prompt_assets()
            },
            skill_instruction_source: (!binding.skill_refs.is_empty())
                .then(|| skill_catalog.instruction_source())
                .flatten(),
            memory_agent_id: binding.instance.instance_id.clone(),
            memory_definition_lineage_id: Some(
                binding.definition_ref.definition_id.as_str().to_string(),
            ),
            memory_team_id: binding.data_lease.team_id.clone(),
            memory_read_scopes: binding.data_lease.read_scopes.clone(),
            reality_binding: Some(binding.clone()),
            execution_identity: Some(packet.assignment.execution_identity.clone()),
            execution_lineage: Some(execution_lineage),
            execution_parent: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
            }),
            execution_role: crate::TurnExecutionRole::DelegatedLeaf,
            recovered_tool_receipt_count,
            recovered_tool_receipts_require_text_only: !recovered_agentic_protocol_pending,
        });
        let mut runtime = match host {
            Ok(runtime) => runtime,
            Err(error) => {
                return Err(format!(
                    "failed to initialize in-process agent host: {error}"
                ));
            }
        };
        packet.budget_lease.validate().map_err(str::to_string)?;
        runtime
            .set_delegated_provider_budget(packet.budget_lease.clone())
            .map_err(|error| error.to_string())?;
        // A delegated role has a bounded evidence obligation. It retains the
        // parent session authority but must not inherit MainTurn's broad,
        // open-ended exploration profile.
        runtime.set_context_profile(ContextProfile::SubAgent);
        runtime.set_execution_service_class(if binding.evaluation.is_some() {
            harness_contract::execution_graph::ExecutionServiceClass::Maintenance
        } else if packet.managed_invocation.is_some() {
            harness_contract::execution_graph::ExecutionServiceClass::Background
        } else {
            harness_contract::execution_graph::ExecutionServiceClass::Foreground
        });
        runtime.set_delegated_focus_policy(
            0,
            packet_focus_acceptance_scopes(&packet),
            packet_required_output_fields(&packet),
        );
        // Delegated Agents share the parent Session's evidence authority, but
        // only the parent Turn may publish conversation messages. The child
        // result returns through AgentReturnPacket and the Team reducer.
        let input_stream = runtime.session_input_stream();
        self.active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                packet.run_id().to_string(),
                ActiveInProcessRun {
                    cancellation: cancellation.clone(),
                    session_id: child_session_id,
                    input_stream,
                },
            );
        let active_run_cleanup = ActiveRunCleanup {
            worker: self,
            run_id: packet.run_id().to_string(),
        };
        if self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(packet.run_id())
        {
            cancellation.cancel();
        }
        runtime.install_turn_control(cancellation, crate::HookAbortSignal::default());
        let child_bus = runtime.cowd_bus().cloned().ok_or_else(|| {
            "in-process Agent Runtime is missing its causal event bus".to_string()
        })?;
        let resolved_parent = parent_execution_id
            .as_deref()
            .and_then(|execution_id| services.resolve_active_execution_bus(execution_id));
        if let Some((root_execution_id, parent_bus)) = resolved_parent.as_ref() {
            child_bus.forward_to(
                parent_bus,
                crate::CowdExecutionLineage {
                    parent_execution_id: root_execution_id.clone(),
                    graph_id: packet.graph_id().to_string(),
                    node_id: packet.node_id().to_string(),
                    team_id: packet.team_id().map(str::to_owned),
                    agent_id: Some(packet.agent_id().to_string()),
                },
            );
        } else if let Some(parent_execution_id) = parent_execution_id.as_deref() {
            tracing::debug!(
                parent_execution_id,
                graph_id = packet.graph_id(),
                agent_id = packet.agent_id(),
                "root Session event bus is no longer active; child evidence remains durable"
            );
        }
        let root_execution_id = resolved_parent
            .as_ref()
            .map(|(root_execution_id, _)| root_execution_id.clone())
            .unwrap_or_else(|| packet.graph_id().to_string());
        let activity_generation = parent_lineage.generation;
        let activity_id = format!(
            "activity:execution:{}:node:{}",
            packet.graph_id(),
            packet.node_id()
        );
        let child_execution_scope = child_bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: packet.run_id().to_string(),
                session_id: packet.session_id().to_string(),
                turn_id: packet
                    .assignment
                    .execution_identity
                    .turn_id()
                    .unwrap_or(packet.run_id())
                    .to_string(),
            },
            Some(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id,
                session_id: packet.session_id().to_string(),
                turn_id: packet
                    .assignment
                    .execution_identity
                    .turn_id()
                    .unwrap_or(packet.run_id())
                    .to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                task_id: packet.task_id().to_string(),
                activity_id: activity_id.clone(),
                node_id: Some(packet.node_id().to_string()),
                parent_activity_id: Some(format!("activity:execution:{}", packet.graph_id())),
                initiator_activity_id: Some(activity_id),
                team_run_id: packet.team_id().map(str::to_owned),
                agent_instance_id: Some(packet.agent_id().to_string()),
                agent_run_id: Some(packet.run_id().to_string()),
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                parallel_group_id: None,
                revision: u64::from(packet.attempt.max(1)),
                fence: packet.expected_graph_revision.max(1),
                generation: activity_generation,
            }),
        );
        let _ = services.agent_runtime().record_progress(
            packet.agent_id(),
            "agent.execution.started",
            "provider-backed child execution admitted",
        );
        // The real worker future, rather than dispatch or a detached Runtime
        // task, owns claim renewal. It waits for this Agent's first explicit
        // task_claim and is aborted automatically on every worker exit path.
        let _claim_heartbeat: Option<crate::agentic::AgenticClaimHeartbeatGuard> =
            crate::agentic::start_agentic_claim_heartbeat(Arc::downgrade(&services), &packet)?;
        let result = runtime
            .submit_turn(
                "Begin the bounded role using the Runtime-attested shared Team context and private role brief above. Return verified findings, evidence, and genuine gaps.",
                &SharedPrompter::none(),
            )
            .await;
        let mut summary = match result {
            Ok(summary) => {
                if let Some(request) = initial_agentic_topic_ack.take() {
                    if let Err(error) = services
                        .agent_action_service()
                        .acknowledge_topic_observations(request)
                    {
                        let _ = services.agent_runtime().record_progress(
                            packet.agent_id(),
                            "agent.agentic_topic_ack_deferred",
                            &format!(
                                "Program topic page will be redelivered after ack failure: {error}"
                            ),
                        );
                    }
                }
                summary
            }
            Err(error) => {
                let error = format!("in-process agent turn failed: {error}");
                settle_failed_agentic_attempt(&services, &packet, &error).await;
                services.fail_live_execution(packet.run_id(), error.clone());
                drop(runtime);
                drop(child_execution_scope);
                drop(active_run_cleanup);
                return Err(error);
            }
        };
        // Agent autonomy is a bounded lifecycle, not a one-shot prompt hint.
        // Before accepting a terminal answer, surface newly committed Team
        // work/inbox facts and give this same bound Agent a chance to close an
        // actionable bid, claim, submission or review. This is never a pure
        // bidding round: it runs only when durable work already needs an
        // action from this identity and remains under the original budget,
        // permission, graph and attempt fences.
        // A Team may need more than one lifecycle action per Agent (proposal,
        // bid, claim, submit, then an independent review).  Keep the loop
        // finite and budget-governed, but do not abandon an actionable work
        // item merely because a provider marks an intermediate response
        // `Partial` or `Open`.
        // Continue while durable work is actionable and the owning execution
        // still has time/budget. There is deliberately no fixed "N rounds"
        // business rule here: a checkpoint is useful only when it observes a
        // new durable state. Repeating the same checkpoint twice is a
        // liveness fuse; a changing state may continue until the packet's
        // existing deadline/budget fence is reached.
        let mut previous_checkpoint_digest: Option<String> = None;
        let mut repeated_checkpoint_count = 0usize;
        loop {
            if packet.deadline_at_ms > 0
                && crate::tool_invocation::now_ms() >= packet.deadline_at_ms
            {
                let _ = services.agent_runtime().record_progress(
                    packet.agent_id(),
                    "agent.autonomy.checkpoint_stopped",
                    "autonomy continuation reached the durable execution deadline",
                );
                break;
            }
            let Some(checkpoint) = agent_autonomy_checkpoint(&services, &packet)? else {
                break;
            };
            let checkpoint_digest = autonomy_checkpoint_progress_digest(&checkpoint.prompt);
            if previous_checkpoint_digest.as_deref() == Some(checkpoint_digest.as_str()) {
                repeated_checkpoint_count = repeated_checkpoint_count.saturating_add(1);
            } else {
                previous_checkpoint_digest = Some(checkpoint_digest);
                repeated_checkpoint_count = 0;
            }
            if repeated_checkpoint_count >= 2 {
                let _ = services.agent_runtime().record_progress(
                    packet.agent_id(),
                    "agent.autonomy.checkpoint_stopped",
                    "identical actionable checkpoint repeated without durable progress",
                );
                break;
            }
            let _ = services.agent_runtime().record_progress(
                packet.agent_id(),
                "agent.autonomy.checkpoint",
                "durable Team work requires a progress-bearing continuation",
            );
            if checkpoint.requires_tool_action {
                runtime.require_next_model_tool_action(checkpoint.tool_ids.clone());
            }
            let continuation = runtime
                .submit_turn(&checkpoint.prompt, &SharedPrompter::none())
                .await;
            match continuation {
                Ok(updated) => {
                    let may_continue =
                        autonomy_continuation_may_advance(updated.terminal_completion);
                    if let Some(request) = checkpoint.agentic_topic_ack {
                        if let Err(error) = services
                            .agent_action_service()
                            .acknowledge_topic_observations(request)
                        {
                            let _ = services.agent_runtime().record_progress(
                                packet.agent_id(),
                                "agent.agentic_topic_ack_deferred",
                                &format!(
                                    "Program topic page will be redelivered after ack failure: {error}"
                                ),
                            );
                        }
                    }
                    summary = updated;
                    if !may_continue && !checkpoint.requires_tool_action {
                        let _ = services.agent_runtime().record_progress(
                            packet.agent_id(),
                            "agent.autonomy.checkpoint_stopped",
                            "bounded collaboration continuation reached a non-satisfied terminal Goal; no duplicate continuation was attempted",
                        );
                        break;
                    }
                }
                Err(error) => {
                    let _ = services.agent_runtime().record_progress(
                        packet.agent_id(),
                        "agent.autonomy.checkpoint_failed",
                        &format!("bounded collaboration continuation failed: {error}"),
                    );
                    // The next bounded round reloads the durable Team state
                    // and may receive a fresh action template (for example
                    // after a concurrent bid advanced the work revision).
                    continue;
                }
            }
        }
        normalize_verified_narrative_terminal(&packet, &tool_executor, &mut summary);
        let scoped_receipts = tool_executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let observed_evidence = model_observed_evidence(
            &packet.required_acceptance,
            &summary.model_observations,
            &scoped_receipts,
        );
        let evidence_refs = agent_evidence_refs(
            &packet,
            &summary.context_turn_report.audit_projections,
            &scoped_receipts,
        );
        let (acceptance, runtime_change_receipts) = derive_receipt_backed_satisfied_criteria(
            &packet,
            &summary,
            &evidence_refs,
            &tool_executor,
            &observed_evidence,
        );
        // `submit_turn` is the delegated child terminal boundary. From this
        // point onward Runtime performs only deterministic presentation
        // normalization and receipt evaluation: it never asks a Provider to
        // rewrite or repair the terminal answer. Receipt-backed technical
        // prose may be carried into a presentation field, while missing
        // risk/unresolved declarations remain missing so the canonical Agent
        // validator/Team reducer can degrade or reject without hidden model
        // work.
        // Dropping the host drops the provider callback sender. The bounded
        // reporter owns no runtime state beyond the lifecycle projection, so
        // it can be joined before the terminal Agent result is committed.
        drop(runtime);
        let _ = progress_reporter.await;
        let mut runtime_write_attempt_paths = summary.write_attempt_paths.clone();
        runtime_write_attempt_paths.extend(
            tool_executor
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|receipt| {
                    receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write
                })
                .flat_map(|receipt| receipt.paths.iter().cloned()),
        );
        runtime_write_attempt_paths.sort();
        runtime_write_attempt_paths.dedup();
        let required_acceptance =
            crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
                &packet.required_acceptance,
                &packet.acceptance,
            );
        let terminal_structured_fields = structured_agent_output(&summary.final_answer)
            .map(|object| object.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        tracing::debug!(
            run_id = packet.run_id(),
            final_answer_bytes = summary.final_answer.len(),
            structured_fields = ?terminal_structured_fields,
            terminal_completion = ?summary.terminal_completion,
            "delegated Agent terminal carrier prepared"
        );
        let receipt_snapshot =
            crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                required_acceptance,
                acceptance.clone(),
                observed_evidence,
            );
        let (observed_acceptance, acceptance_evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(receipt_snapshot);
        let changes = runtime_change_receipts
            .iter()
            .map(|receipt| receipt.path.clone())
            .collect::<Vec<_>>();
        let receipt_summary = tool_executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|receipt| {
                let digest_changed =
                    receipt
                        .paths
                        .iter()
                        .any(|path| match receipt.prior_states.get(path) {
                            Some(harness_contract::context::WorkspacePriorState::Existing {
                                sha256,
                            }) => {
                                receipt
                                    .after_digests
                                    .get(path)
                                    .and_then(|digest| digest.as_deref())
                                    != Some(sha256.as_str())
                            }
                            Some(harness_contract::context::WorkspacePriorState::Absent) => {
                                receipt.after_digests.get(path).is_some_and(Option::is_some)
                            }
                            None => false,
                        });
                format!(
                    "{}:{:?}:{:?}:changed={digest_changed}",
                    receipt.sequence, receipt.effect_kind, receipt.paths
                )
            })
            .collect::<Vec<_>>();
        let contract_criteria = packet_acceptance_contract(&packet)
            .into_iter()
            .map(|requirement| requirement.criterion)
            .collect::<Vec<_>>();
        let pending_evidence_scopes = packet_focus_acceptance_scopes(&packet);
        let _ = services.agent_runtime().record_progress(
            packet.agent_id(),
            "agent.acceptance.evaluated",
            &format!(
                "accepted={acceptance:?}; changes={changes:?}; receipts={receipt_summary:?}; contract={contract_criteria:?}; pending_evidence_scopes={pending_evidence_scopes:?}; observed_acceptance={observed_acceptance:?}"
            ),
        );
        let (status, failure) =
            agent_terminal_outcome(summary.terminal_completion, &summary.final_answer);
        settle_failed_agentic_attempt(
            &services,
            &packet,
            "Agent execution ended without a durable task submission",
        )
        .await;
        let terminal_ref = format!("agent-terminal:{}", packet.run_id());
        match status {
            AgentTerminalStatus::Completed => services.complete_live_execution(
                packet.run_id(),
                &summary.context_turn_report,
                &runtime_write_attempt_paths,
                terminal_ref,
            ),
            AgentTerminalStatus::Blocked => services.block_live_execution(
                packet.run_id(),
                &summary.context_turn_report,
                &runtime_write_attempt_paths,
                terminal_ref,
                failure
                    .clone()
                    .unwrap_or_else(|| "delegated Agent was blocked".to_string()),
            ),
            AgentTerminalStatus::Cancelled => services.cancel_live_execution(
                packet.run_id(),
                failure
                    .clone()
                    .unwrap_or_else(|| "delegated Agent was cancelled".to_string()),
            ),
            AgentTerminalStatus::Failed => services.fail_live_execution(
                packet.run_id(),
                failure
                    .clone()
                    .unwrap_or_else(|| "delegated Agent failed".to_string()),
            ),
        }
        drop(child_execution_scope);
        drop(active_run_cleanup);
        Ok(AgentReturnPacket {
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            session_id: packet.session_id().to_string(),
            mission_id: packet.mission_id().to_string(),
            team_id: packet.team_id().map(str::to_owned),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status,
            outcome: summary.final_answer,
            answer_candidate: None,
            observed_acceptance,
            acceptance_evaluation: Some(acceptance_evaluation),
            acceptance,
            evidence_refs,
            changes,
            runtime_change_receipts,
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: u64::from(summary.usage.input_tokens),
            output_tokens: u64::from(summary.usage.output_tokens),
            cached_tokens: summary
                .model_telemetry
                .cache_create_tokens
                .saturating_add(summary.model_telemetry.cache_read_tokens),
            // Keep the model that actually completed the child turn. The
            // selector value remains the requested lease and may differ after
            // a configured provider fallback.
            model: summary
                .model_telemetry
                .model
                .clone()
                .unwrap_or(selection.model),
            provider: selection.provider,
            tool_calls: summary.tool_results.len() as u64,
            duplicate_tool_calls: summary.duplicate_tool_calls,
            max_tool_concurrency_observed: u64::try_from(summary.max_tool_concurrency_observed)
                .unwrap_or(u64::MAX),
            parallel_tool_batches: u64::try_from(summary.parallel_tool_batches).unwrap_or(u64::MAX),
            runtime_write_attempt_paths,
            runtime_observed_resource_scopes: Vec::new(),
            failure,
        })
    }

    async fn command(
        &self,
        handle: &AgentRunHandle,
        request: &AgentCommandRequest,
    ) -> Result<(), harness_contract::agent::AgentCommandRejectReason> {
        match request.command {
            harness_contract::agent::AgentCommand::Interrupt
            | harness_contract::agent::AgentCommand::Cancel
            | harness_contract::agent::AgentCommand::Shutdown => {
                let active_token = self
                    .active_runs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&handle.run_id)
                    .map(|active| {
                        // Register the durable-in-worker intent while the
                        // active handle is still protected. Cleanup acquires
                        // the locks in the same order, so it cannot remove the
                        // handle and then leave a late tombstone behind.
                        self.pending_cancellations
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(handle.run_id.clone());
                        active.cancellation.clone()
                    });
                if let Some(token) = active_token {
                    // Command acceptance means the cancellation intent reached
                    // the canonical run owner. Provider/tool unwind and map
                    // cleanup are asynchronous lifecycle facts; waiting for
                    // them here made a slow but valid cancel look like an
                    // unsupported backend and broke Session cascade.
                    token.cancel();
                    Ok(())
                } else if self.run_completed(&handle.run_id) {
                    // A terminal command is idempotent across the narrow
                    // cleanup-to-Runtime-commit window.
                    Ok(())
                } else {
                    // A restored Running projection without an active owner
                    // is not a recoverable backend handle. Never acknowledge
                    // a cancellation that no execution can observe.
                    Err(harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend)
                }
            }
            harness_contract::agent::AgentCommand::SendInput => {
                let input = request
                    .input
                    .as_ref()
                    .ok_or(harness_contract::agent::AgentCommandRejectReason::InvalidInput)?;
                let active = self
                    .active_runs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&handle.run_id)
                    .cloned()
                    .ok_or(
                        harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend,
                    )?;
                let envelope = SessionInputEnvelope::text(
                    active.session_id,
                    InputSourceKind::Agent,
                    agent_input_text(input),
                )
                .with_source_ref(format!("agent:{}", handle.agent_id))
                .with_source_message_id(request.command_id.clone());
                active
                    .input_stream
                    .admit(envelope, active.input_stream.runtime_state());
                Ok(())
            }
            harness_contract::agent::AgentCommand::Pause
            | harness_contract::agent::AgentCommand::Resume => {
                Err(harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend)
            }
        }
    }
}
