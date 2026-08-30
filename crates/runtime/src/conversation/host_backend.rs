//! Graph-owned provider and governed tool execution backends.

use super::*;

#[async_trait]
impl<C, T> ScopedNodeBackend for TurnModelStepBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        if self.state.lock().await.terminal_override.is_some() {
            // A verified Host-admitted Team already produced the canonical
            // terminal synthesis. Do not spend another provider/tool loop
            // restating or re-executing that checked result in the parent.
            let mut synthesize = dynamic_node(
                ticket,
                0,
                "precommitted-terminal-synthesize",
                ExecutionNodeKind::Synthesize,
                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                "inline_model",
            );
            synthesize.executor_kind =
                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
            return Ok(NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:precommitted-terminal", ticket.graph_id)),
                ExecutionUsage::default(),
            ))
            .with_replan(ExecutionGraphReplan {
                nodes: vec![synthesize.clone()],
                edges: dynamic_edges(&ticket.node_id, &[synthesize]),
                reason: "verified Team terminal result bypassed duplicate parent model execution"
                    .to_string(),
            }));
        }
        let prefetched_review_calls = {
            let mut state = self.state.lock().await;
            let reviewer_prefetch = should_prefetch_focus_verification(
                state.first_model_step,
                state.bounded_evidence_role,
                state.focus_verification_prefetched,
                &state.focus_acceptance_pending_scopes,
            );
            let calls = reviewer_prefetch
                .then(|| {
                    focus_verification_tool_calls(
                        &state.focus_acceptance_pending_scopes,
                        state.iterations,
                        self.services.workspace_root(),
                    )
                })
                .flatten();
            if calls.is_some() {
                state.focus_verification_prefetched = true;
            }
            calls.map(|calls| (state.session_id.clone(), state.iterations, calls))
        };
        if let Some((session_id, iteration, calls)) = prefetched_review_calls {
            let nodes = tool_nodes_for_calls(
                ticket,
                iteration,
                &session_id,
                calls,
                self.services.workspace_root(),
            )?;
            return Ok(NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:runtime-review-prefetch", ticket.graph_id)),
                ExecutionUsage::default(),
            ))
            .with_replan(ExecutionGraphReplan {
                edges: dynamic_edges(&ticket.node_id, &nodes),
                nodes,
                reason: "Runtime prefetched exact immutable upstream-change evidence before reviewer synthesis"
                    .to_string(),
            }));
        }
        let (
            content,
            first_step,
            fuse_intervention,
            force_text_only_response,
            force_tool_allowlist,
            force_reasoning_effort,
            clean_terminal_synthesis,
            clean_terminal_evidence,
            provider_retry_fenced,
            pending_next_model_context,
        ) = {
            let mut state = self.state.lock().await;
            let first = state.first_model_step;
            state.first_model_step = false;
            let mut clean_terminal_synthesis =
                std::mem::take(&mut state.clean_terminal_synthesis_next);
            if !clean_terminal_synthesis
                && !state.clean_terminal_synthesis_attempted
                && state.successful_tool_calls > 0
                && state.iterations.saturating_add(2) >= state.safety_lease.max_model_steps
            {
                state.clean_terminal_synthesis_attempted = true;
                clean_terminal_synthesis = true;
            }
            let clean_terminal_evidence = clean_terminal_synthesis.then(|| {
                let mut evidence = terminal_evidence_digest(&state.tool_results);
                let early_receipt_messages = state
                    .early_tool_receipts
                    .values()
                    .map(early_tool_receipt_message)
                    .collect::<Vec<_>>();
                let early_evidence = terminal_evidence_digest(&early_receipt_messages);
                if !early_evidence.is_empty() {
                    if !evidence.is_empty() {
                        evidence.push_str("\n\n");
                    }
                    evidence.push_str(&early_evidence);
                }
                evidence
            });
            let made_progress = std::mem::take(&mut state.last_verified_progress);
            let intervention = if clean_terminal_synthesis {
                None
            } else {
                match crate::execution_core::SafetyFusePolicy::evaluate(
                    &state.safety_lease,
                    state.iterations,
                    made_progress,
                ) {
                    crate::execution_core::SafetyFuseDecision::Continue => None,
                    crate::execution_core::SafetyFuseDecision::Block { reason } => {
                        state.terminal_override = Some((
                            GoalCompletion::Partial,
                            format!(
                                "Execution blocked safely: {reason}\n\nChecked evidence and progress were preserved. Continue with a new constraint, additional evidence, or an explicit replan."
                            ),
                        ));
                        let mut observation = runtime_observation(
                            runtime_observation_identity(&self.services, &state, ticket),
                            RuntimeObservationKind::StrategyHistory,
                            "runtime.safety_fuse",
                            u64::try_from(state.iterations).unwrap_or(u64::MAX),
                            reason.clone(),
                            format!(
                                "safety-fuse:{}:{}",
                                ticket.node_id, state.safety_lease.max_model_steps
                            ),
                            ObservationResultClass::Failed,
                        );
                        observation.failure_class = Some(ObservationFailureClass::Policy);
                        let intervention = RuntimeIntervention {
                            goal_id: state.goal_id.clone(),
                            kind: RuntimeInterventionKind::Block,
                            reason,
                            evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                            expected_graph_revision: None,
                        };
                        Some((intervention, observation))
                    }
                }
            };
            let pending_next_model_context = model_context_for_step(
                std::mem::take(&mut state.pending_next_model_context),
                &state.persistent_collaboration_context,
            );
            (
                state.content.clone(),
                first,
                intervention,
                // The override applies to exactly one Provider request. A
                // recovery may schedule another explicit override below, but
                // stale state must never disable tools for later turns.
                std::mem::take(&mut state.force_text_only_next_model),
                state.force_tool_allowlist_next_model.take(),
                state.force_reasoning_effort_next_model.take(),
                clean_terminal_synthesis,
                clean_terminal_evidence,
                state.tool_receipts_observed > 0,
                pending_next_model_context,
            )
        };
        if let Some((intervention, observation)) = fuse_intervention {
            let mut synthesize = dynamic_node(
                ticket,
                0,
                "safety-block-synthesize",
                ExecutionNodeKind::Synthesize,
                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                "inline_model",
            );
            synthesize.executor_kind =
                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
            let mut outcome = NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:safety-fuse", ticket.graph_id)),
                ExecutionUsage::default(),
            ))
            .with_replan(ExecutionGraphReplan {
                nodes: vec![synthesize.clone()],
                edges: dynamic_edges(&ticket.node_id, &[synthesize]),
                reason: "safety fuse requested an honest blocked synthesis".to_string(),
            });
            outcome.domain_events.push(
                self.services
                    .goal_store()
                    .observation_event(
                        &observation,
                        format!("{}:safety-observation", ticket.idempotency_key),
                    )
                    .map_err(|reason| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason,
                    })?,
            );
            outcome.domain_events.push(
                self.services
                    .goal_store()
                    .intervention_event(
                        &intervention,
                        std::slice::from_ref(&observation),
                        format!("{}:safety-intervention", ticket.idempotency_key),
                    )
                    .map_err(|reason| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason,
                    })?,
            );
            return Ok(outcome);
        }
        let (early_session_id, early_model_lease, observation_wave_sequence) = {
            let state = self.state.lock().await;
            (
                state.session_id.clone(),
                state.model.clone(),
                u64::try_from(state.iterations.saturating_add(1)).unwrap_or(u64::MAX),
            )
        };
        let mut runtime = self.runtime.lock().await;
        let required_control_plane = {
            let state = self.state.lock().await;
            if state.execution_role.is_delegated_leaf()
                || state.collaboration_started
                || has_completed_program_terminal(&state.tool_results)
                || has_admitted_program_receipt(&state.tool_results)
            {
                None
            } else {
                state.task_understanding.as_ref().and_then(|value| {
                    (value.required_team_count > 0).then_some((
                        value.required_team_count,
                        state.root_control_plane_phase,
                        value.required_workspace_evidence_scopes.clone(),
                    ))
                })
            }
        };
        if let Some((required_team_count, local_phase, required_workspace_evidence_scopes)) =
            required_control_plane
        {
            let (session_id, turn_id) = {
                let state = self.state.lock().await;
                (state.session_id.clone(), state.turn_id.clone())
            };
            let phase = recovered_root_control_plane_phase(&self.services, &session_id, &turn_id)
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!(
                        "recover root collaboration control-plane phase before provider exposure: {error}"
                    ),
                })?
                .unwrap_or(local_phase);
            {
                let mut state = self.state.lock().await;
                state.root_control_plane_phase = phase;
                state.pending_root_control_plane_requirement = Some(required_team_count);
            }
            runtime
                .require_active_turn_collaboration_control_plane(required_team_count)
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!(
                        "pin required root collaboration strategy before control-plane exposure: {error}"
                    ),
                })?;
            match phase {
                RootControlPlanePhase::CapabilityOrProposal
                | RootControlPlanePhase::ProposalOnly => {
                    runtime.require_next_model_named_tool_action(
                        harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
                    );
                    runtime.require_next_model_reasoning_effort("none");
                    // The provider wire constraint is necessary but not
                    // sufficient: compatibility endpoints can return prose
                    // when the surrounding normal-conversation context is
                    // much larger than the control contract.  Supply one
                    // latest, Runtime-owned micro-instruction that names only
                    // the narrow semantic codec. It contains the user-bound
                    // cardinality but no Runtime-owned roles, templates, or
                    // graph ids, so model autonomy remains semantic rather
                    // than protocol-fragile.
                    let mut item = ContextItem::new(
                        format!(
                            "runtime-root-collaboration-decision:{}:{}",
                            session_id, turn_id
                        ),
                        ContextSourceKind::Task,
                        ContextRole::Instruction,
                        root_collaboration_decision_instruction(
                            required_team_count,
                            &required_workspace_evidence_scopes,
                            match runtime.permission_policy().active_mode() {
                                crate::PermissionMode::ReadOnly => {
                                    harness_contract::policy::PermissionMode::ReadOnly
                                }
                                crate::PermissionMode::WorkspaceWrite => {
                                    harness_contract::policy::PermissionMode::WorkspaceWrite
                                }
                                crate::PermissionMode::DangerFullAccess => {
                                    harness_contract::policy::PermissionMode::DangerFullAccess
                                }
                            },
                        ),
                    );
                    item.authority = ContextAuthority::System;
                    item.visibility = ContextVisibility::Private;
                    item.evidence = vec![format!("turn:{turn_id}")];
                    runtime.push_next_model_context_item(item);
                }
                RootControlPlanePhase::ProposalSubmitted => {}
            }
        }
        for item in pending_next_model_context {
            runtime.push_next_model_context_item(item);
        }
        if let Some(bus) = runtime.cowd_bus().cloned() {
            bus.emit(CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::CallingModel,
                detail: Some("requesting model".to_string()),
            });
        }
        if !clean_terminal_synthesis {
            if force_text_only_response {
                runtime.require_next_model_final_response();
            } else if let Some(tool_ids) = force_tool_allowlist {
                runtime.require_next_model_tool_action(tool_ids);
            }
            if let Some(effort) = force_reasoning_effort {
                runtime.require_next_model_reasoning_effort(effort);
            }
        }
        let transcript_len = runtime.session_head().await.message_count;
        let disposition_model_lease = Some(runtime.active_model_lease());
        let disposition_permission_ceiling = match runtime.permission_policy().active_mode() {
            crate::PermissionMode::ReadOnly => harness_contract::policy::PermissionMode::ReadOnly,
            crate::PermissionMode::WorkspaceWrite => {
                harness_contract::policy::PermissionMode::WorkspaceWrite
            }
            crate::PermissionMode::DangerFullAccess => {
                harness_contract::policy::PermissionMode::DangerFullAccess
            }
        };
        let disposition_capabilities = runtime
            .tool_executor()
            .tool_discovery_receipt()
            .activation_candidates
            .into_iter()
            .map(|tool_id| format!("tool:{tool_id}"))
            .collect::<Vec<_>>();
        let early_dispatcher: Option<Arc<dyn crate::conversation::EarlyToolDispatcher>> =
            if clean_terminal_synthesis || force_text_only_response {
                None
            } else {
                runtime.active_turn_strategy().and_then(|strategy| {
                    self.services.tool_execution_host().map(|_| {
                        Arc::new(HostEarlyToolDispatcher {
                            tool_executor: Arc::clone(runtime.tool_executor()),
                            services: Arc::clone(&self.services),
                            event_bus: runtime.cowd_bus().cloned(),
                            ticket: ticket.clone(),
                            session_id: early_session_id.clone(),
                            memory_context: runtime.memory_turn_context(),
                            model_lease: early_model_lease.clone(),
                            observation_wave_sequence,
                            decision: strategy.decision,
                            permission_policy: runtime.permission_policy().clone(),
                            authorization_negotiator: runtime.authorization_negotiator(),
                            timeout: runtime
                                .tool_timeout()
                                .unwrap_or_else(|| std::time::Duration::from_secs(60)),
                            early_read_locks: Arc::new(tokio::sync::Mutex::new(
                                std::collections::HashMap::new(),
                            )),
                        })
                            as Arc<dyn crate::conversation::EarlyToolDispatcher>
                    })
                })
            };
        let result = if clean_terminal_synthesis {
            runtime
                .execute_clean_terminal_synthesis(
                    &content,
                    clean_terminal_evidence.as_deref().unwrap_or_default(),
                )
                .await
        } else {
            runtime
                .execute_model_step_with_early_dispatch(
                    &content,
                    first_step,
                    early_dispatcher,
                    provider_retry_fenced,
                )
                .await
        };
        runtime
            .session_mut_async()
            .await
            .truncate_messages(transcript_len);
        let consumed_inputs = runtime.take_consumed_session_inputs();
        let cowd_bus = runtime.cowd_bus().cloned();
        drop(runtime);
        match result {
            Ok(step) => {
                let committed_graph = self
                    .services
                    .graph_state_store()
                    .load_async(ticket.graph_id.clone())
                    .await
                    .map_err(|error| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: format!("load committed predecessor results: {error}"),
                    })?;
                let step_output_chars = step
                    .assistant_message
                    .blocks
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::Text { text } => Some(text.chars().count() as u64),
                        _ => None,
                    })
                    .sum::<u64>();
                let usage = ExecutionUsage {
                    model: step.model.clone(),
                    input_tokens: u64::from(step.usage.input_tokens),
                    output_tokens: u64::from(step.usage.output_tokens),
                    cached_tokens: u64::from(step.usage.cache_read_input_tokens)
                        .saturating_add(u64::from(step.usage.cache_creation_input_tokens)),
                    duration_ms: step.wall_duration_ms,
                    tool_calls: 0,
                    ..ExecutionUsage::default()
                };
                if !step.early_tool_deferrals.is_empty() {
                    crate::execution_core::performance::observe_count(
                        "early_tool_deferred_count",
                        step.early_tool_deferrals.len() as u64,
                    );
                    for deferral in &step.early_tool_deferrals {
                        if let Err(error) =
                            self.services
                                .event_store()
                                .append(crate::RuntimeEventInput {
                                    stream_id: format!("execution-node:{}", ticket.node_id),
                                    scope: crate::RuntimeEventScope::ExecutionNode,
                                    kind: "execution.tool_early_deferred".to_string(),
                                    status: Some("deferred".to_string()),
                                    actor: Some(
                                        "conversation_runtime.model_step_tool_plan".to_string(),
                                    ),
                                    refs: vec![
                                        crate::RuntimeEventRef {
                                            kind: "execution_graph".to_string(),
                                            id: ticket.graph_id.clone(),
                                        },
                                        crate::RuntimeEventRef {
                                            kind: "execution_node".to_string(),
                                            id: ticket.node_id.clone(),
                                        },
                                        crate::RuntimeEventRef {
                                            kind: "tool_call".to_string(),
                                            id: deferral.tool_call_id.clone(),
                                        },
                                    ],
                                    payload: serde_json::json!({
                                        "tool_call_id": deferral.tool_call_id,
                                        "reason": deferral.reason,
                                        "ready_at_ms": deferral.ready_at_ms,
                                    }),
                                })
                        {
                            tracing::warn!(
                                %error,
                                tool_call_id = %deferral.tool_call_id,
                                "failed to persist early tool deferral evidence"
                            );
                        }
                    }
                }
                let seen_disposition_inputs = consumed_inputs
                    .iter()
                    .filter(|record| {
                        record.checkpoint != Some(TurnInputCheckpoint::AfterProviderResponse)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let late_disposition_inputs = consumed_inputs
                    .iter()
                    .filter(|record| {
                        record.checkpoint == Some(TurnInputCheckpoint::AfterProviderResponse)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let (
                    pending_disposition_inputs,
                    disposition_goal_id,
                    disposition_session_id,
                    disposition_repair_count,
                ) =
                    {
                        let mut state = self.state.lock().await;
                        for record in seen_disposition_inputs {
                            if !state.pending_disposition_inputs.iter().any(|existing| {
                                existing.envelope.input_id == record.envelope.input_id
                            }) {
                                state.pending_disposition_inputs.push(record);
                            }
                        }
                        (
                            state.pending_disposition_inputs.clone(),
                            state.goal_id.clone(),
                            state.session_id.clone(),
                            state.input_disposition_repairs,
                        )
                    };
                let route_resolution = if pending_disposition_inputs.is_empty() {
                    RouteInputResolution::NotRequired
                } else {
                    parse_route_input_intent(&step.intent, pending_disposition_inputs.len())
                };
                let applied_disposition = match &route_resolution {
                    RouteInputResolution::Valid(parsed) => {
                        let lineage = committed_graph.lineage.clone().ok_or_else(|| {
                            NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason: "active graph has no canonical Task lineage for input disposition"
                                    .to_string(),
                            }
                        })?;
                        let mission_id = self
                            .services
                            .task_aggregate_service()
                            .get(&lineage.root_task_id)
                            .map_err(|reason| NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason,
                            })?
                            .map(|task| task.mission_id)
                            .unwrap_or_else(|| {
                                self.services
                                    .mission_runtime()
                                    .default_mission_id()
                                    .to_string()
                            });
                        let binding = crate::orchestration::input_disposition::InputDispositionRuntimeBinding {
                            session_id: disposition_session_id,
                            turn_id: lineage.turn_id.clone(),
                            execution_id: ticket.graph_id.clone(),
                            execution_node_id: ticket.node_id.clone(),
                            execution_revision: committed_graph.revision,
                            lineage,
                            mission_id,
                            goal_id: disposition_goal_id,
                            model_lease: disposition_model_lease.clone(),
                            permission_ceiling: disposition_permission_ceiling,
                            capabilities: disposition_capabilities.clone(),
                            constraints: parsed.constraints.clone(),
                        };
                        let slot_input_ids = pending_disposition_inputs
                            .iter()
                            .map(|record| record.envelope.input_id.as_str().to_string())
                            .collect::<Vec<_>>();
                        match crate::orchestration::input_disposition::apply_input_disposition_batch(
                            &self.services,
                            &binding,
                            &slot_input_ids,
                            &parsed.batch,
                        )
                        .await
                        {
                            Ok(applied) => Some(Ok(applied)),
                            Err(error) => Some(Err(error)),
                        }
                    }
                    RouteInputResolution::Invalid(error) => Some(Err(error.clone())),
                    RouteInputResolution::NotRequired => None,
                };
                let failed_disposition_receipts = if applied_disposition
                    .as_ref()
                    .is_some_and(|result| result.is_err())
                {
                    let mut receipts = Vec::new();
                    if let Some(query) = self.services.session_query_port() {
                        for record in &pending_disposition_inputs {
                            if let Ok(Some(durable)) = query
                                .runtime_input_by_input_id(record.envelope.input_id.as_str())
                                .await
                            {
                                if let Some(receipt) = durable.application_receipt {
                                    if !receipts.iter().any(|existing: &harness_contract::input_disposition::SessionInputApplicationReceipt| {
                                        existing.disposition_id == receipt.disposition_id
                                    }) {
                                        receipts.push(receipt);
                                    }
                                }
                            }
                        }
                    }
                    receipts
                } else {
                    Vec::new()
                };
                let mut state = self.state.lock().await;
                for receipt in &step.early_tool_receipts {
                    if receipt.started_at_ms < step.response_completed_at_ms {
                        crate::execution_core::performance::observe_count(
                            "early_tool_overlap_count",
                            1,
                        );
                        crate::execution_core::performance::observe_duration(
                            "early_tool_model_overlap_ms",
                            std::time::Duration::from_millis(
                                receipt
                                    .completed_at_ms
                                    .min(step.response_completed_at_ms)
                                    .saturating_sub(receipt.started_at_ms),
                            ),
                        );
                    }
                    state
                        .early_tool_receipts
                        .insert(receipt.call.id.clone(), receipt.clone());
                }
                state.iterations = state.iterations.saturating_add(1);
                state.input_tokens = state
                    .input_tokens
                    .saturating_add(u64::from(step.usage.input_tokens));
                state.output_tokens = state
                    .output_tokens
                    .saturating_add(u64::from(step.usage.output_tokens));
                state.cache_create_tokens = state
                    .cache_create_tokens
                    .saturating_add(u64::from(step.usage.cache_creation_input_tokens));
                state.cache_read_tokens = state
                    .cache_read_tokens
                    .saturating_add(u64::from(step.usage.cache_read_input_tokens));
                state.output_chars = state.output_chars.saturating_add(step_output_chars);
                state.output_chunks = state.output_chunks.saturating_add(1);
                state.wall_duration_ms =
                    state.wall_duration_ms.saturating_add(step.wall_duration_ms);
                state.model = step.model.clone();
                for model in &step.models_used {
                    if !state.models_used.contains(model) {
                        state.models_used.push(model.clone());
                    }
                }
                if state.first_token_latency_ms.is_none() {
                    state.first_token_latency_ms = step.first_token_latency_ms;
                }
                state.active_stream_duration_ms = state
                    .active_stream_duration_ms
                    .saturating_add(step.active_stream_duration_ms.unwrap_or_default());
                if let Some(bus) = cowd_bus.as_ref() {
                    let rate = |value: u64, duration_ms: u64| {
                        (duration_ms > 0).then(|| value as f64 * 1_000.0 / duration_ms as f64)
                    };
                    bus.emit(CowdEvent::RunModelTelemetry {
                        telemetry: crate::cowd_event::RunModelTelemetry {
                            model: state.model.clone(),
                            models_used: state.models_used.clone(),
                            first_token_latency_ms: state.first_token_latency_ms,
                            active_stream_duration_ms: Some(state.active_stream_duration_ms.max(1)),
                            wall_duration_ms: state.wall_duration_ms.max(1),
                            output_chars: state.output_chars,
                            output_chunks: state.output_chunks,
                            input_tokens: state.input_tokens,
                            output_tokens: state.output_tokens,
                            cache_create_tokens: state.cache_create_tokens,
                            cache_read_tokens: state.cache_read_tokens,
                            total_tokens: state.input_tokens.saturating_add(state.output_tokens),
                            usage_source: "provider".to_string(),
                            wall_chars_per_second: rate(state.output_chars, state.wall_duration_ms),
                            wall_tokens_per_second: rate(
                                state.output_tokens,
                                state.wall_duration_ms,
                            ),
                            active_chars_per_second: rate(
                                state.output_chars,
                                state.active_stream_duration_ms,
                            ),
                            active_tokens_per_second: rate(
                                state.output_tokens,
                                state.active_stream_duration_ms,
                            ),
                            chars_per_second: rate(
                                state.output_chars,
                                state.active_stream_duration_ms,
                            ),
                            tokens_per_second: rate(
                                state.output_tokens,
                                state.active_stream_duration_ms,
                            ),
                        },
                    });
                }
                state
                    .assistant_messages
                    .push(step.assistant_message.clone());
                let mut committed_messages = Vec::new();
                if first_step {
                    committed_messages.push(ConversationMessage::user_text(content));
                }
                committed_messages.push(step.assistant_message.clone());
                state
                    .pending_transcript
                    .insert(ticket.node_id.clone(), committed_messages);
                let goal_id = state.goal_id.clone();
                let late_inputs = consumed_inputs.iter().any(|record| {
                    record.checkpoint == Some(TurnInputCheckpoint::AfterProviderResponse)
                });
                if late_inputs {
                    // The provider result was produced against an older input
                    // cursor. Keep its usage and observations, but never
                    // publish the stale assistant candidate as transcript.
                    state.assistant_messages.pop();
                    state.pending_transcript.remove(&ticket.node_id);
                }
                let applied_receipts = applied_disposition
                    .as_ref()
                    .and_then(|result| result.as_ref().ok())
                    .map(|applied| applied.receipts.clone())
                    .unwrap_or_default();
                if applied_disposition
                    .as_ref()
                    .is_some_and(|result| result.is_ok())
                {
                    if let (RouteInputResolution::Valid(parsed), Some(Ok(applied))) =
                        (&route_resolution, applied_disposition.as_ref())
                    {
                        if applied.structural || parsed.remaining_calls.is_empty() {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                        } else {
                            let TurnGraphState {
                                assistant_messages,
                                pending_transcript,
                                ..
                            } = &mut *state;
                            remove_tool_call_from_latest_assistant(
                                assistant_messages,
                                pending_transcript,
                                &ticket.node_id,
                                &parsed.route_call_id,
                            );
                        }
                    }
                    state.pending_disposition_inputs.clear();
                    state.input_disposition_repairs = 0;
                    for receipt in &applied_receipts {
                        if matches!(
                            receipt.action,
                            harness_contract::input_disposition::InputDispositionAction::AmendCurrentTurn
                                | harness_contract::input_disposition::InputDispositionAction::ReplanCurrentGraph
                        ) {
                            state.content.push_str("\n\nApplied running-Turn input:\n");
                            state.content.push_str(&receipt.objective);
                            let supplemental = harness_contract::strategy::understand(
                                &harness_contract::strategy::StrategyInput::from_prompt(
                                    &receipt.objective,
                                ),
                            );
                            if let Some(authority) = state.task_understanding.as_mut() {
                                authority.required_team_count = authority
                                    .required_team_count
                                    .max(supplemental.required_team_count);
                                authority.requires_write |= supplemental.requires_write;
                                authority.requires_external_facts |=
                                    supplemental.requires_external_facts;
                            } else {
                                state.task_understanding = Some(supplemental);
                            }
                        }
                    }
                    state.pending_next_model_context.extend(
                        crate::turn_inbox::checkpoint_context_items(
                            TurnInputCheckpoint::BeforeProviderRequest,
                            &pending_disposition_inputs,
                        ),
                    );
                    if let Some(applied) = applied_disposition
                        .as_ref()
                        .and_then(|result| result.as_ref().ok())
                    {
                        let receipt_details = applied
                            .receipts
                            .iter()
                            .map(|receipt| {
                                format!(
                                    "- action={:?}; objective={}; task_ids=[{}]; team_ids=[{}]; execution_ids=[{}]; target_session={}",
                                    receipt.action,
                                    receipt.objective,
                                    receipt.task_ids.join(","),
                                    receipt.team_ids.join(","),
                                    receipt.execution_ids.join(","),
                                    receipt.target_session_id.as_deref().unwrap_or("none"),
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n");
                        let mut item = ContextItem::new(
                            format!("input-disposition-applied:{}", ticket.node_id),
                            ContextSourceKind::Task,
                            ContextRole::Evidence,
                            format!(
                                "Runtime applied the running-Turn input disposition(s): {}\n{}",
                                applied.summaries.join("; "),
                                receipt_details,
                            ),
                        );
                        item.authority = ContextAuthority::Tool;
                        item.visibility = ContextVisibility::Private;
                        item.evidence = applied
                            .receipts
                            .iter()
                            .map(|receipt| format!("input_disposition:{}", receipt.disposition_id))
                            .collect();
                        state.pending_next_model_context.push(item);
                        if let Some(replacement) = applied.receipts.iter().find(|receipt| {
                            receipt.action
                                == harness_contract::input_disposition::InputDispositionAction::ReplaceCurrentTask
                        }) {
                            state.terminal_override = Some((
                                GoalCompletion::Cancelled,
                                format!(
                                    "The current Task was cancelled and the new request was queued as its successor: {}",
                                    replacement.objective
                                ),
                            ));
                        }
                        if let Some(bus) = cowd_bus.as_ref() {
                            for receipt in &applied.receipts {
                                bus.emit(CowdEvent::SessionInputDispositionChanged {
                                    receipt: receipt.clone(),
                                });
                            }
                        }
                    }
                }
                for record in late_disposition_inputs {
                    if !state
                        .pending_disposition_inputs
                        .iter()
                        .any(|existing| existing.envelope.input_id == record.envelope.input_id)
                    {
                        state.pending_disposition_inputs.push(record);
                    }
                }
                let disposition_failure = applied_disposition
                    .as_ref()
                    .and_then(|result| result.as_ref().err())
                    .cloned();
                if let Some(error) = disposition_failure.as_deref() {
                    state.assistant_messages.pop();
                    state.pending_transcript.remove(&ticket.node_id);
                    if disposition_repair_count == 0 {
                        state.input_disposition_repairs = 1;
                        let guidance = format!(
                            "Runtime input disposition repair (one attempt): {error}. Call runtime_orchestrate(operation=route_input) once and cover every input_slot exactly once. Do not execute ordinary tools until the disposition is valid and applied."
                        );
                        let mut item = ContextItem::new(
                            format!("input-disposition-repair:{}", ticket.node_id),
                            ContextSourceKind::Task,
                            ContextRole::Instruction,
                            guidance,
                        );
                        item.authority = ContextAuthority::System;
                        item.visibility = ContextVisibility::Private;
                        state.pending_next_model_context.push(item);
                        let repair_inputs = state.pending_disposition_inputs.clone();
                        state.pending_next_model_context.extend(
                            crate::turn_inbox::checkpoint_context_items(
                                TurnInputCheckpoint::BeforeProviderRequest,
                                &repair_inputs,
                            ),
                        );
                    } else {
                        state.terminal_override = Some((
                            GoalCompletion::Partial,
                            format!(
                                "running-Turn input disposition remained invalid after one contract repair: {error}"
                            ),
                        ));
                    }
                }
                if let Some(bus) = cowd_bus.as_ref() {
                    for receipt in &failed_disposition_receipts {
                        bus.emit(CowdEvent::SessionInputDispositionChanged {
                            receipt: receipt.clone(),
                        });
                    }
                }
                let observation_identity =
                    runtime_observation_identity(&self.services, &state, ticket);
                let observation_revision = state.iterations as u64;
                let mut upstream_observations =
                    predecessor_goal_observations(&committed_graph, ticket, &observation_identity);
                let applied_observation_keys = self
                    .services
                    .goal_store()
                    .projection(&goal_id)
                    .map_err(|reason| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason,
                    })?
                    .map(|projection| projection.progress.applied_observation_keys)
                    .unwrap_or_default();
                upstream_observations.retain(|observation| {
                    !applied_observation_keys
                        .iter()
                        .any(|key| key == &observation.idempotency_fingerprint())
                });
                let input_observation = (!consumed_inputs.is_empty()).then(|| {
                    let evidence_refs = consumed_inputs
                        .iter()
                        .map(|record| format!("session_input:{}", record.envelope.input_id))
                        .collect::<Vec<_>>();
                    let mut observation = runtime_observation(
                        observation_identity.clone(),
                        RuntimeObservationKind::UserInput,
                        "runtime.session_input_checkpoint",
                        observation_revision,
                        format!(
                            "consumed {} session input update(s); applied_disposition_count={}",
                            consumed_inputs.len(),
                            applied_receipts.len(),
                        ),
                        format!("session-input:{}", sha256_digest(&evidence_refs.join("\n"))),
                        ObservationResultClass::Informational,
                    );
                    observation.evidence_refs.clone_from(&evidence_refs);
                    if applied_receipts.iter().any(|receipt| {
                        receipt.action
                            == harness_contract::input_disposition::InputDispositionAction::ReplanCurrentGraph
                    }) {
                        observation.information_gain = InformationGain {
                            distinguishing_evidence_refs: evidence_refs,
                            resolved_unknown_refs: Vec::new(),
                            provenance: MeasureProvenance::Observed,
                        };
                        observation.unknown_deltas.push(UnknownDelta {
                            unknown_id: format!("replan-after-user-input:{}", ticket.node_id),
                            change: ResolutionDeltaKind::Opened,
                            evidence_refs: observation.evidence_refs.clone(),
                        });
                    }
                    observation
                });
                let mut intent = if late_inputs {
                    ModelStepIntent::Replan {
                        reason:
                            "new Session input arrived after the Provider response; continue from the newer durable input cursor"
                                .to_string(),
                    }
                } else {
                    match (&route_resolution, applied_disposition.as_ref()) {
                        (RouteInputResolution::Valid(parsed), Some(Ok(applied)))
                            if !applied.requires_fresh_model_step
                                && !parsed.remaining_calls.is_empty() =>
                        {
                            ModelStepIntent::ToolCalls {
                                calls: parsed.remaining_calls.clone(),
                            }
                        }
                        (RouteInputResolution::Valid(_), Some(Ok(_))) => ModelStepIntent::Replan {
                            reason: "continue from the applied running-Turn input disposition"
                                .to_string(),
                        },
                        (_, Some(Err(error))) if disposition_repair_count > 0 => {
                            ModelStepIntent::FinalAnswer {
                                text: format!("Input disposition is blocked: {error}"),
                            }
                        }
                        (_, Some(Err(error))) => ModelStepIntent::Replan {
                            reason: format!("repair the running-Turn input disposition: {error}"),
                        },
                        _ => step.intent.clone(),
                    }
                };
                if force_text_only_response || step.text_only_response {
                    intent = match intent {
                        ModelStepIntent::ToolCalls { .. } => {
                            let final_text = step
                                .assistant_message
                                .blocks
                                .iter()
                                .filter_map(|block| match block {
                                    ContentBlock::Text { text } => Some(text.as_str()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join("\n")
                                .trim()
                                .to_string();
                            // A provider can hallucinate a native call even
                            // when this request exposed zero schemas. Treat
                            // that as an unusable terminal answer so the
                            // existing governed final-answer recovery gets one
                            // evidence-only retry. Do not execute the call,
                            // and do not fail the whole graph before recovery.
                            ModelStepIntent::FinalAnswer { text: final_text }
                        }
                        ModelStepIntent::Replan { .. } if applied_receipts.is_empty() => {
                            // ConversationRuntime turns a tool call outside the
                            // current exposure lease into Replan. During a
                            // text-only checkpoint that replan must not restore
                            // tool schemas on the following request; consume
                            // any visible text as a terminal candidate and let
                            // the bounded terminal recovery own the retry.
                            let final_text = step
                                .assistant_message
                                .blocks
                                .iter()
                                .filter_map(|block| match block {
                                    ContentBlock::Text { text } => Some(text.as_str()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join("\n")
                                .trim()
                                .to_string();
                            ModelStepIntent::FinalAnswer { text: final_text }
                        }
                        other => other,
                    };
                }
                let mut observation = runtime_observation(
                    observation_identity.clone(),
                    RuntimeObservationKind::GraphProgress,
                    "runtime.model_step",
                    observation_revision,
                    model_intent_summary(&intent),
                    format!(
                        "model-intent:{}:{}",
                        model_intent_kind(&intent),
                        state.iterations
                    ),
                    ObservationResultClass::Informational,
                );
                observation.evidence_refs = vec![format!("execution_node:{}", ticket.node_id)];
                observation.parallelism_delta.ready_work =
                    u16::try_from(independent_tool_call_count(&intent)).unwrap_or(u16::MAX);
                if applied_receipts.iter().any(|receipt| {
                    receipt.action
                        == harness_contract::input_disposition::InputDispositionAction::ReplanCurrentGraph
                }) {
                    observation.unknown_deltas.push(UnknownDelta {
                        unknown_id: format!("replan-after-user-input:{}", ticket.node_id),
                        change: ResolutionDeltaKind::Resolved,
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                    });
                }
                let mut provider_observation = runtime_observation(
                    observation_identity.clone(),
                    RuntimeObservationKind::ProviderProgress,
                    "runtime.provider_stream",
                    observation_revision,
                    format!(
                        "provider completed model step input_tokens={} output_tokens={} duration_ms={}",
                        usage.input_tokens, usage.output_tokens, usage.duration_ms
                    ),
                    format!(
                        "provider-step:{}:{}",
                        state.model.as_deref().unwrap_or("configured-primary"),
                        model_intent_kind(&intent)
                    ),
                    ObservationResultClass::Succeeded,
                );
                provider_observation.evidence_refs =
                    vec![format!("execution_node:{}", ticket.node_id)];
                provider_observation.cost_delta = CostDelta {
                    model_steps: 1,
                    duration_ms: usage.duration_ms,
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    cached_tokens: usage.cached_tokens,
                    ..CostDelta::default()
                };
                let context_pressure_basis_points =
                    context_pressure_basis_points(usage.input_tokens, state.context_window);
                let mut context_observation = runtime_observation(
                    observation_identity.clone(),
                    RuntimeObservationKind::ContextPressure,
                    "runtime.context_ledger",
                    observation_revision,
                    format!(
                        "model request consumed {} input tokens against a {} token context window",
                        usage.input_tokens, state.context_window
                    ),
                    format!(
                        "context-pressure:{}:{}",
                        state.context_window, context_pressure_basis_points
                    ),
                    ObservationResultClass::Informational,
                );
                context_observation.evidence_refs =
                    vec![format!("execution_node:{}", ticket.node_id)];
                context_observation.context_delta = ContextDelta {
                    context_window_tokens: u64::from(state.context_window),
                    input_tokens: usage.input_tokens,
                    pressure_basis_points: u16::try_from(context_pressure_basis_points)
                        .unwrap_or(10_000)
                        .min(10_000),
                };
                let mut strategy_observation = runtime_observation(
                    observation_identity,
                    RuntimeObservationKind::StrategyHistory,
                    "runtime.strategy_checkpoint",
                    observation_revision,
                    format!(
                        "model intent {} has {} independent ready tool action(s)",
                        model_intent_kind(&intent),
                        independent_tool_call_count(&intent)
                    ),
                    format!(
                        "strategy:{}:{}",
                        model_intent_kind(&intent),
                        independent_tool_call_count(&intent)
                    ),
                    ObservationResultClass::Informational,
                );
                strategy_observation.evidence_refs =
                    vec![format!("execution_node:{}", ticket.node_id)];
                strategy_observation.parallelism_delta = ParallelismDelta {
                    ready_work: u16::try_from(independent_tool_call_count(&intent))
                        .unwrap_or(u16::MAX),
                };
                let mut committed_result_ref = format!("{}:model-result", ticket.graph_id);
                let reasoning_only_response =
                    step.assistant_message
                        .blocks
                        .iter()
                        .any(|block| match block {
                            ContentBlock::Thinking { thinking, .. } => !thinking.trim().is_empty(),
                            ContentBlock::ReasoningSummary { text } => !text.trim().is_empty(),
                            _ => false,
                        })
                        && step
                            .assistant_message
                            .blocks
                            .iter()
                            .filter_map(|block| match block {
                                ContentBlock::Text { text } => Some(text.as_str()),
                                _ => None,
                            })
                            .all(|text| text.trim().is_empty());
                // Parallel scheduling is performed from the concrete tool DAG
                // after the model names its calls. Do not feed an early,
                // planning-only Parallelize proposal back into the prompt: a
                // stale proposal previously encouraged a completed protocol
                // synthesis role to continue exploring.
                let mut model_intervention = None;
                let mut next_model_context = None;
                let next = match intent {
                    ModelStepIntent::FinalAnswer { text } => 'final_answer: {
                        let mut text = strip_trailing_simulated_tool_markup(text);
                        let task_understanding = state.task_understanding.clone();
                        let required_team_executions =
                            required_team_execution_count_for_execution_context(
                                task_understanding
                                    .as_ref()
                                    .map_or(0, |value| value.required_team_count),
                                state.execution_role.is_delegated_leaf(),
                                state.evaluation_judge_only,
                            );
                        let verified_team_executions =
                            completed_program_team_ids(&state.tool_results).len();
                        if verified_team_executions < required_team_executions {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            // Compatibility providers can return prose even
                            // with a named-tool wire constraint. Retry the
                            // Runtime-owned semantic admission a small,
                            // explicit number of times before reporting the
                            // missing native receipt; no retry creates a
                            // hidden Program.
                            if state.team_orchestration_requests < ROOT_CONTROL_PLANE_REPAIR_BUDGET
                            {
                                state.team_orchestration_requests =
                                    state.team_orchestration_requests.saturating_add(1);
                                let catalog_hint = self
                                    .services
                                    .definition_registry()
                                    .runnable_team_catalog()
                                    .ok()
                                    .into_iter()
                                    .flatten()
                                    .filter(|entry| {
                                        let id = entry.revision_ref.template_id.as_str();
                                        id.starts_with("workspace/") || id.starts_with("user/")
                                    })
                                    .take(3)
                                    .map(|entry| {
                                        let roles = entry
                                            .roles
                                            .iter()
                                            .filter_map(|role| {
                                                role.display_name
                                                    .as_deref()
                                                    .or(Some(role.role_id.as_str()))
                                            })
                                            .collect::<Vec<_>>()
                                            .join("、");
                                        format!(
                                            "{}（revision {}；名称：{}；角色：{}）",
                                            entry.revision_ref.template_id.as_str(),
                                            entry.revision_ref.revision,
                                            entry.name,
                                            roles
                                        )
                                    })
                                    .collect::<Vec<_>>()
                                    .join("；");
                                let reason = format!(
                                "团队编排尚未完成：当前 turn 还没有任何已验证的团队执行。{}请提交一次 submit_collaboration_decision：只填写本任务的独立 workstreams、它们的 depends_on、objective、evidence_contract 与必要 focuses；Runtime 会绑定模板、身份、权限与物理图。不要输出总结文本，也不要发送 runtime_orchestrate 的完整图提案（受控尝试 {}/{}）。",
                                    if catalog_hint.is_empty() {
                                        String::new()
                                    } else {
                                        format!("当前已发布的用户模板：{catalog_hint}。")
                                    },
                                    state.team_orchestration_requests,
                                    ROOT_CONTROL_PLANE_REPAIR_BUDGET,
                                );
                                state.content.push_str("\n\n");
                                state.content.push_str(&reason);
                                let mut item = ContextItem::new(
                                    format!("team-orchestration-replan:{}", ticket.node_id),
                                    ContextSourceKind::Task,
                                    ContextRole::Instruction,
                                    reason.clone(),
                                );
                                item.authority = ContextAuthority::System;
                                item.visibility = ContextVisibility::Private;
                                item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                                next_model_context = Some(item);
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Replan,
                                        reason,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                break 'final_answer vec![dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "team-orchestration-replan-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                )];
                            }
                            let reason = format!(
                                "missing_control_plane_proposal: explicit Team acceptance is incomplete after {ROOT_CONTROL_PLANE_REPAIR_BUDGET} bounded control-plane repairs; verified {verified_team_executions} of {required_team_executions} required Team execution(s)"
                            );
                            state.pending_root_control_plane_receipt = Some(reason.clone());
                            state.terminal_override =
                                Some((GoalCompletion::Partial, reason.clone()));
                            model_intervention =
                                Some(harness_contract::goal::RuntimeIntervention {
                                    goal_id: state.goal_id.clone(),
                                    kind: RuntimeInterventionKind::Block,
                                    reason,
                                    evidence_refs: vec![format!(
                                        "execution_node:{}",
                                        ticket.node_id
                                    )],
                                    expected_graph_revision: None,
                                });
                            let mut node = dynamic_node(
                                ticket,
                                state.iterations,
                                "explicit-team-acceptance-block-synthesize",
                                ExecutionNodeKind::Synthesize,
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                "inline_model",
                            );
                            node.executor_kind =
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                                    .to_string();
                            break 'final_answer vec![node];
                        }
                        let successful_write_observed = write_obligation_satisfied(
                            state.required_write_for_completion,
                            &state.required_workspace_write_scopes,
                            &state.committed_workspace_observed_evidence,
                            state.collaboration_committed_write
                                || state.committed_workspace_write_observed,
                            self.services.path_identity_resolver(),
                        );
                        let missing_write =
                            state.required_write_for_completion && !successful_write_observed;
                        // Delegated Agent output is an internal Team artifact. It may use the
                        // most effective working language because the root turn owns the final,
                        // user-visible synthesis and its language contract.
                        let missing_language = response_language_mismatch_for_role(
                            &state.content,
                            &text,
                            state.execution_role.is_delegated_leaf(),
                        );
                        let acceptance_disposition = root_acceptance_disposition(
                            missing_write,
                            missing_language,
                            state.root_write_replans,
                            state.root_language_replan_attempted,
                        );
                        if let RootAcceptanceDisposition::Replan {
                            write: recover_write,
                            language: recover_language,
                        } = acceptance_disposition
                        {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            let mut missing = Vec::new();
                            if recover_write {
                                state.root_write_replans =
                                    state.root_write_replans.saturating_add(1);
                                if state.required_workspace_write_scopes.is_empty() {
                                    missing
                                        .push("a committed workspace artifact write".to_string());
                                } else {
                                    missing.push(format!(
                                        "a committed workspace artifact write to the exact target(s) [{}]",
                                        state.required_workspace_write_scopes.join(", ")
                                    ));
                                }
                                state.force_tool_allowlist_next_model =
                                    Some(required_mutation_tool_allowlist());
                            }
                            if recover_language {
                                state.root_language_replan_attempted = true;
                                missing.push("a final response in the user's language".to_string());
                                if !recover_write {
                                    state.force_text_only_next_model = true;
                                }
                            }
                            let reason = format!(
                                "Runtime root-goal acceptance is incomplete: {}. Preserve verified Team/tool evidence, complete only these missing obligations, and do not claim success before Runtime observes them.",
                                missing.join("; ")
                            );
                            state.content.push_str("\n\n");
                            state.content.push_str(&reason);
                            let mut item = ContextItem::new(
                                format!("runtime-root-acceptance-replan:{}", ticket.node_id),
                                ContextSourceKind::Task,
                                ContextRole::Instruction,
                                reason.clone(),
                            );
                            item.authority = ContextAuthority::System;
                            item.visibility = ContextVisibility::Private;
                            item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                            next_model_context = Some(item);
                            model_intervention =
                                Some(harness_contract::goal::RuntimeIntervention {
                                    goal_id: state.goal_id.clone(),
                                    kind: RuntimeInterventionKind::Replan,
                                    reason,
                                    evidence_refs: vec![format!(
                                        "execution_node:{}",
                                        ticket.node_id
                                    )],
                                    expected_graph_revision: None,
                                });
                            break 'final_answer vec![dynamic_node(
                                ticket,
                                state.iterations,
                                "root-acceptance-replan-model",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            )];
                        }
                        // Presentation quality must not invalidate completed business work.
                        // After one language rewrite attempt, preserve a usable verified answer
                        // in any language. Missing required writes remain a hard blocker because
                        // the requested business artifact still does not exist.
                        if acceptance_disposition == RootAcceptanceDisposition::BlockMissingWrite {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            let reason = format!(
                                "Execution blocked because the required workspace artifact remained incomplete after bounded recovery: write_required={}, write_satisfied={}",
                                state.required_write_for_completion,
                                !missing_write,
                            );
                            state.terminal_override =
                                Some((GoalCompletion::Partial, reason.clone()));
                            model_intervention =
                                Some(harness_contract::goal::RuntimeIntervention {
                                    goal_id: state.goal_id.clone(),
                                    kind: RuntimeInterventionKind::Block,
                                    reason,
                                    evidence_refs: vec![format!(
                                        "execution_node:{}",
                                        ticket.node_id
                                    )],
                                    expected_graph_revision: None,
                                });
                            let mut node = dynamic_node(
                                ticket,
                                state.iterations,
                                "root-acceptance-block-synthesize",
                                ExecutionNodeKind::Synthesize,
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                "inline_model",
                            );
                            node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                            break 'final_answer vec![node];
                        }
                        let normalized = normalize_terminal_answer_with_evidence(
                            &text,
                            &state.tool_results,
                            self.services.workspace_root(),
                            &state.content,
                        );
                        if normalized != text {
                            text = normalized;
                            let TurnGraphState {
                                assistant_messages,
                                pending_transcript,
                                ..
                            } = &mut *state;
                            replace_latest_assistant_text(
                                assistant_messages,
                                pending_transcript,
                                &ticket.node_id,
                                &text,
                            );
                        }
                        let focus_acceptance_continuation = if state.bounded_evidence_role
                            && !state.focus_acceptance_pending_scopes.is_empty()
                            && !reasoning_only_response
                            && !text.trim().is_empty()
                        {
                            if state.pending_focus_terminal_candidate.is_none() {
                                let pending = state.focus_acceptance_pending_scopes.join(", ");
                                let instruction = format!(
                                    "Runtime Focus acceptance recovery (mandatory): retain the candidate final JSON, but do not finish yet. Complete the missing Runtime-verified action(s) with native tools: {pending}. For verify_after_write:path, perform a new exact-path read after this role's committed write receipt. For verify_upstream_change:path, independently read the exact upstream-changed path. Do not return another final answer until these actions complete."
                                );
                                state.pending_focus_terminal_candidate = Some(text.clone());
                                state.assistant_messages.pop();
                                state.pending_transcript.remove(&ticket.node_id);
                                let concrete_verification_scopes =
                                    concrete_focus_verification_scopes(
                                        &state.focus_acceptance_pending_scopes,
                                        &state.focus_observed_evidence,
                                        self.services.path_identity_resolver(),
                                    );
                                let verification_calls = focus_verification_tool_calls(
                                    &concrete_verification_scopes,
                                    state.iterations,
                                    self.services.workspace_root(),
                                );
                                state.content.push_str("\n\n");
                                state.content.push_str(&instruction);
                                let mut item = ContextItem::new(
                                    format!("runtime-focus-acceptance-recovery:{}", ticket.node_id),
                                    ContextSourceKind::Task,
                                    ContextRole::Instruction,
                                    instruction.clone(),
                                );
                                item.authority = ContextAuthority::System;
                                item.visibility = ContextVisibility::Private;
                                item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                                next_model_context = Some(item);
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Replan,
                                        reason: instruction,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                if let Some(calls) = verification_calls {
                                    Some(tool_nodes_for_calls(
                                        ticket,
                                        state.iterations,
                                        &state.session_id,
                                        calls,
                                        self.services.workspace_root(),
                                    )?)
                                } else {
                                    Some(vec![dynamic_node(
                                        ticket,
                                        state.iterations,
                                        "focus-acceptance-recovery-model",
                                        ExecutionNodeKind::InlineModel,
                                        "inline_model",
                                        "inline_model",
                                    )])
                                }
                            } else {
                                let reason = format!(
                                    "delegated role returned a second final answer before completing Focus acceptance actions: {}",
                                    state.focus_acceptance_pending_scopes.join(", ")
                                );
                                state.assistant_messages.pop();
                                state.pending_transcript.remove(&ticket.node_id);
                                state.terminal_override =
                                    Some((GoalCompletion::Partial, reason.clone()));
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Block,
                                        reason,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                let mut node = dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "focus-acceptance-block-synthesize",
                                    ExecutionNodeKind::Synthesize,
                                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                    "inline_model",
                                );
                                node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                Some(vec![node])
                            }
                        } else {
                            None
                        };
                        let structured_output_continuation = if focus_acceptance_continuation
                            .is_none()
                            && state.bounded_evidence_role
                            && state.focus_acceptance_pending_scopes.is_empty()
                            && !state.focus_required_output_fields.is_empty()
                            && !reasoning_only_response
                            && !text.trim().is_empty()
                        {
                            let normalized = normalized_team_terminal_candidate(
                                &text,
                                &state.focus_required_output_fields,
                            )
                            .or_else(|| {
                                (state.structured_output_replans > 0)
                                    .then(|| {
                                        normalized_declared_custom_terminal_after_recovery(
                                            &text,
                                            &state.focus_required_output_fields,
                                        )
                                    })
                                    .flatten()
                            });
                            if let Some(normalized) = normalized {
                                if normalized != text {
                                    text = normalized;
                                    let TurnGraphState {
                                        assistant_messages,
                                        pending_transcript,
                                        ..
                                    } = &mut *state;
                                    replace_latest_assistant_text(
                                        assistant_messages,
                                        pending_transcript,
                                        &ticket.node_id,
                                        &text,
                                    );
                                }
                                None
                            } else {
                                let missing = missing_required_structured_fields(
                                    &text,
                                    &state.focus_required_output_fields,
                                );
                                state.assistant_messages.pop();
                                state.pending_transcript.remove(&ticket.node_id);
                                state.structured_output_replans =
                                    state.structured_output_replans.saturating_add(1);
                                if state.structured_output_replans
                                    <= STRUCTURED_OUTPUT_RECOVERY_BUDGET
                                {
                                    let instruction = format!(
                                        "Runtime terminal-presentation recovery {}/{} (mandatory): retained evidence satisfies the bounded role, but the terminal answer omits required field(s): {}. Tools are disabled. Give a concise answer with every field [{}], using native structured output, JSON, Markdown headings, or `Field: value` labels. Ground it only in retained receipts; risks or unresolved work must be explicitly stated when applicable.",
                                        state.structured_output_replans,
                                        STRUCTURED_OUTPUT_RECOVERY_BUDGET,
                                        missing.join(", "),
                                        state.focus_required_output_fields.join(", "),
                                    );
                                    state.content.push_str("\n\n");
                                    state.content.push_str(&instruction);
                                    // A normal text-only continuation retains
                                    // the exploratory assistant/tool history.
                                    // Providers that have entered a broad
                                    // inspection loop commonly ignore the
                                    // presentation-only instruction and keep
                                    // enumerating files. Reuse Runtime's
                                    // evidence-only, zero-tool synthesis path
                                    // so this single recovery sees the exact
                                    // contract and committed receipts without
                                    // the stale tool trajectory.
                                    state.clean_terminal_synthesis_attempted = true;
                                    state.clean_terminal_synthesis_next = true;
                                    model_intervention =
                                        Some(harness_contract::goal::RuntimeIntervention {
                                            goal_id: state.goal_id.clone(),
                                            kind: RuntimeInterventionKind::Replan,
                                            reason: instruction,
                                            evidence_refs: vec![format!(
                                                "execution_node:{}",
                                                ticket.node_id
                                            )],
                                            expected_graph_revision: None,
                                        });
                                    Some(vec![dynamic_node(
                                        ticket,
                                        state.iterations,
                                        "structured-output-clean-recovery-model",
                                        ExecutionNodeKind::InlineModel,
                                        "inline_model",
                                        "inline_model",
                                    )])
                                } else {
                                    let reason = format!(
                                        "delegated role omitted required structured field(s) after bounded recovery: {}",
                                        missing.join(", ")
                                    );
                                    state.terminal_override =
                                        Some((GoalCompletion::Partial, reason.clone()));
                                    model_intervention =
                                        Some(harness_contract::goal::RuntimeIntervention {
                                            goal_id: state.goal_id.clone(),
                                            kind: RuntimeInterventionKind::Block,
                                            reason,
                                            evidence_refs: vec![format!(
                                                "execution_node:{}",
                                                ticket.node_id
                                            )],
                                            expected_graph_revision: None,
                                        });
                                    let mut node = dynamic_node(
                                        ticket,
                                        state.iterations,
                                        "structured-output-block-synthesize",
                                        ExecutionNodeKind::Synthesize,
                                        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                        "inline_model",
                                    );
                                    node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                    Some(vec![node])
                                }
                            }
                        } else {
                            None
                        };
                        let normal_reasoning_continuation = if reasoning_only_response
                            && !force_text_only_response
                            && !step.text_only_response
                        {
                            state.reasoning_only_attempts =
                                state.reasoning_only_attempts.saturating_add(1);
                            let continuation_budget =
                                terminal_recovery_retry_budget(&state.safety_lease);
                            if state.reasoning_only_attempts <= continuation_budget {
                                let instruction = format!(
                                    "Runtime continuation (mandatory): the previous model step produced reasoning but no visible answer. Continue the same goal from retained evidence. If evidence is still missing, use the smallest relevant available tool; otherwise write the visible final answer now. Do not finish with reasoning only. Continuation attempt {}/{}.",
                                    state.reasoning_only_attempts, continuation_budget,
                                );
                                state.content.push_str("\n\n");
                                state.content.push_str(&instruction);
                                let mut item = ContextItem::new(
                                    format!("runtime-reasoning-continuation:{}", ticket.node_id),
                                    ContextSourceKind::Task,
                                    ContextRole::Instruction,
                                    instruction.clone(),
                                );
                                item.authority = ContextAuthority::System;
                                item.visibility = ContextVisibility::Private;
                                item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                                next_model_context = Some(item);
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Replan,
                                        reason: instruction,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                Some(vec![dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "reasoning-continuation-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                )])
                            } else {
                                // Private reasoning without visible output is
                                // not an infinite retry class. After the same
                                // lease-derived allowance used elsewhere, use
                                // the normal no-tool terminal recovery path.
                                state.reasoning_only_attempts = 0;
                                None
                            }
                        } else {
                            None
                        };
                        if let Some(next) = focus_acceptance_continuation {
                            next
                        } else if let Some(next) = structured_output_continuation {
                            next
                        } else if let Some(next) = normal_reasoning_continuation {
                            next
                        // A frozen Judge turn deliberately returns machine JSON
                        // rather than a user-facing answer. The harness validates
                        // its exact schema; running normal prose recovery here can
                        // replace a valid score object with unrelated fallback text.
                        } else if let Some(reason) = (!state.evaluation_judge_only)
                            .then(|| {
                                final_answer_recovery_reason_for_execution_scope(
                                    &text,
                                    self.services.workspace_root(),
                                    &state.content,
                                    state.bounded_evidence_role,
                                )
                            })
                            .flatten()
                        {
                            // A malformed or obviously unfinished answer is
                            // not proof that the user goal failed. First try
                            // one normal text-only continuation. If the
                            // exploratory transcript still traps the
                            // provider in its prior tool protocol, run one
                            // clean evidence-only synthesis with no historical
                            // tool-call messages. Neither path may loop.
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            state.terminal_recovery_attempts =
                                state.terminal_recovery_attempts.saturating_add(1);
                            let normal_recovery_budget =
                                terminal_recovery_retry_budget(&state.safety_lease).min(1);
                            if state.terminal_recovery_attempts <= normal_recovery_budget {
                                let instruction = format!(
                                    "Runtime final-answer recovery (mandatory): the prior provider response was unusable ({reason}). Do not call tools or emit simulated tool markup. Use only already committed evidence and return a concise final answer now; name any remaining uncertainty explicitly. Recovery attempt {}/{}.",
                                    state.terminal_recovery_attempts, normal_recovery_budget,
                                );
                                state.content.push_str("\n\n");
                                state.content.push_str(&instruction);
                                state.force_text_only_next_model = true;
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Replan,
                                        reason: instruction,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                vec![dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "final-answer-recovery-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                )]
                            } else if !state.clean_terminal_synthesis_attempted
                                && state.iterations < state.safety_lease.max_model_steps
                            {
                                state.clean_terminal_synthesis_attempted = true;
                                state.clean_terminal_synthesis_next = true;
                                model_intervention = Some(
                                    harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Synthesize,
                                        reason: format!(
                                            "normal final-answer recovery remained unusable ({reason}); isolate committed evidence from exploratory history and synthesize once"
                                        ),
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    },
                                );
                                vec![dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "clean-terminal-synthesis-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                )]
                            } else if let Some(fallback) = retained_orchestration_terminal_candidate(
                                &state.tool_results,
                                self.services.workspace_root(),
                                &state.content,
                            ) {
                                state
                                    .assistant_messages
                                    .push(ConversationMessage::assistant(vec![
                                        ContentBlock::Text {
                                            text: fallback.clone(),
                                        },
                                    ]));
                                state.pending_transcript.insert(
                                    ticket.node_id.clone(),
                                    vec![ConversationMessage::assistant(vec![
                                        ContentBlock::Text {
                                            text: fallback.clone(),
                                        },
                                    ])],
                                );
                                committed_result_ref = format!(
                                    "assistant_json:{}",
                                    serde_json::to_string(&fallback).map_err(|error| {
                                        NodeExecutorError::Poll {
                                            node_id: ticket.node_id.clone(),
                                            reason: error.to_string(),
                                        }
                                    })?
                                );
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Synthesize,
                                        reason: "clean provider synthesis was unusable; published the checked Team terminal candidate after deterministic source-evidence normalization"
                                            .to_string(),
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                let mut node = dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "retained-team-terminal-synthesize",
                                    ExecutionNodeKind::Synthesize,
                                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                    "inline_model",
                                );
                                node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                vec![node]
                            } else if state.clean_terminal_synthesis_attempted
                                && !state.clean_terminal_retry_attempted
                                && state.iterations < state.safety_lease.max_model_steps
                            {
                                state.clean_terminal_retry_attempted = true;
                                state.clean_terminal_synthesis_next = true;
                                model_intervention = Some(
                                    harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Synthesize,
                                        reason: format!(
                                            "the first isolated terminal synthesis remained unusable ({reason}); retry exactly once from the same committed evidence without exploratory history"
                                        ),
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    },
                                );
                                vec![dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "clean-terminal-synthesis-retry-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                )]
                            } else {
                                state.terminal_override = Some((
                                    GoalCompletion::Partial,
                                    format!(
                                        "Execution could not obtain a usable final answer after bounded normal and clean synthesis recovery: {reason}. Committed evidence was retained; provide a new constraint, provider, or explicit replan to continue."
                                    ),
                                ));
                                model_intervention = Some(
                                    harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Block,
                                        reason: format!(
                                            "provider produced unusable final output after one normal and one clean synthesis attempt: {reason}",
                                        ),
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    },
                                );
                                let mut node = dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "final-answer-block-synthesize",
                                    ExecutionNodeKind::Synthesize,
                                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                    "inline_model",
                                );
                                node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                vec![node]
                            }
                        } else {
                            committed_result_ref = format!(
                                "assistant_json:{}",
                                serde_json::to_string(&text).map_err(|error| {
                                    NodeExecutorError::Poll {
                                        node_id: ticket.node_id.clone(),
                                        reason: error.to_string(),
                                    }
                                })?
                            );
                            let mut node = dynamic_node(
                                ticket,
                                state.iterations,
                                "synthesize",
                                ExecutionNodeKind::Synthesize,
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                "inline_model",
                            );
                            node.executor_kind =
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                                    .to_string();
                            vec![node]
                        }
                    }
                    ModelStepIntent::ToolCalls { calls } => {
                        record_write_attempt_paths(
                            &mut state.write_attempt_paths,
                            &calls,
                            self.services.workspace_root(),
                        );
                        let pending_focus_write_action = pending_focus_write_action_violation(
                            &state.focus_acceptance_pending_scopes,
                            &state.focus_observed_resource_scopes,
                            &calls,
                            self.services.workspace_root(),
                        );
                        let evaluation_scope_violation = evaluation_scope_violation(
                            &state.evaluation_resource_scopes,
                            &calls,
                            self.services.workspace_root(),
                        );
                        if let Some(pending_writes) = pending_focus_write_action {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            let (intervention, next) =
                                focus_action_rejection_outcome(ticket, &mut state, &pending_writes);
                            model_intervention = Some(intervention);
                            next
                        } else if let Some(violation) = evaluation_scope_violation {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            let authorized_scopes = state.evaluation_resource_scopes.join(", ");
                            let (intervention, next) = evaluation_scope_rejection_outcome(
                                ticket,
                                &mut state,
                                &violation,
                                self.services.workspace_root(),
                                self.services.path_identity_resolver(),
                                "eval-resource-ceiling-replan-model",
                                format!(
                                    "the pre-registered evaluation resource ceiling rejected `{violation}`; authorized exact scopes are [{authorized_scopes}]. Do not use broad workspace, shell, execute-code, glob, or pathless search calls. Use exact-path file tools for those scopes, including the authorized write tool when the objective requires mutation"
                                ),
                            );
                            model_intervention = Some(intervention);
                            next
                        } else if requests_runtime_orchestration(&calls)
                            && state.nested_orchestration_forbidden
                        {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            if state.team_orchestration_requests == 0 {
                                state.team_orchestration_requests = 1;
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                    goal_id: state.goal_id.clone(),
                                    kind: RuntimeInterventionKind::Replan,
                                    reason: "this delegated Agent is a leaf execution; complete the bounded Focus with the currently authorized local tools and do not request Agent, Team, Session, or Mission orchestration"
                                        .to_string(),
                                    evidence_refs: vec![format!(
                                        "execution_node:{}",
                                        ticket.node_id
                                    )],
                                    expected_graph_revision: None,
                                });
                                vec![dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "delegated-local-replan-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                )]
                            } else {
                                let reason = "delegated Agent repeated a forbidden nested orchestration request after the bounded local replan".to_string();
                                state.terminal_override =
                                    Some((GoalCompletion::Partial, reason.clone()));
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Block,
                                        reason,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                let mut node = dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "delegated-orchestration-block-synthesize",
                                    ExecutionNodeKind::Synthesize,
                                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                    "inline_model",
                                );
                                node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                vec![node]
                            }
                        } else if let Some(missing_scopes) =
                            missing_root_collaboration_evidence_scopes(
                                &calls,
                                state
                                    .task_understanding
                                    .as_ref()
                                    .map_or(&[], |understanding| {
                                        understanding.required_workspace_evidence_scopes.as_slice()
                                    }),
                            )
                        {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            let required_scopes = state.task_understanding.as_ref().map_or_else(
                                Vec::new,
                                |understanding| {
                                    understanding.required_workspace_evidence_scopes.clone()
                                },
                            );
                            if state.root_evidence_scope_repairs == 0 {
                                state.root_evidence_scope_repairs = 1;
                                let reason = format!(
                                    "Runtime rejected the root collaboration proposal because it substituted or omitted user-named immutable evidence scope(s): [{}]. Submit the same required Team count again, preserving every exact scope in [{}] as evidence_scope entries; do not replace them with logs, directories, or generated artifacts.",
                                    missing_scopes.join(", "),
                                    required_scopes.join(", "),
                                );
                                state.content.push_str("\n\n");
                                state.content.push_str(&reason);
                                let mut item = ContextItem::new(
                                    format!(
                                        "runtime-root-evidence-scope-recovery:{}",
                                        ticket.node_id
                                    ),
                                    ContextSourceKind::Task,
                                    ContextRole::Instruction,
                                    reason.clone(),
                                );
                                item.authority = ContextAuthority::System;
                                item.visibility = ContextVisibility::Private;
                                item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                                next_model_context = Some(item);
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Replan,
                                        reason,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                vec![dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "root-evidence-scope-recovery-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                )]
                            } else {
                                let reason = format!(
                                    "root collaboration proposal repeatedly omitted immutable user-named evidence scope(s): [{}]",
                                    missing_scopes.join(", "),
                                );
                                state.pending_root_control_plane_receipt = Some(reason.clone());
                                state.terminal_override =
                                    Some((GoalCompletion::Partial, reason.clone()));
                                model_intervention =
                                    Some(harness_contract::goal::RuntimeIntervention {
                                        goal_id: state.goal_id.clone(),
                                        kind: RuntimeInterventionKind::Block,
                                        reason,
                                        evidence_refs: vec![format!(
                                            "execution_node:{}",
                                            ticket.node_id
                                        )],
                                        expected_graph_revision: None,
                                    });
                                let mut node = dynamic_node(
                                    ticket,
                                    state.iterations,
                                    "root-evidence-scope-block-synthesize",
                                    ExecutionNodeKind::Synthesize,
                                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                    "inline_model",
                                );
                                node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                vec![node]
                            }
                        } else if requests_team_orchestration(&calls) {
                            if !team_orchestration_request_available(
                                &state.content,
                                state.collaboration_started,
                                state.team_orchestration_requests,
                            ) {
                                state.assistant_messages.pop();
                                state.pending_transcript.remove(&ticket.node_id);
                                let objective_requires_write = state.required_write_for_completion
                                    || state
                                        .task_understanding
                                        .as_ref()
                                        .is_some_and(|value| value.requires_write);
                                match exhausted_team_lease_disposition(
                                    objective_requires_write,
                                    write_obligation_satisfied(
                                        objective_requires_write,
                                        &state.required_workspace_write_scopes,
                                        &state.committed_workspace_observed_evidence,
                                        state.collaboration_committed_write
                                            || state.committed_workspace_write_observed,
                                        self.services.path_identity_resolver(),
                                    ),
                                ) {
                                    ExhaustedTeamLeaseDisposition::CompleteRemainingWrite => {
                                        state.root_write_replans =
                                            state.root_write_replans.saturating_add(1);
                                        state.force_tool_allowlist_next_model =
                                            Some(required_mutation_tool_allowlist());
                                        let required_targets = if state
                                            .required_workspace_write_scopes
                                            .is_empty()
                                        {
                                            "the artifact requested by the user".to_string()
                                        } else {
                                            format!(
                                                "the exact target(s) [{}]",
                                                state.required_workspace_write_scopes.join(", ")
                                            )
                                        };
                                        let reason = format!(
                                            "the bounded Team phase is complete and its checked evidence is retained, but the parent objective still requires a committed workspace artifact at {required_targets}. Do not start another Team. Use the exposed write tool now to create that exact requested artifact from the retained evidence, then return the best supported result; presentation language can be repaired independently and must not block completed business work."
                                        );
                                        state.content.push_str("\n\n");
                                        state.content.push_str(&reason);
                                        let mut item = ContextItem::new(
                                            format!(
                                                "runtime-team-lease-remaining-write:{}",
                                                ticket.node_id
                                            ),
                                            ContextSourceKind::Task,
                                            ContextRole::Instruction,
                                            reason.clone(),
                                        );
                                        item.authority = ContextAuthority::System;
                                        item.visibility = ContextVisibility::Private;
                                        item.evidence =
                                            vec![format!("execution_node:{}", ticket.node_id)];
                                        next_model_context = Some(item);
                                        model_intervention =
                                            Some(harness_contract::goal::RuntimeIntervention {
                                                goal_id: state.goal_id.clone(),
                                                kind: RuntimeInterventionKind::Replan,
                                                reason,
                                                evidence_refs: vec![format!(
                                                    "execution_node:{}",
                                                    ticket.node_id
                                                )],
                                                expected_graph_revision: None,
                                            });
                                        vec![dynamic_node(
                                            ticket,
                                            state.iterations,
                                            "team-lease-remaining-write-model",
                                            ExecutionNodeKind::InlineModel,
                                            "inline_model",
                                            "inline_model",
                                        )]
                                    }
                                    ExhaustedTeamLeaseDisposition::CleanSynthesis => {
                                        if let Some(candidate) =
                                            retained_orchestration_terminal_candidate(
                                                &state.tool_results,
                                                self.services.workspace_root(),
                                                &state.content,
                                            )
                                        {
                                            state.terminal_override =
                                                Some((GoalCompletion::Satisfied, candidate));
                                            model_intervention = Some(
                                                harness_contract::goal::RuntimeIntervention {
                                                    goal_id: state.goal_id.clone(),
                                                    kind: RuntimeInterventionKind::Synthesize,
                                                    reason: "a verified Team terminal already satisfies the bounded collaboration phase; publish its typed terminal carrier instead of asking another model to reconstruct evidence"
                                                        .to_string(),
                                                    evidence_refs: vec![format!(
                                                        "execution_node:{}",
                                                        ticket.node_id
                                                    )],
                                                    expected_graph_revision: None,
                                                },
                                            );
                                            let mut node = dynamic_node(
                                                ticket,
                                                state.iterations,
                                                "retained-team-terminal-synthesize",
                                                ExecutionNodeKind::Synthesize,
                                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                                "inline_model",
                                            );
                                            node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                            vec![node]
                                        } else {
                                            state.clean_terminal_synthesis_attempted = true;
                                            state.clean_terminal_synthesis_next = true;
                                            model_intervention = Some(
                                                harness_contract::goal::RuntimeIntervention {
                                                    goal_id: state.goal_id.clone(),
                                                    kind: RuntimeInterventionKind::Synthesize,
                                                    reason: "one Team execution has already consumed this turn's collaboration lease; synthesize from its retained terminal and evidence receipts instead of starting another Team"
                                                        .to_string(),
                                                    evidence_refs: vec![format!(
                                                        "execution_node:{}",
                                                        ticket.node_id
                                                    )],
                                                    expected_graph_revision: None,
                                                },
                                            );
                                            vec![dynamic_node(
                                                ticket,
                                                state.iterations,
                                                "team-lease-clean-synthesis-model",
                                                ExecutionNodeKind::InlineModel,
                                                "inline_model",
                                                "inline_model",
                                            )]
                                        }
                                    }
                                }
                            } else {
                                state.team_orchestration_requests = 1;
                                tool_nodes_for_calls(
                                    ticket,
                                    state.iterations,
                                    &state.session_id,
                                    calls,
                                    self.services.workspace_root(),
                                )?
                            }
                        } else {
                            tool_nodes_for_calls(
                                ticket,
                                state.iterations,
                                &state.session_id,
                                calls,
                                self.services.workspace_root(),
                            )?
                        }
                    }
                    ModelStepIntent::Replan { reason } => {
                        model_intervention = Some(harness_contract::goal::RuntimeIntervention {
                            goal_id: state.goal_id.clone(),
                            kind: RuntimeInterventionKind::Replan,
                            reason: reason.clone(),
                            evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                            expected_graph_revision: None,
                        });
                        state.content.push_str("\n\nRuntime replan guidance: ");
                        state.content.push_str(&reason);
                        vec![dynamic_node(
                            ticket,
                            state.iterations,
                            "model",
                            ExecutionNodeKind::InlineModel,
                            "inline_model",
                            "inline_model",
                        )]
                    }
                };
                let edges = dynamic_edges(&ticket.node_id, &next);
                if next_model_context.is_none() {
                    next_model_context =
                        runtime_replan_context_item(&ticket.node_id, model_intervention.as_ref());
                }
                if let Some(item) = next_model_context {
                    state.pending_next_model_context.push(item);
                }
                let mut outcome =
                    NodeExecutionOutcome::new(completed_result(Some(committed_result_ref), usage))
                        .with_replan(ExecutionGraphReplan {
                            nodes: next,
                            edges,
                            reason: "provider intent advanced the turn graph".to_string(),
                        });
                for (index, upstream) in upstream_observations.iter().enumerate() {
                    outcome.domain_events.push(
                        self.services
                            .goal_store()
                            .observation_event(
                                upstream,
                                format!("{}:upstream-observation:{index}", ticket.idempotency_key),
                            )
                            .map_err(|reason| NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason,
                            })?,
                    );
                }
                if let Some(input_observation) = input_observation.as_ref() {
                    outcome.domain_events.push(
                        self.services
                            .goal_store()
                            .observation_event(
                                input_observation,
                                format!("{}:input-observation", ticket.idempotency_key),
                            )
                            .map_err(|reason| NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason,
                            })?,
                    );
                }
                for (suffix, observation) in [
                    ("goal-observation", &observation),
                    ("strategy-observation", &strategy_observation),
                    ("provider-observation", &provider_observation),
                    ("context-observation", &context_observation),
                ] {
                    outcome.domain_events.push(
                        self.services
                            .goal_store()
                            .observation_event(
                                observation,
                                format!("{}:{suffix}", ticket.idempotency_key),
                            )
                            .map_err(|reason| NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason,
                            })?,
                    );
                }
                if let Some(intervention) = model_intervention {
                    outcome.domain_events.push(
                        self.services
                            .goal_store()
                            .intervention_event(
                                &intervention,
                                std::slice::from_ref(&observation),
                                format!("{}:goal-intervention", ticket.idempotency_key),
                            )
                            .map_err(|reason| NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason,
                            })?,
                    );
                }
                Ok(outcome)
            }
            Err(error) => {
                // A provider failure is execution evidence, not an implicit
                // graph terminal. Preserve it and let the same Goal policy
                // that governs tools decide whether the next node retries,
                // changes strategy, or produces an honest blocked result.
                let protocol_failure = error.is_provider_tool_protocol_failure();
                let tool_exposure_miss = error.is_tool_exposure_miss();
                let provider_usage = error.provider_usage();
                let effect_receipts = error.effect_receipts().to_vec();
                let reason = error.to_string();
                let protocol_failure_detail =
                    protocol_failure.then(|| reason.chars().take(512).collect::<String>());
                let (
                    goal_id,
                    iteration,
                    protocol_attempt,
                    post_receipt_failure,
                    clean_terminal_synthesis_attempted,
                    observation_identity,
                ) = {
                    let mut state = self.state.lock().await;
                    for receipt in effect_receipts {
                        let inserted = state
                            .early_tool_receipts
                            .insert(receipt.call.id.clone(), receipt)
                            .is_none();
                        if inserted {
                            state.tool_receipts_observed =
                                state.tool_receipts_observed.saturating_add(1);
                        }
                    }
                    state.iterations = state.iterations.saturating_add(1);
                    if let Some(usage) = provider_usage {
                        state.input_tokens = state
                            .input_tokens
                            .saturating_add(u64::from(usage.input_tokens));
                        state.output_tokens = state
                            .output_tokens
                            .saturating_add(u64::from(usage.output_tokens));
                        state.cache_create_tokens = state
                            .cache_create_tokens
                            .saturating_add(u64::from(usage.cache_creation_input_tokens));
                        state.cache_read_tokens = state
                            .cache_read_tokens
                            .saturating_add(u64::from(usage.cache_read_input_tokens));
                    }
                    // `execute_model_step` temporarily appends the ingress user
                    // before asking the provider, and the host rolls that
                    // uncommitted mutation back after every attempt.  A failed
                    // first attempt still commits a real graph node, so publish
                    // the ingress user with that node before scheduling its
                    // recovery.  Otherwise the retry runs with
                    // `first_step=false`, loses the current objective, and can
                    // incorrectly reuse the previous turn's terminal answer.
                    if first_step {
                        state.pending_transcript.insert(
                            ticket.node_id.clone(),
                            vec![ConversationMessage::user_text(content.clone())],
                        );
                    }
                    let post_receipt_failure = state.tool_receipts_observed > 0;
                    // A known deferred schema is a Runtime-owned replan, not
                    // a provider failure after a successful side effect.  A
                    // managed Agent commonly has its first source receipt
                    // before requesting its required escalation; synthesizing
                    // at that point discards the one governed retry and makes
                    // the terminal obligation impossible to satisfy.
                    let protocol_attempt = (protocol_failure
                        && (!post_receipt_failure || tool_exposure_miss))
                        .then(|| {
                            state.provider_protocol_recovery_attempts =
                                state.provider_protocol_recovery_attempts.saturating_add(1);
                            state.provider_protocol_recovery_attempts
                        });
                    let identity = runtime_observation_identity(&self.services, &state, ticket);
                    (
                        state.goal_id.clone(),
                        state.iterations,
                        protocol_attempt,
                        post_receipt_failure,
                        state.clean_terminal_synthesis_attempted,
                        identity,
                    )
                };
                let mut observation = runtime_observation(
                    observation_identity,
                    RuntimeObservationKind::ProviderProgress,
                    "runtime.provider_stream",
                    iteration as u64,
                    format!("provider model step failed: {reason}"),
                    "provider_failure".to_string(),
                    ObservationResultClass::Failed,
                );
                observation.evidence_refs = vec![format!("execution_node:{}", ticket.node_id)];
                observation.cost_delta.model_steps = 1;
                if let Some(usage) = provider_usage {
                    observation.cost_delta.input_tokens = u64::from(usage.input_tokens);
                    observation.cost_delta.output_tokens = u64::from(usage.output_tokens);
                    observation.cost_delta.cached_tokens = u64::from(usage.cache_read_input_tokens);
                }
                observation.failure_class = Some(ObservationFailureClass::Provider);
                let intervention = if post_receipt_failure && !tool_exposure_miss {
                    let already_synthesizing =
                        clean_terminal_synthesis || clean_terminal_synthesis_attempted;
                    // A tool-protocol violation inside the zero-tool synthesis
                    // means the model emitted tool calls despite no exposed
                    // schemas. Give that synthesis ONE strict text-only retry
                    // before blocking, so a committed write receipt is not
                    // lost to a single model protocol slip.
                    let allow_protocol_synthesis_retry = protocol_failure
                        && clean_terminal_synthesis_attempted
                        && !clean_terminal_synthesis;
                    let kind = if already_synthesizing && !allow_protocol_synthesis_retry {
                        provider_failure_intervention_kind_after_receipt(already_synthesizing)
                    } else {
                        RuntimeInterventionKind::Synthesize
                    };
                    harness_contract::goal::RuntimeIntervention {
                        goal_id: goal_id.clone(),
                        kind,
                        reason: if allow_protocol_synthesis_retry {
                            "provider emitted tool calls during zero-tool synthesis; retry once with strict text-only output while preserving the committed receipt"
                                .to_string()
                        } else if kind == RuntimeInterventionKind::Synthesize {
                            "the provider failed after a committed tool receipt; preserve the receipt and synthesize once from retained evidence with zero tools instead of retrying the provider action"
                                .to_string()
                        } else {
                            "the isolated evidence synthesis failed after a committed tool receipt; stop without replaying the provider action or its effects"
                                .to_string()
                        },
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                        expected_graph_revision: None,
                    }
                } else if let Some(attempt) = protocol_attempt {
                    let kind = provider_protocol_intervention_kind(attempt);
                    harness_contract::goal::RuntimeIntervention {
                        goal_id: goal_id.clone(),
                        kind,
                        reason: if attempt <= PROVIDER_PROTOCOL_RECOVERY_BUDGET {
                            if tool_exposure_miss {
                                "provider selected a known healthy deferred tool; Runtime activated its schema and will retry exactly once under the revised exposure lease"
                                    .to_string()
                            } else {
                                "provider emitted an invalid tool protocol frame or requested an unknown, unavailable, or unauthorized tool; retry exactly once without treating protocol bytes as assistant text"
                                    .to_string()
                            }
                        } else {
                            "provider repeated an invalid or unexposed tool protocol action after the single governed retry"
                                .to_string()
                        },
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                        expected_graph_revision: None,
                    }
                } else {
                    propose_intervention_after_observation(
                        &self.services,
                        &goal_id,
                        observation.clone(),
                    )
                    .map_err(|reason| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason,
                    })?
                };
                let (next, replan_reason, next_model_instruction) = {
                    let mut state = self.state.lock().await;
                    let (node, next_model_instruction) = match intervention.kind {
                        RuntimeInterventionKind::Synthesize => {
                            if clean_terminal_synthesis {
                                state.clean_terminal_retry_attempted = true;
                            }
                            state.clean_terminal_synthesis_attempted = true;
                            state.clean_terminal_synthesis_next = true;
                            (
                                dynamic_node(
                                    ticket,
                                    iteration,
                                    "provider-protocol-clean-synthesis-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                ),
                                None,
                            )
                        }
                        RuntimeInterventionKind::Block => {
                            state.terminal_override = Some((
                                GoalCompletion::Partial,
                                format!(
                                    "Execution blocked after repeated provider failures: {}\n\nExact provider validation evidence: {}\n\nCommitted goal and evidence state were retained. Provide a new provider, constraint, or explicit replan to continue.",
                                    intervention.reason,
                                    protocol_failure_detail.as_deref().unwrap_or(&reason),
                                ),
                            ));
                            let mut node = dynamic_node(
                                ticket,
                                iteration,
                                "provider-block-synthesize",
                                ExecutionNodeKind::Synthesize,
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                "inline_model",
                            );
                            node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                            (node, None)
                        }
                        RuntimeInterventionKind::Switch => {
                            let instruction = "Runtime recovery strategy (mandatory): the prior provider path failed repeatedly. Reassess the objective from already committed goal/evidence, avoid repeating the failed transport path, reduce the next step to the smallest independently verifiable action, and state any remaining blocker explicitly.".to_string();
                            state.content.push_str("\n\n");
                            state.content.push_str(&instruction);
                            state.content.push('\n');
                            (
                                dynamic_node(
                                    ticket,
                                    iteration,
                                    "provider-recovery-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                ),
                                Some(instruction),
                            )
                        }
                        RuntimeInterventionKind::Replan => {
                            let instruction = if protocol_failure {
                                let detail = protocol_failure_detail
                                    .as_deref()
                                    .map(|detail| format!(" Exact validation evidence: {detail}"))
                                    .unwrap_or_default();
                                if tool_exposure_miss {
                                    format!(
                                        "Runtime tool-exposure recovery (single attempt): the prior response selected a known deferred tool and Runtime has now activated its canonical native schema.{detail} Continue the same objective by invoking that exposed schema with valid arguments, or return a normal visible final answer when no call is needed."
                                    )
                                } else {
                                    format!(
                                        "Runtime provider-protocol recovery (single attempt): the prior response used an invalid tool-call frame or requested an unknown, unavailable, or unauthorized tool.{detail} Retry from committed evidence using only an exposed native tool with valid arguments, or return a normal visible final answer. Never print tool-protocol markup as prose."
                                    )
                                }
                            } else {
                                "Runtime recovery directive: a provider step failed. Replan from the committed goal and evidence before retrying; do not assume uncommitted output is valid."
                                    .to_string()
                            };
                            state.content.push_str("\n\n");
                            state.content.push_str(&instruction);
                            state.content.push('\n');
                            (
                                dynamic_node(
                                    ticket,
                                    iteration,
                                    "provider-replan-model",
                                    ExecutionNodeKind::InlineModel,
                                    "inline_model",
                                    "inline_model",
                                ),
                                Some(instruction),
                            )
                        }
                        _ => (
                            dynamic_node(
                                ticket,
                                iteration,
                                "provider-recovery-model",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            ),
                            None,
                        ),
                    };
                    (
                        node,
                        format!(
                            "Runner applied provider failure intervention: {:?}",
                            intervention.kind
                        ),
                        next_model_instruction,
                    )
                };
                if let Some(instruction) = next_model_instruction {
                    let mut item = ContextItem::new(
                        format!("runtime-provider-recovery:{}", ticket.node_id),
                        ContextSourceKind::Task,
                        ContextRole::Instruction,
                        instruction,
                    );
                    item.authority = ContextAuthority::System;
                    item.visibility = ContextVisibility::Private;
                    item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                    self.runtime.lock().await.push_next_model_context_item(item);
                }
                let mut outcome = NodeExecutionOutcome::new(completed_result(
                    Some(format!(
                        "{}:provider-failure:{}",
                        ticket.graph_id,
                        sha256_digest(&reason)
                    )),
                    ExecutionUsage::default(),
                ));
                outcome.domain_events.push(
                    self.services
                        .goal_store()
                        .observation_event(
                            &observation,
                            format!("{}:provider-failure-observation", ticket.idempotency_key),
                        )
                        .map_err(|reason| NodeExecutorError::Poll {
                            node_id: ticket.node_id.clone(),
                            reason,
                        })?,
                );
                outcome.domain_events.push(
                    self.services
                        .goal_store()
                        .intervention_event(
                            &intervention,
                            std::slice::from_ref(&observation),
                            format!("{}:provider-failure-intervention", ticket.idempotency_key),
                        )
                        .map_err(|reason| NodeExecutorError::Poll {
                            node_id: ticket.node_id.clone(),
                            reason,
                        })?,
                );
                outcome.replan = Some(ExecutionGraphReplan {
                    nodes: vec![next.clone()],
                    edges: dynamic_edges(&ticket.node_id, &[next]),
                    reason: replan_reason,
                });
                Ok(outcome)
            }
        }
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        tracing::debug!(node_id = %ticket.node_id, "publishing committed model transcript");
        let (
            messages,
            required_control_plane_team_count,
            missing_control_plane_proposal,
            session_id,
            turn_id,
        ) = {
            let mut state = self.state.lock().await;
            (
                state
                    .pending_transcript
                    .remove(&ticket.node_id)
                    .unwrap_or_default(),
                state.pending_root_control_plane_requirement.take(),
                state.pending_root_control_plane_receipt.take(),
                state.session_id.clone(),
                state.turn_id.clone(),
            )
        };
        tracing::debug!(node_id = %ticket.node_id, message_count = messages.len(), "model transcript staged for publication");
        self.runtime
            .lock()
            .await
            .session_mut_async()
            .await
            .extend_messages(messages);
        if let Some(required_team_count) = required_control_plane_team_count {
            self.services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: format!("session:{session_id}"),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "runtime.control_plane.required".to_string(),
                    status: Some("waiting".to_string()),
                    actor: Some("conversation_runtime.root_control_plane".to_string()),
                    refs: vec![
                        crate::RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: ticket.graph_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "execution_node".to_string(),
                            id: ticket.node_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: turn_id.clone(),
                        },
                    ],
                    payload: serde_json::json!({
                        "required_team_count": required_team_count,
                        "required_tool_choice": harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
                        "program_admitted": false,
                    }),
                })
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("persist root control-plane requirement: {error}"),
                })?;
        }
        if let Some(reason) = missing_control_plane_proposal {
            self.services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: format!("session:{session_id}"),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "runtime.control_plane.missing_proposal".to_string(),
                    status: Some("blocked".to_string()),
                    actor: Some("conversation_runtime.root_control_plane".to_string()),
                    refs: vec![
                        crate::RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: ticket.graph_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "execution_node".to_string(),
                            id: ticket.node_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: turn_id,
                        },
                    ],
                    payload: serde_json::json!({
                        "reason": reason,
                        "repair_attempts": 1_u8,
                        "program_admitted": false,
                    }),
                })
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("persist missing root control-plane proposal receipt: {error}"),
                })?;
        }
        tracing::debug!(node_id = %ticket.node_id, "committed model transcript published");
        Ok(())
    }
}

#[async_trait]
impl<C, T> ScopedNodeBackend for TurnToolBatchBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
            bus.emit(CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::CallingTool,
                detail: Some("executing tool batch".to_string()),
            });
        }
        let (prompter, iteration, session_id, model_lease, delegated_agent_role) = {
            let state = self.state.lock().await;
            (
                state.prompter.clone(),
                state.iterations,
                state.session_id.clone(),
                state.model.clone(),
                state.execution_role.is_delegated_leaf(),
            )
        };
        let (calls, continue_with_tool_batch) =
            decode_tool_batch(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("tool batch persistent payload is invalid: {error}"),
            })?;
        if calls.is_empty() {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "tool batch has no model-requested calls".to_string(),
            });
        }
        let precompleted = {
            let state = self.state.lock().await;
            calls
                .iter()
                .filter_map(|call| {
                    state
                        .early_tool_receipts
                        .get(&call.id)
                        .cloned()
                        .map(|receipt| (call.id.clone(), receipt))
                })
                .collect::<BTreeMap<_, _>>()
        };
        let governed_host = if delegated_agent_role {
            None
        } else {
            self.services.tool_execution_host()
        };
        // Only the ToolHost graph path prepares and compiles here. Delegated
        // agents use execute_tool_batch_step(), which owns the same work for
        // that path; doing both would duplicate policy revision and approval.
        let governed_bundle: Option<(
            std::collections::HashMap<String, harness_contract::tool::ToolExecutionAuthorization>,
            std::collections::HashMap<String, harness_contract::tool::GovernedToolInvocation>,
            std::collections::HashMap<String, harness_contract::policy::CapabilityAssessment>,
            crate::execution_core::RuntimeExecutionDecision,
            Result<crate::GovernedToolCompilation, crate::GovernedToolCompileError>,
        )> = if governed_host.is_some() {
            Some({
                let runtime = self.runtime.lock().await;
                let tool_exec = Arc::clone(runtime.tool_executor());
                let default_timeout = runtime
                    .tool_timeout()
                    .unwrap_or_else(|| std::time::Duration::from_secs(60));
                let requests = calls
                    .iter()
                    .map(|call| crate::tool_dispatch::ToolRequest {
                        tool_use_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        input: call.input.clone(),
                        depends_on: call.depends_on.clone(),
                    })
                    .collect::<Vec<_>>();
                let prepared = tool_exec.prepare_governed_invocations(&requests);
                let prepared_by_id = prepared
                    .iter()
                    .map(|invocation| (invocation.invocation_id.clone(), invocation.clone()))
                    .collect::<std::collections::HashMap<_, _>>();
                // A permissive session profile is an authorization ceiling,
                // never permission to contradict a read-only user objective.
                // Keep this decision at the governed effect boundary so it
                // applies to every mutating tool descriptor, including tools
                // added after this host is compiled; no role, template, or
                // tool-name branch is involved.
                let task_requires_workspace_write = runtime
                    .active_turn_strategy()
                    .is_none_or(|strategy| strategy.decision.strategy.understanding.requires_write);
                let governed_compilation = crate::GovernedToolCompiler.compile_partial(
                    self.services.workspace_root(),
                    &requests,
                    |name, input| {
                        prepared.iter().find_map(|invocation| {
                            (invocation.intent.tool_name == name
                                && invocation.intent.normalized_input == *input)
                                .then(|| {
                                    (
                                        invocation.effect.clone(),
                                        invocation.catalog_revision,
                                        invocation.descriptor_set_hash.clone(),
                                    )
                                })
                        })
                    },
                );
                let execution_decision = match governed_compilation
                    .as_ref()
                    .ok()
                    .and_then(|compilation| compilation.plan.as_ref())
                {
                    Some(plan) => {
                        runtime.retarget_active_turn_strategy_for_governed_plan(plan, &calls)
                    }
                    None => runtime
                        .active_turn_strategy()
                        .map(|strategy| strategy.decision)
                        .ok_or_else(|| {
                            RuntimeError::new("tool batch has no admitted strategy decision")
                        }),
                }
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: error.to_string(),
                })?;
                let mut auths = std::collections::HashMap::new();
                let mut gaps = std::collections::HashMap::new();
                for call in &calls {
                    if call.name == "runtime_orchestrate" {
                        tracing::debug!(
                            call_id = %call.id,
                            prepared = %prepared.iter().any(|invocation| invocation.invocation_id == call.id),
                            "runtime_orchestrate authorization preparation"
                        );
                    }
                    if let Some(invocation) = prepared
                        .iter()
                        .find(|invocation| invocation.invocation_id == call.id)
                    {
                        let descriptor = invocation.effect.clone();
                        if !task_requires_workspace_write
                            && descriptor.required_permission
                                != harness_contract::tool::ToolPermissionMode::ReadOnly
                        {
                            gaps.insert(
                                call.id.clone(),
                                synthetic_capability_gap(
                                    &descriptor,
                                    runtime.active_permission_mode(),
                                    "the active user task is read-only; Runtime rejects mutating tool effects even under a full-trust session profile".to_string(),
                                ),
                            );
                            continue;
                        }
                        let request_id = format!("{}:{}:{}", session_id, call.id, ticket.attempt);
                        match runtime
                            .negotiate_tool_authorization(
                                &descriptor,
                                &call.input,
                                request_id,
                                crate::PermissionContext::default(),
                                default_timeout.as_secs(),
                                &prompter,
                            )
                            .await
                        {
                            Ok(crate::conversation::ToolAuthorizationDecision::Authorized(
                                decision,
                            )) => {
                                if call.name == "runtime_orchestrate" {
                                    tracing::debug!(
                                        call_id = %call.id,
                                        lease_ceiling = ?decision.authorization.authorization_lease.ceiling,
                                        "runtime_orchestrate authorization lease issued"
                                    );
                                }
                                auths.insert(call.id.clone(), decision.authorization);
                            }
                            Ok(crate::conversation::ToolAuthorizationDecision::Gap {
                                assessment,
                                ..
                            }) => {
                                gaps.insert(call.id.clone(), assessment);
                            }
                            Err(error) => {
                                gaps.insert(
                                    call.id.clone(),
                                    synthetic_capability_gap(
                                        &descriptor,
                                        runtime.active_permission_mode(),
                                        error.to_string(),
                                    ),
                                );
                            }
                        }
                    }
                }
                (
                    auths,
                    prepared_by_id,
                    gaps,
                    execution_decision,
                    governed_compilation,
                )
            })
        } else {
            None
        };
        let (
            result,
            orchestration_terminal_summary,
            _prepared_tool_invocations,
            successful_observed_evidence,
        ) = if let Some(host) = governed_host {
            let (
                tool_authorizations,
                prepared_tool_invocations,
                capability_gaps,
                execution_decision,
                governed_compilation,
            ) = governed_bundle.ok_or_else(|| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "governed ToolHost is missing its compiled execution bundle".to_string(),
            })?;
            let (event_bus, memory_context, execution_policy) = {
                let runtime = self.runtime.lock().await;
                (
                    runtime.cowd_bus().cloned(),
                    runtime.memory_turn_context(),
                    runtime
                        .permission_policy()
                        .execution_policy_control()
                        .snapshot(),
                )
            };
            let governed = execute_governed_runtime_tool_batch(
                Arc::clone(host),
                event_bus,
                &calls,
                &session_id,
                execution_policy.sandbox_posture,
                execution_policy.revision,
                Some(&memory_context),
                model_lease.as_deref(),
                ticket,
                u64::try_from(iteration).unwrap_or(u64::MAX).max(1),
                &tool_authorizations,
                &capability_gaps,
                &prepared_tool_invocations,
                governed_compilation,
                &execution_decision,
                self.services.tool_execution_plane(),
                self.services.commit_service(),
                &precompleted,
            )
            .await;
            let GovernedToolBatchResult {
                messages: governed_messages,
                invocations,
                observed_evidence,
                max_concurrency_observed,
                parallel_batches,
            } = governed;
            // Preserve the full, durable orchestration receipt long enough to
            // derive a verified Program terminal. The model-facing compactor
            // deliberately replaces large JSON with an elided summary, so
            // parsing only `messages` below used to turn a completed Team
            // Program into an apparent missing control-plane proposal.
            let orchestration_terminal_summary = completed_orchestration_terminal_summary(
                &calls,
                &governed_messages,
                self.services.workspace_root(),
                true,
            );
            // Graph scheduling executes outside the legacy adapter. Before
            // the next model node sees the result, route its raw output
            // through the same durable evidence and context-ledger path used
            // by normal conversation tool calls.
            let messages = compact_governed_tool_messages(
                &self.runtime,
                &calls,
                governed_messages,
                &invocations,
            )
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("tool evidence durability barrier failed: {error}"),
            })?;
            (
                crate::conversation::ToolBatchStepResult {
                    failed: messages
                        .iter()
                        .flat_map(|message| &message.blocks)
                        .filter(|block| {
                            matches!(block, ContentBlock::ToolResult { is_error: true, .. })
                        })
                        .count(),
                    messages,
                    max_concurrency_observed,
                    parallel_batches,
                },
                orchestration_terminal_summary,
                prepared_tool_invocations,
                observed_evidence,
            )
        } else {
            let mut runtime = self.runtime.lock().await;
            let transcript_len = runtime.session_head().await.message_count;
            let requests = calls
                .iter()
                .map(|call| crate::tool_dispatch::ToolRequest {
                    tool_use_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    input: call.input.clone(),
                    depends_on: call.depends_on.clone(),
                })
                .collect::<Vec<_>>();
            let prepared_tool_invocations = runtime
                .tool_executor()
                .prepare_governed_invocations(&requests)
                .into_iter()
                .map(|invocation| (invocation.invocation_id.clone(), invocation))
                .collect::<std::collections::HashMap<_, _>>();
            let result = runtime
                .execute_tool_batch_step(&calls, &prompter, iteration)
                .await;
            let observed_evidence = runtime.tool_executor().observed_evidence_snapshot();
            // The legacy conversation engine writes tool messages eagerly. Roll them
            // back until the graph transition commits; after_commit publishes them.
            runtime
                .session_mut_async()
                .await
                .truncate_messages(transcript_len);
            drop(runtime);
            let result = result.map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
            let orchestration_terminal_summary: Option<String> = None;
            (
                result,
                orchestration_terminal_summary,
                prepared_tool_invocations,
                observed_evidence,
            )
        };
        self.runtime
            .lock()
            .await
            .consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::AfterToolResult);
        {
            let mut state = self.state.lock().await;
            for call in &calls {
                state.early_tool_receipts.remove(&call.id);
            }
        }
        let tool_calls = result.messages.len() as u64;
        let failed = result.failed;
        let failed_tools = failed_tool_names(&result.messages);
        let retryable_collaboration_diagnostic =
            retryable_collaboration_compile_diagnostic(&result.messages);
        let successful_call_ids = successful_tool_call_ids(&result.messages);
        let action_fingerprint = tool_batch_fingerprint(&calls);
        let goal_id = self.state.lock().await.goal_id.clone();
        let prior_observations =
            self.services
                .goal_store()
                .observations(&goal_id)
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
        let repeated_success = prior_observations.iter().any(|observation| {
            observation.kind == RuntimeObservationKind::ToolProgress
                && !observation.failed()
                && observation.has_verified_gain()
                && observation.fingerprint == action_fingerprint
        });
        let coverage_keys = tool_batch_coverage_keys(&calls);
        let scope_keys = tool_batch_scope_keys(&calls);
        let prior_observed_evidence = prior_observations
            .iter()
            .filter(|observation| observation.kind == RuntimeObservationKind::ToolProgress)
            .flat_map(|observation| observation.observed_evidence.iter().cloned())
            .collect::<Vec<_>>();
        let new_observed_fingerprints =
            crate::path_identity::fresh_novel_observed_evidence_fingerprints(
                &prior_observed_evidence,
                &successful_observed_evidence,
            );
        let successful_resource_scope_keys = successful_observed_evidence
            .iter()
            .map(crate::path_identity::observed_scope_key)
            .collect::<BTreeSet<_>>();
        let successful_focus_resource_scope_keys = successful_resource_scope_keys.clone();
        let successful_workspace_write_scope_keys = successful_observed_evidence
            .iter()
            .filter(|evidence| {
                matches!(
                    &evidence.target,
                    harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                        if scope.access_mode
                            == harness_contract::context::WorkspaceAccessMode::Write
                )
            })
            .map(crate::path_identity::observed_scope_key)
            .collect::<BTreeSet<_>>();
        let covered_before = prior_observations
            .iter()
            .filter(|observation| observation.kind == RuntimeObservationKind::ToolProgress)
            .flat_map(|observation| observation.evidence_refs.iter())
            .filter_map(|reference| reference.strip_prefix("tool_coverage:"))
            .collect::<BTreeSet<_>>();
        let scopes_covered_before = prior_observations
            .iter()
            .filter(|observation| observation.kind == RuntimeObservationKind::ToolProgress)
            .flat_map(|observation| observation.evidence_refs.iter())
            .filter_map(|reference| reference.strip_prefix("tool_scope:"))
            .collect::<BTreeSet<_>>();
        let resource_scopes_covered_before = prior_observed_evidence
            .iter()
            .map(crate::path_identity::observed_scope_key)
            .collect::<BTreeSet<_>>();
        let focus_resource_scopes_covered_before = resource_scopes_covered_before.clone();
        // Source verification is sequence-sensitive: a read only proves the
        // post-write state when a committed write receipt already exists from
        // an earlier batch. A same-wave read/write pair is deliberately not
        // accepted because the scheduler may execute independent calls in
        // either order.
        let newly_covered = coverage_keys
            .iter()
            .filter(|coverage| !covered_before.contains(coverage.as_str()))
            .count();
        let newly_scoped = scope_keys
            .iter()
            .filter(|scope| !scopes_covered_before.contains(scope.as_str()))
            .count();
        let coverage_novelty_bp = if coverage_keys.is_empty() {
            5_000_u16
        } else if newly_covered == 0 {
            0
        } else {
            u16::try_from(
                newly_covered
                    .saturating_mul(10_000)
                    .saturating_div(coverage_keys.len()),
            )
            .unwrap_or(10_000)
        };
        let (
            bounded_evidence_role,
            novelty_target_bp,
            focus_acceptance_scopes,
            has_retained_focus_terminal_candidate,
        ) = {
            let state = self.state.lock().await;
            (
                state.bounded_evidence_role,
                state.focus_novelty_target_bp,
                state.focus_acceptance_scopes.clone(),
                state.pending_focus_terminal_candidate.is_some(),
            )
        };
        let mut satisfied_focus_acceptance_scope_keys = typed_satisfied_focus_acceptance_scope_keys(
            &focus_acceptance_scopes,
            &successful_observed_evidence,
            &prior_observed_evidence,
            self.services.path_identity_resolver(),
        );
        // A deterministic Focus prefetch is compiled by Runtime and reaches
        // this point only after its governed ToolBatch produced a successful
        // tool-result receipt. In-process delegated executors can retain that
        // receipt before their typed evidence snapshot is visible to this
        // host; recognize the exact Runtime-authored call here so a completed
        // read is never spuriously re-requested from the provider. This is
        // deliberately limited to exact scopes in the immutable role contract.
        satisfied_focus_acceptance_scope_keys.extend(successful_runtime_focus_scope_keys(
            &calls,
            &successful_call_ids,
            &focus_acceptance_scopes,
        ));
        let verified_focus_acceptance_scope_keys = satisfied_focus_acceptance_scope_keys
            .iter()
            .filter(|scope| {
                scope.starts_with("verify_after_write:")
                    || scope.starts_with("verify_upstream_change:")
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let low_novelty = failed == 0
            && !coverage_keys.is_empty()
            && novelty_target_bp > 0
            && coverage_novelty_bp < novelty_target_bp;
        let evidence_saturated =
            failed == 0 && !coverage_keys.is_empty() && newly_covered == 0 && newly_scoped == 0;
        // File-level receipts may be individually new while adding no new
        // responsibility zone. A delegated role has a finite evidence
        // contract, so repeated work inside already-covered zones is a
        // saturation signal. Main turns retain their normal open exploration.
        let scope_saturated =
            bounded_evidence_role && failed == 0 && !scope_keys.is_empty() && newly_scoped == 0;
        let mut automatic_focus_verification = None;
        let mut state = self.state.lock().await;
        // A successful Team-admission call has already created the durable
        // Program authority.  Keep only this turn-local "do not submit the
        // same admission again" marker; completion is still read exclusively
        // from the typed Program terminal projection below.
        state.collaboration_started |= calls
            .iter()
            .any(|call| successful_call_ids.contains(&call.id) && is_team_orchestration_call(call))
            || has_admitted_program_receipt(&result.messages);
        let root_control_plane_required = !state.execution_role.is_delegated_leaf()
            && !state.collaboration_started
            && !has_completed_program_terminal(&state.tool_results)
            && !has_admitted_program_receipt(&state.tool_results)
            && state
                .task_understanding
                .as_ref()
                .is_some_and(|understanding| understanding.required_team_count > 0);
        if root_control_plane_required {
            let next_phase = root_control_plane_phase_after_tool_batch(
                state.root_control_plane_phase,
                &calls,
                &successful_call_ids,
            );
            if next_phase != state.root_control_plane_phase {
                // Do not expose this transition to another model node until
                // the ToolBatch itself is durable. `after_commit` publishes
                // the matching Session event and then advances live state.
                state.pending_root_control_plane_phase = Some(next_phase);
            }
            if let Some(diagnostic) = retryable_collaboration_diagnostic.as_deref() {
                // The provider may return prose after a tool-result turn even
                // when it correctly recognized a retryable compiler receipt.
                // Make the repair an explicit, bounded next-step contract so
                // the control plane cannot terminate between diagnosis and
                // the corrected semantic submissions permitted for this root
                // admission.
                if state.team_orchestration_requests < ROOT_CONTROL_PLANE_REPAIR_BUDGET {
                    state.team_orchestration_requests =
                        state.team_orchestration_requests.saturating_add(1);
                    state.force_tool_allowlist_next_model = Some(BTreeSet::from([
                        harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID
                            .to_string(),
                    ]));
                    let reason = format!(
                        "Runtime requires a corrected semantic collaboration submission now (bounded attempt {}/{}). The retryable Runtime diagnostic is `{diagnostic}`. Call submit_collaboration_decision in this next response with a complete replacement decision; repair exactly the diagnostic's field paths and allowed repairs, preserve valid workstreams, retain the current decision_id because no Program was admitted, and do not write a conclusion or invoke any other tool.",
                        state.team_orchestration_requests,
                        ROOT_CONTROL_PLANE_REPAIR_BUDGET,
                    );
                    let mut item = ContextItem::new(
                        format!("runtime-root-collaboration-repair:{}", ticket.node_id),
                        ContextSourceKind::Task,
                        ContextRole::Instruction,
                        reason,
                    );
                    item.authority = ContextAuthority::System;
                    item.visibility = ContextVisibility::Private;
                    item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                    state.pending_next_model_context.push(item);
                }
            }
        }
        state.max_tool_concurrency_observed = state
            .max_tool_concurrency_observed
            .max(result.max_concurrency_observed);
        state.parallel_tool_batches = state
            .parallel_tool_batches
            .saturating_add(result.parallel_batches);
        let focus_acceptance_pending_scopes = focus_acceptance_scopes
            .iter()
            .filter(|required_scope| {
                !satisfied_focus_acceptance_scope_keys.contains(*required_scope)
            })
            .cloned()
            .collect::<Vec<_>>();
        // Focus acceptance is scope based, not batch based. A parallel batch
        // may contain a failed discovery call and a successful authoritative
        // fetch. The failed sibling remains visible as evidence and risk, but
        // it must not erase the successful receipt that closes the bounded
        // role's required scope.
        let focus_acceptance_met = focus_acceptance_is_met(
            bounded_evidence_role,
            &focus_acceptance_scopes,
            &focus_acceptance_pending_scopes,
        );
        let focus_acceptance_pending =
            bounded_evidence_role && !focus_acceptance_scopes.is_empty() && !focus_acceptance_met;
        if bounded_evidence_role {
            tracing::debug!(
                execution_id = %ticket.graph_id,
                node_id = %ticket.node_id,
                required_scopes = ?focus_acceptance_scopes,
                current_observed = successful_observed_evidence.len(),
                prior_observed = prior_observed_evidence.len(),
                satisfied_scopes = ?satisfied_focus_acceptance_scope_keys,
                pending_scopes = ?focus_acceptance_pending_scopes,
                retained_terminal_candidate = has_retained_focus_terminal_candidate,
                "delegated Focus acceptance evaluated from typed tool receipts"
            );
        }
        state.focus_acceptance_pending_scopes = focus_acceptance_pending_scopes.clone();
        state
            .focus_observed_resource_scopes
            .extend(successful_focus_resource_scope_keys.iter().cloned());
        state
            .focus_observed_resource_scopes
            .extend(verified_focus_acceptance_scope_keys.iter().cloned());
        for evidence in &successful_observed_evidence {
            if !state.focus_observed_evidence.contains(evidence) {
                state.focus_observed_evidence.push(evidence.clone());
            }
        }
        if let Some(instruction) =
            upstream_verification_completion_instruction(&verified_focus_acceptance_scope_keys)
        {
            // Runtime, rather than the provider, compiled and executed the
            // reviewer's exact-path reads. Tell the following synthesis
            // request what those retained receipts mean. Without this
            // authority-preserving hand-off, a text-only synthesis can
            // incorrectly claim that independent verification was
            // impossible merely because acquisition is already complete.
            let mut item = ContextItem::new(
                format!("runtime-upstream-verification-complete:{}", ticket.node_id),
                ContextSourceKind::ToolTrace,
                ContextRole::Instruction,
                instruction,
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            item.evidence = successful_call_ids
                .iter()
                .map(|call_id| format!("tool_call:{call_id}"))
                .collect();
            state.pending_next_model_context.push(item);
            // Evidence acquisition is complete and the next Reviewer
            // request only reduces authoritative receipts into bounded JSON.
            state.force_reasoning_effort_next_model = Some("none".to_string());
        }
        let pending_write_paths = state
            .focus_acceptance_pending_scopes
            .iter()
            .filter_map(|scope| scope.strip_prefix("write:"))
            .map(str::to_string)
            .collect::<Vec<_>>();
        let successful_write_in_batch = !successful_workspace_write_scope_keys.is_empty();
        state.committed_workspace_write_observed |= successful_write_in_batch;
        state
            .committed_workspace_write_scopes
            .extend(successful_workspace_write_scope_keys);
        for evidence in &successful_observed_evidence {
            if !state
                .committed_workspace_observed_evidence
                .contains(evidence)
            {
                state
                    .committed_workspace_observed_evidence
                    .push(evidence.clone());
            }
        }
        if state.bounded_evidence_role
            && !pending_write_paths.is_empty()
            && !successful_write_in_batch
            && pending_write_paths.iter().all(|path| {
                state
                    .focus_observed_resource_scopes
                    .contains(&format!("read:{path}"))
            })
        {
            // The provider has already supplied the necessary read evidence.
            // The next request should author the mutation, not spend another
            // turn rediscovering the same files.
            state.force_tool_allowlist_next_model = Some(required_mutation_tool_allowlist());
        }
        if state.bounded_evidence_role
            && successful_write_in_batch
            && !state.focus_acceptance_pending_scopes.is_empty()
            && state
                .focus_acceptance_pending_scopes
                .iter()
                .all(|scope| scope.starts_with("verify_after_write:"))
        {
            // `state.iterations` names the model step that compiled the
            // currently executing write ToolBatch. A Runtime-authored
            // follow-up must use the next namespace or Runner will correctly
            // reject it as a duplicate node replan.
            let followup_iteration = state.iterations.saturating_add(1);
            let concrete_verification_scopes = concrete_focus_verification_scopes(
                &state.focus_acceptance_pending_scopes,
                &state.focus_observed_evidence,
                self.services.path_identity_resolver(),
            );
            automatic_focus_verification = focus_verification_tool_calls(
                &concrete_verification_scopes,
                followup_iteration,
                self.services.workspace_root(),
            )
            .map(|calls| (state.session_id.clone(), followup_iteration, calls));
        }
        state
            .pending_transcript
            .insert(ticket.node_id.clone(), result.messages.clone());
        state.successful_tool_calls = state
            .successful_tool_calls
            .saturating_add(successful_call_ids.len());
        state.tool_receipts_observed = state
            .tool_receipts_observed
            .saturating_add(result.messages.len());
        if repeated_success {
            state.duplicate_tool_calls = state.duplicate_tool_calls.saturating_add(tool_calls);
        }
        if failed > 0 {
            state.consecutive_tool_failure_batches =
                state.consecutive_tool_failure_batches.saturating_add(1);
        } else {
            state.consecutive_tool_failure_batches = 0;
        }
        if failed == 0 && (low_novelty || scope_saturated || evidence_saturated) {
            state.consecutive_low_novelty_batches =
                state.consecutive_low_novelty_batches.saturating_add(1);
        } else {
            state.consecutive_low_novelty_batches = 0;
        }
        let repeated_local_failures = state.consecutive_tool_failure_batches >= 2;
        let repeated_evidence_saturation = state.consecutive_low_novelty_batches
            >= evidence_saturation_limit(bounded_evidence_role);
        let focus_synthesis_ready = should_force_focus_synthesis(
            focus_acceptance_met,
            &focus_acceptance_scopes,
            repeated_evidence_saturation,
            has_retained_focus_terminal_candidate,
        );
        let successful_write_observed = write_obligation_satisfied(
            state.required_write_for_completion,
            &state.required_workspace_write_scopes,
            &state.committed_workspace_observed_evidence,
            state.committed_workspace_write_observed || state.collaboration_committed_write,
            self.services.path_identity_resolver(),
        );
        let required_write_recovery = should_recover_missing_required_write(
            state.required_write_for_completion,
            bounded_evidence_role,
            repeated_evidence_saturation,
            &state.write_attempt_paths,
            successful_write_observed,
            state.required_write_replans,
        );
        let authorized_write_scopes = state
            .evaluation_resource_scopes
            .iter()
            .filter(|scope| scope.starts_with("write:"))
            .cloned()
            .collect::<Vec<_>>();
        if required_write_recovery {
            state.required_write_replans = state.required_write_replans.saturating_add(1);
            state.consecutive_low_novelty_batches = 0;
        }
        let has_successful_tool_evidence = state.successful_tool_calls > 0;
        let newly_completed_program_team_ids = completed_program_team_ids(&result.messages);
        let completed_root_team_this_batch = root_team_terminal_requires_text_only(
            state.execution_role.is_delegated_leaf(),
            state
                .task_understanding
                .as_ref()
                .map_or(0, |understanding| understanding.required_team_count),
            &newly_completed_program_team_ids,
        );
        state.tool_results.extend(result.messages);
        if completed_root_team_this_batch {
            // `submit_collaboration_decision` synchronously returns only once
            // its Team terminal is verified. The parent model's remaining job
            // is presentation, not another graph mutation or workspace call.
            state.force_text_only_next_model = true;
            state.force_reasoning_effort_next_model = Some("none".to_string());
        }
        if focus_synthesis_ready {
            if let Some(item) = focus_synthesis_evidence_context_item(
                &ticket.node_id,
                &calls,
                &state.tool_results,
                &state.focus_required_output_fields,
            ) {
                state.pending_next_model_context.push(item);
            }
            // Evidence acquisition has completed. The next request is a
            // deterministic contract reduction, so it should not spend another
            // deep reasoning pass or reopen tool exploration.
            state.force_text_only_next_model = true;
            state.force_reasoning_effort_next_model = Some("none".to_string());
        }
        state.last_verified_progress = !new_observed_fingerprints.is_empty();
        let verified_evidence_refs = if state.last_verified_progress {
            new_observed_fingerprints
                .iter()
                .map(|fingerprint| format!("tool_observation:{fingerprint}"))
                .chain(
                    successful_focus_resource_scope_keys
                        .iter()
                        .filter(|scope| {
                            !focus_resource_scopes_covered_before.contains(scope.as_str())
                        })
                        .map(|scope| format!("focus_resource_scope:{scope}")),
                )
                .chain(
                    satisfied_focus_acceptance_scope_keys
                        .iter()
                        .map(|scope| format!("focus_resource_scope:{scope}")),
                )
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let observation_fingerprint = if failed_tools.is_empty() {
            action_fingerprint
        } else {
            format!("tool_failure:{}", failed_tools.join(","))
        };
        let mut observation = runtime_observation(
            runtime_observation_identity(&self.services, &state, ticket),
            RuntimeObservationKind::ToolProgress,
            "runtime.tool_batch",
            u64::try_from(state.iterations).unwrap_or(u64::MAX),
            if failed_tools.is_empty() && repeated_success {
                format!(
                    "tool batch reused an already-completed action calls={tool_calls}; retained receipt must be used before another identical request"
                )
            } else if failed_tools.is_empty() && (scope_saturated || evidence_saturated) {
                format!(
                    "tool batch completed calls={tool_calls} but added no new evidence coverage; retain receipts and converge"
                )
            } else if failed_tools.is_empty() {
                format!(
                    "tool batch completed calls={tool_calls} failed=0 coverage_new={newly_covered}/{} scope_new={newly_scoped}/{}",
                    coverage_keys.len(),
                    scope_keys.len()
                )
            } else {
                format!(
                    "tool batch completed calls={tool_calls} failed={failed} failed_tools={}",
                    failed_tools.join(",")
                )
            },
            observation_fingerprint.clone(),
            if failed == 0 {
                ObservationResultClass::Succeeded
            } else if failed < calls.len() {
                ObservationResultClass::Partial
            } else {
                ObservationResultClass::Failed
            },
        );
        observation.failure_class = (failed > 0).then_some(ObservationFailureClass::Tool);
        observation.observed_evidence = successful_observed_evidence.clone();
        observation.evidence_refs = calls
            .iter()
            .map(|call| format!("tool_call:{}", call.id))
            .chain(
                coverage_keys
                    .iter()
                    .map(|coverage| format!("tool_coverage:{coverage}")),
            )
            .chain(scope_keys.iter().map(|scope| format!("tool_scope:{scope}")))
            .chain(
                successful_resource_scope_keys
                    .iter()
                    .map(|scope| format!("tool_resource_scope:{scope}")),
            )
            .chain(
                successful_focus_resource_scope_keys
                    .iter()
                    .map(|scope| format!("focus_resource_scope:{scope}")),
            )
            .chain(
                satisfied_focus_acceptance_scope_keys
                    .iter()
                    .map(|scope| format!("focus_resource_scope:{scope}")),
            )
            .collect();
        observation.evidence_delta.added = verified_evidence_refs.clone();
        observation.effect_deltas.push(EffectDelta {
            effect_id: format!("tool-batch:{observation_fingerprint}"),
            terminal_class: if failed == 0 {
                EffectTerminalClass::Completed
            } else {
                EffectTerminalClass::Failed
            },
            idempotency_ref: ticket.idempotency_key.clone(),
        });
        observation.cost_delta.tool_calls = tool_calls;
        observation.information_gain = InformationGain {
            distinguishing_evidence_refs: verified_evidence_refs,
            resolved_unknown_refs: satisfied_focus_acceptance_scope_keys
                .iter()
                .map(|scope| format!("focus-acceptance:{scope}"))
                .collect(),
            provenance: if state.last_verified_progress {
                MeasureProvenance::Observed
            } else {
                MeasureProvenance::Unknown
            },
        };
        for scope in &focus_acceptance_pending_scopes {
            observation.unknown_deltas.push(UnknownDelta {
                unknown_id: format!("focus-acceptance:{scope}"),
                change: ResolutionDeltaKind::Opened,
                evidence_refs: Vec::new(),
            });
        }
        for scope in &satisfied_focus_acceptance_scope_keys {
            observation.unknown_deltas.push(UnknownDelta {
                unknown_id: format!("focus-acceptance:{scope}"),
                change: ResolutionDeltaKind::Resolved,
                evidence_refs: vec![format!("focus_resource_scope:{scope}")],
            });
        }
        let goal_id = state.goal_id.clone();
        drop(state);
        if focus_synthesis_ready
            || (repeated_evidence_saturation
                && !focus_acceptance_pending
                && !required_write_recovery)
        {
            self.runtime
                .lock()
                .await
                .record_turn_strategy_early_stop(if focus_synthesis_ready {
                    "the bounded Focus contract is complete and further evidence acquisition saturated"
                } else if bounded_evidence_role {
                    "two consecutive bounded evidence batches added no required evidence coverage"
                } else {
                    "three consecutive main-turn tool batches added no evidence coverage"
                })
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: error.to_string(),
                })?;
        }
        let intervention = if focus_synthesis_ready {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Synthesize,
                reason: "the bounded Focus contract is complete and further evidence acquisition saturated; retain its receipts and synthesize without another tool/model exploration step"
                    .to_string(),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if focus_acceptance_met {
            // A directory-level read contract is a minimum evidence boundary,
            // not proof that the model has enough material for every requested
            // claim. Keep the authorized read tools available while evidence
            // still adds information; the bounded saturation policy below
            // closes repeated exploration deterministically.
            None
        } else if continue_with_tool_batch || orchestration_terminal_summary.is_some() {
            None
        } else if required_write_recovery {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason: format!(
                    "the objective requires a workspace write, but repeated read-only batches produced no write attempt. Execute one authorized write now before further reading or synthesis. Authorized exact write scopes: {}",
                    if authorized_write_scopes.is_empty() {
                        "the bounded scope declared by the active permission lease".to_string()
                    } else {
                        authorized_write_scopes.join(", ")
                    }
                ),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if repeated_evidence_saturation && focus_acceptance_pending {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason: format!(
                    "bounded evidence reads repeated before required action scope(s) were observed: {}; execute one authorized missing action instead of rereading or synthesizing",
                    focus_acceptance_scopes.join(", ")
                ),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if repeated_evidence_saturation {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Synthesize,
                reason: if bounded_evidence_role {
                    "two consecutive bounded evidence batches added no required coverage; retain checked evidence and stop the child before another model/tool step"
                } else {
                    "three consecutive main-turn tool batches added no evidence coverage; disable tools and synthesize from retained receipts before the token lease is exhausted"
                }
                .to_string(),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if repeated_local_failures {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: if has_successful_tool_evidence {
                    RuntimeInterventionKind::Synthesize
                } else {
                    RuntimeInterventionKind::Block
                },
                reason: if has_successful_tool_evidence {
                    "multiple consecutive tool batches failed after checked evidence was already retained; stop retrying and synthesize the bounded result with the failure explicit"
                        .to_string()
                } else {
                    "multiple consecutive tool batches failed before any checked evidence was retained; stop speculative retries"
                        .to_string()
                },
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else {
            (!continue_with_tool_batch)
                .then(|| {
                    propose_intervention_after_observation(
                        &self.services,
                        &goal_id,
                        observation.clone(),
                    )
                    .map(Some)
                    .map_err(|reason| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason,
                    })
                })
                .transpose()?
                .flatten()
        };
        if let Some(intervention) = intervention
            .as_ref()
            .filter(|intervention| intervention.kind != RuntimeInterventionKind::Continue)
        {
            self.state.lock().await.content.push_str(&format!(
                "\n\nRuntime intervention ({:?}): {}",
                intervention.kind, intervention.reason
            ));
        }
        let mut automatic_focus_verification_node =
            if let Some((session_id, iteration, calls)) = automatic_focus_verification {
                let mut nodes = tool_nodes_for_calls(
                    ticket,
                    iteration,
                    &session_id,
                    calls,
                    self.services.workspace_root(),
                )?;
                (nodes.len() == 1).then(|| nodes.remove(0))
            } else {
                None
            };
        let next = {
            let mut state = self.state.lock().await;
            let node = if let Some(answer) = orchestration_terminal_summary.as_ref() {
                state.terminal_override = Some((GoalCompletion::Satisfied, answer.clone()));
                let mut node = dynamic_node(
                    ticket,
                    state.iterations,
                    "orchestration-terminal-synthesize",
                    ExecutionNodeKind::Synthesize,
                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                    "inline_model",
                );
                node.executor_kind =
                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                        .to_string();
                node
            } else {
                let kind = intervention
                    .as_ref()
                    .map_or(RuntimeInterventionKind::Continue, |value| value.kind);
                match kind {
                    RuntimeInterventionKind::Synthesize => {
                        let focus_terminal_candidate = if focus_synthesis_ready {
                            // This candidate was written before Runtime
                            // executed a mandatory Focus recovery. Reusing it
                            // after the receipt closes can preserve stale
                            // prose such as "one more read is needed" in a
                            // successful Team terminal. Discard it at the
                            // evidence boundary: a write role may use the
                            // deterministic receipt-backed carrier below;
                            // every other role receives one text-only model
                            // synthesis grounded in the committed receipt.
                            state.pending_focus_terminal_candidate.take();
                            runtime_verified_implementation_terminal_candidate(
                                &state.focus_required_output_fields,
                                &state.focus_observed_resource_scopes,
                                &state.write_attempt_paths,
                                &state.tool_results,
                                self.services.workspace_root(),
                            )
                        } else {
                            None
                        };
                        if let Some(candidate) = focus_terminal_candidate {
                            state.focus_acceptance_pending_scopes.clear();
                            state.terminal_override = Some((GoalCompletion::Satisfied, candidate));
                            let mut node = dynamic_node(
                                ticket,
                                state.iterations,
                                "focus-acceptance-synthesize",
                                ExecutionNodeKind::Synthesize,
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                "inline_model",
                            );
                            node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                            node
                        } else {
                            state.force_text_only_next_model = true;
                            state.content.push_str(
                                "\n\nRuntime evidence checkpoint: the required evidence is complete, but no valid terminal presentation is available. Tools are disabled for the next response. Give every required Team output field using native structured output, JSON, Markdown headings, or `Field: value` labels, and ground every claim in retained receipts. State risks or unresolved work explicitly when applicable.\n",
                            );
                            dynamic_node(
                                ticket,
                                state.iterations,
                                "policy-text-only-conclusion",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            )
                        }
                    }
                    RuntimeInterventionKind::Block => {
                        let reason = intervention
                            .as_ref()
                            .map(|value| value.reason.as_str())
                            .unwrap_or("goal intervention blocked execution");
                        state.terminal_override = Some((
                            GoalCompletion::Partial,
                            format!(
                                "Execution blocked: {reason}\n\nChecked evidence was retained and no further speculative work was performed."
                            ),
                        ));
                        let mut node = dynamic_node(
                            ticket,
                            state.iterations,
                            "policy-block-synthesize",
                            ExecutionNodeKind::Synthesize,
                            crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                            "inline_model",
                        );
                        node.executor_kind =
                            crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                                .to_string();
                        node
                    }
                    _ => automatic_focus_verification_node.take().unwrap_or_else(|| {
                        dynamic_node(
                            ticket,
                            state.iterations,
                            "model",
                            ExecutionNodeKind::InlineModel,
                            "inline_model",
                            "inline_model",
                        )
                    }),
                }
            };
            node
        };
        let mut outcome = NodeExecutionOutcome::new(completed_result(
            Some(format!("{}:tool-results:{tool_calls}", ticket.graph_id)),
            ExecutionUsage {
                tool_calls,
                ..ExecutionUsage::default()
            },
        ));
        outcome.domain_events.push(
            self.services
                .goal_store()
                .observation_event(
                    &observation,
                    format!("{}:goal-observation", ticket.idempotency_key),
                )
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?,
        );
        if let Some(intervention) = intervention
            .as_ref()
            .filter(|intervention| intervention.kind != RuntimeInterventionKind::Continue)
        {
            outcome.domain_events.push(
                self.services
                    .goal_store()
                    .intervention_event(
                        intervention,
                        std::slice::from_ref(&observation),
                        format!("{}:goal-intervention", ticket.idempotency_key),
                    )
                    .map_err(|reason| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason,
                    })?,
            );
        }
        if !continue_with_tool_batch || orchestration_terminal_summary.is_some() {
            outcome.replan = Some(ExecutionGraphReplan {
                nodes: vec![next.clone()],
                edges: dynamic_edges(&ticket.node_id, &[next]),
                reason: format!(
                    "{}",
                    if orchestration_terminal_summary.is_some() {
                        "Runner committed completed orchestration terminal summary".to_string()
                    } else {
                        format!(
                            "Runner applied goal intervention: {:?}",
                            intervention
                                .as_ref()
                                .map_or(RuntimeInterventionKind::Continue, |value| value.kind)
                        )
                    }
                ),
            });
        }
        Ok(outcome)
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let runtime_authored_calls = decode_tool_batch(&ticket.payload_ref)
            .map(|(calls, _)| calls)
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("tool batch persistent payload is invalid after commit: {error}"),
            })?;
        let (mut messages, root_control_plane_phase, session_id, turn_id) = {
            let mut state = self.state.lock().await;
            let phase = state.pending_root_control_plane_phase.take();
            if let Some(phase) = phase {
                state.root_control_plane_phase = phase;
            }
            (
                state
                    .pending_transcript
                    .remove(&ticket.node_id)
                    .unwrap_or_default(),
                phase,
                state.session_id.clone(),
                state.turn_id.clone(),
            )
        };
        if runtime_authored_tool_batch(&runtime_authored_calls) {
            // These reads were scheduled by Runtime to close a bounded
            // evidence contract, rather than emitted by a provider.  Make
            // their provenance explicit while preserving the wire protocol's
            // required assistant tool-call predecessor for the committed
            // tool-result messages.
            messages.insert(
                0,
                runtime_authored_tool_call_message(&runtime_authored_calls),
            );
        }
        self.runtime
            .lock()
            .await
            .session_mut_async()
            .await
            .extend_messages(messages);
        if let Some(phase) = root_control_plane_phase {
            self.services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: format!("session:{session_id}"),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "runtime.control_plane.phase".to_string(),
                    status: Some(
                        (phase == RootControlPlanePhase::ProposalSubmitted)
                            .then_some("satisfied")
                            .unwrap_or("waiting")
                            .to_string(),
                    ),
                    actor: Some("conversation_runtime.root_control_plane".to_string()),
                    refs: vec![
                        crate::RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: ticket.graph_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "execution_node".to_string(),
                            id: ticket.node_id.clone(),
                        },
                        crate::RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: turn_id,
                        },
                    ],
                    payload: serde_json::json!({
                        "phase": phase,
                        "required_tool_choice": phase.required_tool_choice(),
                        "program_admitted": phase == RootControlPlanePhase::ProposalSubmitted,
                    }),
                })
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("persist root control-plane phase: {error}"),
                })?;
        }
        Ok(())
    }
}
