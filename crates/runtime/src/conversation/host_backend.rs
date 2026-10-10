//! Graph-owned provider and governed tool execution backends.

use super::*;

pub(super) fn requested_agentic_worker_wait(
    calls: &[ModelToolCall],
    successful: &BTreeSet<String>,
) -> bool {
    calls.iter().any(|call| {
        successful.contains(&call.id)
            && call.name == harness_contract::agent_action::STATE_INSPECT_TOOL_ID
            && serde_json::from_str::<harness_contract::agent_action::StateInspectInput>(
                &call.input,
            )
            .is_ok_and(|input| input.wait_for_workers)
    })
}

fn terminal_provider_route_intervention(
    goal_id: &str,
    ticket: &NodeExecutionTicket,
    scope: model_protocol::provider_failure::ProviderFailureScope,
) -> RuntimeIntervention {
    let reason = match scope {
        model_protocol::provider_failure::ProviderFailureScope::Account => {
            "the configured provider account is unavailable (credentials, balance, quota, or plan capacity); model replanning cannot repair account state"
        }
        model_protocol::provider_failure::ProviderFailureScope::Configuration => {
            "no valid configured provider route remains; model replanning cannot repair runtime configuration"
        }
        model_protocol::provider_failure::ProviderFailureScope::Request => {
            "the provider route is unavailable"
        }
    };
    RuntimeIntervention {
        goal_id: goal_id.to_string(),
        kind: RuntimeInterventionKind::Block,
        reason: reason.to_string(),
        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
        expected_graph_revision: None,
    }
}

fn provider_blocked_reason(terminal: bool, intervention_reason: &str, evidence: &str) -> String {
    if terminal {
        format!(
            "Execution blocked because the provider route is unavailable: {intervention_reason}\n\nExact provider evidence: {evidence}\n\nNo same-account retry or model replan was attempted. Committed goal, tool receipts, and evidence state were retained. Restore the account/configuration or select a separately configured provider account, then start a new turn."
        )
    } else {
        format!(
            "Execution blocked after repeated provider failures: {intervention_reason}\n\nExact provider validation evidence: {evidence}\n\nCommitted goal and evidence state were retained. Provide a new provider, constraint, or explicit replan to continue."
        )
    }
}

fn workspace_mutation_is_objectively_forbidden(
    task_explicitly_forbids_workspace_write: bool,
    required_permission: harness_contract::tool::ToolPermissionMode,
) -> bool {
    task_explicitly_forbids_workspace_write
        && required_permission != harness_contract::tool::ToolPermissionMode::ReadOnly
}

/// Merge mutation epochs persisted by already-committed Goal observations with
/// epochs completed earlier in this owned graph turn. The latter is essential
/// for the immediate successor node: graph scheduling may make that node
/// runnable before the Goal-stream event transaction is observable through a
/// new store read. Both inputs are Runtime-authored facts; no model text or
/// requested path participates in this causal proof.
fn prior_workspace_mutation_sequences(
    durable: impl IntoIterator<Item = u64>,
    in_turn: &BTreeSet<u64>,
) -> BTreeSet<u64> {
    let mut sequences = durable.into_iter().collect::<BTreeSet<_>>();
    sequences.extend(in_turn.iter().copied());
    sequences
}

/// Whether a completed Runtime-authorized effect can establish the mutation
/// epoch required before an exact workspace postcondition read. A direct file
/// adapter remains stronger because it identifies the changed file and its
/// pre-image. This deliberately admits a local shell `Destructive` effect
/// too: a command that creates artifacts and then removes its own temporary
/// file is conservatively classified as destructive, but its later exact read
/// can still prove the required final state. System, package, and network
/// effects never cross this boundary.
fn is_local_workspace_mutation_effect(
    effect: &harness_contract::tool::ToolEffectDescriptor,
) -> bool {
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    use harness_contract::tool::ToolEffectKind;

    if effect.effect_kind == ToolEffectKind::Write
        && effect.assessment.externality == harness_contract::policy::EffectExternality::Workspace
    {
        return true;
    }

    effect.effect_kind == ToolEffectKind::Destructive
        && !effect.uses_network
        && !effect.mutates_system
        && !effect.mutates_packages
        && effect.scopes.iter().any(|scope| {
            scope.resource == PermissionResource::Shell
                && scope.operation == PermissionOperation::Write
        })
}

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
        let result = async {
        if self.state.lock().await.terminal_override.is_some() {
            return Ok(precommitted_terminal_outcome(ticket));
        }
        {
            // A weak model woken by `all_tasks_accepted` can coordinate forever
            // without making the exit decision (Qwen v6: 162 steps, 0 requests).
            // Once every framework-owned rung has been satisfied for several
            // consecutive root steps, drive the completion request on the
            // root's behalf. The drive stays behind the completion_gap gate, so
            // an Objective that is not genuinely complete is never forced.
            let (session_id, turn_id, owns_root) = {
                let state = self.state.lock().await;
                (
                    state.session_id.clone(),
                    state.turn_id.clone(),
                    state.execution_role.owns_root_presentation(),
                )
            };
            if owns_root {
                if let Some(program) = super::root_agentic_program_projection(
                    self.services.as_ref(),
                    &session_id,
                    &turn_id,
                ) {
                    let outstanding = super::outstanding_closure_steps(&program)
                        .into_iter()
                        .filter(|step| step != "objective_complete_request")
                        .count();
                    let gap = crate::agentic::agentic_program_completion_gap(
                        self.services.as_ref(),
                        &session_id,
                        &turn_id,
                    );
                    let mut state = self.state.lock().await;
                    if outstanding == 0 && gap.is_none() && program.completion_request.is_none() {
                        state.closure_ready_steps = state.closure_ready_steps.saturating_add(1);
                        if state.closure_ready_steps >= closure_drive_after_steps() {
                            state.closure_ready_steps = 0;
                            drop(state);
                            match self
                                .services
                                .drive_agentic_objective_completion(
                                    &session_id,
                                    &turn_id,
                                    &ticket.graph_id,
                                )
                                .await
                            {
                                Ok(true) => tracing::info!(
                                    "closure-ready drive submitted the completion request on the root's behalf"
                                ),
                                Ok(false) => {}
                                Err(error) => tracing::warn!(
                                    %error,
                                    "closure-ready drive declined"
                                ),
                            }
                        }
                    } else {
                        state.closure_ready_steps = 0;
                    }
                }
            }
        }
        {
            // Objective-level wall-clock budget for a required-Team turn. Checked
            // at the top of every root model step so it cannot be bypassed by the
            // clean-terminal-synthesis short circuit below. The no-progress fuse
            // cannot bound a turn that keeps making nominal progress.
            let mut state = self.state.lock().await;
            if collaboration_budget_elapsed(
                state.collaboration_obligation.as_ref(),
                state.execution_role.is_delegated_leaf(),
                state.started_at.elapsed(),
                collaboration_wall_budget(),
                state.last_verified_progress,
            ) {
                let disclosure = "Execution blocked safely: bounded convergence reached the Agent-first collaboration budget with the required verified-Team minimum still unmet. The objective is only partially satisfied and unresolved work stays in the Program projection; Runtime stopped the turn instead of running unbounded.".to_string();
                state.terminal_override = Some((GoalCompletion::Partial, disclosure));
                return Ok(precommitted_terminal_outcome(ticket));
            }
        }
        let delegated_protocol_at_step =
            delegated_agentic_protocol_state(self.services.as_ref(), ticket)?;
        if delegated_protocol_at_step.is_none() {
            let (session_id, turn_id) = {
                let state = self.state.lock().await;
                (state.session_id.clone(), state.turn_id.clone())
            };
            if let Some(program) =
                root_agentic_program_projection(self.services.as_ref(), &session_id, &turn_id)
            {
                let checkpoint = if self.state.lock().await.agentic_program_context_revision
                    != Some(program.revision)
                {
                    let services = self.services.clone();
                    let snapshot = program.clone();
                    Some(
                        tokio::task::spawn_blocking(move || {
                            compact_agentic_program_checkpoint(Some(services.as_ref()), &snapshot)
                        })
                        .await
                        .map_err(|error| NodeExecutorError::Poll {
                            node_id: ticket.node_id.clone(),
                            reason: format!("prepare Agentic checkpoint: {error}"),
                        })?,
                    )
                } else {
                    None
                };
                let mut state = self.state.lock().await;
                if let Some(checkpoint) = checkpoint {
                    retain_agentic_program_checkpoint(
                        &mut state.persistent_collaboration_context,
                        &program,
                        checkpoint,
                    );
                    state.agentic_program_context_revision = Some(program.revision);
                }
            }
        }
        let prefetched_review_calls = {
            let mut state = self.state.lock().await;
            let reviewer_prefetch = state.pending_disposition_inputs.is_empty()
                && should_prefetch_focus_verification(
                    state.first_model_step,
                    state.bounded_evidence_role,
                    state.focus_verification_prefetched,
                    &state.focus_acceptance_pending_scopes,
                    self.services.workspace_root(),
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
                framework_authored: true,
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
            pending_disposition_inputs,
        ) = {
            let mut state = self.state.lock().await;
            let pending_disposition_inputs = state.pending_disposition_inputs.clone();
            let first = state.first_model_step;
            state.first_model_step = false;
            let made_progress = std::mem::take(&mut state.last_verified_progress);
            state.consecutive_unverified_model_steps = next_unverified_model_step_streak(
                state.consecutive_unverified_model_steps,
                made_progress,
            );
            let mut clean_terminal_synthesis =
                std::mem::take(&mut state.clean_terminal_synthesis_next);
            if !pending_disposition_inputs.is_empty() {
                // A terminal candidate produced for the pre-update topology is
                // stale. Discard it and let the post-disposition graph decide
                // whether a fresh synthesis is still appropriate.
                clean_terminal_synthesis = false;
                state.clean_terminal_synthesis_attempted = false;
            }
            if !clean_terminal_synthesis
                && !state.clean_terminal_synthesis_attempted
                && state.successful_tool_calls > 0
                && state.consecutive_unverified_model_steps >= state.safety_lease.max_model_steps
                && delegated_protocol_at_step
                    .as_ref()
                    .is_none_or(DelegatedAgenticProtocolState::is_terminal)
            {
                state.clean_terminal_synthesis_attempted = true;
                clean_terminal_synthesis = true;
            }
            let clean_terminal_evidence = clean_terminal_synthesis.then(|| {
                let mut result_messages = state.tool_results.clone();
                let mut assistant_messages = state.assistant_messages.clone();
                let early_receipts = state
                    .early_tool_receipts
                    .values()
                    .collect::<Vec<_>>();
                for receipt in early_receipts {
                    result_messages.push(early_tool_receipt_message(receipt));
                    let already_present = assistant_messages.iter().any(|message| {
                        message.blocks.iter().any(|block| {
                            matches!(block, ContentBlock::ToolUse { id, .. } if id == &receipt.call.id)
                        })
                    });
                    if !already_present {
                        assistant_messages.push(ConversationMessage::assistant(vec![
                            ContentBlock::ToolUse {
                                id: receipt.call.id.clone(),
                                name: receipt.call.name.clone(),
                                input: receipt.call.input.clone(),
                            },
                        ]));
                    }
                }
                let digest = terminal_evidence_digest(&result_messages);
                let history = terminal_evidence_history(&assistant_messages, &result_messages);
                (digest, history)
            });
            let intervention = if clean_terminal_synthesis || !pending_disposition_inputs.is_empty()
            {
                None
            } else {
                // Absolute objective-level bound for Agent-first collaboration:
                // the no-progress fuse cannot fire while the objective keeps
                // making nominal progress (dispatching, claiming), so a
                // required-Team turn could otherwise run unbounded. Past the
                // ceiling the turn ends as an honest partial with disclosure.
                let collaboration_budget_elapsed = collaboration_budget_elapsed(
                    state.collaboration_obligation.as_ref(),
                    state.execution_role.is_delegated_leaf(),
                    state.started_at.elapsed(),
                    collaboration_wall_budget(),
                    state.last_verified_progress,
                );
                let collaboration_ceiling_reached = collaboration_ceiling_reached(
                    state.collaboration_obligation.as_ref(),
                    state.execution_role.is_delegated_leaf(),
                    state.iterations,
                    state.safety_lease.max_model_steps,
                    collaboration_step_ceiling_mult(),
                );
                let fuse_decision = if collaboration_budget_elapsed || collaboration_ceiling_reached
                {
                    crate::execution_core::SafetyFuseDecision::Block {
                        reason: "bounded convergence: the required verified-Team minimum is still unmet after the Agent-first collaboration budget; the objective is only partially satisfied, unresolved work stays in the Program projection, and Runtime stopped the turn instead of running unbounded".to_string(),
                    }
                } else {
                    crate::execution_core::SafetyFusePolicy::evaluate(
                        &state.safety_lease,
                        state.consecutive_unverified_model_steps,
                        made_progress,
                    )
                };
                match fuse_decision {
                    crate::execution_core::SafetyFuseDecision::Continue => None,
                    crate::execution_core::SafetyFuseDecision::Block { .. }
                        if delegated_protocol_at_step
                            .as_ref()
                            .is_some_and(|protocol| !protocol.is_terminal())
                            && state.agentic_protocol_recovery_attempts < 2 =>
                    {
                        state.agentic_protocol_recovery_attempts =
                            state.agentic_protocol_recovery_attempts.saturating_add(1);
                        if let Some(protocol) = delegated_protocol_at_step.as_ref() {
                            state.force_text_only_next_model = false;
                            state.force_tool_allowlist_next_model = None;
                            let mut item = ContextItem::new(
                                format!("agentic-protocol-fuse-recovery:{}", ticket.node_id),
                                ContextSourceKind::Task,
                                ContextRole::Instruction,
                                protocol.continuation_instruction(),
                            );
                            item.authority = ContextAuthority::System;
                            item.visibility = ContextVisibility::Private;
                            item.evidence = protocol.artifact_evidence_refs.clone();
                            state.pending_next_model_context.push(item);
                        }
                        None
                    }
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
                            u64::try_from(state.consecutive_unverified_model_steps)
                                .unwrap_or(u64::MAX),
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
            // PLAN D2b: the generic terminal obligation.
            //
            // The bounded-Focus mechanism forces a terminal presentation only
            // when a Focus declared its acceptance scopes; a long open-ended
            // turn that keeps producing receipts has no such trigger, which is
            // the "progress without committing" failure the Facet A run showed
            // (367 observations, 27.9MB, no terminal). This evaluates the
            // obligation against a CAUSAL clock - armed when the turn first had
            // enough committed receipts, never at turn start - and, once the
            // commit window has elapsed, asks the model once to commit.
            //
            // It NEVER forces text-only and NEVER ends the turn: `NonCommitting`
            // is deliberately inert. PLAN D1: the runtime helps a turn finish,
            // it does not kill one the model has not declared complete.
            {
                use crate::liveness::{TerminalVerdict, TurnProgress};
                let progress = TurnProgress {
                    committed_receipts: state.tool_receipts_observed,
                    saw_terminal_marker: false,
                    committed_terminal: state.committed_terminal_answer.is_some(),
                };
                if !state.terminal_tracker.is_armed()
                    && !progress.committed_terminal
                    && progress.satisfied(&state.terminal_tracker.obligation().satisfaction)
                {
                    state.terminal_tracker.arm();
                    state.terminal_satisfaction_since = Some(std::time::Instant::now());
                }
                if let TerminalVerdict::RequestCommit { since_satisfied, .. } =
                    state.terminal_tracker.evaluate(
                        &progress,
                        state.terminal_satisfaction_since.map(|since| since.elapsed()),
                    )
                {
                    if state.terminal_tracker.request_commit() {
                        let mut item = ContextItem::new(
                            "runtime-terminal-obligation",
                            ContextSourceKind::Task,
                            ContextRole::Instruction,
                            format!(
                                "Runtime terminal obligation: {} tool receipt(s) are committed and \
                                 the turn has been within the commit window for {}s. If the \
                                 objective is satisfied, commit the final answer now from the \
                                 receipts already held. If it is not, say what is still missing \
                                 and continue - the runtime does not end the turn for you.",
                                progress.committed_receipts,
                                since_satisfied.as_secs()
                            ),
                        );
                        item.authority = ContextAuthority::System;
                        item.visibility = ContextVisibility::Private;
                        state.pending_next_model_context.push(item);
                    }
                }
            }
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
                pending_disposition_inputs,
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
                framework_authored: true,
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
        let topic_delivery = if !clean_terminal_synthesis && delegated_protocol_at_step.is_some() {
            if let Some(packet) = delegated_agent_task_packet(self.services.as_ref(), ticket) {
                let services = Arc::clone(&self.services);
                tokio::task::spawn_blocking(move || {
                    crate::agentic::topic_delivery::prepare(
                        &services.agent_action_service(),
                        &packet,
                    )
                })
                .await
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("prepare Topic delta: {error}"),
                })?
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("read Topic delta: {error}"),
                })?
            } else {
                None
            }
        } else {
            None
        };
        let working_item = if clean_terminal_synthesis {
            None
        } else {
            let context = self.runtime.lock().await.memory_turn_context();
            self.services
                .working_context_item(&context)
                .await
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("prepare working context: {reason}"),
                })?
        };
        let mut runtime = self.runtime.lock().await;
        let mut replayed_agent_inputs = Vec::new();
        if let Some(packet) = delegated_input_task_packet(self.services.as_ref(), ticket) {
            let stream = runtime.session_input_stream();
            for input in self.services.agent_runtime().pending_agent_inputs(packet.agent_id(), packet.run_id())
                .map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })? {
                let content = crate::AgentRuntime::input_text(input.input.as_ref().ok_or_else(|| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: "durable Agent input has no content".into() })?);
                let envelope = harness_contract::turn::SessionInputEnvelope::text(packet.session_id(), harness_contract::turn::InputSourceKind::Agent, content)
                    .with_source_ref(format!("agent-input:{}", packet.run_id()))
                    .with_source_message_id(input.command_id);
                let receipt = stream.admit(envelope, stream.runtime_state());
                if let Some(record) = stream.record_snapshot(&receipt.input_id).filter(|record| record.consumed_at.is_some()) {
                    // Checkpoint extraction is not durable consumption. A
                    // failed Provider request (or graph commit) may have
                    // drained the transient prompt, while its journal intent
                    // remains pending. Re-project it until an actual successful
                    // ModelStep commits; do not repeat Turn disposition.
                    for item in crate::turn_inbox::checkpoint_context_items(TurnInputCheckpoint::BeforeProviderRequest, std::slice::from_ref(&record)) {
                        runtime.push_next_model_context_item(item);
                    }
                    replayed_agent_inputs.push(record);
                }
            }
        }
        if let Some(item) = working_item {
            runtime.push_next_model_context_item(item);
        }
        if let Some(delivery) = topic_delivery.as_ref() {
            runtime.push_next_model_context_item(delivery.item.clone());
        }

        let required_control_plane = {
            let state = self.state.lock().await;
            if state.execution_role.is_delegated_leaf()
                || state.collaboration_started
                || has_completed_program_terminal(&state.tool_results)
                || has_admitted_program_receipt(&state.tool_results)
            {
                None
            } else {
                state.collaboration_obligation.as_ref().map(|obligation| {
                    (
                        obligation.required_team_count(),
                        state
                            .task_understanding
                            .as_ref()
                            .map(|value| value.required_workspace_evidence_scopes.clone())
                            .unwrap_or_default(),
                    )
                })
            }
        };
        let control_plane_priority = next_model_control_plane_priority(
            pending_disposition_inputs.len(),
            required_control_plane.is_some(),
        );
        if let NextModelControlPlanePriority::InputDisposition { slot_count } =
            control_plane_priority
        {
            let (session_id, turn_id) = {
                let state = self.state.lock().await;
                (state.session_id.clone(), state.turn_id.clone())
            };
            if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(
                TurnInputCheckpoint::BeforeProviderRequest,
                &pending_disposition_inputs,
            ) {
                let mut item = ContextItem::new(
                    format!("runtime-input-disposition:{session_id}:{turn_id}"),
                    ContextSourceKind::Task,
                    ContextRole::Instruction,
                    format!(
                        "Runtime input priority: incorporate all {slot_count} pending input slot(s) before continuing or returning a terminal response. {guidance}"
                    ),
                );
                item.authority = ContextAuthority::System;
                item.visibility = ContextVisibility::Private;
                item.evidence = pending_disposition_inputs
                    .iter()
                    .map(|record| format!("session_input:{}", record.envelope.input_id.as_str()))
                    .collect();
                runtime.push_next_model_context_item(item);
            }
        } else if let Some((required_team_count, required_workspace_evidence_scopes)) =
            required_control_plane
        {
            let (session_id, turn_id) = {
                let state = self.state.lock().await;
                (state.session_id.clone(), state.turn_id.clone())
            };
            {
                let mut state = self.state.lock().await;
                state.pending_root_control_plane_requirement = Some(required_team_count);
            }
            let scope_note = if required_workspace_evidence_scopes.is_empty() {
                String::new()
            } else {
                format!(
                    " Preserve these user-selected evidence scopes when publishing Tasks: [{}].",
                    required_workspace_evidence_scopes.join(", ")
                )
            };
            let mut item = ContextItem::new(
                format!("runtime-agentic-collaboration:{session_id}:{turn_id}"),
                ContextSourceKind::Task,
                ContextRole::Instruction,
                format!(
                    "This Objective requires at least {required_team_count} real Team execution(s). You retain semantic control: decide the Teams, roles, Tasks, dependencies, discussion and replanning. Batch independent work into one round or one `tool_orchestrate` program. Use team_create, agent_invite, task_publish and the other small Agent actions; Runtime binds identities, leases and execution. When failed or challenged work must be replaced or split, first publish its concrete successor Task(s), then use task_supersede with durable evidence; retirement never counts as accepted work. Write plans and long results as normal content and commit references—never submit a complete graph or long JSON.{scope_note} Request completion only after committed artifacts, independent review and all required Teams are complete. Design Tasks so no model capability is wasted: scope each Task to a batch of related reads (not one file per Task), and instruct reviewers to verify through `evidence_retrieve` on the claimant's durable receipt instead of re-reading source files - a receipt carries the same bytes with a Runtime-bound hash. One live run re-read a single lib.rs 40 times across agents; every re-read of already-read content is avoidable spend. {task_authoring} {economy}"
                    ,
                    task_authoring = crate::agentic::order_contract::TASK_AUTHORING_DISCIPLINE,
                    economy = crate::agentic::order_contract::EVIDENCE_ECONOMY,
                ),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            item.evidence = vec![format!("turn:{turn_id}")];
            runtime.push_next_model_context_item(item);
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
        if matches!(
            control_plane_priority,
            NextModelControlPlanePriority::ExistingConstraint
        ) && !clean_terminal_synthesis
        {
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
                            reality_context: runtime.reality_data_lease(),
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
                    clean_terminal_evidence
                        .as_ref()
                        .map(|(digest, _)| digest.as_str())
                        .unwrap_or_default(),
                    clean_terminal_evidence
                        .as_ref()
                        .map(|(_, history)| history.clone())
                        .unwrap_or_default(),
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
        if let Some(packet) = result.as_ref().ok().and_then(|_| delegated_input_task_packet(self.services.as_ref(), ticket)) {
            let (before, after): (Vec<_>, Vec<_>) = consumed_inputs.iter().cloned().partition(|record|
                matches!(record.checkpoint, Some(TurnInputCheckpoint::BeforeProviderRequest | TurnInputCheckpoint::TurnStart | TurnInputCheckpoint::IngressDispatched)));
            let mut before = before;
            before.extend(replayed_agent_inputs);
            let events = self.services.agent_runtime().agent_input_consumption_events(&packet, &before)
                .map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?;
            let deferred = self.services.agent_runtime().agent_input_consumption_events(&packet, &after)
                .map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?;
            let mut state = self.state.lock().await;
            let previous = state.deferred_agent_input_consumption.clone();
            let pending = state.pending_agent_input_consumption.entry(ticket.node_id.clone()).or_default();
            pending.extend(previous);
            pending.extend(events);
            state.deferred_agent_input_consumption.extend(deferred);
        }
        let topic_selected = topic_delivery
            .as_ref()
            .is_some_and(|delivery| delivery.selected_in(runtime.last_context_envelope().as_ref()));
        let cache_dimensions_known = runtime.turn_cache_dimensions_known();
        let cowd_bus = runtime.cowd_bus().cloned();
        drop(runtime);
        match result {
            Ok(step) => {
                if topic_selected {
                    if let Some(delivery) = topic_delivery {
                        let services = Arc::clone(&self.services);
                        let ack = tokio::task::spawn_blocking(move || {
                            services
                                .agent_action_service()
                                .acknowledge_topic_observations(delivery.ack)
                        })
                        .await;
                        if !matches!(ack, Ok(Ok(()))) {
                            tracing::warn!(?ack, "selected Topic delta acknowledgement deferred; unread entries remain replayable");
                        }
                    }
                }

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
                    cache_creation_input_tokens: u64::from(step.usage.cache_creation_input_tokens),
                    cache_read_input_tokens: u64::from(step.usage.cache_read_input_tokens),
                    cached_tokens: u64::from(step.usage.cache_read_input_tokens),
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
                let pending_disposition_inputs =
                    {
                        let mut state = self.state.lock().await;
                        for record in seen_disposition_inputs {
                            if !state.pending_disposition_inputs.iter().any(|existing| {
                                existing.envelope.input_id == record.envelope.input_id
                            }) {
                                state.pending_disposition_inputs.push(record);
                            }
                        }
                        state.pending_disposition_inputs.clone()
                    };
                let input_update_applied = !pending_disposition_inputs.is_empty();
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
                state.record_root_model_usage(RootModelUsageDelta {
                    usage: step.usage,
                    model: step.model.clone(),
                    models_used: step.models_used.clone(),
                    first_token_latency_ms: step.first_token_latency_ms,
                    active_stream_duration_ms: step.active_stream_duration_ms,
                    wall_duration_ms: step.wall_duration_ms,
                    output_chars: step_output_chars,
                    output_chunks: 1,
                    cache_dimensions_known,
                });
                if let Some(bus) = cowd_bus.as_ref() {
                    bus.emit(CowdEvent::RunModelTelemetry {
                        telemetry: state.root_model_telemetry(),
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
                committed_messages.extend(step.preflight_tool_results.clone());
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
                if input_update_applied {
                    // The provider candidate did not observe these durable inputs.
                    // Preserve usage telemetry, but force a fresh semantic model step
                    // with the updates as ordinary authoritative context. The model
                    // can then amend work incrementally through Agent Actions; no
                    // separate graph-shaped routing contract exists.
                    state.assistant_messages.pop();
                    state.pending_transcript.remove(&ticket.node_id);
                    state.pending_disposition_inputs.clear();
                    state.clean_terminal_synthesis_next = false;
                    state.clean_terminal_synthesis_attempted = false;
                    state.last_verified_progress = true;
                    for record in &pending_disposition_inputs {
                        state.content.push_str("\n\nApplied running-Turn input:\n");
                        state.content.push_str(&record.envelope.content);
                        let supplemental = harness_contract::strategy::understand(
                            &harness_contract::strategy::StrategyInput::from_prompt(
                                &record.envelope.content,
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
                    state.pending_next_model_context.extend(
                        crate::turn_inbox::checkpoint_context_items(
                            TurnInputCheckpoint::BeforeProviderRequest,
                            &pending_disposition_inputs,
                        ),
                    );
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
                            "consumed {} session input update(s); incorporated_into_agent_context={input_update_applied}",
                            consumed_inputs.len(),
                        ),
                        format!("session-input:{}", sha256_digest(&evidence_refs.join("\n"))),
                        ObservationResultClass::Informational,
                    );
                    observation.evidence_refs.clone_from(&evidence_refs);
                    if input_update_applied {
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
                let mut intent = if late_inputs || input_update_applied {
                    ModelStepIntent::Replan {
                        reason:
                            "new Session input was incorporated into authoritative Agent context; continue from the newer durable input cursor"
                                .to_string(),
                    }
                } else {
                    step.intent.clone()
                };
                // A zero-tool clean synthesis is text-only by construction: the
                // request exposed no schemas, so any call the provider parsed
                // out of the response is a contract violation, not work. One
                // live run had a model emit `objective_complete_request`
                // during exactly such a request and Runtime durably executed
                // it, feeding the request/reject loop the gap guidance was
                // trying to break.
                if force_text_only_response || step.text_only_response || clean_terminal_synthesis {
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
                        ModelStepIntent::Replan { .. } if !input_update_applied => {
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
                if input_update_applied {
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
                let agentic_content_bridge_active = state.collaboration_obligation.is_some()
                    || state.execution_role.is_delegated_leaf()
                    || intent_requests_explicit_content(&intent);
                let _retained_agentic_content_ref =
                    if late_inputs || input_update_applied || !agentic_content_bridge_active {
                        None
                    } else {
                        persist_agentic_content_draft(
                            self.services.as_ref(),
                            ticket,
                            &step.assistant_message,
                        )
                        .await?
                    };
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
                        if let Some(protocol) =
                            delegated_agentic_protocol_state(self.services.as_ref(), ticket)?
                                .filter(|protocol| !protocol.is_terminal())
                        {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            state.force_text_only_next_model = false;
                            state.clean_terminal_synthesis_next = false;
                            state.force_tool_allowlist_next_model = None;
                            let reason = protocol.continuation_instruction();
                            let mut item = ContextItem::new(
                                format!("agentic-task-protocol-continue:{}", ticket.node_id),
                                ContextSourceKind::Task,
                                ContextRole::Instruction,
                                reason.clone(),
                            );
                            item.authority = ContextAuthority::System;
                            item.visibility = ContextVisibility::Private;
                            item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                            next_model_context = Some(item);
                            model_intervention = Some(RuntimeIntervention {
                                goal_id: state.goal_id.clone(),
                                kind: RuntimeInterventionKind::Replan,
                                reason,
                                evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                                expected_graph_revision: None,
                            });
                            break 'final_answer vec![dynamic_node(
                                ticket,
                                state.iterations,
                                "agentic-task-protocol-continue-model",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            )];
                        }
                        let required_team_executions =
                            required_team_execution_count_for_execution_context(
                                state.collaboration_obligation.as_ref(),
                                state.execution_role.is_delegated_leaf(),
                                state.evaluation_judge_only,
                            );
                        let agentic_program = root_agentic_program_projection(
                            &self.services,
                            &state.session_id,
                            &state.turn_id,
                        );
                        // Count Teams that have actually delivered accepted work.
                        // Requiring the whole Program to already be Verified made
                        // this always 0 while the root itself was the one that had
                        // to request verification, so the root could never
                        // converge and only ever replanned (or stopped partial).
                        let verified_team_executions = agentic_program
                            .as_ref()
                            .map_or(0, |program| {
                                program
                                    .tasks
                                    .values()
                                    .filter(|task| {
                                        task.status == crate::AgenticTaskStatus::Accepted
                                    })
                                    .map(|task| task.team_id.clone())
                                    .collect::<std::collections::BTreeSet<_>>()
                                    .len()
                            });
                        let blocked_program = agentic_program.as_ref().filter(|program| {
                            program.status == crate::AgenticProgramStatus::Blocked
                        });
                        if let Some(program) = blocked_program {
                            text = format!(
                                "Agent-first collaboration reached a durable blocked terminal state ({:?}). {}",
                                program.unresolved, text
                            );
                        }
                        if blocked_program.is_none()
                            && verified_team_executions < required_team_executions
                        {
                            state.collaboration_convergence_replans =
                                state.collaboration_convergence_replans.saturating_add(1);
                            let convergence_bound = collaboration_convergence_replan_bound(
                                state.safety_lease.complexity,
                            );
                            if state.collaboration_convergence_replans > convergence_bound {
                                // Bounded convergence: the verified-Team minimum
                                // stays unmet after repeated replans. End the turn
                                // as an honest partial with explicit disclosure
                                // instead of replanning without end.
                                let disclosure = format!(
                                    "Bounded convergence reached after {convergence_bound} replan(s): observed {verified_team_executions} verified Team(s) against a required minimum of {required_team_executions}. The objective is only partially satisfied; unresolved work stays visible in the Agent-first Program. Runtime stopped replanning instead of looping without end. {}",
                                    text
                                );
                                text = disclosure.clone();
                                state.terminal_override =
                                    Some((GoalCompletion::Partial, disclosure));
                            } else {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            // #GoalState + #ScratchPad: the model's position and
                            // the three exit channels, injected together with the
                            // re-drive so a stalled turn never continues blind.
                            let framing = crate::host::agentic_turn_framing(
                                self.services.as_ref(),
                                &state.session_id,
                                &state.turn_id,
                                state.iterations,
                                state.tool_results.len(),
                                state.started_at.elapsed().as_millis(),
                            );
                            let mut goal_state = ContextItem::new(
                                format!("goal-state:{}", ticket.node_id),
                                ContextSourceKind::Task,
                                ContextRole::Instruction,
                                format!(
                                    "#GoalState {}",
                                    serde_json::to_string(&framing)
                                        .unwrap_or_else(|_| "{}".to_string())
                                ),
                            );
                            goal_state.authority = ContextAuthority::System;
                            goal_state.visibility = ContextVisibility::Private;
                            state.pending_next_model_context.push(goal_state);
                            let mut scratch_pad = ContextItem::new(
                                format!("scratch-pad:{}", ticket.node_id),
                                ContextSourceKind::Task,
                                ContextRole::Instruction,
                                "#ScratchPad\nBefore you act, record four lines: `Goal:` the objective; `Actions:` the action(s) you will issue now; `Observation:` what the last result actually showed; `Reflection:` closure.current=…, closure.exit_ready=…, unmet=[…] => therefore choose ONE of: (a) advance the next unmet rung, (b) exit_ready -> call objective_complete_request, (c) judged unreachable after repeated attempts -> call objective_complete_request with blocked=true, a blocked_reason, human_review_required=true and recommended_next_actions.\nRuntime does not read this back: the durable Program projection stays the single source of truth for what is satisfied.".to_string(),
                            );
                            scratch_pad.authority = ContextAuthority::System;
                            scratch_pad.visibility = ContextVisibility::Private;
                            state.pending_next_model_context.push(scratch_pad);
                            // The framework states what is left; the model keeps
                            // deciding how. Name the outstanding rungs explicitly
                            // instead of burying them inside the checkpoint blob.
                            let remaining_steps = agentic_program
                                .as_ref()
                                .map(|program| {
                                    let tasks = program.tasks.values().collect::<Vec<_>>();
                                    let published = tasks.len();
                                    let claimed = tasks
                                        .iter()
                                        .filter(|task| task.claimant.is_some())
                                        .count();
                                    let reviewed = tasks
                                        .iter()
                                        .filter(|task| task.reviewed_by.is_some())
                                        .count();
                                    let accepted = tasks
                                        .iter()
                                        .filter(|task| {
                                            task.status == crate::AgenticTaskStatus::Accepted
                                        })
                                        .count();
                                    let mut outstanding: Vec<String> = Vec::new();
                                    if program.teams.len()
                                        < usize::from(program.required_team_count.max(1))
                                    {
                                        outstanding.push("team".to_string());
                                    }
                                    if published == 0 {
                                        outstanding.push("publish_task".to_string());
                                    }
                                    if claimed < published {
                                        outstanding.push("claim_task".to_string());
                                    }
                                    if reviewed < published {
                                        outstanding.push("independent_review".to_string());
                                    }
                                    if accepted < published {
                                        outstanding.push("accepted_task".to_string());
                                    }
                                    if !program.unresolved.is_empty() {
                                        outstanding.push("classify_issues".to_string());
                                    }
                                    if program.completion_request.is_none() {
                                        outstanding.push("objective_complete_request".to_string());
                                    }
                                    outstanding.join(", ")
                                })
                                .unwrap_or_default();
                            let reason = format!(
                                "Agent-first collaboration is not yet verified: observed {verified_team_executions} verified Team(s), required at least {required_team_executions}. Outstanding closure steps: [{}]. Current Program state: {}. Continue through the small Team/Agent/Task/Artifact actions; do not resubmit a full graph or repeat this final answer. The model owns the next semantic decision and Runtime will return a receipt for each action.",
                                if remaining_steps.is_empty() {
                                    "none recorded".to_string()
                                } else {
                                    remaining_steps
                                },
                                agentic_program.as_ref().map_or_else(
                                    || "not_started".to_string(),
                                    |program| {
                                        compact_agentic_program_checkpoint(
                                            None,
                                            program,
                                        )
                                    }
                                )
                            );
                            state.force_tool_allowlist_next_model = None;
                            let mut item = ContextItem::new(
                                format!("agentic-collaboration-continue:{}", ticket.node_id),
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
                                "agentic-collaboration-continue-model",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            )];
                            }
                        }
                        // ObjectiveSupervisor is the sole business completion
                        // authority. Once an Agentic Program is Verified, its
                        // reviewed final artifact satisfies the root delivery
                        // write obligation even when a delegated Agent, rather
                        // than the presentation Agent, performed the physical
                        // workspace write. Rechecking only root-local write
                        // receipts here creates a second owner and causes
                        // post-terminal no-op edits/model rounds.
                        let successful_write_observed = root_delivery_write_satisfied(
                            agentic_program.as_ref().is_some_and(|program| {
                                program.status == crate::AgenticProgramStatus::Verified
                            }),
                            state.required_write_for_completion,
                            &state.required_workspace_write_scopes,
                            &state.committed_workspace_observed_evidence,
                            state.committed_workspace_write_observed,
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
                                state.force_tool_allowlist_next_model = None;
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
                            let concrete_verification_scopes = concrete_focus_verification_scopes(
                                &state.focus_acceptance_pending_scopes,
                                &state.focus_observed_evidence,
                                self.services.path_identity_resolver(),
                            );
                            let verification_calls = focus_verification_tool_calls(
                                &concrete_verification_scopes,
                                state.iterations,
                                self.services.workspace_root(),
                            );
                            let broad_discovery_recovery = verification_calls.is_none();
                            let recovery_available = if broad_discovery_recovery {
                                state.focus_discovery_replans < 2
                            } else {
                                state.pending_focus_terminal_candidate.is_none()
                            };
                            if recovery_available {
                                let pending = state.focus_acceptance_pending_scopes.join(", ");
                                let instruction = if broad_discovery_recovery {
                                    state.focus_discovery_replans =
                                        state.focus_discovery_replans.saturating_add(1);
                                    format!(
                                        "Runtime Focus evidence recovery (mandatory): the prior answer was discarded because the broad authority scope `{pending}` has no source receipt. Use the available discovery tools (glob_search/grep_search/read_file as applicable) to locate and inspect semantically relevant evidence inside that scope, then produce a new grounded terminal answer. A directory is not represented by an arbitrary file. Do not return another unsupported answer."
                                    )
                                } else {
                                    state.pending_focus_terminal_candidate = Some(text.clone());
                                    format!(
                                        "Runtime Focus acceptance recovery (mandatory): retain the candidate final JSON, but do not finish yet. Complete the missing Runtime-verified action(s) with native tools: {pending}. For verify_after_write:path, perform a new exact-path read after this role's committed write receipt. For verify_upstream_change:path, independently read the exact upstream-changed path. Do not return another final answer until these actions complete."
                                    )
                                };
                                state.assistant_messages.pop();
                                state.pending_transcript.remove(&ticket.node_id);
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
                            let receipt_evidence = if state.structured_output_replans > 0 {
                                runtime_tool_receipt_evidence(
                                    &state.tool_results,
                                    self.services.workspace_root(),
                                )
                            } else {
                                Vec::new()
                            };
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
                            })
                            .or_else(|| {
                                (state.structured_output_replans > 0)
                                    .then(|| {
                                        normalized_receipt_backed_terminal_after_recovery(
                                            &text,
                                            &state.focus_required_output_fields,
                                            &receipt_evidence,
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
                                && state.consecutive_unverified_model_steps
                                    < state.safety_lease.max_model_steps
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
                                && state.consecutive_unverified_model_steps
                                    < state.safety_lease.max_model_steps
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
                    ModelStepIntent::ToolCalls { mut calls } => {
                        resolve_explicit_agentic_content_refs(
                            self.services.as_ref(),
                            ticket,
                            &step.assistant_message,
                            &mut calls,
                        )
                        .await?;
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
                            framework_authored: true,
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
                let provider_failure_scope = error.provider_failure_scope();
                let terminal_provider_route_failure = provider_failure_scope.route_is_unavailable();
                let provider_usage = error.provider_usage();
                let effect_receipts = error.effect_receipts().to_vec();
                let effect_closed_agentic_protocol =
                    effect_receipts.iter().any(|receipt| {
                        receipt.call.name.eq_ignore_ascii_case(
                            harness_contract::agent_action::TASK_SUBMIT_TOOL_ID,
                        ) || receipt.call.name.eq_ignore_ascii_case(
                            harness_contract::agent_action::TASK_REVIEW_TOOL_ID,
                        )
                    });
                let reason = error.to_string();
                let protocol_failure_detail =
                    protocol_failure.then(|| reason.chars().take(512).collect::<String>());
                let (
                    goal_id,
                    iteration,
                    protocol_attempt,
                    frame_repair_attempt,
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
                        state.record_root_model_usage(RootModelUsageDelta {
                            usage,
                            model: None,
                            models_used: Vec::new(),
                            first_token_latency_ms: None,
                            active_stream_duration_ms: None,
                            wall_duration_ms: 0,
                            output_chars: 0,
                            output_chunks: 0,
                            cache_dimensions_known,
                        });
                    } else {
                        state.cache_dimensions_known = cache_dimensions_known;
                    }
                    if let Some(bus) = cowd_bus.as_ref() {
                        bus.emit(CowdEvent::RunModelTelemetry {
                            telemetry: state.root_model_telemetry(),
                        });
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
                    // A malformed tool-call frame is a deterministic, model
                    // repairable defect: the intended action is usually fully
                    // visible in the rejected frame. Prior successful receipts
                    // say nothing about whether THIS frame is repairable, so
                    // `post_receipt_failure` must not convert one bad frame
                    // into a zero-tool terminal that silently discards the
                    // model's in-flight plan. One bounded repair continuation
                    // feeds the exact validation error back and lets the model
                    // re-emit; a second identical failure escalates as before.
                    // A tool-input-contract error is the same class of defect as
                    // a malformed provider frame: the model's intended action is
                    // fully visible and one re-emission away from working. The
                    // early dispatcher surfaces it as an ordinary step error
                    // (not a provider protocol failure), so without this it
                    // falls into the catch-all Synthesize and the plan is
                    // silently dropped - the exact loop Qwen v9 died in.
                    let repairable_tool_frame = protocol_failure
                        || repairable_tool_input_error(&reason);
                    let frame_repair_attempt = provider_frame_repair_attempt(
                        repairable_tool_frame,
                        tool_exposure_miss,
                        post_receipt_failure,
                        terminal_provider_route_failure,
                        clean_terminal_synthesis,
                        state.provider_frame_repair_attempts,
                    );
                    if frame_repair_attempt.is_some() {
                        state.provider_frame_repair_attempts =
                            state.provider_frame_repair_attempts.saturating_add(1);
                    }
                    let identity = runtime_observation_identity(&self.services, &state, ticket);
                    (
                        state.goal_id.clone(),
                        state.iterations,
                        protocol_attempt,
                        frame_repair_attempt,
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
                let fresh_delegated_protocol =
                    delegated_agentic_protocol_state(self.services.as_ref(), ticket)?;
                let pending_delegated_protocol = fresh_delegated_protocol
                    .as_ref()
                    .filter(|protocol| !protocol.is_terminal());
                let protocol_closed_after_receipt = effect_closed_agentic_protocol
                    && fresh_delegated_protocol
                        .as_ref()
                        .is_some_and(DelegatedAgenticProtocolState::is_terminal);
                let agentic_recovery_attempt: Option<u8> = {
                    let mut state = self.state.lock().await;
                    guarded_delegation_attempt(
                        self.services.as_ref(),
                        &mut state,
                        ticket,
                        post_receipt_failure,
                        pending_delegated_protocol,
                    )
                };
                let intervention = if terminal_provider_route_failure {
                    terminal_provider_route_intervention(&goal_id, ticket, provider_failure_scope)
                } else if protocol_closed_after_receipt {
                    RuntimeIntervention {
                        goal_id: goal_id.clone(),
                        kind: RuntimeInterventionKind::Synthesize,
                        reason: "the provider failed after the durable Agent action closed its Program Task; finish locally without another provider request"
                            .to_string(),
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                        expected_graph_revision: None,
                    }
                } else if let (Some(protocol), Some(attempt)) =
                    (pending_delegated_protocol, agentic_recovery_attempt)
                {
                    delegation_intervention(&goal_id, ticket, &protocol, attempt)
                } else if let Some(attempt) = frame_repair_attempt {
                    // The rejected frame is Runtime-owned validation evidence:
                    // hand it back verbatim so the model re-emits the same
                    // intended action in valid schema. Synthesizing here would
                    // silently drop a plan that was one shape error away from
                    // closing the Objective.
                    let kind = provider_protocol_intervention_kind(attempt);
                    harness_contract::goal::RuntimeIntervention {
                        goal_id: goal_id.clone(),
                        kind,
                        reason: format!(
                            "the prior response carried a tool-call frame Runtime could not parse (attempt {attempt} of {PROVIDER_FRAME_REPAIR_BUDGET}); committed evidence is preserved - re-emit the same intended action(s) in the exact exposed schema"
                        ),
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                        expected_graph_revision: None,
                    }
                } else if post_receipt_failure && !tool_exposure_miss {
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
                            if protocol_closed_after_receipt {
                                state.terminal_override = Some((
                                    GoalCompletion::Satisfied,
                                    "Delegated collaboration protocol was durably committed before the provider stream failed."
                                        .to_string(),
                                ));
                                let mut node = dynamic_node(
                                    ticket,
                                    iteration,
                                    "agentic-protocol-receipt-synthesize",
                                    ExecutionNodeKind::Synthesize,
                                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                    "inline_model",
                                );
                                node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                                (node, None)
                            } else {
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
                        }
                        RuntimeInterventionKind::Block => {
                            let blocked_reason = provider_blocked_reason(
                                terminal_provider_route_failure,
                                &intervention.reason,
                                protocol_failure_detail.as_deref().unwrap_or(&reason),
                            );
                            if terminal_provider_route_failure {
                                let language =
                                    crate::conversation::user_reply_language(&state.content);
                                state.terminal_failure_narration =
                                    Some(TerminalFailureNarration::Local(
                                        user_facing_blocked_answer(&blocked_reason, language),
                                    ));
                            }
                            state.terminal_override =
                                Some((GoalCompletion::Partial, blocked_reason));
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
                            let instruction = if let Some(protocol) = pending_delegated_protocol {
                                state.force_text_only_next_model = false;
                                state.force_tool_allowlist_next_model = None;
                                protocol.continuation_instruction()
                            } else if let Some(attempt) = frame_repair_attempt {
                                let detail = protocol_failure_detail
                                    .as_deref()
                                    .map(|detail| format!(" Exact validation evidence: {detail}"))
                                    .unwrap_or_default();
                                format!(
                                    "Runtime tool-frame repair (attempt {attempt} of {PROVIDER_FRAME_REPAIR_BUDGET}): your previous tool call was rejected by Runtime's frame parser before it could run, so nothing from it was executed.{detail} Re-emit the SAME intended action(s) now, in the exact exposed schema, with every field at its declared type (an array field must be a JSON array, never a string). Keep arguments compact and issue independent actions in one batch. Do not summarize, do not produce a final answer, and do not repeat the rejected frame unchanged."
                                )
                            } else if protocol_failure {
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
                                        "Runtime provider-protocol recovery (single attempt): the prior response used an invalid tool-call frame or requested an unknown, unavailable, or unauthorized tool.{detail} Retry from committed evidence using only one exposed action with compact valid arguments, or return a normal visible final answer when the Objective is already verified. Never print tool-protocol markup as prose."
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
                    framework_authored: true,
                });
                Ok(outcome)
            }
        }
        }.await;
        let mut outcome = result?;
        if outcome.result.status == ExecutionNodeStatus::Completed {
            if let Some(events) = self
                .state
                .lock()
                .await
                .pending_agent_input_consumption
                .get(&ticket.node_id)
            {
                let mut seen = BTreeSet::new();
                outcome.domain_events.extend(
                    events
                        .iter()
                        .filter(|event| seen.insert(event.idempotency_key.clone()))
                        .cloned(),
                );
            }
        }
        Ok(outcome)
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        tracing::debug!(node_id = %ticket.node_id, "publishing committed model transcript");
        let (messages, required_control_plane_team_count, session_id, turn_id) = {
            let mut state = self.state.lock().await;
            if let Some(consumed) = state
                .pending_agent_input_consumption
                .remove(&ticket.node_id)
            {
                let keys = consumed
                    .iter()
                    .map(|event| event.idempotency_key.clone())
                    .collect::<BTreeSet<_>>();
                state
                    .deferred_agent_input_consumption
                    .retain(|event| !keys.contains(&event.idempotency_key));
            }
            (
                state
                    .pending_transcript
                    .remove(&ticket.node_id)
                    .unwrap_or_default(),
                state.pending_root_control_plane_requirement.take(),
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
                        "required_actions": [
                            harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                            harness_contract::agent_action::AGENT_INVITE_TOOL_ID,
                            harness_contract::agent_action::TASK_PUBLISH_TOOL_ID,
                        ],
                        "program_admitted": false,
                    }),
                })
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("persist root control-plane requirement: {error}"),
                })?;
        }
        tracing::debug!(node_id = %ticket.node_id, "committed model transcript published");
        Ok(())
    }
}

fn next_unverified_model_step_streak(current: usize, made_verified_progress: bool) -> usize {
    if made_verified_progress {
        0
    } else {
        current.saturating_add(1)
    }
}

/// One bounded repair continuation for a malformed tool-call frame. A frame
/// defect is deterministic and model-repairable, so prior successful receipts
/// (`post_receipt_failure`) must not convert it into a zero-tool terminal that
/// discards the model's in-flight plan. Tool-exposure misses keep their own
/// governed path, an unavailable provider route cannot be repaired by
/// re-emitting a frame, and a step that is already a zero-tool synthesis has
/// no frame contract left to honor.
/// Whether a step error is a tool-input-contract violation the model can
/// repair by re-emitting the same intended action in the correct shape. These
/// are Runtime-owned error strings (the governed tool contracts), never model
/// output, so matching them cannot be gamed by the model.
fn repairable_tool_input_error(reason: &str) -> bool {
    const MARKERS: [&str; 6] = [
        "requires a non-empty",
        "unknown field",
        "expected one of",
        "invalid_input",
        "input_contract",
        "is not a $reference",
    ];
    MARKERS.iter().any(|marker| reason.contains(marker))
}

pub(super) fn provider_frame_repair_attempt(
    protocol_failure: bool,
    tool_exposure_miss: bool,
    post_receipt_failure: bool,
    terminal_provider_route_failure: bool,
    clean_terminal_synthesis: bool,
    attempts_so_far: u8,
) -> Option<u8> {
    (protocol_failure
        && !tool_exposure_miss
        && post_receipt_failure
        && !terminal_provider_route_failure
        && !clean_terminal_synthesis
        && attempts_so_far < PROVIDER_FRAME_REPAIR_BUDGET)
        .then_some(attempts_so_far.saturating_add(1))
}

#[test]
fn a_tool_input_contract_error_is_repairable_but_a_transport_error_is_not() {
    // The governed tool contracts' own error strings name a shape the model can
    // re-emit; a transport/route failure cannot be repaired by re-emitting.
    assert!(repairable_tool_input_error(
        "agent_action_batch requires a non-empty `actions` array"
    ));
    assert!(
        repairable_tool_input_error(
            "task_publish evidence tool://x has no canonical durable receipt"
        ) == false
    );
    assert!(
        repairable_tool_input_error("provider stream attempt failed before terminal completion")
            == false
    );
}

#[test]
#[cfg(test)]
fn malformed_frame_earns_one_repair_attempt_even_after_receipts() {
    assert_eq!(
        provider_frame_repair_attempt(true, false, true, false, false, 0),
        Some(1)
    );
    // The budget is a hard bound: a second identical defect escalates.
    assert_eq!(
        provider_frame_repair_attempt(true, false, true, false, false, 1),
        None
    );
    // Not a frame failure at all.
    assert_eq!(
        provider_frame_repair_attempt(false, false, true, false, false, 0),
        None
    );
    // Tool-exposure misses own their governed retry.
    assert_eq!(
        provider_frame_repair_attempt(true, true, true, false, false, 0),
        None
    );
    // Without prior receipts the existing single protocol retry owns the case.
    assert_eq!(
        provider_frame_repair_attempt(true, false, false, false, false, 0),
        None
    );
    // An unavailable route is not repairable by re-emitting a frame.
    assert_eq!(
        provider_frame_repair_attempt(true, false, true, true, false, 0),
        None
    );
    // A zero-tool synthesis step has no frame contract to repair.
    assert_eq!(
        provider_frame_repair_attempt(true, false, true, false, true, 0),
        None
    );
}

/// How many consecutive identical (same tool set, canonical args and result)
/// tool batches before a loop-breaker notice is injected into the next model
/// request. The notice is advisory: it gives a weak model a chance to correct
/// itself instead of hard-stopping a turn that may still be productive.
pub(super) fn loop_nudge_after() -> usize {
    std::env::var("COWD_LOOP_NUDGE_AFTER")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3)
}

/// Opt-in hard stop for identical-call loops. Default **off**: matching the
/// loop-breaker-first posture of leading agents, the notice is advisory unless
/// an operator explicitly enables the circuit breaker.
pub(super) fn loop_hard_stop_enabled() -> bool {
    matches!(
        std::env::var("COWD_LOOP_HARD_STOP").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Identical-call streak at which the opt-in hard stop fires.
pub(super) fn loop_block_after() -> usize {
    std::env::var("COWD_LOOP_BLOCK_AFTER")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(5)
}

/// Pollers legitimately re-issue identical calls while waiting on external
/// progress, so an unchanged result is expected rather than a loop. They are
/// exempt from the identical-call notice to avoid false positives.
fn is_loop_nudge_repeatable_tool(tool_name: &str) -> bool {
    let lower = tool_name.to_ascii_lowercase();
    lower == "process" || lower.ends_with("_get_result") || lower.ends_with("_poll")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoopGuardAction {
    Continue,
    Nudge { streak: usize },
    Block { streak: usize },
}

/// Decide what (if anything) to do about a consecutive identical-call streak.
/// Nudge-first; a hard stop only when the operator opted in.
#[must_use]
pub(super) fn evaluate_loop_guard(
    streak: usize,
    nudge_after: usize,
    hard_stop: bool,
    block_after: usize,
) -> LoopGuardAction {
    if streak == 0 || streak < nudge_after {
        return LoopGuardAction::Continue;
    }
    if hard_stop && streak >= block_after {
        return LoopGuardAction::Block { streak };
    }
    LoopGuardAction::Nudge { streak }
}

/// Per-turn cap on how many times a single tool may run within one turn.
/// `COWD_TOOL_CAP_DEFAULT` applies to every tool; `COWD_TOOL_CAP_<TOOL>` (the
/// upper-cased tool name) overrides it for one tool. `0` (the default)
/// disables the cap entirely.
pub(super) fn tool_scope_cap(tool_name: &str) -> usize {
    let specific = format!(
        "COWD_TOOL_CAP_{}",
        tool_name.to_ascii_uppercase().replace('-', "_")
    );
    std::env::var(&specific)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .or_else(|| {
            std::env::var("COWD_TOOL_CAP_DEFAULT")
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0)
}

/// Opt-in result deduplication. When enabled, large byte-identical duplicate
/// tool-result payloads are replaced by a short reference stub so a repeating
/// model cannot inflate the context. Default **off**.
pub(super) fn loop_stub_results_enabled() -> bool {
    matches!(
        std::env::var("COWD_LOOP_STUB_RESULTS").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Payloads smaller than this are not worth stubbing: the stub itself plus the
/// lost locality would outweigh the savings. Error results are never stubbed.
const IDENTICAL_RESULT_STUB_MIN_CHARS: usize = 512;

/// Replace large byte-identical duplicate tool-result payloads with a short
/// reference stub. Returns the number of payloads stubbed. Pure and directly
/// unit-testable; callers only invoke it when `COWD_LOOP_STUB_RESULTS` is on.
fn stub_duplicate_result_payloads(messages: &mut [ConversationMessage]) -> usize {
    let mut first_seen: std::collections::HashMap<(String, String), String> =
        std::collections::HashMap::new();
    let mut stubbed = 0usize;
    for message in messages.iter_mut() {
        for block in message.blocks.iter_mut() {
            let ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error,
            } = block
            else {
                continue;
            };
            if *is_error || output.len() < IDENTICAL_RESULT_STUB_MIN_CHARS {
                continue;
            }
            let key = (tool_name.clone(), output.clone());
            match first_seen.get(&key) {
                Some(origin) => {
                    *output =
                        format!("[duplicate identical `{tool_name}` result omitted; see {origin}]");
                    stubbed = stubbed.saturating_add(1);
                }
                None => {
                    first_seen.insert(key, format!("tool_call:{tool_use_id}"));
                }
            }
        }
    }
    stubbed
}

/// Optional objective-level wall-clock cost ceiling for a required-Team
/// collaboration turn. Configurable with `COWD_COLLABORATION_BUDGET_SECS`;
/// **unset or `0` disables it** (the default). When enabled it is deliberately
/// a rare last-resort cost guard, not the primary terminus: the no-progress
/// fuse, the loop-breaker and the tool caps bound turns by *progress/side
/// effects*, while this only bounds raw spend. A turn that keeps making real
/// progress must never be fused by elapsed time alone.
pub(super) fn collaboration_wall_budget() -> Option<std::time::Duration> {
    std::env::var("COWD_COLLABORATION_BUDGET_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(std::time::Duration::from_secs)
}

/// The attempt count a protocol is escalated to when it has had no recovery
/// attempt at all within the deadline. It is past the fuse's retry budget, so the
/// existing no-progress terminus fires instead of replanning again.
const NO_RECOVERY_ATTEMPT_CEILING: u8 = 3;

/// How long a delegated closure protocol may sit with **zero** recovery attempts
/// before Runtime stops waiting.
///
/// The no-progress fuse counts recovery attempts, and an attempt is only recorded
/// when the model answers and fails to close. A model that stops answering
/// therefore never increments the counter, so the fuse never fires: one live run
/// sat for twenty-three minutes after its last action with a Task stuck in
/// `Claimed` until the harness timeout cancelled it. This guard covers exactly
/// that shape - a published protocol the model never responded to again - and
/// deliberately leaves every other turn alone, because a turn that keeps
/// answering always has attempts.
/// Consecutive root model steps with every framework-owned closure rung
/// satisfied before Runtime drives the completion request on the root's
/// behalf. A weak model can be woken by `all_tasks_accepted` and then
/// coordinate indefinitely without making the exit decision; the grace period
/// keeps a model that is merely mid-thought from being pre-empted. The drive
/// itself stays behind the completion_gap gate, so this can never force an
/// incomplete Objective.
fn closure_drive_after_steps() -> usize {
    std::env::var("COWD_CLOSURE_DRIVE_AFTER_STEPS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3)
}

#[test]
fn closure_drive_grace_period_defaults_to_three_steps() {
    // The grace period must be positive: zero would pre-empt a model that is
    // merely mid-thought on its first ready step.
    assert!(closure_drive_after_steps() >= 1);
}

fn zero_recovery_deadline() -> std::time::Duration {
    std::env::var("COWD_ZERO_RECOVERY_DEADLINE_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(std::time::Duration::from_secs)
        .unwrap_or(std::time::Duration::from_secs(600))
}

/// Whether a pending protocol reached the ceiling by elapsed time alone.
///
/// A model that keeps failing to close records attempts; a model that stopped
/// answering records none. Both land on the same ceiling number, so the two cannot be
/// told apart from the number afterwards - and "still working" and "stuck" are
/// different facts that an operator must be able to distinguish.
fn zero_recovery_stall(attempts: u8, protocol_pending: bool, elapsed: std::time::Duration) -> bool {
    protocol_pending && attempts == 0 && elapsed >= zero_recovery_deadline()
}

/// The attempt count for a pending delegated closure, plus the durable record when the
/// ceiling was reached by elapsed time alone.
///
/// The two branches produce the same number, so the record has to be written where the
/// branch is known. Reading it back from the returned number cannot tell "still working"
/// from "stuck", and that distinction is the whole point of the event.
fn guarded_delegation_attempt(
    services: &crate::RuntimeServices,
    state: &mut TurnGraphState,
    ticket: &NodeExecutionTicket,
    post_receipt_failure: bool,
    protocol: Option<&DelegatedAgenticProtocolState>,
) -> Option<u8> {
    let elapsed = state.started_at.elapsed();
    let attempt = delegation_attempt_count(
        &mut state.agentic_protocol_recovery_attempts,
        post_receipt_failure,
        protocol.is_some(),
        elapsed,
    );
    if let Some(protocol) = protocol
        .filter(|_| zero_recovery_stall(state.agentic_protocol_recovery_attempts, true, elapsed))
    {
        record_program_closure_timeout(
            services,
            &state.session_id,
            &state.turn_id,
            ticket,
            protocol,
            elapsed,
        );
    }
    attempt
}

/// The durable record of a stalled delegated closure.
///
/// The guard changes an in-memory counter and nothing else, so the event stream shows
/// a turn that simply stopped: nothing says which Program Task it was stuck on, or that
/// the stall was recognized at all. Naming the Program and the Task is the whole
/// content of this event, and it is what makes "still working" and "stuck" separable
/// after the fact.
fn record_program_closure_timeout(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
    ticket: &NodeExecutionTicket,
    protocol: &DelegatedAgenticProtocolState,
    elapsed: std::time::Duration,
) {
    let append = services.event_store().append(crate::RuntimeEventInput {
        stream_id: format!("session:{session_id}"),
        scope: crate::RuntimeEventScope::Session,
        kind: "agentic.program_closure_timeout".to_string(),
        status: Some("stalled".to_string()),
        actor: Some("conversation_runtime.delegated_closure".to_string()),
        refs: vec![
            crate::RuntimeEventRef {
                kind: "agentic_program".to_string(),
                id: protocol.program_id.clone(),
            },
            crate::RuntimeEventRef {
                kind: "agentic_task".to_string(),
                id: protocol.task_id.clone(),
            },
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
                id: turn_id.to_string(),
            },
        ],
        payload: serde_json::json!({
            "program_id": protocol.program_id,
            "task_id": protocol.task_id,
            "task_status": format!("{:?}", protocol.status),
            "elapsed_ms": elapsed.as_millis(),
            "deadline_ms": zero_recovery_deadline().as_millis(),
            "reason": "a pending delegated closure recorded no attempt past the deadline",
        }),
    });
    if let Err(error) = append {
        tracing::warn!(%error, "program closure timeout event could not be recorded");
    }
}

/// How far this turn is into the no-progress fuse for a delegated closure.
///
/// Two ways to advance, and both must stay bounded. A model that answers and
/// fails to close records an attempt, so repeated failures eventually pass the
/// fuse's retry budget. A model that stops answering records nothing, and that is
/// the case that used to wait forever: with no attempt there is nothing to
/// exceed, so the fuse never fires and the turn runs until an outer timeout
/// cancels it. When a protocol has had no attempt at all past the deadline it is
/// carried to the ceiling instead, which lands on the same no-progress terminus.

/// What to tell the Goal when a delegated closure is not finished.
///
/// A first or second miss is still a replan with the protocol's own continuation
/// instruction. Past that the reason stops naming an instruction and records no
/// progress, because the same instruction has already been tried twice - a
/// distinct terminus from a protocol violation, which is why it is its own
/// message rather than the generic one.
fn delegation_intervention(
    goal_id: &str,
    ticket: &NodeExecutionTicket,
    protocol: &DelegatedAgenticProtocolState,
    attempt: u8,
) -> RuntimeIntervention {
    RuntimeIntervention {
        goal_id: goal_id.to_string(),
        kind: if attempt <= 2 {
            RuntimeInterventionKind::Replan
        } else {
            RuntimeInterventionKind::Block
        },
        reason: if attempt <= 2 {
            protocol.continuation_instruction()
        } else {
            format!(
                "agentic_protocol_no_progress: Program Task `{}` remained {:?} after two compact closure retries",
                protocol.task_id, protocol.status
            )
        },
        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
        expected_graph_revision: None,
    }
}

/// Which attempt count a pending delegated closure protocol is at.
///
/// Two ways to advance, and both must stay bounded. A model that answers and
/// fails to close records an attempt, so repeated failures eventually pass the
/// fuse's retry budget. A model that stops answering records nothing, and that is
/// the case that used to wait forever: with no attempt there is nothing to
/// exceed, so the fuse never fires and the turn runs until an outer timeout
/// cancels it. When a protocol has had no attempt at all past the deadline it is
/// carried to the ceiling instead, which lands on the same no-progress terminus.
fn delegation_attempt_count(
    attempts: &mut u8,
    post_receipt_failure: bool,
    protocol_pending: bool,
    elapsed: std::time::Duration,
) -> Option<u8> {
    if post_receipt_failure && protocol_pending {
        *attempts = attempts.saturating_add(1);
        return Some(*attempts);
    }
    if !protocol_pending {
        return None;
    }
    (*attempts == 0 && elapsed >= zero_recovery_deadline()).then_some(NO_RECOVERY_ATTEMPT_CEILING)
}

/// True when a required-Team collaboration turn has spent its **enabled**
/// objective-level wall-clock ceiling. A `None` budget (the default) never
/// fires. Delegated leaves never inherit the root's budget, and a turn without
/// a Team obligation is bounded by the no-progress fuse alone.
///
/// The ceiling is deliberately **progress-exempt**: a turn with verified
/// progress is never fused by elapsed time alone. Elapsed time is only
/// consulted once progress has stopped, which is exactly what the no-progress
/// fuse already measures; this guard exists for the case where nominal progress
/// (dispatching/claiming) continues without any *verified* progress. A turn that
/// keeps making real progress must never be stopped for being long-lived.
fn collaboration_budget_elapsed(
    obligation: Option<&harness_contract::strategy::CollaborationExecutionObligation>,
    is_delegated_leaf: bool,
    elapsed: std::time::Duration,
    budget: Option<std::time::Duration>,
    has_verified_progress: bool,
) -> bool {
    budget.is_some_and(|budget| {
        obligation.is_some_and(|obligation| obligation.minimum_team_count > 0)
            && !is_delegated_leaf
            && !has_verified_progress
            && elapsed >= budget
    })
}

/// Optional multiplier for the objective-level **step** ceiling. `0` (the
/// default) disables it. Steps, like seconds, are orthogonal to progress, so
/// this is likewise an operator-selectable guard rather than the primary
/// terminus.
pub(super) fn collaboration_step_ceiling_mult() -> usize {
    std::env::var("COWD_COLLABORATION_STEP_CEILING_MULT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// True when an Agent-first collaboration turn has spent its **enabled**
/// absolute step ceiling without reaching its verified-Team minimum. A `0`
/// multiplier (the default) never fires.
fn collaboration_ceiling_reached(
    obligation: Option<&harness_contract::strategy::CollaborationExecutionObligation>,
    is_delegated_leaf: bool,
    iterations: usize,
    max_model_steps: usize,
    multiplier: usize,
) -> bool {
    multiplier > 0
        && obligation.is_some_and(|obligation| obligation.minimum_team_count > 0)
        && !is_delegated_leaf
        && iterations >= max_model_steps.saturating_mul(multiplier)
}

/// Policy bound on how many times the Agent-first collaboration completion gate
/// may re-drive the root before the turn terminates as an honest partial. This
/// is a convergence bound, never a topology: the model still owns every
/// semantic decision, but it cannot replan without end while the verified-Team
/// minimum stays unmet.
fn collaboration_convergence_replan_bound(
    complexity: harness_contract::core::TaskComplexity,
) -> usize {
    crate::execution_core::ComplexityBudget::derive(complexity).convergence_replans
}

#[cfg(test)]
mod safety_fuse_streak_tests {
    use super::next_unverified_model_step_streak;

    #[test]
    fn verified_progress_resets_consecutive_stall_streak() {
        assert_eq!(next_unverified_model_step_streak(23, true), 0);
        assert_eq!(next_unverified_model_step_streak(0, false), 1);
        assert_eq!(next_unverified_model_step_streak(1, false), 2);
    }

    #[test]
    fn collaboration_convergence_bound_scales_with_complexity() {
        use super::collaboration_convergence_replan_bound;
        use harness_contract::core::TaskComplexity;
        assert_eq!(
            collaboration_convergence_replan_bound(TaskComplexity::Simple),
            2
        );
        assert_eq!(
            collaboration_convergence_replan_bound(TaskComplexity::Strategic),
            6
        );
        assert!(
            collaboration_convergence_replan_bound(TaskComplexity::Strategic)
                > collaboration_convergence_replan_bound(TaskComplexity::Moderate)
        );
    }

    #[test]
    fn collaboration_ceiling_bounds_required_team_turns() {
        use super::collaboration_ceiling_reached;
        use harness_contract::strategy::CollaborationExecutionObligation;
        let obligation = CollaborationExecutionObligation {
            minimum_team_count: 3,
            exact_team_count: None,
        };
        assert!(!collaboration_ceiling_reached(
            Some(&obligation),
            false,
            5,
            24,
            3
        ));
        assert!(collaboration_ceiling_reached(
            Some(&obligation),
            false,
            72,
            24,
            3
        ));
        // A delegated leaf never inherits the root's Team obligation.
        assert!(!collaboration_ceiling_reached(
            Some(&obligation),
            true,
            999,
            24,
            3
        ));
        // Without an obligation only the no-progress fuse decides.
        assert!(!collaboration_ceiling_reached(None, false, 999, 24, 3));
        // A zero multiplier disables the ceiling entirely (the default).
        assert!(!collaboration_ceiling_reached(
            Some(&obligation),
            false,
            999_999,
            24,
            0
        ));
    }

    #[test]
    fn collaboration_wall_budget_bounds_required_team_turns() {
        use super::collaboration_budget_elapsed;
        use harness_contract::strategy::CollaborationExecutionObligation;
        use std::time::Duration;
        let obligation = CollaborationExecutionObligation {
            minimum_team_count: 3,
            exact_team_count: None,
        };
        assert!(collaboration_budget_elapsed(
            Some(&obligation),
            false,
            Duration::from_secs(100),
            Some(Duration::from_secs(100)),
            false
        ));
        assert!(!collaboration_budget_elapsed(
            Some(&obligation),
            false,
            Duration::from_secs(99),
            Some(Duration::from_secs(100)),
            false
        ));
        assert!(!collaboration_budget_elapsed(
            Some(&obligation),
            true,
            Duration::from_secs(10_000),
            Some(Duration::from_secs(100)),
            false
        ));
        assert!(!collaboration_budget_elapsed(
            None,
            false,
            Duration::from_secs(10_000),
            Some(Duration::from_secs(100)),
            false
        ));
        // Disabled (the default) never bounds by time, no matter how long the
        // turn has run: a turn that keeps making real progress must not be
        // fused by elapsed time alone.
        assert!(!collaboration_budget_elapsed(
            Some(&obligation),
            false,
            Duration::from_secs(10_000_000),
            None,
            false
        ));
        // The ceiling is progress-exempt: a turn with verified progress is
        // never fused by elapsed time, however long it has run.
        assert!(!collaboration_budget_elapsed(
            Some(&obligation),
            false,
            Duration::from_secs(10_000_000),
            Some(Duration::from_secs(100)),
            true
        ));
    }

    #[test]
    fn loop_guard_is_nudge_first_with_opt_in_hard_stop() {
        use super::{LoopGuardAction, evaluate_loop_guard};
        assert_eq!(
            evaluate_loop_guard(0, 3, false, 5),
            LoopGuardAction::Continue
        );
        assert_eq!(
            evaluate_loop_guard(2, 3, false, 5),
            LoopGuardAction::Continue
        );
        assert_eq!(
            evaluate_loop_guard(3, 3, false, 5),
            LoopGuardAction::Nudge { streak: 3 }
        );
        // Advisory by default: even far past the block threshold it only nudges.
        assert_eq!(
            evaluate_loop_guard(50, 3, false, 5),
            LoopGuardAction::Nudge { streak: 50 }
        );
        // With the opt-in circuit breaker it blocks at the block threshold.
        assert_eq!(
            evaluate_loop_guard(4, 3, true, 5),
            LoopGuardAction::Nudge { streak: 4 }
        );
        assert_eq!(
            evaluate_loop_guard(5, 3, true, 5),
            LoopGuardAction::Block { streak: 5 }
        );
    }

    #[test]
    fn duplicate_result_payloads_are_stubbed_except_errors_and_small_payloads() {
        use super::{ContentBlock, ConversationMessage, stub_duplicate_result_payloads};
        let big = "x".repeat(600);
        let small = "tiny".to_string();
        let mut messages = vec![
            ConversationMessage::tool_result("call-1", "read_file", big.clone(), false),
            ConversationMessage::tool_result("call-2", "read_file", big.clone(), false),
            ConversationMessage::tool_result("call-3", "read_file", small.clone(), false),
            ConversationMessage::tool_result("call-4", "read_file", small.clone(), false),
            ConversationMessage::tool_result("call-5", "read_file", big.clone(), true),
        ];
        let stubbed = stub_duplicate_result_payloads(&mut messages);
        // Only the second large successful payload is a stub candidate: small
        // payloads are left alone and error payloads are never stubbed.
        assert_eq!(stubbed, 1);
        let outputs = messages
            .iter()
            .map(|message| match message.blocks.first() {
                Some(ContentBlock::ToolResult { output, .. }) => output.clone(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(outputs[0], big);
        assert!(outputs[1].contains("duplicate identical"));
        assert_eq!(outputs[2], small);
        assert_eq!(outputs[3], small);
        assert_eq!(outputs[4], big);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AgenticContentDraftScope {
    pub(super) execution_id: String,
    pub(super) node_id: String,
    pub(super) attempt: u32,
    pub(super) actor_id: String,
    /// The Task this draft belongs to. A draft is never reused across Tasks: a body
    /// written for one Task is not a deliverable for another, so the fallback that
    /// recovers a body written in an earlier step must be pinned to the same Task.
    pub(super) task_id: Option<String>,
}

impl AgenticContentDraftScope {
    fn stream_id(&self) -> String {
        format!(
            "agentic-content-draft:{}",
            sha256_digest(&format!(
                "{}\n{}\n{}\n{}\n{}",
                self.execution_id,
                self.node_id,
                self.attempt,
                self.actor_id,
                self.task_id.as_deref().unwrap_or("")
            ))
        )
    }

    fn pin_owner(&self) -> String {
        format!("agentic-content:{}", self.stream_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AgenticContentDraft {
    pub(super) content_ref: String,
    content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DelegatedAgenticProtocolState {
    pub(super) program_id: String,
    pub(super) task_id: String,
    pub(super) mode: String,
    pub(super) status: crate::AgenticTaskStatus,
    pub(super) artifact_refs: Vec<String>,
    pub(super) artifact_evidence_refs: Vec<String>,
    pub(super) owns_active_attempt: bool,
    /// The reviewer's rework reason, surfaced to the worker on the next
    /// `Rework` attempt so it knows what concretely must change to pass.
    pub(super) review_reason: Option<String>,
}

impl DelegatedAgenticProtocolState {
    pub(super) fn is_terminal(&self) -> bool {
        if !self.owns_active_attempt {
            return true;
        }
        match self.mode.as_str() {
            "review" => matches!(
                self.status,
                crate::AgenticTaskStatus::Accepted
                    | crate::AgenticTaskStatus::Rework
                    | crate::AgenticTaskStatus::Blocked
            ),
            _ => matches!(
                self.status,
                crate::AgenticTaskStatus::Submitted
                    | crate::AgenticTaskStatus::Accepted
                    | crate::AgenticTaskStatus::Blocked
            ),
        }
    }

    pub(super) fn continuation_instruction(&self) -> String {
        let rework_note = self.rework_note();
        match self.mode.as_str() {
            "review" => format!(
                "Runtime collaboration protocol: review Task `{}` is still {:?}. Independently inspect the exact submitted artifact_refs [{}] through evidence refs [{}], then use `task_review`. Accept only if its acceptance criterion is proved; otherwise request rework with a concrete reason. Cite the inspected evidence and do not return prose before the durable review action succeeds. Program `{}`.",
                self.task_id,
                self.status,
                self.artifact_refs.join(", "),
                self.artifact_evidence_refs.join(", "),
                self.program_id
            ),
            _ if self.artifact_refs.is_empty() => format!(
                "Runtime collaboration protocol: execute Task `{}` is still {:?}. Publish an explicit file/hash via `artifact_publish`, or select an exact text block in the current response with `artifact_commit` content_ref=current_message_block:<zero-based-index>. Never assume a prior draft is selected. Commit the returned content_ref related to this exact Task, then call `task_submit` with its returned `artifact:...` changed_ref plus real durable source/test/tool evidence refs. Runtime binds artifact content automatically; do not duplicate the internal artifact:// selector as evidence. Do not return prose before durable submission succeeds. Program `{}`.{}",
                self.task_id, self.status, self.program_id, rework_note
            ),
            _ => format!(
                "Runtime collaboration protocol: artifact evidence for Task `{}` is durable. Call `task_submit` now with artifact_refs [{}] and evidence_refs [{}]; do not repeat research, recommit the artifact, guess references, or return prose before submission succeeds. The Task is still {:?}. Program `{}`.{}",
                self.task_id,
                self.artifact_refs.join(", "),
                self.artifact_evidence_refs.join(", "),
                self.status,
                self.program_id,
                rework_note
            ),
        }
    }

    /// The reviewer's rework reason, surfaced to the worker so a `Rework`
    /// attempt knows the concrete unmet criterion instead of repeating work.
    /// `review_reason` is durable on the Task but was previously write-only.
    fn rework_note(&self) -> String {
        // Surface the reason only while the Task is actually in Rework, so a
        // re-arming attempt after a physical review failure (which returns the
        // Task to Published) cannot repeat a stale reason.
        if self.status != crate::AgenticTaskStatus::Rework {
            return String::new();
        }
        match self.review_reason.as_deref() {
            Some(reason) if !reason.trim().is_empty() => format!(
                " A previous review requested rework for this exact reason, which you must resolve before resubmitting: {reason}"
            ),
            _ => String::new(),
        }
    }

    /// The one durable protocol action that can close the current Agent
    /// boundary. Runtime uses this only after the model itself has exhausted
    /// its evidence work or attempted a text terminal; it never narrows the
    /// Agent's ordinary research/planning tool surface. That split preserves
    /// semantic autonomy while preventing a generic presentation synthesis
    /// from orphaning a claimed Task between its artifact and submission.
    pub(super) fn closure_tool_ids(&self) -> BTreeSet<String> {
        let tool = match self.mode.as_str() {
            "review" => harness_contract::agent_action::TASK_REVIEW_TOOL_ID,
            _ if self.artifact_refs.is_empty() => {
                harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID
            }
            _ => harness_contract::agent_action::TASK_SUBMIT_TOOL_ID,
        };
        [tool.to_string()].into_iter().collect()
    }
}

pub(super) fn pending_delegated_action_is_ready(
    protocol: Option<&DelegatedAgenticProtocolState>,
    repeated_evidence_saturation: bool,
    has_successful_tool_evidence: bool,
) -> bool {
    repeated_evidence_saturation
        && has_successful_tool_evidence
        && protocol.is_some_and(|protocol| !protocol.is_terminal())
}

pub(super) fn delegated_agentic_protocol_state(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
) -> Result<Option<DelegatedAgenticProtocolState>, NodeExecutorError> {
    let protocol_error = |reason: String| NodeExecutorError::Poll {
        node_id: ticket.node_id.clone(),
        reason,
    };
    let graph = services
        .graph_state_store()
        .load(&ticket.graph_id)
        .map_err(|error| protocol_error(format!("load delegated conversation graph: {error}")))?;
    let Some(parent) = graph.parent_execution.as_ref() else {
        return Ok(None);
    };
    let parent_graph = services
        .graph_state_store()
        .load(&parent.execution_id)
        .map_err(|error| protocol_error(format!("load parent execution graph: {error}")))?;
    let Some(node) = parent_graph
        .nodes
        .iter()
        .find(|node| node.id == parent.node_id)
    else {
        return Err(protocol_error(format!(
            "parent execution node `{}` is missing",
            parent.node_id
        )));
    };
    if node.kind != ExecutionNodeKind::AgentTask {
        return Ok(None);
    }
    let packet =
        serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&node.payload_ref)
            .map_err(|error| {
                protocol_error(format!("decode delegated AgentTask packet: {error}"))
            })?;
    let Some(agentic) = packet.agentic_binding.as_ref() else {
        return Ok(None);
    };
    let program_id = agentic.program_id.clone();
    let (task_id, mode) = match &agentic.focus {
        harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } => {
            (task_ref.clone(), "execute")
        }
        harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => {
            (task_ref.clone(), "review")
        }
        _ => return Ok(None),
    };
    let projection = services
        .agent_action_service()
        .project(&program_id)
        .map_err(|error| protocol_error(format!("project Agent-first Program: {error}")))?;
    let task = projection.tasks.get(&task_id).ok_or_else(|| {
        protocol_error(format!(
            "Agent-first Program `{program_id}` has no Task `{task_id}`"
        ))
    })?;
    // `AgentTaskPacket::agent_id()` is the physical Binding instance identity
    // (for run locks, cancellation and process isolation). Agent-first
    // Program state is instead owned by the immutable logical identity that
    // Runtime attested in `agentic_binding`. These IDs intentionally differ
    // in production. Comparing the physical instance to Task.claimant made a
    // live claim appear foreign, which allowed generic text synthesis to
    // preempt artifact/submit closure and caused cross-graph retry loops.
    let actor_id = agentic.agent_id.as_str();
    let mut artifacts = projection
        .artifacts
        .values()
        .filter(|artifact| match mode {
            // A review is intentionally bound to the submitted artifact set,
            // which TaskSubmit has already fenced to its claim.
            "review" => task.artifact_refs.contains(&artifact.artifact_ref),
            // Execution must see only evidence committed by this exact
            // physical claim. A rework retains historical artifacts for
            // auditability, but those artifacts cannot close its new attempt.
            _ => {
                artifact.committed_by == actor_id
                    && artifact.relates_to.contains(&task_id)
                    && artifact.claim_execution_id.as_deref()
                        == Some(packet.assignment.graph_id.as_str())
                    && artifact.claim_generation == Some(u64::from(packet.attempt))
            }
        })
        .map(|artifact| (artifact.artifact_ref.clone(), artifact.content_ref.clone()))
        .collect::<Vec<_>>();
    artifacts.sort();
    artifacts.dedup();
    let owns_active_attempt = match mode {
        "execute" => {
            task.status == crate::AgenticTaskStatus::Claimed
                && task.claimant.as_deref() == Some(actor_id)
                && task.claim_execution_id.as_deref() == Some(packet.assignment.graph_id.as_str())
                && task.claim_generation == u64::from(packet.attempt)
        }
        "review" => {
            task.status == crate::AgenticTaskStatus::Submitted
                && task.claimant.as_deref() != Some(actor_id)
                && task.review_generation.saturating_add(1) == u64::from(packet.attempt)
        }
        _ => false,
    };
    Ok(Some(DelegatedAgenticProtocolState {
        program_id,
        task_id,
        mode: mode.to_string(),
        status: task.status,
        artifact_refs: artifacts
            .iter()
            .map(|(artifact_ref, _)| artifact_ref.clone())
            .collect(),
        artifact_evidence_refs: artifacts
            .iter()
            .map(|(_, content_ref)| content_ref.clone())
            .collect(),
        owns_active_attempt,
        review_reason: task.review_reason.clone(),
    }))
}

// Artifact pin timestamps use signed 64-bit durable values. This is effectively
// permanent while still remaining representable by the durable repository.
const AGENTIC_CONTENT_DRAFT_PIN_UNTIL_MS: u64 = i64::MAX as u64;

/// Resolve the stable logical execution fence behind dynamic model-step
/// nodes. A delegated Conversation graph points back to exactly one immutable
/// AgentTask packet; using that packet's actor/node/attempt keeps consecutive
/// model steps together without allowing another Agent in the same Program to
/// observe the draft. Root turns fall back to their unique execution graph.
fn agentic_content_draft_scope(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
) -> AgenticContentDraftScope {
    let root_scope = || AgenticContentDraftScope {
        execution_id: ticket.graph_id.clone(),
        node_id: ticket.graph_id.clone(),
        attempt: ticket.attempt,
        actor_id: format!("root:{}", ticket.graph_id),
        task_id: None,
    };
    let Some(packet) = delegated_agent_task_packet(services, ticket) else {
        return root_scope();
    };
    AgenticContentDraftScope {
        execution_id: packet.assignment.graph_id.clone(),
        node_id: packet.assignment.node_id.clone(),
        attempt: packet.attempt,
        actor_id: format!("{}:{}", packet.agent_id(), packet.run_id()),
        task_id: Some(packet.task_id().to_string()),
    }
}

/// Resolve the immutable AgentTask packet behind a delegated Conversation
/// graph. Dynamic model-step graphs point at the Agent node in their parent
/// graph, so inspecting only the current graph loses both Session visibility
/// and the stable Agent attempt identity.
fn delegated_input_task_packet(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
) -> Option<harness_contract::agent::AgentTaskPacket> {
    let graph = services.graph_state_store().load(&ticket.graph_id).ok()?;
    let parent = graph.parent_execution.as_ref()?;
    let parent_graph = services
        .graph_state_store()
        .load(&parent.execution_id)
        .ok()?;
    let node = parent_graph
        .nodes
        .iter()
        .find(|node| node.id == parent.node_id && node.kind == ExecutionNodeKind::AgentTask)?;
    let packet: harness_contract::agent::AgentTaskPacket =
        serde_json::from_str(&node.payload_ref).ok()?;
    (packet.graph_id() == parent.execution_id && packet.node_id() == parent.node_id)
        .then_some(packet)
}

fn delegated_agent_task_packet(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
) -> Option<harness_contract::agent::AgentTaskPacket> {
    let graph = services.graph_state_store().load(&ticket.graph_id).ok()?;
    let payload_ref = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::AgentTask)
        .map(|node| node.payload_ref.clone())
        .or_else(|| {
            let parent = graph.parent_execution.as_ref()?;
            let parent_graph = services
                .graph_state_store()
                .load(&parent.execution_id)
                .ok()?;
            parent_graph
                .nodes
                .into_iter()
                .find(|node| node.id == parent.node_id && node.kind == ExecutionNodeKind::AgentTask)
                .map(|node| node.payload_ref)
        })?;
    let packet =
        serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&payload_ref).ok()?;
    packet.agentic_binding.is_some().then_some(packet)
}

fn agentic_message_text(message: &ConversationMessage) -> Option<String> {
    let content = message
        .blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } if !text.trim().is_empty() => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    (!content.trim().is_empty()).then_some(content)
}

/// Read a persisted draft body back as text.
fn agentic_content_draft_text(
    services: &crate::RuntimeServices,
    draft: &AgenticContentDraft,
) -> Option<String> {
    let artifact = services.artifact_store().resolve(&draft.content_ref).ok()?;
    let bytes = services
        .artifact_store()
        .read_blocking(&artifact, &artifact.visibility_scope, None)
        .ok()?;
    String::from_utf8(bytes).ok()
}

pub(super) fn latest_agentic_content_draft(
    services: &crate::RuntimeServices,
    scope: &AgenticContentDraftScope,
) -> Result<Option<AgenticContentDraft>, NodeExecutorError> {
    let stream_id = scope.stream_id();
    let event = services
        .event_store()
        .list_stream_page_desc(&stream_id, 1, 0)
        .map_err(|error| NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: format!("load preceding Agent content draft: {error}"),
        })?
        .into_iter()
        .next();
    let Some(event) = event else {
        return Ok(None);
    };
    let exact_scope = event.kind == "agentic.content_draft.persisted"
        && event
            .payload
            .get("execution_id")
            .and_then(serde_json::Value::as_str)
            == Some(scope.execution_id.as_str())
        && event
            .payload
            .get("node_id")
            .and_then(serde_json::Value::as_str)
            == Some(scope.node_id.as_str())
        && event
            .payload
            .get("attempt")
            .and_then(serde_json::Value::as_u64)
            == Some(u64::from(scope.attempt))
        && event
            .payload
            .get("actor_id")
            .and_then(serde_json::Value::as_str)
            == Some(scope.actor_id.as_str());
    if !exact_scope {
        return Err(NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: "preceding Agent content draft scope is corrupt".to_string(),
        });
    }
    let content_ref = event
        .payload
        .get("content_ref")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: "preceding Agent content draft has no durable content_ref".to_string(),
        })?
        .to_string();
    let content_sha256 = event
        .payload
        .get("content_sha256")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: "preceding Agent content draft has no content digest".to_string(),
        })?
        .to_string();
    services
        .artifact_store()
        .resolve(&content_ref)
        .map_err(|error| NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: format!("resolve preceding Agent content artifact: {error}"),
        })?;
    Ok(Some(AgenticContentDraft {
        content_ref,
        content_sha256,
    }))
}

pub(super) async fn persist_agentic_content_draft_for_scope(
    services: &crate::RuntimeServices,
    scope: &AgenticContentDraftScope,
    visibility_scope: String,
    content: &str,
) -> Result<String, NodeExecutorError> {
    let content_sha256 = sha256_digest(content);
    let previous = latest_agentic_content_draft(services, scope)?;
    if let Some(previous) = previous
        .as_ref()
        .filter(|draft| draft.content_sha256 == content_sha256)
    {
        return Ok(previous.content_ref.clone());
    }
    let artifact = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown; charset=utf-8".to_string(),
                visibility_scope,
                expected_bytes: Some(content.len() as u64),
                original_name: Some(format!(
                    "agent-content-{}.md",
                    scope.node_id.replace([':', '/'], "-")
                )),
            },
            content.as_bytes(),
        )
        .await
        .map_err(|error| NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: format!("persist preceding Agent content: {error}"),
        })?;
    services
        .artifact_store()
        .pin(
            &artifact,
            &scope.pin_owner(),
            AGENTIC_CONTENT_DRAFT_PIN_UNTIL_MS,
        )
        .map_err(|error| NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: format!("pin preceding Agent content: {error}"),
        })?;
    let append = services.event_store().append(crate::RuntimeEventInput {
        stream_id: scope.stream_id(),
        scope: crate::RuntimeEventScope::ExecutionNode,
        kind: "agentic.content_draft.persisted".to_string(),
        status: Some("available".to_string()),
        actor: Some(scope.actor_id.clone()),
        refs: vec![
            crate::RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: scope.execution_id.clone(),
            },
            crate::RuntimeEventRef {
                kind: "execution_node".to_string(),
                id: scope.node_id.clone(),
            },
            crate::RuntimeEventRef {
                kind: "artifact".to_string(),
                id: artifact.selector.clone(),
            },
        ],
        payload: serde_json::json!({
            "execution_id": scope.execution_id,
            "node_id": scope.node_id,
            "attempt": scope.attempt,
            "actor_id": scope.actor_id,
            "content_ref": artifact.selector,
            "content_sha256": content_sha256,
        }),
    });
    if let Err(error) = append {
        let _ = services
            .artifact_store()
            .unpin(&artifact, &scope.pin_owner());
        return Err(NodeExecutorError::Poll {
            node_id: scope.node_id.clone(),
            reason: format!("index preceding Agent content draft: {error}"),
        });
    }
    if let Some(previous) = previous {
        if let Ok(previous_artifact) = services.artifact_store().resolve(&previous.content_ref) {
            let _ = services
                .artifact_store()
                .unpin(&previous_artifact, &scope.pin_owner());
        }
    }
    Ok(artifact.selector)
}

pub(super) async fn persist_agentic_content_draft(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    message: &ConversationMessage,
) -> Result<Option<String>, NodeExecutorError> {
    let Some(content) = agentic_message_text(message) else {
        return Ok(None);
    };
    let scope = agentic_content_draft_scope(services, ticket);
    persist_agentic_content_draft_for_scope(
        services,
        &scope,
        message_artifact_visibility(services, message, ticket),
        &content,
    )
    .await
    .map(Some)
}

/// A current-message source names an exact block. Never substitute a nearby
/// paragraph or a previous draft when that block is absent.
/// Choose the one-shot Task deliverable body from the model's response.
///
/// A weak model cannot reliably count response blocks, so a requested block is
/// used only when it is substantive; otherwise the longest non-empty Text block
/// of this response is delivered. The content is always the model's own words.
fn select_one_shot_deliverable_text(
    message: &ConversationMessage,
    hint: Option<usize>,
) -> Option<(usize, &str)> {
    const SUBSTANTIVE_MIN_CHARS: usize = 200;
    let texts: Vec<(usize, &str)> = message
        .blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| match block {
            ContentBlock::Text { text } if !text.trim().is_empty() => Some((index, text.as_str())),
            _ => None,
        })
        .collect();
    if let Some(hint) = hint {
        if let Some((index, text)) = texts
            .iter()
            .find(|(index, _)| *index == hint)
            .filter(|(_, text)| text.trim().len() >= SUBSTANTIVE_MIN_CHARS)
        {
            return Some((*index, *text));
        }
    }
    texts.into_iter().rev().next()
}

pub(super) async fn resolve_explicit_agentic_content_refs(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    message: &ConversationMessage,
    calls: &mut [ModelToolCall],
) -> Result<(), NodeExecutorError> {
    for call in calls {
        let is_commit = call.name == harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID;
        let is_submit = call.name == harness_contract::agent_action::TASK_SUBMIT_TOOL_ID;
        let is_review = call.name == harness_contract::agent_action::TASK_REVIEW_TOOL_ID;
        let is_publish = call.name == harness_contract::agent_action::MESSAGE_PUBLISH_TOOL_ID;
        // A read names a durable reference too, and it is the one place where a
        // spelling mistake is silent: `read_ref` on `artifact:<id>` resolves to
        // `Bare` and is delegated to `state_inspect`, so the caller receives
        // program guidance in place of the artifact body. The reviewers that
        // must physically read every cited result therefore never accumulate
        // coverage and are refused with `review_requires_current_physical_reads`.
        // Normalize the read's reference before the delegation happens.
        let is_read = call.name == "read_ref" || call.name == "evidence_retrieve";
        if !is_commit && !is_submit && !is_review && !is_publish && !is_read {
            continue;
        }
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&call.input) else {
            continue;
        };
        // Normalize bare evidence references to their durable scheme so a model
        // that dropped `tool://`/`artifact://` is not rejected with
        // unsupported_evidence_ref / issue_disposition_requires_durable_evidence.
        normalize_bare_evidence_refs(&mut value);
        if is_read {
            // A read carries nothing else to resolve: it has no message block,
            // no deliverable and no verdict. Normalizing its reference is the
            // whole job, so re-encode and move on.
            if let Some(object) = value.as_object_mut() {
                for field in ["reference", "evidence_ref"] {
                    let Some(raw) = object.get(field).and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let normalized = normalize_bare_evidence_ref(raw);
                    if normalized != raw {
                        object.insert(field.to_string(), serde_json::Value::String(normalized));
                    }
                }
            }
            call.input =
                serde_json::to_string(&value).map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("encode normalized read reference: {error}"),
                })?;
            continue;
        }
        if is_review || is_publish {
            // A review verdict and a discussion publish carry no publishable
            // artifact body; only their evidence references needed normalizing.
            call.input =
                serde_json::to_string(&value).map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("encode normalized evidence: {error}"),
                })?;
            continue;
        }
        // Both shapes name exactly one Text block of THIS model response:
        // `artifact_commit.content_ref=current_message_block:<i>` and the
        // one-shot `task_submit.deliverable.block_index`.
        let index = if is_commit {
            value
                .get("content_ref")
                .and_then(serde_json::Value::as_str)
                .and_then(|reference| reference.strip_prefix("current_message_block:"))
                .and_then(|index| index.parse::<usize>().ok())
        } else {
            None
        };
        // One-shot deliverable: a weak model must not have to count response
        // blocks. If `artifact_commit` names an exact block, honour it; for the
        // one-shot submit instead take the requested block when it is
        // substantive, otherwise the longest non-empty Text block of this
        // response. Content is always the model's own words.
        let one_shot_hint = if is_commit {
            None
        } else {
            value
                .get("deliverable")
                .and_then(|deliverable| deliverable.get("block_index"))
                .and_then(serde_json::Value::as_u64)
                .map(|index| index as usize)
        };
        // The commit reference can be:
        //   artifact://...            -> already durable; leave it untouched
        //   current_message_block:<i> -> exactly one Text block of this response
        //   message://current         -> this whole response's body
        //   missing / anything else   -> this whole response's body
        // A weak model must never persist a bodyless artifact, so every
        // unresolvable reference falls back to the model's own words here. An
        // empty response is still refused below, and an out-of-range explicit
        // block stays unresolved by contract.
        let raw_ref = if is_commit {
            value
                .get("content_ref")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
        } else {
            ""
        };
        let is_artifact_ref = raw_ref.starts_with("artifact://");
        let is_block_ref = raw_ref.starts_with("current_message_block:");
        let selected: Option<(usize, &str)> = if !is_commit {
            select_one_shot_deliverable_text(message, one_shot_hint)
        } else if is_artifact_ref {
            // A durable artifact reference already carries its own body.
            None
        } else if !is_block_ref {
            // `message://current`, an empty reference, or any other unusable
            // value: use this response's longest substantive Text block.
            select_one_shot_deliverable_text(message, None)
        } else {
            match index {
                Some(index) if index < message.blocks.len() => match &message.blocks[index] {
                    ContentBlock::Text { text } if !text.trim().is_empty() => {
                        Some((index, text.as_str()))
                    }
                    // In range but not a non-empty Text block: fall back to this
                    // response's longest non-empty Text block.
                    _ => select_one_shot_deliverable_text(message, Some(index)),
                },
                // Out of range stays unresolved by contract: a missing block must
                // never substitute unrelated content such as a preamble paragraph.
                _ => None,
            }
        };
        // A body written in an earlier step of the same Task is still this Agent's own
        // deliverable. Runtime used to resolve only the current response, so a model
        // whose habit is "write the report, then commit a few steps later" could never
        // submit: in d6v10 one Agent wrote a 3795-character synthesis report across three
        // steps and lost all thirteen of its commits to `content_ref_unresolved`, because
        // no text block and no commit ever shared a step. The draft ledger is already
        // pinned to one execution, node, attempt, actor and Task, so this recovers the
        // same body and never crosses a Task fence.
        // Only a reference that names no block at all falls back. An explicit
        // `current_message_block:<i>` that is out of range stays unresolved: the model
        // named a specific block, and substituting a different body would silently
        // deliver something it never selected.
        let mut prior_text = if selected.is_none() && !is_block_ref {
            let scope = agentic_content_draft_scope(services, ticket);
            latest_agentic_content_draft(services, &scope)
                .ok()
                .flatten()
                .and_then(|draft| agentic_content_draft_text(services, &draft))
        } else {
            None
        };
        // Recover the body written in an earlier step of this same Task. It is kept
        // alive for the borrow below; `selected` above borrows the current response.
        let prior_text = if selected.is_none() { prior_text } else { None };
        let selected = selected.or(prior_text
            .as_ref()
            .map(|prior| (usize::MAX, prior.as_str())));
        let Some((selected_index, text)) = selected else {
            continue;
        };
        if text.trim().is_empty() {
            // An empty selection must never become an empty deliverable: leave
            // the reference unresolved so the action is rejected with
            // actionable guidance instead of persisting a bodyless artifact.
            continue;
        }
        let mut scope = agentic_content_draft_scope(services, ticket);
        scope.node_id = format!("{}:selected-block:{selected_index}", scope.node_id);
        let content_ref = persist_agentic_content_draft_for_scope(
            services,
            &scope,
            message_artifact_visibility(services, message, ticket),
            text,
        )
        .await?;
        if is_commit {
            value["content_ref"] = serde_json::Value::String(content_ref);
        } else {
            // A submission with no artifact reference must still carry a durable
            // body: attach this response's resolved content as its deliverable so
            // it is never rejected with task_submission_has_no_artifact.
            let has_artifact = value
                .get("artifact_refs")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|references| !references.is_empty());
            match value
                .get_mut("deliverable")
                .and_then(serde_json::Value::as_object_mut)
            {
                Some(deliverable) => {
                    deliverable.insert(
                        "resolved_content_ref".to_string(),
                        serde_json::Value::String(content_ref),
                    );
                }
                None if !has_artifact => {
                    value["deliverable"] = serde_json::json!({
                        "resolved_content_ref": content_ref,
                    });
                }
                None => {}
            }
        }
        call.input = serde_json::to_string(&value).map_err(|error| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason: format!("encode selected content: {error}"),
        })?;
    }
    Ok(())
}

/// Prefix a bare evidence reference with its durable scheme. A weak model may
/// cite `tool-raw-call_...` or `art_...` without the `tool://`/`artifact://`
/// prefix; the identity is unambiguous, so normalize it instead of rejecting.
///
/// The single-colon forms matter as much as the bare ones. Runtime itself
/// hands the model `artifact:<24 hex>` from `one_shot_artifact_ref`
/// (crates/runtime/src/agentic/work_market.rs) and repeats it verbatim in the
/// `uncovered_result_refs` clause of a completion gap. Every durable reader
/// requires `artifact://`, and `Address::parse` turns a reference without
/// `://` into `Bare`, which the unified read routes to `state_inspect`. A
/// model that copies Runtime's own spelling therefore reads the program
/// directory instead of the artifact and is never told anything went wrong.
/// Normalizing here keeps the spelling Runtime emits and the spelling the
/// readers accept from drifting apart again.
fn normalize_bare_evidence_ref(reference: &str) -> String {
    let trimmed = reference.trim();
    if trimmed.contains("://") {
        return trimmed.to_string();
    }
    if let Some(rest) = trimmed.strip_prefix("artifact:") {
        if !rest.is_empty() {
            return format!("artifact://{rest}");
        }
    }
    if let Some(rest) = trimmed.strip_prefix("tool:") {
        if !rest.is_empty() {
            return format!("tool://{rest}");
        }
    }
    if trimmed.starts_with("tool-") || trimmed.starts_with("tool_") {
        return format!("tool://{trimmed}");
    }
    if trimmed.starts_with("artifact-")
        || trimmed.starts_with("art_")
        || trimmed.starts_with("sha256:")
    {
        return format!("artifact://{trimmed}");
    }
    trimmed.to_string()
}

fn normalize_bare_evidence_refs(value: &mut serde_json::Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    for field in ["evidence_refs", "result_refs"] {
        if let Some(array) = object.get_mut(field).and_then(|value| value.as_array_mut()) {
            normalize_ref_array(array);
        }
    }
    // An issue disposition also carries durable references that must be
    // canonical: a bare reason_ref/evidence_refs previously failed
    // issue_disposition_requires_durable_evidence.
    if let Some(dispositions) = object
        .get_mut("issue_dispositions")
        .and_then(|value| value.as_array_mut())
    {
        for disposition in dispositions {
            let Some(entry) = disposition.as_object_mut() else {
                continue;
            };
            let reason = entry
                .get("reason_ref")
                .and_then(|value| value.as_str())
                .map(str::to_string);
            if let Some(raw) = reason {
                let normalized = normalize_bare_evidence_ref(&raw);
                if normalized != raw {
                    entry.insert(
                        "reason_ref".to_string(),
                        serde_json::Value::String(normalized),
                    );
                }
            }
            if let Some(array) = entry
                .get_mut("evidence_refs")
                .and_then(|value| value.as_array_mut())
            {
                normalize_ref_array(array);
            }
        }
    }
}

fn normalize_ref_array(array: &mut [serde_json::Value]) {
    for entry in array.iter_mut() {
        let Some(raw) = entry.as_str() else {
            continue;
        };
        let normalized = normalize_bare_evidence_ref(raw);
        if normalized != raw {
            *entry = serde_json::Value::String(normalized);
        }
    }
}

#[cfg(test)]
mod reference_spelling_tests {
    use super::normalize_bare_evidence_ref;

    /// Runtime hands the model `artifact:<24 hex>` for a one-shot Task
    /// deliverable and repeats it in the `uncovered_result_refs` clause of a
    /// completion gap. Every durable reader wants `artifact://`, so a model that
    /// copies Runtime's own spelling must still reach the artifact.
    #[test]
    fn single_colon_artifact_ref_normalizes_to_the_durable_scheme() {
        assert_eq!(
            normalize_bare_evidence_ref("artifact:62f88802633947c6e8de0251"),
            "artifact://62f88802633947c6e8de0251"
        );
    }

    #[test]
    fn single_colon_tool_ref_normalizes_to_the_durable_scheme() {
        assert_eq!(
            normalize_bare_evidence_ref("tool:raw-chatcmpl-tool-94c9d8cd"),
            "tool://raw-chatcmpl-tool-94c9d8cd"
        );
    }

    /// Normalizing must be idempotent: an already canonical reference is
    /// returned untouched, so a second pass cannot double the separator.
    #[test]
    fn already_canonical_ref_is_untouched() {
        for canonical in [
            "artifact://art_921fd251e9ee4288bd051be2328d2c3c",
            "tool://tool-raw-chatcmpl-tool-94c9d8cd84c6ca7a-6fc516bc30abf659",
            "approval:v1:abc",
        ] {
            assert_eq!(normalize_bare_evidence_ref(canonical), canonical);
        }
    }

    /// The legacy bare prefixes keep working: this is the behavior the
    /// normalizer was originally written for.
    #[test]
    fn legacy_bare_prefixes_still_normalize() {
        assert_eq!(
            normalize_bare_evidence_ref("art_921fd251e9ee4288bd051be2328d2c3c"),
            "artifact://art_921fd251e9ee4288bd051be2328d2c3c"
        );
        assert_eq!(
            normalize_bare_evidence_ref("tool-raw-chatcmpl-tool-94c9d8cd"),
            "tool://tool-raw-chatcmpl-tool-94c9d8cd"
        );
    }

    /// A visibility scope and a program key are not evidence references. They
    /// must pass through untouched or a scope check would start failing.
    #[test]
    fn scopes_and_program_keys_are_left_alone() {
        for untouched in [
            "session:8a7f7d30-12e8-4ee1-8844-177d06d78015",
            "execution:session-ingress-graph",
            "task:1890cf27e957ec7b95501c38",
            "goal:session-ingress-graph:96259006e758bd4b674f5a88110b734d",
            "fact:42",
            "matrix:7",
        ] {
            assert_eq!(normalize_bare_evidence_ref(untouched), untouched);
        }
    }

    /// An empty tail carries no identity, so there is nothing to prefix.
    #[test]
    fn empty_tail_is_left_alone() {
        assert_eq!(normalize_bare_evidence_ref("artifact:"), "artifact:");
        assert_eq!(normalize_bare_evidence_ref("tool:"), "tool:");
    }
}

fn intent_requests_explicit_content(intent: &ModelStepIntent) -> bool {
    matches!(intent, ModelStepIntent::ToolCalls { calls } if calls.iter().any(|call| {
        call.name == harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID
            || call.name == harness_contract::agent_action::TASK_SUBMIT_TOOL_ID
    }))
}

fn message_artifact_visibility(
    services: &crate::RuntimeServices,
    _message: &ConversationMessage,
    ticket: &NodeExecutionTicket,
) -> String {
    if let Some(packet) = delegated_agent_task_packet(services, ticket) {
        return format!("session:{}", packet.session_id());
    }
    format!("execution:{}", ticket.graph_id)
}

/// Builds the graph transition used when a verified Host-admitted Team has
/// already produced the canonical terminal result. Keeping this transition
/// outside the provider stage makes the early-exit invariant explicit and
/// prevents protocol recovery changes from growing the model executor.
fn precommitted_terminal_outcome(ticket: &NodeExecutionTicket) -> NodeExecutionOutcome {
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
    NodeExecutionOutcome::new(completed_result(
        Some(format!("{}:precommitted-terminal", ticket.graph_id)),
        ExecutionUsage::default(),
    ))
    .with_replan(ExecutionGraphReplan {
        nodes: vec![synthesize.clone()],
        edges: dynamic_edges(&ticket.node_id, &[synthesize]),
        reason: "verified Team terminal result bypassed duplicate parent model execution"
            .to_string(),
        framework_authored: true,
    })
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
        {
            // A required-Team turn spends most of its wall clock inside tool
            // batches rather than root model steps, so the objective budget must
            // be enforced here too: otherwise the turn only ends at the external
            // scenario wait instead of an in-bound honest terminal.
            let mut state = self.state.lock().await;
            if collaboration_budget_elapsed(
                state.collaboration_obligation.as_ref(),
                state.execution_role.is_delegated_leaf(),
                state.started_at.elapsed(),
                collaboration_wall_budget(),
                state.last_verified_progress,
            ) {
                let disclosure = "Execution blocked safely: bounded convergence reached the Agent-first collaboration budget with the required verified-Team minimum still unmet. The objective is only partially satisfied and unresolved work stays in the Program projection; Runtime stopped the turn instead of running unbounded.".to_string();
                state.terminal_override = Some((GoalCompletion::Partial, disclosure));
                return Ok(precommitted_terminal_outcome(ticket));
            }
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
                // `requires_write` is a planning prediction, not a negative
                // authorization grant. A false negative must not turn a
                // user-requested artifact or a model-selected, otherwise
                // authorized effect into a permanent capability gap. Only an
                // explicit read-only user constraint is an effect-boundary
                // deny, which keeps the boundary universal for future tools
                // without reducing agent autonomy to a keyword classifier.
                let task_explicitly_forbids_workspace_write =
                    runtime.active_turn_strategy().is_some_and(|strategy| {
                        strategy
                            .decision
                            .strategy
                            .understanding
                            .forbids_workspace_write
                    });
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
                    if let Some(invocation) = prepared
                        .iter()
                        .find(|invocation| invocation.invocation_id == call.id)
                    {
                        let descriptor = invocation.effect.clone();
                        if workspace_mutation_is_objectively_forbidden(
                            task_explicitly_forbids_workspace_write,
                            descriptor.required_permission,
                        ) {
                            gaps.insert(
                                call.id.clone(),
                                synthetic_capability_gap(
                                    &descriptor,
                                    runtime.active_permission_mode(),
                                    "the active user task explicitly prohibits workspace mutation; Runtime rejects mutating tool effects even under a full-trust session profile".to_string(),
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
            mut orchestration_terminal_summary,
            prepared_tool_invocations,
            mut successful_observed_evidence,
            agentic_progress_refs,
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
            let (event_bus, memory_context, reality_context, execution_policy) = {
                let runtime = self.runtime.lock().await;
                (
                    runtime.cowd_bus().cloned(),
                    runtime.memory_turn_context(),
                    runtime.reality_data_lease(),
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
                reality_context.as_ref(),
                model_lease.as_deref(),
                ticket,
                u64::try_from(iteration).unwrap_or(u64::MAX).max(1),
                &tool_authorizations,
                &capability_gaps,
                &prepared_tool_invocations,
                governed_compilation,
                &execution_decision,
                self.services.tool_execution_plane(),
                self.services.tool_batch_executor(),
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
            // Semantic Action progress must be read from the complete
            // Runtime receipt. The model-facing evidence compactor may
            // legitimately truncate a large projection after this point.
            let agentic_progress_refs = agentic_action_progress_refs(&governed_messages);
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
                agentic_progress_refs,
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
            let agentic_progress_refs = agentic_action_progress_refs(&result.messages);
            (
                result,
                orchestration_terminal_summary,
                prepared_tool_invocations,
                observed_evidence,
                agentic_progress_refs,
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
        let successful_call_ids = successful_tool_call_ids(&result.messages);
        // Read the registered, Runtime-compiled descriptor that was actually
        // authorized for the successful call. Do not infer an effect from
        // model input or stdout: generic mutation tools retain their autonomy,
        // while a later exact ToolHost read can prove their final state.
        let successful_workspace_mutation_effect = successful_call_ids.iter().any(|call_id| {
            prepared_tool_invocations
                .get(call_id)
                .is_some_and(|invocation| is_local_workspace_mutation_effect(&invocation.effect))
        });
        // Active workers do not mean the root has finished planning. A root
        // needs each published Task ref before it can wire downstream work.
        // Only its explicit yield requests a durable, event-driven barrier.
        let (session_id, turn_id, delegated) = {
            let state = self.state.lock().await;
            (
                state.session_id.clone(),
                state.turn_id.clone(),
                state.execution_role.is_delegated_leaf(),
            )
        };
        let agentic_program_wait_request = if delegated {
            None
        } else if let Some(program) =
            root_agentic_program_projection(self.services.as_ref(), &session_id, &turn_id)
        {
            // Durable collaboration deadline for this Program: the barrier
            // itself carries it, so the periodic reconciliation releases the
            // parked root at the budget even while every model step is idle.
            let deadline_at_ms = {
                let state = self.state.lock().await;
                state
                    .collaboration_obligation
                    .as_ref()
                    .is_some_and(|obligation| obligation.minimum_team_count > 0)
                    .then(|| collaboration_wall_budget())
                    .flatten()
                    .map(|budget| {
                        let remaining = budget.saturating_sub(state.started_at.elapsed());
                        crate::tool_invocation::now_ms()
                            .saturating_add(u64::try_from(remaining.as_millis()).unwrap_or(0))
                    })
            };
            crate::execution_core::graph::executors::AgenticProgramWaitRequest::for_active_root(
                &program,
                &ticket.graph_id,
                self.services.graph_state_store(),
                requested_agentic_worker_wait(&calls, &successful_call_ids),
                deadline_at_ms,
            )
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("derive Agentic Program wait invariant: {reason}"),
            })?
        } else {
            None
        };
        let closed_delegated_protocol = calls.iter().any(|call| {
            successful_call_ids.contains(&call.id)
                && (call
                    .name
                    .eq_ignore_ascii_case(harness_contract::agent_action::TASK_SUBMIT_TOOL_ID)
                    || call
                        .name
                        .eq_ignore_ascii_case(harness_contract::agent_action::TASK_REVIEW_TOOL_ID))
        });
        if closed_delegated_protocol {
            if let Some(protocol) =
                delegated_agentic_protocol_state(self.services.as_ref(), ticket)?
            {
                // A successful `task_submit`/`task_review` call does not by itself
                // prove the Program Task closed: the action can be applied while
                // the task still needs evidence or a reviewer. Do not fail the
                // tool node; leave the protocol open so the FinalAnswer path
                // forces a continuation with `continuation_instruction` instead.
                if protocol.is_terminal() {
                    orchestration_terminal_summary = Some(format!(
                        "Delegated collaboration Task `{}` completed its `{}` protocol with durable status {:?}.",
                        protocol.task_id, protocol.mode, protocol.status
                    ));
                }
            }
        }
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
        // These markers are durable Runtime observation facts. They are
        // emitted only when an authorized workspace effect completed
        // successfully, so a model cannot manufacture a mutation epoch by
        // claiming one in text or tool input.
        let durable_workspace_mutation_sequences = prior_observations
            .iter()
            .filter(|observation| observation.kind == RuntimeObservationKind::ToolProgress)
            .flat_map(|observation| observation.evidence_refs.iter())
            .filter_map(|reference| workspace_mutation_effect_sequence(reference));
        let (prior_workspace_mutation_sequences, required_workspace_write_scopes) = {
            let state = self.state.lock().await;
            (
                prior_workspace_mutation_sequences(
                    durable_workspace_mutation_sequences,
                    &state.workspace_mutation_effect_sequences,
                ),
                state.required_workspace_write_scopes.clone(),
            )
        };
        let verified_postcondition_writes = verified_workspace_postcondition_write_evidence(
            &required_workspace_write_scopes,
            &prior_workspace_mutation_sequences,
            &successful_observed_evidence,
            self.services.path_identity_resolver(),
        );
        for evidence in verified_postcondition_writes {
            if !successful_observed_evidence.contains(&evidence) {
                successful_observed_evidence.push(evidence);
            }
        }
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
            && agentic_progress_refs.is_empty()
            && !coverage_keys.is_empty()
            && novelty_target_bp > 0
            && coverage_novelty_bp < novelty_target_bp;
        let evidence_saturated = failed == 0
            && agentic_progress_refs.is_empty()
            && !coverage_keys.is_empty()
            && newly_covered == 0
            && newly_scoped == 0;
        // File-level receipts may be individually new while adding no new
        // responsibility zone. A delegated role has a finite evidence
        // contract, so repeated work inside already-covered zones is a
        // saturation signal. Main turns retain their normal open exploration.
        let scope_saturated =
            bounded_evidence_role && failed == 0 && !scope_keys.is_empty() && newly_scoped == 0;
        let committed_agentic_artifact = calls.iter().any(|call| {
            successful_call_ids.contains(&call.id)
                && call
                    .name
                    .eq_ignore_ascii_case(harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID)
        });
        let delegated_protocol_for_batch =
            delegated_agentic_protocol_state(self.services.as_ref(), ticket)?;
        let delegated_protocol_after_batch = if committed_agentic_artifact {
            delegated_protocol_for_batch
                .clone()
                .filter(|protocol| !protocol.is_terminal())
        } else {
            None
        };
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
        if successful_workspace_mutation_effect {
            state
                .workspace_mutation_effect_sequences
                .insert(u64::try_from(iteration).unwrap_or(u64::MAX).max(1));
        }
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
            state.force_tool_allowlist_next_model = None;
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
        state.consecutive_tool_failure_batches = next_consecutive_tool_failure_batches(
            state.consecutive_tool_failure_batches,
            executed_failures(failed, &calls, &turn_id),
            false,
        );
        if failed == 0 && (low_novelty || scope_saturated || evidence_saturated) {
            state.consecutive_low_novelty_batches =
                state.consecutive_low_novelty_batches.saturating_add(1);
        } else {
            state.consecutive_low_novelty_batches = 0;
        }
        // ---- Loop-breaker and targeted runaway caps (nudge-first) ----------
        // A batch that repeats the same tool set, canonical arguments AND the
        // same result is an identical call. Advise the model (default) and only
        // hard stop when the operator opted into the circuit breaker. Pollers
        // are exempt because an unchanged result is their normal contract.
        let loop_repeatable = !calls.is_empty()
            && calls
                .iter()
                .all(|call| is_loop_nudge_repeatable_tool(&call.name));
        let batch_result_digest = terminal_evidence_digest(&result.messages);
        if loop_repeatable {
            state.identical_action_streak = 0;
            state.identical_action_digest.clear();
            state.identical_action_result_digest.clear();
        } else if state.identical_action_digest == action_fingerprint
            && state.identical_action_result_digest == batch_result_digest
        {
            state.identical_action_streak = state.identical_action_streak.saturating_add(1);
        } else {
            state.identical_action_digest = action_fingerprint.clone();
            state.identical_action_result_digest = batch_result_digest;
            state.identical_action_streak = 1;
        }
        match evaluate_loop_guard(
            state.identical_action_streak,
            loop_nudge_after(),
            loop_hard_stop_enabled(),
            loop_block_after(),
        ) {
            LoopGuardAction::Nudge { streak } => {
                let mut item = ContextItem::new(
                    format!("loop-breaker:{}:{}", ticket.node_id, streak),
                    ContextSourceKind::ToolTrace,
                    ContextRole::Instruction,
                    format!(
                        "This is the {streak}th consecutive identical tool call (same tools, same arguments, same result). Do not repeat it: change the arguments, use a different tool, or proceed with the evidence you already have."
                    ),
                );
                item.authority = ContextAuthority::System;
                item.visibility = ContextVisibility::Private;
                state.pending_next_model_context.push(item);
            }
            LoopGuardAction::Block { streak } => {
                state.terminal_override = Some((
                    GoalCompletion::Partial,
                    format!(
                        "Execution blocked safely: {streak} consecutive identical tool calls with an identical result (possible loop). Retained evidence and progress were preserved. Continue with a new constraint, additional evidence, or an explicit replan."
                    ),
                ));
            }
            LoopGuardAction::Continue => {}
        }
        // Targeted per-turn runaway caps (optional; `0` disables each cap).
        for call in &calls {
            let key = call.name.to_ascii_lowercase();
            *state.per_turn_tool_counts.entry(key).or_insert(0) += 1;
        }
        if let Some((tool, count, cap)) = calls
            .iter()
            .filter_map(|call| {
                let key = call.name.to_ascii_lowercase();
                let count = state.per_turn_tool_counts.get(&key).copied().unwrap_or(0);
                let cap = tool_scope_cap(&key);
                (cap > 0 && count > cap).then_some((key, count, cap))
            })
            .next()
        {
            state.terminal_override = Some((
                GoalCompletion::Partial,
                format!(
                    "Execution blocked safely: tool `{tool}` exceeded its per-turn cap ({count} > {cap}); retained evidence and progress were preserved. Continue with a new constraint, additional evidence, or an explicit replan."
                ),
            ));
        }
        // Weak-model auto-downgrade (advisory): a collaborating turn whose
        // guardrail signals are hot is steered to converge with the topology it
        // already has instead of grinding on an ever-more-expensive Team
        // protocol. Termination semantics are unchanged; this is a nudge.
        if state.collaboration_obligation.is_some()
            && crate::execution_core::strategy_decision::collaboration_downgrade_recommended(
                state.identical_action_streak,
                state.consecutive_low_novelty_batches,
                state.consecutive_tool_failure_batches,
            )
        {
            let mut item = ContextItem::new(
                format!("collaboration-downgrade-nudge:{}", ticket.node_id),
                ContextSourceKind::ToolTrace,
                ContextRole::Instruction,
                "Collaboration is stalling (repeated identical calls, no new coverage, or repeated failures). Stop chasing the Team minimum: converge with the Teams, Agents and evidence you already have, or report the blocker honestly. A smaller honest result is better than an unbounded attempt."
                    .to_string(),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            state.pending_next_model_context.push(item);
        }
        if let Some(protocol) = delegated_protocol_after_batch.as_ref() {
            // An artifact is intermediate collaboration evidence, not the
            // Task's semantic terminal. Keep the Agent action channel open
            // and hand the exact durable artifact refs back to the model so
            // it can submit/review without guessing or replaying exploration.
            state.consecutive_low_novelty_batches = 0;
            state.force_text_only_next_model = false;
            state.clean_terminal_synthesis_next = false;
            state.force_tool_allowlist_next_model = None;
            let mut item = ContextItem::new(
                format!("agentic-artifact-ready:{}", ticket.node_id),
                ContextSourceKind::ToolTrace,
                ContextRole::Instruction,
                protocol.continuation_instruction(),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            item.evidence = protocol.artifact_refs.clone();
            state.pending_next_model_context.push(item);
        }
        let repeated_local_failures = state.consecutive_tool_failure_batches >= 2;
        let repeated_evidence_saturation = state.consecutive_low_novelty_batches
            >= evidence_saturation_limit(bounded_evidence_role);
        let pending_delegated_action = pending_delegated_action_is_ready(
            delegated_protocol_for_batch.as_ref(),
            repeated_evidence_saturation,
            state.successful_tool_calls > 0,
        );
        // A stalled/failing turn gets an explicit, advisory notice before the
        // existing forced-synthesis or fail-closed paths engage. Delegated
        // protocol closure has its own authoritative continuation, so it is
        // left untouched.
        if repeated_local_failures && !pending_delegated_action {
            let mut item = ContextItem::new(
                format!("tool-failure-nudge:{}", ticket.node_id),
                ContextSourceKind::ToolTrace,
                ContextRole::Instruction,
                "The same tool calls failed again with no retained evidence. Do not repeat them unchanged: change the arguments, use a different tool, or report the blocker."
                    .to_string(),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            state.pending_next_model_context.push(item);
        }
        if repeated_evidence_saturation && !pending_delegated_action {
            let mut item = ContextItem::new(
                format!("evidence-saturation-nudge:{}", ticket.node_id),
                ContextSourceKind::ToolTrace,
                ContextRole::Instruction,
                "Recent tool calls added no new evidence coverage. Stop re-acquiring: use the receipts you already have or converge on the answer."
                    .to_string(),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            state.pending_next_model_context.push(item);
        }
        if pending_delegated_action {
            // Evidence saturation for delegated work is not a presentation
            // terminal. The business terminal is the next durable protocol
            // action: artifact_commit/task_submit for execution or
            // task_review for review. Keep only those actions exposed and
            // skip the generic text-only synthesis detour. Otherwise an Agent
            // can state that work is complete with an empty tool set, only
            // for the outer autonomy checkpoint to spend another model
            // request restoring the exact action it already needed.
            state.consecutive_low_novelty_batches = 0;
            state.force_text_only_next_model = false;
            state.clean_terminal_synthesis_next = false;
            state.force_tool_allowlist_next_model = delegated_protocol_for_batch
                .as_ref()
                .map(DelegatedAgenticProtocolState::closure_tool_ids);
            if let Some(protocol) = delegated_protocol_for_batch.as_ref() {
                let mut item = ContextItem::new(
                    format!("agentic-terminal-action-ready:{}", ticket.node_id),
                    ContextSourceKind::Task,
                    ContextRole::Instruction,
                    format!(
                        "Runtime Agent-first evidence boundary: retained tool receipts are sufficient and further acquisition added no required coverage. {}",
                        protocol.continuation_instruction()
                    ),
                );
                item.authority = ContextAuthority::System;
                item.visibility = ContextVisibility::Private;
                item.evidence = protocol.artifact_evidence_refs.clone();
                state.pending_next_model_context.push(item);
            }
        }
        let exact_workspace_evidence = focus_scopes_are_exact_workspace_files(
            &focus_acceptance_scopes,
            self.services.workspace_root(),
        );
        let focus_synthesis_ready = !pending_delegated_action
            && delegated_protocol_after_batch.is_none()
            && should_force_focus_synthesis(
                focus_acceptance_met,
                &focus_acceptance_scopes,
                repeated_evidence_saturation,
                has_retained_focus_terminal_candidate,
                exact_workspace_evidence,
            );
        let successful_write_observed = write_obligation_satisfied(
            state.required_write_for_completion,
            &state.required_workspace_write_scopes,
            &state.committed_workspace_observed_evidence,
            state.committed_workspace_write_observed,
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
        let delegated_failure_replan_available = delegated_tool_failure_replan_available(
            bounded_evidence_role,
            has_successful_tool_evidence,
            state.delegated_failure_replans,
        );
        if repeated_local_failures && delegated_failure_replan_available {
            // Consume the single semantic-replan lease before releasing the
            // state guard. The next model step receives a fresh failure
            // streak, while a second consecutive failure is still terminal.
            state.delegated_failure_replans = state.delegated_failure_replans.saturating_add(1);
            state.consecutive_tool_failure_batches = 0;
        }
        let newly_completed_program_team_ids = completed_program_team_ids(&result.messages);
        let completed_root_team_this_batch = root_team_terminal_requires_text_only(
            state.execution_role.is_delegated_leaf(),
            state.collaboration_obligation.as_ref(),
            &newly_completed_program_team_ids,
        );
        if loop_stub_results_enabled() && state.identical_action_streak >= 2 && failed == 0 {
            // Opt-in context dedup for a repeating model: replace large
            // byte-identical duplicate payloads in the retained transcript with
            // a short reference stub. Evidence stays keyed by tool_call id, so
            // dedup only shrinks redundant context.
            let mut deduped = result.messages;
            stub_duplicate_result_payloads(&mut deduped);
            state.tool_results.extend(deduped);
        } else {
            state.tool_results.extend(result.messages);
        }
        if completed_root_team_this_batch {
            // A verified collaboration terminal leaves only presentation.
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
            // deterministic contract reduction. Use the isolated terminal
            // request instead of replaying the exploratory assistant/tool-call
            // transcript: every committed receipt remains present exactly once,
            // while provider-private reasoning, tool schemas, and stale protocol
            // turns cannot inflate the reduction request.
            state.force_text_only_next_model = true;
            state.force_reasoning_effort_next_model = Some("none".to_string());
            state.clean_terminal_synthesis_attempted = true;
            state.clean_terminal_synthesis_next = true;
        }
        // "Verified progress" means a durable Program-level change was applied
        // this batch (an Agent action that is Applied, not duplicate, and did
        // change refs). Reading a *new file* is not progress: a model that opens
        // one new path per round would otherwise be exempt from every bound,
        // which is exactly the unbounded-70-rounds failure this guards against.
        state.last_verified_progress = !agentic_progress_refs.is_empty();
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
                .chain(agentic_progress_refs.iter().cloned())
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
            .chain(
                successful_workspace_mutation_effect
                    .then(|| {
                        workspace_mutation_effect_evidence_ref(
                            u64::try_from(iteration).unwrap_or(u64::MAX).max(1),
                        )
                    })
                    .into_iter(),
            )
            .chain(agentic_progress_refs.iter().cloned())
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
        if !pending_delegated_action
            && (focus_synthesis_ready
                || (repeated_evidence_saturation
                    && !focus_acceptance_pending
                    && !required_write_recovery))
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
        let mut intervention = if pending_delegated_action {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason: "delegated Agent evidence is saturated; commit the next durable Agent-first protocol action without a text-only synthesis round"
                    .to_string(),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if delegated_protocol_after_batch.is_some() {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Continue,
                reason: "delegated Agent committed an artifact and must now close the durable Task protocol"
                    .to_string(),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if focus_synthesis_ready {
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
                kind: if delegated_failure_replan_available {
                    RuntimeInterventionKind::Replan
                } else if has_successful_tool_evidence {
                    RuntimeInterventionKind::Synthesize
                } else {
                    RuntimeInterventionKind::Block
                },
                reason: if delegated_failure_replan_available {
                    format!(
                        "{}: one bounded delegated recovery lease is available: change the failed tool approach, inspect the exact Runtime diagnostic, and execute only an exposed authorized action; a second consecutive failure will block",
                        DELEGATED_TOOL_FAILURE_REPLAN_MARKER
                    )
                } else if has_successful_tool_evidence {
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
        // A generic evidence-saturation synthesis is allowed for ordinary
        // work, but it cannot terminate a claimed Agent Task half-way through
        // its durable artifact/submit/review protocol. The prior behavior
        // disabled tools here, so a model could only describe its pending
        // submission in prose; the outer autonomy loop then re-dispatched the
        // same claimed work in a fresh graph. Preserve all model-led research
        // until this exact boundary, then require only the irreducible durable
        // closure action instead of synthesizing text.
        let protocol_closure_required = protocol_closure_can_be_required(
            delegated_protocol_for_batch.as_ref(),
            intervention.as_ref(),
            &turn_id,
        );
        if protocol_closure_required {
            let protocol = delegated_protocol_for_batch
                .as_ref()
                .expect("active delegated protocol was checked above");
            intervention = Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason: format!(
                    "delegated Agent Task `{}` remains {:?}; a generic text synthesis cannot preempt its durable closure action",
                    protocol.task_id, protocol.status
                ),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            });
        }
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
            if protocol_closure_required {
                let protocol = delegated_protocol_for_batch
                    .as_ref()
                    .expect("active delegated protocol was checked above");
                state.force_text_only_next_model = false;
                state.clean_terminal_synthesis_next = false;
                state.force_tool_allowlist_next_model = Some(protocol.closure_tool_ids());
                let mut item = ContextItem::new(
                    format!("agentic-protocol-closure-required:{}", ticket.node_id),
                    ContextSourceKind::Task,
                    ContextRole::Instruction,
                    format!(
                        "Runtime closure boundary: exploration is complete for this Agent turn. {}",
                        protocol.continuation_instruction()
                    ),
                );
                item.authority = ContextAuthority::System;
                item.visibility = ContextVisibility::Private;
                item.evidence = protocol.artifact_evidence_refs.clone();
                state.pending_next_model_context.push(item);
            }
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
            let mut replan_nodes = vec![next];
            let mut replan_reason = if orchestration_terminal_summary.is_some() {
                "Runner committed completed orchestration terminal summary".to_string()
            } else {
                format!(
                    "Runner applied goal intervention: {:?}",
                    intervention
                        .as_ref()
                        .map_or(RuntimeInterventionKind::Continue, |value| value.kind)
                )
            };
            if let Some(request) = agentic_program_wait_request
                .filter(|_| replan_nodes[0].kind == ExecutionNodeKind::InlineModel)
            {
                let mut wait = dynamic_node(
                    ticket,
                    self.state.lock().await.iterations,
                    "agentic-program-wait",
                    ExecutionNodeKind::Verify,
                    crate::execution_core::graph::executors::AgenticProgramWaitExecutor::KIND,
                    "agentic_program_wait",
                );
                wait.payload_ref =
                    serde_json::to_string(&request).map_err(|error| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: format!("encode Agentic Program wait request: {error}"),
                    })?;
                replan_nodes.insert(0, wait);
                replan_reason =
                    "Runner parked the root on an event-driven Agentic Program barrier".to_string();
            }
            outcome.replan = Some(ExecutionGraphReplan {
                edges: dynamic_edges(&ticket.node_id, &replan_nodes),
                nodes: replan_nodes,
                reason: replan_reason,
                framework_authored: true,
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
        let mut messages = {
            let mut state = self.state.lock().await;
            state
                .pending_transcript
                .remove(&ticket.node_id)
                .unwrap_or_default()
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_local_workspace_mutation_effect, prior_workspace_mutation_sequences,
        workspace_mutation_is_objectively_forbidden,
    };
    use harness_contract::{
        policy::{PermissionOperation, PermissionResource, PermissionScope},
        tool::{
            ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
            ToolPermissionMode,
        },
    };
    use std::collections::BTreeSet;

    #[test]
    fn planning_false_negative_cannot_deny_an_authorized_workspace_effect() {
        assert!(!workspace_mutation_is_objectively_forbidden(
            false,
            ToolPermissionMode::WorkspaceWrite,
        ));
    }

    #[test]
    fn explicit_read_only_objective_still_denies_workspace_effects() {
        assert!(workspace_mutation_is_objectively_forbidden(
            true,
            ToolPermissionMode::WorkspaceWrite,
        ));
        assert!(!workspace_mutation_is_objectively_forbidden(
            true,
            ToolPermissionMode::ReadOnly,
        ));
    }

    #[test]
    fn same_turn_mutation_epoch_is_visible_before_goal_event_commit() {
        let epochs = prior_workspace_mutation_sequences([2_u64], &BTreeSet::from([1_u64, 3_u64]));

        assert_eq!(epochs, BTreeSet::from([1_u64, 2_u64, 3_u64]));
    }

    #[test]
    fn local_shell_cleanup_still_establishes_a_postcondition_epoch() {
        let effect = ToolEffectDescriptor {
            tool_id: "bash".to_string(),
            descriptor_hash: "test".to_string(),
            effect_kind: ToolEffectKind::Destructive,
            idempotency: ToolIdempotency::Unknown,
            scopes: vec![PermissionScope {
                resource: PermissionResource::Shell,
                operation: PermissionOperation::Write,
                target: None,
            }],
            required_permission: ToolPermissionMode::DangerFullAccess,
            approval_class: ToolApprovalClass::Administrator,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        };

        assert!(is_local_workspace_mutation_effect(&effect));
        let mut networked = effect;
        networked.uses_network = true;
        assert!(!is_local_workspace_mutation_effect(&networked));
    }
}

#[cfg(test)]
mod zero_recovery_guard_tests {
    use super::*;

    /// The guard must never fire while the model is still answering: a turn with
    /// recovery attempts is bounded by the attempt counter, not by the clock.
    #[test]
    fn the_zero_recovery_deadline_is_a_last_resort_not_a_clock() {
        let deadline = zero_recovery_deadline();
        assert!(!deadline.is_zero(), "a zero deadline fires immediately");
        assert!(
            deadline >= std::time::Duration::from_secs(60),
            "the deadline must outlast any single model round"
        );
    }

    /// The ceiling is what routes an unanswered protocol to the existing
    /// no-progress terminus instead of replanning it again.
    #[test]
    fn the_ceiling_is_past_the_fuse_retry_budget() {
        assert!(
            NO_RECOVERY_ATTEMPT_CEILING > 2,
            "at or below the retry budget this would replan instead of stopping"
        );
    }

    /// The two ways to reach the ceiling must stay distinguishable, because the durable
    /// record names them differently.
    ///
    /// A model that keeps failing to close records attempts; a model that stopped
    /// answering records none. Both produce the same number, so a report cannot tell
    /// "still working" from "stuck" unless the branch is known at the moment it fires.
    #[test]
    fn the_stall_branch_is_not_the_increment_branch() {
        let past = zero_recovery_deadline() + std::time::Duration::from_secs(1);
        // Increment branch: attempts exist, so elapsed time alone is not a stall.
        assert!(!zero_recovery_stall(
            NO_RECOVERY_ATTEMPT_CEILING,
            true,
            past
        ));
        // Stall branch: no attempt at all past the deadline.
        assert!(zero_recovery_stall(0, true, past));
        // A protocol that reached its own end is not stalled.
        assert!(!zero_recovery_stall(0, false, past));
        // Before the deadline, no attempt is a model that is still working.
        assert!(!zero_recovery_stall(
            0,
            true,
            std::time::Duration::from_secs(1)
        ));
    }
}
