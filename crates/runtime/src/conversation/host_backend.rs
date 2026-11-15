//! Graph-owned provider and governed tool execution backends.

use super::*;

// Construct result futures after the producer returns, outside its poll frame.
#[inline(never)]
fn construct_result_phase<F: std::future::Future>(
    make: impl FnOnce() -> F,
) -> std::pin::Pin<Box<F>> {
    Box::pin(make())
}

use crate::execution_core::goal::{
    finish_user_goal_application, read_user_goal_application, user_application_clock,
    user_goal_application_committed, user_goal_application_committed_sources, GoalWriteCheckpoint,
    GoalWriteDisposition, PreparedUserGoalApplication,
};

/// The call identity a governed grant carries for one batch member.
///
/// `ToolPolicy::authorize` copies this value into the lease's idempotency key,
/// and the durable local-network binding compares both against this call's own
/// `tool_use_id` (`local_network.rs:58-59`), so the bare call id is the
/// contract. A composite spelling once reached the grant from here and made
/// every network effect on this path unclosable; nothing in the tree
/// decomposes this value, and the attempt is already carried by the ticket and
/// by the effect's own key.
fn original_call_request_id(call_id: &str) -> String {
    call_id.to_string()
}

impl<C: ApiClient + Send + Sync + 'static, T: ToolExecutor> TurnModelStepBackend<C, T> {
    async fn resume_user_input_application(
        &self,
        ticket: &NodeExecutionTicket,
        known_only: bool,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        use harness_contract::input_disposition::InputApplicationState;
        let fail = |reason: String| NodeExecutorError::Recover {
            node_id: ticket.node_id.clone(),
            reason,
        };
        let (session, turn, owns_root) = {
            let state = self.state.lock().await;
            (
                state.session_id.clone(),
                state.turn_id.clone(),
                state.execution_role.owns_root_presentation(),
            )
        };
        if !owns_root {
            return Ok(None);
        }
        let Some(query) = self.services.session_query_port() else {
            return Ok(None);
        };
        let Some(admission) = query
            .input_admission(&session)
            .await
            .map_err(|e| fail(e.to_string()))?
        else {
            return Ok(None);
        };
        let mut after = None;
        loop {
            let page = query
                .pending_session_input_applications(
                    &session,
                    admission.generation,
                    after.clone(),
                    32,
                )
                .await
                .map_err(|e| fail(e.to_string()))?;
            if page.is_empty() {
                return Ok(None);
            }
            for row in &page {
                let Some(receipt) = row.application_receipt.as_ref() else {
                    continue;
                };
                let Some(identity) = receipt.prepared_application.as_ref() else {
                    continue;
                };
                if identity.graph_id != ticket.graph_id
                    || identity.turn_id != turn
                    || identity.node_id != ticket.node_id
                {
                    continue;
                }
                let prepared = read_user_goal_application(self.services.as_ref(), receipt)
                    .await
                    .map_err(fail)?;
                if prepared.ticket != *ticket {
                    return Err(fail(
                        "prepared user input cannot borrow another original attempt".into(),
                    ));
                }
                let committed_sources = user_goal_application_committed_sources(
                    self.services.as_ref(),
                    &prepared,
                    &identity.artifact_ref,
                )
                .map_err(fail)?;
                let applied = committed_sources.is_some();
                if known_only && (!applied || receipt.cancellation.is_some()) {
                    return Ok(None);
                }
                if applied && !known_only {
                    finish_user_goal_application(self.services.as_ref(), &prepared)
                        .await
                        .map_err(fail)?;
                }
                if receipt.cancellation.is_some() {
                    // The original settled lane owns physical closure and final
                    // cancellation receipt. Do not wait for this node's own exit.
                    self.services
                        .cancel_execution_tree(
                            &ticket.graph_id,
                            "durable user input cancellation recovered",
                        )
                        .await
                        .map_err(|e| fail(e.to_string()))?;
                    self.services.notify_agentic_followups();
                    return Err(fail("user input cancellation is requested; original physical settlement remains pending".into()));
                }
                let current = self
                    .services
                    .graph_state_store()
                    .load(&ticket.graph_id)
                    .map_err(|e| fail(e.to_string()))?;
                let original_result = current
                    .node_results
                    .get(&prepared.ticket.node_id)
                    .and_then(|r| r.result_ref.as_deref());
                if applied
                    && original_result
                        == Some(
                            format!("user_input_application:{}", identity.artifact_ref).as_str(),
                        )
                {
                    continue;
                }
                let mut outcome = NodeExecutionOutcome::new(completed_result(
                    Some(format!("user_input_application:{}", identity.artifact_ref)),
                    prepared.usage.clone(),
                ));
                outcome.result.finished_at_ms = prepared.finished_at_ms;
                outcome.replan = Some(prepared.continuation.clone());
                // A committed Goal fact is not authority to rebase the original
                // Graph outcome onto a newer graph cut.
                outcome
                    .source_revisions
                    .insert(ticket.graph_id.clone(), prepared.graph_revision);
                if !applied {
                    let source = self
                        .services
                        .goal_store()
                        .projection(&prepared.goal.id)
                        .map_err(fail)?
                        .ok_or_else(|| fail("prepared user Goal disappeared".into()))?;
                    if current.revision != prepared.graph_revision
                        || *ticket != prepared.ticket
                        || source.stream_revision != prepared.goal_stream_revision
                        || source.goal.spec_digest != prepared.goal.spec_digest
                    {
                        // Neither cut may be refreshed. The monotonically moved
                        // original read set fences every outstanding old commit.
                        if user_goal_application_committed(
                            self.services.as_ref(),
                            &prepared,
                            &identity.artifact_ref,
                        )
                        .map_err(fail)?
                        {
                            return Err(fail("user input committed during stale-cut check; retain original fact for replay".into()));
                        }
                        let mut rows = Vec::new();
                        for id in &receipt.input_ids {
                            rows.push(
                                query
                                    .runtime_input_by_input_id(id)
                                    .await
                                    .map_err(|e| fail(e.to_string()))?
                                    .ok_or_else(|| fail("prepared input disappeared".into()))?,
                            );
                        }
                        let mut rejected = rows[0]
                            .application_receipt
                            .clone()
                            .ok_or_else(|| fail("prepared receipt missing".into()))?;
                        if rows
                            .iter()
                            .any(|r| r.application_receipt.as_ref() != Some(&rejected))
                        {
                            return Err(fail("prepared input batch changed".into()));
                        }
                        rejected.state = InputApplicationState::Failed;
                        rejected.error = Some("prepared_user_goal_source_cut_stale".into());
                        rejected.revision += 1;
                        rejected.updated_at_ms = user_application_clock();
                        self.services
                            .session_journal_port()
                            .ok_or_else(|| fail("prepared input journal missing".into()))?
                            .commit_input_application_receipt(
                                &rejected.input_ids,
                                &rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
                                &rejected,
                                rejected.updated_at_ms,
                            )
                            .await
                            .map_err(|e| fail(e.to_string()))?;
                        return Err(fail("prepared user Goal source cut is stale; no Goal write or second interpretation".into()));
                    }
                    let mut rows = Vec::new();
                    for id in &receipt.input_ids {
                        rows.push(
                            query
                                .runtime_input_by_input_id(id)
                                .await
                                .map_err(|e| fail(e.to_string()))?
                                .ok_or_else(|| fail("prepared input disappeared".into()))?,
                        );
                    }
                    let mut reserved = rows[0]
                        .application_receipt
                        .clone()
                        .ok_or_else(|| fail("prepared receipt missing".into()))?;
                    if rows
                        .iter()
                        .any(|r| r.application_receipt.as_ref() != Some(&reserved))
                    {
                        return Err(fail("prepared input reservation changed".into()));
                    }
                    if reserved.state == InputApplicationState::Prepared {
                        reserved.state = InputApplicationState::Materializing;
                        reserved.revision += 1;
                        reserved.updated_at_ms = user_application_clock();
                        self.services
                            .session_journal_port()
                            .ok_or_else(|| fail("prepared input journal missing".into()))?
                            .commit_input_application_receipt(
                                &reserved.input_ids,
                                &rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
                                &reserved,
                                reserved.updated_at_ms,
                            )
                            .await
                            .map_err(|e| fail(e.to_string()))?;
                    }
                    if let Some(mutation) = &prepared.mutation {
                        if let Some((_, revision)) = prepared.program_cut {
                            self.services
                                .commit_user_input_goal_revision(
                                    &prepared.goal,
                                    &prepared.frame,
                                    mutation,
                                    revision,
                                    prepared.graph_revision,
                                    &identity.artifact_ref,
                                )
                                .map_err(fail)?;
                        } else {
                            outcome
                                .source_revisions
                                .insert(ticket.graph_id.clone(), prepared.graph_revision);
                            outcome.source_revisions.insert(
                                mutation.stream_id.clone(),
                                mutation.expected_stream_revision,
                            );
                            outcome.domain_events.push(mutation.event.clone());
                        }
                    } else {
                        outcome
                            .source_revisions
                            .insert(ticket.graph_id.clone(), prepared.graph_revision);
                        outcome.source_revisions.insert(
                            format!("goal:{}", prepared.goal.id),
                            prepared.goal_stream_revision,
                        );
                    }
                }
                if let Some(sources) = committed_sources {
                    // Exact cuts from the original committed transaction, never
                    // an assumed +1 or a refreshed current head.
                    outcome.source_revisions.extend(sources);
                }
                if known_only {
                    let commits = self.services.commit_service().clone();
                    let original = ticket.clone();
                    let session = session.clone();
                    let turn = turn.clone();
                    let (count, digest) = prepared.transcript_prefix.clone();
                    let required = prepared.root_control_team_count;
                    let event = tokio::task::spawn_blocking(move || {
                        commits.prepare_host_transcript(
                            &original,
                            &session,
                            &turn,
                            count,
                            digest,
                            vec![],
                            required,
                        )
                    })
                    .await
                    .map_err(|error| fail(error.to_string()))?
                    .map_err(|error| fail(error.to_string()))?;
                    crate::ExecutionCommitService::attach_host_transcript(&mut outcome, event)
                        .map_err(|error| fail(error.to_string()))?;
                } else {
                    self.state.lock().await.first_model_step = false;
                }
                return Ok(Some(outcome));
            }
            let last = page.last().unwrap();
            let next = (last.sequence, last.input_id.clone());
            if after.as_ref().is_some_and(|old| old >= &next) {
                return Err(fail("pending input replay cursor did not advance".into()));
            }
            after = Some(next);
            if page.len() < 32 {
                return Ok(None);
            }
        }
    }

    async fn accept_user_input_interpretation(
        &self,
        ticket: &NodeExecutionTicket,
        goal: crate::execution_core::goal::GoalProjection,
        frame: crate::execution_core::goal::UserGoalInputFrame,
        graph_revision: u64,
        program_cut: Option<(String, u64)>,
        step: crate::conversation::ModelStepResult,
        consumed: &[crate::SessionInputRecord],
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        use harness_contract::input_disposition::*;
        let fail = |reason: String| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        };
        let usage = ExecutionUsage {
            model: step.model.clone(),
            provider_usage_observed: Some(step.assistant_message.usage.is_some()),
            input_tokens: step.usage.input_tokens.into(),
            output_tokens: step.usage.output_tokens.into(),
            cache_creation_input_tokens: step.usage.cache_creation_input_tokens.into(),
            cache_read_input_tokens: step.usage.cache_read_input_tokens.into(),
            cached_tokens: step.usage.cache_read_input_tokens.into(),
            duration_ms: step.wall_duration_ms,
            ..Default::default()
        };
        {
            let mut state = self.state.lock().await;
            state.iterations = state.iterations.saturating_add(1);
            state.record_root_model_usage(RootModelUsageDelta {
                usage: step.usage,
                model: step.model.clone(),
                models_used: step.models_used.clone(),
                first_token_latency_ms: step.first_token_latency_ms,
                active_stream_duration_ms: step.active_stream_duration_ms,
                wall_duration_ms: step.wall_duration_ms,
                output_chars: 0,
                output_chunks: 1,
                cache_dimensions_known: step.assistant_message.usage.is_some(),
            });
            for record in consumed {
                if !state
                    .pending_disposition_inputs
                    .iter()
                    .any(|r| r.envelope.input_id == record.envelope.input_id)
                {
                    state.pending_disposition_inputs.push(record.clone());
                }
            }
        }
        // Every new input not in the fixed request cut invalidates the proposal,
        // including one first consumed at the Provider request checkpoint.
        let unseen = consumed.iter().any(|r| {
            !frame
                .inputs
                .iter()
                .any(|(i, _, _)| i.input_id == r.envelope.input_id.as_str())
        });
        let proposal = step
            .input_interpretation
            .clone()
            .ok_or_else(|| fail("input interpretation purpose missing".into()))?;
        let mut outcome = NodeExecutionOutcome::new(completed_result(None, usage.clone()))
            .with_replan(ExecutionGraphReplan {
                nodes: vec![dynamic_node(
                    ticket,
                    self.state.lock().await.iterations,
                    "user-input-continuation",
                    ExecutionNodeKind::InlineModel,
                    "inline_model",
                    "inline_model",
                )],
                edges: vec![],
                reason: "user-input interpretation continues original Goal execution".into(),
                framework_authored: true,
            });
        outcome.replan.as_mut().unwrap().edges =
            dynamic_edges(&ticket.node_id, &outcome.replan.as_ref().unwrap().nodes);
        if unseen {
            // Re-read the original pending inputs after a genuine cut change.
            // This says nothing about the validity of the stale proposal.
            return Ok(outcome);
        }
        let proposal = proposal.map_err(|reason| fail(format!(
            "user input interpretation rejected: {reason}"
        )))?;
        let decision = &proposal.decisions[0];
        let mutation = if decision.action == InputDispositionAction::AmendCurrentTurn {
            Some(
                self.services
                    .goal_store()
                    .prepare_user_input_revision(&goal, &frame, &proposal, ticket)
                    .map_err(fail)?,
            )
        } else {
            None
        };
        // Capture the original transcript prefix with the proposal. A cold
        // result replay must not bind itself to a later Session prefix.
        let snapshot = self.runtime.lock().await.session_snapshot().await;
        if snapshot.session_id != goal.goal.session_id {
            return Err(fail("prepared user transcript Session mismatch".into()));
        }
        let prefix = snapshot.materialize_messages();
        let transcript_prefix = (
            prefix.len() as u64,
            host_message_prefix_digest(&prefix).map_err(fail)?,
        );
        let root_control_team_count = self
            .state
            .lock()
            .await
            .pending_root_control_plane_requirement;
        let prepared = PreparedUserGoalApplication {
            frame,
            goal_stream_revision: goal.stream_revision,
            goal: goal.goal,
            proposal,
            mutation,
            ticket: ticket.clone(),
            graph_revision,
            program_cut,
            continuation: outcome
                .replan
                .clone()
                .ok_or_else(|| fail("prepared input continuation missing".into()))?,
            finished_at_ms: outcome.result.finished_at_ms,
            transcript_prefix,
            root_control_team_count,
            usage,
            provider_message: host_message_value(&step.assistant_message),
            provider_usage: step.usage,
        };
        let bytes = serde_json::to_vec(&prepared).map_err(|e| fail(e.to_string()))?;
        if bytes.len() > 2 * 1024 * 1024 {
            return Err(fail(
                "user application artifact exceeds bounded contract".into(),
            ));
        }
        let session_id = prepared.goal.session_id.clone();
        let artifact = self
            .services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/json".into(),
                    visibility_scope: format!("session:{session_id}"),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                &bytes,
            )
            .await
            .map_err(|e| fail(e.to_string()))?;
        {
            let query = self
                .services
                .session_query_port()
                .ok_or_else(|| fail("user application query missing".into()))?;
            let journal = self
                .services
                .session_journal_port()
                .ok_or_else(|| fail("user application journal missing".into()))?;
            let mut rows = Vec::new();
            for (source, _, _) in &prepared.frame.inputs {
                let current = query
                    .runtime_input_by_input_id(&source.input_id)
                    .await
                    .map_err(|e| fail(e.to_string()))?
                    .ok_or_else(|| fail("user input vanished before reservation".into()))?;
                if current.revision != source.revision {
                    return Err(fail(
                        "user application original input revision is stale".into(),
                    ));
                }
                rows.push(current);
            }
            let binding = prepared
                .goal
                .execution_binding
                .as_ref()
                .ok_or_else(|| fail("user Goal binding missing".into()))?;
            let ids = rows.iter().map(|r| r.input_id.clone()).collect::<Vec<_>>();
            let identity = PreparedInputApplication {
                artifact_ref: artifact.selector.clone(),
                artifact_sha256: format!("{:x}", sha2::Sha256::digest(&bytes)),
                session_id: session_id.clone(),
                session_generation: rows[0].session_generation,
                turn_id: binding.turn_id.clone(),
                goal_id: prepared.goal.id.clone(),
                goal_revision: prepared.goal.revision,
                goal_spec_digest: prepared.goal.spec_digest.clone(),
                graph_id: ticket.graph_id.clone(),
                graph_revision,
                node_id: ticket.node_id.clone(),
                attempt_id: ticket.attempt.to_string(),
                source_digest: prepared.frame.digest().map_err(fail)?,
                program_cut: prepared.program_cut.clone(),
            };
            let mut receipt = SessionInputApplicationReceipt {
                disposition_id: format!(
                    "user-goal:{}:{}",
                    prepared.goal.id, identity.source_digest
                ),
                prepared_application: Some(identity),
                cancellation: None,
                leader_input_id: ids[0].clone(),
                input_ids: ids,
                action: prepared.proposal.decisions[0].action,
                relation: prepared.proposal.decisions[0].relation,
                state: InputApplicationState::Prepared,
                objective: prepared.proposal.decisions[0].objective.clone(),
                required: true,
                attempts: 1,
                summary: if prepared.mutation.is_some() {
                    "authenticated user Goal revision"
                } else {
                    "authenticated user input observation without Goal revision"
                }
                .into(),
                task_ids: vec![],
                team_ids: vec![],
                agent_ids: vec![],
                execution_ids: vec![],
                target_session_id: None,
                target_session_created: false,
                error: None,
                revision: 0,
                updated_at_ms: user_application_clock(),
            };
            rows = journal
                .commit_input_application_receipt(
                    &receipt.input_ids,
                    &rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
                    &receipt,
                    receipt.updated_at_ms,
                )
                .await
                .map_err(|e| fail(e.to_string()))?;
            receipt.state = InputApplicationState::Materializing;
            receipt.revision += 1;
            receipt.updated_at_ms = user_application_clock();
            journal
                .commit_input_application_receipt(
                    &receipt.input_ids,
                    &rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
                    &receipt,
                    receipt.updated_at_ms,
                )
                .await
                .map_err(|e| fail(e.to_string()))?;
            if let Some(mutation) = &prepared.mutation {
                if let Some((_, revision)) = prepared.program_cut {
                    self.services
                        .commit_user_input_goal_revision(
                            &prepared.goal,
                            &prepared.frame,
                            mutation,
                            revision,
                            graph_revision,
                            &artifact.selector,
                        )
                        .map_err(fail)?;
                } else {
                    outcome
                        .source_revisions
                        .insert(ticket.graph_id.clone(), graph_revision);
                    outcome.source_revisions.insert(
                        mutation.stream_id.clone(),
                        mutation.expected_stream_revision,
                    );
                    outcome.domain_events.push(mutation.event.clone());
                }
            } else {
                outcome
                    .source_revisions
                    .insert(ticket.graph_id.clone(), graph_revision);
                outcome.source_revisions.insert(
                    format!("goal:{}", prepared.goal.id),
                    prepared.goal_stream_revision,
                );
            }
        }
        outcome.result.result_ref = Some(format!("user_input_application:{}", artifact.selector));
        // No internal proposal JSON enters the public assistant transcript.
        let mut state = self.state.lock().await;
        state.last_verified_progress = true;
        state.pending_disposition_inputs.retain(|r| {
            !prepared
                .frame
                .inputs
                .iter()
                .any(|(i, _, _)| i.input_id == r.envelope.input_id.as_str())
        });
        Ok(outcome)
    }
}

fn host_message_value(message: &ConversationMessage) -> serde_json::Value {
    serde_json::json!({"role":message.role.role_str(),"blocks":message.blocks,"usage":message.usage})
}

fn host_message_from_value(value: &serde_json::Value) -> Result<ConversationMessage, String> {
    let role = match value["role"].as_str() {
        Some("system") => crate::MessageRole::System,
        Some("user") => crate::MessageRole::User,
        Some("assistant") => crate::MessageRole::Assistant,
        Some("tool") => crate::MessageRole::Tool,
        _ => return Err("Host transcript message role invalid".into()),
    };
    Ok(ConversationMessage {
        role,
        blocks: serde_json::from_value(value["blocks"].clone())
            .map_err(|error| error.to_string())?,
        usage: serde_json::from_value(value["usage"].clone()).map_err(|error| error.to_string())?,
    })
}

fn host_message_prefix_digest(messages: &[ConversationMessage]) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    if messages.len() > 16384 {
        return Err("Host transcript prefix message budget exceeded".into());
    }
    let mut hasher = Sha256::new();
    let mut total = 0usize;
    for message in messages {
        let bytes =
            serde_json::to_vec(&host_message_value(message)).map_err(|error| error.to_string())?;
        total = total
            .checked_add(bytes.len())
            .ok_or("Host transcript prefix byte overflow")?;
        if total > 8 * 1024 * 1024 {
            return Err("Host transcript prefix byte budget exceeded".into());
        }
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn apply_host_transcript_messages(
    session: &mut crate::Session,
    session_id: &str,
    prefix_count: u64,
    prefix_sha256: &str,
    messages: &[serde_json::Value],
) -> Result<(), String> {
    if session.session_id != session_id || messages.len() > 256 {
        return Err("Host transcript Session/message identity mismatch".into());
    }
    let prefix_count =
        usize::try_from(prefix_count).map_err(|_| "Host transcript prefix count overflow")?;
    if prefix_count > 16384 || session.message_count() < prefix_count {
        return Err("Host transcript original Session prefix missing".into());
    }
    let prefix = session
        .messages()
        .take(prefix_count)
        .cloned()
        .collect::<Vec<_>>();
    if host_message_prefix_digest(&prefix)? != prefix_sha256 {
        return Err("Host transcript original Session prefix changed".into());
    }
    let pending = messages
        .iter()
        .map(host_message_from_value)
        .collect::<Result<Vec<_>, _>>()?;
    let present = session
        .message_count()
        .saturating_sub(prefix_count)
        .min(pending.len());
    for (offset, expected) in pending.iter().take(present).enumerate() {
        if session.message(prefix_count + offset) != Some(expected) {
            return Err("Host transcript published Session segment diverged".into());
        }
    }
    session.extend_messages(pending.into_iter().skip(present));
    Ok(())
}

async fn stage_host_transcript<C, T>(
    runtime: &Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    services: &Arc<crate::RuntimeServices>,
    ticket: &NodeExecutionTicket,
    session_id: String,
    turn_id: String,
    messages: Vec<ConversationMessage>,
    root_control_team_count: Option<u8>,
    outcome: &mut NodeExecutionOutcome,
) -> Result<(), NodeExecutorError>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    let invalid = |reason: String| NodeExecutorError::Poll {
        node_id: ticket.node_id.clone(),
        reason,
    };
    let snapshot = runtime.lock().await.session_snapshot().await;
    if snapshot.session_id != session_id {
        return Err(invalid("Host transcript stage Session mismatch".into()));
    }
    let prefix = snapshot.materialize_messages();
    let prefix_sha256 = host_message_prefix_digest(&prefix).map_err(&invalid)?;
    let prefix_count = prefix.len() as u64;
    let messages = messages.iter().map(host_message_value).collect();
    let commits = services.commit_service().clone();
    let staged_ticket = ticket.clone();
    let event = tokio::task::spawn_blocking(move || {
        commits.prepare_host_transcript(
            &staged_ticket,
            &session_id,
            &turn_id,
            prefix_count,
            prefix_sha256,
            messages,
            root_control_team_count,
        )
    })
    .await
    .map_err(|error| invalid(error.to_string()))?
    .map_err(|error| invalid(error.to_string()))?;
    crate::ExecutionCommitService::attach_host_transcript(outcome, event)
        .map_err(|error| invalid(error.to_string()))
}

async fn publish_host_transcript<C, T>(
    runtime: &Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    services: &Arc<crate::RuntimeServices>,
    ticket: &NodeExecutionTicket,
) -> Result<(String, String, Option<u8>), NodeExecutorError>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    let invalid = |reason: String| NodeExecutorError::Poll {
        node_id: ticket.node_id.clone(),
        reason,
    };
    let commits = services.commit_service().clone();
    let original = ticket.clone();
    #[cfg(test)]
    if std::env::var_os("COWD_R622_TRACE").is_some() {
        eprintln!("r622_toolbatch phase=transcript_read_enter node={} executor={}", ticket.node_id, ticket.executor_kind);
    }
    let document =
        tokio::task::spawn_blocking(move || commits.committed_host_transcript(&original))
            .await
            .map_err(|error| invalid(error.to_string()))?
            .map_err(|error| invalid(error.to_string()))?;
    #[cfg(test)]
    if std::env::var_os("COWD_R622_TRACE").is_some() {
        eprintln!("r622_toolbatch phase=transcript_runtime_lock_enter node={} executor={}", ticket.node_id, ticket.executor_kind);
    }
    let mut owner = runtime.lock().await;
    #[cfg(test)]
    if std::env::var_os("COWD_R622_TRACE").is_some() {
        eprintln!("r622_toolbatch phase=transcript_session_lock_enter node={} executor={}", ticket.node_id, ticket.executor_kind);
    }
    let mut session = owner.session_mut_async().await;
    apply_host_transcript_messages(
        &mut session,
        &document.session_id,
        document.prefix_count,
        &document.prefix_sha256,
        &document.messages,
    )
    .map_err(&invalid)?;
    #[cfg(test)]
    if std::env::var_os("COWD_R622_TRACE").is_some() {
        eprintln!("r622_toolbatch phase=transcript_apply_done node={} executor={}", ticket.node_id, ticket.executor_kind);
    }
    Ok((
        document.session_id,
        document.turn_id,
        document.root_control_team_count,
    ))
}

fn persist_host_control_plane_event(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    event: crate::RuntimeEventInput,
) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    let key = format!(
        "host-transcript:{:x}:root-control",
        Sha256::digest(serde_json::to_vec(ticket).map_err(|error| error.to_string())?)
    );
    let store = services.event_store();
    for _attempt in 0..4 {
        if let Some(existing) = store
            .event_by_idempotency_key(&event.stream_id, &key)
            .map_err(|error| error.to_string())?
        {
            if existing.kind != event.kind
                || existing.actor != event.actor
                || existing.scope != event.scope
                || existing.refs != event.refs
                || existing.payload["required_team_count"] != event.payload["required_team_count"]
            {
                return Err("Host control-plane publication identity mismatch".into());
            }
            return Ok(());
        }
        let revision = store
            .stream_revision(&event.stream_id)
            .map_err(|error| error.to_string())?;
        let result = store.append_batch_if_revision(
            event.stream_id.clone(),
            revision,
            key.clone(),
            vec![crate::RuntimeTransactionEventInput {
                event: event.clone(),
                schema_version: 1,
                idempotency_key: Some(key.clone()),
            }],
        );
        if result.is_ok() {
            return Ok(());
        }
    }
    Err("Host control-plane publication did not obtain its original source CAS".into())
}

/// Consecutive count of the same tool batch, driven by the question alone.
///
/// The result digest deliberately does not take part. A point lookup whose
/// payload carries a revision counter changes bytes on every round while
/// telling the model nothing new, so keying the streak on the answer let a
/// stuck loop run forever: one live run issued the same `state_inspect` on one
/// task entry 18 times over 11 minutes, the result bytes moved each time, the
/// streak reset to 1 each time, and only an external monitor stopped it. What
/// makes a repeat legitimate is not that the answer moved - it is that the
/// caller is waiting, which `is_loop_nudge_repeatable_tool` decides before this
/// function is ever reached.
fn next_identical_action_streak(
    previous_digest: &str,
    action_fingerprint: &str,
    current_streak: usize,
) -> usize {
    if previous_digest == action_fingerprint {
        current_streak.saturating_add(1)
    } else {
        1
    }
}

/// The durable `tool://` receipt references this turn has actually produced.
///
/// Bounded and ordered oldest-first so the list is stable across a run: a model
/// that cites one of these can never be refused for identity, and one that needs
/// a ref it does not hold sees immediately that it must read again rather than
/// guessing. Only references the turn produced are listed - never a remembered
/// one from an earlier session.
fn durable_receipt_refs(messages: &[ConversationMessage]) -> Vec<String> {
    const HELD_RECEIPT_LIMIT: usize = 16;
    let mut refs = Vec::new();
    for message in messages {
        for block in &message.blocks {
            let ContentBlock::ToolResult { output, .. } = block else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
                continue;
            };
            for key in ["evidence_ref", "full_output_ref", "output_ref"] {
                if let Some(reference) = value.get(key).and_then(serde_json::Value::as_str) {
                    if reference.starts_with("tool://") && !refs.iter().any(|r| r == reference) {
                        refs.push(reference.to_string());
                    }
                }
            }
        }
    }
    if refs.len() > HELD_RECEIPT_LIMIT {
        refs.split_off(refs.len() - HELD_RECEIPT_LIMIT)
    } else {
        refs
    }
}

/// Record guidance that a terminating turn will never deliver.
///
/// `pending_next_model_context` is drained unconditionally at the start of a
/// model step, but a fuse intervention ends that same turn, so anything drained
/// is delivered to nobody. One live run wrote the tool-failure nudge, then
/// blocked the turn: the framework had decided what to tell the model and then
/// closed its mouth. Recording the identities on the durable intervention makes
/// the loss attributable (1006r M4: a failure path must produce evidence
/// isomorphic to the success path) without leaking Private guidance text into
/// the user-visible terminal.
fn record_undelivered_guidance(
    intervention: &mut harness_contract::goal::RuntimeIntervention,
    undelivered: &[String],
) {
    if undelivered.is_empty() {
        return;
    }
    let count = undelivered.len();
    intervention
        .evidence_refs
        .extend(undelivered.iter().cloned());
    if intervention.kind == harness_contract::goal::RuntimeInterventionKind::Block {
        intervention.reason.push_str(&format!(
            " Runtime prepared {count} guidance item(s) for a next model step that this \
             termination prevented; they are recorded on the intervention evidence refs."
        ));
    }
}

#[test]
fn duplicate_result_stubbing_is_on_unless_the_operator_opts_out() {
    let _lock = crate::test_env_lock();
    struct Restore(Option<std::ffi::OsString>);
    impl Drop for Restore {
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var("COWD_LOOP_STUB_RESULTS", value),
                None => std::env::remove_var("COWD_LOOP_STUB_RESULTS"),
            }
        }
    }
    let original = std::env::var_os("COWD_LOOP_STUB_RESULTS");
    let restore = Restore(original.clone());
    // Default-on matters: one live delegated Agent issued the same `read_file`
    // sixteen times in a row against a 145k-token context, and the stub that
    // would have broken that loop was opt-in, so it never ran. Only an explicit
    // opt-out disables it.
    std::env::remove_var("COWD_LOOP_STUB_RESULTS");
    assert!(
        !result_stub_disabled(),
        "with no operator setting, stubbing must be on"
    );
    std::env::set_var("COWD_LOOP_STUB_RESULTS", "0");
    assert!(result_stub_disabled(), "an explicit 0 opts out");
    std::env::set_var("COWD_LOOP_STUB_RESULTS", "false");
    assert!(result_stub_disabled(), "an explicit false opts out");
    // Anything else, including a stale truthy value from the old opt-in spelling,
    // keeps stubbing on.
    std::env::set_var("COWD_LOOP_STUB_RESULTS", "1");
    assert!(
        !result_stub_disabled(),
        "the old opt-in spelling must not silently disable it"
    );
    drop(restore);
    assert_eq!(std::env::var_os("COWD_LOOP_STUB_RESULTS"), original);
    let result = std::panic::catch_unwind(|| {
        let _restore = Restore(original.clone());
        std::env::set_var("COWD_LOOP_STUB_RESULTS", "panic-scope");
        panic!("exercise environment restoration during unwind");
    });
    assert!(result.is_err());
    assert_eq!(std::env::var_os("COWD_LOOP_STUB_RESULTS"), original);
}

#[test]
fn held_receipts_name_only_what_this_turn_produced() {
    // A model that has lost the thread cites a ref from an earlier session or a
    // round it no longer holds; one live run was refused with "no canonical
    // durable receipt in Session ...". Listing what the turn actually produced
    // makes that refusal impossible to earn by identity.
    use crate::ConversationMessage;
    let mut messages = vec![ConversationMessage::tool_result(
        "call-1",
        "read_file",
        serde_json::json!({"evidence_ref": "tool://first", "bytes": 10}).to_string(),
        false,
    )];
    messages.push(ConversationMessage::tool_result(
        "call-2",
        "read_ref",
        serde_json::json!({"full_output_ref": "tool://second"}).to_string(),
        false,
    ));
    // A remembered reference from another session must never be offered.
    messages.push(ConversationMessage::tool_result(
        "call-3",
        "read_ref",
        serde_json::json!({"evidence_ref": "artifact://not-a-tool-receipt"}).to_string(),
        false,
    ));
    let held = durable_receipt_refs(&messages);
    assert_eq!(
        held,
        vec!["tool://first".to_string(), "tool://second".to_string()]
    );
    // The list is bounded: an unbounded projection would grow the prompt every
    // round for no new information.
    let many = (0..40)
        .map(|index| {
            ConversationMessage::tool_result(
                format!("call-{index}"),
                "read_file",
                serde_json::json!({"evidence_ref": format!("tool://r{index}")}).to_string(),
                false,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(durable_receipt_refs(&many).len(), 16);
}

#[test]
fn the_same_question_counts_as_a_repeat_even_when_the_answer_moved() {
    // The bug this pins: the streak used to require an identical result too, so
    // a point lookup whose payload carries a revision counter never accumulated.
    // One live run issued the same `state_inspect` 18 times over 11 minutes.
    let question = "tool_action:same";
    let mut streak = next_identical_action_streak("", question, 0);
    assert_eq!(streak, 1, "the first ask starts the streak");
    for round in 2..=18 {
        // Each round returns different bytes: a revision counter moved.
        let _moved_result = format!("result-revision-{round}");
        streak = next_identical_action_streak(question, question, streak);
        assert_eq!(streak, round, "an unchanged question must keep counting");
    }
    assert_eq!(streak, 18);

    // A different question resets it, so a genuine change of subject is not a loop.
    assert_eq!(
        next_identical_action_streak(question, "tool_action:other", 18),
        1
    );
}

#[test]
fn only_a_genuine_wait_is_exempt_from_the_identical_call_streak() {
    let call = |name: &str, input: &str| ModelToolCall {
        binding: None,
        id: "call-1".to_string(),
        name: name.to_string(),
        input: input.to_string(),
        depends_on: Vec::new(),
    };
    // The documented wait: the model parks until a worker moves.
    assert!(is_loop_nudge_repeatable_tool(&call(
        "state_inspect",
        r#"{"wait_for_workers": true}"#
    )));
    // A point lookup of one entry is not a wait. This is the shape that looped.
    assert!(!is_loop_nudge_repeatable_tool(&call(
        "state_inspect",
        r#"{"entry_ref": "task:47819154d3e1b2385139b9bd"}"#
    )));
    // An explicit false is not a wait either.
    assert!(!is_loop_nudge_repeatable_tool(&call(
        "state_inspect",
        r#"{"wait_for_workers": false}"#
    )));
    // The transport pollers stay exempt.
    assert!(is_loop_nudge_repeatable_tool(&call("process", "{}")));
    assert!(is_loop_nudge_repeatable_tool(&call("job_get_result", "{}")));
    assert!(is_loop_nudge_repeatable_tool(&call("job_poll", "{}")));
    // Ordinary tools are never exempt.
    assert!(!is_loop_nudge_repeatable_tool(&call("read_file", "{}")));
}

#[test]
fn a_terminating_turn_records_the_guidance_it_will_not_deliver() {
    use harness_contract::goal::{RuntimeIntervention, RuntimeInterventionKind};

    let blocked = || {
        RuntimeIntervention {
        goal_id: "goal:1".to_string(),
        kind: RuntimeInterventionKind::Block,
        reason: "multiple consecutive tool batches failed before any checked evidence was retained; stop speculative retries".to_string(),
        evidence_refs: vec!["execution_node:node-1".to_string()],
        expected_graph_revision: None,
    }
    };

    // Nothing was prepared: the intervention is untouched, so a normal block
    // does not grow a meaningless clause.
    let mut untouched = blocked();
    record_undelivered_guidance(&mut untouched, &[]);
    assert_eq!(
        untouched.evidence_refs,
        vec!["execution_node:node-1".to_string()]
    );
    assert!(!untouched.reason.contains("guidance item(s)"));

    // Guidance was written for a next step the termination prevents: the
    // identities become durable and the reason states the count.
    let mut recorded = blocked();
    record_undelivered_guidance(
        &mut recorded,
        &[
            "tool-failure-nudge:node-1".to_string(),
            "evidence-saturation-nudge:node-1".to_string(),
        ],
    );
    assert!(
        recorded
            .evidence_refs
            .contains(&"tool-failure-nudge:node-1".to_string()),
        "the nudge identity must be durable: {:?}",
        recorded.evidence_refs
    );
    assert!(
        recorded
            .reason
            .contains("Runtime prepared 2 guidance item(s)"),
        "the blocked reason must state what was not delivered: {}",
        recorded.reason
    );
    assert!(
        !recorded.reason.contains("Do not repeat them unchanged"),
        "Private guidance text must not leak into the operator-visible reason"
    );

    // A non-blocking intervention still records the identities, but must not
    // grow a clause the model is never shown.
    let mut replan = RuntimeIntervention {
        kind: RuntimeInterventionKind::Replan,
        ..blocked()
    };
    record_undelivered_guidance(&mut replan, &["tool-failure-nudge:node-1".to_string()]);
    assert!(replan
        .evidence_refs
        .contains(&"tool-failure-nudge:node-1".to_string()));
    assert!(!replan.reason.contains("guidance item(s)"));
}

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
    fn retains_physical_execution(&self) -> bool {
        true
    }

    fn request_stop(&self, ticket: &NodeExecutionTicket) {
        if ticket.graph_id == self.ticket.graph_id
            && ticket.node_id == self.ticket.node_id
            && ticket.attempt == self.ticket.attempt
            && ticket.idempotency_key == self.ticket.idempotency_key
            && ticket.executor_kind == self.ticket.executor_kind
            && ticket.payload_ref == self.ticket.payload_ref
            && ticket.service_class == self.ticket.service_class
        {
            self.stop.cancel();
        }
    }

    async fn recover_known_outcome(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        self.resume_user_input_application(ticket, true).await
    }

    async fn reconcile_original_custody(
        &self,
        ticket: &NodeExecutionTicket,
        workers: &[(String, crate::conversation::EarlyToolDispatchResult)],
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        if ticket != &self.ticket || self.stop.is_cancelled() { return Ok(None); }
        let Some(error) = self.local_fence.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).clone() else { return Ok(None) };
        if !error.early_tool_failures().is_empty() { return Ok(None); }
        let (mut early, originals, observed) = {
            let state = self.state.lock().await;
            (state.early_tool_receipts.clone(), state.pending_member_batches.clone(), state.tool_receipts_observed)
        };
        let recorded_early = early.clone();
        for receipt in error.effect_receipts() {
            if early.get(&receipt.call.id).is_some_and(|old| old != receipt) { return Ok(None); }
            early.insert(receipt.call.id.clone(), receipt.clone());
        }
        let mut deferred_sources = BTreeMap::new();
        let mut worker_ids = BTreeSet::new();
        for (call_id, result) in workers {
            if !worker_ids.insert(call_id.clone()) { return Ok(None); }
            if let crate::conversation::EarlyToolDispatchResult::Deferred(deferred) = result {
                if early.contains_key(call_id) { return Ok(None); }
                let Some(sources) = original_early_deferral_sources(&self.services, ticket, call_id, deferred)
                    .unwrap_or(None) else { return Ok(None) };
                for (stream, head) in sources {
                    if deferred_sources.insert(stream, head).is_some_and(|old| old != head) { return Ok(None); }
                }
                continue;
            }
            let crate::conversation::EarlyToolDispatchResult::Executed(original) = result else { return Ok(None) };
            let Some(receipt) = early.get(call_id) else { return Ok(None) };
            let (Some(expected), Some(actual)) = (&receipt.original_source, &original.original_source) else { return Ok(None) };
            if original.call != receipt.call || original.outcome != receipt.outcome
                || expected.ticket != actual.ticket || !Arc::ptr_eq(&expected.request, &actual.request) {
                return Ok(None);
            }
        }
        let early = early.into_values().collect::<Vec<_>>();
        if !verify_original_early_read_set(&self.services, &ticket.graph_id, &early, &originals).unwrap_or(false) {
            return Ok(None);
        }
        let settled = if !originals.is_empty() {
            settle_original_host_carriers(&self.services, &self.state, &ticket.graph_id, &early)
                .await.unwrap_or(false)
        } else { true };
        if !settled || self.stop.is_cancelled() { return Ok(None); }
        {
            let state = self.state.lock().await;
            if !state.pending_member_batches.is_empty() || state.early_tool_receipts != recorded_early
                || state.tool_receipts_observed != observed { return Ok(None); }
        }
        let Some(mut sources) = original_sibling_closure_cut(&self.services, ticket).unwrap_or(None) else {
            return Ok(None);
        };
        for (stream, head) in deferred_sources {
            if sources.insert(stream, head).is_some_and(|old| old != head) { return Ok(None); }
        }
        // Pin the current heads actually read for each original call. The
        // closure cut pins the original graph and its physical sources.
        for receipt in &early {
            let Some(source) = receipt.original_source.as_ref() else { return Ok(None) };
            let stream = format!("execution-effect:{}", source.request.idempotency_key);
            let head = self.services.event_store().stream_revision(&stream)
                .map_err(|error| NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
            if sources.insert(stream, head).is_some_and(|old| old != head) { return Ok(None); }
        }
        for ((node_id, attempt), calls) in &originals {
            let index = format!("execution-agent-receipts:{}:{}:{}", ticket.graph_id, node_id, attempt);
            let head = self.services.event_store().stream_revision(&index)
                .map_err(|error| NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
            if sources.insert(index, head).is_some_and(|old| old != head) { return Ok(None); }
            for (_, pending) in calls {
                let Some((_, calls)) = pending.original_host_call_set() else { return Ok(None) };
                for call in calls {
                    let Some(binding) = call.binding.as_ref() else { return Ok(None) };
                    let stream = format!("execution-effect:{}", binding.effect_idempotency_key());
                    let head = self.services.event_store().stream_revision(&stream)
                        .map_err(|error| NodeExecutorError::OriginalCustodyUnknown { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
                    if sources.insert(stream, head).is_some_and(|old| old != head) { return Ok(None); }
                }
            }
        }
        if self.stop.is_cancelled() { return Ok(None); }
        let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Failed, result_ref: None, wait_class: None,
            summary: None, evidence_refs: Vec::new(),
            failure: Some(harness_contract::execution_graph::ExecutionFailure {
                kind: "session_execution_fence".into(), message: error.to_string(),
                retryable: false, evidence_refs: Vec::new(),
            }),
            usage: ExecutionUsage::default(), finished_at_ms: user_application_clock(),
        });
        outcome.source_revisions = sources;
        Ok(Some(outcome))
    }

    async fn confirm_original_early_release(
        &self,
        ticket: &NodeExecutionTicket,
        workers: &[(String, crate::conversation::EarlyToolDispatchResult)],
    ) -> Result<bool, NodeExecutorError> {
        if ticket != &self.ticket || self.stop.is_cancelled() { return Ok(false); }
        if workers.is_empty() { return Ok(true); }
        let mut receipts = self.state.lock().await.early_tool_receipts.clone();
        if let Some(error) = self.local_fence.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).clone()
        {
            for receipt in error.effect_receipts() {
                if receipts.get(&receipt.call.id).is_some_and(|old| old != receipt) { return Ok(false); }
                receipts.insert(receipt.call.id.clone(), receipt.clone());
            }
        }
        let mut checked = BTreeSet::new();
        for (call_id, result) in workers {
            if !checked.insert(call_id.clone()) { return Ok(false); }
            if let crate::conversation::EarlyToolDispatchResult::Deferred(deferred) = result {
                if receipts.contains_key(call_id)
                    || original_early_deferral_sources(&self.services, ticket, call_id, deferred)
                        .unwrap_or(None).is_none() { return Ok(false); }
                continue;
            }
            let crate::conversation::EarlyToolDispatchResult::Executed(original) = result else { return Ok(false) };
            let Some(receipt) = receipts.get(call_id) else { return Ok(false) };
            let (Some(sealed), Some(source)) = (&receipt.original_source, &original.original_source) else { return Ok(false) };
            if original.call != receipt.call || original.outcome != receipt.outcome
                || source.ticket != sealed.ticket || !Arc::ptr_eq(&source.request, &sealed.request)
                || !original_early_read_is_committed(&self.services, ticket, receipt).unwrap_or(false) {
                return Ok(false);
            }
        }
        Ok(!self.stop.is_cancelled())
    }

    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        if self.stop.is_cancelled() {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "model step cancelled before execution".into(),
            });
        }
        let result = async {
        if let Some(outcome)=self.resume_user_input_application(ticket, false).await? {return Ok(outcome);}
        // A Program verdict may wake the barrier before the ingress loop has
        // installed its presentation override. Derive it from the committed
        // Goal so that the resumed model node cannot race into another call.
        let (session, turn, goal_id, is_root) = {
            let state = self.state.lock().await;
            (state.session_id.clone(), state.turn_id.clone(), state.goal_id.clone(), state.execution_role.owns_root_presentation())
        };
        let program = is_root.then(|| super::root_agentic_program_projection(self.services.as_ref(), &session, &turn)).flatten();
        if let Some(verdict) = program.as_ref().and_then(|program| program.objective_verdict.as_ref())
            .filter(|verdict| verdict.kind == harness_contract::goal::ObjectiveTerminalKind::Blocked)
        {
            let goal = self.services.goal_store().get(&goal_id)
                .map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?
                .ok_or_else(|| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: "blocked Program Goal missing".into() })?;
            let terminal = goal.terminal.as_ref().filter(|terminal| goal.id == verdict.goal_id
                && goal.revision == verdict.goal_revision && terminal.terminal_fence == verdict.terminal_fence
                && terminal.authority_revision == verdict.authority_revision
                && terminal.kind == verdict.kind && goal.completion == GoalCompletion::Blocked)
                .ok_or_else(|| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: "blocked Program Goal verdict mismatch".into() })?;
            self.state.lock().await.terminal_override = Some((GoalCompletion::Blocked, terminal.reason.clone()));
        }
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
                    // The Objective is the purpose; a Task is a means. Waiting for
                    // every Task rung to close as well deadlocks the exit: one live
                    // run satisfied the Objective review and then spent its whole
                    // remaining window in a Task rework cycle, never reaching the
                    // request that would have bound the verdict.
                    //
                    // The Goal stays the authority. A reworked Task the Objective
                    // genuinely depends on reopens the completion gap, which closes
                    // this gate again on the next step; an unclassified issue keeps
                    // `unresolved` non-empty and holds it shut the same way.
                    let objective_ready = gap.is_none() && program.unresolved.is_empty();
                    let mut state = self.state.lock().await;
                    if (outstanding == 0 || objective_ready)
                        && gap.is_none()
                        && program.completion_request.is_none()
                    {
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
            // The Objective can be satisfied by the very action that closes it,
            // which means the root's last step is that tool call and it is never
            // given the turn that writes the user-visible answer. One live run
            // reached a satisfied verdict with every Task accepted and then went
            // silent for 600s until an inactivity watchdog cancelled it: the
            // business was finished and nothing was delivered. Once the verdict
            // is bound, hand the root the turn it is missing.
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
                    let mut state = self.state.lock().await;
                    if program.status == crate::AgenticProgramStatus::Verified
                        && program.objective_verdict.is_some()
                        && !state.final_answer_observed
                    {
                        state.verified_answer_ready_steps =
                            state.verified_answer_ready_steps.saturating_add(1);
                        if state.verified_answer_ready_steps >= verified_answer_drive_after_steps()
                        {
                            state.verified_answer_ready_steps = 0;
                            state.final_answer_observed = true;
                            // Hand the root the turn it never got. The carrier
                            // names every accepted Task's delivered artifact, and
                            // the terminal path narrates it into the answer the
                            // operator reads - the same route a collaboration
                            // presentation takes.
                            let results = verified_program_delivered_results(&program);
                            if !results.is_empty() {
                                state.terminal_override = Some((
                                    harness_contract::goal::GoalCompletion::Satisfied,
                                    super::collaboration_synthesis_layer(results, 0),
                                ));
                                drop(state);
                                return Ok(precommitted_terminal_outcome(ticket));
                            }
                        }
                    } else {
                        state.verified_answer_ready_steps = 0;
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
             Some(self.services.as_ref()), true, )?;
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
        let write_goal_id = self.state.lock().await.goal_id.clone();
        let write_goal_source = self.services.goal_store().projection(&write_goal_id)
            .map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?
            .ok_or_else(|| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: "current Goal missing".into() })?;
        let required_workspace_write_scopes = {
            let state = self.state.lock().await;
            current_turn_write_scopes(&write_goal_source.goal, state.execution_role.is_delegated_leaf(), &state.focus_acceptance_scopes, state.task_understanding.as_ref().is_some_and(|understanding|understanding.requires_write))
        };
        let required_write_for_completion = !required_workspace_write_scopes.is_empty();
        let (root_requires_action_channel, required_effects_pending, operator_forbids_tool_use) = {
            let state = self.state.lock().await;
            let program_status = if state.execution_role.owns_root_presentation() {
                let objective_id = harness_contract::agent_action::root_objective_id(
                    &state.session_id,
                    &state.turn_id,
                );
                let program_id =
                    harness_contract::agent_action::program_id_for_objective(&objective_id);
                self.services
                    .agent_action_service()
                    .project_if_exists(&program_id)
                    .map_err(|error| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: format!("load root request boundary: {error}"),
                    })?
                    .map(|program| program.status)
            } else {
                None
            };
            (
                root_program_requires_action_channel(state.execution_role, program_status),
                state.execution_role.is_delegated_leaf()
                    && (!state.focus_acceptance_pending_scopes.is_empty()
                        || (required_write_for_completion
                            && !write_obligation_satisfied(
                                required_write_for_completion,
                                &required_workspace_write_scopes,
                                &state.committed_workspace_observed_evidence,
                                state.committed_workspace_write_observed,
                                self.services.path_identity_resolver(),
                            ))),
                current_turn_forbids_tools(&write_goal_source.goal,
                    state.task_understanding.as_ref().is_some_and(|understanding|understanding.forbids_tool_use)),
            )
        };
        let action_channel_required = root_requires_action_channel || required_effects_pending;
        let (
            content,
            first_step,
            fuse_intervention,
            force_text_only_response,
            forced_terminal_presentation_ready,
            force_tool_allowlist,
            force_reasoning_effort,
            clean_terminal_synthesis,
            clean_terminal_evidence,
            provider_retry_fenced,
            pending_next_model_context,
            undelivered_guidance_refs,
            pending_disposition_inputs,
        ) = {
            let mut state = self.state.lock().await;
            let pending_disposition_inputs = state.pending_disposition_inputs.clone();
            let first = state.first_model_step;
            state.first_model_step = false;
            let turn_framing = crate::host::scope_turn_framing_for_role(
                crate::host::agentic_turn_framing(
                    self.services.as_ref(),
                    &state.session_id,
                    &state.turn_id,
                    state.iterations,
                    state.tool_results.len(),
                    state.started_at.elapsed().as_millis(),
                ),
                state.execution_role,
            );
            let forced_terminal_presentation_ready = forced_terminal_presentation_allowed(
                &turn_framing,
                state.execution_role.is_delegated_leaf()
                    && delegated_protocol_at_step.as_ref().is_none_or(DelegatedAgenticProtocolState::is_terminal)
                    && focus_acceptance_is_met(
                    state.bounded_evidence_role, &state.focus_acceptance_scopes,
                    &state.focus_acceptance_pending_scopes,
                ),
            );
            let refusal_text_only = crate::repetition_guard::take_refusal_ignored(
                &self.services.event_store().repetition_scope(&state.session_id, &state.turn_id));
            let made_progress = std::mem::take(&mut state.last_verified_progress);
            state.consecutive_unverified_model_steps = next_unverified_model_step_streak(
                state.consecutive_unverified_model_steps,
                made_progress,
            );
            let mut clean_terminal_synthesis =
                std::mem::take(&mut state.clean_terminal_synthesis_next)
                    && !action_channel_required && forced_terminal_presentation_ready;
            if !pending_disposition_inputs.is_empty() {
                // A terminal candidate produced for the pre-update topology is
                // stale. Discard it and let the post-disposition graph decide
                // whether a fresh synthesis is still appropriate.
                clean_terminal_synthesis = false;
                state.clean_terminal_synthesis_attempted = false;
            }
            if !action_channel_required
                && forced_terminal_presentation_ready
                && !clean_terminal_synthesis
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
                    made_progress,
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
            // Guidance written for a next model step can outlive the step it
            // was written for: a fuse intervention ends the turn right here, so
            // anything drained below is delivered to nobody. Keep the identities
            // so the durable intervention can record what Runtime wanted to say
            // and did not, instead of letting the words evaporate.
            let mut drained_guidance = std::mem::take(&mut state.pending_next_model_context);
            let undelivered_guidance_refs = drained_guidance
                .iter()
                .map(|item| item.id.clone())
                .collect::<Vec<_>>();
            // The turn-position packet is re-rendered on every request by
            // design: `context_plane` routes a `GOAL_STATE_PACKET_PREFIX` item to
            // the volatile tail, so it never forks the stable prefix. But it was
            // only ever pushed on the re-drive path, so a model working normally
            // never learned where it stood and had to ask with `state_inspect`
            // (one live run issued 153 of them). Push it on every step; the
            // routing above already makes it per-request.
            let mut goal_state = ContextItem::new(
                format!("goal-state:{}", ticket.node_id),
                ContextSourceKind::Task,
                ContextRole::Instruction,
                format!(
                    "#GoalState {}",
                    serde_json::to_string(&turn_framing).unwrap_or_else(|_| "{}".to_string())
                ),
            );
            goal_state.authority = ContextAuthority::System;
            goal_state.visibility = ContextVisibility::Private;
            // The instances this execution may cite as evidence. Every contract
            // that needs one says where to fetch it and none says what it
            // currently is, so a model that has lost the thread cites a ref from
            // an earlier session or a round it no longer holds - one live run was
            // refused with "no canonical durable receipt in Session ..." for
            // exactly that. These are the durable receipts this turn actually
            // produced, so citing one can never be refused for identity.
            let held = durable_receipt_refs(&state.tool_results);
            if !held.is_empty() {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(
                    goal_state.content.trim_start_matches("#GoalState "),
                ) {
                    let mut value = value;
                    if let Some(object) = value.as_object_mut() {
                        object.insert("held_receipts".to_string(), serde_json::json!(held));
                    }
                    goal_state.content =
                        format!("#GoalState {}", serde_json::to_string(&value).unwrap_or_default());
                }
            }
            // The re-drive path pushes the same id in the same step; keep one.
            drained_guidance.retain(|item| item.id != goal_state.id);
            drained_guidance.push(goal_state);
            let pending_next_model_context = model_context_for_step(
                drained_guidance,
                &state.persistent_collaboration_context,
            );
            (
                state.content.clone(),
                first,
                intervention,
                // The override applies to exactly one Provider request. A
                // recovery may schedule another explicit override below, but
                // stale state must never disable tools for later turns.
                // A refusal the model answered by issuing the same call again
                // also forces this step to be text-only: the model must
                // conclude instead of paying for another refused round.
                effective_text_only_request(std::mem::take(&mut state.force_text_only_next_model),
                    forced_terminal_presentation_ready, refusal_text_only, action_channel_required),
                forced_terminal_presentation_ready,
                state.force_tool_allowlist_next_model.take(),
                state.force_reasoning_effort_next_model.take(),
                clean_terminal_synthesis,
                clean_terminal_evidence,
                state.tool_receipts_observed > 0,
                pending_next_model_context,
                undelivered_guidance_refs,
                pending_disposition_inputs,
            )
        };
        if let Some((mut intervention, observation)) = fuse_intervention {
            record_undelivered_guidance(&mut intervention, &undelivered_guidance_refs);
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
        let (user_goal_id,user_ingress,user_ids,owns_user_goal)={
            let state=self.state.lock().await;
            (state.goal_id.clone(),state.ingress.clone(),
                state.pending_disposition_inputs.iter().map(|r|r.envelope.input_id.to_string()).collect::<Vec<_>>(),state.execution_role.owns_root_presentation())
        };
        let user_goal_source=self.services.goal_store().projection(&user_goal_id)
            .map_err(|reason|NodeExecutorError::Poll{node_id:ticket.node_id.clone(),reason})?
            .ok_or_else(||NodeExecutorError::Poll{node_id:ticket.node_id.clone(),reason:"user Goal source missing".into()})?;
        let user_frame = if owns_user_goal {
            crate::turn_inbox::authenticated_goal_inputs(
                self.services.as_ref(), &user_goal_source, user_ingress.as_ref(), &user_ids,
            ).await.map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?
        } else { None };
        // These cuts belong to amendment preparation, not ordinary execution.
        let user_source_cuts = if user_frame.is_some() {
            let graph_revision = self.services.graph_state_store().load(&ticket.graph_id)
                .map_err(|e| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: e.to_string() })?.revision;
            let program_cut = if let Some(binding) = user_goal_source.goal.execution_binding.as_ref() {
                self.services.agent_action_service().project_if_exists(&binding.agentic_program_id)
                    .map_err(|e| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: e.to_string() })?
                    .map(|p| (p.program_id, p.revision))
            } else { None };
            Some((graph_revision, program_cut))
        } else { None };
        let user_framing=user_frame.as_ref().map(|frame|serde_json::json!({
            "current_goal":user_goal_source.goal,
            "inputs":frame.inputs.iter().enumerate().map(|(slot,(_,_,text))|serde_json::json!({"input_slot":slot,"text":text})).collect::<Vec<_>>()
        }).to_string());
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
                let count = current_turn_required_team_execution_count(
                    &user_goal_source.goal, state.collaboration_obligation.as_ref(),
                    false, state.evaluation_judge_only,
                );
                (count > 0).then(|| (u8::try_from(count).unwrap_or(u8::MAX), required_workspace_write_scopes.clone()))
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
                    "This Objective requires at least {required_team_count} real Team execution(s). You retain semantic control: decide the Teams, roles, Tasks, dependencies, discussion and replanning. Use team_create, agent_invite, task_publish and the other small Agent actions; Runtime binds identities, leases and execution. When failed or challenged work must be replaced or split, first publish its concrete successor Task(s), then use task_supersede with durable evidence; retirement never counts as accepted work. Write plans and long results as normal content and commit references—never submit a complete graph or long JSON.{scope_note} Request completion only after committed artifacts, independent review and all required Teams are complete. {task_authoring}",
                    task_authoring = crate::agentic::order_contract::TASK_AUTHORING_DISCIPLINE,
                ),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            item.evidence = vec![format!("turn:{turn_id}")];
            runtime.push_next_model_context_item(item);
        }
        let request_tool_snapshot = runtime.api_client_mut().step_tool_definitions()
            .map(|definitions| definitions.into_iter().map(|definition| definition.name).collect::<BTreeSet<_>>());
        let request_framing = pending_next_model_context.iter()
            .find(|item| item.id.starts_with("goal-state:"))
            .and_then(|item| item.content.strip_prefix("#GoalState "))
            .and_then(|content| serde_json::from_str::<serde_json::Value>(content).ok());
        for item in pending_next_model_context {
            runtime.push_next_model_context_item(item);
        }
        if let Some(bus) = runtime.cowd_bus().cloned() {
            bus.emit(CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::CallingModel,
                detail: Some("requesting model".to_string()),
            });
        }
        // Taken once, before either consumer, and read by both: the model-step
        // boundary below uses it to demand a tool call, and the terminal-request
        // builder further down uses it to keep that request from being tool-less.
        // Declaring it inside the boundary's block would leave the builder unable
        // to see it, and taking it twice would leave the second reader with
        // `false` - either way the escalation is silently dropped.
        let force_tool_call = {
            let mut state = self.state.lock().await;
            std::mem::take(&mut state.force_tool_call_next_model)
        };
        if matches!(
            control_plane_priority,
            NextModelControlPlanePriority::ExistingConstraint
        ) && !clean_terminal_synthesis
        {
            // Hard invariant: while the framework still owes the model a
            // mandatory action, it may not withdraw the tools that perform it.
            //
            // Run one19 broke exactly here. Its root Team completed, which set
            // `force_text_only_next_model` on the strength of "a verified
            // collaboration terminal leaves only presentation" - and at that
            // same moment a `issue_requires_classification` gap opened that only
            // `message_publish` can close. The next six provider requests went
            // out with `tool_array_bytes == 0`; the model stated in its own
            // final answer that the executable tool set was empty and that it
            // could not classify the issue. Asking for an action and removing
            // the only means of taking it is a framework defect, not a model
            // failure, and it is fenced here rather than at each of the five
            // sites that set the flag.
            // The obligation is read from the Program projection as well as from
            // the two continuation flags. The flags only exist once a gap has been
            // *detected*, which is late: run one25 was sent a text-only step while
            // three Teams existed and no Task had been published, so both flags
            // were false, the invariant read "nothing owed", and the request went
            // out with an empty `tools` array. Reading the projection names
            // `publish_task` the moment the Teams exist.
            let mandatory_action_outstanding = required_effects_pending || request_framing.as_ref().is_some_and(|framing| {
                framing.get("execution_role").and_then(serde_json::Value::as_str) != Some("DelegatedLeaf")
                    && framing.get("blocked").is_none_or(serde_json::Value::is_null)
                    && framing["remaining"].as_array().is_some_and(|entries| !entries.is_empty())
            });
            let (state_iterations, state_completion_gap_outstanding, state_owed_rungs) = {
                let state = self.state.lock().await;
                (
                    state.iterations,
                    state.completion_gap_outstanding,
                    state.outstanding_closure_rungs.clone(),
                )
            };
            // Only the persistent route is projected here.
            //
            // The one-shot `require_next_model_final_response` is a deliberate single
            // request and needs no projection. `force_text_only_next_model` survives
            // across rounds, and that is the shape that disarms a model: one live run
            // spent six consecutive rounds with a zero-byte tool array while every
            // outer metric stayed healthy. A monitor cannot tell a deliberate terminal
            // synthesis from a framework that disarmed the model by looking at one
            // request, but it can count a run of them with no progress between.
            if force_text_only_response && !mandatory_action_outstanding && !force_tool_call {
                // Project the decision, not just take it.
                //
                // One live run spent six consecutive rounds with a zero-byte tool
                // array and told the model six times that it had no tools, while every
                // outer metric - cursor, revision, team completed - stayed healthy. The
                // evaluator could not see it because the state that caused it lives in
                // this process, and nothing wrote it anywhere a monitor could read.
                //
                // `tool_array_bytes` was measurable only after the fact, from provider
                // attempts. This is the same fact at the moment Runtime decides it,
                // which is the only point at which anything could still act.
                runtime.append_execution_runtime_event(
                    crate::RuntimeEventScope::Tool,
                    "model.request.text_only",
                    Some("text_only".to_string()),
                    vec![crate::RuntimeEventRef {
                        kind: "execution_node".to_string(),
                        id: ticket.node_id.clone(),
                    }],
                    serde_json::json!({
                        "graph_id": ticket.graph_id,
                        "iteration": state_iterations,
                        "reason": "text-only checkpoint with no owed action",
                        "completion_gap_outstanding": state_completion_gap_outstanding,
                        "owed_rungs": state_owed_rungs,
                        "note": "this request exposes no tool schema; a run that repeats it is a model the framework has disarmed"
                    }),
                );
                runtime.require_next_model_final_response();
            } else if let Some(tool_ids) = force_tool_allowlist {
                runtime.require_next_model_tool_action(tool_ids);
            } else if force_tool_call {
                // The model concluded again instead of acting. Require a tool call
                // and leave the tool set alone.
                runtime.require_next_model_tool_call();
            } else if mandatory_action_outstanding {
                let framing = request_framing.clone().unwrap_or_else(|| serde_json::json!({
                    "blocked":{"reason":"request_goal_state_unavailable"},"remaining":[]
                }));
                let guidance = owed_action_context(&framing, request_tool_snapshot.as_ref());
                let mut item = crate::ContextItem::new(
                    format!("owed-action:{}", ticket.node_id),
                    crate::ContextSourceKind::Task,
                    crate::ContextRole::TaskState,
                    guidance.to_string(),
                );
                item.authority = crate::ContextAuthority::Tool;
                item.visibility = crate::ContextVisibility::Private;
                item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                runtime.push_next_model_context_item(item);
            }
            if let Some(effort) = force_reasoning_effort {
                runtime.require_next_model_reasoning_effort(effort);
            }
        }
        let transcript_len = runtime.session_head().await.message_count;
        let root = self.state.lock().await.execution_role == TurnExecutionRole::RootTurn;
        let mut step_runtime = runtime.scoped_model_stop(self.stop.clone()).map_err(|reason| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(), reason,
        })?;
        let mut model_runtime = step_runtime.scoped_root_node_activity(ticket, root)
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(), reason: error.to_string(),
            })?;
        let original_provider_owner = if root {
            model_runtime.root_effect_owner_for_ticket(ticket).ok().map(|(activity, attempt)|
                crate::provider_runtime_client::ProviderGraphOwner { activity, attempt })
        } else { None };
        // Early dispatch cannot consume the ToolBatch's original evaluation
        // scope fact. Keep scoped calls on the normal per-call admission path.
        let evaluation_scopes = self.state.lock().await.evaluation_resource_scopes.clone();
        let early_dispatcher: Option<Arc<dyn crate::conversation::EarlyToolDispatcher>> =
            if !evaluation_scope_allows_early_dispatch(
                clean_terminal_synthesis, force_text_only_response, &evaluation_scopes,
            ) {
                None
            } else {
                model_runtime.active_turn_strategy().and_then(|strategy| {
                    self.services.tool_execution_host().map(|_| {
                        Arc::new(HostEarlyToolDispatcher {
                            tool_executor: Arc::clone(model_runtime.tool_executor()),
                            services: Arc::clone(&self.services),
                            event_bus: model_runtime.cowd_bus().cloned(),
                            ticket: ticket.clone(),
                            session_id: early_session_id.clone(),
                            memory_context: model_runtime.memory_turn_context(),
                            reality_context: model_runtime.reality_data_lease(),
                            model_lease: early_model_lease.clone(),
                            observation_wave_sequence,
                            decision: strategy.decision,
                            permission_policy: model_runtime.permission_policy().clone(),
                            authorization_negotiator: model_runtime.authorization_negotiator(),
                            timeout: model_runtime
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
        let model_goal_id = self.state.lock().await.goal_id.clone();
        let model_goal_source = self.services.goal_store().projection(&model_goal_id)
            .map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?
            .ok_or_else(|| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: "model Goal source missing".into() })?;
        if write_goal_source.stream_revision != model_goal_source.stream_revision {
            return Err(NodeExecutorError::Poll {node_id:ticket.node_id.clone(),reason:"Goal changed before Provider request".into()});
        }
        let interpretation = match (&user_frame, &user_framing) {
            (Some(frame), Some(framing)) => Some((frame.inputs.len(), framing.as_str())),
            _ => None,
        };
        let result = if interpretation.is_none() && clean_terminal_synthesis {
            model_runtime
                .execute_clean_terminal_synthesis(
                    &content,
                    clean_terminal_evidence.as_ref().map(|(digest, _)| digest.as_str()).unwrap_or_default(),
                    clean_terminal_evidence.as_ref().map(|(_, history)| history.clone()).unwrap_or_default(),
                )
                .await
        } else {
            let (dispatcher, forbids) = if interpretation.is_some() {
                (None, false)
            } else {
                (early_dispatcher, operator_forbids_tool_use)
            };
            let mut result = model_runtime.execute_model_step_for_purpose(
                &content, first_step, dispatcher, provider_retry_fenced, forbids, interpretation,
            ).await;
            if let (Ok(step), Some((slots, _))) = (&mut result, interpretation) {
                crate::ConversationRuntime::<C, T>::finish_input_interpretation(step, slots);
            }
            result
        };
        let result = result.and_then(crate::conversation::ModelStepResult::into_host_result);
        drop(model_runtime);
        drop(step_runtime);
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
        // Construct the remaining result phase only after the original model
        // scoped guards are dropped and transcript/input cleanup has run.
        construct_result_phase(|| async {
        if result.as_ref().ok().is_some_and(|step|step.input_interpretation.is_some()) {
            let step=result.map_err(|e|NodeExecutorError::Poll{node_id:ticket.node_id.clone(),reason:e.to_string()})?;
            let (user_graph_revision, user_program_cut) = user_source_cuts.ok_or_else(||
                NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: "interpretation has no frozen source cuts".into() })?;
            return self.accept_user_input_interpretation(ticket,user_goal_source,
                user_frame.ok_or_else(||NodeExecutorError::Poll{node_id:ticket.node_id.clone(),reason:"interpretation has no authenticated frame".into()})?,
                user_graph_revision,user_program_cut,step,&consumed_inputs).await;
        }
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
                    provider_usage_observed: Some(step.assistant_message.usage.is_some()),
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
                let input_update_pending = !pending_disposition_inputs.is_empty();
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
                if input_update_pending {
                    // The provider candidate did not observe these durable inputs.
                    // Preserve usage telemetry, but force a fresh semantic model step
                    // with new data context. Only authenticated original inputs
                    // enter the dedicated business-interpretation checkpoint;
                    // context absorption cannot alter the current Goal contract.
                    state.assistant_messages.pop();
                    state.pending_transcript.remove(&ticket.node_id);
                    state.clean_terminal_synthesis_next = false;
                    state.clean_terminal_synthesis_attempted = false;
                    state.last_verified_progress = true;
                    // Keep the labelled originals pending until the original
                    // application transaction and receipt adopt them. Context
                    // absorption is not a second business application owner.
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
                            "consumed {} session input update(s); awaiting_original_application={input_update_pending}",
                            consumed_inputs.len(),
                        ),
                        format!("session-input:{}", sha256_digest(&evidence_refs.join("\n"))),
                        ObservationResultClass::Informational,
                    );
                    observation.evidence_refs.clone_from(&evidence_refs);
                    if input_update_pending {
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
                let mut intent = if late_inputs || input_update_pending {
                    ModelStepIntent::Replan {
                        reason:
                            "new Session input awaits original authenticated application; continue from the newer durable input cursor"
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
                if step.text_only_response {
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
                        ModelStepIntent::Replan { .. } if !input_update_pending => {
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
                    if late_inputs || input_update_pending || !agentic_content_bridge_active {
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
                let mut write_recovery = None;
                let mut write_goal_cut = None;
                let mut next_model_context = None;
                // All Goal observations and the final write checkpoint in this
                // outcome must share one durable source cut.  The model was
                // dispatched against `model_goal_source`; the checkpoint below
                // may only use the newer cut when the Goal itself is unchanged.
                let observation_source = self.services.goal_store().projection(observation.goal_id())
                    .map_err(|reason| NodeExecutorError::Poll { node_id:ticket.node_id.clone(),reason })?
                    .ok_or_else(|| NodeExecutorError::Poll { node_id:ticket.node_id.clone(),reason:"observation Goal source missing".into() })?;
                let next = match intent {
                    ModelStepIntent::FinalAnswer { text } => 'final_answer: {
                        let mut text = strip_trailing_simulated_tool_markup(text);
                        // The root has chosen to answer, so the verified-answer
                        // drive must not pre-empt it on a later step.
                        state.final_answer_observed = true;
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
                            current_turn_required_team_execution_count(
                                &user_goal_source.goal,
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
                            let progress_sources = stable_collaboration_sources(self.services.as_ref(), &state.session_id, &state.turn_id);
                            let progress_known = progress_sources.as_ref().is_some_and(|(program, _)| {
                                program.status != crate::AgenticProgramStatus::Blocked
                                    && agentic_program.as_ref().is_some_and(|before| before.revision == program.revision)
                            });
                            let verified_progress = if let Some((program, goal)) = progress_sources.as_ref().filter(|_| progress_known) {
                                state.verified_collaboration_progress.observe(
                                    Some(program), 0, goal.observations.iter()
                                        .filter(|observation| observation.has_verified_gain())
                                        .flat_map(|observation| observation.information_gain.distinguishing_evidence_refs.iter().cloned())
                                        .filter(|reference| !reference.starts_with("verified_collaboration:"))
                                        .chain(goal.progress.criteria.iter().filter_map(|(id, status)|
                                            matches!(status, AcceptanceStatus::Satisfied | AcceptanceStatus::Waived)
                                                .then(|| format!("criterion:{id}:{status:?}"))))
                                        .chain(goal.goal.obligations.iter().filter_map(|obligation|
                                            matches!(obligation.state, harness_contract::goal::ObjectiveObligationState::Satisfied | harness_contract::goal::ObjectiveObligationState::Waived)
                                                .then(|| format!("obligation:{}:{:?}", obligation.obligation_id, obligation.state)))),
                                )
                            } else { false };
                            state.collaboration_convergence_replans = next_collaboration_redrives(
                                state.collaboration_convergence_replans, verified_progress, progress_known,
                            );
                            let convergence_bound = collaboration_convergence_replan_bound(
                                state.safety_lease.complexity,
                            );
                            if progress_known && state.collaboration_convergence_replans > convergence_bound {
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
                            let framing = crate::host::scope_turn_framing_for_role(
                                crate::host::agentic_turn_framing(
                                    self.services.as_ref(),
                                    &state.session_id,
                                    &state.turn_id,
                                    state.iterations,
                                    state.tool_results.len(),
                                    state.started_at.elapsed().as_millis(),
                                ),
                                state.execution_role,
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
                                        < required_team_executions
                                    {
                                        outstanding.push("team".to_string());
                                    }
                                    if published == 0 && required_team_executions > 0 {
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
                        let task_focus = state.execution_role.is_delegated_leaf().then_some((
                            required_workspace_write_scopes.as_slice(),
                            state.committed_workspace_observed_evidence.as_slice(),
                            state.committed_workspace_write_observed,
                        ));
                        if model_goal_source.goal != observation_source.goal {
                            return Err(NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason: "final model answer saw a changed Goal".into(),
                            });
                        }
                        let (write_decision, cut) = self.services.goal_store().assess_write_checkpoint(
                            &observation_source, ticket, self.services.path_identity_resolver(),
                            GoalWriteCheckpoint::FinalAnswer, task_focus, &[],
                        ).map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?;
                        write_goal_cut = Some(cut);
                        let blocked = match write_decision {
                            GoalWriteDisposition::Recover(recovery) => { write_recovery = Some(recovery); None }
                            GoalWriteDisposition::Blocked(intervention) => Some(intervention),
                            GoalWriteDisposition::Satisfied | GoalWriteDisposition::Pending => None,
                        };
                        // Business exhaustion is decided by Goal. A presentation
                        // language correction cannot turn it into another write retry.
                        if let Some(intervention) = blocked {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            model_intervention = Some(intervention);
                            let mut node = dynamic_node(ticket, state.iterations,
                                "goal-write-gap-synthesize", ExecutionNodeKind::Synthesize,
                                crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                                "inline_model");
                            node.executor_kind = crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
                            break 'final_answer vec![node];
                        }
                        let recover_write = write_recovery.is_some();
                        let recover_language = should_recover_response_language(
                            response_language_mismatch_for_role(&state.content, &text,
                                state.execution_role.is_delegated_leaf()),
                            state.root_language_replan_attempted,
                        );
                        if recover_write || recover_language {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            let mut missing = Vec::new();
                            if recover_write {
                                if required_workspace_write_scopes.is_empty() {
                                    missing
                                        .push("a committed workspace artifact write".to_string());
                                } else {
                                    missing.push(format!(
                                        "a committed workspace artifact write to the exact target(s) [{}]",
                                        required_workspace_write_scopes.join(", ")
                                    ));
                                }
                                state.force_tool_allowlist_next_model = None;
                            }
                            if recover_language {
                                state.root_language_replan_attempted = true;
                                missing.push("a final response in the user's language".to_string());
                                // Never while the Program still owes work.
                                //
                                // A text-only request tells the model the executable tool
                                // set is empty. One live run received that while
                                // `publish_task` was still outstanding, read "the tool
                                // set is empty" as "the user prohibited tool calls", and
                                // wrote four consecutive blocked declarations saying so -
                                // handed a language correction and a work obligation in
                                // the same breath, with no way to satisfy both.
                                //
                                // The model-step boundary already refuses to force a
                                // text-only response while a mandatory action is
                                // outstanding; this path armed the flag that narrows the
                                // tool exposure without asking that question at all.
                                let owed_work = !state.outstanding_closure_rungs.is_empty();
                                if !recover_write && !owed_work && forced_terminal_presentation_ready {
                                    state.force_text_only_next_model = true;
                                    tracing::warn!(
                                        target: "cowd::text_only_step",
                                        site = "root_goal_acceptance_or_language_recovery",
                                        completion_gap_outstanding = state.completion_gap_outstanding,
                                        outstanding_closure_rungs = ?state.outstanding_closure_rungs,
                                        "a text-only model step was armed; the invariant at the model-step boundary keeps the closing tool available while a mandatory action is outstanding"
                                    );
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
                                     Some(self.services.as_ref()), true, )?)
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
                        } else if let Some(reason) = (!state.evaluation_judge_only && !root_requires_action_channel && forced_terminal_presentation_ready)
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
                                state.force_text_only_next_model = true;
                                tracing::warn!(
                                    target: "cowd::text_only_step",
                                    site = "terminal_presentation_recovery",
                                    completion_gap_outstanding = state.completion_gap_outstanding,
                                    outstanding_closure_rungs = ?state.outstanding_closure_rungs,
                                    "a text-only model step was armed; the invariant at the model-step boundary keeps the closing tool available while a mandatory action is outstanding"
                                );
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
                        if let Some(pending_writes) = pending_focus_write_action {
                            state.assistant_messages.pop();
                            state.pending_transcript.remove(&ticket.node_id);
                            let (intervention, next) =
                                focus_action_rejection_outcome(ticket, &mut state, &pending_writes);
                            model_intervention = Some(intervention);
                            next
                        } else {
                            tool_nodes_for_calls(
                                ticket,
                                state.iterations,
                                &state.session_id,
                                calls,
                                self.services.workspace_root(),
                             Some(self.services.as_ref()), false, )?
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
                            .observation_event_at(
                                &observation_source,
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
                            .observation_event_at(
                                &observation_source,
                                input_observation,
                                format!("{}:input-observation", ticket.idempotency_key),
                            )
                            .map_err(|reason| NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason,
                            })?,
                    );
                }
                // The candidate is metadata on this attempt's original Goal
                // observation, not a second append of the same observation.
                let goal_observation_event = if let Some(encoded) = outcome
                    .result
                    .result_ref
                    .as_deref()
                    .and_then(|value| value.strip_prefix("assistant_json:"))
                {
                    if model_goal_source.goal.revision != observation_source.goal.revision
                        || model_goal_source.goal.spec_digest != observation_source.goal.spec_digest
                    {
                        return Err(NodeExecutorError::Poll {
                            node_id: ticket.node_id.clone(),
                            reason: "final model candidate saw a stale Goal revision".into(),
                        });
                    }
                    let answer: String = serde_json::from_str(encoded).map_err(|error| {
                        NodeExecutorError::Poll {
                            node_id: ticket.node_id.clone(),
                            reason: error.to_string(),
                        }
                    })?;
                    self.services.goal_store().turn_candidate_event_at(
                        &observation_source,
                        &observation,
                        ticket,
                        &answer,
                    )
                } else {
                    self.services.goal_store().observation_event_at(
                        &observation_source,
                        &observation,
                        format!("{}:goal-observation", ticket.idempotency_key),
                    )
                }
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
                outcome.domain_events.push(goal_observation_event);
                for (suffix, observation) in [
                    ("strategy-observation", &strategy_observation),
                    ("provider-observation", &provider_observation),
                    ("context-observation", &context_observation),
                ] {
                    outcome.domain_events.push(
                        self.services
                            .goal_store()
                            .observation_event_at(
                                &observation_source,
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
                if let Some(cut) = write_goal_cut {
                    outcome.source_revisions.insert(cut.stream_id, cut.expected_revision);
                }
                if let Some(recovery) = write_recovery {
                    outcome.source_revisions.insert(recovery.stream_id, recovery.expected_stream_revision);
                    outcome.domain_events.push(recovery.event);
                }
                Ok(outcome)
            }
            Err(error) => {
                if !error.early_tool_failures().is_empty() {
                    // Preserve completed siblings, but never turn an unknown
                    // early authorization commit into a fresh batch admission.
                    let mut state = self.state.lock().await;
                    for receipt in error.effect_receipts() {
                        if !state.early_tool_receipts.contains_key(&receipt.call.id) {
                            state.early_tool_receipts.insert(receipt.call.id.clone(), receipt.clone());
                            state.tool_receipts_observed = state.tool_receipts_observed.saturating_add(1);
                        }
                    }
                    drop(state);
                    *self.local_fence.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(error.clone());
                    return Err(NodeExecutorError::OriginalCustodyUnknown {
                        node_id: ticket.node_id.clone(),
                        reason: error.to_string(),
                    });
                }
                if let (Some(proof), Some(owner)) = (error.authenticated_provider_cancellation(), original_provider_owner.as_ref()) {
                    if error.effect_receipts().is_empty() && proof.matches_original(owner, ticket) {
                        return Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                            status: ExecutionNodeStatus::Cancelled,
                            result_ref: Some(proof.durable_reference()), wait_class: None,
                            summary: Some("original local provider response observed authorized cancellation".into()),
                            evidence_refs: Vec::new(),
                            failure: None, usage: ExecutionUsage::default(), finished_at_ms: user_application_clock(),
                        }));
                    }
                }
                if let Some(fence) = error.session_execution_fence_error() {
                    *self.local_fence.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(error.clone());
                    // A fence refusal is a durable, named fact about this
                    // request's ownership, not an unknown custody.  The node
                    // closes on that fact: a mid-air release is retryable,
                    // because the input went back to the queue after this
                    // request was admitted, and an owner's terminal
                    // withdrawal is a typed terminal failure.  Neither leaves
                    // the original callback custody Unknown.
                    let retryable = fence.is_retryable();
                    return Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                        status: ExecutionNodeStatus::Failed,
                        result_ref: None,
                        wait_class: None,
                        summary: None,
                        evidence_refs: Vec::new(),
                        failure: Some(harness_contract::execution_graph::ExecutionFailure {
                            kind: "session_execution_fence".into(),
                            message: error.to_string(),
                            retryable,
                            evidence_refs: Vec::new(),
                        }),
                        usage: ExecutionUsage::default(),
                        finished_at_ms: user_application_clock(),
                    }));
                }
                // A provider failure is execution evidence, not an implicit
                // graph terminal. Preserve it and let the same Goal policy
                // that governs tools decide whether the next node retries,
                // changes strategy, or produces an honest blocked result.
                let protocol_failure = error.is_provider_tool_protocol_failure();
                let repeated_frame_refusal = error.is_repeated_tool_frame_refusal();
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
                #[cfg(test)]
                if let Some(path) = std::env::var_os("COWD_R638_ERROR") {
                    use std::io::Write as _;
                    use std::os::unix::fs::OpenOptionsExt as _;
                    if let Ok(mut file) = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path) {
                        let record = serde_json::json!({"node":ticket.node_id,"attempt":ticket.attempt,"protocol_failure":protocol_failure,"tool_exposure_miss":tool_exposure_miss,"route_unavailable":terminal_provider_route_failure,"reason":reason.chars().take(4096).collect::<String>()});
                        let saved = serde_json::to_writer(&mut file, &record).is_ok() && file.flush().is_ok() && file.sync_all().is_ok();
                        eprintln!("r622_toolbatch phase=provider_first_error_saved ok={} node={} attempt={} protocol_failure={} tool_exposure_miss={} route_unavailable={}", saved, ticket.node_id, ticket.attempt, protocol_failure, tool_exposure_miss, terminal_provider_route_failure);
                    }
                }
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
                    // Runtime already spent the governed repair the moment it
                    // answered the first identical frame in-step with the exact
                    // shape to send. Recording that here is what stops the
                    // terminal narration from calling the same provider once
                    // more merely to describe a defect the model was already
                    // told how to fix - the guard in `terminal_narrated_answer`
                    // keys on this counter and would otherwise see an unspent
                    // budget.
                    if repeated_frame_refusal {
                        state.provider_protocol_recovery_attempts = state
                            .provider_protocol_recovery_attempts
                            .max(PROVIDER_PROTOCOL_RECOVERY_BUDGET + 1);
                    }
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
                    // The budget belongs to the defect, not to the turn. A
                    // model that recovered from one mistake and then made a
                    // different one has not stopped learning, and a turn that
                    // runs for many minutes will hit more than one defect.
                    let frame_repair_attempt = provider_frame_repair_attempt(
                        repairable_tool_frame,
                        tool_exposure_miss,
                        post_receipt_failure,
                        terminal_provider_route_failure,
                        clean_terminal_synthesis,
                        state
                            .provider_frame_repair_attempts
                            .get(&frame_repair_defect_signature(&reason))
                            .copied()
                            .unwrap_or(0),
                    );
                    if frame_repair_attempt.is_some() {
                        *state
                            .provider_frame_repair_attempts
                            .entry(frame_repair_defect_signature(&reason))
                            .or_insert(0) += 1;
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
                let intervention = if repeated_frame_refusal {
                    // The most specific signal wins. Runtime already answered
                    // this exact frame in-step with the shape to send and the
                    // model re-sent it unchanged, so the governed single retry
                    // would buy one more identical frame. Stop here rather than
                    // paying a provider round-trip to be told the same thing a
                    // third time.
                    RuntimeIntervention {
                        goal_id: goal_id.clone(),
                        kind: RuntimeInterventionKind::Block,
                        reason: "provider re-sent a tool frame Runtime had already refused and answered with the exact shape to send; the frame was not executed and no further retry is granted".to_string(),
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                        expected_graph_revision: None,
                    }
                } else if terminal_provider_route_failure {
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
        }).await
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
        let (session_id, turn_id, messages, required_team_count) = {
            let state = self.state.lock().await;
            (
                state.session_id.clone(),
                state.turn_id.clone(),
                state
                    .pending_transcript
                    .get(&ticket.node_id)
                    .cloned()
                    .unwrap_or_default(),
                state.pending_root_control_plane_requirement,
            )
        };
        stage_host_transcript(
            &self.runtime,
            &self.services,
            ticket,
            session_id,
            turn_id,
            messages,
            required_team_count,
            &mut outcome,
        )
        .await?;
        Ok(outcome)
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let graph = self
            .services
            .graph_state_store()
            .load(&ticket.graph_id)
            .map_err(|e| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: e.to_string(),
            })?;
        if let Some(selector) = graph
            .node_results
            .get(&ticket.node_id)
            .and_then(|r| r.result_ref.as_deref())
            .and_then(|r| r.strip_prefix("user_input_application:"))
        {
            let artifact = self
                .services
                .artifact_store()
                .resolve(selector)
                .map_err(|e| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: e.to_string(),
                })?;
            if artifact.bytes > 2 * 1024 * 1024 {
                return Err(NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: "user application artifact exceeds read budget".into(),
                });
            }
            let session = self.state.lock().await.session_id.clone();
            let bytes = self
                .services
                .artifact_store()
                .read(&artifact, &format!("session:{session}"), None)
                .await
                .map_err(|e| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: e.to_string(),
                })?;
            let prepared: PreparedUserGoalApplication =
                serde_json::from_slice(&bytes).map_err(|e| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: e.to_string(),
                })?;
            finish_user_goal_application(self.services.as_ref(), &prepared)
                .await
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
        }
        tracing::debug!(node_id = %ticket.node_id, "publishing committed model transcript");
        let (session_id, turn_id, required_control_plane_team_count) =
            publish_host_transcript(&self.runtime, &self.services, ticket).await?;
        {
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
            state.pending_transcript.remove(&ticket.node_id);
            state.pending_root_control_plane_requirement = None;
        }
        if let Some(required_team_count) = required_control_plane_team_count {
            // `program_admitted` and `required_actions` used to be literals:
            // `false` and `[team_create, agent_invite, task_publish]` on every
            // event, forever. Run one19 emitted 19 of them - including the ones
            // written after `team_create` succeeded, six Agents were invited and
            // three Tasks were accepted - so the durable journal asserted the
            // Program was never admitted while the Program projection said it
            // was complete. A monitoring layer that trusts this event reads a
            // lie; a model that trusts it is told to redo finished work. Derive
            // both from the Program the Runtime actually holds.
            let program = super::root_agentic_program_projection(
                self.services.as_ref(),
                &session_id,
                &turn_id,
            );
            let (program_admitted, mut required_actions, closure_steps) =
                root_control_plane_requirement(program.as_ref(), required_team_count);
            // The event is a statement about what is still owed, so its status
            // has to say whether anything is.
            let status = if program_admitted && required_actions.is_empty() {
                "satisfied"
            } else {
                "waiting"
            };
            required_actions.sort_unstable();
            persist_host_control_plane_event(
                self.services.as_ref(),
                ticket,
                crate::RuntimeEventInput {
                    stream_id: format!("session:{session_id}"),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "runtime.control_plane.required".to_string(),
                    status: Some(status.to_string()),
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
                        "required_actions": required_actions,
                        "program_admitted": program_admitted,
                        "closure_steps": closure_steps,
                    }),
                },
            )
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("persist root control-plane requirement: {error}"),
            })?;
        }
        tracing::debug!(node_id = %ticket.node_id, "committed model transcript published");
        Ok(())
    }
}

/// What the root control-plane event must assert, derived from the Program the
/// Runtime actually holds.
///
/// Returns `(program_admitted, required_actions, closure_steps)`. `required_actions`
/// names only the admission actions that are genuinely still missing, and
/// `closure_steps` the Program-owned rungs still outstanding, so the event states
/// what is owed rather than replaying a fixed list. A caller with no Program at all
/// gets the original three actions, which is the only state in which they are true.
pub(super) fn root_control_plane_requirement(
    program: Option<&crate::AgenticProgramProjection>,
    required_team_count: u8,
) -> (bool, Vec<&'static str>, Vec<String>) {
    let Some(program) = program else {
        return (
            false,
            vec![
                harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                harness_contract::agent_action::AGENT_INVITE_TOOL_ID,
                harness_contract::agent_action::TASK_PUBLISH_TOOL_ID,
            ],
            Vec::new(),
        );
    };
    if program.teams.is_empty() {
        return (
            false,
            vec![
                harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                harness_contract::agent_action::AGENT_INVITE_TOOL_ID,
                harness_contract::agent_action::TASK_PUBLISH_TOOL_ID,
            ],
            Vec::new(),
        );
    }
    let mut actions = Vec::new();
    if program.teams.len() < usize::from(required_team_count) {
        actions.push(harness_contract::agent_action::TEAM_CREATE_TOOL_ID);
    }
    if program.agents.is_empty() {
        actions.push(harness_contract::agent_action::AGENT_INVITE_TOOL_ID);
    }
    if program.tasks.is_empty() {
        actions.push(harness_contract::agent_action::TASK_PUBLISH_TOOL_ID);
    }
    let steps = super::outstanding_closure_steps(program)
        .into_iter()
        .filter(|step| step != "objective_complete_request")
        .collect::<Vec<_>>();
    (true, actions, steps)
}

pub(super) fn root_program_requires_action_channel(
    role: TurnExecutionRole,
    program_status: Option<crate::AgenticProgramStatus>,
) -> bool {
    role.owns_root_presentation()
        && program_status.is_some_and(|status| {
            matches!(
                status,
                crate::AgenticProgramStatus::Open
                    | crate::AgenticProgramStatus::Waiting
                    | crate::AgenticProgramStatus::CompletionRequested
                    | crate::AgenticProgramStatus::Draining
            )
        })
}

/// The tool set that can close whatever mandatory action is still outstanding.
///
/// It is deliberately wider than one tool: closing an issue gap legitimately
/// needs the references read first, and a one-tool allowlist that the model
/// cannot satisfy without a read produces an invalid `required` tool choice
/// (`provider_plane` fails closed to an omitted choice in that case, which is
/// how the model ends up tool-less again).
pub(super) fn mandatory_action_tool_ids(
    completion_gap_outstanding: bool,
    outstanding_closure_rungs: &[String],
) -> BTreeSet<String> {
    let mut tools = BTreeSet::new();
    // Every rung the closure view can advertise has to name the tool that closes
    // it. `publish_task` was missing, which is the rung a fresh collaboration is
    // shortest of: run one23 was handed a tool-less request with three Teams and
    // nothing published, and the fix for that mapped `publish_task` to no tool at
    // all - so the invariant would have read "tools owed" and then handed back an
    // empty set, which `.filter` turned back into "nothing owed". A rung with no
    // tool is not a rung; it is a label.
    for tool in outstanding_closure_rungs.iter().map(String::as_str) {
        match tool {
            "team" => {
                tools.insert("team_create".to_string());
            }
            "publish_task" | "publish_integration_task" => {
                tools.insert("task_publish".to_string());
            }
            "claim_task" => {
                tools.insert("task_claim".to_string());
            }
            "independent_review" | "accepted_task" => {
                tools.insert("task_review".to_string());
            }
            "classify_issues" | "resolve_issue" => {
                tools.insert("message_publish".to_string());
            }
            "objective_review" => {
                tools.insert("objective_review".to_string());
            }
            "register_delivery" => {
                tools.insert("artifact_commit".to_string());
            }
            "objective_complete_request" => {
                tools.insert("objective_complete_request".to_string());
            }
            _ => {}
        }
    }
    // The reads are part of the closing set, not a standing allowance. A
    // terminal request with nothing left to close must stay genuinely
    // tool-less: that is the case where rendering from the receipts already in
    // hand is the whole job, and leaving two tools exposed would quietly turn
    // every clean terminal synthesis into a two-tool step. The unit test that
    // asserts the empty case is what caught this.
    if tools.is_empty() {
        return tools;
    }
    tools
}

pub(super) fn owed_action_context(
    framing: &serde_json::Value,
    advertised: Option<&BTreeSet<String>>,
) -> serde_json::Value {
    let rungs = framing["remaining"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry["actionability"] != "blocked"
                && entry["actionability"] != "requires_explicit_independent_actor"
        })
        .filter_map(|entry| entry["step"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    let advisory_routes = framing["remaining"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry["publication_route"].is_object() || entry["optional_route"].is_object()
        })
        .map(|entry| {
            serde_json::json!({
                "step":entry["step"],
                "publication_route":entry["publication_route"],
                "optional_route":entry["optional_route"],
            })
        })
        .collect::<Vec<_>>();
    let candidates = mandatory_action_tool_ids(false, &rungs);
    let candidate_tools = candidates
        .iter()
        .filter(|tool| advertised.is_some_and(|names| names.contains(*tool)))
        .cloned()
        .collect::<Vec<_>>();
    let reads = [
        "read_ref",
        "state_inspect",
        "read_file",
        "evidence_retrieve",
    ]
    .into_iter()
    .filter(|tool| advertised.is_some_and(|names| names.contains(*tool)))
    .collect::<Vec<_>>();
    serde_json::json!({
        "program_source":framing["program_source"],
        "goal":framing["goal"],
        "remaining":framing["remaining"],
        "blocked":framing["blocked"],
        "candidate_mutations":candidate_tools,
        "advisory_routes":advisory_routes,
        "prerequisite_reads":reads,
        "tool_inventory_status":if advertised.is_some() {"provider_snapshot_before_compilation"} else {"unknown"},
        "final_wire_inventory":"unknown_until_provider_compilation",
        "note":"Candidate mutations and prerequisite reads are distinct. A read does not close mutation debt. Advisory publication/reviewer routes describe existing optional authorized calls, not mandatory topology or callable-tool grants. An unadmitted independent review is not an immediate Root mutation; neither absence nor presence of a Team proves reviewer eligibility. Only native schemas in this actual request establish callable tools; the pre-compilation snapshot is not a final exposure grant. If a prerequisite, actor, grant or tool is unavailable, state the blocker, do not invent a result or force unrelated work.",
    })
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

/// The defect a refused frame names, normalized to what the model must change.
///
/// The repair budget is keyed on this, so it has to separate "the model has not
/// learned" from "the model erred again". Two live frames differed by a factor
/// of three in length - 1642 and 5511 characters - and were the same mistake:
/// every entry omitted `tool`. Keeping anything that varies between attempts,
/// such as length or the declared/received restatement, would make them look
/// like two different defects and hand out a fresh budget each time.
///
/// Everything is read out of Runtime's own refusal string, which the governed
/// tool contracts own, so the model cannot shape the signature.
pub(super) fn frame_repair_defect_signature(reason: &str) -> String {
    let tool = reason
        .split("exposed tool `")
        .nth(1)
        .and_then(|rest| rest.split('`').next())
        .unwrap_or("unknown")
        .to_string();
    // `tool `X` input violates its pinned schema: $.actions: value has the wrong
    // JSON type (declared type: array; ...)`. The pointer plus the defect phrase
    // is the whole claim; the parenthetical restates it and is dropped.
    let defect = reason
        .split("pinned schema: ")
        .nth(1)
        .map(|rest| rest.split('(').next().unwrap_or(rest).trim().to_string())
        .unwrap_or_else(|| reason.chars().take(160).collect());
    format!("{tool}|{defect}")
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

/// The budget belongs to the defect, not to the turn.
///
/// One live run recovered perfectly from its first malformed frame - the next
/// batch was a well-formed array - and was then killed by a second frame that
/// differed only in length. A per-turn counter had already spent its single
/// attempt, so a model that was demonstrably learning was stopped anyway.
#[test]
fn a_second_distinct_defect_earns_its_own_repair_attempt() {
    let first = frame_repair_defect_signature(
        "tool_protocol_violation: provider supplied invalid arguments for exposed tool \
         `agent_action_batch`: tool `agent_action_batch` input violates its pinned schema: \
         $.actions: value has the wrong JSON type (declared type: array; received: string)",
    );
    let second = frame_repair_defect_signature(
        "tool_protocol_violation: provider supplied invalid arguments for exposed tool \
         `agent_action_batch`: tool `agent_action_batch` input violates its pinned schema: \
         $.actions[0]: missing required field `tool`",
    );
    assert_ne!(
        first, second,
        "distinct defects must be distinct signatures"
    );
    // Both defects share the same live budget map. Spending one key must not
    // spend the other, and neither key may regain a repair after exhaustion.
    let mut attempts = std::collections::BTreeMap::new();
    for (signature, expected) in [
        (&first, Some(1)),
        (&second, Some(1)),
        (&first, None),
        (&second, None),
    ] {
        let attempt = provider_frame_repair_attempt(
            true,
            false,
            true,
            false,
            false,
            attempts.get(signature).copied().unwrap_or(0),
        );
        assert_eq!(attempt, expected, "repair budget for {signature}");
        if attempt.is_some() {
            *attempts.entry(signature.clone()).or_insert(0) += 1;
        }
    }
    assert_eq!(attempts.len(), 2);
}

/// Frames that differ only in length are the same mistake.
///
/// The two live frames were 1642 and 5511 characters and both omitted `tool` on
/// every entry. Anything that varies between attempts - length, the
/// declared/received restatement - must not reach the signature, or the budget
/// resets on every retry and stops meaning anything.
#[test]
fn frames_that_differ_only_in_length_share_one_signature() {
    let short = frame_repair_defect_signature(
        "tool_protocol_violation: provider supplied invalid arguments for exposed tool \
         `agent_action_batch`: tool `agent_action_batch` input violates its pinned schema: \
         $.actions: value has the wrong JSON type (declared type: array; received: string; \
         send a array value as raw JSON, not as a JSON-encoded string)",
    );
    let long = frame_repair_defect_signature(
        "tool_protocol_violation: provider supplied invalid arguments for exposed tool \
         `agent_action_batch`: tool `agent_action_batch` input violates its pinned schema: \
         $.actions: value has the wrong JSON type (declared type: array; received: string)",
    );
    assert_eq!(short, long, "{short} vs {long}");
}

/// A refusal that names no pinned schema still normalizes to something stable
/// rather than swallowing the whole reason, which would make every occurrence
/// look distinct.
#[test]
fn a_refusal_without_a_pinned_schema_still_normalizes() {
    let first = frame_repair_defect_signature("tool_protocol_violation: unparseable frame");
    let second = frame_repair_defect_signature("tool_protocol_violation: unparseable frame");
    assert_eq!(first, second);
    assert_ne!(
        first,
        frame_repair_defect_signature("tool_protocol_violation: a different defect")
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
fn is_loop_nudge_repeatable_tool(call: &ModelToolCall) -> bool {
    let lower = call.name.to_ascii_lowercase();
    if lower == "process" || lower.ends_with("_get_result") || lower.ends_with("_poll") {
        return true;
    }
    // `state_inspect(wait_for_workers=true)` is the documented wait: the model
    // parks on it until a worker moves, so an unchanged answer is its normal
    // contract and repeating it is not a loop. A point lookup of one entry is
    // a different thing entirely and must keep counting.
    lower == "state_inspect" && call_declares_wait_for_workers(call)
}

/// Count the non-wait calls requested in one batch and report repeated requests.
/// A count alone does not establish whether a result or its source changed.
pub(super) fn record_repeated_calls(
    turn_id: &crate::repetition_guard::RepetitionScope,
    calls: &[ModelToolCall],
    successful_call_ids: &std::collections::BTreeSet<String>,
) -> Vec<(String, usize)> {
    let mut repeated = Vec::new();
    let _ = successful_call_ids;
    for call in calls {
        // Every call the model emitted counts, whether Runtime dispatched it or
        // not. The repeat counter answers "did the model ask for the same thing
        // again", and that question is about the model's decision, not about
        // Runtime's dispatch.
        //
        // The first version counted only calls present in `successful_call_ids`.
        // Run one26 re-issued the same `agent_invite` sixteen times and every one
        // of them was refused before dispatch, so none was ever in that set, the
        // count never moved, and the block bound was unreachable - the guard was
        // blind to exactly the loop it was built for.
        //
        // A wait is not a repeat: parking on `wait_for_workers` until a worker
        // moves is the documented contract, and an unchanged answer is its normal
        // result. Same exemption the streak guard uses.
        if is_loop_nudge_repeatable_tool(call) {
            continue;
        }
        let input_hash = crate::tool_invocation::stable_hash(&call.input);
        if let Some(count) =
            crate::repetition_guard::record_repeated_call(turn_id, &call.name, &input_hash)
        {
            repeated.push((call.name.clone(), count));
        }
    }
    repeated
}

/// Whether this call's input asks Runtime to wait for workers.
fn call_declares_wait_for_workers(call: &ModelToolCall) -> bool {
    serde_json::from_str::<serde_json::Value>(&call.input)
        .ok()
        .and_then(|value| {
            value
                .get("wait_for_workers")
                .and_then(serde_json::Value::as_bool)
        })
        .unwrap_or(false)
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
/// Whether byte-identical duplicate result payloads keep re-entering context.
///
/// Default **on**: the durable evidence is untouched either way (bytes stay keyed
/// by `tool_call` id and `evidence_retrieve` returns them in full), so the only
/// thing an operator loses by stubbing is the ability to re-read a payload it
/// already has. Set `COWD_LOOP_STUB_RESULTS=0` to opt out.
pub(super) fn result_stub_disabled() -> bool {
    matches!(
        std::env::var("COWD_LOOP_STUB_RESULTS").ok().as_deref(),
        Some("0") | Some("false") | Some("FALSE")
    )
}

/// Payloads smaller than this are not worth stubbing: the stub itself plus the
/// lost locality would outweigh the savings. Error results are never stubbed.
const IDENTICAL_RESULT_STUB_MIN_CHARS: usize = 512;

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
        .unwrap_or(CLOSURE_DRIVE_AFTER_STEPS_DEFAULT)
}

/// How long to wait before the framework drives the completion request on the
/// root's behalf once the closure gate passes.
///
/// The gate - no outstanding rung, no Goal completion gap, no request already
/// pending - is itself the proof that the closure is ready, so waiting several
/// more root steps buys no confirmation and spends the window. One live run
/// satisfied the Objective review and then used its remaining steps closing
/// Tasks; the request that would have bound the verdict never came, and the
/// turn ended cancelled with the Goal still open.
///
/// The gate self-corrects against premature driving: publishing another Task
/// raises `published` above `accepted`, which makes `accepted_task` outstanding
/// again and closes the gate on the next step.
const CLOSURE_DRIVE_AFTER_STEPS_DEFAULT: usize = 1;

/// Monotonic, durable collaboration facts, kept separately from attempted work.
#[derive(Debug, Clone, Default)]
pub(super) struct VerifiedCollaborationProgress {
    teams: std::collections::BTreeSet<String>,
    accepted_tasks: std::collections::BTreeSet<String>,
    evidence: std::collections::BTreeSet<String>,
    objective_reviews: usize,
}

impl VerifiedCollaborationProgress {
    pub(super) fn observe(&mut self, program: Option<&crate::AgenticProgramProjection>, reviews: usize, evidence: impl IntoIterator<Item = String>) -> bool {
        let mut advanced = reviews > self.objective_reviews;
        self.objective_reviews = self.objective_reviews.max(reviews);
        for reference in evidence { advanced |= self.evidence.insert(reference); }
        if let Some(program) = program {
            for task in program.tasks.values().filter(|task| task.status == crate::AgenticTaskStatus::Accepted) {
                advanced |= self.teams.insert(task.team_id.clone());
                advanced |= self.accepted_tasks.insert(task.task_id.clone());
                for reference in task.artifact_refs.iter().chain(&task.evidence_refs) {
                    advanced |= self.evidence.insert(reference.clone());
                }
            }
        }
        advanced
    }
}

/// Compare both durable heads twice: matching revisions bound one overlapping
/// observation interval; a missing or changing source remains unknown.
pub(super) fn stable_collaboration_sources(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
) -> Option<(crate::AgenticProgramProjection, crate::execution_core::goal::GoalProjection)> {
    let program = root_agentic_program_projection(services, session_id, turn_id)?;
    let root = program.root_execution_id.as_ref()?;
    let goal_id = format!("goal:{root}");
    let goal = services.goal_store().projection(&goal_id).ok().flatten()?;
    let binding = goal.goal.execution_binding.as_ref()?;
    if binding.agentic_program_id != program.program_id || binding.session_id != session_id
        || binding.turn_id != turn_id || binding.root_execution_id != *root {
        return None;
    }
    let check_program = root_agentic_program_projection(services, session_id, turn_id)?;
    let check_goal = services.goal_store().projection(&goal_id).ok().flatten()?;
    if check_program.revision != program.revision || check_goal.stream_revision != goal.stream_revision {
        return None;
    }
    Some((program, goal))
}

fn next_collaboration_redrives(previous: usize, verified_progress: bool, redriven: bool) -> usize {
    if verified_progress { 0 } else if redriven { previous.saturating_add(1) } else { previous }
}

fn failed_agent_action_receipt(output: &str) -> Option<harness_contract::agent_action::AgentActionObservation> {
    let body = output.rsplit_once("Structured receipt: ").map_or(output, |(_, receipt)| receipt);
    serde_json::from_str(body.trim()).ok()
}

/// The Objective convergence milestones, separate from execution health.
///
/// These are the facts that mean convergence moved toward finishing. Every
/// other Program mutation - publishing, claiming, submitting, reviewing a Task -
/// is work, and a run can do that work forever without approaching the
/// Objective: one live run spent 304 steps submitting and re-reviewing Tasks
/// without ever requesting completion, and under the previous definition every
/// one of those steps counted as verified progress, so the fuse never fired.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ObjectiveMilestones {
    pub(super) convergence_started: bool,
    /// A completion request is durable - the model asked to finish.
    pub(super) completion_requested: bool,
    /// Runtime bound an Objective verdict, satisfied or blocked.
    pub(super) verdict_bound: bool,
    /// How many Objective reviews are recorded on the Goal.
    pub(super) objective_reviews: usize,
    /// How many closure rungs the root still has to climb. Fewer is closer.
    pub(super) outstanding_closure_rungs: usize,
}

/// The Objective milestones as they stand right now.
///
/// Read from the Program projection and the Goal's own review records, both of
/// which are durable facts, so two observations of the same run always agree. A
/// turn with no Program yet reports the zero milestones, which makes the first
/// real milestone register as progress.
fn objective_milestones(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
) -> ObjectiveMilestones {
    let Some(program) = super::root_agentic_program_projection(services, session_id, turn_id)
    else {
        return ObjectiveMilestones::default();
    };
    let goal_reviews = program
        .root_execution_id
        .as_deref()
        .and_then(|root| {
            services
                .goal_store()
                .projection(&format!("goal:{root}"))
                .ok()
                .flatten()
        })
        .map(|goal| goal.goal.reviews.len())
        .unwrap_or(0);
    ObjectiveMilestones {
        convergence_started: false,
        completion_requested: program.completion_request.is_some(),
        verdict_bound: program.objective_verdict.is_some(),
        objective_reviews: goal_reviews,
        outstanding_closure_rungs: super::outstanding_closure_steps(&program).len(),
    }
}

/// Whether the Objective moved closer between two observations.
///
/// Only convergence counts: a request landing, a verdict binding, a review
/// landing, or the closure ladder shrinking. Once the ladder is at its floor
/// and the model keeps churning Tasks, nothing here advances and the
/// no-progress fuse is free to fire - which is exactly the run the old
/// definition let through.
pub(super) fn objective_milestone_advanced(
    before: &ObjectiveMilestones,
    after: &ObjectiveMilestones,
) -> bool {
    (after.completion_requested && !before.completion_requested)
        || (after.verdict_bound && !before.verdict_bound)
        || after.objective_reviews > before.objective_reviews
        || after.outstanding_closure_rungs < before.outstanding_closure_rungs
}

fn execution_health_identity(
    evidence: &harness_contract::context::ObservedEvidence,
) -> Option<String> {
    use harness_contract::context::{
        EvidenceTargetIdentity, LocalNetworkTerminal, ToolExecutionStopReason,
        ToolExecutionTerminal, WorkspaceAccessMode, WorkspacePriorState,
    };
    let identity = match &evidence.target {
        EvidenceTargetIdentity::Workspace { scope } => {
            let digest = scope
                .path
                .observed_revision_or_digest
                .as_deref()
                .filter(|digest| !digest.is_empty())?;
            if scope.access_mode == WorkspaceAccessMode::Write
                && matches!(&evidence.workspace_prior_state,
                    Some(WorkspacePriorState::Existing { sha256 })
                        if sha256.trim_start_matches("sha256:") == digest.trim_start_matches("sha256:"))
            {
                return None;
            }
            serde_json::json!([
                "workspace",
                scope.path.workspace_id,
                scope.path.repository_id,
                scope.path.workspace_relative_path,
                scope.path.repository_relative_path,
                scope.path.object_kind,
                digest
            ])
        }
        EvidenceTargetIdentity::ToolExecution { execution } => {
            let ToolExecutionTerminal::Settled {
                exit_code: Some(0),
                signal: None,
                closure,
                stop_reason: ToolExecutionStopReason::CommandExit,
            } = &execution.terminal
            else {
                return None;
            };
            if !execution.capture_complete || !closure.all_children_reaped || !closure.init_reaped {
                return None;
            }
            let inputs = execution
                .admission
                .inputs
                .iter()
                .map(|input| (&input.relative_path, &input.sha256, input.bytes))
                .collect::<Vec<_>>();
            serde_json::json!([
                "process",
                execution.admission.code_sha256,
                inputs,
                execution.stdout_sha256,
                execution.stderr_sha256,
                execution.stdout_bytes,
                execution.stderr_bytes
            ])
        }
        EvidenceTargetIdentity::LocalNetwork { operation } => {
            if !operation.host_joined
                || !operation.coverage_complete
                || operation.operations.is_empty()
            {
                return None;
            }
            let mut operations = Vec::new();
            for observed in &operation.operations {
                let LocalNetworkTerminal::BodyDone {
                    status,
                    body_bytes,
                    body_sha256,
                } = &observed.terminal
                else {
                    return None;
                };
                if !(200..300).contains(status) {
                    return None;
                }
                operations.push(serde_json::json!([
                    observed.endpoint,
                    observed.input_sha256,
                    status,
                    body_bytes,
                    body_sha256
                ]));
            }
            serde_json::json!(["network", operations])
        }
        _ => return None,
    };
    serde_json::to_string(&identity).ok()
}

fn fresh_execution_health_progress(
    prior: &[harness_contract::context::ObservedEvidence],
    current: &[harness_contract::context::ObservedEvidence],
    new_observed_fingerprints: &BTreeSet<String>,
) -> bool {
    let known = prior
        .iter()
        .filter_map(execution_health_identity)
        .collect::<BTreeSet<_>>();
    current
        .iter()
        .filter(|evidence| {
            evidence.provenance
                == harness_contract::context::ObservedEvidenceProvenance::FreshExecution
                && new_observed_fingerprints.contains(
                    &crate::path_identity::observed_evidence_fingerprint(evidence),
                )
        })
        .filter_map(execution_health_identity)
        .any(|identity| !known.contains(&identity))
}

fn execution_progress_advanced(
    before: &ObjectiveMilestones,
    after: &mut ObjectiveMilestones,
    typed_closure: bool,
    fresh_execution_health: bool,
) -> bool {
    after.convergence_started = after.convergence_started
        || before.convergence_started
        || before.completion_requested
        || before.verdict_bound
        || before.objective_reviews > 0
        || after.completion_requested
        || after.verdict_bound
        || after.objective_reviews > 0
        || typed_closure;
    objective_milestone_advanced(before, after)
        || (!after.convergence_started && fresh_execution_health)
}

fn next_acquisition_stall_streak(
    previous: usize,
    failed: usize,
    stale: bool,
    verified_progress: bool,
) -> usize {
    if failed == 0 && stale && !verified_progress {
        previous.saturating_add(1)
    } else {
        0
    }
}

fn forced_terminal_presentation_allowed(
    framing: &serde_json::Value,
    delegated_bounded_complete: bool,
) -> bool {
    delegated_bounded_complete
        || (framing["exit_ready"].as_bool() == Some(true)
            && framing
                .get("blocked")
                .is_none_or(serde_json::Value::is_null)
            && framing["remaining"]
                .as_array()
                .is_some_and(|remaining| remaining.is_empty()))
}

fn effective_text_only_request(
    heuristic: bool,
    terminal_ready: bool,
    refusal: bool,
    action_required: bool,
) -> bool {
    ((heuristic && terminal_ready) || refusal) && !action_required
}

fn acquisition_saturation_intervention(
    terminal_ready: bool,
    streak: usize,
    limit: usize,
) -> RuntimeInterventionKind {
    if terminal_ready {
        RuntimeInterventionKind::Synthesize
    } else if streak > limit {
        RuntimeInterventionKind::Block
    } else {
        RuntimeInterventionKind::Replan
    }
}

#[cfg(test)]
mod own865_host_saturation_tests {
    use super::*;

    #[test]
    fn bounded_role_without_declared_acceptance_scopes_is_not_blocked_by_saturation() {
        // A delegated bounded-evidence role only declares required scopes when
        // its packet carries evidence obligations. A role without any has
        // nothing outstanding, so its acceptance is trivially met. Requiring a
        // non-empty required-scope list made that case permanently
        // unsatisfiable, and the saturation guard could then only Block the
        // role - even after its work and evidence were already retained.
        assert!(
            focus_acceptance_is_met(true, &[], &[]),
            "a bounded role with no declared obligation has nothing pending"
        );
        assert!(
            should_force_focus_synthesis(true, &[], true, false, false),
            "repeated saturation with nothing to acquire must present retained evidence, not block"
        );
        // A declared obligation that is still pending is not acceptance.
        assert!(!focus_acceptance_is_met(
            true,
            &["read:src/lib.rs".to_string()],
            &["read:src/lib.rs".to_string()]
        ));
        // A declared obligation that is still pending is not acceptance, and
        // an unmet acceptance never forces synthesis.
        assert!(!should_force_focus_synthesis(
            false,
            &["read:src/lib.rs".to_string()],
            true,
            false,
            false
        ));
        // A declared and satisfied obligation still synthesizes once the
        // bounded novelty policy observes repeated saturation.
        assert!(should_force_focus_synthesis(
            true,
            &["read:src/lib.rs".to_string()],
            true,
            false,
            false
        ));
        // A main turn is never a bounded evidence role.
        assert!(!focus_acceptance_is_met(false, &[], &[]));
    }

    #[test]
    fn healthy_edit_then_two_reads_does_not_repeat_873_premature_synthesis() {
        use harness_contract::context::{
            EvidenceTargetIdentity, WorkspaceAccessMode, WorkspacePriorState,
        };
        let prior = super::own865_execution_health_tests::workspace(&"1".repeat(64));
        let mut edit = super::own865_execution_health_tests::workspace(&"2".repeat(64));
        if let EvidenceTargetIdentity::Workspace { scope } = &mut edit.target {
            scope.access_mode = WorkspaceAccessMode::Write;
        }
        edit.workspace_prior_state = Some(WorkspacePriorState::Existing {
            sha256: "1".repeat(64),
        });
        let fresh = crate::path_identity::fresh_novel_observed_evidence_fingerprints(
            std::slice::from_ref(&prior),
            std::slice::from_ref(&edit),
        );
        let fresh_health = fresh_execution_health_progress(
            std::slice::from_ref(&prior),
            std::slice::from_ref(&edit),
            &fresh,
        );
        assert!(fresh_health);
        let before = ObjectiveMilestones::default();
        let mut after = before.clone();
        let health = execution_progress_advanced(&before, &mut after, false, fresh_health);
        assert!(health);
        let mut streak = next_acquisition_stall_streak(2, 0, true, health);
        assert_eq!(streak, 0);
        for expected in 1..=2 {
            streak = next_acquisition_stall_streak(streak, 0, true, false);
            assert_eq!(streak, expected);
            assert!(streak < evidence_saturation_limit(false));
        }
    }

    #[test]
    fn repeated_reads_replan_with_tools_then_block_without_success() {
        let framing = serde_json::json!({"current":"uninitialized","exit_ready":false,"remaining":[],
            "program_source":{"status":"absent"}});
        assert!(!forced_terminal_presentation_allowed(&framing, false));
        let limit = evidence_saturation_limit(false);
        assert_eq!(
            acquisition_saturation_intervention(false, limit, limit),
            RuntimeInterventionKind::Replan
        );
        assert_eq!(
            acquisition_saturation_intervention(false, limit + 1, limit),
            RuntimeInterventionKind::Block
        );
        let mut streak = limit;
        streak = next_acquisition_stall_streak(streak, 0, true, false);
        assert_eq!(streak, limit + 1);
        assert!(
            !forced_terminal_presentation_allowed(&framing, false),
            "a provider replan or an absent-schema attempted call is not terminal proof"
        );
    }

    #[test]
    fn genuine_terminal_and_delegated_completion_allow_text_not_pending_debt() {
        let ready = serde_json::json!({"exit_ready":true,"remaining":[]});
        assert!(forced_terminal_presentation_allowed(&ready, false));
        assert_eq!(
            acquisition_saturation_intervention(true, 3, 3),
            RuntimeInterventionKind::Synthesize
        );
        let debt = serde_json::json!({"exit_ready":true,"remaining":["review"]});
        assert!(!forced_terminal_presentation_allowed(&debt, false));
        let unknown = serde_json::json!({"exit_ready":false,"remaining":[]});
        assert!(!forced_terminal_presentation_allowed(&unknown, false));
        assert!(forced_terminal_presentation_allowed(&unknown, true));
    }

    #[test]
    fn closure_churn_does_not_renew_acquisition_health() {
        let before = ObjectiveMilestones {
            convergence_started: true,
            ..Default::default()
        };
        let mut after = before.clone();
        assert!(!execution_progress_advanced(
            &before, &mut after, true, true
        ));
        assert_eq!(next_acquisition_stall_streak(2, 0, true, false), 3);
        assert_eq!(next_acquisition_stall_streak(2, 1, true, false), 0);
    }

    #[test]
    fn presentation_hint_does_not_disarm_unknown_but_safety_refusal_is_retained() {
        assert!(!effective_text_only_request(true, false, false, false));
        assert!(effective_text_only_request(true, true, false, false));
        assert!(effective_text_only_request(false, false, true, false));
        assert!(!effective_text_only_request(true, true, true, true));
        assert!(!effective_text_only_request(false, false, false, false));
    }

    #[test]
    fn identical_observation_after_schema_recovery_is_not_health() {
        let original = super::own865_execution_health_tests::workspace(&"1".repeat(64));
        let mut replay = original.clone();
        replay.observed_at_sequence += 1;
        let fresh = crate::path_identity::fresh_novel_observed_evidence_fingerprints(
            std::slice::from_ref(&original),
            std::slice::from_ref(&replay),
        );
        assert!(!fresh_execution_health_progress(
            std::slice::from_ref(&original),
            std::slice::from_ref(&replay),
            &fresh
        ));
        assert_eq!(next_acquisition_stall_streak(2, 0, true, false), 3);
    }
}

/// Root model steps after a satisfied verdict with no final answer before
/// Runtime hands the root the turn that writes it.
///
/// The Objective is routinely satisfied by the very action that closes it, so
/// the root's last step is that tool call. Nothing then gives it another turn,
/// and one live run went silent for 600s after a satisfied verdict with every
/// Task accepted until an inactivity watchdog cancelled it. The grace period
/// keeps a model that is about to answer on its own from being pre-empted.

fn verified_answer_drive_after_steps() -> usize {
    std::env::var("COWD_VERIFIED_ANSWER_DRIVE_AFTER_STEPS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2)
}

/// Every accepted Task's delivered result, as the text the terminal narrator
/// synthesizes the user-visible answer from.
///
/// Read from the Program projection rather than from a model transcript, so the
/// carrier describes what was actually delivered and accepted.
fn verified_program_delivered_results(program: &crate::AgenticProgramProjection) -> Vec<String> {
    let mut results = Vec::new();
    for task in program.tasks.values() {
        if task.status != crate::AgenticTaskStatus::Accepted {
            continue;
        }
        for artifact_ref in &task.artifact_refs {
            let Some(artifact) = program.artifacts.get(artifact_ref) else {
                continue;
            };
            results.push(format!(
                "Task `{}` delivered `{}` ({}): {}",
                task.task_id, artifact.title, artifact.artifact_ref, artifact.content_ref,
            ));
        }
    }
    results.sort();
    results.dedup();
    results
}

#[test]
fn verified_answer_drive_grace_period_defaults_to_two_steps() {
    // Positive, and short: the verdict is already durable, so the only thing
    // being waited for is the root's own answer.
    assert!(verified_answer_drive_after_steps() >= 1);
}

/// Task churn is work, not convergence. One live run spent 304 steps publishing,
/// submitting and re-reviewing Tasks without ever requesting completion, and the
/// old definition counted every one of those steps as verified progress.
#[test]
fn task_churn_alone_is_not_semantic_progress() {
    let before = ObjectiveMilestones {
        outstanding_closure_rungs: 3,
        ..Default::default()
    };
    // The same rungs, the same requests, the same reviews: only Tasks moved.
    let after = before.clone();
    assert!(
        !objective_milestone_advanced(&before, &after),
        "identical milestones cannot be progress"
    );
}

/// A completion request landing is the clearest convergence signal there is.
#[test]
fn a_completion_request_is_semantic_progress() {
    let before = ObjectiveMilestones::default();
    let after = ObjectiveMilestones {
        completion_requested: true,
        ..Default::default()
    };
    assert!(objective_milestone_advanced(&before, &after));
}

/// The closure ladder shrinking means the root has fewer rungs left to climb,
/// even when nothing else changed.
#[test]
fn a_reduced_closure_rung_is_semantic_progress() {
    let before = ObjectiveMilestones {
        outstanding_closure_rungs: 4,
        ..Default::default()
    };
    let after = ObjectiveMilestones {
        outstanding_closure_rungs: 3,
        ..before.clone()
    };
    assert!(objective_milestone_advanced(&before, &after));
}

/// Once the ladder is at its floor, further Task churn registers nothing, so the
/// no-progress fuse is free to fire. That is the run the old definition let
/// through indefinitely.
#[test]
fn a_floored_ladder_stops_reporting_progress() {
    let floored = ObjectiveMilestones {
        outstanding_closure_rungs: 0,
        ..Default::default()
    };
    assert!(
        !objective_milestone_advanced(&floored, &floored),
        "a run at its closure floor with no new milestone is not progressing"
    );
}

/// A verdict binding is terminal progress even though it ends the turn.
#[test]
fn a_bound_verdict_is_semantic_progress() {
    let before = ObjectiveMilestones::default();
    let after = ObjectiveMilestones {
        verdict_bound: true,
        ..Default::default()
    };
    assert!(objective_milestone_advanced(&before, &after));
}

#[test]
fn closure_drive_grace_period_defaults_to_three_steps() {
    // The grace period must be positive: zero would pre-empt a model that is
    // merely mid-thought on its first ready step.
    assert!(closure_drive_after_steps() >= 1);
}

#[cfg(test)]
fn protocol_state(mode: &str, artifact_refs: Vec<&str>) -> DelegatedAgenticProtocolState {
    DelegatedAgenticProtocolState {
        program_id: "program-1".to_string(),
        task_id: "task-1".to_string(),
        mode: mode.to_string(),
        purpose: harness_contract::agent_action::TaskPurpose::Delivery,
        tool_ceiling: test_protocol_tool_ceiling(),
        status: crate::AgenticTaskStatus::Submitted,
        artifact_refs: artifact_refs.into_iter().map(str::to_string).collect(),
        artifact_evidence_refs: Vec::new(),
        owns_active_attempt: true,
        review_reason: None,
    }
}

/// The closure set becomes the next request's activation ceiling, so it has to
/// carry the read-only tools the closing Agent still needs. One live run was
/// blocked with `tool_protocol_violation: ... [grep_search]` because the set
/// held exactly one action tool and the Agent could not read the file its own
/// objective named.
#[test]
fn closure_tool_ids_includes_the_read_only_evidence_tools() {
    for (mode, artifacts, action_tool) in [
        (
            "review",
            vec![],
            harness_contract::agent_action::TASK_REVIEW_TOOL_ID,
        ),
        (
            "execute",
            vec![],
            harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID,
        ),
        (
            "execute",
            vec!["artifact:a"],
            harness_contract::agent_action::TASK_SUBMIT_TOOL_ID,
        ),
    ] {
        let ids = protocol_state(mode, artifacts).closure_tool_ids();
        assert!(
            ids.contains(action_tool),
            "{mode} must still close with {action_tool}: {ids:?}"
        );
        for read_only in CLOSURE_READ_ONLY_EVIDENCE_TOOLS {
            assert!(
                ids.contains(read_only),
                "{mode} must keep the read-only tool {read_only}: {ids:?}"
            );
        }
    }
}

#[test]
fn delivery_closure_preserves_submission_paths_without_opening_new_work_or_mutation() {
    let ids = protocol_state("execute", vec!["artifact:a"]).closure_tool_ids();
    assert!(ids.contains("task_submit"));
    assert!(ids.contains("artifact_commit"));
    assert!(ids.contains("artifact_publish"));
    for forbidden in [
        "write_file",
        "edit",
        "bash",
        "task_publish",
        "task_claim",
        "task_supersede",
        "task_withdraw",
        "agent_invite",
        "team_create",
        "message_publish",
        "objective_review",
        "objective_complete_request",
        "objective_update",
    ] {
        assert!(
            !ids.contains(forbidden),
            "closure must not admit {forbidden}: {ids:?}"
        );
    }
}

/// The reviewer mode closes with `task_review`, and it must not also admit the
/// worker's `task_submit` - otherwise a reviewer could commit new work.
#[test]
fn the_reviewer_mode_does_not_admit_the_workers_submit() {
    let ids = protocol_state("review", vec![]).closure_tool_ids();
    assert!(ids.contains(harness_contract::agent_action::TASK_REVIEW_TOOL_ID));
    assert!(
        !ids.contains(harness_contract::agent_action::TASK_SUBMIT_TOOL_ID),
        "a reviewer must not be able to submit new work: {ids:?}"
    );
    assert!(
        !ids.contains(harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID),
        "a reviewer must not be able to commit a new artifact: {ids:?}"
    );
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
/// True when the turn must converge instead of re-driving delegated work.
///
/// The bound is the product's own convergence-replan policy value: a turn that
/// re-drives delegated work past it must present the retained receipts and
/// their verified facts. A mandatory closure rung or an outstanding
/// completion gap keeps the turn open - convergence must never drop work the
/// framework still owes.
fn forced_convergence_required(
    delegated_redrives: usize,
    bound: usize,
    completion_gap_outstanding: bool,
    outstanding_closure_rungs: usize,
    blocked_program: bool,
    progress_known: bool,
) -> bool {
    progress_known && !blocked_program && delegated_redrives > bound
        && !completion_gap_outstanding
        && outstanding_closure_rungs == 0
}

fn collaboration_convergence_replan_bound(
    complexity: harness_contract::core::TaskComplexity,
) -> usize {
    crate::execution_core::ComplexityBudget::derive(complexity).convergence_replans
}

#[cfg(test)]
mod forced_convergence_tests {
    use super::forced_convergence_required;

    #[test]
    fn progress_resets_redrive_and_unrelated_batches_do_not_count() {
        assert_eq!(super::next_collaboration_redrives(6, true, true), 0);
        assert_eq!(super::next_collaboration_redrives(6, false, false), 6);
        assert_eq!(super::next_collaboration_redrives(6, false, true), 7);
    }

    #[test]
    fn forced_convergence_starts_past_the_policy_bound() {
        assert!(!forced_convergence_required(6, 6, false, 0, false, true));
        assert!(forced_convergence_required(7, 6, false, 0, false, true));
    }

    #[test]
    fn forced_convergence_never_drops_owed_work() {
        assert!(!forced_convergence_required(9, 6, true, 0, false, true));
        assert!(!forced_convergence_required(9, 6, false, 1, false, true));
        assert!(forced_convergence_required(9, 6, false, 0, false, true));
        assert!(!forced_convergence_required(9, 6, false, 0, true, true));
        assert!(!forced_convergence_required(9, 6, false, 0, false, false));
    }

    #[test]
    fn durable_progress_is_counted_once() {
        let mut seen = super::VerifiedCollaborationProgress::default();
        assert!(seen.observe(None, 1, ["durable:one".to_string()]));
        assert!(!seen.observe(None, 1, ["durable:one".to_string()]));
        assert!(seen.observe(None, 2, ["durable:one".to_string()]));
    }
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
            team_count_contract_version: 1,
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
            team_count_contract_version: 1,
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
        use super::{evaluate_loop_guard, LoopGuardAction};
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
}

#[cfg(test)]
mod own865_execution_health_tests {
    use super::*;
    use harness_contract::context::{
        ArtifactRef, EvidenceCoverageKind, EvidenceDurability, EvidenceTargetIdentity,
        ObservedEvidence, ObservedEvidenceProvenance, ToolExecutionAdmission, ToolExecutionBinding,
        ToolExecutionFact, ToolExecutionInput, ToolExecutionStopReason, ToolExecutionTerminal,
        ToolProcessClosure, WorkspaceAccessMode, WorkspaceObjectKind, WorkspacePathIdentity,
        WorkspacePriorState, WorkspaceScopeIdentity,
    };

    pub(super) fn workspace(digest: &str) -> ObservedEvidence {
        ObservedEvidence {
            obligation_id: String::new(),
            target: EvidenceTargetIdentity::Workspace {
                scope: WorkspaceScopeIdentity {
                    access_mode: WorkspaceAccessMode::Read,
                    coverage: EvidenceCoverageKind::ExactContent,
                    path: WorkspacePathIdentity {
                        workspace_id: "health-workspace".into(),
                        repository_id: "health-repository".into(),
                        workspace_relative_path: "project/src/main.rs".into(),
                        repository_relative_path: "src/main.rs".into(),
                        object_kind: WorkspaceObjectKind::File,
                        observed_revision_or_digest: Some(digest.into()),
                    },
                },
            },
            observed_at_sequence: 1,
            tool_name: "read_file".into(),
            provenance: ObservedEvidenceProvenance::FreshExecution,
            evidence_ref: None,
            model_observation: None,
            workspace_prior_state: None,
        }
    }

    fn process() -> ObservedEvidence {
        let mut evidence = workspace("unused");
        evidence.tool_name = "execute_code".into();
        let artifact = ArtifactRef {
            selector: "artifact://health-original".into(),
            sha256: "sha256:".to_string() + &"1".repeat(64),
            bytes: 1,
            media_type: "application/json".into(),
            durability: EvidenceDurability::Durable,
            visibility_scope: "session:health".into(),
        };
        evidence.target = EvidenceTargetIdentity::ToolExecution {
            execution: ToolExecutionFact {
                version: 1,
                admission: ToolExecutionAdmission {
                    version: 1,
                    binding: ToolExecutionBinding {
                        call_key: "health-call-1".into(),
                        tool_use_id: "health-tool-1".into(),
                        input_sha256: "2".repeat(64),
                        descriptor_sha256: "3".repeat(64),
                        authority_sha256: "4".repeat(64),
                    },
                    code_sha256: "5".repeat(64),
                    inputs: vec![ToolExecutionInput {
                        relative_path: "src/main.rs".into(),
                        sha256: "6".repeat(64),
                        bytes: 1,
                        device: 1,
                        inode: 1,
                        mtime_ns: 1,
                        ctime_ns: 1,
                    }],
                    source_bundle: artifact.clone(),
                },
                terminal: ToolExecutionTerminal::Settled {
                    exit_code: Some(0),
                    signal: None,
                    stop_reason: ToolExecutionStopReason::CommandExit,
                    closure: ToolProcessClosure {
                        backend: "test-original".into(),
                        namespace_inode: 1,
                        init_host_pid: 1,
                        init_start_ticks: 1,
                        all_children_reaped: true,
                        init_reaped: true,
                        kernel_hardening_applied: true,
                        network_disabled: true,
                        project_read_only: true,
                        status_channel_protected: true,
                    },
                },
                body: artifact,
                stdout_sha256: "7".repeat(64),
                stderr_sha256: "8".repeat(64),
                stdout_bytes: 1,
                stderr_bytes: 0,
                capture_complete: true,
            },
        };
        evidence
    }

    fn health(prior: &[ObservedEvidence], current: &[ObservedEvidence]) -> bool {
        let fresh =
            crate::path_identity::fresh_novel_observed_evidence_fingerprints(prior, current);
        fresh_execution_health_progress(prior, current, &fresh)
    }

    #[test]
    fn ordinary_programming_health_continues_beyond_original_model_step_threshold() {
        let lease = crate::execution_core::SafetyFusePolicy::derive(
            128_000,
            harness_contract::core::TaskComplexity::Moderate,
            None,
        );
        let mut previous = ObjectiveMilestones::default();
        let mut prior = Vec::new();
        let mut streak = 0;
        for step in 0..lease.max_model_steps * 4 + 1 {
            let observed = workspace(&format!("{step:064x}"));
            let mut current = ObjectiveMilestones::default();
            let progress = execution_progress_advanced(
                &previous,
                &mut current,
                false,
                health(&prior, std::slice::from_ref(&observed)),
            );
            assert!(progress);
            streak = next_unverified_model_step_streak(streak, progress);
            assert!(matches!(
                crate::execution_core::SafetyFusePolicy::evaluate(&lease, streak, progress),
                crate::execution_core::SafetyFuseDecision::Continue
            ));
            assert!(!current.convergence_started);
            previous = current;
            prior.push(observed);
        }
        assert_eq!(streak, 0);
    }

    #[test]
    fn duplicate_content_replay_and_noop_write_do_not_renew_execution_health() {
        let original = workspace(&"1".repeat(64));
        let mut duplicate = original.clone();
        duplicate.observed_at_sequence += 1;
        assert!(!health(
            std::slice::from_ref(&original),
            std::slice::from_ref(&duplicate)
        ));
        duplicate.provenance = ObservedEvidenceProvenance::RetainedReplay;
        assert!(!health(&[], std::slice::from_ref(&duplicate)));
        let mut unchanged_write = original.clone();
        if let EvidenceTargetIdentity::Workspace { scope } = &mut unchanged_write.target {
            scope.access_mode = WorkspaceAccessMode::Write;
        }
        unchanged_write.workspace_prior_state = Some(WorkspacePriorState::Existing {
            sha256: "1".repeat(64),
        });
        assert!(!health(&[], std::slice::from_ref(&unchanged_write)));
        assert!(!health(&[], &[]));
    }

    #[test]
    fn fresh_process_nonce_and_filesystem_timestamp_churn_are_not_health_but_new_output_is() {
        let original = process();
        let mut duplicate = original.clone();
        duplicate.observed_at_sequence += 1;
        if let EvidenceTargetIdentity::ToolExecution { execution } = &mut duplicate.target {
            execution.admission.binding.call_key = "health-call-2".into();
            execution.admission.binding.tool_use_id = "health-tool-2".into();
            execution.admission.inputs[0].mtime_ns += 1;
            execution.admission.inputs[0].inode += 1;
            execution.body.selector = "artifact://health-second".into();
            if let ToolExecutionTerminal::Settled { closure, .. } = &mut execution.terminal {
                closure.init_host_pid += 1;
            }
        }
        assert!(!health(
            std::slice::from_ref(&original),
            std::slice::from_ref(&duplicate)
        ));
        if let EvidenceTargetIdentity::ToolExecution { execution } = &mut duplicate.target {
            execution.stdout_sha256 = "9".repeat(64);
        }
        assert!(health(
            std::slice::from_ref(&original),
            std::slice::from_ref(&duplicate)
        ));
        if let EvidenceTargetIdentity::ToolExecution { execution } = &mut duplicate.target {
            if let ToolExecutionTerminal::Settled { exit_code, .. } = &mut execution.terminal {
                *exit_code = Some(1);
            }
        }
        assert!(!health(
            std::slice::from_ref(&original),
            std::slice::from_ref(&duplicate)
        ));
    }

    #[test]
    fn actual_typed_convergence_latches_and_304_step_churn_still_fuses() {
        let lease = crate::execution_core::SafetyFusePolicy::derive(
            128_000,
            harness_contract::core::TaskComplexity::Moderate,
            None,
        );
        let mut previous = ObjectiveMilestones::default();
        let mut streak = 0;
        for step in 0..304 {
            let mut current = ObjectiveMilestones::default();
            let progress = execution_progress_advanced(&previous, &mut current, step == 0, true);
            assert!(!progress);
            assert!(current.convergence_started);
            streak = next_unverified_model_step_streak(streak, progress);
            previous = current;
        }
        assert!(matches!(
            crate::execution_core::SafetyFusePolicy::evaluate(&lease, streak, false),
            crate::execution_core::SafetyFuseDecision::Block { .. }
        ));
        let mut requested = previous.clone();
        requested.completion_requested = true;
        assert!(execution_progress_advanced(
            &previous,
            &mut requested,
            false,
            false
        ));
        assert_eq!(next_unverified_model_step_streak(streak, true), 0);
        previous = requested.clone();
        assert!(!execution_progress_advanced(
            &previous,
            &mut requested,
            false,
            true
        ));
    }

    #[test]
    fn completion_request_and_verdict_restrict_health_without_program_title_or_host_flags() {
        for mut current in [
            ObjectiveMilestones {
                completion_requested: true,
                ..Default::default()
            },
            ObjectiveMilestones {
                verdict_bound: true,
                ..Default::default()
            },
            ObjectiveMilestones {
                objective_reviews: 1,
                ..Default::default()
            },
        ] {
            let before = current.clone();
            assert!(!execution_progress_advanced(
                &before,
                &mut current,
                false,
                true
            ));
            assert!(current.convergence_started);
        }
    }

    #[test]
    fn post_take_collaboration_guard_uses_preserved_verified_progress() {
        let mut stored_progress = true;
        let made_progress = std::mem::take(&mut stored_progress);
        assert!(!stored_progress);
        let obligation = harness_contract::strategy::CollaborationExecutionObligation {
            team_count_contract_version: 1,
            minimum_team_count: 1,
            exact_team_count: None,
        };
        assert!(!collaboration_budget_elapsed(
            Some(&obligation),
            false,
            std::time::Duration::from_secs(100),
            Some(std::time::Duration::from_secs(1)),
            made_progress
        ));
        assert!(collaboration_budget_elapsed(
            Some(&obligation),
            false,
            std::time::Duration::from_secs(100),
            Some(std::time::Duration::from_secs(1)),
            stored_progress
        ));
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
    pub(super) purpose: harness_contract::agent_action::TaskPurpose,
    pub(super) tool_ceiling: BTreeSet<String>,
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
        if !self.owns_active_attempt
            || matches!(
                self.status,
                crate::AgenticTaskStatus::CancelRequested
                    | crate::AgenticTaskStatus::Withdrawn
                    | crate::AgenticTaskStatus::Superseded
            )
        {
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
                "Runtime collaboration protocol: review Task `{}` is still {:?}. Independently inspect the exact submitted artifact_refs [{}] through evidence refs [{}], then use `task_review`. Accept only if its acceptance criterion is proved; otherwise request rework with a concrete reason. Cite the inspected evidence and do not treat prose as durable review completion. {} Program `{}`.",
                self.task_id,
                self.status,
                self.artifact_refs.join(", "),
                self.artifact_evidence_refs.join(", "),
                crate::agentic::order_contract::REVIEWER_SCOPE_DISCIPLINE,
                self.program_id
            ),
            _ if self.purpose == harness_contract::agent_action::TaskPurpose::Exploration => format!(
                "Runtime collaboration protocol: exploration Task `{}` is still {:?}. {} Program `{}`.{}",
                self.task_id, self.status, crate::agentic::order_contract::EXPLORATION_CLOSEOUT_CONTRACT,
                self.program_id, rework_note,
            ),
            _ if self.artifact_refs.is_empty() => format!(
                "Runtime collaboration protocol: execute Task `{}` is still {:?}. {} Program `{}`.{}",
                self.task_id, self.status, crate::agentic::order_contract::TASK_REPORT_SUBMISSION_CONTRACT, self.program_id, rework_note
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

    pub(super) fn closure_tool_ids(&self) -> BTreeSet<String> {
        crate::agentic::order_contract::worker_closeout_action_ids(&self.mode, self.purpose)
            .into_iter()
            .map(str::to_string)
            .chain(
                CLOSURE_READ_ONLY_EVIDENCE_TOOLS
                    .iter()
                    .map(|name| name.to_string()),
            )
            .filter(|tool| self.tool_ceiling.contains(tool))
            .collect()
    }

    pub(super) fn terminal_action_ids(&self) -> impl Iterator<Item = &str> {
        crate::agentic::order_contract::worker_terminal_action_ids(&self.mode, self.purpose)
            .iter()
            .copied()
            .filter(|tool| self.tool_ceiling.contains(*tool))
    }
}

const CLOSURE_READ_ONLY_EVIDENCE_TOOLS: [&str; 6] = [
    "read_file",
    "read_ref",
    "evidence_retrieve",
    "grep_search",
    "glob_search",
    "state_inspect",
];

#[cfg(test)]
pub(super) fn test_protocol_tool_ceiling() -> BTreeSet<String> {
    harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .copied()
        .chain(CLOSURE_READ_ONLY_EVIDENCE_TOOLS)
        .chain(["artifact_publish"])
        .map(str::to_string)
        .collect()
}

pub(super) fn pending_delegated_action_is_ready(
    protocol: Option<&DelegatedAgenticProtocolState>,
    repeated_evidence_saturation: bool,
    has_successful_tool_evidence: bool,
    required_effects_pending: bool,
) -> bool {
    repeated_evidence_saturation
        && has_successful_tool_evidence
        && !required_effects_pending
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
    if packet.graph_id() != parent_graph.id
        || packet.node_id() != parent.node_id
        || packet.session_id() != projection.session_id
        || graph
            .lineage
            .as_ref()
            .is_some_and(|lineage| lineage.session_id != packet.session_id())
    {
        return Err(protocol_error(
            "packet purpose source has mismatched execution or Session identity".into(),
        ));
    }
    let task = projection.tasks.get(&task_id).ok_or_else(|| {
        protocol_error(format!(
            "Agent-first Program `{program_id}` has no Task `{task_id}`"
        ))
    })?;
    if agentic
        .task_purpose
        .is_some_and(|purpose| purpose != task.purpose)
    {
        return Err(protocol_error(
            "packet purpose does not match the bound Task".into(),
        ));
    }
    // `AgentTaskPacket::agent_id()` is the physical Binding instance identity
    // (for run locks, cancellation and process isolation). Agent-first
    // Program state is instead owned by the immutable logical identity that
    // Runtime attested in `agentic_binding`. These IDs intentionally differ
    // in production. Comparing the physical instance to Task.claimant made a
    // live claim appear foreign, which allowed generic text synthesis to
    // preempt artifact/submit closure and caused cross-graph retry loops.
    let actor_id = agentic.agent_id.as_str();
    let business_generation = crate::agentic::registered_attempt_generation(
        task, agentic, packet.graph_id(),
        if mode == "review" { harness_contract::agent_action::AgentAttemptMode::Review }
        else { harness_contract::agent_action::AgentAttemptMode::Execute },
    );
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
                    && business_generation.and_then(|generation| generation.checked_add(1))
                        .is_some_and(|generation| artifact.claim_generation == Some(generation))
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
                && business_generation.and_then(|generation| generation.checked_add(1)) == Some(task.claim_generation)
        }
        "review" => {
            task.status == crate::AgenticTaskStatus::Submitted
                && task.claimant.as_deref() != Some(actor_id)
                && business_generation == Some(task.review_generation)
        }
        _ => false,
    };
    Ok(Some(DelegatedAgenticProtocolState {
        program_id,
        task_id,
        mode: mode.to_string(),
        purpose: task.purpose,
        tool_ceiling: packet.allowed_tools.iter().cloned().collect(),
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
        if !is_commit && !is_submit && !is_review && !is_publish {
            continue;
        }
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&call.input) else {
            continue;
        };
        // Normalize bare evidence references to their durable scheme so a model
        // that dropped `tool://`/`artifact://` is not rejected with
        // unsupported_evidence_ref / issue_disposition_requires_durable_evidence.
        normalize_bare_evidence_refs(&mut value);
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
/// Public because the reference arrives through more than one door: a direct
/// tool call, and each action inside an `agent_action_batch`, which the Gateway
/// expands on its own. A reference Runtime itself spells `artifact:<24 hex>` and
/// repeats in a completion gap reaches the batch path too, and a batch that did
/// not normalize it refused a review the model had already done the reading for.
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
pub fn normalize_bare_evidence_ref(reference: &str) -> String {
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

// Run before Native source registration; early candidates that need this
// transformation must be deferred before an original request is dispatched.
pub(crate) fn normalize_native_read_input(tool_name: &str, input: &str) -> String {
    if !matches!(tool_name, "read_ref" | "evidence_retrieve") {
        return input.to_string();
    }
    let Ok(mut value) = harness_contract::step::parse_strict_json(input) else {
        return input.to_string();
    };
    let original = value.clone();
    normalize_bare_evidence_refs(&mut value);
    if let Some(object) = value.as_object_mut() {
        for field in ["reference", "evidence_ref"] {
            if let Some(raw) = object.get(field).and_then(|v| v.as_str()) {
                let normalized = normalize_bare_evidence_ref(raw);
                if normalized != raw {
                    object.insert(field.to_string(), serde_json::Value::String(normalized));
                }
            }
        }
    }
    if value == original { input.to_string() } else {
        serde_json::to_string(&value).unwrap_or_else(|_| input.to_string())
    }
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
    use super::{normalize_bare_evidence_ref, normalize_native_read_input};

    #[test]
    fn native_read_input_normalization_preserves_identity_and_invalid_frames() {
        let raw = r#"{"reference":"artifact:ad3e1d53f09c3d25e1953fda"}"#;
        let canonical = r#"{"reference":"artifact://ad3e1d53f09c3d25e1953fda"}"#;
        assert_eq!(normalize_native_read_input("read_ref", raw), canonical);
        assert_eq!(normalize_native_read_input("read_ref", canonical), canonical);
        assert_eq!(normalize_native_read_input("state_inspect", raw), raw);
        let foreign = r#"{"reference":"artifact:another"}"#;
        assert_ne!(normalize_native_read_input("read_ref", foreign), canonical);
        let duplicate = r#"{"reference":"artifact:one","reference":"artifact:two"}"#;
        assert_eq!(normalize_native_read_input("read_ref", duplicate), duplicate);
    }

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

/// Original checked results already owned by this physical ToolBatch.
enum OriginalPostActionCommit {
    Single(Arc<crate::agentic::OriginalCommittedSingle>),
    Member(Arc<crate::OriginalCommittedMember>),
    Conclusion(Arc<crate::agentic::OriginalCommittedConclusion>),
    Replay(Arc<crate::agentic::OriginalReplayedAction>),
}
impl OriginalPostActionCommit {
    fn request(&self) -> Option<&crate::CheckedAppendRequest> { match self { Self::Single(v) => Some(v.request()), Self::Member(v) => Some(v.request()), Self::Conclusion(v) => Some(v.request()), Self::Replay(_) => None } }
    fn receipt(&self) -> &crate::AppendTransactionReceipt { match self { Self::Single(v) => &v.receipt().commit, Self::Member(v) => &v.receipt().commit, Self::Conclusion(v) => &v.receipt().commit, Self::Replay(v) => v.receipt() } }
    fn observation(&self) -> Option<&harness_contract::agent_action::AgentActionObservation> { match self { Self::Single(v) => v.observation().map(Arc::as_ref), Self::Member(v) => v.observation().map(Arc::as_ref), Self::Conclusion(_) => None, Self::Replay(v) => v.observation() } }
    fn belongs_to(&self, store: &Arc<crate::RuntimeEventStore>) -> bool { match self { Self::Single(v) => v.belongs_to(store), Self::Member(v) => v.belongs_to(store), Self::Conclusion(v) => v.belongs_to(store), Self::Replay(v) => v.belongs_to(store) } }
}

/// The entity one certified action of this node created, with the action kind
/// that produced it. Only an applied action creates one; a rejected action has
/// no effect to look for.
fn original_applied_effect(
    handle: &OriginalPostActionCommit,
    program_stream: &str,
) -> Option<(String, String)> {
    if !handle.observation().is_some_and(|observation|
        observation.status == harness_contract::agent_action::AgentActionStatus::Applied) {
        return None;
    }
    let request = handle.request()?;
    let event = request.transaction.events.iter()
        .find(|event| event.event.stream_id == program_stream)?;
    let effect = event.event.payload.get("entity_ref").and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())?.to_string();
    let kind = event.event.payload.pointer("/envelope/action/kind")
        .and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
    Some((effect, kind))
}

// Optional private fixture diagnostic: original carrier metadata only, never authority.
#[cfg(all(feature = "test-fixtures", target_os = "linux"))]
fn a9_goal_carrier_diagnostic(
    ticket: &NodeExecutionTicket, before_goal: &str, program_stream: &str,
    handles: &[OriginalPostActionCommit], effects: &[(String, String)],
) -> Result<(), String> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::io::Write;
    if std::env::var_os("COWD_A9_GOAL_CARRIER_DIAG").is_none() { return Ok(()); }
    if handles.len() > 32 || effects.len() > 32 { return Err("carrier count exceeds diagnostic budget".into()); }
    let directory = std::path::PathBuf::from(std::env::var_os("COWD_FIXTURE_EVIDENCE_DIR")
        .ok_or("private evidence directory missing")?);
    let metadata = std::fs::symlink_metadata(&directory).map_err(|e| e.to_string())?;
    let own_uid = std::fs::metadata("/proc/self").map_err(|e| e.to_string())?.uid();
    let on_tmpfs = std::process::Command::new("stat").args(["-f", "-c", "%T"]).arg(&directory)
        .output().is_ok_and(|output| output.status.success() && output.stdout == b"tmpfs\n");
    if !directory.is_absolute() || directory.canonicalize().map_err(|e| e.to_string())? != directory
        || !directory.starts_with("/dev/shm") || !metadata.is_dir() || metadata.file_type().is_symlink()
        || metadata.uid() != own_uid || metadata.permissions().mode() & 0o777 != 0o700 || !on_tmpfs {
        return Err("private current-owner tmpfs directory required".into());
    }
    let carriers = handles.iter().map(|handle| {
        let receipt = handle.receipt();
        let events = handle.request().map(|request| request.transaction.events.iter().enumerate().take(32).map(|(index, event)| {
            serde_json::json!({"index":index,"stream":event.event.stream_id,"kind":event.event.kind,
                "action_kind":event.event.payload.pointer("/envelope/action/kind"),
                "entity_ref":event.event.payload.get("entity_ref"),
                "action_entity_ref":event.event.payload.get("action_entity_ref"),
                "receipt_event_id":receipt.event_ids.get(index)})
        }).collect::<Vec<_>>());
        let variant = match handle { OriginalPostActionCommit::Single(_) => "single", OriginalPostActionCommit::Member(_) => "member",
            OriginalPostActionCommit::Conclusion(_) => "conclusion", OriginalPostActionCommit::Replay(_) => "replay" };
        let replay_goal = match handle { OriginalPostActionCommit::Replay(replay) => replay.goal_event().map(|event| serde_json::json!({
            "stream":event.stream_id,"kind":event.kind,"event_id":event.event_id,"transaction_id":event.transaction_id,
            "index":event.transaction_index,"cursor":event.commit_cursor })), _ => None };
        serde_json::json!({"variant":variant,"transaction_id":receipt.transaction_id,
            "cursor":receipt.commit_cursor,"event_ids":receipt.event_ids,"stream_revisions":receipt.stream_revisions,
            "observation":handle.observation().map(|observation| format!("{:?}", observation.status)),
            "request_txn":handle.request().map(|request| &request.transaction.transaction_id),
            "request_event_count":handle.request().map(|request| request.transaction.events.len()),
            "request_events":events,"replay_goal":replay_goal})
    }).collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&serde_json::json!({"graph":ticket.graph_id,"node":ticket.node_id,
        "attempt":ticket.attempt,"before_goal":before_goal,"program_stream":program_stream,
        "effects":effects,"carriers":carriers})).map_err(|e| e.to_string())?;
    if bytes.len() > 256 * 1024 { return Err("carrier diagnostic byte budget exceeded".into()); }
    let path = directory.join(format!("a9-goal-carrier-{}-{}.json", std::process::id(), uuid::Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600)
        .open(&path).map_err(|e| e.to_string())?;
    file.write_all(&bytes).and_then(|_| file.sync_all()).map_err(|e| e.to_string())?;
    eprintln!("A9_GOAL_CARRIER_PRIVATE path={} bytes={}", path.display(), bytes.len());
    Ok(())
}

/// The Program facts the visibility check needs, read from the certified
/// projection. Kept as plain data so the judgment itself is testable without
/// a Program behind it.
#[derive(Debug, Default)]
struct OriginalTaskView {
    claim_generation: u64,
    review_generation: u64,
    /// A review records its reviewer even when the decision is Accept.
    reviewed_by: bool,
    /// A review that settled the Task: Accepted, Rework or Blocked.
    review_outcome: bool,
}

#[derive(Debug, Default)]
struct OriginalProgramEntityView {
    tasks: std::collections::BTreeMap<String, OriginalTaskView>,
    teams: std::collections::BTreeSet<String>,
    agents: std::collections::BTreeSet<String>,
    artifacts: std::collections::BTreeSet<String>,
}

fn original_program_entity_view(
    projection: &crate::agentic::AgenticProgramProjection,
) -> OriginalProgramEntityView {
    OriginalProgramEntityView {
        tasks: projection.tasks.iter().map(|(task_id, task)| {
            (task_id.clone(), OriginalTaskView {
                claim_generation: task.claim_generation,
                review_generation: task.review_generation,
                reviewed_by: task.reviewed_by.is_some(),
                review_outcome: matches!(task.status,
                    crate::agentic::AgenticTaskStatus::Accepted
                    | crate::agentic::AgenticTaskStatus::Rework
                    | crate::agentic::AgenticTaskStatus::Blocked),
            })
        }).collect(),
        teams: projection.teams.keys().cloned().collect(),
        agents: projection.agents.keys().cloned().collect(),
        artifacts: projection.artifacts.keys().cloned().collect(),
    }
}

/// What the certified Program projection says about one certified effect of
/// this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OriginalEffectJudgment {
    /// The projection carries this node's effect.
    Carried,
    /// The projection does not carry this node's effect.
    NotCarried,
    /// The effect has no Program entity to be carried by: a published message
    /// is named by a digest in its own id space, not by a Program entry, so
    /// there is nothing in the projection that could attest it. Its own
    /// durable facts are the action event, its receipt and the topic owner's
    /// entry.
    NoEntityClaim,
}

/// Whether one certified effect of this node is carried by the certified
/// Program projection. The lookup is kind-scoped: a Task effect is only
/// satisfied by a Task, never by an entity of another kind that happens to
/// carry the same id. For the actions that only move an existing Task, the
/// projection's own facts must show that the move happened - a claim advances
/// the claim generation, a review records its reviewer and settles the Task
/// (an accepted review does not advance the review generation).
fn original_effect_judgment(
    view: &OriginalProgramEntityView,
    effect: &str,
    action_kind: &str,
) -> OriginalEffectJudgment {
    let (kind, id) = match effect.split_once(':') {
        Some((kind, id)) if !id.is_empty() => (kind, id),
        _ => return OriginalEffectJudgment::NotCarried,
    };
    match kind {
        "task" => {
            let Some(task) = view.tasks.get(effect).or_else(|| view.tasks.get(id)) else {
                return OriginalEffectJudgment::NotCarried;
            };
            let carried = match action_kind {
                "task_claim" => task.claim_generation > 0,
                "task_review" => task.review_generation > 0 || task.reviewed_by || task.review_outcome,
                _ => true,
            };
            if carried { OriginalEffectJudgment::Carried } else { OriginalEffectJudgment::NotCarried }
        }
        "team" => if view.teams.contains(effect) || view.teams.contains(id) {
            OriginalEffectJudgment::Carried } else { OriginalEffectJudgment::NotCarried },
        "agent" => if view.agents.contains(effect) || view.agents.contains(id) {
            OriginalEffectJudgment::Carried } else { OriginalEffectJudgment::NotCarried },
        "artifact" => if view.artifacts.contains(effect) || view.artifacts.contains(id) {
            OriginalEffectJudgment::Carried } else { OriginalEffectJudgment::NotCarried },
        "message" => OriginalEffectJudgment::NoEntityClaim,
        _ => OriginalEffectJudgment::NotCarried,
    }
}

/// Authenticate an Objective review in the Goal event committed alongside the
/// original Program action. Objective reviews belong to Goal, not the Program
/// entity projection. This only checks the original checked transaction; it
/// does not execute or invent an action.
fn original_goal_review_carried(
    program_payload: &serde_json::Value,
    goal_payload: &serde_json::Value,
    original_goal: &harness_contract::goal::GoalContract,
    goal: &harness_contract::goal::GoalContract,
) -> bool {
    use harness_contract::agent_action::{AgentAction, AgentActionEnvelope, ObjectiveReviewDecision};
    let Ok(envelope) = serde_json::from_value::<AgentActionEnvelope>(program_payload["envelope"].clone()) else { return false };
    let AgentAction::ObjectiveReview(input) = &envelope.action else { return false };
    let Some(binding) = goal.execution_binding.as_ref() else { return false };
    if envelope.actor.objective_id != binding.objective_id
        || envelope.actor.program_id != binding.agentic_program_id
        || envelope.actor.session_id != binding.session_id
        || envelope.actor.turn_id != binding.turn_id
        || envelope.actor.root_execution_id.as_deref() != Some(binding.root_execution_id.as_str()) {
        return false;
    }
    let Some(effect) = program_payload.get("entity_ref").and_then(serde_json::Value::as_str) else { return false };
    if goal_payload.get("action_entity_ref").and_then(serde_json::Value::as_str) != Some(effect) { return false; }
    let decision = match input.decision {
        ObjectiveReviewDecision::Satisfied => "satisfied",
        ObjectiveReviewDecision::Gap => "gap",
        ObjectiveReviewDecision::Blocked => "blocked",
    };
    // Match GoalStore::normalize_criterion_ref against the Goal read by the
    // original action: an exact known id wins over its suffix.
    let known = |id: &str| original_goal.criteria.iter().any(|criterion| criterion.id == id)
        || original_goal.obligations.iter().any(|obligation| obligation.obligation_id == id);
    let criterion = if known(&input.criterion_ref) { input.criterion_ref.as_str() }
        else { input.criterion_ref.rsplit_once(':').map(|(_, tail)| tail).filter(|tail| known(tail)).unwrap_or(&input.criterion_ref) };
    goal.review_refs.iter().any(|reference| reference == effect)
        && goal.reviews.iter().any(|review| {
            criterion == review.criterion_ref
                && (goal.criteria.iter().any(|criterion| criterion.id == review.criterion_ref)
                    || goal.obligations.iter().any(|obligation| obligation.obligation_id == review.criterion_ref))
                && review.review_id == effect
                && review.spec_revision == goal.spec_revision
                && review.input_manifest_digest == goal.spec_digest
                && crate::execution_core::goal::objective_review_record_id(&goal.id, &envelope.action_id, &review.criterion_ref) == effect
                && review.reviewer_actor == envelope.actor.actor_id
                && review.reviewer_execution_id == envelope.actor.execution_id
                && review.decision == decision
                && review.result_refs == input.result_refs
                && review.evidence_refs == input.evidence_refs
        })
}

/// The existing transaction consumer invokes this before advancing its Goal
/// source. The two events and both stream cuts must be from the same receipt.
fn original_goal_review_transaction(
    transaction: &crate::AppendTransactionRequest,
    receipt: &crate::AppendTransactionReceipt,
    program_stream: &str,
    goal_stream: &str,
    original_goal: &harness_contract::goal::GoalContract,
    next: &harness_contract::goal::GoalContract,
) -> Result<Option<(String, serde_json::Value, serde_json::Value)>, String> {
    let events = transaction.events.iter().enumerate().filter(|(_, event)|
        event.event.stream_id == program_stream
            && event.event.payload.pointer("/envelope/action/kind").and_then(serde_json::Value::as_str) == Some("objective_review"))
        .collect::<Vec<_>>();
    if events.is_empty() { return Ok(None); }
    let [(index, event)] = events.as_slice() else { return Err("original Objective review action is ambiguous".into()) };
    let goal_events = transaction.events.iter().enumerate().filter(|(_, event)| event.event.stream_id == goal_stream).collect::<Vec<_>>();
    let [(goal_index, goal_event)] = goal_events.as_slice() else { return Err("original Goal action is ambiguous".into()) };
    let program_advanced = receipt.stream_revisions.iter().any(|cut|
        cut.stream_id == program_stream && cut.committed_revision > cut.expected_revision);
    let goal_advanced = receipt.stream_revisions.iter().any(|cut|
        cut.stream_id == goal_stream && cut.committed_revision > cut.expected_revision);
    if transaction.transaction_id != receipt.transaction_id
        || event.event.kind != "agentic.action_applied" || goal_event.event.kind != "goal.revised"
        || !program_advanced || !goal_advanced
        || receipt.event_ids.get(*index).is_none() || receipt.event_ids.get(*goal_index).is_none()
        || serde_json::from_value::<harness_contract::goal::GoalContract>(goal_event.event.payload["goal"].clone()).ok().as_ref() != Some(next)
        || !original_goal_review_carried(&event.event.payload, &goal_event.event.payload, original_goal, next) {
        return Err("original Objective review is not carried by its committed Goal".into());
    }
    let effect = event.event.payload["entity_ref"].as_str().ok_or("original Objective review identity missing")?;
    Ok(Some((effect.to_string(), event.event.payload.clone(), goal_event.event.payload.clone())))
}

/// Which Program cut a node's own certified effects have to be visible at.
///
/// The node writes the Program back under the live head, and that fence stays
/// exactly as it was. What must not travel with the fence is the *evidence*
/// check: this node's facts are guaranteed only at the cut its own commits
/// produced. Reading the projection at the live head let a later, legitimate
/// advance by somebody else (a rework that resets a claim generation, or the
/// bounded review rebuild landing a fresh cut) decide whether work this node
/// already completed counted as carried, and one such refusal blocked the whole
/// batch as `effect_completion_uncertain` with `retryable: false`. So the cut
/// is the node's own, while a stream that moved backwards below it stays the
/// original refusal.
fn original_program_certification_cut(own_cut: u64, live_head: u64) -> Result<u64, String> {
    if live_head < own_cut {
        return Err("Program changed between original actions".into());
    }
    Ok(own_cut)
}

/// Judge this node's certified effects against one Program entity view. The
/// refusal names the effect, the action kind, the cut actually certified at and
/// the live head, so a blocked publish can be classified from its own sentence
/// instead of only from a batch-level wrapper.
fn certify_original_program_effects(
    view: &OriginalProgramEntityView,
    effects: &[(String, String)],
    certified_cut: u64,
    live_head: u64,
) -> Result<(), String> {
    for (effect, action_kind) in effects.iter() {
        if original_effect_judgment(view, effect, action_kind) == OriginalEffectJudgment::NotCarried {
            return Err(format!(
                "original Program effect is not visible at the certified head; effect={effect} action={action_kind} certified_cut={certified_cut} live_head={live_head}"
            ));
        }
    }
    Ok(())
}

/// Read the Program at the cut this node's own certified actions produced and
/// confirm every effect is visible there. The revision certified is one the
/// node actually wrote, not one it assumed, and the live head is reported
/// beside it rather than used as the window.
fn certify_original_program_head(
    services: &Arc<crate::RuntimeServices>,
    program_id: &str,
    own_cut: u64,
    live_head: u64,
    effects: &[(String, String)],
) -> Result<(), String> {
    let certified_cut = original_program_certification_cut(own_cut, live_head)?;
    let store = services.event_store();
    let stream = format!("agentic-program:{}", program_id);
    let opened = store.event_by_idempotency_key(&stream, "agentic-program-opened")
        .map_err(|error| error.to_string())?
        .ok_or("original Program opening missing")?;
    let actions = services.agent_action_service();
    let projection = actions.project_snapshot_bounded(program_id, certified_cut, &opened, None,
        |after, through| store.list_stream_after(&stream, after, through, 100, 1024 * 1024),
        |error| error.to_string())
        .map_err(|error| format!("original Program projection unavailable: {error}"))?;
    certify_original_program_effects(
        &original_program_entity_view(&projection),
        effects,
        certified_cut,
        live_head,
    )
}

/// Reuse the original conclusion owner only for an authenticated completion
/// request whose same-call result has not yet reached its original sink.
fn publish_existing_original_conclusion<C: ApiClient, T: ToolExecutor>(
    backend: &TurnToolBatchBackend<C, T>,
    request: &crate::CheckedAppendRequest,
    receipt: &crate::AppendTransactionReceipt,
    action_id: &str,
) -> Result<bool, String> {
    let _services = backend.services.upgrade().ok_or("original ToolBatch services unavailable")?;
    if request.transaction.transaction_id != receipt.transaction_id {
        return Err("original completion action receipt differs".into());
    }
    let mut actions = request.transaction.events.iter().enumerate().filter(|(_, event)|
        event.event.kind == "agentic.action_applied"
            && event.event.payload.pointer("/envelope/action_id").and_then(serde_json::Value::as_str) == Some(action_id));
    let (position, action) = actions.next().ok_or("original completion action unavailable")?;
    if actions.next().is_some() || receipt.event_ids.get(position).is_none() {
        return Err("original completion action ambiguous".into());
    }
    let envelope: harness_contract::agent_action::AgentActionEnvelope = serde_json::from_value(
        action.event.payload.get("envelope").cloned().ok_or("original action envelope missing")?)
        .map_err(|error| error.to_string())?;
    if !matches!(&envelope.action, harness_contract::agent_action::AgentAction::ObjectiveCompleteRequest(_)) {
        return Ok(false);
    }
    let stream = format!("agentic-program:{}", envelope.actor.program_id);
    if action.event.stream_id != stream { return Err("original completion Program differs".into()); }
    let cut = receipt.stream_revisions.iter().find(|source| source.stream_id == stream)
        .ok_or("original completion Program source unavailable")?;
    if cut.expected_revision.checked_add(1) != Some(cut.committed_revision) {
        return Err("original completion Program revision ambiguous".into());
    }
    authenticate_original_conclusion_at_revision(backend, &envelope.actor.program_id, action_id, cut.committed_revision)
}

fn authenticate_original_conclusion_at_revision<C: ApiClient, T: ToolExecutor>(
    backend: &TurnToolBatchBackend<C, T>, program_id: &str, action_id: &str, revision: u64,
) -> Result<bool, String> {
    let services = backend.services.upgrade().ok_or("original ToolBatch services unavailable")?;
    let stream = format!("agentic-program:{program_id}");
    let store = services.event_store();
    let opened = store.event_by_idempotency_key(&stream, "agentic-program-opened")
        .map_err(|error| error.to_string())?.ok_or("original Program opening missing")?;
    let actions = services.agent_action_service();
    let projection = actions.project_snapshot_bounded(program_id, revision, &opened, None,
        |after, through| store.list_stream_after(&stream, after, through, 100, 1024 * 1024),
        |error| error.to_string())?;
    if projection.completion_request.as_ref().is_none_or(|completion|
        completion.action_id != action_id || completion.program_revision != revision) {
        return Err("original completion request projection differs".into());
    }
    // False means no authenticated conclusion winner, not rollback or Fresh.
    actions.authenticated_existing_request_conclusion(&projection).map_err(|error| error.to_string())
}

/// Apply same-call Goal revisions from original checked results. Later changes
/// remain outside these cuts and are rejected by the original node CAS.
fn original_committed_goal<C: ApiClient, T: ToolExecutor>(
    backend: &TurnToolBatchBackend<C, T>, ticket: &NodeExecutionTicket,
    before: &crate::execution_core::goal::GoalProjection,
    successful: &std::collections::BTreeSet<String>,
    delivered: &std::collections::BTreeSet<String>,
    members: &[(String, crate::PendingAgentBatch)],
    replays: &BTreeMap<String, HostOriginalReplay>,
) -> Result<Option<(crate::execution_core::goal::GoalProjection, Option<(String, u64)>)>, String> {
    let services = backend.services.upgrade().ok_or("original ToolBatch services unavailable")?;
    let mut committed = Vec::new();
    for (call_id, replay) in replays {
        if !successful.contains(call_id) || replay.call.id != *call_id
            || replay.original_action.call_id() != call_id || replay.original_action.ticket() != ticket {
            return Err("original replay delivery or owner differs".into());
        }
        if replay.call.name == harness_contract::agent_action::OBJECTIVE_COMPLETE_REQUEST_TOOL_ID {
            let action = replay.original_action.program_event();
            let envelope: harness_contract::agent_action::AgentActionEnvelope = serde_json::from_value(
                action.payload.get("envelope").cloned().ok_or("original replay action envelope missing")?)
                .map_err(|error| error.to_string())?;
            if !matches!(&envelope.action, harness_contract::agent_action::AgentAction::ObjectiveCompleteRequest(_)) {
                return Err("original replay completion action differs".into());
            }
            if authenticate_original_conclusion_at_revision(backend, &envelope.actor.program_id, &envelope.action_id, action.sequence)? {
                let store = services.event_store();
                let (_, original, owner) = store.retained_original_first_effect(&format!("{}:intent", envelope.action_id))
                    .map_err(|error| error.to_string())?.ok_or("original completion custody unavailable")?;
                let owner = owner.ok_or("original completion owner unavailable")?;
                if !owner.matches_original(store, &original) || original.idempotency_key != envelope.action_id
                    || original.tool_use_id != *call_id
                    || serde_json::to_value(original.as_ref()).map_err(|error| error.to_string())?
                        != action.payload["original_single_call"]["request"] {
                    return Err("original completion replay custody differs".into());
                }
                crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, &original)
                    .map_err(|error| error.to_string())?;
                let conclusion = original.tool_progress.original_conclusion(None, &envelope.action_id)?
                    .ok_or("original authenticated replay conclusion remains unpublished")?;
                committed.push(OriginalPostActionCommit::Conclusion(conclusion));
            }
        }
        committed.push(OriginalPostActionCommit::Replay(Arc::clone(&replay.original_action)));
    }
    for (call_id, pending) in members {
        if matches!(pending.value(), crate::PendingAgentBatchValue::WithCause { .. }) {
            return Err("original member result remains unresolved".into());
        }
        let crate::PendingAgentBatchValue::OriginalCall(carrier) = pending.value() else { continue };
        if carrier.original_request().tool_use_id != *call_id { return Err("original member call differs".into()); }
        if let Some(handle) = carrier.original_request().tool_progress.original_single()? {
            if !successful.contains(call_id) || !handle.belongs_to(services.event_store()) {
                return Err("original committed action delivery or owner unavailable".into());
            }
            if carrier.original_request().tool_progress.original_conclusion(None, &carrier.original_request().idempotency_key)?.is_none()
                && publish_existing_original_conclusion(backend, handle.request(), &handle.receipt().commit,
                    &carrier.original_request().idempotency_key)?
                && carrier.original_request().tool_progress.original_conclusion(None, &carrier.original_request().idempotency_key)?.is_none() {
                return Err("original authenticated conclusion remains unpublished".into());
            }
            committed.push(OriginalPostActionCommit::Single(handle));
        }
        let mut consumed_conclusions: Vec<(Option<usize>, Arc<crate::agentic::OriginalCommittedConclusion>)> = Vec::new();
        if carrier.original_request().tool_name != "agent_action_batch" {
            let action_id = &carrier.original_request().idempotency_key;
            if let Some(conclusion) = carrier.original_request().tool_progress.original_conclusion(None, action_id)? {
                if conclusion.action_id() != action_id || !successful.contains(call_id) {
                    return Err("original conclusion call or delivery differs".into());
                }
                consumed_conclusions.push((None, Arc::clone(&conclusion)));
                committed.push(OriginalPostActionCommit::Conclusion(conclusion));
            }
        }
        let mut member_indices = std::collections::BTreeSet::new();
        for (index, outcome) in carrier.outcomes() {
            let crate::tool_host::OriginalMemberSubmitOutcome::Committed(member) = outcome.as_ref() else {
                return Err("original member commit remains unknown".into());
            };
            if member.index() != *index || !member_indices.insert(*index) {
                return Err("original member ordinal differs or is ambiguous".into());
            }
            member.node_commit_fact(services.event_store(), ticket)?;
            let handle = member.same_owner_committed().ok_or("original member committed owner missing")?;
            // The member's outcome is certified by its original owner above.
            // What the outer still needs is that the call produced a published
            // result at all: a batch that never published anything has nothing
            // the model could have seen. Publication is not success, so an
            // error result still qualifies.
            if !delivered.contains(call_id) { return Err("original member delivery is unavailable".into()); }
            let action_id = member.action().payload().pointer("/envelope/action_id")
                .and_then(serde_json::Value::as_str).filter(|value| !value.is_empty())
                .ok_or("original member action identity unavailable")?;
            if carrier.original_request().tool_progress.original_conclusion(Some(*index), action_id)?.is_none()
                && publish_existing_original_conclusion(backend, handle.request(), &handle.receipt().commit, action_id)?
                && carrier.original_request().tool_progress.original_conclusion(Some(*index), action_id)?.is_none() {
                return Err("original authenticated member conclusion remains unpublished".into());
            }
            if let Some(conclusion) = carrier.original_request().tool_progress.original_conclusion(Some(*index), action_id)? {
                if conclusion.action_id() != action_id { return Err("original conclusion member differs".into()); }
                consumed_conclusions.push((Some(*index), Arc::clone(&conclusion)));
                committed.push(OriginalPostActionCommit::Conclusion(conclusion));
            }
            committed.push(OriginalPostActionCommit::Member(Arc::clone(handle)));
        }
        let observed = carrier.original_request().tool_progress.original_conclusion_entries()?;
        if observed.len() != consumed_conclusions.len() || observed.iter().any(|(index, result)|
            !consumed_conclusions.iter().any(|(seen_index, seen)| index == seen_index && Arc::ptr_eq(result, seen))) {
            return Err("original conclusion is pending its matching member result".into());
        }
    }
    committed.sort_by_key(|handle| handle.receipt().commit_cursor);
    committed.dedup_by(|a, b| a.belongs_to(services.event_store())
        && b.belongs_to(services.event_store()) && a.receipt() == b.receipt());
    let goal_stream = format!("goal:{}", before.goal.id);
    let binding = before.goal.execution_binding.as_ref();
    let mut source = before.clone();
    let mut program_source: Option<(String, u64)> = None;
    // The highest Program revision this node's own certified commits produced.
    // `program_source` only ever holds the last handle's cut, so a node whose
    // actions advanced the Program more than once would certify against one
    // commit while its other effects came from a later one. The certification
    // window is the max over this node's own commits.
    let mut own_program_cut: Option<u64> = None;
    let mut changed = false;
    let mut authenticated_goal_reviews = std::collections::BTreeMap::new();
    // Every committed action of this node carries the original owner's
    // Program identity. It is what a Program advance in the gap between two
    // members is compared against.
    let mut program_effects = binding.map(|binding| {
        let program_stream = format!("agentic-program:{}", binding.agentic_program_id);
        committed.iter().filter_map(|handle| original_applied_effect(handle, &program_stream)).collect::<Vec<_>>()
    }).unwrap_or_default();
    for handle in &committed {
        if !handle.belongs_to(services.event_store()) { return Err("original committed store differs".into()); }
        let observation = handle.observation();
        if !matches!(handle, OriginalPostActionCommit::Conclusion(_))
            && observation.is_none_or(|value| value.status != harness_contract::agent_action::AgentActionStatus::Applied) {
            return Err("original committed action observation unavailable or not applied".into());
        }
        let transaction = handle.request().map(|request| &request.transaction);
        let receipt = handle.receipt();
        if transaction.is_some_and(|transaction| receipt.transaction_id != transaction.transaction_id) { return Err("original checked transaction differs".into()); }
        let replay_goal = match handle { OriginalPostActionCommit::Replay(replay) => replay.goal_event(), _ => None };
        let Some(binding) = binding else {
            if replay_goal.is_some() || transaction.is_some_and(|transaction| transaction.events.iter().any(|event| event.event.stream_id == goal_stream)) {
                return Err("original Goal Program binding missing".into());
            }
            continue;
        };
        if let Some(observation) = observation {
            if observation.program_id != binding.agentic_program_id {
                return Err("original committed Program differs".into());
            }
        }
        let program_stream = format!("agentic-program:{}", binding.agentic_program_id);
        if let Some(cut) = receipt.stream_revisions.iter().find(|cut| cut.stream_id == program_stream && cut.committed_revision > cut.expected_revision) {
            // The Program revision this node's own certified actions produced.
            // Who else advanced the Program in between is not this node's
            // question: that fact is already durable in the events and their
            // receipts, and re-deriving it here only produced a second
            // authority that every legitimate concurrency looked like an
            // anomaly.
            program_source = Some((program_stream.clone(), cut.committed_revision));
            own_program_cut = Some(own_program_cut.map_or(cut.committed_revision, |previous| previous.max(cut.committed_revision)));
        }
        let goal_payload = if let Some(transaction) = transaction {
            let events = transaction.events.iter().enumerate().filter(|(_, event)| event.event.stream_id == goal_stream).collect::<Vec<_>>();
            if events.is_empty() { continue; }
            let [(index, event)] = events.as_slice() else { return Err("original Goal action is ambiguous".into()) };
            if receipt.event_ids.get(*index).is_none() { return Err("original Goal receipt position missing".into()); }
            &event.event.payload
        } else if let Some(event) = replay_goal {
            if event.stream_id != goal_stream { return Err("original replay Goal differs".into()); }
            &event.payload
        } else { continue };
        let cut = receipt.stream_revisions.iter().find(|cut| cut.stream_id == goal_stream).ok_or("original committed Goal source missing")?;
        if cut.expected_revision != source.stream_revision || cut.committed_revision != source.stream_revision.checked_add(1).ok_or("Goal revision overflow")? { return Err("Goal changed outside the original actions".into()); }
        let next: harness_contract::goal::GoalContract = serde_json::from_value(goal_payload["goal"].clone())
            .map_err(|error| format!("original committed Goal is unavailable: {error}"))?;
        if next.id != source.goal.id || next.session_id != source.goal.session_id
            || next.execution_binding != source.goal.execution_binding { return Err("original committed Goal identity differs".into()); }
        if let Some(transaction) = transaction {
            if let Some((effect, program_payload, goal_payload)) = original_goal_review_transaction(
                transaction, receipt, &program_stream, &goal_stream, &source.goal, &next)? {
                authenticated_goal_reviews.insert(effect, (program_payload, goal_payload, source.goal.clone()));
            }
        }
        crate::execution_core::goal::GoalProgressReducer::reconcile_goal(&mut source.progress, &source.goal, &next);
        source.goal = next;
        source.stream_revision = cut.committed_revision;
        changed = true;
    }
    if changed && program_source.is_none() { return Err("original committed Program source missing".into()); }
    if let Some((stream, revision)) = program_source.as_ref() {
        let program_id = binding.map(|binding| binding.agentic_program_id.as_str())
            .ok_or("original committed Program binding missing")?;
        // The node writes the Program back under the live head, so the fence is
        // unchanged and a Program that moves again before that write is still
        // rejected by the node transaction's own CAS. The certification is a
        // different question from the fence: it asks whether this node's own
        // certified effects are visible, and that is only ever decidable at the
        // cut this node produced. An unrelated writer that did not carry this
        // node's work forward still fails the check, because the check reads
        // the projection through this node's own commits.
        let head = services.event_store().stream_revision(stream).map_err(|error| error.to_string())?;
        let certified_cut = original_program_certification_cut(
            own_program_cut.unwrap_or(*revision),
            head,
        )?;
        // Goal-owned reviews are proved by their own same-transaction Goal
        // record above, not by inventing a Program entity for them. Every
        // other effect remains subject to the Program projection check.
        for (program_payload, goal_payload, original_goal) in authenticated_goal_reviews.values() {
            if !original_goal_review_carried(program_payload, goal_payload, original_goal, &source.goal) {
                return Err("original Objective review is not carried by the final Goal".into());
            }
        }
        program_effects.retain(|(effect, kind)| !(kind == "objective_review" && authenticated_goal_reviews.contains_key(effect)));
        #[cfg(all(feature = "test-fixtures", target_os = "linux"))]
        if program_effects.iter().any(|(_, kind)| kind == "objective_review") {
            if let Err(error) = a9_goal_carrier_diagnostic(ticket, &before.goal.id, stream, &committed, &program_effects) {
                eprintln!("A9_GOAL_CARRIER_PRIVATE unavailable={error}");
            }
        }
        certify_original_program_head(&services, program_id, certified_cut, head, &program_effects)?;
        program_source = Some((stream.clone(), head));
    }
    // This fixture-only read marks the exact same-call post-action boundary.
    // Its result is not a Goal source and never changes the production CAS.
    #[cfg(feature = "test-fixtures")]
    if changed {
        let store = services.event_store();
        store.fixture_scope_original_goal_read(ticket, &goal_stream,
            || store.stream_revision(&goal_stream)).map_err(|error| error.to_string())?;
    }
    Ok(changed.then_some((source, program_source)))
}

// Keep the saved projection, observation and outcome construction out of the
// tool batch future: every tool batch carries that future even when it is not
// a completion request. This does not change the lifetime or authority of the
// original physical backend.
#[inline(never)]
fn remember_tool_goal_revalidation<C: ApiClient, T: ToolExecutor>(
    backend: &TurnToolBatchBackend<C, T>,
    ticket: &NodeExecutionTicket,
    source: &crate::execution_core::goal::GoalProjection,
    observation: &RuntimeObservation,
    checkpoint: GoalWriteCheckpoint,
    original_satisfied: bool,
    outcome: &NodeExecutionOutcome,
) {
    *backend.goal_revalidation.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ToolGoalRevalidation {
        ticket: ticket.clone(), source: source.clone(), observation: observation.clone(),
        checkpoint, original_satisfied, outcome: outcome.clone(),
    });
}

pub(super) struct HostRuntimeStrategyGate<'a, C: ApiClient, T: ToolExecutor> {
    pub(super) runtime: &'a Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    pub(super) prompter: &'a crate::permissions::SharedPrompter,
}

impl<C, T> HostOriginalStrategyGate for HostRuntimeStrategyGate<'_, C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn check<'a>(&'a self, task: &'a crate::GovernedToolPlanTask,
        plan: &'a crate::GovernedToolPlan,
        decision: &'a crate::execution_core::RuntimeExecutionDecision,
        ticket: &'a NodeExecutionTicket,
    ) -> crate::GovernedToolFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.runtime.lock().await.satisfy_original_call_strategy_gate(
                task, plan, decision, self.prompter, Some(ticket), None,
            ).await
        })
    }
}

/// Pin the exact graph cut and each sibling's original durable closure.
/// A missing closure or a moved physical source remains unobserved.
fn original_sibling_closure_cut(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
) -> Result<Option<BTreeMap<String, u64>>, String> {
    let store = services.graph_state_store();
    let events = services.event_store();
    let current = store.load(&ticket.graph_id).map_err(|error| error.to_string())?;
    let cut = store.load_graph_at_revision(&ticket.graph_id, current.revision)
        .map_err(|error| error.to_string())?;
    if cut != current || cut.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running)
        || cut.recovery_cursor.node_attempts.get(&ticket.node_id) != Some(&ticket.attempt) {
        return Ok(None);
    }
    let mut sources = BTreeMap::from([(cut.id.clone(), cut.revision)]);
    let mut cursor = 0;
    let mut original_found = false;
    while cursor < cut.revision {
        let (next, starts) = store.binding_starts_page(&cut.id, cursor, cut.revision)
            .map_err(|error| error.to_string())?;
        if next <= cursor || next > cut.revision { return Ok(None); }
        cursor = next;
        for (node, binding, start) in starts {
            if node == ticket.node_id && binding.attempt == ticket.attempt {
                if original_found { return Ok(None); }
                let (original, _) = store.binding_start_ticket(&cut, &node, &binding)
                    .map_err(|error| error.to_string())?;
                if original != *ticket { return Ok(None); }
                original_found = true;
                continue;
            }
            let key = binding.lease_closure_key(&cut, &node);
            let Some(record) = events.event_by_idempotency_key(&cut.id, &key)
                .map_err(|error| error.to_string())? else { return Ok(None) };
            if record.sequence > cut.revision || record.transaction_id != key
                || (record.commit_cursor, record.transaction_index) <= (start.1, start.2) {
                return Ok(None);
            }
            let resolved = events.resolve_read_record(record.clone()).map_err(|error| error.to_string())?;
            if !binding.validate_lease_closure(&cut, &node, (start.1, start.2), &resolved)
                .map_err(|error| error.to_string())?
                || !store.binding_lease_closure(&cut, &node, &binding, start)
                    .map_err(|error| error.to_string())? { return Ok(None); }
            let Some(receipt) = events.original_transaction_receipt(&record.transaction_id)
                .map_err(|error| error.to_string())? else { return Ok(None) };
            if receipt.transaction_id != record.transaction_id || receipt.commit_cursor != record.commit_cursor
                || receipt.event_ids.get(record.transaction_index as usize) != Some(&record.event_id)
                || receipt.event_ids.iter().filter(|id| *id == &record.event_id).count() != 1 {
                return Ok(None);
            }
            let mut graph_source = false;
            for source in &receipt.stream_revisions {
                if source.stream_id == cut.id {
                    if graph_source || source.expected_revision.checked_add(1) != Some(record.sequence)
                        || source.committed_revision != record.sequence { return Ok(None); }
                    graph_source = true;
                    continue;
                }
                if source.expected_revision != source.committed_revision { return Ok(None); }
                let head = events.stream_revision(&source.stream_id).map_err(|error| error.to_string())?;
                if head < source.committed_revision
                    || sources.insert(source.stream_id.clone(), head).is_some_and(|old| old != head) {
                    return Ok(None);
                }
            }
            if !graph_source { return Ok(None); }
        }
    }
    Ok(original_found.then_some(sources))
}

fn original_host_carrier_matches_ticket(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    call_id: &str,
    pending: &crate::PendingAgentBatch,
) -> bool {
    let Some((original_ticket, original_calls)) = pending.original_host_call_set() else {
        return false;
    };
    if original_ticket != ticket || !crate::host::decode_tool_batch(&ticket.payload_ref).ok()
        .is_some_and(|(calls, _)| calls == original_calls) {
        return false;
    }
    match pending.value() {
        crate::PendingAgentBatchValue::OriginalCall(carrier) => {
            let request = carrier.original_request();
            carrier.original_host_call_set().is_some_and(|(owner, calls)| owner == ticket && calls == original_calls)
                && request.tool_use_id == call_id
                && crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, request).is_ok()
        },
        crate::PendingAgentBatchValue::OriginalPacketScope(original) => original.matches(ticket, services.event_store(), call_id),
        crate::PendingAgentBatchValue::OriginalBoundedAcquisition(original) => original.matches(ticket, services.event_store(), call_id),
        crate::PendingAgentBatchValue::OriginalReplay(original) => original.replay.ticket() == ticket && original.replay.call_id() == call_id,
        crate::PendingAgentBatchValue::WithCause { primary, .. } => {
            let crate::PendingAgentBatchValue::OriginalCall(carrier) = primary.primary_value() else { return false };
            let same_cause = if let Some(boundary) = pending.original_terminal_boundary_ref() {
                pending.same_original_terminal_carrier(carrier.original_request(), boundary)
            } else if let Some(publication) = pending.original_stop_publication() {
                carrier.original_request().tool_progress.checked_original_stop_publications().ok()
                    .is_some_and(|publications| publications.len() == 1 && Arc::ptr_eq(&publications[0], publication))
                    && pending.same_original_stop_carrier(carrier.original_request(), publication)
            } else { false };
            carrier.original_host_call_set().is_some_and(|(owner, calls)| owner == ticket && calls == original_calls)
                && carrier.original_request().tool_use_id == call_id
                && same_cause
                && crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, carrier.original_request()).is_ok()
        },
        _ => false,
    }
}

fn original_pending_is_released(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    observation: &Arc<crate::execution_core::graph::executors::agent_tool::NoDispatchObservation>,
    call_id: &str,
    pending: &crate::PendingAgentBatch,
) -> Result<bool, String> {
    if !original_host_carrier_matches_ticket(services, ticket, call_id, pending)
        || !observation.ticket_matches(ticket) || observation.member_custody_is_unresolved() {
        return Ok(false);
    }
    let commits = services.commit_service();
    match pending.value() {
        crate::PendingAgentBatchValue::OriginalCall(_) =>
            original_host_carrier_is_released(services, ticket, observation, call_id, pending),
        crate::PendingAgentBatchValue::OriginalPacketScope(original) => {
            let proof = commits.original_agent_packet_scope_refusal(ticket, original.request(), original.fact())
                .map_err(|error| error.to_string())?;
            Ok(original.receipt().is_none_or(|receipt|
                receipt.event_id() == proof.event_id() && receipt.transaction_id() == proof.transaction_id()))
        },
        crate::PendingAgentBatchValue::OriginalBoundedAcquisition(original) => {
            let proof = commits.original_bounded_acquisition_failure(ticket, original.fact())
                .map_err(|error| error.to_string())?;
            Ok(original.receipt().is_none_or(|receipt|
                receipt.event_id() == proof.event_id() && receipt.transaction_id() == proof.transaction_id()))
        },
        crate::PendingAgentBatchValue::OriginalReplay(original) => {
            if original.complete_request.is_some() {
                return original_complete_replay_is_committed(services, ticket, call_id, original);
            }
            commits.original_replayed_action_after_commit(ticket, &original.replay)
                .map_err(|error| error.to_string())?;
            let Some(physical) = original.physical_receipt.as_ref() else { return Ok(false) };
            let Some(key) = physical.idempotency_key.as_deref() else { return Ok(false) };
            let Some(event) = services.event_store().event_by_idempotency_key(&physical.stream_id, key)
                .map_err(|error| error.to_string())? else { return Ok(false) };
            let Some(receipt) = services.event_store().original_transaction_receipt(&event.transaction_id)
                .map_err(|error| error.to_string())? else { return Ok(false) };
            Ok(event.event_id == physical.event_id && event.transaction_id == physical.transaction_id
                && event.commit_cursor == physical.commit_cursor && event.payload == physical.payload
                && receipt.transaction_id == event.transaction_id && receipt.commit_cursor == event.commit_cursor
                && receipt.event_ids.get(event.transaction_index as usize) == Some(&event.event_id))
        },
        crate::PendingAgentBatchValue::WithCause { primary, .. } => {
            let _ = primary;
            original_host_carrier_is_released(services, ticket, observation, call_id, pending)
        },
        _ => Ok(false),
    }
}

/// Authenticate the complete replay against its original semantic and
/// physical transactions; it is not the partial-replay node event.
fn original_complete_replay_is_committed(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    call_id: &str,
    original: &crate::conversation::OriginalReplayedPartial,
) -> Result<bool, String> {
    let (Some(request), Some(physical), Some(observation)) = (
        original.complete_request.as_ref(), original.physical_receipt.as_ref(), original.replay.observation(),
    ) else { return Ok(false) };
    if !original.replay.belongs_to(services.event_store()) || original.replay.ticket() != ticket
        || original.replay.call_id() != call_id || request.tool_use_id != call_id
        || crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, request).is_err()
    { return Ok(false); }
    let semantic = original.replay.program_event();
    let original_receipt = original.replay.receipt();
    let Some(semantic_key) = semantic.idempotency_key.as_deref() else { return Ok(false) };
    let Some(actual_semantic) = services.event_store().event_by_idempotency_key(&semantic.stream_id, semantic_key)
        .map_err(|error| error.to_string())? else { return Ok(false) };
    let Some(semantic_receipt) = services.event_store().original_transaction_receipt(&semantic.transaction_id)
        .map_err(|error| error.to_string())? else { return Ok(false) };
    if actual_semantic != *semantic || semantic_receipt.transaction_id != original_receipt.transaction_id
        || semantic_receipt.request_hash != original_receipt.request_hash
        || semantic_receipt.commit_cursor != original_receipt.commit_cursor
        || semantic_receipt.event_ids != original_receipt.event_ids
        || semantic_receipt.stream_revisions != original_receipt.stream_revisions
        || semantic_receipt.event_ids.get(semantic.transaction_index as usize) != Some(&semantic.event_id)
        || observation.action_id.is_empty() { return Ok(false); }
    let Some(effect) = request.compiled_invocation.as_ref().map(|invocation| &invocation.effect) else { return Ok(false) };
    let Some(outcome) = services.commit_service().completed_tool_effect(request, effect)
        .map_err(|error| error.to_string())? else { return Ok(false) };
    let Some(physical_key) = physical.idempotency_key.as_deref() else { return Ok(false) };
    let Some(actual_physical) = services.event_store().event_by_idempotency_key(&physical.stream_id, physical_key)
        .map_err(|error| error.to_string())? else { return Ok(false) };
    let Some(receipt) = services.event_store().original_transaction_receipt(&physical.transaction_id)
        .map_err(|error| error.to_string())? else { return Ok(false) };
    Ok(actual_physical == **physical && receipt.transaction_id == physical.transaction_id
        && receipt.commit_cursor == physical.commit_cursor
        && receipt.event_ids.get(physical.transaction_index as usize) == Some(&physical.event_id)
        && serde_json::to_value(&outcome).map_err(|error| error.to_string())? == physical.payload["outcome"]
        && (physical.commit_cursor, physical.transaction_index) > (semantic.commit_cursor, semantic.transaction_index))
}

/// Verify the original read and its transaction without reconstructing policy.
fn original_read_is_committed(
    services: &crate::RuntimeServices, ticket: &NodeExecutionTicket,
    request: &crate::RuntimeToolExecutionRequest,
) -> Result<bool, String> {
    services.commit_service().original_read_is_committed(ticket, request)
}

/// Only the original joined worker can attest its pre-dispatch return.
/// Previously acknowledged authorization writes remain separately durable.
pub(super) fn original_early_deferral_sources(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    call_id: &str,
    deferred: &crate::conversation::EarlyToolDeferral,
) -> Result<Option<BTreeMap<String, u64>>, String> {
    let Some(original) = deferred.original_undispatched.as_ref() else { return Ok(None) };
    if original.ticket != *ticket || original.call_id != call_id || deferred.tool_call_id != call_id
        || original.identity.tool_call_id.as_deref() != Some(call_id) {
        return Ok(None);
    }
    let mut seen = BTreeSet::new();
    let mut sources = BTreeMap::new();
    for event in &original.authorization_events {
        let Some(key) = event.idempotency_key.as_deref() else { return Ok(None) };
        if !seen.insert((event.stream_id.clone(), key.to_string()))
            || event.kind != "authorization.lease_transition"
            || event.scope != crate::RuntimeEventScope::Tool || event.schema_version != 1
            || event.actor.as_deref() != Some("conversation_runtime.early_tool")
            || event.transaction_id != format!("authorization-transition:{key}")
            || !event.stream_id.starts_with("authorization-lease:") {
            return Ok(None);
        }
        let Some(actual) = services.event_store().event_by_idempotency_key(&event.stream_id, key)
            .map_err(|error| error.to_string())? else { return Ok(None) };
        let Some(receipt) = services.event_store().original_transaction_receipt(&event.transaction_id)
            .map_err(|error| error.to_string())? else { return Ok(None) };
        if actual != *event || receipt.transaction_id != event.transaction_id
            || receipt.commit_cursor != event.commit_cursor
            || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
            || receipt.event_ids.iter().filter(|id| *id == &event.event_id).count() != 1
            || !receipt.stream_revisions.iter().any(|source| source.stream_id == event.stream_id
                && source.expected_revision < event.sequence && event.sequence <= source.committed_revision)
        { return Ok(None); }
        let head = services.event_store().stream_revision(&event.stream_id).map_err(|error| error.to_string())?;
        if sources.insert(event.stream_id.clone(), head).is_some_and(|old| old != head) { return Ok(None); }
    }
    Ok(Some(sources))
}

fn original_early_read_is_committed(
    services: &crate::RuntimeServices, ticket: &NodeExecutionTicket,
    early: &crate::conversation::EarlyToolExecutionReceipt,
) -> Result<bool, String> {
    services.commit_service().original_early_read_is_committed(ticket, early)
}

/// Verify every early read and the complete original frame, without treating
/// an InlineModel read as a ToolBatch observation.
fn verify_original_early_read_set(
    services: &crate::RuntimeServices,
    owner_graph_id: &str,
    early: &[crate::conversation::EarlyToolExecutionReceipt],
    pending: &BTreeMap<(String, u32), Vec<(String, crate::PendingAgentBatch)>>,
) -> Result<bool, String> {
    if early.is_empty() { return Ok(true); }
    let mut groups = BTreeMap::<(String, String, u32), Vec<crate::conversation::EarlyToolExecutionReceipt>>::new();
    for item in early {
        let Some(source) = item.original_source.as_ref() else { return Ok(false) };
        if source.ticket.graph_id != owner_graph_id || source.ticket.executor_kind != "inline_model" || source.completed_calls.is_none() {
            return Ok(false);
        }
        groups.entry((source.ticket.graph_id.clone(), source.ticket.node_id.clone(), source.ticket.attempt))
            .or_default().push(item.clone());
    }
    for ((graph_id, node_id, attempt), items) in groups {
        let mut unique = BTreeSet::new();
        let expected = items[0].original_source.as_ref().and_then(|source| source.completed_calls.as_ref());
        if expected.is_none() || items.iter().any(|item| {
            !unique.insert(item.call.id.clone())
                || item.original_source.as_ref().and_then(|source| source.completed_calls.as_ref()) != expected
        }) { return Ok(false); }
        let Some(expected) = expected else { return Ok(false) };
        for calls in pending.values() {
            for (call_id, original) in calls {
                if original.original_host_call_set().is_some_and(|(_, calls)| calls == expected) {
                    unique.insert(call_id.clone());
                }
            }
        }
        if expected.len() != unique.len() || expected.iter().any(|call| !unique.contains(&call.id)) {
            return Ok(false);
        }
        for item in &items {
            let Some(source) = item.original_source.as_ref() else { return Ok(false) };
            if source.ticket.graph_id != graph_id || source.ticket.node_id != node_id
                || source.ticket.attempt != attempt
                || !original_early_read_is_committed(services, &source.ticket, item)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Re-read one original Host release from its node transaction and live proof.
/// Missing or mismatched facts remain unknown and cannot authorize a retry.
fn original_host_carrier_is_released(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    observation: &Arc<crate::execution_core::graph::executors::agent_tool::NoDispatchObservation>,
    call_id: &str,
    pending: &crate::PendingAgentBatch,
) -> Result<bool, String> {
    let crate::PendingAgentBatchValue::OriginalCall(carrier) = pending.primary_value() else {
        return Ok(false);
    };
    let request = carrier.original_request();
    if !original_host_carrier_matches_ticket(services, ticket, call_id, pending) {
        return Ok(false);
    }
    let commits = services.commit_service();
    if request.tool_name != "agent_action_batch" {
        if carrier.has_unsettled_host_members() || !carrier.outcomes().is_empty()
            || pending.original_stop_publication().is_some()
            || request.tool_progress.original_fresh_effect()?.is_some()
        {
            return Ok(false);
        }
        return original_read_is_committed(services, ticket, request);
    }
    let outers = commits.original_batch_outers_after_commit(ticket).map_err(|error| error.to_string())?;
    if outers.iter().filter(|outer| *outer == &request.idempotency_key).count() != 1 {
        return Ok(false);
    }
    let facts = commits.original_batch_members_after_commit(ticket, &request.idempotency_key)
        .map_err(|error| error.to_string())?.into_iter()
        .map(|fact| fact.payload().clone()).collect::<Vec<_>>();
    let pending_fact = commits.original_batch_pending_after_commit(ticket, &request.idempotency_key)
        .map_err(|error| error.to_string())?.map(|fact| fact.payload()["pending"].clone());
    let proof = carrier.host_release_slot().lock()
        .map_err(|_| "original Host release proof unavailable".to_string())?.clone();
    let Some(proof) = proof else { return Ok(false) };
    let publications = request.tool_progress.checked_original_stop_publications()?;
    let same_publication = match &proof.publication {
        None => publications.is_empty(),
        Some(original) => publications.len() == 1 && Arc::ptr_eq(original, &publications[0]),
    };
    if proof.ticket != *ticket || !proof.owner.matches_original(services.event_store(), request)
        || proof.facts != facts || proof.pending != pending_fact
        || !same_publication
        || !std::sync::Weak::ptr_eq(&proof.observation, &Arc::downgrade(observation))
        || !observation.ticket_matches(ticket)
        || carrier.has_unsettled_host_members()
    {
        return Ok(false);
    }
    let units = carrier.verified_host_settlements(ticket, services.event_store(), &facts, pending_fact.as_ref())?;
    if units.len() != proof.members.len() || units.iter().zip(&proof.members).any(|(unit, original)| {
        !std::sync::Weak::ptr_eq(original, &Arc::downgrade(unit)) || !unit.is_settled()
    }) {
        return Ok(false);
    }
    let stream = format!("execution-effect:{}", request.idempotency_key);
    let key = format!("{}:receipt", request.idempotency_key);
    let Some(event) = services.event_store().event_by_idempotency_key(&stream, &key)
        .map_err(|error| error.to_string())? else { return Ok(false) };
    let Some(receipt) = services.event_store().original_transaction_receipt(&event.transaction_id)
        .map_err(|error| error.to_string())? else { return Ok(false) };
    Ok(event.kind == "execution.effect.receipt"
        && event.scope == crate::RuntimeEventScope::ExecutionNode
        && event.actor.as_deref() == Some("governed_tool")
        && event.status.as_deref() == Some("completed")
        && event.schema_version == 1
        && matches!(event.payload["outcome"]["status"].as_str(), Some("executed" | "failed" | "blocked_permission"))
        && event.payload["replay"] == crate::execution_core::graph::receipt_replay_contract(request)
            .map_err(|error| error.to_string())?
        && event.transaction_id == proof.outer_receipt.transaction_id
        && event.commit_cursor == proof.outer_receipt.commit_cursor
        && receipt.transaction_id == proof.outer_receipt.transaction_id
        && receipt.request_hash == proof.outer_receipt.request_hash
        && receipt.commit_cursor == proof.outer_receipt.commit_cursor
        && receipt.event_ids == proof.outer_receipt.event_ids
        && receipt.stream_revisions == proof.outer_receipt.stream_revisions
        && receipt.event_ids.get(event.transaction_index as usize) == Some(&event.event_id))
}

/// Ask the original active owner to settle, then verify every captured call.
async fn settle_original_host_carriers(
    services: &Arc<crate::RuntimeServices>,
    state: &Arc<tokio::sync::Mutex<TurnGraphState>>,
    graph_id: &str,
    early: &[crate::conversation::EarlyToolExecutionReceipt],
) -> Result<bool, String> {
    let originals = state.lock().await.pending_member_batches.clone();
    if originals.is_empty() {
        return Ok(true);
    }
    for ((node_id, attempt), calls) in originals {
        let mut unique = BTreeSet::new();
        if calls.is_empty() || calls.iter().any(|(id, _)| id.is_empty()) {
            return Ok(false);
        }
        for (id, _) in &calls { unique.insert(id.clone()); }
        let verified = services.execution_supervisor().settle_retained_original_callback(
            graph_id, &node_id, attempt,
            |ticket| {
                let mut captured_outers = BTreeSet::new();
                let mut original_calls_by_id = BTreeMap::new();
                for (id, pending) in &calls {
                    if !original_host_carrier_matches_ticket(services, ticket, id, pending) { return Ok(false); }
                    let Some((_, original_calls)) = pending.original_host_call_set() else { return Ok(false) };
                    let original_ids = original_calls.iter().map(|call| call.id.clone()).collect::<BTreeSet<_>>();
                    let mut covered = unique.clone();
                    for receipt in early {
                        let Some(source) = receipt.original_source.as_ref() else { return Ok(false) };
                        if source.completed_calls.as_deref() == Some(original_calls) {
                            if !covered.insert(receipt.call.id.clone()) { return Ok(false); }
                        }
                    }
                    if original_ids.len() != original_calls.len() || original_ids != covered {
                        return Ok(false);
                    }
                    if let crate::PendingAgentBatchValue::OriginalCall(carrier) = pending.primary_value() {
                        if let Some(previous) = original_calls_by_id.insert(id, carrier.original_request()) {
                            if previous != carrier.original_request()
                                || !previous.tool_progress.same_original_channels(&carrier.original_request().tool_progress)
                            { return Ok(false); }
                            continue;
                        }
                        if carrier.original_request().tool_name != "agent_action_batch" { continue; }
                        if !captured_outers.insert(carrier.original_request().idempotency_key.clone()) {
                            return Ok(false);
                        }
                    } else if calls.iter().filter(|(other, _)| other == id).count() != 1 {
                        return Ok(false);
                    }
                }
                let durable = services.commit_service().original_batch_outers_after_commit(ticket)
                    .map_err(|error| error.to_string())?;
                Ok(durable.len() == captured_outers.len()
                    && durable.iter().all(|outer| captured_outers.contains(outer)))
            },
            |ticket, observation| {
                let Some(observation) = observation else { return Ok(false) };
                for id in &unique {
                    let mut released = false;
                    for (_, pending) in calls.iter().filter(|(candidate, _)| candidate == id) {
                        if original_pending_is_released(services, ticket, observation, id, pending)? {
                            released = true;
                            break;
                        }
                    }
                    if !released {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        ).await?;
        if verified.is_none() || state.lock().await.pending_member_batches.contains_key(&(node_id, attempt)) {
            return Ok(false);
        }
    }
    Ok(state.lock().await.pending_member_batches.is_empty())
}

/// Retain the same live call carriers only for this turn. This map is not
/// authority and cannot establish that the node transaction committed.
pub(crate) async fn retain_original_member_batches(
    state: &Arc<tokio::sync::Mutex<TurnGraphState>>,
    ticket: &NodeExecutionTicket,
    batches: &[(String, crate::PendingAgentBatch)],
) -> Result<(), NodeExecutorError> {
    if batches.is_empty() { return Ok(()); }
    let mut state = state.lock().await;
    let key = (ticket.node_id.clone(), ticket.attempt);
    let retained = state.pending_member_batches.entry(key).or_default();
    for (call_id, pending) in batches {
        // Arc equality here only avoids retaining the exact same live object
        // twice. It is never used for authorization or fact comparison.
        if !retained.iter().any(|(old_call, old)| old_call == call_id && old == pending
            && old.original_host_call_set() == pending.original_host_call_set()) {
            retained.push((call_id.clone(), pending.clone()));
        }
    }
    Ok(())
}

/// Select only original, contiguous committed members for the node owner.
/// Unknown and unbound submissions never become committed members.
fn original_member_prefixes(
    calls: &[ModelToolCall],
    batches: &[(String, crate::PendingAgentBatch)],
) -> Result<Vec<(String, std::sync::Arc<crate::tool_host::AuthenticatedBatchPrefix>)>, String> {
    let mut prefixes = Vec::new();
    for (call_id, pending) in batches {
        let mut matching = calls.iter().filter(|call| &call.id == call_id);
        let call = matching.next().ok_or("member carrier call missing")?;
        if matching.next().is_some() { return Err("member carrier call ambiguous".into()); }
        if matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalReplay(_) | crate::PendingAgentBatchValue::OriginalPacketScope(_) | crate::PendingAgentBatchValue::OriginalBoundedAcquisition(_)) { continue; }
        let binding = call.binding.as_ref().ok_or("member carrier original Step missing")?;
        let outer = binding.effect_idempotency_key();
        let mut prefix = crate::tool_host::AuthenticatedBatchPrefix::new();
        let members = match pending.primary_value() {
            crate::PendingAgentBatchValue::Recorded(members) => members.clone(),
            crate::PendingAgentBatchValue::RecordedSink(sink) => sink.original_members(),
            crate::PendingAgentBatchValue::OriginalCall(carrier) => carrier.outcomes().to_vec(),
            _ => return Err("member carrier representation not yet connected".into()),
        };
        {
                for (index, result) in &members {
                    let crate::tool_host::OriginalMemberSubmitOutcome::Committed(member) = result.as_ref() else { break };
                    if *index != member.index() { return Err("member carrier index mismatch".into()); }
                    let action = member.action();
                    let member_outer = action.payload().pointer("/member_admission/prepared/outer_key")
                        .and_then(serde_json::Value::as_str).ok_or("member original outer missing")?;
                    if member_outer != outer { return Err("member original outer mismatch".into()); }
                    prefix.push(member.clone()).map_err(|_| "member carrier order mismatch")?;
                }
            }
        // A checked member with no observation arrives in the private stop,
        // not in the successful outcome vector. Include its original handle
        // only when the same live publication has no conflict; the node
        // commit owner performs the authoritative ticket/store verification.
        if let (Some(publication), crate::PendingAgentBatchValue::OriginalCall(carrier)) =
            (pending.original_stop_publication(), pending.primary_value())
        {
            let stop = publication.current();
            let coordinate = stop.coordinate();
            if publication.failure().is_none()
                && stop.belongs_to_original_sink(&carrier.original_request().tool_progress)
                && coordinate.call_id() == call_id
                && coordinate.outer_key() == outer
                && coordinate.step() == Some(binding)
            {
                if let crate::tool_host::OriginalBatchMemberStopReason::CommittedObservationUnavailable(member) = stop.reason() {
                    if member.index() != coordinate.index() || prefix.members().len() != member.index() {
                        return Err("committed stop prefix order mismatch".into());
                    }
                    prefix.push(member.clone()).map_err(|_| "committed stop prefix mismatch")?;
                }
            }
        }
        if !prefix.members().is_empty() { prefixes.push((call_id.clone(), std::sync::Arc::new(prefix))); }
    }
    Ok(prefixes)
}

fn attach_original_member_prefixes(
    services: &crate::RuntimeServices,
    outcome: &mut NodeExecutionOutcome,
    prefixes: &[(String, std::sync::Arc<crate::tool_host::AuthenticatedBatchPrefix>)],
    batches: &[(String, crate::PendingAgentBatch)],
) -> Result<(), String> {
    for (_, prefix) in prefixes {
        services.commit_service().attach_original_batch_members(outcome, std::sync::Arc::clone(prefix))
            .map_err(|error| error.to_string())?;
    }
    for (call_id, pending) in batches {
        if matches!(pending.value(), crate::PendingAgentBatchValue::WithCause { .. })
            && matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalReplay(_)
                | crate::PendingAgentBatchValue::OriginalPacketScope(_)
                | crate::PendingAgentBatchValue::OriginalBoundedAcquisition(_)) {
            return Err("original call has an independent unresolved cause".into());
        }
        if matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalPacketScope(_) | crate::PendingAgentBatchValue::OriginalBoundedAcquisition(_)) { continue; }
        if let crate::PendingAgentBatchValue::OriginalReplay(original) = pending.primary_value() {
            if original.replay.call_id() != call_id { return Err("original replay call differs".into()); }
            if original.complete_request.is_some() { continue; }
            services.commit_service().attach_original_replayed_action(outcome, Arc::clone(&original.replay))
                .map_err(|error| error.to_string())?;
            continue;
        }
        let crate::PendingAgentBatchValue::OriginalCall(carrier) = pending.primary_value() else {
            return Err("original member call carrier unavailable".into());
        };
        if carrier.original_request().tool_use_id != *call_id {
            return Err("original member call carrier mismatch".into());
        }
        // Multiple distinct direct publications cannot be compressed into
        // A's single-publication API. Keep the original carriers retained and
        // fail closed rather than dropping either one.
        if carrier.stop_publications().len() > 1 {
            return Err("multiple original stop publications require owner settlement".into());
        }
        let direct = pending.original_stop_publication().cloned();
        if let (Some(direct), Some(retained)) = (&direct, carrier.stop_publications().first()) {
            if !std::sync::Arc::ptr_eq(direct, retained) {
                return Err("different original stop publications require owner settlement".into());
            }
        }
        // Only the original commit owner can materialize this sealed carrier.
        services.commit_service().attach_original_batch_pending(
            outcome, std::sync::Arc::clone(carrier), direct.or_else(|| carrier.stop_publications().first().cloned()),
        );
    }
    Ok(())
}

/// Render only original owner-authenticated observations for an unresolved
/// batch. This is a partial tool result, never proof of Host delivery.
fn partial_member_messages(
    calls: &[ModelToolCall],
    messages: &mut Vec<ConversationMessage>,
    batches: &[(String, crate::PendingAgentBatch)],
    prefixes: &[(String, std::sync::Arc<crate::tool_host::AuthenticatedBatchPrefix>)],
) -> Result<(), String> {
    for (call_id, pending) in batches {
        if messages.iter().any(|message| message.blocks.iter().any(|block|
            matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == call_id))) {
            continue;
        }
        if matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalReplay(_) | crate::PendingAgentBatchValue::OriginalPacketScope(_) | crate::PendingAgentBatchValue::OriginalBoundedAcquisition(_)) { continue; }
        let crate::PendingAgentBatchValue::OriginalCall(carrier) = pending.primary_value() else {
            return Err("original member call carrier unavailable for partial message".into());
        };
        if carrier.original_request().tool_use_id != *call_id {
            return Err("original member partial call mismatch".into());
        }
        let prefix = prefixes.iter().find(|(id, _)| id == call_id).map(|(_, prefix)| prefix);
        let mut parts = Vec::new();
        for (index, result) in carrier.outcomes() {
            match result.as_ref() {
                crate::tool_host::OriginalMemberSubmitOutcome::Committed(member) => {
                    if prefix.and_then(|prefix| prefix.members().get(*index)).is_none() {
                        return Err("committed member missing from original node prefix".into());
                    }
                    let committed = match member.committed_fact().ok_or("original committed member fact missing")? {
                        crate::tool_host::OriginalMemberCommittedFact::Applied { .. } => "action committed",
                        crate::tool_host::OriginalMemberCommittedFact::Rejected { .. } => "rejection committed",
                    };
                    let observation = match member.observation() {
                        crate::tool_host::OriginalMemberObservation::Available(value) =>
                            format!("original observation: {}", serde_json::to_string(value).map_err(|error| error.to_string())?),
                        crate::tool_host::OriginalMemberObservation::Unavailable =>
                            "original observation unavailable".to_string(),
                    };
                    parts.push(format!("member {index}: {committed}; {observation}; Host delivery pending"));
                }
                crate::tool_host::OriginalMemberSubmitOutcome::CommittedUnbound(_) =>
                    parts.push(format!("member {index}: candidate original transaction committed; association with this member unverified")),
                crate::tool_host::OriginalMemberSubmitOutcome::NoCommittedReceipt(_) =>
                    parts.push(format!("member {index}: no commit receipt observed; outcome unknown")),
            }
        }
        if let Some(publication) = pending.original_stop_publication() {
            let stop = publication.current();
            let coordinate = stop.coordinate();
            if publication.failure().is_some() || !stop.belongs_to_original_sink(&carrier.original_request().tool_progress)
                || coordinate.call_id() != call_id {
                parts.push("Original stop publication is unresolved; no rollback is implied.".into());
            } else {
                let index = coordinate.index();
                let status = match stop.reason() {
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown(_) =>
                        "member outcome is unknown before a committed result",
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeError(_) =>
                        "member encountered an error before a committed result; rollback is unproven",
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeStoreError(_) =>
                        "member storage outcome is unknown; rollback is unproven",
                    crate::tool_host::OriginalBatchMemberStopReason::ConfirmedNoncommit(_) =>
                        "member noncommit is authenticated; node and physical closure remain pending",
                    crate::tool_host::OriginalBatchMemberStopReason::UnknownNoReceipt(_) =>
                        "no commit receipt was observed; outcome remains unknown",
                    crate::tool_host::OriginalBatchMemberStopReason::CommittedObservationUnavailable(_) =>
                        "action committed; original observation unavailable and Host delivery pending",
                    crate::tool_host::OriginalBatchMemberStopReason::CommittedUnbound(_) =>
                        "candidate original transaction committed; association with this member unverified",
                };
                let category = match stop.reason() {
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeError(error) => {
                        let store = error.fixed_store_kind().map(|kind| format!("/{kind}")).unwrap_or_default();
                        format!(" [{}{store}]", error.fixed_kind())
                    },
                    _ => String::new(),
                };
                parts.push(format!("member {index}: {status}{category}"));
            }
        }
        if parts.is_empty() { continue; }
        parts.push("Any unobserved later member remains unresolved; no rollback is implied.".into());
        messages.push(ConversationMessage::tool_result(
            call_id.clone(), "agent_action_batch", parts.join("\n"), true));
    }
    messages.sort_by_key(|message| message.blocks.iter().find_map(|block| match block {
        ContentBlock::ToolResult { tool_use_id, .. } => calls.iter().position(|call| &call.id == tool_use_id),
        _ => None,
    }).unwrap_or(usize::MAX));
    Ok(())
}

/// Commit the already-consumed siblings with the original node transition.
/// An unresolved effect remains Blocked; the transcript reference is evidence
/// of message publication only, never evidence that the effect settled.
async fn stage_uncertain_tool_batch<C, T>(
    runtime: &Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    services: &Arc<crate::RuntimeServices>,
    ticket: &NodeExecutionTicket,
    session_id: String,
    turn_id: String,
    calls: &[ModelToolCall],
    mut messages: Vec<ConversationMessage>,
    unresolved: Vec<crate::conversation::ToolBatchUnresolved>,
) -> Result<NodeExecutionOutcome, NodeExecutorError>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    let reason = serde_json::to_string(&unresolved).map_err(|error| NodeExecutorError::Uncertain {
        node_id: ticket.node_id.clone(),
        reason: format!("could not encode unsettled original calls: {error}"),
    })?;
    let mut outcome = NodeExecutionOutcome::new(harness_contract::execution_graph::ExecutionNodeResult {
        status: harness_contract::execution_graph::ExecutionNodeStatus::Blocked,
        result_ref: None,
        wait_class: None,
        summary: None,
        evidence_refs: Vec::new(),
        failure: Some(harness_contract::execution_graph::ExecutionFailure {
            kind: "effect_completion_uncertain".into(),
            message: reason.clone(),
            retryable: false,
            evidence_refs: Vec::new(),
        }),
        usage: Default::default(),
        finished_at_ms: crate::tool_invocation::now_ms(),
    });
    if runtime_authored_tool_batch(calls) {
        messages.insert(0, runtime_authored_tool_call_message(calls));
    }
    stage_host_transcript(runtime, services, ticket, session_id, turn_id, messages, None, &mut outcome)
        .await
        .map_err(|error| NodeExecutorError::Uncertain {
            node_id: ticket.node_id.clone(),
            reason: format!("unsettled calls {reason}; sibling transcript staging failed: {error}"),
        })?;
    Ok(outcome)
}

/// Recover only the exact original Host receipt after the semantic action
/// transaction and its turn owner have both been authenticated. No current
/// policy, hook, grant, actor or projected response is constructed here.
pub(crate) fn pregrant_original_action_receipt(
    issuer: &crate::conversation::tool_plane::OwnedRootActionIssuer,
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
    call: &ModelToolCall,
) -> Result<OriginalActionReceiptState, NodeExecutorError> {
    let unknown = |reason: String| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason };
    let Some(witness) = issuer.original_replay_witness(ticket, &call.id).map_err(|e| unknown(e.to_string()))? else {
        let binding = call.binding.as_ref().ok_or_else(|| unknown("original call Step missing".into()))?;
        let effect_key = binding.effect_idempotency_key();
        let stream = format!("execution-effect:{effect_key}");
        for suffix in ["intent", "read-intent", "receipt", "read-receipt"] {
            let key = format!("{effect_key}:{suffix}");
            if services.event_store().event_by_idempotency_key(&stream, &key)
                .map_err(|e| unknown(e.to_string()))?.is_some() {
                return Err(unknown("original effect evidence exists without an authenticated action transaction".into()));
            }
        }
        return Ok(OriginalActionReceiptState::Unobserved); // Not rollback or admission.
    };
    let original_action = Arc::new(services.verify_original_root_replay(&witness).map_err(unknown)?);
    let request = witness.original_request();
    crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, request)
        .map_err(|e| unknown(format!("original ToolBatch call proof unavailable: {e}")))?;
    let effect = &request.compiled_invocation.as_ref()
        .ok_or_else(|| unknown("original compiled invocation missing".into()))?.effect;
    let read = effect.effect_kind == harness_contract::tool::ToolEffectKind::Read;
    let suffix = if read { "read-receipt" } else { "receipt" };
    let stream = format!("execution-effect:{}", request.idempotency_key);
    let key = format!("{}:{suffix}", request.idempotency_key);
    let event = match services.event_store().event_by_idempotency_key(&stream, &key)
        .map_err(|e| unknown(e.to_string()))? {
        Some(event) => event,
        None if original_action.observation().is_none() => {
            let partial = ConversationMessage::tool_result(&call.id, &call.name,
                "original action committed; observation and physical outcome unavailable", true);
            let carrier = crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::OriginalReplay(
                crate::conversation::OriginalReplayedPartial { replay: Arc::clone(&original_action), physical_receipt: None, complete_request: None }))
                .with_original_host_call_set(ticket, &call.id);
            return Ok(OriginalActionReceiptState::CommittedPartial(partial, carrier));
        }
        None => return Err(unknown("original semantic transaction exists but Host outcome receipt is unobserved".into())),
    };
    let receipt = services.event_store().original_transaction_receipt(&event.transaction_id)
        .map_err(|e| unknown(e.to_string()))?
        .ok_or_else(|| unknown("original Host outcome transaction receipt unavailable".into()))?;
    if event.stream_id != stream || event.scope != crate::RuntimeEventScope::ExecutionNode
        || event.schema_version != 1 || event.actor.as_deref() != Some("governed_tool")
        || event.kind != if read { "execution.read.receipt" } else { "execution.effect.receipt" }
        || event.idempotency_key.as_deref() != Some(key.as_str())
        || event.payload["resolved_effect_scope"]["version"].as_u64() != Some(2)
        || receipt.transaction_id != event.transaction_id || receipt.commit_cursor != event.commit_cursor
        || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
        || (event.commit_cursor, event.transaction_index)
            <= (witness.original_action_event().commit_cursor, witness.original_action_event().transaction_index) {
        return Err(unknown("original Host outcome receipt identity or ordering mismatch".into()));
    }
    // A missing flag is not false: old mutation receipts bounded errors
    // silently. Truncated bodies require a separately authenticated full
    // original artifact, which this consumer cannot yet recover.
    match event.payload.get("outcome_truncated").and_then(serde_json::Value::as_bool) {
        Some(false) => {},
        Some(true) => return Err(unknown("original Host outcome was truncated; full original artifact is unavailable".into())),
        None => return Err(unknown("original Host outcome completeness was not recorded".into())),
    }
    // The primary and the original graph/node/attempt index must be the same
    // atomic physical receipt and must both explicitly prove non-truncation.
    let index_stream = format!("execution-agent-receipts:{}:{}:{}", ticket.graph_id, ticket.node_id, ticket.attempt);
    let index_key = format!("agent-tool-receipt:{}", request.idempotency_key);
    let index = services.event_store().event_by_idempotency_key(&index_stream, &index_key)
        .map_err(|e| unknown(e.to_string()))?
        .ok_or_else(|| unknown("original Host receipt index unavailable".into()))?;
    if index.stream_id != index_stream || index.scope != crate::RuntimeEventScope::ExecutionNode
        || index.schema_version != 2 || index.kind != "execution.agent_tool.receipt"
        || index.status.as_deref() != Some("completed") || index.actor.as_deref() != Some("governed_tool")
        || index.idempotency_key.as_deref() != Some(index_key.as_str())
        || index.transaction_id != event.transaction_id || index.commit_cursor != event.commit_cursor
        || index.transaction_index <= event.transaction_index
        || receipt.event_ids.get(index.transaction_index as usize) != Some(&index.event_id)
        || index.payload["idempotency_key"].as_str() != Some(request.idempotency_key.as_str())
        || index.payload["outcome"] != event.payload["outcome"] {
        return Err(unknown("original Host receipt index identity or payload mismatch".into()));
    }
    match index.payload.get("outcome_truncated").and_then(serde_json::Value::as_bool) {
        Some(false) => {},
        Some(true) => return Err(unknown("original Host receipt index is truncated; full original artifact is unavailable".into())),
        None => return Err(unknown("original Host receipt index completeness was not recorded".into())),
    }
    let outcome = services.commit_service().completed_tool_effect(request, effect)
        .map_err(|e| unknown(e.to_string()))?
        .ok_or_else(|| unknown("original Host outcome receipt vanished".into()))?;
    if outcome.tool_use_id != call.id || outcome.tool_name != call.name
        || outcome.status == crate::RuntimeToolExecutionStatus::Unknown
        || serde_json::to_value(&outcome).map_err(|e| unknown(e.to_string()))? != event.payload["outcome"] {
        return Err(unknown("original Host outcome differs from the original call or remains unknown".into()));
    }
    // Semantic observation and physical receipt are different objects. The
    // original action transaction and original effect receipt were each
    // authenticated above against the same ticket, Step, call and effect key.
    // Return the recorded Host bytes without comparing either format or
    // reconstructing Gateway follow-ups from a current projection.
    if original_action.observation().is_none() {
        let partial = ConversationMessage::tool_result(&call.id, &call.name,
            "original action committed; semantic observation unavailable", true);
        let carrier = crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::OriginalReplay(
            crate::conversation::OriginalReplayedPartial { replay: original_action, physical_receipt: Some(Arc::new(event)), complete_request: None }))
            .with_original_host_call_set(ticket, &call.id);
        return Ok(OriginalActionReceiptState::CommittedPartial(partial, carrier));
    }
    Ok(OriginalActionReceiptState::Complete(HostOriginalReplay { call: call.clone(),
        outcome: crate::bound_tool_batch::retained_replay_outcome(outcome), original_action,
        original_request: Arc::new(request.clone()), physical_receipt: Arc::new(event) }))
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
        let services = self.services.upgrade().ok_or_else(|| NodeExecutorError::Uncertain {
            node_id: ticket.node_id.clone(), reason: "original ToolBatch services unavailable".into(),
        })?;
        if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
            bus.emit(CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::CallingTool,
                detail: Some("executing tool batch".to_string()),
            });
        }
        let precommitted = {
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
                true
            } else {
                false
            }
        };
        if precommitted {
            let mut outcome = precommitted_terminal_outcome(ticket);
            let (session_id, turn_id) = {
                let state = self.state.lock().await;
                (state.session_id.clone(), state.turn_id.clone())
            };
            stage_host_transcript(
                &self.runtime,
                &services,
                ticket,
                session_id,
                turn_id,
                Vec::new(),
                None,
                &mut outcome,
            )
            .await?;
            return Ok(outcome);
        }
        let write_goal_id = self.state.lock().await.goal_id.clone();
        let write_goal_source = services
            .goal_store()
            .projection(&write_goal_id)
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?
            .ok_or_else(|| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "current Goal missing".into(),
            })?;
        let required_workspace_write_scopes = {
            let state = self.state.lock().await;
            current_turn_write_scopes(
                &write_goal_source.goal,
                state.execution_role.is_delegated_leaf(),
                &state.focus_acceptance_scopes,
                state
                    .task_understanding
                    .as_ref()
                    .is_some_and(|understanding| understanding.requires_write),
            )
        };
        let required_write_for_completion = !required_workspace_write_scopes.is_empty();
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
        #[cfg(feature = "test-fixtures")]
        let rc_completion_trace = std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some()
            && calls.iter().any(|call| (call.id == "recovery-complete" && call.name == "objective_complete_request") || (call.id == "d-objective-review" && call.name == "objective_review"));
        #[cfg(feature = "test-fixtures")]
        let rc_review_trace = calls.iter().any(|call| call.id == "d-objective-review" && call.name == "objective_review");
        #[cfg(feature = "test-fixtures")]
        if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_decoded" } else { "recovery-complete:host_decoded" }, None, None); }
        let admitted_turn = self.state.lock().await.turn_id.clone();
        if calls.iter().any(|call| {
            call.binding.as_ref().is_some_and(|binding| {
                binding.snapshot.identity.session_id != session_id
                    || binding.snapshot.identity.turn_id != admitted_turn
            })
        }) {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "tool_step_session_or_turn_mismatch".into(),
            });
        }
        if calls.is_empty() {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "tool batch has no model-requested calls".to_string(),
            });
        }
        // Apply the same registered evaluation ceiling at the actual per-call
        // ToolBatch boundary, including system-authored and restored batches.
        // Refusals become releasable only after the original owner commits them;
        // a scope finding alone never releases a conflict-only successor.
        let evaluation_scope_rejections = {
            let state = self.state.lock().await;
            evaluation_scope_call_violations(
                &state.evaluation_resource_scopes,
                &calls,
                services.workspace_root(),
            )
        };
        let mut precompleted = {
            let state = self.state.lock().await;
            calls
                .iter()
                .filter_map(|call| {
                    state
                        .early_tool_receipts
                        .get(&call.id)
                        .filter(|receipt| {
                            receipt.call.id == call.id
                                && receipt.call.name == call.name
                                && receipt.call.binding == call.binding
                                && receipt.call.depends_on == call.depends_on
                                && harness_contract::step::parse_strict_json(&call.input)
                                    .ok()
                                    .is_some_and(|input| {
                                        harness_contract::step::parse_strict_json(
                                            &receipt.call.input,
                                        )
                                        .ok()
                                            == Some(input)
                                    })
                        })
                        .cloned()
                        .map(|receipt| (call.id.clone(), receipt))
                })
                .collect::<BTreeMap<_, _>>()
        };
        if precompleted.keys().any(|id| evaluation_scope_rejections.contains_key(id)) {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "evaluation_scope_precompleted_call_cannot_be_certified_undispatched".into(),
            });
        }
        let governed_host = if delegated_agent_role {
            None
        } else {
            services.tool_execution_host()
        };
        // Only the ToolHost graph path prepares and compiles here. Delegated
        // agents use execute_tool_batch_step(), which owns the same work for
        // that path; doing both would duplicate policy revision and approval.
        // Capture the original owner before any per-call policy negotiation;
        // a replay witness may be minted here without consuming a grant.
        let pregrant_action_issuer = if governed_host.is_some() {
            let runtime = self.runtime.lock().await;
            let owner = Arc::new(runtime.shared_policy_owner());
            Some(Arc::new(runtime.owned_root_action_issuer(owner)))
        } else { None };
        let mut original_replays = BTreeMap::new();
        if let Some(issuer) = pregrant_action_issuer.as_deref() {
            for call in &calls {
                let directory_read = if call.name == "read_ref" {
                    harness_contract::step::parse_strict_json(&call.input).ok()
                        .and_then(|input| crate::agentic::action_invocation::read_ref_directory_input(&input).ok().flatten()).is_some()
                } else { false };
                if (precompleted.contains_key(&call.id) && !directory_read)
                    || (!directory_read && !harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&call.name.as_str())) { continue; }
                if directory_read { precompleted.remove(&call.id); }
                match pregrant_original_action_receipt(issuer, services.as_ref(), ticket, call) {
                    Ok(OriginalActionReceiptState::Complete(receipt)) => {
                        let retained = crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::OriginalReplay(
                            crate::conversation::OriginalReplayedPartial {
                                replay: Arc::clone(&receipt.original_action),
                                physical_receipt: Some(Arc::clone(&receipt.physical_receipt)),
                                complete_request: Some(Arc::clone(&receipt.original_request)),
                            })).with_original_host_call_set(ticket, &call.id);
                        retain_original_member_batches(&self.state, ticket, &[(call.id.clone(), retained)]).await?;
                        original_replays.insert(call.id.clone(), receipt);
                    }
                    Ok(OriginalActionReceiptState::Unobserved) => {},
                    Ok(OriginalActionReceiptState::CommittedPartial(partial, original)) => {
                        let messages = calls.iter().filter_map(|item| {
                            if item.id == call.id { Some(partial.clone()) }
                            else { original_replays.get(&item.id).map(|replay: &HostOriginalReplay| tool_outcome_message(replay.outcome.clone()))
                                .or_else(|| precompleted.get(&item.id).map(early_tool_receipt_message)) }
                        }).collect();
                        let unresolved = calls.iter().filter(|item| !original_replays.contains_key(&item.id) && !precompleted.contains_key(&item.id))
                            .map(|item| crate::conversation::ToolBatchUnresolved::Unknown {
                                call_id: item.id.clone(), detail: "original action committed; Host delivery incomplete".into(),
                                terminal_detail: "original Gateway postprocessing and effect receipt unobserved".into(), commit_error: None,
                            }).collect();
                        let batches = vec![(call.id.clone(), original)];
                        retain_original_member_batches(&self.state, ticket, &batches).await?;
                        let mut node = stage_uncertain_tool_batch(&self.runtime, &services, ticket,
                            session_id.clone(), admitted_turn.clone(), &calls, messages, unresolved).await?;
                        attach_original_member_prefixes(&services, &mut node, &[], &batches)
                            .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                        return Ok(node);
                    },
                    Err(error) => {
                        let messages = calls.iter().filter_map(|item| {
                            original_replays.get(&item.id).map(|replay: &HostOriginalReplay| tool_outcome_message(replay.outcome.clone()))
                                .or_else(|| precompleted.get(&item.id).map(early_tool_receipt_message))
                        }).collect();
                        let unresolved = calls.iter().filter(|item| !original_replays.contains_key(&item.id) && !precompleted.contains_key(&item.id))
                            .map(|item| crate::conversation::ToolBatchUnresolved::Unknown {
                                call_id: item.id.clone(), detail: error.to_string(),
                                terminal_detail: "pre-grant original outcome unresolved".into(), commit_error: None,
                            }).collect();
                        return stage_uncertain_tool_batch(&self.runtime, &services, ticket,
                            session_id.clone(), admitted_turn.clone(), &calls, messages, unresolved).await;
                    }
                }
            }
        }
        let single_captures: Arc<Mutex<HashMap<String, Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>>> = Arc::new(Mutex::new(HashMap::new()));
        let governed_bundle: Option<(
            std::collections::HashMap<String, harness_contract::tool::ToolExecutionAuthorization>,
            std::collections::HashMap<String, harness_contract::tool::GovernedToolInvocation>,
            std::collections::HashMap<String, harness_contract::policy::CapabilityAssessment>,
            std::collections::HashMap<String, HostPolicyRefusal>,
            std::collections::HashMap<String, String>,
            std::collections::HashMap<String, String>,
            std::collections::HashMap<String, crate::conversation::PreparedCallEffect>,
            std::collections::HashMap<String, crate::RuntimeToolExecutionRequest>,
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
                let original_executor: Arc<dyn crate::ToolExecutor> = tool_exec.clone();
                let plan_pin = original_executor.pin_step_catalog().map(Arc::new);
                let mut prepared = Vec::with_capacity(requests.len());
                let mut original_pins = std::collections::HashMap::new();
                let mut admission_rejections = BTreeMap::new();
                for request in &requests {
                    if admission_rejections.contains_key(&request.tool_use_id) {
                        continue;
                    }
                    match tool_exec.prepare_step_invocations(std::slice::from_ref(request), &calls, plan_pin.as_ref(), &original_executor)
                    {
                        Ok(mut invocations) if invocations.len() == 1 => {
                            let item = Arc::new(invocations.remove(0));
                            original_pins.insert(item.invocation().invocation_id.clone(), Arc::clone(&item));
                            prepared.push(item.invocation().clone());
                        }
                        Ok(_) => {
                            admission_rejections.insert(
                                request.tool_use_id.clone(),
                                "missing or ambiguous prepared invocation".into(),
                            );
                        }
                        Err(error) => {
                            admission_rejections.insert(
                                request.tool_use_id.clone(),
                                format!("step admission refused: {error}"),
                            );
                        }
                    }
                }
                let prepared_by_id = prepared
                    .iter()
                    .map(|invocation| (invocation.invocation_id.clone(), invocation.clone()))
                    .collect::<std::collections::HashMap<_, _>>();
                let legacy_forbids_write = runtime.active_turn_strategy().is_some_and(|strategy| {
                    strategy
                        .decision
                        .strategy
                        .understanding
                        .forbids_workspace_write
                });
                let forbids_write = current_turn_forbids_workspace_write(
                    &write_goal_source.goal,
                    legacy_forbids_write,
                );
                let mut governed_compilation = crate::GovernedToolCompiler
                    .compile_prepared_partial(
                        services.workspace_root(),
                        &requests,
                        &prepared,
                        &admission_rejections,
                    );
                if let Ok(compilation) = governed_compilation.as_mut() {
                    if let Some(plan) = compilation.plan.as_mut() {
                        crate::step_snapshot::bind_plan_calls(plan, &calls);
                        for task in &mut plan.tasks {
                            task.original_pin = original_pins.get(&task.tool_call_id).cloned();
                        }
                    }
                }
                let execution_decision = runtime
                    .active_turn_strategy()
                    .map(|strategy| strategy.decision)
                    .ok_or_else(|| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: "tool batch has no admitted turn identity".to_string(),
                    })?;
                let mut auths = std::collections::HashMap::new();
                let mut gaps = std::collections::HashMap::new();
                let mut policy_refusals = std::collections::HashMap::new();
                let mut policy_failures = std::collections::HashMap::new();
                let mut effective_inputs = std::collections::HashMap::new();
                let mut prepared_effects = std::collections::HashMap::new();
                let mut prepared_requests = std::collections::HashMap::new();
                for call in &calls {
                    if precompleted.contains_key(&call.id) || original_replays.contains_key(&call.id) { continue; }
                    if let Some(invocation) = prepared
                        .iter()
                        .find(|invocation| invocation.invocation_id == call.id)
                    {
                        let task = governed_compilation.as_ref().ok()
                            .and_then(|compilation| compilation.plan.as_ref())
                            .and_then(|plan| plan.tasks.iter().find(|task| task.tool_call_id == call.id));
                        let hook = match task {
                            Some(task) => match runtime.prepare_governed_hook(task, &call.input) {
                                Ok(hook) => Some(hook),
                                Err(error) => { policy_failures.insert(call.id.clone(), error.to_string()); continue; }
                            },
                            None => None,
                        };
                        let effective_input = hook.as_ref().map_or(call.input.as_str(), |hook| hook.effective_input.as_str());
                        effective_inputs.insert(call.id.clone(), effective_input.to_string());
                        let descriptor = invocation.effect.clone();
                        let objective_refusal = crate::execution_core::goal::goal_tool_effect_rejection(
                            &write_goal_source.goal, &descriptor, services.path_identity_resolver(),
                        ).or_else(||workspace_mutation_is_objectively_forbidden(forbids_write,descriptor.required_permission)
                            .then_some("original admitted user constraint prohibits workspace mutation"));
                        if let Some(reason) = objective_refusal {
                            gaps.insert(
                                call.id.clone(),
                                synthetic_capability_gap(
                                    &descriptor,
                                    runtime.active_permission_mode(),
                                    reason.to_string(),
                                ),
                            );
                            continue;
                        }
                        let request_id = original_call_request_id(&call.id);
                        let mut bound_request = governed_compilation.as_ref().ok()
                            .and_then(|compilation| compilation.plan.as_ref())
                            .and_then(|plan| plan.tasks.iter().find(|task| task.tool_call_id == call.id).map(|task| (plan, task)))
                            .filter(|(_, task)| task.step_binding == call.binding)
                            .map(|(plan, task)| {
                                let policy = runtime.permission_policy().execution_policy_control().snapshot();
                                let memory = runtime.memory_turn_context();
                                let reality = runtime.reality_data_lease();
                                let mut effective_call = call.clone();
                                effective_call.input = effective_input.to_string();
                                bound_runtime_tool_request(&effective_call, task, &plan.plan_id, plan.revision,
                                    u64::try_from(iteration).unwrap_or(u64::MAX).max(1), &session_id,
                                    policy.sandbox_posture, policy.revision, Some(&memory), reality.as_ref(),
                                    model_lease.as_deref(), ticket, Some(&execution_decision), None, None)
                            });
                        let bounded = tools::tool_orchestrator::effect_is_bounded_read_only_compute(&descriptor)
                            || call.name == "execute_code";
                        if let Some(request) = bound_request.as_mut() {
                            let (Some(task), Some(hook)) = (task, hook.as_ref()) else {
                                if bounded { return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:"bounded original plan or hook missing".into() }); }
                                policy_failures.insert(call.id.clone(), "original unbound plan or hook missing".into());
                                continue;
                            };
                            let Some(plan) = governed_compilation.as_ref().ok().and_then(|compilation| compilation.plan.as_ref()) else {
                                if bounded { return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:"bounded original plan missing".into() }); }
                                policy_failures.insert(call.id.clone(), "original unbound plan missing".into());
                                continue;
                            };
                            if let Err(error) = runtime.bind_original_governed_hook(ticket, task, &call.input, hook, request,
                                &plan.plan_id, plan.revision) {
                                if bounded { return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:error.to_string() }); }
                                policy_failures.insert(call.id.clone(), error.to_string());
                                continue;
                            }
                        }
                        let decision = if let Some(request) = bound_request.as_ref() {
                            if request.step_binding.is_none() {
                                if call.name == "agent_action_batch"
                                    || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&call.name.as_str()) {
                                    policy_failures.insert(call.id.clone(), "original action Step binding missing".into());
                                    continue;
                                }
                                prepared_requests.insert(call.id.clone(), request.clone());
                                runtime.negotiate_tool_authorization(&descriptor, effective_input, request_id,
                                    hook.as_ref().map_or_else(crate::PermissionContext::default, |hook| hook.permission_context.clone()),
                                    hook.as_ref().map_or(default_timeout, |hook| hook.tool_timeout).as_secs(), &prompter).await
                            } else {
                            let effect = match runtime.prepare_call_effect_evidence(ticket, request) {
                                Ok(effect) => effect,
                                Err(error) => { policy_failures.insert(call.id.clone(), error.to_string()); continue; }
                            };
                            let capture = Arc::new(crate::conversation::tool_plane::OriginalSingleProducerCapture::default());
                            capture.bind_original_decision(&execution_decision);
                            let decision = Arc::clone(&capture).scope(runtime.negotiate_tool_authorization_for_call(
                                &descriptor,
                                effective_input,
                                request_id,
                                hook.as_ref().map_or_else(crate::PermissionContext::default, |hook| hook.permission_context.clone()),
                                hook.as_ref().map_or(default_timeout, |hook| hook.tool_timeout).as_secs(),
                                &prompter,
                                crate::execution_core::graph::CallRefusalOwner::Node(ticket),
                                request,
                                Some(&effect),
                            )).await;
                            if call.name == "read_ref" || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&call.name.as_str()) {
                                single_captures.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(call.id.clone(), capture);
                            }
                            prepared_effects.insert(call.id.clone(), effect);
                            prepared_requests.insert(call.id.clone(), request.clone());
                            decision
                            }
                        } else {
                            policy_failures.insert(call.id.clone(), "original call Step binding missing or mismatched".into());
                            continue;
                        };
                        match decision {
                            Ok(crate::conversation::ToolAuthorizationDecision::Authorized(
                                decision,
                            )) => {
                                auths.insert(call.id.clone(), decision.authorization);
                            }
                            Ok(crate::conversation::ToolAuthorizationDecision::Gap {
                                assessment,
                                assessment_event,
                                ..
                            }) => {
                                if assessment.path == harness_contract::policy::AuthorizationPath::HardDeny {
                                    if let (Some(request), Some(assessment_event)) = (bound_request, assessment_event) {
                                        policy_refusals.insert(call.id.clone(), HostPolicyRefusal { request, assessment: assessment_event });
                                    }
                                }
                                gaps.insert(call.id.clone(), assessment);
                            }
                            Err(error) => {
                                policy_failures.insert(call.id.clone(), error.to_string());
                            }
                        }
                    }
                }
                (
                    auths,
                    prepared_by_id,
                    gaps,
                    policy_refusals,
                    policy_failures,
                    effective_inputs,
                    prepared_effects,
                    prepared_requests,
                    execution_decision,
                    governed_compilation,
                )
            })
        } else {
            None
        };
        let mut original_member_batches = Vec::new();
        let (
            result,
            mut orchestration_terminal_summary,
            prepared_tool_invocations,
            mut successful_observed_evidence,
            agentic_progress_refs,
            orchestrated_search_coverage,
        ) = if let Some(host) = governed_host {
            let (
                tool_authorizations,
                prepared_tool_invocations,
                capability_gaps,
                policy_refusals,
                policy_failures,
                effective_inputs,
                prepared_effects,
                prepared_requests,
                execution_decision,
                governed_compilation,
            ) = governed_bundle.ok_or_else(|| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "governed ToolHost is missing its compiled execution bundle".to_string(),
            })?;
            let (event_bus, memory_context, reality_context, execution_policy, original_producers) = {
                let runtime = self.runtime.lock().await;
                let mut original_producers = HashMap::new();
                for call in &calls {
                    if crate::context_evidence::native_read_candidate_tool(&call.name) {
                        let input_hash = crate::tool_invocation::stable_hash(&call.input);
                        if let Some(owner) = runtime.native_tool_activity_owner(&call.id, &call.name, &input_hash)
                            .map_err(|error| NodeExecutorError::Poll { node_id: ticket.node_id.clone(),
                                reason: format!("original model tool owner: {error}") })? {
                            original_producers.insert(call.id.clone(), owner);
                        }
                    }
                }
                if let Ok(compilation) = &governed_compilation {
                    if let Some(plan) = &compilation.plan {
                        runtime.record_governed_tool_plan(plan).map_err(|error| {
                            NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason: format!(
                                    "persist original governed tool plan before execution: {error}"
                                ),
                            }
                        })?;
                    }
                }
                (
                    runtime.cowd_bus().cloned(),
                    runtime.memory_turn_context(),
                    runtime.reality_data_lease(),
                    runtime
                        .permission_policy()
                        .execution_policy_control()
                        .snapshot(),
                    original_producers,
                )
            };
            if !precompleted.is_empty() {
                let original = services.tool_batch_executor().binding_observation(ticket)
                    .ok_or_else(|| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "early read original batch custody unavailable".into() })?;
                for receipt in precompleted.values() {
                    let source = receipt.original_source.as_ref().ok_or_else(|| NodeExecutorError::Uncertain {
                        node_id: ticket.node_id.clone(), reason: "early read original source unavailable".into() })?;
                    if !original_early_read_is_committed(services.as_ref(), &source.ticket, receipt)
                        .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })? {
                        return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "early read original source unconfirmed".into() });
                    }
                    original.hold_original_early_read(ticket, receipt.clone())
                        .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                }
                let runtime = self.runtime.lock().await;
                let state = self.state.lock().await;
                if state.has_evaluation_scope() {
                    let plan = governed_compilation.as_ref().ok().and_then(|compiled| compiled.plan.as_ref())
                        .ok_or_else(|| NodeExecutorError::Poll { node_id: ticket.node_id.clone(),
                            reason: "precompleted scope requires the original compiled plan".into() })?;
                    for call in calls.iter().filter(|call| precompleted.contains_key(&call.id)) {
                        let task = plan.tasks.iter().find(|task| task.tool_call_id == call.id)
                            .ok_or_else(|| NodeExecutorError::Poll { node_id: ticket.node_id.clone(),
                                reason: "precompleted scope requires its original compiled call".into() })?;
                        let request = bound_runtime_tool_request(call, task, &plan.plan_id, plan.revision,
                            u64::try_from(iteration).unwrap_or(u64::MAX).max(1), &session_id,
                            execution_policy.sandbox_posture, execution_policy.revision, Some(&memory_context),
                            reality_context.as_ref(), model_lease.as_deref(), ticket, Some(&execution_decision), None, None);
                        let effect = runtime.prepare_call_effect_evidence(ticket, &request)
                            .map_err(|error| NodeExecutorError::Poll { node_id: ticket.node_id.clone(),
                                reason: format!("precompleted original pinned effect unavailable: {error}") })?;
                        match state.evaluate_scope_refusal(ticket, &request, services.as_ref(), &effect) {
                            Ok(None) => {}
                            Ok(Some(_)) => return Err(NodeExecutorError::Poll { node_id: ticket.node_id.clone(),
                                reason: "precompleted call exceeds its current evaluation scope".into() }),
                            Err(error) => return Err(NodeExecutorError::Poll { node_id: ticket.node_id.clone(),
                                reason: format!("precompleted call scope unresolved: {error}") }),
                        }
                    }
                }
            }
            let strategy_gate = HostRuntimeStrategyGate { runtime: &self.runtime, prompter: &prompter };
            let policy_owner = Arc::new(self.runtime.lock().await.shared_policy_owner());
            let action_issuer = pregrant_action_issuer.ok_or_else(|| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(), reason: "original pre-grant action owner unavailable".into(),
            })?;
            let owner: Arc<dyn crate::tool_host::AgentBatchPolicyOwner> = Arc::new(OriginalBatchPolicyOwner {
                state: Arc::downgrade(&self.state), services: self.services.clone(),
                owner: Arc::clone(&policy_owner), prompter: prompter.clone(),
                parent: harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: ticket.graph_id.clone(), node_id: ticket.node_id.clone(),
                }, attempt: ticket.attempt, ticket: ticket.clone(),
                event_store: Arc::clone(services.event_store()),
                action_service: services.agent_action_service(),
            });
            let governed = ROOT_SINGLE_CAPTURES.scope(Arc::clone(&single_captures), ROOT_ACTION_ISSUER.scope(action_issuer, ROOT_BATCH_POLICY_OWNER.scope(owner, Box::pin(execute_governed_runtime_tool_batch(
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
                services.tool_execution_plane(),
                services.tool_batch_executor(),
                services.commit_service(),
                &precompleted,
                &original_replays,
                &original_producers,
                Some(&evaluation_scope_rejections),
                Some(&self.state),
                Some(services.as_ref()),
                Some(&policy_refusals),
                Some(&policy_failures),
                Some(&effective_inputs),
                Some(&prepared_effects),
                Some(&prepared_requests),
                Some(&strategy_gate),
            ))))).await;
            if policy_owner.take_control_requested() {
                self.runtime.lock().await.consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::AfterToolResult);
            }
            let governed = governed.map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(), reason,
            })?;
            #[cfg(feature = "test-fixtures")]
            if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_governed_returned" } else { "recovery-complete:host_governed_returned" }, None, None); }
            let GovernedToolBatchResult {
                member_batches,
                unresolved,
                unknown_effect,
                refused_before_dispatch,
                messages: governed_messages,
                invocations,
                observed_evidence,
                max_concurrency_observed,
                parallel_batches,
            } = governed;
            if unknown_effect.is_some() {
                retain_original_member_batches(&self.state, ticket, &member_batches).await?;
            }
            // Preserve the full, durable orchestration receipt long enough to
            // derive a verified Program terminal. The model-facing compactor
            // deliberately replaces large JSON with an elided summary, so
            // parsing only `messages` below used to turn a completed Team
            // Program into an apparent missing control-plane proposal.
            let orchestration_terminal_summary = completed_orchestration_terminal_summary(
                &calls,
                &governed_messages,
                services.workspace_root(),
                true,
            );
            // Semantic Action progress must be read from the complete
            // Runtime receipt. The model-facing evidence compactor may
            // legitimately truncate a large projection after this point.
            let mut agentic_progress_refs = agentic_action_progress_refs(&governed_messages);
            let batch_ids = calls.iter().filter(|call| call.name == "agent_action_batch")
                .map(|call| call.id.clone()).collect();
            agentic_progress_refs.extend(agentic_batch_action_progress_refs(&governed_messages, &batch_ids));
            // Graph scheduling executes outside the legacy adapter. Before
            // the next model node sees the result, route its raw output
            // through the same durable evidence and context-ledger path used
            // by normal conversation tool calls.
            #[cfg(feature = "test-fixtures")]
            if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_compactor_enter" } else { "recovery-complete:host_compactor_enter" }, None, None); }
            let messages = compact_governed_tool_messages(
                &self.runtime,
                &calls,
                governed_messages,
                &invocations,
                &precompleted,
            )
            .await;
            #[cfg(feature = "test-fixtures")]
            if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_compactor_return" } else { "recovery-complete:host_compactor_return" }, None, Some(messages.is_ok())); }
            let messages = messages.map_err(|error| match &unknown_effect {
                Some(unknown) => NodeExecutorError::Uncertain {
                    node_id: ticket.node_id.clone(),
                    reason: format!("{unknown}; unresolved calls {unresolved:?}; completed sibling consumption failed: {error}"),
                },
                None => NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("tool evidence durability barrier failed: {error}"),
                },
            })?;
            if let Some(unknown) = unknown_effect {
                let retained = self.runtime.lock().await
                    .verify_completed_sibling_prefix(&calls, &messages, Some(&invocations));
                if let Err(error) = retained {
                    return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(),
                        reason: format!("{unknown}; unresolved calls {unresolved:?}; completed sibling retention failed: {error}") });
                }
                let (session_id, turn_id) = {
                    let state = self.state.lock().await;
                    (state.session_id.clone(), state.turn_id.clone())
                };
                let prefixes = original_member_prefixes(&calls, &member_batches)
                    .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                let mut messages = messages;
                partial_member_messages(&calls, &mut messages, &member_batches, &prefixes)
                    .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                let mut node = stage_uncertain_tool_batch(
                    &self.runtime, &services, ticket, session_id, turn_id,
                    &calls, messages, unresolved,
                ).await?;
                attach_original_member_prefixes(&services, &mut node, &prefixes, &member_batches)
                    .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                return Ok(node);
            }
            original_member_batches = member_batches;
            #[cfg(feature = "test-fixtures")]
            if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_compactor_returned" } else { "recovery-complete:host_compactor_returned" }, None, None); }
            let successful_calls = successful_tool_call_ids(&messages);
            let mut orchestrated_search_coverage = Some(BTreeSet::new());
            for call in calls.iter().filter(|call| call.name.eq_ignore_ascii_case("tool_orchestrate")) {
                let keys = if successful_calls.contains(&call.id) {
                    prepared_requests.get(&call.id).and_then(|request| {
                        let auth = tool_authorizations.get(&call.id)?;
                        let invocation = prepared_tool_invocations.get(&call.id)?;
                        original_orchestrated_search_coverage(request, auth, invocation)
                    })
                } else { None };
                match (&mut orchestrated_search_coverage, keys) {
                    (Some(current), Some(keys)) => current.extend(keys),
                    _ => { orchestrated_search_coverage = None; break; }
                }
            }
            (
                crate::conversation::ToolBatchStepResult {
                    orchestrated_search_coverage: Some(BTreeSet::new()),
                    member_batches: Vec::new(),
                    refused_before_dispatch,
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
                orchestrated_search_coverage,
            )
        } else {
            let mut runtime = self.runtime.lock().await;
            let mut root_owner = None;
            if !delegated_agent_role
                && calls
                    .iter()
                    .any(|call| {
                        // Use the same resolved owner decision as preparation
                        // and execution. Unknown descriptors conservatively
                        // require the original ticket owner.
                        let effect = call.binding.as_ref()
                            .and_then(|binding| binding.resolve(runtime.tool_executor().pin_step_catalog()).ok())
                            .and_then(|step| harness_contract::step::parse_strict_json(&call.input).ok()
                                .map(|input| step.lease().describe_effect(&call.name, &input)));
                        effect.as_ref().is_none_or(|effect|
                            !runtime.executor_owns_governed_effect(&call.name, effect))
                    })
            {
                let (owner, attempt) =
                    runtime
                        .root_effect_owner_for_ticket(ticket)
                        .map_err(|error| NodeExecutorError::Poll {
                            node_id: ticket.node_id.clone(),
                            reason: error.to_string(),
                        })?;
                if owner.root_execution_id != ticket.graph_id
                    || owner.node_id.as_deref() != Some(ticket.node_id.as_str())
                    || attempt != ticket.attempt
                {
                    return Err(NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: "tool batch original ticket does not match its activity graph/node/attempt".into(),
                    });
                }
                root_owner = Some((owner, attempt));
            }
            let transcript_len = runtime.session_head().await.message_count;
            let policy_owner = Arc::new(runtime.shared_policy_owner());
            let member_parent = runtime.tool_executor().durable_tool_effect_parent();
            let work = Box::pin(runtime.execute_tool_batch_step_with_owner(
                    &calls,
                    &prompter,
                    iteration,
                    &precompleted,
                    &evaluation_scope_rejections,
                    root_owner.as_ref(),
                    Some(ticket),
                    Some(&self.state),
                    Some(services.as_ref()),
                    Some(services.commit_service()),
                ));
            let outcome = if delegated_agent_role {
                if let Some((parent, attempt)) = member_parent {
                    let owner: Arc<dyn crate::tool_host::AgentBatchPolicyOwner> = Arc::new(OriginalBatchPolicyOwner {
                        state: Arc::downgrade(&self.state), services: self.services.clone(),
                        owner: Arc::clone(&policy_owner), prompter: prompter.clone(), parent, attempt,
                        ticket: ticket.clone(), event_store: Arc::clone(services.event_store()),
                        action_service: services.agent_action_service(),
                    });
                    crate::tool_host::DELEGATED_BATCH_POLICY_OWNER.scope(owner, work).await
                } else { work.await }
            } else { work.await };
            if !runtime.original_policy_control_is_parent_owned() && policy_owner.take_control_requested() {
                runtime.consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::AfterToolResult);
            }
            let observed_evidence = runtime.tool_executor().observed_evidence_snapshot();
            // The legacy conversation engine writes tool messages eagerly. Roll them
            // back until the graph transition commits; after_commit publishes them.
            runtime
                .session_mut_async()
                .await
                .truncate_messages(transcript_len);
            drop(runtime);
            let (result, prepared_invocations) = match outcome {
                Ok(value) => value,
                Err(error) => {
                    if let (Some(_unknown), Some(partial)) =
                        (error.tool_effect_unknown(), error.partial_tool_batch())
                    {
                        let (session_id, turn_id) = {
                            let state = self.state.lock().await;
                            (state.session_id.clone(), state.turn_id.clone())
                        };
                        // Retain the raw original carrier before verifying it;
                        // a mismatch remains Unknown and must not drop custody.
                        retain_original_member_batches(&self.state, ticket, &partial.member_batches).await?;
                        for (call_id, pending) in &partial.member_batches {
                            if let crate::PendingAgentBatchValue::OriginalBoundedAcquisition(original) = pending.primary_value() {
                                if !original.matches(ticket, services.event_store(), call_id) {
                                    return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original bounded acquisition custody mismatch".into() });
                                }
                            }
                            if let crate::PendingAgentBatchValue::OriginalPacketScope(original) = pending.primary_value() {
                                if !original.matches(ticket, services.event_store(), call_id) {
                                    return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original packet scope custody mismatch".into() });
                                }
                            }
                        }
                        let prefixes = original_member_prefixes(&calls, &partial.member_batches)
                            .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                        let mut messages = partial.messages.clone();
                        partial_member_messages(&calls, &mut messages, &partial.member_batches, &prefixes)
                            .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                        let mut node = stage_uncertain_tool_batch(
                            &self.runtime, &services, ticket, session_id, turn_id,
                            &calls, messages, partial.unresolved.clone(),
                        ).await?;
                        attach_original_member_prefixes(&services, &mut node, &prefixes, &partial.member_batches)
                            .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
                        return Ok(node);
                    }
                    return Err(if let Some(unknown) = error.tool_effect_unknown() {
                        NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: unknown.to_string() }
                    } else {
                        NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: error.to_string() }
                    });
                }
            };
            let prepared_tool_invocations = prepared_invocations
                .into_iter()
                .map(|invocation| (invocation.invocation_id.clone(), invocation))
                .collect::<std::collections::HashMap<_, _>>();
            original_member_batches = result.member_batches.clone();
            let orchestration_terminal_summary: Option<String> = None;
            let mut agentic_progress_refs = agentic_action_progress_refs(&result.messages);
            let batch_ids = calls.iter().filter(|call| call.name == "agent_action_batch")
                .map(|call| call.id.clone()).collect();
            agentic_progress_refs.extend(agentic_batch_action_progress_refs(&result.messages, &batch_ids));
            let orchestrated_search_coverage = result.orchestrated_search_coverage.clone();
            (
                result,
                orchestration_terminal_summary,
                prepared_tool_invocations,
                observed_evidence,
                agentic_progress_refs,
                orchestrated_search_coverage,
            )
        };
        let successful_original_calls = successful_tool_call_ids(&result.messages);
        let delivered_original_calls = delivered_tool_call_ids(&result.messages);
        let mut original_call_results = self.state.lock().await.pending_member_batches
            .get(&(ticket.node_id.clone(), ticket.attempt)).cloned().unwrap_or_default();
        original_call_results.extend(original_member_batches.iter().cloned());
        let committed_goal = original_committed_goal(self, ticket, &write_goal_source,
            &successful_original_calls, &delivered_original_calls, &original_call_results, &original_replays)
            .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
        let (write_goal_source, post_action_program_source) = committed_goal
            .unwrap_or_else(|| (write_goal_source.clone(), None));
        // The original authorization used the pre-dispatch source above.
        // All following progress/checkpoint computation uses the committed source.
        let required_workspace_write_scopes = {
            let state = self.state.lock().await;
            current_turn_write_scopes(&write_goal_source.goal, state.execution_role.is_delegated_leaf(),
                &state.focus_acceptance_scopes, state.task_understanding.as_ref().is_some_and(|understanding| understanding.requires_write))
        };
        let required_write_for_completion = !required_workspace_write_scopes.is_empty();
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
        if let Some(observation) = services
            .tool_batch_executor()
            .binding_observation(ticket)
        {
            for refusal in &result.refused_before_dispatch {
                observation.refuse(&refusal.call_id, refusal.reason.clone());
            }
        }
        let tool_calls = result.messages.len() as u64;
        let failed = result.failed;
        let failed_tools = failed_tool_names(&result.messages);
        let mut rejection_receipts = result.messages.iter().flat_map(|message| &message.blocks)
            .filter_map(|block| match block {
                ContentBlock::ToolResult { output, is_error: true, .. } =>
                    failed_agent_action_receipt(output)
                        .filter(|observation| observation.status == harness_contract::agent_action::AgentActionStatus::Rejected)
                        .map(|observation| observation.receipt_id),
                _ => None,
            }).collect::<Vec<_>>();
        rejection_receipts.sort();
        let mut failure_causes = result.messages.iter().flat_map(|message| &message.blocks)
            .filter_map(|block| match block {
                ContentBlock::ToolResult { tool_use_id, tool_name, output, is_error: true, .. } => {
                    let input = calls.iter().find(|call| call.id == *tool_use_id)
                        .map(|call| tool_batch_fingerprint(std::slice::from_ref(call)))
                        .unwrap_or_else(|| format!("unbound:{tool_use_id}"));
                    let reason = failed_agent_action_receipt(output)
                        .and_then(|observation| observation.error.map(|error|
                            format!("{}:{}:{}", observation.program_id, error.code, error.message)))
                        .unwrap_or_else(|| sha256_digest(output));
                    Some(format!("{tool_name}:{input}:{reason}"))
                }
                _ => None,
            }).collect::<Vec<_>>();
        failure_causes.sort();
        let failure_cause_digest = sha256_digest(&failure_causes.join("\n"));
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
        #[cfg(feature = "test-fixtures")]
        if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_before_program_wait" } else { "recovery-complete:host_before_program_wait" }, None, None); }
        let agentic_program_wait_request = if delegated {
            None
        } else if let Some(program) =
            root_agentic_program_projection(services.as_ref(), &session_id, &turn_id)
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
                services.graph_state_store(),
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
        #[cfg(feature = "test-fixtures")]
        if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_after_program_wait" } else { "recovery-complete:host_after_program_wait" }, None, None); }
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
                delegated_agentic_protocol_state(services.as_ref(), ticket)?
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
            services
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
        let mut coverage_keys = calls.iter().filter(|call| !call.name.eq_ignore_ascii_case("tool_orchestrate"))
            .flat_map(tool_call_coverage_keys).collect::<BTreeSet<_>>();
        let coverage_complete = orchestrated_search_coverage.is_some();
        if let Some(keys) = orchestrated_search_coverage { coverage_keys.extend(keys); }
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
                required_workspace_write_scopes.clone(),
            )
        };
        let verified_postcondition_writes = verified_workspace_postcondition_write_evidence(
            &required_workspace_write_scopes,
            &prior_workspace_mutation_sequences,
            &successful_observed_evidence,
            services.path_identity_resolver(),
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
        let fresh_execution_health = !successful_call_ids.is_empty()
            && fresh_execution_health_progress(
                &prior_observed_evidence,
                &successful_observed_evidence,
                &new_observed_fingerprints,
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
            services.path_identity_resolver(),
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
        let low_novelty = coverage_complete && failed == 0
            && agentic_progress_refs.is_empty()
            && !coverage_keys.is_empty()
            && novelty_target_bp > 0
            && coverage_novelty_bp < novelty_target_bp;
        let evidence_saturated = coverage_complete && failed == 0
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
        let mut delegated_protocol_for_batch =
            delegated_agentic_protocol_state(services.as_ref(), ticket)?;
        if let Some(protocol) = delegated_protocol_for_batch.as_mut() {
            let available = self
                .runtime
                .lock()
                .await
                .tool_executor()
                .available_tool_names()
                .into_iter()
                .collect::<BTreeSet<_>>();
            protocol
                .tool_ceiling
                .retain(|tool| available.contains(tool));
        }
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
                services.path_identity_resolver(),
            );
            automatic_focus_verification = focus_verification_tool_calls(
                &concrete_verification_scopes,
                followup_iteration,
                services.workspace_root(),
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
            executed_failures(
                failed,
                &calls,
                &services
                    .event_store()
                    .repetition_scope(&session_id, &turn_id),
            ),
            !agentic_progress_refs.is_empty(),
        );
        let mut current_objective_milestones = objective_milestones(
            services.as_ref(),
            &state.session_id.clone(),
            &state.turn_id.clone(),
        );
        let mut acquisition_milestones = current_objective_milestones.clone();
        let verified_batch_progress = execution_progress_advanced(
            &state.previous_objective_milestones,
            &mut acquisition_milestones,
            state.completion_gap_outstanding
                || !state.outstanding_closure_rungs.is_empty()
                || bounded_evidence_role,
            fresh_execution_health,
        );
        state.consecutive_low_novelty_batches = next_acquisition_stall_streak(
            state.consecutive_low_novelty_batches,
            failed,
            low_novelty || scope_saturated || evidence_saturated,
            verified_batch_progress,
        );
        // ---- Loop-breaker and targeted runaway caps (nudge-first) ----------
        // A batch that repeats the same tool set, canonical arguments AND the
        // same result is an identical call. Advise the model (default) and only
        // hard stop when the operator opted into the circuit breaker. Pollers
        // are exempt because an unchanged result is their normal contract.
        let loop_repeatable = !calls.is_empty() && calls.iter().all(is_loop_nudge_repeatable_tool);
        let batch_result_digest = terminal_evidence_digest(&result.messages);
        if loop_repeatable {
            state.identical_action_streak = 0;
            state.identical_action_digest.clear();
            state.identical_action_result_digest.clear();
        } else {
            state.identical_action_streak = next_identical_action_streak(
                &state.identical_action_digest,
                &action_fingerprint,
                state.identical_action_streak,
            );
            if state.identical_action_digest != action_fingerprint {
                state.identical_action_digest = action_fingerprint.clone();
            }
        }
        state.identical_action_result_digest = batch_result_digest;
        // Cumulative repeat detection, deliberately separate from the streak
        // guard above. The streak counts consecutive identical batches and is
        // reset by every re-dispatch and by every interleaved call; a real loop
        // inside an Agentic Program is neither consecutive nor confined to one
        // graph. Run one20 made 458 tool calls with 179 distinct inputs and 31
        // byte-identical `state_inspect` queries across five agent graphs, and
        // the streak guard saw none of it because nothing was ever consecutive.
        //
        // Diagnose repeated requests without inferring that their results or
        // their sources stayed unchanged. The model keeps the decision.
        let repeat_turn = services
            .event_store()
            .repetition_scope(&state.session_id, &state.turn_id);
        let repeated_calls = record_repeated_calls(&repeat_turn, &calls, &successful_call_ids);
        if let Some((tool, count)) = repeated_calls.first() {
            let repeated_tools = repeated_calls
                .iter()
                .map(|(name, count)| format!("{name} x{count}"))
                .collect::<Vec<_>>()
                .join(", ");
            tracing::warn!(
                target: "cowd::repeated_call",
                %repeat_turn,
                repeated = %repeated_tools,
                "the same call has been requested repeatedly in this turn"
            );
            let mut item = ContextItem::new(
                format!("repeated-call:{}:{}", ticket.node_id, count),
                ContextSourceKind::ToolTrace,
                ContextRole::Instruction,
                format!(
                    "This turn has requested the same tool calls repeatedly: {repeated_tools}. \
                     Consider the results already obtained and the original task when deciding whether to continue."
                ),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            state.pending_next_model_context.push(item);
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
                        "This is the {streak}th consecutive identical tool call (same tools, same arguments). Do not repeat it: change the arguments, use a different tool, or proceed with the evidence you already have. A wait declared with wait_for_workers is never counted here."
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
        let batch_terminal_framing = crate::host::scope_turn_framing_for_role(
            crate::host::agentic_turn_framing(
                services.as_ref(),
                &state.session_id,
                &state.turn_id,
                state.iterations,
                state.tool_results.len(),
                state.started_at.elapsed().as_millis(),
            ),
            state.execution_role,
        );
        let batch_forced_terminal_ready = forced_terminal_presentation_allowed(
            &batch_terminal_framing,
            state.execution_role.is_delegated_leaf()
                && focus_acceptance_met
                && delegated_protocol_for_batch
                    .as_ref()
                    .is_none_or(DelegatedAgenticProtocolState::is_terminal),
        );
        let acquisition_stall_streak = state.consecutive_low_novelty_batches;
        let repeated_local_failures = state.consecutive_tool_failure_batches >= 2;
        let repeated_evidence_saturation = state.consecutive_low_novelty_batches
            >= evidence_saturation_limit(bounded_evidence_role);
        let successful_write_observed = write_obligation_satisfied(
            required_write_for_completion,
            &required_workspace_write_scopes,
            &state.committed_workspace_observed_evidence,
            state.committed_workspace_write_observed,
            services.path_identity_resolver(),
        );
        let required_effects_pending = !focus_acceptance_pending_scopes.is_empty()
            || (required_write_for_completion && !successful_write_observed);
        let pending_delegated_action = pending_delegated_action_is_ready(
            delegated_protocol_for_batch.as_ref(),
            repeated_evidence_saturation,
            state.successful_tool_calls > 0,
            required_effects_pending,
        ) && delegated_protocol_for_batch.as_ref().is_some_and(
            |protocol| {
                protocol.terminal_action_ids().any(|tool| {
                    !crate::repetition_guard::cause_blocked_for_tool(
                        &services
                            .event_store()
                            .repetition_scope(&state.session_id, &state.turn_id),
                        tool,
                    )
                })
            },
        );
        let progress_sources = stable_collaboration_sources(services.as_ref(), &state.session_id, &state.turn_id);
        let progress_known = progress_sources.is_some();
        let collaboration_progress = if let Some((program, goal)) = progress_sources.as_ref() {
            state.verified_collaboration_progress.observe(
                Some(program), 0, goal.observations.iter()
                    .filter(|observation| observation.has_verified_gain())
                    .flat_map(|observation| observation.information_gain.distinguishing_evidence_refs.iter().cloned())
                    .filter(|reference| !reference.starts_with("verified_collaboration:"))
                    .chain(goal.progress.criteria.iter().filter_map(|(id, status)|
                        matches!(status, AcceptanceStatus::Satisfied | AcceptanceStatus::Waived)
                            .then(|| format!("criterion:{id}:{status:?}"))))
                    .chain(goal.goal.obligations.iter().filter_map(|obligation|
                        matches!(obligation.state, harness_contract::goal::ObjectiveObligationState::Satisfied | harness_contract::goal::ObjectiveObligationState::Waived)
                            .then(|| format!("obligation:{}:{:?}", obligation.obligation_id, obligation.state)))),
            )
        } else { false };
        let delegated_redrives = next_collaboration_redrives(
            state.delegated_redrives, collaboration_progress, pending_delegated_action && progress_known,
        );
        state.delegated_redrives = delegated_redrives;
        let delegated_redrive_bound = collaboration_convergence_replan_bound(state.safety_lease.complexity);
        let blocked_program = progress_sources.as_ref().is_some_and(|(program, _)| program.status == crate::AgenticProgramStatus::Blocked);
        let forced_convergence = forced_convergence_required(
            delegated_redrives, delegated_redrive_bound, state.completion_gap_outstanding,
            state.outstanding_closure_rungs.len(), blocked_program, progress_known,
        );
        let forced_convergence_reason = forced_convergence.then(|| format!(
            "forced convergence: the turn re-drove delegated work {delegated_redrives} time(s) beyond the convergence bound ({delegated_redrive_bound}); present the retained receipts and their verified facts now instead of acquiring again"
        ));
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
            services.workspace_root(),
        );
        let focus_synthesis_ready = batch_forced_terminal_ready
            && !pending_delegated_action
            && delegated_protocol_after_batch.is_none()
            && should_force_focus_synthesis(
                focus_acceptance_met,
                &focus_acceptance_scopes,
                repeated_evidence_saturation,
                has_retained_focus_terminal_candidate,
                exact_workspace_evidence,
            );
        let task_focus = state.execution_role.is_delegated_leaf().then_some((
            required_workspace_write_scopes.as_slice(),
            state.committed_workspace_observed_evidence.as_slice(),
            state.committed_workspace_write_observed,
        ));
        let revalidation_checkpoint = GoalWriteCheckpoint::ToolProgress {
            bounded_role: bounded_evidence_role,
            saturated: repeated_evidence_saturation,
            write_attempted: !state.write_attempt_paths.is_empty(),
        };
        let root_write_checkpoint = task_focus.is_none();
        let (write_decision, write_goal_cut) = services
            .goal_store()
            .assess_write_checkpoint(
                &write_goal_source,
                ticket,
                services.path_identity_resolver(),
                revalidation_checkpoint,
                task_focus,
                &successful_observed_evidence,
            )
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?;
        let original_satisfied = matches!(&write_decision, GoalWriteDisposition::Satisfied);
        let revalidation_disposition = matches!(&write_decision, GoalWriteDisposition::Satisfied | GoalWriteDisposition::Pending);
        let write_recovery = match write_decision {
            GoalWriteDisposition::Recover(recovery) => Some(recovery),
            GoalWriteDisposition::Satisfied | GoalWriteDisposition::Pending => None,
            GoalWriteDisposition::Blocked(_) => {
                unreachable!("tool progress never declares business exhaustion")
            }
        };
        let required_write_recovery = write_recovery.is_some();
        if required_write_recovery {
            state.consecutive_low_novelty_batches = 0;
        }
        let authorized_write_scopes = state
            .evaluation_resource_scopes
            .iter()
            .filter(|scope| scope.starts_with("write:"))
            .cloned()
            .collect::<Vec<_>>();
        let has_successful_tool_evidence = state.successful_tool_calls > 0;
        let cold_start_failure_replan_available = super::cold_start_tool_failure_replan_available(
            has_successful_tool_evidence,
            state.cold_start_failure_replans,
        );
        if repeated_local_failures && cold_start_failure_replan_available {
            // Consume the single semantic-replan lease before releasing the
            // state guard. The next model step receives a fresh failure
            // streak, while a second consecutive failure is still terminal.
            state.cold_start_failure_replans = state.cold_start_failure_replans.saturating_add(1);
            state.consecutive_tool_failure_batches = 0;
        }
        let newly_completed_program_team_ids = completed_program_team_ids(&result.messages);
        let completed_root_team_this_batch = root_team_terminal_requires_text_only(
            state.execution_role.is_delegated_leaf(),
            state.collaboration_obligation.as_ref(),
            &newly_completed_program_team_ids,
        );
        if failed == 0 && !result_stub_disabled() {
            // Byte-identical duplicate payloads never re-enter context. A model
            // that cannot find what it already read reads it again, and every
            // re-read puts the whole payload back - one live delegated Agent did
            // the same `read_file` sixteen times over three and a half minutes
            // against a 145k-token context.
            //
            // The dedup is seeded from the whole turn transcript, not just this
            // batch: those sixteen reads were each in their own round, so a
            // per-batch map never saw a repeat and stubbed nothing. Stubbing is
            // default-on because the durable evidence is untouched - bytes stay
            // keyed by tool_call id and `evidence_retrieve` still returns them
            // in full. An operator opts out with COWD_LOOP_STUB_RESULTS=0.
            let mut transcript = std::mem::take(&mut state.tool_results);
            transcript.extend(result.messages);
            crate::summary_compression::stub_duplicate_result_payloads(&mut transcript);
            state.tool_results = transcript;
        } else {
            state.tool_results.extend(result.messages);
        }
        if completed_root_team_this_batch && batch_forced_terminal_ready {
            // A verified collaboration terminal leaves only presentation.
            state.force_text_only_next_model = true;
            tracing::warn!(
                target: "cowd::text_only_step",
                site = "completed_root_team_this_batch",
                completion_gap_outstanding = state.completion_gap_outstanding,
                outstanding_closure_rungs = ?state.outstanding_closure_rungs,
                "a text-only model step was armed; the invariant at the model-step boundary keeps the closing tool available while a mandatory action is outstanding"
            );
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
            tracing::warn!(
                target: "cowd::text_only_step",
                site = "focus_synthesis_ready",
                completion_gap_outstanding = state.completion_gap_outstanding,
                outstanding_closure_rungs = ?state.outstanding_closure_rungs,
                "a text-only model step was armed; the invariant at the model-step boundary keeps the closing tool available while a mandatory action is outstanding"
            );
            state.force_reasoning_effort_next_model = Some("none".to_string());
            state.clean_terminal_synthesis_attempted = true;
            state.clean_terminal_synthesis_next = true;
        }
        state.last_verified_progress = execution_progress_advanced(
            &state.previous_objective_milestones,
            &mut current_objective_milestones,
            state.completion_gap_outstanding
                || !state.outstanding_closure_rungs.is_empty()
                || pending_delegated_action,
            fresh_execution_health,
        );
        state.previous_objective_milestones = current_objective_milestones;
        let verified_evidence_refs = new_observed_fingerprints
                .iter()
                .map(|fingerprint| format!("tool_observation:{fingerprint}"))
                .filter(|reference| !write_goal_source.progress.evidence_refs.contains(reference))
                .chain(
                    successful_focus_resource_scope_keys
                        .iter()
                        .filter(|scope| {
                            !focus_resource_scopes_covered_before.contains(scope.as_str())
                                && !write_goal_source.progress.evidence_refs.contains(&format!("focus_resource_scope:{scope}"))
                        })
                        .map(|scope| format!("focus_resource_scope:{scope}")),
                )
                .chain(progress_sources.as_ref().filter(|_| collaboration_progress).map(|(program, goal)| {
                    let accepted = program.tasks.values().filter(|task| task.status == crate::AgenticTaskStatus::Accepted)
                        .map(|task| &task.task_id).collect::<Vec<_>>();
                    let gain = goal.observations.iter().filter(|observation| observation.has_verified_gain())
                        .flat_map(|observation| observation.information_gain.distinguishing_evidence_refs.iter())
                        .filter(|reference| !reference.starts_with("verified_collaboration:"))
                        .collect::<BTreeSet<_>>();
                    let identity = serde_json::to_string(&(accepted, &goal.progress.criteria, &goal.goal.obligations, gain)).unwrap_or_default();
                    format!("verified_collaboration:{}", sha256_digest(&identity))
                }))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
        let observation_fingerprint = if failed == 0 {
            action_fingerprint
        } else if failure_causes.is_empty() {
            format!("unidentified-tool-failure:{}:{}", ticket.idempotency_key, ticket.attempt)
        } else {
            format!("tool_failure:{failure_cause_digest}")
        };
        let mut observation = runtime_observation(
            runtime_observation_identity(&services, &state, ticket),
            RuntimeObservationKind::ToolProgress,
            "runtime.tool_batch",
            u64::try_from(state.iterations).unwrap_or(u64::MAX),
            if failed_tools.is_empty() && repeated_success {
                format!(
                    "tool batch reused an already-completed action calls={tool_calls}; retained receipt must be used before another identical request"
                )
            } else if failed_tools.is_empty() && scope_saturated && !coverage_complete {
                format!(
                    "tool batch completed calls={tool_calls} in a previously covered scope; search coverage remains unknown"
                )
            } else if failed_tools.is_empty() && !coverage_complete {
                format!("tool batch completed calls={tool_calls}; search coverage remains unknown")
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
        if let Some(reason) = forced_convergence_reason.clone() {
            observation.source = "runtime.convergence.forced".to_string();
            observation.summary = format!("{}; {reason}", observation.summary);
        }
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
        let resolved_unknown_refs = satisfied_focus_acceptance_scope_keys
            .iter()
            .filter(|scope| !focus_resource_scopes_covered_before.contains(scope.as_str())
                && write_goal_source.progress.open_unknowns.contains(&format!("focus-acceptance:{scope}")))
            .map(|scope| format!("focus-acceptance:{scope}"))
            .collect::<Vec<_>>();
        let has_verified_evidence = !verified_evidence_refs.is_empty() || !resolved_unknown_refs.is_empty();
        observation.information_gain = InformationGain {
            distinguishing_evidence_refs: verified_evidence_refs,
            resolved_unknown_refs,
            provenance: if has_verified_evidence {
                MeasureProvenance::Observed
            } else {
                MeasureProvenance::Unknown
            },
        };
        if failed > 0 && rejection_receipts.len() == failed && !observation.has_verified_gain() {
            // Repeated delivery of the same original action rejection is one
            // observation, even when the graph retries with another ticket.
            observation.source = format!("runtime.tool_rejection:{}", sha256_digest(&rejection_receipts.join("\n")));
            observation.source_revision = 1;
        }
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
        if let Some(reason) = forced_convergence_reason.clone() {
            // Present from the retained receipts: one clean synthesis with the
            // evidence already committed, and no further acquisition.
            state.clean_terminal_synthesis_next = true;
            state.force_text_only_next_model = true;
            state.force_tool_allowlist_next_model = None;
            let mut item = ContextItem::new(
                format!("forced-convergence:{}", ticket.node_id),
                ContextSourceKind::ToolTrace,
                ContextRole::Instruction,
                format!(
                    "{reason}. No further tool calls are available: state the verified facts from the receipts you already hold, and name any remaining uncertainty explicitly."
                ),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            state.pending_next_model_context.push(item);
        }
        drop(state);
        if !pending_delegated_action
            && batch_forced_terminal_ready
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
        let mut intervention = if let Some(reason) = forced_convergence_reason.clone() {
            // The forced convergence is recorded with this intervention, so the
            // durable goal history states why the turn stopped acquiring.
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: RuntimeInterventionKind::Synthesize,
                reason,
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if pending_delegated_action {
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
                kind: acquisition_saturation_intervention(batch_forced_terminal_ready,
                    acquisition_stall_streak, evidence_saturation_limit(bounded_evidence_role)),
                reason: if bounded_evidence_role {
                    "two consecutive bounded evidence batches added no required coverage; retain checked evidence and stop the child before another model/tool step"
                } else if batch_forced_terminal_ready {
                    "verified terminal closure permits presentation from retained evidence"
                } else {
                    "Repeated acquisition added no verified progress. Replan from retained results toward an authorized unmet execution or verification action, or explain a real blocker. Use only schemas exposed on the actual next request; schema presence is not permission. Continued unchanged acquisition will stop safely with an honest partial result, not certify completion."
                }
                .to_string(),
                evidence_refs: observation.evidence_refs.clone(),
                expected_graph_revision: None,
            })
        } else if repeated_local_failures {
            Some(RuntimeIntervention {
                goal_id: goal_id.clone(),
                kind: if cold_start_failure_replan_available {
                    RuntimeInterventionKind::Replan
                } else if has_successful_tool_evidence {
                    RuntimeInterventionKind::Synthesize
                } else {
                    RuntimeInterventionKind::Block
                },
                reason: if cold_start_failure_replan_available {
                    format!(
                        "{}: one bounded cold-start recovery lease is available: read the exact Runtime diagnostic on each rejected call, change the tool or its arguments, and execute only an exposed authorized action; a second consecutive failure will block",
                        COLD_START_TOOL_FAILURE_REPLAN_MARKER
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
                        &services,
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
        if !batch_forced_terminal_ready
            && intervention
                .as_ref()
                .is_some_and(|value| value.kind == RuntimeInterventionKind::Synthesize)
        {
            if let Some(value) = intervention.as_mut() {
                value.kind = if repeated_evidence_saturation {
                    acquisition_saturation_intervention(
                        false,
                        acquisition_stall_streak,
                        evidence_saturation_limit(bounded_evidence_role),
                    )
                } else {
                    RuntimeInterventionKind::Block
                };
                value.reason = "Terminal completion is not verified. Retain governed actions for a bounded replan; unchanged acquisition or exhausted failure recovery must stop honestly partial, not assert that required evidence is complete.".into();
            }
        }
        let protocol_closure_required = !required_effects_pending
            && protocol_closure_can_be_required(
                delegated_protocol_for_batch.as_ref(),
                intervention.as_ref(),
                &services
                    .event_store()
                    .repetition_scope(&session_id, &turn_id),
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
                    services.workspace_root(),
                    Some(services.as_ref()),
                    true,
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
                                services.workspace_root(),
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
                            tracing::warn!(
                                target: "cowd::text_only_step",
                                site = "bounded_evidence_role_terminal",
                                completion_gap_outstanding = state.completion_gap_outstanding,
                                outstanding_closure_rungs = ?state.outstanding_closure_rungs,
                                "a text-only model step was armed; the invariant at the model-step boundary keeps the closing tool available while a mandatory action is outstanding"
                            );
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
        #[cfg(feature = "test-fixtures")]
        if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_outcome_constructed" } else { "recovery-complete:host_outcome_constructed" }, None, None); }
        let mut outcome = NodeExecutionOutcome::new(completed_result(
            Some(format!("{}:tool-results:{tool_calls}", ticket.graph_id)),
            ExecutionUsage {
                tool_calls,
                ..ExecutionUsage::default()
            },
        ));
        outcome.domain_events.push(
            services
                .goal_store()
                .observation_event_at(
                    &write_goal_source,
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
                services
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
        outcome
            .source_revisions
            .insert(write_goal_cut.stream_id, write_goal_cut.expected_revision);
        if let Some((stream, revision)) = post_action_program_source {
            if outcome.source_revisions.insert(stream, revision).is_some_and(|previous| previous != revision) {
                return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original Program source conflict".into() });
            }
        }

        if let Some(recovery) = write_recovery {
            outcome
                .source_revisions
                .insert(recovery.stream_id, recovery.expected_stream_revision);
            outcome.domain_events.push(recovery.event);
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
        let (session_id, turn_id, mut messages) = {
            let state = self.state.lock().await;
            let messages = state
                .pending_transcript
                .get(&ticket.node_id)
                .cloned()
                .ok_or_else(|| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: "Host tool batch lost its original publication before outcome".into(),
                })?;
            (state.session_id.clone(), state.turn_id.clone(), messages)
        };
        if runtime_authored_tool_batch(&calls) {
            messages.insert(0, runtime_authored_tool_call_message(&calls));
        }
        #[cfg(feature = "test-fixtures")]
        if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_before_transcript" } else { "recovery-complete:host_before_transcript" }, None, None); }
        stage_host_transcript(
            &self.runtime,
            &services,
            ticket,
            session_id,
            turn_id,
            messages,
            None,
            &mut outcome,
        )
        .await?;
        retain_original_member_batches(&self.state, ticket, &original_member_batches).await?;
        let prefixes = original_member_prefixes(&calls, &original_member_batches)
            .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
        attach_original_member_prefixes(&services, &mut outcome, &prefixes, &original_member_batches)
            .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
        // Retain the exact original post-tool computation in this physical
        // backend only. This does not grant authority to dispatch again.
        if root_write_checkpoint && revalidation_disposition
            && calls.iter().any(|call| call.name == "objective_complete_request")
            && outcome.domain_events.iter().filter(|e| e.event.stream_id == format!("goal:{}", write_goal_source.goal.id)).count() == 1
            && outcome.domain_events.iter().any(|e| e.event.kind == "goal.observation" && e.event.stream_id == format!("goal:{}", write_goal_source.goal.id))
        {
            remember_tool_goal_revalidation(self, ticket, &write_goal_source, &observation,
                revalidation_checkpoint, original_satisfied, &outcome);
        }
        #[cfg(feature = "test-fixtures")]
        if rc_completion_trace { crate::execution_core::graph::k05_completion_trace(ticket, if rc_review_trace { "d-objective-review:host_executor_return" } else { "recovery-complete:host_executor_return" }, None, None); }
        Ok(outcome)
    }

    async fn revalidate_goal_source(
        &self,
        ticket: &NodeExecutionTicket,
        outcome: &NodeExecutionOutcome,
        stale_stream: &str,
        expected: u64,
        actual: u64,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        let services = self.services.upgrade().ok_or_else(|| NodeExecutorError::Uncertain {
            node_id: ticket.node_id.clone(), reason: "original ToolBatch services unavailable".into(),
        })?;
        let saved = self.goal_revalidation.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        let Some(saved) = saved else { return Ok(None) };
        let fail = |reason: String| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason };
        let mut original = outcome.clone();
        if original.result.usage.duration_ms < saved.outcome.result.usage.duration_ms { return Ok(None); }
        original.result.usage.duration_ms = saved.outcome.result.usage.duration_ms;
        original.no_dispatch = saved.outcome.no_dispatch.clone();
        if saved.ticket != *ticket || original != saved.outcome
            || stale_stream != format!("goal:{}", saved.source.goal.id)
            || saved.source.stream_revision != expected
            || outcome.source_revisions.get(stale_stream) != Some(&expected)
            || expected.checked_add(1) != Some(actual)
        { return Ok(None); }
        let Some(current) = services.goal_store().projection(&saved.source.goal.id).map_err(&fail)? else { return Ok(None) };
        if current.stream_revision != actual || saved.source.goal.completion != GoalCompletion::Open
            || saved.source.goal.terminal.is_some() || current.goal.completion != GoalCompletion::Satisfied
            || current.goal.phase != "completed"
            || current.goal.revision != saved.source.goal.revision.saturating_add(1)
            || !saved.source.goal.evidence_refs.iter().all(|e| current.goal.evidence_refs.contains(e))
        { return Ok(None); }
        let Some(terminal) = current.goal.terminal.as_ref() else { return Ok(None) };
        if terminal.kind != harness_contract::goal::ObjectiveTerminalKind::Satisfied { return Ok(None); }
        // Compare every contractual field, allowing only the terminal owner's
        // completion, phase, revision and additive evidence to advance.
        let mut comparable = current.goal.clone();
        comparable.completion = saved.source.goal.completion;
        comparable.phase = saved.source.goal.phase.clone();
        comparable.revision = saved.source.goal.revision;
        comparable.terminal = saved.source.goal.terminal.clone();
        comparable.evidence_refs = saved.source.goal.evidence_refs.clone();
        if comparable != saved.source.goal { return Ok(None); }
        let Some(binding) = current.goal.execution_binding.as_ref() else { return Ok(None) };
        if binding.root_execution_id != ticket.graph_id || binding.session_id != current.goal.session_id { return Ok(None); }
        let store = services.event_store();
        let events = store.list_stream_after(stale_stream, expected, actual, 2, 1024 * 1024).map_err(&fail)?;
        let [completed] = events.as_slice() else { return Ok(None) };
        if completed.sequence != actual || completed.kind != "goal.completed"
            || completed.actor.as_deref() != Some("runtime.objective_supervisor")
        { return Ok(None); }
        let program_stream = format!("agentic-program:{}", binding.agentic_program_id);
        let program_revision = store.stream_revision(&program_stream).map_err(|e| fail(e.to_string()))?;
        let actions = crate::AgentActionService::new(Arc::clone(store));
        let Some(program) = actions.project_if_exists(&binding.agentic_program_id).map_err(|e| fail(e.to_string()))? else { return Ok(None) };
        let Some(verdict) = program.objective_verdict.as_ref() else { return Ok(None) };
        if program.objective_id != binding.objective_id || program.session_id != binding.session_id
            || program.turn_id != binding.turn_id || program.root_execution_id.as_deref() != Some(ticket.graph_id.as_str())
            || verdict.goal_id != current.goal.id || verdict.goal_revision != current.goal.revision
            || verdict.terminal_fence != terminal.terminal_fence || verdict.authority_revision != terminal.authority_revision
            || verdict.kind != terminal.kind
        { return Ok(None); }
        let bound_verdict = store.event_by_idempotency_key(&program_stream, &format!("agentic-objective-verdict:{}", verdict.terminal_fence)).map_err(|e| fail(e.to_string()))?;
        let conclusion = store.event_by_idempotency_key(&program_stream, &format!("program-conclusion:{}", verdict.authority_revision)).map_err(|e| fail(e.to_string()))?;
        let verdict_event = match (bound_verdict, conclusion) {
            (Some(first), Some(second)) if first != second => return Ok(None),
            (Some(event), _) | (_, Some(event)) => event,
            (None, None) => return Ok(None),
        };
        if verdict_event.kind != "agentic.objective_verdict_bound"
            || verdict_event.transaction_id != completed.transaction_id
            || verdict_event.payload["program_id"] != binding.agentic_program_id
            || verdict_event.payload["verdict"] != serde_json::to_value(verdict).map_err(|e| fail(e.to_string()))?
            || store.stream_revision(&program_stream).map_err(|e| fail(e.to_string()))? != program_revision
            || outcome.source_revisions.get(&program_stream).is_some_and(|r| *r != program_revision)
        { return Ok(None); }
        let (decision, cut) = services.goal_store().assess_write_checkpoint(
            &current, ticket, services.path_identity_resolver(), saved.checkpoint, None,
            &saved.observation.observed_evidence,
        ).map_err(&fail)?;
        if cut.stream_id != stale_stream || cut.expected_revision != actual
            || !matches!((&decision, saved.original_satisfied), (GoalWriteDisposition::Satisfied, true) | (GoalWriteDisposition::Pending, false))
        { return Ok(None); }
        let Some(index) = outcome.domain_events.iter().position(|e| e.event.stream_id == stale_stream && e.event.kind == "goal.observation") else { return Ok(None) };
        let Some(key) = outcome.domain_events[index].idempotency_key.clone() else { return Ok(None) };
        let mut observation = saved.observation.clone();
        observation.evidence_delta.added.retain(|reference| !current.progress.evidence_refs.contains(reference));
        observation.information_gain.distinguishing_evidence_refs.retain(|reference|
            !current.progress.evidence_refs.contains(reference));
        observation.information_gain.resolved_unknown_refs.retain(|reference|
            current.progress.open_unknowns.contains(reference));
        if observation.information_gain.distinguishing_evidence_refs.is_empty()
            && observation.information_gain.resolved_unknown_refs.is_empty()
        {
            observation.information_gain.provenance = MeasureProvenance::Unknown;
        }
        let event = services.goal_store().observation_event_at(&current, &observation, key).map_err(&fail)?;
        let mut rebuilt = outcome.clone();
        rebuilt.domain_events[index] = event;
        rebuilt.source_revisions.insert(stale_stream.to_owned(), actual);
        rebuilt.source_revisions.insert(program_stream, program_revision);
        Ok(Some(rebuilt))
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let services = self.services.upgrade().ok_or_else(|| NodeExecutorError::Uncertain {
            node_id: ticket.node_id.clone(), reason: "original ToolBatch services unavailable".into(),
        })?;
        let (calls, _) = decode_tool_batch(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason: format!("tool batch persistent payload is invalid after commit: {error}"),
        })?;
        let node_id = ticket.node_id.clone();
        let original_ticket = ticket.clone();
        let commits = services.commit_service().clone();
        // Only the original node owner may enumerate the manifest from the
        // node transaction. An empty manifest makes no claim about rollback.
        let (pending_member_facts, committed_member_facts) = tokio::task::spawn_blocking(move || -> Result<(std::collections::BTreeMap<String, (String, serde_json::Value)>, std::collections::BTreeMap<String, Vec<serde_json::Value>>), String> {
            let outers = commits.original_batch_outers_after_commit(&original_ticket)
                .map_err(|error| error.to_string())?;
            let mut seen = std::collections::BTreeSet::new();
            let mut pending_member_facts = std::collections::BTreeMap::new();
            let mut committed_member_facts = std::collections::BTreeMap::new();
            for outer in outers {
                if !seen.insert(outer.clone()) { return Err("duplicate committed batch outer".into()); }
                let mut matching = calls.iter().filter(|call| {
                    call.name == "agent_action_batch" && call.binding.as_ref()
                        .is_some_and(|binding| binding.effect_idempotency_key() == outer)
                });
                let Some(call) = matching.next() else { return Err("committed batch outer has no original call".into()); };
                if matching.next().is_some() {
                    return Err("committed batch outer does not match one original call".into());
                }
                let members = commits.original_batch_members_after_commit(&original_ticket, &outer)
                    .map_err(|error| error.to_string())?;
                let pending = commits.original_batch_pending_after_commit(&original_ticket, &outer)
                    .map_err(|error| error.to_string())?;
                if let Some(pending) = &pending {
                    if pending.payload()["call_id"].as_str() != Some(call.id.as_str())
                        || calls.iter().filter(|candidate| candidate.id == call.id).count() != 1 {
                        return Err("committed batch pending call differs or is ambiguous".into());
                    }
                    pending_member_facts.insert(outer.clone(), (call.id.clone(), pending.payload()["pending"].clone()));
                }
                if members.is_empty() && pending.is_none() {
                    return Err("committed batch manifest has neither prefix nor pending fact".into());
                }
                // These are committed business facts. They do not prove that
                // the complete Host result or downstream effects were delivered.
                committed_member_facts.insert(outer, members.iter().map(|member| member.payload().clone()).collect());
            }
            Ok((pending_member_facts, committed_member_facts))
        }).await.map_err(|error| NodeExecutorError::Uncertain { node_id: node_id.clone(), reason: error.to_string() })?
          .map_err(|reason| NodeExecutorError::Uncertain { node_id, reason })?;
        let retained_replays = self.state.lock().await.pending_member_batches
            .get(&(ticket.node_id.clone(), ticket.attempt)).cloned().unwrap_or_default();
        let mut settled_packet_scope = Vec::new();
        let mut packet_scope_unresolved = false;
        for (call_id, pending) in &retained_replays {
            if matches!(pending.value(), crate::PendingAgentBatchValue::WithCause { .. })
                && matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalPacketScope(_)
                    | crate::PendingAgentBatchValue::OriginalBoundedAcquisition(_)) {
                return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original call has an independent unresolved cause".into() });
            }
            let crate::PendingAgentBatchValue::OriginalPacketScope(original) = pending.primary_value() else { continue };
            if !original.matches(ticket, services.event_store(), call_id) {
                return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original packet scope custody mismatch".into() });
            }
            let request = original.request();
            let proof = match services.commit_service().original_agent_packet_scope_refusal(ticket, request, original.fact()) {
                Ok(proof) => proof,
                Err(_) => { packet_scope_unresolved = true; continue; }
            };
            if original.receipt().is_some_and(|receipt|
                receipt.event_id() != proof.event_id() || receipt.transaction_id() != proof.transaction_id()) {
                return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:"original packet scope winner mismatch".into() });
            }
            settled_packet_scope.push((pending.clone(), proof));
        }
        for (call_id, pending) in &retained_replays {
            let crate::PendingAgentBatchValue::OriginalBoundedAcquisition(original) = pending.primary_value() else { continue };
            if !original.matches(ticket, services.event_store(), call_id) {
                return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:"original bounded acquisition custody mismatch".into() });
            }
            let proof = match services.commit_service().original_bounded_acquisition_failure(ticket, original.fact()) {
                Ok(proof) => proof,
                Err(_) => { packet_scope_unresolved = true; continue; }
            };
            if original.receipt().is_some_and(|receipt| receipt.event_id() != proof.event_id() || receipt.transaction_id() != proof.transaction_id()) {
                return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:"original bounded acquisition winner mismatch".into() });
            }
            settled_packet_scope.push((pending.clone(), proof));
        }
        if packet_scope_unresolved {
            // The Scoped physical owner retains this backend and the original
            // same-ticket state when after_commit returns Uncertain.
            return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(),
                reason: "original packet scope transaction remains unresolved".into() });
        }
        let mut settled_complete_replays = Vec::new();
        for (call_id, pending) in &retained_replays {
            if matches!(pending.value(), crate::PendingAgentBatchValue::WithCause { .. })
                && matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalReplay(_)) {
                return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original replay has an independent unresolved cause".into() });
            }
            if let crate::PendingAgentBatchValue::OriginalReplay(original) = pending.primary_value() {
                if original.replay.call_id() != call_id { return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original replay call differs".into() }); }
                if original.complete_request.is_some() {
                    if !original_complete_replay_is_committed(&services, ticket, call_id, original)
                        .map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })? {
                        return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "complete original replay remains unresolved".into() });
                    }
                    settled_complete_replays.push(pending.clone());
                    continue;
                }
                services.commit_service().original_replayed_action_after_commit(ticket, &original.replay)
                    .map_err(|error| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
            }
        }
        publish_host_transcript(&self.runtime, &services, ticket).await?;
        let mut settled_calls = std::collections::BTreeSet::new();
        let mut settlement_error = None;
        {
            let retained = {
                let state = self.state.lock().await;
                let existing = state.pending_member_batches.get(&(ticket.node_id.clone(), ticket.attempt));
                existing.cloned().unwrap_or_default()
            };
            let mut grouped = std::collections::BTreeMap::<String, Vec<crate::PendingAgentBatch>>::new();
            for (call_id, pending) in retained { grouped.entry(call_id).or_default().push(pending); }
            let mut candidates = Vec::new();
            let replay_close = crate::execution_core::graph::executors::current_original_physical_close(ticket);
            for (call_id, items) in grouped {
                let mut original: Option<Arc<crate::tool_host::OriginalMemberCallCarrier>> = None;
                let mut unverified = false;
                for pending in &items {
                    if let crate::PendingAgentBatchValue::OriginalCall(carrier) = pending.primary_value() {
                        if carrier.original_request().tool_use_id != call_id {
                            unverified = true;
                        } else if let Some(existing) = &original {
                            if !existing.original_request().tool_progress.same_original_channels(&carrier.original_request().tool_progress)
                                || existing.original_request() != carrier.original_request() {
                                unverified = true;
                            }
                        } else { original = Some(Arc::clone(carrier)); }
                    }
                }
                let Some(original) = original else { continue; };
                let request = original.original_request();
                let pending_fact = pending_member_facts.get(&request.idempotency_key).map(|(_, fact)| fact);
                let terminal_publication = if pending_fact.is_some_and(|fact| matches!(fact["state"].as_str(),
                    Some("confirmed_noncommit" | "before_outcome_unknown" | "before_outcome_error" | "before_outcome_store_unknown"))) {
                    // The node fact stays unchanged. Only this original stop may
                    // supply later terminal evidence to the existing receipt/close validators.
                    request.tool_progress.reconcile_original_member_stop();
                    match request.tool_progress.checked_original_stop_publications() {
                        Ok(publications) if publications.len() == 1 => {
                            let publication = &publications[0];
                            match (publication.as_ref(), request.tool_progress.original_member_stop()) {
                                (crate::tool_host::OriginalStopPublication::Recorded(stop),
                                    crate::tool_host::OriginalStopRead::Available(Some(current)))
                                    if Arc::ptr_eq(stop, &current)
                                        && stop.recorded_in_original_sink()
                                        && stop.belongs_to_original_sink(&request.tool_progress)
                                        && stop.coordinate().ticket() == ticket
                                        && stop.coordinate().outer_key() == request.idempotency_key
                                        && stop.coordinate().call_id() == request.tool_use_id
                                        && stop.coordinate().step() == request.step_binding.as_ref()
                                        && pending_fact.and_then(|fact| fact["index"].as_u64()) == u64::try_from(stop.coordinate().index()).ok()
                                        && matches!(stop.reason(), crate::tool_host::OriginalBatchMemberStopReason::ConfirmedNoncommit(terminal)
                                            if terminal.matches_request(request)) =>
                                    Some(Arc::clone(publication)),
                                _ => None,
                            }
                        },
                        _ => None,
                    }
                } else { None };
                // Only the exact recorded terminal may resolve a WithCause;
                // preserve all other independent causes as unresolved.
                fn same_terminal_carrier(
                    pending: &crate::PendingAgentBatch,
                    request: &crate::RuntimeToolExecutionRequest,
                    publication: &Arc<crate::tool_host::OriginalStopPublication>,
                ) -> bool {
                    match pending.value() {
                        crate::PendingAgentBatchValue::WithCause { primary, cause } =>
                            same_terminal_carrier(primary, request, publication) && same_terminal_carrier(cause, request, publication),
                        crate::PendingAgentBatchValue::OriginalCall(carrier) =>
                            carrier.original_request() == request
                                && carrier.original_request().tool_progress.same_original_channels(&request.tool_progress),
                        crate::PendingAgentBatchValue::RecordedSink(sink) => request.tool_progress.same_original_channels(sink),
                        crate::PendingAgentBatchValue::Recorded(recorded) => {
                            let actual = request.tool_progress.original_members();
                            recorded.len() == actual.len() && recorded.iter().zip(&actual).all(|((i,a),(j,b))|
                                i == j && Arc::ptr_eq(a,b))
                        },
                        crate::PendingAgentBatchValue::OriginalStopPublication(other) => Arc::ptr_eq(other, publication),
                        crate::PendingAgentBatchValue::OriginalTerminalBoundary(boundary) => {
                            Arc::ptr_eq(&boundary.publication, publication)
                                && boundary.sink.same_original_channels(&request.tool_progress)
                                && boundary.index == publication.current().coordinate().index()
                                && serde_json::from_str::<serde_json::Value>(&request.input).ok()
                                    .and_then(|value| value.get("actions").and_then(serde_json::Value::as_array).map(Vec::len))
                                    == Some(boundary.total)
                        },
                        _ => false,
                    }
                }
                for pending in &items {
                    if let Some(publication) = &terminal_publication {
                        if !same_terminal_carrier(pending, request, publication) { unverified = true; }
                        continue;
                    }
                    match pending.value() {
                        crate::PendingAgentBatchValue::OriginalCall(carrier) => {
                            if carrier.original_request() != request
                                || !request.tool_progress.same_original_channels(&carrier.original_request().tool_progress) {
                                unverified = true;
                            }
                        },
                        crate::PendingAgentBatchValue::RecordedSink(sink) => {
                            if !request.tool_progress.same_original_channels(sink) { unverified = true; }
                        },
                        crate::PendingAgentBatchValue::Recorded(recorded) => {
                            let actual = request.tool_progress.original_members();
                            if recorded.len() != actual.len() || recorded.iter().zip(&actual).any(|((i,a),(j,b))|
                                i != j || !Arc::ptr_eq(a,b)) { unverified = true; }
                        },
                        _ => { unverified = true; }
                    }
                }
                if unverified || (request.tool_progress.has_original_member_stop() && terminal_publication.is_none()) {
                    continue;
                }
                let facts = committed_member_facts.get(&request.idempotency_key).map(Vec::as_slice).unwrap_or(&[]);
                if let Some(close) = &replay_close {
                    match original.verified_host_release(ticket, close, services.event_store(), facts, pending_fact, terminal_publication.as_ref()) {
                        Ok(Some(units)) => {
                            for unit in units { unit.mark_settled(); }
                            settled_calls.insert(call_id);
                            continue;
                        },
                        Ok(None) => {},
                        Err(reason) => { settlement_error.get_or_insert(reason); continue; }
                    }
                }
                match request.tool_progress.original_fresh_effect() {
                    Ok(Some(_)) if pending_fact.is_some() && terminal_publication.is_none() => {},
                    Ok(Some(owner)) if owner.original_request().tool_use_id == call_id
                        && owner.original_request().tool_progress.same_original_channels(&request.tool_progress) => {
                            match original.verified_host_settlements(ticket, services.event_store(), facts, pending_fact) {
                                // A preparation-only terminal may have no unit. The release
                                // owner verifies the real closed observation's exact member
                                // set together with the sealed terminal and node receipts.
                                Ok(units) => candidates.push((call_id, Some(owner), units, Arc::clone(&original), facts.to_vec(), pending_fact.cloned(), terminal_publication.clone())),
                                Err(reason) => { settlement_error.get_or_insert(reason); }
                            }
                        },
                    Ok(Some(_)) => { settlement_error.get_or_insert("original effect owner does not match call sink".to_string()); },
                    Ok(None) if pending_fact.is_none() && request.compiled_invocation.as_ref().is_some_and(|invocation|
                        invocation.effect.effect_kind == harness_contract::tool::ToolEffectKind::Read) => {
                            candidates.push((call_id, None, Vec::new(), Arc::clone(&original), facts.to_vec(), None, None))
                        },
                    Ok(None) => {},
                    Err(reason) => { settlement_error.get_or_insert(reason); }
                }
            }
            if !candidates.is_empty() {
                if let Some(close) = crate::execution_core::graph::executors::current_original_physical_close(ticket) {
                    let commits = services.commit_service().clone();
                    let original_ticket = ticket.clone();
                    let (settled, failed) = tokio::task::spawn_blocking(move || {
                        let mut settled = Vec::new();
                        let mut failures = Vec::new();
                        for (call_id, owner, units, carrier, facts, pending, publication) in candidates {
                            if let Some(owner) = owner {
                                if let Err(error) = commits.release_original_host_effect_after_node(
                                    &original_ticket, &close, &owner, &carrier, &facts, pending.as_ref(), publication.as_ref(), &units,
                                ) {
                                    failures.push(error.to_string());
                                    continue;
                                }
                            }
                            for unit in units { unit.mark_settled(); }
                            settled.push(call_id);
                        }
                        (settled, failures)
                    }).await.map_err(|error| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
                    settled_calls.extend(settled);
                    if let Some(reason) = failed.into_iter().next() { settlement_error.get_or_insert(reason); }
                } else {
                }
            }
        }
        let goal_id = self.state.lock().await.goal_id.clone();
        let committed = services.event_store().event_by_idempotency_key(
            &format!("goal:{goal_id}"), &format!("{}:goal-observation", ticket.idempotency_key),
        ).map_err(|error| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
        if let Some(event) = committed {
            let value = services.event_store().resolve_observation_record(&event)
                .map_err(|reason| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason })?;
            let observation: RuntimeObservation = serde_json::from_value(value)
                .map_err(|error| NodeExecutorError::Poll { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
            if observation.identity.graph_id == ticket.graph_id
                && observation.identity.node_id.as_deref() == Some(ticket.node_id.as_str())
                && observation.goal_id() == goal_id && observation.has_verified_gain()
            {
                let mut state = self.state.lock().await;
                let first_commit = state.verified_collaboration_progress.observe(
                    None, 0, std::iter::once(format!("committed_goal_observation:{}", observation.idempotency_fingerprint())),
                );
                if first_commit {
                    state.last_terminal_answer_digest.clear();
                    state.repeated_terminal_answers = 0;
                }
            }
        }
        if let Some(reason) = settlement_error.as_ref().filter(|_| !settled_packet_scope.is_empty()) {
            return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: reason.clone() });
        }
        if !settled_packet_scope.is_empty() {
            let observation = services.tool_batch_executor().binding_observation(ticket)
                .ok_or_else(|| NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:"original packet scope observation unavailable".into() })?;
            for (pending, proof) in &settled_packet_scope {
                observation.settle_packet_scope(proof)
                    .map_err(|reason| NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason })?;
                if matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalBoundedAcquisition(_)) {
                    services.event_store().release_original_bounded_decision(proof)
                        .map_err(|error| NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(), reason:error.to_string() })?;
                }
            }
        }
        // Keep each original carrier, including its release proof, until the
        // whole callback can finish; another call or later read may fail.
        let observation_unresolved = services.tool_batch_executor().binding_observation(ticket)
            .is_some_and(|observation| observation.member_custody_is_unresolved());
        let mut state = self.state.lock().await;
        let member_custody_unresolved = state.pending_member_batches.get(&(ticket.node_id.clone(), ticket.attempt))
            .is_some_and(|entries| entries.iter().any(|(call_id, pending)| {
                if settled_calls.contains(call_id) { return false; }
                match pending.primary_value() {
                    crate::PendingAgentBatchValue::OriginalCall(carrier) => carrier.has_unsettled_host_members()
                        || pending_member_facts.contains_key(&carrier.original_request().idempotency_key),
                    _ => false,
                }
            }));
        if let Some(reason) = settlement_error {
            return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason });
        }
        let node_pending_unsettled = pending_member_facts.values().any(|(call_id, _)| !settled_calls.contains(call_id));
        if node_pending_unsettled || member_custody_unresolved || observation_unresolved {
            return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original member custody remains unresolved".into() });
        }
        state.pending_transcript.remove(&ticket.node_id);
        if let Some(retained) = state.pending_member_batches.get_mut(&(ticket.node_id.clone(), ticket.attempt)) {
            retained.retain(|(call_id, pending)| {
                if settled_complete_replays.iter().any(|settled| settled == pending) {
                    false
                } else if matches!(pending.primary_value(), crate::PendingAgentBatchValue::OriginalPacketScope(_) | crate::PendingAgentBatchValue::OriginalBoundedAcquisition(_)) {
                    !settled_packet_scope.iter().any(|(settled, _)| settled == pending)
                } else { !settled_calls.contains(call_id) }
            });
            if retained.is_empty() { state.pending_member_batches.remove(&(ticket.node_id.clone(), ticket.attempt)); }
        }
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
    fn structured_rejection_inside_tool_error_retains_original_receipt_identity() {
        let receipt = harness_contract::agent_action::AgentActionObservation {
            receipt_id: "rejection:program:action".into(), action_id: "action".into(),
            action: "objective_complete_request".into(), program_id: "program".into(), revision: 7,
            status: harness_contract::agent_action::AgentActionStatus::Rejected, duplicate: true,
            changed_refs: vec![], actionable: vec![], projection: None,
            error: Some(harness_contract::agent_action::AgentActionErrorObservation {
                code: "objective_not_verifiable".into(), message: "review missing".into(), recoverable: true,
            }),
        };
        let output = format!("Runtime did not execute the action.\nStructured receipt: {}", serde_json::to_string(&receipt).unwrap());
        assert_eq!(super::failed_agent_action_receipt(&output), Some(receipt));
    }

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

    /// The verified-answer drive reads delivered results from the Program
    /// projection, so an unaccepted Task and an artifact nobody claimed must not
    /// reach the answer the operator reads.
    #[test]
    fn only_accepted_tasks_delivered_artifacts_reach_the_answer_carrier() {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        program.artifacts.insert(
            "artifact:done".to_string(),
            crate::AgenticArtifactProjection {
                artifact_ref: "artifact:done".to_string(),
                content_ref: "artifact://art_done".to_string(),
                kind: "report".to_string(),
                title: "Delivered finding".to_string(),
                relates_to: vec![],
                committed_by: "agent:worker".to_string(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        program.artifacts.insert(
            "artifact:pending".to_string(),
            crate::AgenticArtifactProjection {
                artifact_ref: "artifact:pending".to_string(),
                content_ref: "artifact://art_pending".to_string(),
                kind: "report".to_string(),
                title: "Still in flight".to_string(),
                relates_to: vec![],
                committed_by: "agent:worker".to_string(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        for (task_id, status, artifact) in [
            (
                "task:done",
                crate::AgenticTaskStatus::Accepted,
                "artifact:done",
            ),
            (
                "task:pending",
                crate::AgenticTaskStatus::Submitted,
                "artifact:pending",
            ),
        ] {
            program.tasks.insert(
                task_id.to_string(),
                crate::AgenticTaskProjection {
                    obligations: Vec::new(),
                    rework_directives: Vec::new(),
                    unsettled_released_attempts: Vec::new(),
                    terminal_note: None,
                    task_id: task_id.to_string(),
                    team_id: "team:1".to_string(),
                    title: "work".to_string(),
                    objective: "objective".to_string(),
                    acceptance: "evidence".to_string(),
                    acceptance_checks: Vec::new(),
                    criterion_source: Default::default(),
                    required_capabilities: Vec::new(),
                    depends_on: Vec::new(),
                    status,
                    claimant: Some("agent:worker".to_string()),
                    claim_generation: 1,
                    claim_execution_id: None,
                    claimed_at_ms: None,
                    lease_expires_at_ms: None,
                    claim_renewal_streak: 0,
                    active_attempts: Default::default(),
                    artifact_refs: vec![artifact.to_string()],
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    review_reason: None,
                    reviewed_by: None,
                    failed_attempts: 0,
                    review_generation: 1,
                    failed_review_attempts: 0,
                    structural_execution_stalls: 0,
                    structural_review_stalls: 0,
                    reviews_applied: 0,
                    review_obligation: None,
                    reviews_accepted: 0,
                    last_failure: None,
                    replacement_task_refs: Vec::new(),
                    supersede_evidence_refs: Vec::new(),
                    superseded_reason: None,
                    superseded_by: None,
                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                    cancel_requested_by: None,
                    cancel_reason_ref: None,
                    cancel_evidence_refs: Vec::new(),
                    pending_retirement: None,
                },
            );
        }
        let results = verified_program_delivered_results(&program);
        assert_eq!(results.len(), 1, "{results:?}");
        assert!(results[0].contains("Delivered finding"), "{results:?}");
        assert!(results[0].contains("artifact:done"), "{results:?}");
    }
}

#[cfg(test)]
mod host_publication_450_tests {
    use super::*;

    #[test]
    fn r1_450_host_publication_restore_repeat_and_later_suffix_are_exact() {
        let mut session = crate::Session::new();
        let session_id = session.session_id.clone();
        let prefix = vec![ConversationMessage {
            role: crate::MessageRole::User,
            blocks: vec![ContentBlock::Text {
                text: "original".into(),
            }],
            usage: None,
        }];
        session.extend_messages(prefix.clone());
        let digest = host_message_prefix_digest(&prefix).unwrap();
        let messages = vec![
            serde_json::json!({"role":"assistant","blocks":[{"Text":{"text":"real output"}}],"usage":null}),
            serde_json::json!({"role":"tool","blocks":[{"ToolResult":{"tool_use_id":"call","tool_name":"read_file","output":"real receipt","is_error":false}}],"usage":null}),
        ];
        apply_host_transcript_messages(&mut session, &session_id, 1, &digest, &messages).unwrap();
        let once = session.materialize_messages();
        apply_host_transcript_messages(&mut session, &session_id, 1, &digest, &messages).unwrap();
        assert_eq!(session.materialize_messages(), once);
        session.extend_messages(prefix.clone());
        apply_host_transcript_messages(&mut session, &session_id, 1, &digest, &messages).unwrap();
        assert_eq!(session.message_count(), 4);
        let mut partial = crate::Session::new();
        partial.session_id = session_id.clone();
        partial.extend_messages(prefix);
        partial.extend_messages(vec![host_message_from_value(&messages[0]).unwrap()]);
        apply_host_transcript_messages(&mut partial, &session_id, 1, &digest, &messages).unwrap();
        assert_eq!(partial.materialize_messages(), once);
    }

    #[test]
    fn r1_450_host_publication_missing_prefix_divergence_and_identity_fail_closed() {
        let mut session = crate::Session::new();
        let session_id = session.session_id.clone();
        let empty = host_message_prefix_digest(&[]).unwrap();
        let message = serde_json::json!({"role":"assistant","blocks":[{"Text":{"text":"expected"}}],"usage":null});
        assert!(apply_host_transcript_messages(
            &mut session,
            &session_id,
            1,
            &empty,
            &[message.clone()]
        )
        .is_err());
        assert!(apply_host_transcript_messages(
            &mut session,
            "foreign",
            0,
            &empty,
            &[message.clone()]
        )
        .is_err());
        session.extend_messages(vec![ConversationMessage {
            role: crate::MessageRole::Assistant,
            blocks: vec![ContentBlock::Text {
                text: "different output".into(),
            }],
            usage: None,
        }]);
        let before = session.materialize_messages();
        assert!(
            apply_host_transcript_messages(&mut session, &session_id, 0, &empty, &[message])
                .is_err()
        );
        assert_eq!(session.materialize_messages(), before);
    }

    #[test]
    fn r1_450_host_explicit_no_message_obligation_is_not_pending_map_inference() {
        let mut session = crate::Session::new();
        let session_id = session.session_id.clone();
        let digest = host_message_prefix_digest(&[]).unwrap();
        let ticket = NodeExecutionTicket { graph_id:"host-no-message".into(),node_id:"tools".into(),executor_kind:"tool_batch".into(),
            service_class:Default::default(),attempt:1,idempotency_key:"same-logical-key".into(),
            payload_ref:serde_json::json!({"calls":[{"id":"planned-but-budget-skipped","name":"read_file","input":"{}"}]}).to_string() };
        let outcome = precommitted_terminal_outcome(&ticket);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&ticket.payload_ref).unwrap()["calls"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            outcome.result.result_ref.as_deref(),
            Some("host-no-message:precommitted-terminal")
        );
        apply_host_transcript_messages(&mut session, &session_id, 0, &digest, &[]).unwrap();
        assert_eq!(session.message_count(), 0);
    }
}

#[cfg(test)]
mod closed_host_continuity_475_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct NoReplay475(Arc<AtomicUsize>);
    impl ApiClient for NoReplay475 {
        fn configure_turn_cancellation(
            &mut self,
            _control: Option<Arc<crate::execution_core::services::TurnCancellationControl>>,
        ) -> Result<(), RuntimeError> {
            // This fixture rejects every provider replay and owns no producer.
            Ok(())
        }
        fn stream(
            &mut self,
            _request: crate::ApiRequest,
        ) -> std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::AssistantEvent, RuntimeError>> + Send + '_,
            >,
        > {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(vec![Err(RuntimeError::new(
                "475 forbids provider replay",
            ))]))
        }
    }
    struct NoTools475;
    #[async_trait::async_trait]
    impl ToolExecutor for NoTools475 {
        async fn execute_output(
            &self,
            name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new(format!(
                "475 forbids tool replay: {name}"
            )))
        }
    }

    #[tokio::test]
    async fn r1_475_closed_model_doc_restores_real_cold_host_session_before_continuation() {
        use crate::execution_core::graph::{
            ExecutionGraphStateStore, ExecutionNodeBinding, NodeExecutionContext, NodeExecutor,
        };
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeSpec,
        };
        for root_control in [None, Some(1)] {
            let root = tempfile::tempdir().unwrap();
            let backend = Arc::new(crate::runtime_event_store::EphemeralRuntimeEventStore::new());
            let artifacts = Arc::new(
                crate::ArtifactStore::for_test(
                    root.path().join("blobs"),
                    crate::ArtifactStoreConfig {
                        compact_threshold_bytes: 1,
                        ..crate::ArtifactStoreConfig::default()
                    },
                )
                .unwrap(),
            );
            let make_services = || {
                crate::RuntimeServices::builder(root.path().join("home"), root.path())
                    .runtime_event_store(Arc::new(crate::RuntimeEventStore::from_backend(
                        backend.clone(),
                    )))
                    .artifact_store(artifacts.clone())
                    .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
                    .build()
                    .unwrap()
            };
            let services = make_services();
            let mut session = crate::Session::new();
            session.extend_messages([ConversationMessage::user_text("original user prefix")]);
            let original_session = session.clone();
            let calls = Arc::new(AtomicUsize::new(0));
            let runtime = Arc::new(tokio::sync::Mutex::new(
                Box::new(crate::ConversationRuntime::new(
                    session,
                    NoReplay475(calls.clone()),
                    NoTools475,
                    PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
                    vec!["475".into()],
                )
                .without_memory()
                .with_runtime_event_store(services.event_store().clone())
                .with_artifact_store(artifacts.clone())),
            ));
            let state = Arc::new(tokio::sync::Mutex::new(model_publication_state475(
                &original_session.session_id,
                "475-no-goal-proof",
            )));
            let mut graph = ExecutionGraph::new("475 closed Model continuity");
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: original_session.session_id.clone(),
                turn_id: "475-turn".into(),
                root_task_id: "475-task".into(),
                task_id: "475-task".into(),
                generation: 1,
            });
            let mut node=ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel,"inline_model",
                serde_json::json!({"session_id":original_session.session_id,"turn_id":"475-turn","step":0}).to_string());
            node.id = "model475".into();
            graph.nodes.push(node.clone());
            let mut timer =
                ExecutionNodeSpec::new(ExecutionNodeKind::Timer, "timer", "475-continuation");
            timer.id = "timer475".into();
            graph.nodes.push(timer);
            graph
                .edges
                .push(harness_contract::execution_graph::ExecutionEdge {
                    from: node.id.clone(),
                    to: "timer475".into(),
                    kind: harness_contract::execution_graph::ExecutionEdgeKind::DependsOn,
                });
            let graph = services
                .commit_service()
                .register_graph(graph)
                .unwrap()
                .graph;
            let ready = services
                .commit_service()
                .transition_node(
                    &graph,
                    &node.id,
                    ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            let ticket = services
                .model_step_executor()
                .start(NodeExecutionContext {
                    graph: Arc::new(ready.clone()),
                    node: node.clone(),
                    attempt: 1,
                })
                .await
                .unwrap();
            let binding = ExecutionNodeBinding {
                executor_kind: ticket.executor_kind.clone(),
                ticket_idempotency_key: ticket.idempotency_key.clone(),
                attempt: 1,
                resource_lease_refs: Vec::new(),
                scope_lease_ref: None,
                worktree_lease_ref: None,
            };
            let key = binding
                .lease_closure_key(&ready, &node.id)
                .rsplit(':')
                .next()
                .unwrap()
                .to_string();
            let mut owner = services
                .scope_locks()
                .acquire_binding_owner(&key, || {
                    services
                        .commit_service()
                        .validate_unadmitted_binding_start(&ready, &node, &ticket)
                        .map_err(|error| error.to_string())
                })
                .unwrap();
            let running = services
                .commit_service()
                .bind_and_start_nodes(
                    &ready,
                    BTreeMap::from([(node.id.clone(), binding.clone())]),
                    vec![ticket.clone()],
                )
                .unwrap()
                .graph;
            let cut = original_sibling_closure_cut(&services, &ticket)
                .expect("original graph cut is readable")
                .expect("the only started binding is the original model ticket");
            assert_eq!(cut.get(&running.id), Some(&running.revision));
            let mut foreign = ticket.clone();
            foreign.idempotency_key.push_str(":foreign");
            assert!(original_sibling_closure_cut(&services, &foreign)
                .map_or(true, |cut| cut.is_none()), "a drifted ticket cannot authenticate the cut");
            let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("475-model-result".into()),
                wait_class: None,
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            });
            let message = ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "original committed Model fragment".into(),
            }]);
            stage_host_transcript(
                &runtime,
                &services,
                &ticket,
                original_session.session_id.clone(),
                "475-turn".into(),
                vec![message.clone()],
                root_control,
                &mut outcome,
            )
            .await
            .unwrap();
            let graph = services
                .commit_service()
                .transition_execution_outcome(&running, &ticket, &outcome, &BTreeMap::new())
                .unwrap()
                .graph;
            let hot = TurnModelStepBackend {
                ticket: ticket.clone(),
                stop: crate::CancellationToken::new(),
                local_fence: Mutex::new(None),
                runtime: runtime.clone(),
                state: state.clone(),
                services: services.clone(),
            };
            {
                let mut runtime_guard = runtime.lock().await;
                assert!(runtime_guard.turn_cancelled().is_err());
                assert!(runtime_guard.scoped_model_stop(crate::CancellationToken::new()).is_err());
                let control = crate::execution_core::services::TurnCancellationControl::operational(
                    "475-session", "475-turn", "475-execution",
                );
                runtime_guard.install_provider_cancellation_control(Some(control.clone())).unwrap();
                let turn_stop = control.token();
                assert!(hot.retains_physical_execution());
                for field in [
                    "graph",
                    "node",
                    "attempt",
                    "idempotency",
                    "executor",
                    "payload",
                ] {
                    let mut foreign = ticket.clone();
                    match field {
                        "graph" => foreign.graph_id.push_str("-foreign"),
                        "node" => foreign.node_id.push_str("-foreign-call"),
                        "attempt" => foreign.attempt += 1,
                        "idempotency" => foreign.idempotency_key.push_str("-foreign"),
                        "executor" => foreign.executor_kind.push_str("-foreign"),
                        "payload" => foreign.payload_ref.push(' '),
                        _ => unreachable!(),
                    }
                    hot.request_stop(&foreign);
                    assert!(!hot.stop.is_cancelled(), "{field}");
                }
                hot.request_stop(&ticket);
                assert!(hot.stop.is_cancelled());
                assert!(!turn_stop.is_cancelled());
                {
                    let scoped = runtime_guard.scoped_model_stop(hot.stop.clone()).unwrap();
                    assert!(scoped.cancellation_token().is_cancelled());
                }
                assert!(!runtime_guard.turn_cancelled().unwrap());
                let fresh_stop = crate::CancellationToken::new();
                assert!(!runtime_guard.scoped_model_stop(fresh_stop).unwrap().cancellation_token().is_cancelled());
                let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let scoped = runtime_guard.scoped_model_stop(hot.stop.clone()).unwrap();
                    assert!(scoped.cancellation_token().is_cancelled());
                    panic!("controlled model scope unwind");
                }));
                assert!(unwind.is_err());
                assert!(!runtime_guard.turn_cancelled().unwrap());
                assert!(matches!(
                    hot.execute(&ticket).await,
                    Err(NodeExecutorError::Poll { reason, .. })
                        if reason == "model step cancelled before execution"
                ));
                assert_eq!(calls.load(Ordering::SeqCst), 0);
            }
            hot.after_commit(&ticket).await.unwrap();
            assert_eq!(
                runtime
                    .lock()
                    .await
                    .session_snapshot()
                    .await
                    .materialize_messages(),
                vec![
                    ConversationMessage::user_text("original user prefix"),
                    message.clone()
                ]
            );
            let position = services
                .graph_state_store()
                .binding_start_position(&graph, &node.id, &binding)
                .unwrap()
                .unwrap();
            let proof = owner
                .release_original(
                    &graph,
                    &node.id,
                    &binding,
                    position,
                    &mut None,
                    &mut None,
                    &mut None,
                    services.worktree_leases(),
                )
                .unwrap();
            let closed = services
                .commit_service()
                .commit_observed_binding_closure_for_test(&graph, &proof)
                .unwrap()
                .graph;
            let closed = services
                .commit_service()
                .transition_node(
                    &closed,
                    "timer475",
                    ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            drop(proof);
            drop(owner);
            drop(hot);
            drop(runtime);
            drop(state);
            let cold_services = make_services();
            let cursor = cold_services.event_store().durable_commit_cursor().unwrap();
            assert!(cold_services
                .commit_service()
                .binding_requires_host_publication(&closed, &node.id, &binding)
                .unwrap());
            assert!(cold_services
                .execution_supervisor()
                .recover_graph(&closed.id)
                .await
                .is_err());
            assert_eq!(
                cold_services.event_store().durable_commit_cursor().unwrap(),
                cursor
            );
            assert!(!cold_services.graph_executors_are_bound(&closed).unwrap());
            let report = cold_services
                .recover_execution_graphs_on_startup()
                .await
                .unwrap();
            assert!(report.errors.is_empty());
            assert_eq!(report.notified_graphs, 0);
            assert!(report
                .records
                .iter()
                .any(|record| record.graph_id == closed.id
                    && record.action == "deferred_turn_scoped_executor_binding"
                    && record.error.is_none()));
            assert_eq!(
                cold_services.graph_state_store().load(&closed.id).unwrap(),
                closed
            );
            let cold = Arc::new(tokio::sync::Mutex::new(Box::new(
                crate::ConversationRuntime::new(
                    original_session.clone(),
                    NoReplay475(calls.clone()),
                    NoTools475,
                    PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
                    vec!["475".into()],
                )
                .without_memory()
                .with_runtime_event_store(cold_services.event_store().clone())
                .with_artifact_store(artifacts.clone()),
            )));
            let cold_state = Arc::new(tokio::sync::Mutex::new(model_publication_state475(
                &original_session.session_id,
                "475-no-goal-proof",
            )));
            cold_services
                .model_step_executor()
                .install_resolver(Arc::new(TurnModelResolver {
                    session_id: original_session.session_id.clone(),
                    graph_id: closed.id.clone(),
                    runtime: Arc::downgrade(&cold),
                    state: Arc::downgrade(&cold_state),
                    services: Arc::downgrade(&cold_services),
                }));
            assert!(cold_services.graph_executors_are_bound(&closed).unwrap());
            assert_eq!(
                cold_services
                    .execution_supervisor()
                    .recover_graph(&closed.id)
                    .await
                    .unwrap(),
                closed
            );
            let restored = cold
                .lock()
                .await
                .session_snapshot()
                .await
                .materialize_messages();
            assert_eq!(
                restored,
                vec![
                    ConversationMessage::user_text("original user prefix"),
                    message
                ]
            );
            {
                let mut runtime = cold.lock().await;
                runtime
                    .session_mut_async()
                    .await
                    .extend_messages([ConversationMessage::user_text("next user input")]);
            }
            let before = cold
                .lock()
                .await
                .session_snapshot()
                .await
                .materialize_messages();
            assert_eq!(
                cold_services
                    .execution_supervisor()
                    .recover_graph(&closed.id)
                    .await
                    .unwrap(),
                closed
            );
            assert_eq!(
                cold.lock()
                    .await
                    .session_snapshot()
                    .await
                    .materialize_messages(),
                before
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(cold_services
                .event_store()
                .session_terminal_for_execution(
                    &original_session.session_id,
                    &closed.id,
                    "475-turn"
                )
                .unwrap()
                .is_none());
            let store = ExecutionGraphStateStore::new(cold_services.event_store().clone());
            assert!(store
                .binding_lease_closure(&closed, &node.id, &binding, position)
                .unwrap());
            let publication = cold_services
                .event_store()
                .list_stream(&format!("{}:node:{}", closed.id, node.id))
                .unwrap()
                .into_iter()
                .find(|event| event.kind == "execution.host.transcript")
                .unwrap();
            let digest = publication.payload["body"]["body"]["artifact"]["sha256"]
                .as_str()
                .unwrap()
                .strip_prefix("sha256:")
                .unwrap()
                .to_string();
            let object_directory = root
                .path()
                .join("blobs")
                .join("objects")
                .join(&digest[..2])
                .join(digest);
            assert!(
                publication.payload["body"]["body"]["artifact"]["bytes"]
                    .as_u64()
                    .unwrap()
                    > artifacts.config().compact_threshold_bytes
            );
            assert!(object_directory.is_dir());
            std::fs::remove_dir_all(object_directory).unwrap();
            assert!(cold_services.graph_executors_are_bound(&closed).is_err());
            let failed = cold_services
                .recover_execution_graphs_on_startup()
                .await
                .unwrap();
            assert!(failed
                .errors
                .iter()
                .any(|error| error.graph_id == closed.id));
            assert!(failed
                .records
                .iter()
                .any(|record| record.graph_id == closed.id
                    && record.action == "host_publication_readiness_failed"
                    && record.error.is_some()));
            assert!(!failed
                .records
                .iter()
                .any(|record| record.graph_id == closed.id
                    && record.action == "deferred_turn_scoped_executor_binding"));
            assert_eq!(
                cold_services.graph_state_store().load(&closed.id).unwrap(),
                closed
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            services.shutdown_execution().await;
            services.shutdown_maintenance().await;
            cold_services.shutdown_execution().await;
            cold_services.shutdown_maintenance().await;
        }
    }
    fn model_publication_state475(session_id: &str, goal_id: &str) -> TurnGraphState {
        TurnGraphState {
            content: "answer directly".into(),
            task_understanding: Some(harness_contract::strategy::understand(
                &harness_contract::strategy::StrategyInput::from_prompt("answer directly"),
            )),
            collaboration_obligation: None,
            prompter: SharedPrompter::none(),
            first_model_step: true,
            pending_next_model_context: Vec::new(),
            persistent_collaboration_context: Vec::new(),
            assistant_messages: Vec::new(),
            tool_results: Vec::new(),
            iterations: 0,
            input_tokens: 0,
            output_tokens: 0,
            cache_create_tokens: 0,
            cache_read_tokens: 0,
            cache_dimensions_known: false,
            provider_usage_attempt_observed: false,
            output_chars: 0,
            output_chunks: 0,
            wall_duration_ms: 0,
            model: None,
            models_used: Vec::new(),
            first_token_latency_ms: None,
            active_stream_duration_ms: 0,
            summary: None,
            failure: None,
            terminal_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
            pending_transcript: std::collections::BTreeMap::new(),
            pending_member_batches: std::collections::BTreeMap::new(),
            pending_agent_input_consumption: std::collections::BTreeMap::new(),
            deferred_agent_input_consumption: Vec::new(),
            ingress: None,
            turn_transcript_start: 0,
            session_id: session_id.into(),
            turn_id: "475-turn".into(),
            goal_id: goal_id.into(),
            context_window: 0,
            safety_lease: crate::execution_core::SafetyFusePolicy::derive(
                0,
                harness_contract::core::TaskComplexity::Simple,
                None,
            ),
            terminal_override: None,
            delivery_envelope: None,
            terminal_presentation: None,
            terminal_commit_owner: None,
            authenticated_terminal_owner: None,
            committed_terminal_answer: None,
            committed_terminal_completion: None,
            last_verified_progress: false,
            consecutive_unverified_model_steps: 0,
            closure_ready_steps: 0,
            previous_objective_milestones: Default::default(),
            verified_collaboration_progress: Default::default(),
            verified_answer_ready_steps: 0,
            final_answer_observed: false,
            collaboration_convergence_replans: 0,
            delegated_redrives: 0,
            completion_gap_continuations: 0,
            started_at: std::time::Instant::now(),
            reasoning_only_attempts: 0,
            force_text_only_next_model: false,
            force_tool_allowlist_next_model: None,
            force_tool_call_next_model: false,
            force_reasoning_effort_next_model: None,
            terminal_recovery_attempts: 0,
            provider_protocol_recovery_attempts: 0,
            provider_frame_repair_attempts: std::collections::BTreeMap::new(),
            agentic_protocol_recovery_attempts: 0,
            agentic_program_context_revision: None,
            execution_role: TurnExecutionRole::RootTurn,
            bounded_evidence_role: false,
            focus_novelty_target_bp: 0,
            focus_acceptance_scopes: Vec::new(),
            focus_acceptance_pending_scopes: Vec::new(),
            focus_required_output_fields: Vec::new(),
            structured_output_replans: 0,
            focus_observed_resource_scopes: BTreeSet::new(),
            focus_observed_evidence: Vec::new(),
            focus_action_rejections: 0,
            pending_focus_terminal_candidate: None,
            focus_discovery_replans: 0,
            focus_verification_prefetched: false,
            clean_terminal_synthesis_next: false,
            clean_terminal_synthesis_attempted: false,
            clean_terminal_retry_attempted: false,
            terminal_failure_narration: None,
            consecutive_tool_failure_batches: 0,
            cold_start_failure_replans: 0,
            consecutive_low_novelty_batches: 0,
            successful_tool_calls: 0,
            last_terminal_answer_digest: String::new(),
            repeated_terminal_answers: 0,
            completion_gap_outstanding: false,
            outstanding_closure_rungs: Vec::new(),
            tool_receipts_observed: 0,
            duplicate_tool_calls: 0,
            identical_action_digest: String::new(),
            identical_action_result_digest: String::new(),
            identical_action_streak: 0,
            per_turn_tool_counts: BTreeMap::new(),
            write_attempt_paths: Vec::new(),

            committed_workspace_write_observed: false,
            committed_workspace_write_scopes: BTreeSet::new(),
            committed_workspace_observed_evidence: Vec::new(),
            workspace_mutation_effect_sequences: BTreeSet::new(),

            max_tool_concurrency_observed: 0,
            parallel_tool_batches: 0,
            early_tool_receipts: BTreeMap::new(),
            evaluation_resource_scopes: Vec::new(),
            evaluation_scope_rejections: 0,
            evaluation_judge_only: false,
            collaboration_started: false,
            pending_root_control_plane_requirement: None,

            root_language_replan_attempted: false,
            pending_terminal_artifact: None,
            pending_controlled_recovery_claim_fingerprints: Vec::new(),
            pending_disposition_inputs: Vec::new(),
            terminal_tracker: crate::liveness::TerminalTracker::new(
                crate::liveness::TerminalObligation::default(),
            ),
            terminal_satisfaction_since: None,
        }
    }
}

/// Negative controls for the outer certification of a node's Program source.
///
/// The gap attribution this file used to carry is gone: it re-derived who
/// advanced the Program, which was a second authority and made every
/// legitimate concurrency look like an anomaly. What still refuses an
/// unrelated Program write is the node transaction's own CAS, and what still
/// refuses work that never reached the Program is the visibility check on
/// this node's own certified effects. These tests pin both.
#[cfg(test)]
mod original_program_visibility_tests {
    use super::{original_effect_judgment, OriginalEffectJudgment, OriginalProgramEntityView, OriginalTaskView};

    fn view() -> OriginalProgramEntityView {
        let mut view = OriginalProgramEntityView::default();
        view.tasks.insert("task:abc".to_string(), OriginalTaskView {
            claim_generation: 1, review_generation: 0, reviewed_by: false, review_outcome: false });
        view.tasks.insert("task:accepted".to_string(), OriginalTaskView {
            claim_generation: 1, review_generation: 0, reviewed_by: true, review_outcome: true });
        view.tasks.insert("task:quiet".to_string(), OriginalTaskView::default());
        view.teams.insert("team:abc".to_string());
        view.agents.insert("agent:abc".to_string());
        view.artifacts.insert("artifact:abc".to_string());
        view
    }

    fn carried(view: &OriginalProgramEntityView, effect: &str, kind: &str) -> bool {
        original_effect_judgment(view, effect, kind) == OriginalEffectJudgment::Carried
    }

    #[test]
    fn objective_review_requires_its_same_action_goal_record() {
        use harness_contract::goal::{AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract, GoalScope, ObjectiveReviewRecord};
        let review_id = crate::execution_core::goal::objective_review_record_id("goal:test", "action:test", "criterion");
        let mut goal = GoalContract {
            input_contract: None, id: "goal:test".into(), session_id: "session:test".into(),
            objective: "test".into(), criteria: vec![AcceptanceCriterion { id: "criterion".into(), statement: "test".into(), statement_ref: None, source_refs: vec![], required_evidence: vec![], status: AcceptanceStatus::Open, waiver: None }],
            constraints: vec![], phase: "execution".into(), evidence_refs: vec![], unresolved: vec![], blockers: vec![], scope: GoalScope::Internal,
            user_intent_criterion_id: None, source_intent_ref: None, execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
                objective_id: "objective:test".into(), session_id: "session:test".into(), turn_id: "turn:test".into(), root_execution_id: "root:test".into(), agentic_program_id: "program:test".into(),
            }), spec_revision: 1, spec_digest: "digest".into(),
            review_refs: vec![review_id.clone()], reviews: vec![ObjectiveReviewRecord {
                review_id: review_id.clone(), criterion_ref: "criterion".into(), spec_revision: 1, input_manifest_digest: "digest".into(),
                result_refs: vec!["artifact:result".into()], evidence_refs: vec!["tool:read".into()], reviewer_actor: "root".into(),
                reviewer_execution_id: None, producer_refs: vec![], decision: "satisfied".into(), reason_ref: "reason".into(), verification: None,
            }], waiting: None, participation_requirement: None, obligations: vec![], recovery: None, terminal: None,
            completion: GoalCompletion::Open, revision: 1, user_sequence: 1,
        };
        let program = serde_json::json!({"entity_ref": review_id, "envelope": {
            "action_id": "action:test", "actor": {"objective_id":"objective:test", "program_id":"program:test", "session_id":"session:test", "turn_id":"turn:test", "root_execution_id":"root:test", "actor_id":"root", "kind":"root"},
            "action": {"kind":"objective_review", "input": {"criterion_ref":"criterion", "decision":"satisfied", "result_refs":["artifact:result"], "evidence_refs":["tool:read"], "reason_ref":"", "reason":"review"}}
        }});
        let goal_event = serde_json::json!({"action_entity_ref":review_id, "goal":goal});
        assert!(super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        let program_stream = "agentic-program:program:test";
        let goal_stream = "goal:goal:test";
        let event = |stream: &str, scope, kind: &str, payload| crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput { stream_id: stream.into(), scope, kind: kind.into(), status: Some("applied".into()), actor: Some("test".into()), refs: vec![], payload },
            idempotency_key: Some(format!("{kind}:test")), schema_version: 1,
        };
        let transaction = crate::AppendTransactionRequest {
            transaction_id: "review:test".into(),
            expected_streams: vec![crate::ExpectedStreamRevision { stream_id: program_stream.into(), expected_revision: 0 }, crate::ExpectedStreamRevision { stream_id: goal_stream.into(), expected_revision: 0 }],
            events: vec![event(program_stream, crate::RuntimeEventScope::Program, "agentic.action_applied", program.clone()),
                event(goal_stream, crate::RuntimeEventScope::Goal, "goal.revised", goal_event.clone())],
        };
        let store = crate::RuntimeEventStore::for_test();
        let receipt = store.append_transaction(transaction.clone()).expect("original Goal and Program transaction");
        assert!(super::original_goal_review_transaction(&transaction, &receipt, program_stream, goal_stream, &goal, &goal).unwrap().is_some());
        let mut foreign_receipt = receipt.clone();
        foreign_receipt.transaction_id = "other".into();
        assert!(super::original_goal_review_transaction(&transaction, &foreign_receipt, program_stream, goal_stream, &goal, &goal).is_err());
        let mut missing_goal_receipt = receipt.clone();
        missing_goal_receipt.event_ids.pop();
        assert!(super::original_goal_review_transaction(&transaction, &missing_goal_receipt, program_stream, goal_stream, &goal, &goal).is_err());
        goal.reviews[0].criterion_ref = "other".into();
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        goal.reviews[0].criterion_ref = "criterion".into();
        goal.reviews[0].reviewer_actor = "other".into();
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        goal.reviews[0].reviewer_actor = "root".into();
        goal.reviews[0].result_refs.clear();
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        goal.reviews[0].result_refs = vec!["artifact:result".into()];
        assert!(!super::original_goal_review_carried(&program, &serde_json::json!({"action_entity_ref":"objective_review:other"}), &goal, &goal));
        goal.reviews[0].decision = "gap".into();
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        goal.reviews[0].decision = "satisfied".into();
        goal.review_refs.clear();
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        goal.review_refs.push(review_id);
        goal.spec_revision += 1;
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        goal.spec_revision -= 1;
        goal.spec_digest = "different".into();
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
        goal.spec_digest = "digest".into();
        goal.execution_binding.as_mut().unwrap().turn_id = "other".into();
        assert!(!super::original_goal_review_carried(&program, &goal_event, &goal, &goal));
    }

    #[test]
    fn original_effect_visibility_is_kind_scoped() {
        let view = view();
        // A Task effect is only satisfied by a Task. The same bare id owned by
        // a Team or an Agent must not stand in for it.
        assert!(carried(&view, "task:abc", "task_publish"));
        assert!(!carried(&view, "task:missing", "task_publish"));
        assert!(carried(&view, "team:abc", "team_create"));
        assert!(carried(&view, "agent:abc", "agent_invite"));
        assert!(carried(&view, "artifact:abc", "artifact_commit"));
        // An unknown kind is never visible.
        assert!(!carried(&view, "topic:abc", "message_publish"));
    }

    #[test]
    fn original_task_effect_requires_the_matching_program_fact() {
        let view = view();
        // A claim is only carried when the projection's claim generation moved.
        assert!(carried(&view, "task:abc", "task_claim"));
        assert!(!carried(&view, "task:quiet", "task_claim"));
        // A review is carried by the reviewer's own record or the settled
        // status. An accepted review does not advance the review generation.
        assert!(carried(&view, "task:accepted", "task_review"));
        assert!(!carried(&view, "task:abc", "task_review"));
    }

    #[test]
    fn original_message_effect_carries_no_program_entity_claim() {
        let view = view();
        // A published message is named by a digest in its own id space, so no
        // Program entry can attest it. It must not be read as an unattested
        // effect, and it must not be read as a carried one either.
        assert_eq!(
            original_effect_judgment(&view, "message:abc", "message_publish"),
            OriginalEffectJudgment::NoEntityClaim);
    }

    #[test]
    fn program_write_after_the_head_read_makes_the_node_transaction_stale() {
        // The node declares the Program head it read. A write that lands
        // after that read moves the head, and the node's own transaction -
        // which fences on the declared revision - must fail closed.
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let stream = "agentic-program:test";
        store.append(crate::RuntimeEventInput {
            stream_id: stream.to_string(),
            scope: crate::RuntimeEventScope::Program,
            kind: "agentic.action_applied".to_string(),
            status: Some("applied".to_string()),
            actor: Some("runtime.agentic_action".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"entity_ref": null}),
        }).expect("first Program write");
        let head = store.stream_revision(stream).expect("Program head");
        // An unrelated Program write lands after the node read the head.
        store.append(crate::RuntimeEventInput {
            stream_id: stream.to_string(),
            scope: crate::RuntimeEventScope::Program,
            kind: "agentic.action_applied".to_string(),
            status: Some("applied".to_string()),
            actor: Some("runtime.agentic_action".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"entity_ref": null}),
        }).expect("second Program write");
        let request = crate::AppendTransactionRequest {
            transaction_id: "node-transition:test".to_string(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: stream.to_string(),
                expected_revision: head,
            }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream.to_string(),
                    scope: crate::RuntimeEventScope::Program,
                    kind: "goal.revised".to_string(),
                    status: Some("revised".to_string()),
                    actor: Some("conversation_runtime".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({}),
                },
                idempotency_key: Some("goal-revised:test".to_string()),
                schema_version: 1,
            }],
        };
        let outcome = store.append_transaction(request);
        assert!(matches!(outcome, Err(crate::RuntimeEventStoreError::StaleRevision { .. })),
            "a Program write after the declared head must fail the node transaction closed");
    }

    /// T5-N1: the certification window is the cut this node's own commits
    /// produced, never the live head. A later legitimate advance by another
    /// writer must not be able to turn this node's own carried effect into a
    /// batch-blocking refusal.
    #[test]
    fn t5_certification_reads_the_nodes_own_cut_and_not_the_live_head() {
        let own_cut = 14;
        let live_head = 20;
        assert_eq!(
            super::original_program_certification_cut(own_cut, live_head).expect("forward is fine"),
            own_cut,
            "the certified window is the node's own commit cut"
        );
        // The effect this node produced (a claim) is visible at its own cut, and
        // an unrelated writer's later rework reset it at the live head.
        let mut at_own_cut = OriginalProgramEntityView::default();
        at_own_cut.tasks.insert("task:abc".to_string(), OriginalTaskView {
            claim_generation: 1, review_generation: 0, reviewed_by: false, review_outcome: false });
        let mut at_live_head = OriginalProgramEntityView::default();
        at_live_head.tasks.insert("task:abc".to_string(), OriginalTaskView::default());
        let effects = vec![("task:abc".to_string(), "task_claim".to_string())];
        assert!(
            super::certify_original_program_effects(&at_own_cut, &effects, own_cut, live_head).is_ok(),
            "work this node certified at its own cut stays carried"
        );
        let blocked = super::certify_original_program_effects(&at_live_head, &effects, live_head, live_head)
            .expect_err("the old window is recorded so the regression cannot come back silently");
        assert!(blocked.contains("effect=task:abc"), "{blocked}");
    }

    /// T5-N2: a genuinely absent effect is still refused, and the refusal names
    /// the effect, the action kind, the certified cut and the live head. Nothing
    /// is softened: the only change is that the sentence now carries its own
    /// evidence instead of only a batch-level wrapper.
    #[test]
    fn t5_a_true_absent_effect_is_still_refused_with_named_evidence() {
        let view = view();
        let effects = vec![("task:missing".to_string(), "task_publish".to_string())];
        let error = super::certify_original_program_effects(&view, &effects, 7, 9)
            .expect_err("an effect nobody produced is not carried");
        assert!(
            error.starts_with("original Program effect is not visible at the certified head"),
            "the original sentence survives untouched as the prefix: {error}"
        );
        assert_eq!(
            error,
            "original Program effect is not visible at the certified head; effect=task:missing action=task_publish certified_cut=7 live_head=9"
        );
    }

    /// T5-N3: the placeholder shape that really blocked the 029 small-team run
    /// (`":null"` as an agent ref) is refused at the node's own cut too. This
    /// ticket does not launder the validation gap that belongs to 030-C1: an
    /// unsplitable effect fails at every cut, which is exactly why it is a
    /// separate face from the window choice.
    #[test]
    fn t5_a_placeholder_ref_is_still_refused_and_named() {
        let view = view();
        let effects = vec![(":null".to_string(), "agent_invite".to_string())];
        let error = super::certify_original_program_effects(&view, &effects, 3, 3)
            .expect_err("a placeholder ref is not a Program entity at any cut");
        assert!(error.contains("effect=:null"), "{error}");
        assert!(error.contains("action=agent_invite"), "{error}");
    }

    /// T5-N4: a stream that fell below this node's own cut keeps its original
    /// refusal byte for byte, and the node's declared write head is still the
    /// live head, so the CAS fence is untouched by the window change.
    #[test]
    fn t5_a_backwards_stream_keeps_the_original_refusal_and_the_fence() {
        assert_eq!(
            super::original_program_certification_cut(20, 17)
                .expect_err("a truncated or replaced stream is not a window"),
            "Program changed between original actions"
        );
        assert_eq!(
            super::original_program_certification_cut(20, 20).expect("equal head is the node's own cut"),
            20
        );
    }
}

#[cfg(test)]
mod original_call_request_id_tests {
    use super::original_call_request_id;

    /// The producer side of the grant call-identity contract. The Host must
    /// spell the call identity as the bare call id, because the durable
    /// local-network binding compares the grant and its lease against this
    /// call's own `tool_use_id`. A composite spelling reaches the grant and
    /// makes every network effect on this path unclosable.
    #[test]
    fn the_host_spells_the_call_identity_as_the_bare_call_id() {
        assert_eq!(original_call_request_id("chatcmpl-tool-1"), "chatcmpl-tool-1");
    }

    /// The regression guard: the composite that once reached the grant must not
    /// come back, in any of the shapes this tree uses elsewhere.
    #[test]
    fn no_composite_spelling_reaches_the_grant() {
        for call in ["chatcmpl-tool-1", "call-2", "801-call"] {
            let spelled = original_call_request_id(call);
            assert_eq!(spelled, call, "{call}");
            assert!(!spelled.contains(':'), "{spelled}");
        }
    }
}
