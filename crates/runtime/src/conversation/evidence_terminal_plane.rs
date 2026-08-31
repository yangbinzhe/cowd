//! Terminal evidence, strategy ledger, runtime events, and outcome finalization.

use super::*;

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    pub fn set_execution_policy(
        &self,
        policy: harness_contract::policy::SessionExecutionPolicy,
    ) -> Result<u64, String> {
        self.permission_policy
            .execution_policy_control()
            .replace(policy)
    }

    #[must_use]
    pub fn approval_profile(&self) -> harness_contract::policy::ApprovalProfile {
        self.permission_policy
            .execution_policy_control()
            .snapshot()
            .approval_profile
    }

    #[must_use]
    pub fn autonomy_profile(&self) -> crate::AutonomyProfileId {
        self.permission_policy
            .execution_policy_control()
            .snapshot()
            .autonomy_profile
    }

    #[must_use]
    pub(crate) fn active_permission_mode(&self) -> crate::PermissionMode {
        self.permission_policy.active_mode()
    }

    #[must_use]
    pub fn tool_timeout(&self) -> Option<std::time::Duration> {
        self.tool_timeout
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[allow(
        clippy::panic,
        reason = "a synchronous snapshot boundary cannot return an error; a failed scoped reader violates the session read contract"
    )]
    pub(super) fn with_session_read_blocking<R, F>(&self, read: F) -> R
    where
        R: Send,
        F: FnOnce(&Session) -> R + Send,
    {
        if let Ok(session) = self.session.try_read() {
            return read(&session);
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                return tokio::task::block_in_place(|| read(&self.session.blocking_read()));
            }

            // `block_in_place` is unsupported by a current-thread Tokio runtime.
            // Keep the explicitly synchronous boundary off that executor.
            let session = Arc::clone(&self.session);
            return std::thread::scope(|scope| {
                scope
                    .spawn(move || read(&session.blocking_read()))
                    .join()
                    .unwrap_or_else(|_| {
                        panic!("session read worker terminated before returning a session")
                    })
            });
        }
        read(&self.session.blocking_read())
    }

    pub(super) fn read_head(session: &Session) -> SessionReadHead {
        let history = session.history();
        let weight = history.weight();
        SessionReadHead {
            message_count: session.message_count(),
            history_revision: history.revision(),
            history_bytes: weight.bytes,
            history_tokens: weight.tokens,
            updated_at_ms: session.updated_at_ms,
            model: session.model.clone(),
        }
    }

    #[must_use]
    pub fn session_head_blocking(&self) -> SessionReadHead {
        self.with_session_read_blocking(Self::read_head)
    }

    pub async fn session_head(&self) -> SessionReadHead {
        let session = self.session.read().await;
        Self::read_head(&session)
    }

    #[must_use]
    pub fn session_snapshot_blocking(&self) -> Session {
        self.with_session_read_blocking(Clone::clone)
    }

    pub async fn session_snapshot(&self) -> Session {
        self.session.read().await.clone()
    }

    pub fn api_client_mut(&mut self) -> &mut C {
        &mut self.api_client
    }

    #[must_use]
    pub fn request_compiler_stats(&self) -> crate::RequestCompilerStats {
        self.request_compiler.stats()
    }

    pub(crate) async fn session_mut_async(&mut self) -> tokio::sync::RwLockWriteGuard<'_, Session> {
        self.session.write().await
    }

    pub async fn append_external_message(
        &self,
        message: ConversationMessage,
    ) -> Result<(), RuntimeError> {
        let mut session = self.session.write().await;
        session
            .push_message(message.clone())
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let sequence = session.message_count().wrapping_sub(1);
        drop(session);
        self.record_message_event(&message, sequence);
        Ok(())
    }

    #[must_use]
    pub fn fork_session(&self, branch_name: Option<String>) -> Session {
        self.session.blocking_read().fork(branch_name)
    }

    #[must_use]
    pub fn into_session(self) -> Session {
        Arc::try_unwrap(self.session)
            .map(|lock| lock.into_inner())
            .unwrap_or_else(|arc| arc.blocking_read().clone())
    }

    pub(super) async fn compact_session_with_checkpoint(
        &mut self,
        config: CompactionConfig,
    ) -> Result<Option<AutoCompactionEvent>, RuntimeError> {
        if self.session_journal_port.is_none() {
            return Err(RuntimeError::new(
                "semantic compaction requires a durable Session journal port; transcript was retained",
            ));
        }
        let original_session = self.session.read().await.clone();
        let Some(plan) = plan_session_compaction(&original_session, config) else {
            return Ok(None);
        };
        let original_messages = original_session.materialize_messages();

        let source_messages = compacted_source_messages(
            &original_messages,
            plan.source_message_start,
            plan.source_message_end,
        );
        let raw_refs = source_message_evidence_refs(
            &original_session.session_id,
            &original_messages,
            plan.source_message_start,
            plan.source_message_end,
        );
        let checkpoint = if self.semantic_checkpoint_enabled && !source_messages.is_empty() {
            let mem_messages = conversation_messages_to_mem_messages(source_messages);
            let source_range = CompactionSourceRange {
                session_id: original_session.session_id.clone(),
                message_start: plan.source_message_start,
                message_end_exclusive: plan.source_message_end,
                event_start: Some(plan.source_message_start),
                event_end_exclusive: Some(plan.source_message_end),
                raw_refs: raw_refs.clone(),
            };
            let ctx = self.memory_turn_context();
            let checkpoint_id = deterministic_checkpoint_id(
                &original_session.session_id,
                plan.source_message_start,
                plan.source_message_end,
                plan.existing_summary.as_deref(),
            );
            let execution_identity = match self.execution_identity.clone() {
                Some(identity) => identity,
                None => {
                    let turn_id = self.session_input_stream.active_turn_id().map_or_else(
                        || format!("checkpoint-turn:{checkpoint_id}"),
                        |id| id.to_string(),
                    );
                    harness_contract::execution::ExecutionIdentity::for_session_turn(
                        self.memory_agent_id.clone(),
                        self.checkpoint_workspace_id.clone(),
                        original_session.session_id.clone(),
                        turn_id,
                    )
                    .map_err(|error| {
                        RuntimeError::new(format!(
                            "semantic checkpoint execution identity is invalid: {error}"
                        ))
                    })?
                }
            };
            let build_context = SessionCheckpointBuildContext::new(
                original_session.session_id.clone(),
                ctx.agent_id.clone(),
                source_range,
            )
            .with_checkpoint_id(checkpoint_id)
            .with_execution_identity(execution_identity)
            .with_project_id(ctx.project_id.clone())
            .with_task_id(ctx.task_id.clone())
            .with_team_id(ctx.team_id.clone());
            match SessionCompactor::new()
                .with_max_summary_tokens(self.session_compaction_config.summary_max_tokens)
                .build_checkpoint(
                    &mem_messages,
                    plan.existing_summary.as_deref(),
                    build_context,
                )
                .await
            {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    return Err(RuntimeError::new(format!(
                        "semantic compaction checkpoint build failed; transcript was retained: {error}"
                    )));
                }
            }
        } else {
            return Err(RuntimeError::new(
                "semantic compaction requires an enabled memory checkpoint; transcript was retained",
            ));
        };

        // Runtime never synthesizes a second lossy summary. The Memory
        // checkpoint is the sole continuation artifact and the source of all
        // durable fact extraction below.
        let result = apply_compaction_summary(&original_session, plan, checkpoint.summary.clone());

        let fact_extraction_decision = RuntimeFactExtractionScheduler::default()
            .decide(RuntimeFactExtractionTrigger::SessionCompaction);

        let mut receipt = Some({
            let fact_extraction_event = FactExtractionRuntimeEvent::from_decision(
                &fact_extraction_decision,
                "memory-session-checkpoint:v1",
                checkpoint.facts.len(),
                checkpoint.source_range.raw_refs.len(),
                FactExtractionTokenUsage {
                    input_tokens: checkpoint.token_stats.before,
                    output_tokens: checkpoint.token_stats.after,
                    total_tokens: checkpoint
                        .token_stats
                        .before
                        .saturating_add(checkpoint.token_stats.after),
                },
            );
            let mut receipt = CompactionReceipt::new(
                "runtime_auto_compaction",
                checkpoint.token_stats.before,
                checkpoint.token_stats.after,
            )
            .with_evidence_ref(
                EvidenceRef::observed("checkpoint", checkpoint.checkpoint_id.clone())
                    .with_source("semantic_compaction_checkpoint"),
            )
            .with_evidence_ref(
                EvidenceRef::observed(
                    "fact-extraction",
                    fact_extraction_decision.mode.as_str().to_string(),
                )
                .with_source(fact_extraction_event.evidence_label()),
            );
            receipt
                .retained_artifact_ids
                .push(format!("checkpoint:{}", checkpoint.checkpoint_id));
            receipt.retained_artifact_ids.push(format!(
                "fact-extraction:{}",
                fact_extraction_decision.mode.as_str()
            ));
            for evidence in &checkpoint.source_range.raw_refs {
                receipt.evidence_refs.push(evidence.clone());
                receipt
                    .dropped_artifact_ids
                    .push(format!("{}:{}", evidence.ref_type, evidence.id));
            }
            receipt
        });

        tracing::info!(removed = result.removed_message_count, "compaction");
        let compacted_len = result.compacted_session.message_count();
        let compaction = result.compacted_session.compaction.clone().ok_or_else(|| {
            RuntimeError::new("semantic compaction did not produce a session compaction record")
        })?;
        let newly_committed = self
            .record_session_compacted(
                compaction,
                compacted_len,
                receipt.clone(),
                checkpoint.clone(),
            )
            .await?;
        // The checkpoint boundary is now durable. Fact projection is
        // intentionally replayable: if a process stopped after the event
        // transaction but before Memory writes, the next attempt recreates
        // exactly the same deterministic memory IDs instead of losing facts
        // or emitting duplicates.
        if !newly_committed {
            tracing::info!(checkpoint_id = %checkpoint.checkpoint_id, "replaying semantic checkpoint fact projection");
        }
        if let (Some(mgr), Some(receipt_mut)) = (&self.memory_manager, receipt.as_mut()) {
            let ctx = self.memory_turn_context();
            let kernel = MemoryKernel::new(Arc::clone(mgr));
            match kernel.checkpoint_compaction(&ctx, checkpoint).await {
                Ok(memory_receipt) => {
                    receipt_mut.retained_artifact_ids.extend(
                        memory_receipt
                            .memory_ids
                            .iter()
                            .map(|id| format!("memory:{id}")),
                    );
                    receipt_mut.retained_artifact_ids.push(format!(
                        "fact-review:{}",
                        memory_receipt.fact_review.batch_id.as_str()
                    ));
                    receipt_mut.evidence_refs.push(
                        EvidenceRef::observed(
                            "fact-review",
                            memory_receipt.fact_review.batch_id.as_str().to_string(),
                        )
                        .with_source(format!(
                            "promoted={} held={} rejected={} conflicts={}",
                            memory_receipt.fact_review.promoted.len(),
                            memory_receipt.fact_review.held.len(),
                            memory_receipt.fact_review.rejected.len(),
                            memory_receipt.fact_review.conflicts.len()
                        )),
                    );
                }
                Err(error) => {
                    tracing::warn!(%error, "semantic compaction fact projection deferred");
                    receipt_mut.evidence_refs.push(
                        EvidenceRef::observed(
                            "memory",
                            "semantic_checkpoint_fact_projection_deferred",
                        )
                        .with_source(error.to_string()),
                    );
                }
            }
        }
        *self.session.write().await = result.compacted_session;
        Ok(Some(AutoCompactionEvent {
            removed_message_count: result.removed_message_count,
            compaction_receipt: receipt,
        }))
    }

    pub(super) fn compaction_config_for_session(
        &self,
        max_estimated_tokens: usize,
    ) -> CompactionConfig {
        CompactionConfig {
            preserve_recent_messages: self.session_compaction_config.preserve_recent as usize,
            max_estimated_tokens,
            priority_threshold: 3,
            keep_high_priority: true,
        }
    }

    pub(super) async fn record_session_compacted(
        &self,
        compaction: crate::session::SessionCompaction,
        sequence: usize,
        receipt: Option<CompactionReceipt>,
        semantic_checkpoint: SessionSemanticCheckpoint,
    ) -> Result<bool, RuntimeError> {
        let port = self.session_journal_port.as_ref().ok_or_else(|| {
            RuntimeError::new(
                "semantic compaction requires a durable Session journal port; transcript was retained",
            )
        })?;
        let session_id = self.session_id().to_string();
        let payload = serde_json::json!({
            "type": "SessionCompacted",
            "sequence": sequence,
            "compaction": {
                "count": compaction.count,
                "removed_message_count": compaction.removed_message_count,
                "summary": compaction.summary,
            },
            "receipt": receipt,
        });
        let created_at_ms = now_ms();
        let context_event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            0,
            crate::RuntimeSessionEventKind::ContextSessionCompacted,
            payload,
            created_at_ms,
        );
        let checkpoint_id = semantic_checkpoint.checkpoint_id.clone();
        let compaction_event_id = format!("compaction:{session_id}:{checkpoint_id}");
        let events = vec![
            context_event,
            crate::RuntimeSessionEvent::new(
                session_id.clone(),
                0,
                crate::RuntimeSessionEventKind::MemorySemanticCheckpointCreated,
                serde_json::json!({
                    "source": "conversation_runtime.compaction",
                    "compaction_event_id": compaction_event_id,
                    "checkpoint": semantic_checkpoint,
                    "receipt": receipt,
                }),
                created_at_ms,
            ),
        ];
        let committed = port
            .append_compaction_bundle_if_absent(&events, &checkpoint_id)
            .await;
        let committed = match committed {
            Ok(true) => true,
            Ok(false) => {
                tracing::info!(session_id, checkpoint_id = %checkpoint_id, "reusing committed semantic compaction bundle");
                false
            }
            Err(error) => {
                return Err(RuntimeError::new(format!(
                    "atomic compaction persistence failed for session `{session_id}`; transcript was retained: {error}"
                )));
            }
        };
        Ok(committed)
    }

    pub(super) fn record_turn_started(&self, user_input: &str) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert(
            "user_input".to_string(),
            Value::String(user_input.to_string()),
        );
        session_tracer.record("turn_started", attributes);
    }

    #[allow(dead_code)]
    pub(super) fn record_assistant_iteration(
        &self,
        iteration: usize,
        assistant_message: &ConversationMessage,
        pending_tool_use_count: usize,
    ) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert(
            "assistant_blocks".to_string(),
            Value::from(assistant_message.blocks.len() as u64),
        );
        attributes.insert(
            "pending_tool_use_count".to_string(),
            Value::from(pending_tool_use_count as u64),
        );
        session_tracer.record("assistant_iteration_completed", attributes);
    }

    pub(super) fn record_tool_started(&self, iteration: usize, tool_name: &str) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert(
            "tool_name".to_string(),
            Value::String(tool_name.to_string()),
        );
        session_tracer.record("tool_execution_started", attributes);
    }

    #[allow(dead_code)]
    pub(super) fn record_tool_finished(
        &self,
        iteration: usize,
        result_message: &ConversationMessage,
    ) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let Some(ContentBlock::ToolResult {
            tool_name,
            is_error,
            ..
        }) = result_message.blocks.first()
        else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert("tool_name".to_string(), Value::String(tool_name.clone()));
        attributes.insert("is_error".to_string(), Value::Bool(*is_error));
        session_tracer.record("tool_execution_finished", attributes);
    }

    pub(super) fn emit_tool_started(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        causal_parent_ids: &[String],
    ) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit_tool_started_with_dependencies(
            tool_use_id,
            tool_name,
            &preview_chars(input, 200),
            causal_parent_ids,
        );
    }

    pub(super) fn emit_tool_completed(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        output: &str,
        exit_code: Option<i32>,
        causal_parent_ids: &[String],
    ) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit_tool_completed_with_dependencies(
            tool_use_id,
            tool_name,
            &preview_chars(output, 500),
            exit_code,
            causal_parent_ids,
        );
    }

    pub(super) fn record_turn_completed(&self, summary: &TurnSummary) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert(
            "iterations".to_string(),
            Value::from(summary.iterations as u64),
        );
        attributes.insert(
            "assistant_messages".to_string(),
            Value::from(summary.assistant_messages.len() as u64),
        );
        attributes.insert(
            "tool_results".to_string(),
            Value::from(summary.tool_results.len() as u64),
        );
        session_tracer.record("turn_completed", attributes);
    }

    /// Perform post-turn memory housekeeping (micro-compact, drift, seeds).
    ///
    /// Errors are logged and swallowed so a memory failure never aborts a turn.
    pub(super) async fn run_memory_post_turn(&self, user_input: &str) -> Result<(), RuntimeError> {
        let Some((mgr, memory_ctx, mem_messages, callback)) =
            self.memory_post_turn_work(user_input).await
        else {
            return Ok(());
        };
        Self::complete_memory_post_turn(mgr, memory_ctx, mem_messages, callback).await;
        Ok(())
    }

    /// Gateway ingress already has a durable terminal receipt before this
    /// maintenance runs. Keep extraction, drift, and index work off the
    /// surface-critical path, while retaining the exact same maintenance
    /// implementation and telemetry used by synchronous Agent turns.
    pub(super) async fn schedule_memory_post_turn(&self, user_input: &str) {
        let Some((mgr, memory_ctx, mem_messages, callback)) =
            self.memory_post_turn_work(user_input).await
        else {
            return;
        };
        let owner = format!("memory-post-turn:{}", memory_ctx.session_id);
        let work = async move {
            Self::complete_memory_post_turn(mgr, memory_ctx, mem_messages, callback).await;
        };
        if let Some(supervisor) = &self.maintenance_supervisor {
            if !supervisor.submit(owner, work).await {
                tracing::debug!("runtime maintenance supervisor is closed; post-turn work skipped");
            }
        } else {
            work.await;
        }
    }

    pub(super) async fn memory_post_turn_work(
        &self,
        user_input: &str,
    ) -> Option<(
        Arc<CognitiveContextManager>,
        MemoryTurnContext,
        Vec<MemMessage>,
        Option<Arc<dyn MemoryCallback>>,
    )> {
        // The root Session turn is the sole producer of ordinary L1-L3
        // conversation memory. Delegated Team agents receive the parent
        // objective in their synthetic prompt, so extracting it again would
        // multiply identical preferences and decisions across Agent scopes.
        // Their independent results still flow through the governed
        // KnowledgeCandidate/L4 promotion path.
        if !self.owns_conversation_memory_production() {
            return None;
        }
        let mgr = Arc::clone(self.memory_manager.as_ref()?);
        let memory_ctx = self.memory_turn_context();

        // Extract only the completed turn. Re-scanning the full transcript on
        // every turn multiplies cost and repeatedly writes the same memory.
        // Any user supplements appended after the root prompt remain inside the
        // window and are therefore available to extraction.
        let session_messages = self.session.read().await.materialize_messages();
        let mem_messages = conversation_messages_to_mem_messages(current_turn_messages(
            &session_messages,
            user_input,
        ));

        Some((mgr, memory_ctx, mem_messages, self.memory_callback.clone()))
    }

    pub(super) fn owns_conversation_memory_production(&self) -> bool {
        self.memory_team_id.as_deref().is_none_or(str::is_empty)
    }

    pub(super) async fn complete_memory_post_turn(
        mgr: Arc<CognitiveContextManager>,
        memory_ctx: MemoryTurnContext,
        mem_messages: Vec<MemMessage>,
        callback: Option<Arc<dyn MemoryCallback>>,
    ) {
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        let start = Instant::now();
        let mut maintenance_messages = mem_messages;
        let post_turn_result = kernel
            .post_turn(&memory_ctx, &mut maintenance_messages)
            .await;
        let elapsed = start.elapsed();
        tracing::info!(
            elapsed_ms = elapsed.as_millis(),
            "post_turn: memory kernel completed"
        );

        if let Err(ref e) = post_turn_result {
            tracing::warn!(%e, "post_turn: memory kernel failed");
        }

        if let Some(cb) = callback {
            let layers_data = mgr.list_layers().await;
            let total_entries: usize = layers_data
                .iter()
                .filter_map(|l| {
                    l.get("entry_count")
                        .and_then(|c| c.as_u64())
                        .map(|c| c as usize)
                })
                .sum();
            let layer_names: Vec<String> = layers_data
                .iter()
                .filter_map(|l| {
                    l.get("layer")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            let vector_count = mgr.vector_index_count();
            cb.on_memory_stats(total_entries, vector_count, layer_names);
        }
    }

    /// Index oversized tool output by evidence reference instead of retaining
    /// the complete payload in the active conversation context.
    pub(super) fn maybe_index_tool_output(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        output: &str,
        access: Option<&EvidenceAccessRef>,
    ) {
        if output.lines().count() < DEFAULT_OUTPUT_REF_MIN_LINES && output.chars().count() < 16_000
        {
            return;
        }
        let Some(ref sandbox) = self.tool_output_sandbox else {
            return;
        };
        let Ok(mut guard) = sandbox.lock() else {
            tracing::warn!(
                tool_call_id = tool_use_id,
                "tool output sandbox lock poisoned"
            );
            return;
        };
        let content_hash = format!(
            "{:016x}",
            model_protocol::fingerprint::stable_hash_bytes(output.as_bytes())
        );
        let summary = if let Some(access) = access {
            let evidence = memory::types::CanonicalRawEvidence::new(
                access.clone(),
                preview_chars(output, 600),
            );
            guard.index_tool_output_with_evidence(
                tool_use_id,
                tool_name,
                output,
                DEFAULT_OUTPUT_REF_MIN_LINES,
                &evidence,
            )
        } else {
            guard.index_tool_output_ephemeral(
                tool_use_id,
                output,
                DEFAULT_OUTPUT_REF_MIN_LINES,
                tool_use_id,
                &content_hash,
            )
        };
        if let Some(summary) = summary {
            tracing::debug!(
                tool_call_id = tool_use_id,
                tool_name,
                total_lines = summary.total_lines,
                full_size_bytes = summary.full_size_bytes,
                "indexed oversized tool output"
            );
        }
    }

    pub(super) fn record_context_component(
        &self,
        component: crate::context_ledger::ContextComponentKind,
        tokens: u64,
        reference: Option<String>,
        request_sequence: usize,
    ) {
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            ledger.record(component, tokens, reference, request_sequence);
        }
    }

    #[cfg(test)]
    pub(super) fn retrieve_tool_evidence(&self, input: &str) -> Result<String, String> {
        retrieve_tool_evidence_from_sandbox(self.tool_output_sandbox.as_ref(), input)
    }

    pub(super) fn record_provider_context_request(
        &self,
        request: &ApiRequest,
        request_sequence: usize,
        inventory: ProviderContextInventory,
        schema_stats: (u64, u64),
    ) {
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_provider_request(inventory, schema_stats);
        }
        if let Ok(mut metrics) = self.turn_stable_prefix_metrics.lock() {
            metrics.observe_request(request);
        }
        let mut system_tokens = crate::context_ledger::estimate_text_tokens(
            &request.prompt.trusted_system.join("\n\n"),
        );
        let capability_tokens = request
            .prompt
            .trusted_system
            .iter()
            .filter(|fragment| {
                fragment.starts_with("## Runtime evidence plan")
                    || fragment.starts_with("## Runtime execution decision")
            })
            .map(|fragment| crate::context_ledger::estimate_text_tokens(fragment))
            .sum::<u64>();
        let mut history_tokens = 0u64;
        let mut tool_input_tokens = 0u64;
        let mut tool_result_tokens = 0u64;
        for block in request
            .messages
            .iter()
            .flat_map(|message| message.blocks.iter())
        {
            match block {
                ContentBlock::Text { text } => {
                    history_tokens = history_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(text));
                }
                // Public summaries are projected to the user but are not
                // returned as Provider transcript input.
                ContentBlock::ReasoningSummary { .. } => {}
                ContentBlock::Image {
                    media_type, data, ..
                } => {
                    history_tokens = history_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(media_type))
                        .saturating_add((data.len() as u64).div_ceil(4));
                }
                ContentBlock::Thinking { thinking, .. } => {
                    history_tokens = history_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(thinking));
                }
                ContentBlock::ToolUse { id, name, input } => {
                    tool_input_tokens = tool_input_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(id))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(name))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(input));
                }
                ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    output,
                    ..
                } => {
                    tool_result_tokens = tool_result_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(tool_use_id))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(tool_name))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(output));
                }
            }
        }
        let mut memory_tokens = 0u64;
        let mut handoff_tokens = 0u64;
        let mut contextual_tokens = 0u64;
        for packet in &request.prompt.contextual_packets {
            let tokens =
                crate::context_ledger::estimate_text_tokens(&packet.render_for_user_context());
            match packet.source {
                ContextSourceKind::Memory
                | ContextSourceKind::Knowledge
                | ContextSourceKind::Fact
                | ContextSourceKind::Matrix => {
                    memory_tokens = memory_tokens.saturating_add(tokens);
                }
                ContextSourceKind::AgentPeer | ContextSourceKind::Handoff => {
                    handoff_tokens = handoff_tokens.saturating_add(tokens);
                }
                _ => {
                    contextual_tokens = contextual_tokens.saturating_add(tokens);
                }
            }
        }
        system_tokens = system_tokens
            .saturating_add(contextual_tokens)
            .saturating_sub(capability_tokens);
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            ledger
                .begin_request_with_budget(request_sequence, request.budget.hard_input_cap_tokens);
        }
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::System,
            system_tokens,
            Some(format!("provider-request:{request_sequence}:system")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::History,
            history_tokens,
            Some(format!("provider-request:{request_sequence}:history")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::Memory,
            memory_tokens,
            Some(format!("provider-request:{request_sequence}:memory")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::AgentHandoff,
            handoff_tokens,
            Some(format!("provider-request:{request_sequence}:handoff")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::Capability,
            capability_tokens,
            Some(format!(
                "provider-request:{request_sequence}:runtime-capability"
            )),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolInput,
            tool_input_tokens,
            Some(format!("provider-request:{request_sequence}:tool-input")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolResult,
            tool_result_tokens,
            Some(format!("provider-request:{request_sequence}:tool-result")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolSchema,
            request.budget.fixed_input_tokens.saturating_sub(
                crate::context_ledger::estimate_text_tokens(
                    &request.prompt.trusted_system.join("\n\n"),
                )
                .saturating_add(history_tokens),
            ),
            Some(format!("provider-request:{request_sequence}:tools")),
            request_sequence,
        );
    }

    pub(super) fn reconcile_provider_context_usage(&self, usage: TokenUsage) {
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            ledger.reconcile_input_tokens(u64::from(usage.input_tokens));
        }
        if let Ok(mut metrics) = self.turn_stable_prefix_metrics.lock() {
            metrics.observe_usage(usage);
        }
    }

    pub(super) async fn record_tool_raw_evidence(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input_hash: &str,
        output: &str,
        is_error: bool,
        duration_ms: u64,
        source_evidence_ref: Option<&str>,
    ) -> Result<(EvidenceRef, EvidenceAccessRef), RuntimeError> {
        let content_hash = model_protocol::fingerprint::stable_hash_bytes(output.as_bytes());
        let evidence_id = format!("tool-raw-{tool_use_id}-{content_hash:016x}");
        let evidence_ref = EvidenceRef::observed("tool", evidence_id.clone());
        if let Some(access) = self.existing_evidence_access(&evidence_ref) {
            return Ok((evidence_ref, access));
        }
        let Some(ref session_port) = self.session_journal_port else {
            return Err(RuntimeError::new(
                "raw tool evidence cannot be published without the Session store",
            ));
        };
        let Some(ref artifacts) = self.artifact_store else {
            return Err(RuntimeError::new(
                "raw tool evidence cannot be published without the Artifact store",
            ));
        };
        let session_id = self.session_id().to_string();
        let metadata = serde_json::json!({
            "type": "ToolObservationRaw",
            "evidence_id": evidence_id,
            "session_id": session_id,
            "tool_call_id": tool_use_id,
            "tool_name": tool_name,
            "input_hash": input_hash,
            "is_error": is_error,
            "duration_ms": duration_ms,
            "line_count": output.lines().count(),
            "byte_count": output.len(),
            "source_evidence_ref": source_evidence_ref,
        });
        let facade = crate::context_evidence::raw::RawEvidenceFacade::new(
            crate::context_evidence::raw::SessionPortRawEvidenceStore::new(
                Arc::clone(session_port),
                Arc::clone(artifacts),
            ),
        );
        let access = match facade
            .persist(crate::context_evidence::raw::RawEvidenceWrite {
                evidence_ref: evidence_ref.clone(),
                session_id: session_id.clone(),
                media_type: "text/plain; charset=utf-8".to_string(),
                visibility_scope: format!("session:{session_id}"),
                payload: output.to_string(),
                metadata,
            })
            .await
        {
            Ok(access) => access,
            Err(error) => return Err(RuntimeError::new(error.to_string())),
        };
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            let _ = ledger.register_evidence_hash(evidence_id);
        }
        Ok((evidence_ref, access))
    }

    pub(super) async fn record_tool_output_evidence(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input_hash: &str,
        draft: &harness_contract::context::ToolOutputDraft,
        model_text: &str,
        is_error: bool,
        duration_ms: u64,
        source_evidence_ref: Option<&str>,
    ) -> Result<(EvidenceRef, EvidenceAccessRef), RuntimeError> {
        let Some(artifact) = draft.artifact_ref() else {
            return self
                .record_tool_raw_evidence(
                    tool_use_id,
                    tool_name,
                    input_hash,
                    model_text,
                    is_error,
                    duration_ms,
                    source_evidence_ref,
                )
                .await;
        };
        let evidence_id = format!(
            "tool-raw-{tool_use_id}-{}",
            artifact.sha256.trim_start_matches("sha256:")
        );
        let evidence_ref = EvidenceRef::observed("tool", evidence_id.clone());
        if let Some(access) = self.existing_evidence_access(&evidence_ref) {
            return Ok((evidence_ref, access));
        }
        let Some(ref session_port) = self.session_journal_port else {
            return Err(RuntimeError::new(
                "staged tool evidence cannot be published without the Session store",
            ));
        };
        let Some(ref artifacts) = self.artifact_store else {
            return Err(RuntimeError::new(
                "staged tool evidence cannot be published without the Artifact store",
            ));
        };
        let session_id = self.session_id().to_string();
        let metadata = serde_json::json!({
            "type": "ToolObservationRaw",
            "evidence_id": evidence_id,
            "session_id": session_id,
            "tool_call_id": tool_use_id,
            "tool_name": tool_name,
            "input_hash": input_hash,
            "is_error": is_error,
            "duration_ms": duration_ms,
            "summary_line_count": model_text.lines().count(),
            "summary_byte_count": model_text.len(),
            "source_evidence_ref": source_evidence_ref,
            "native_staged_artifact": true,
        });
        let access = crate::context_evidence::raw::SessionPortRawEvidenceStore::new(
            Arc::clone(session_port),
            Arc::clone(artifacts),
        )
        .persist_artifact(evidence_ref.clone(), session_id, artifact.clone(), metadata)
        .await
        .map_err(|error| RuntimeError::new(error.to_string()))?;
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            let _ = ledger.register_evidence_hash(evidence_id);
        }
        Ok((evidence_ref, access))
    }

    pub(super) fn strategy_input_for_turn(&self, user_input: &str) -> StrategyInput {
        let mut input = StrategyInput::from_prompt(user_input.to_string());
        let Some(projector) = self.outcome_projector.as_ref() else {
            return input;
        };
        let understanding = understand(&input);
        let workload_fingerprint =
            StrategyWorkloadFingerprint::from_input(&input, &understanding).digest();
        input.understanding = Some(understanding);
        let Some(model) = self
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
        else {
            return input;
        };
        let Some(provider) = self.api_client.provider_name_for_model(model) else {
            return input;
        };
        let snapshot = projector.snapshot();
        let now = now_ms();
        const EXPERIENCE_FRESHNESS_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
        let mut comparable = Vec::new();
        for candidate in [
            ExecutionCandidateKind::Direct,
            ExecutionCandidateKind::ParallelTools,
            ExecutionCandidateKind::Team,
        ] {
            let key = harness_contract::outcome::StrategyExperienceKey {
                workspace_key: self.checkpoint_workspace_id.clone(),
                workload_fingerprint_sha256: workload_fingerprint.clone(),
                config_revision: self.runtime_config_revision.clone(),
                provider: provider.clone(),
                model: model.to_string(),
                evaluation_environment: "production".to_string(),
                candidate,
            };
            let Some(experience) = snapshot.strategy_experience(&key) else {
                continue;
            };
            if experience.sample_count == 0
                || now.saturating_sub(experience.last_observed_at_ms) > EXPERIENCE_FRESHNESS_MS
            {
                continue;
            }
            input.candidate_costs.insert(
                candidate,
                StrategyCandidateCostSummary {
                    sample_count: u32::try_from(experience.sample_count).unwrap_or(u32::MAX),
                    average_critical_path_ms: experience.duration_p50_ms,
                    average_total_tokens: experience.total_tokens_p50,
                    average_coordination_cost_ms: experience.coordination_cost_p50_ms,
                    calibration_source: format!(
                        "runtime.outcome_strategy.v2:{}:{}",
                        snapshot.revision, workload_fingerprint
                    ),
                },
            );
            comparable.push(experience);
        }
        if !comparable.is_empty() {
            let total = comparable.iter().fold(0_u64, |sum, experience| {
                sum.saturating_add(experience.sample_count)
            });
            let sum = |value: fn(&crate::StrategyExperienceSnapshot) -> u64| {
                comparable.iter().fold(0_u64, |sum, experience| {
                    sum.saturating_add(value(experience))
                })
            };
            let weighted = |value: fn(&crate::StrategyExperienceSnapshot) -> u64| {
                comparable
                    .iter()
                    .fold(0_u64, |sum, experience| {
                        sum.saturating_add(
                            value(experience).saturating_mul(experience.sample_count),
                        )
                    })
                    .saturating_div(total.max(1))
            };
            let basis_points = |count: u64, sample_count: u64| {
                u16::try_from(count.saturating_mul(10_000) / sample_count.max(1)).unwrap_or(10_000)
            };
            let team = comparable.iter().find(|experience| {
                experience
                    .key
                    .as_ref()
                    .is_some_and(|key| key.candidate == ExecutionCandidateKind::Team)
            });
            input.experience = Some(StrategyExperienceSummary {
                sample_count: u32::try_from(total).unwrap_or(u32::MAX),
                success_rate_bp: basis_points(sum(|experience| experience.success_count), total),
                verification_block_rate_bp: basis_points(
                    sum(|experience| experience.verification_block_count),
                    total,
                ),
                context_pressure_rate_bp: basis_points(
                    sum(|experience| experience.context_pressure_count),
                    total,
                ),
                multi_agent_lift_rate_bp: team.map_or(0, |experience| {
                    basis_points(
                        experience.positive_lift_count,
                        experience.paired_comparison_count,
                    )
                }),
                multi_agent_lift_sample_count: team
                    .map(|experience| {
                        u32::try_from(experience.paired_comparison_count).unwrap_or(u32::MAX)
                    })
                    .unwrap_or_default(),
                average_duration_ms: weighted(|experience| experience.duration_p50_ms),
                average_total_tokens: weighted(|experience| experience.total_tokens_p50),
                average_coordination_cost_ms: weighted(|experience| {
                    experience.coordination_cost_p50_ms
                }),
                actual_cost_sample_count: u32::try_from(total).unwrap_or(u32::MAX),
            });
        }
        input
    }

    /// Admit exactly one strategy identity for a turn. This is the only
    /// conversation-layer call site allowed to create a decision.
    #[cfg(test)]
    pub(crate) fn begin_turn_strategy(
        &self,
        turn_ref: impl Into<String>,
        user_input: &str,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        self.begin_turn_strategy_with_resource_snapshot(turn_ref, user_input, None)
    }

    pub(crate) fn begin_turn_strategy_with_resource_snapshot(
        &self,
        turn_ref: impl Into<String>,
        user_input: &str,
        resource_snapshot: Option<harness_contract::strategy::StrategyResourceSnapshot>,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let turn_ref = turn_ref.into();
        *self
            .active_provider_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *self
            .provider_selection_receipt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let mut guard = self
            .active_turn_strategy
            .lock()
            .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
        if let Some(active) = guard.as_ref() {
            if active.turn_ref == turn_ref {
                return Ok(active.clone());
            }
            return Err(RuntimeError::new(format!(
                "turn `{turn_ref}` cannot replace active strategy turn `{}`",
                active.turn_ref
            )));
        }
        let evaluation_isolated = resource_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.sample_source.contains("corpus="));
        let mut strategy_input = if evaluation_isolated {
            StrategyInput::from_prompt(user_input.to_string())
        } else {
            self.strategy_input_for_turn(user_input)
        };
        if let Some(resource_snapshot) = resource_snapshot {
            strategy_input = strategy_input.with_resource_snapshot(resource_snapshot);
        }
        apply_e2e_strategy_fixture(&mut strategy_input, user_input)?;
        let mut decision = crate::execution_core::StrategyDecisionEngine.decide_with_input(
            strategy_input,
            Some(self.context_profile()),
            crate::execution_core::StrategyResourceHealth {
                provider_available: self.api_client.provider_available(),
                tools_available: self.tool_executor.has_registered_tools(),
                collaboration_available: self.runtime_control_policy.enabled
                    && self.runtime_control_policy.agent.enabled
                    && self.context_profile() != ContextProfile::SubAgent
                    && self.tool_executor.collaboration_runtime_available(),
                mission_available: self.runtime_control_policy.enabled
                    && self.tool_executor.mission_runtime_available(),
                observed: true,
            },
        );
        apply_eval_strategy_override(&mut decision)?;
        if !decision.executable {
            return Err(RuntimeError::new(format!(
                "runtime strategy is not executable: {}",
                decision.blocked_reasons.join("; ")
            )));
        }
        let state = crate::execution_core::TurnStrategyDecisionState::admitted(
            decision,
            self.session_id().to_string(),
            turn_ref,
        );
        // Capability discovery and the later `runtime_orchestrate` proposal
        // may be separate provider/tool batches. Bind the just-admitted
        // decision before either one can run so both observe the same
        // turn-owned lease. The executor is a transport cache only: the
        // ConversationRuntime remains the sole decision owner.
        self.tool_executor
            .bind_execution_decision(state.decision.clone());
        *guard = Some(state.clone());
        Ok(state)
    }

    #[must_use]
    pub(crate) fn active_turn_strategy(
        &self,
    ) -> Option<crate::execution_core::TurnStrategyDecisionState> {
        self.active_turn_strategy
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Pin an explicit root collaboration contract to the admitted turn
    /// before exposing the model control plane. This intentionally fixes a
    /// runtime invariant rather than deriving any Team topology: the model
    /// still owns the typed proposal and the durable receipt remains required
    /// before a Program can materialize.
    pub(crate) fn require_active_turn_collaboration_control_plane(
        &self,
        required_team_count: u8,
    ) -> Result<crate::execution_core::RuntimeExecutionDecision, RuntimeError> {
        if required_team_count == 0 {
            return Err(RuntimeError::new(
                "root collaboration control plane requires at least one Team",
            ));
        }
        let frozen_required_team_count = self
            .active_turn_strategy()
            .and_then(|state| state.decision.collaboration_obligation)
            .map(|obligation| obligation.required_team_count())
            .ok_or_else(|| {
                RuntimeError::new(
                    "root collaboration control plane requires a frozen execution obligation",
                )
            })?;
        if frozen_required_team_count != required_team_count {
            return Err(RuntimeError::new(format!(
                "root collaboration cardinality diverged from frozen obligation: expected {frozen_required_team_count}, observed {required_team_count}"
            )));
        }
        let decision = self.revise_active_turn_strategy(
            harness_contract::strategy::ExecutionCandidateKind::Team,
            harness_contract::core::ExecutionPattern::Collaborate,
            crate::execution_core::TurnStrategyDecisionStatus::Running,
            "explicit root collaboration contract pinned the turn strategy lease before model control-plane exposure",
            Some("runtime.strategy.selected"),
        )?;
        self.tool_executor.bind_execution_decision(decision.clone());
        Ok(decision)
    }

    pub(crate) fn bind_turn_strategy_execution(
        &self,
        turn_ref: &str,
        execution_graph_ref: &str,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let recovered = self.recover_turn_strategy_identity(turn_ref, execution_graph_ref);
        let recovered_identity = recovered.is_some();
        let (state, should_emit, previous) = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let previous = guard.clone();
            let state = guard
                .as_mut()
                .filter(|state| state.turn_ref == turn_ref)
                .ok_or_else(|| RuntimeError::new("turn strategy binding scope mismatch"))?;
            let should_emit = state.execution_graph_ref.is_none() && !recovered_identity;
            if let Some(recovered) = recovered {
                state.decision_id = recovered.decision_id.clone();
                state.decision_lease = recovered.decision_lease.clone();
                state.revision = recovered.revision;
                state.policy_version.clone_from(&recovered.policy_version);
                state.selected_candidate = recovered.selected_candidate;
                state.status = recovered.status;
                state.resource_snapshot = recovered.resource_snapshot;
                state.collaboration_receipt = recovered.collaboration_receipt;
                if recovered.collaboration_obligation.is_some() {
                    state.decision.collaboration_obligation = recovered.collaboration_obligation;
                }
                state.focus_partition_plans = recovered.focus_partition_plans;
                state.decision.decision_id = recovered.decision_id;
                state.decision.decision_revision = recovered.revision;
                state.decision.lease.lease_id = recovered.decision_lease;
                state.decision.strategy.policy_version = recovered.policy_version;
                state.decision.strategy.selected_candidate = recovered.selected_candidate;
                state.decision.strategy.resource_snapshot = state.resource_snapshot.clone();
                state.decision.strategy.candidate_estimates = recovered.candidate_estimates;
                let recovered_pattern = recovered.pattern;
                state
                    .decision
                    .strategy
                    .retarget(
                        recovered_pattern,
                        "recovered the durable turn strategy identity before graph resume",
                    )
                    .map_err(RuntimeError::new)?;
                state.decision.lease.locked_pattern = recovered_pattern;
                state.decision.compile_target =
                    crate::execution_core::ExecutionPatternCatalog::current()
                        .find(recovered_pattern)
                        .map_or(
                            crate::execution_core::RuntimeCompileTarget::InlineModel,
                            |spec| spec.compile_target,
                        );
            }
            match state.execution_graph_ref.as_deref() {
                Some(graph_id) if graph_id == execution_graph_ref => {}
                Some(_) => {
                    return Err(RuntimeError::new(
                        "turn strategy cannot be rebound to another execution graph",
                    ));
                }
                None if should_emit || recovered_identity => {
                    // A recovered identity was filtered by this exact graph
                    // reference above.  Rehydrate that durable binding without
                    // producing a second selected event.
                    state.bind_execution_graph(execution_graph_ref);
                }
                None => {
                    return Err(RuntimeError::new(
                        "turn strategy cannot be rebound to another execution graph",
                    ));
                }
            }
            (state.clone(), should_emit, previous)
        };
        if should_emit {
            if let Err(error) = self.append_turn_strategy_event(
                "runtime.strategy.selected",
                &state,
                "turn admitted and parent execution graph bound",
            ) {
                *self
                    .active_turn_strategy
                    .lock()
                    .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                    previous;
                return Err(error);
            }
        }
        Ok(state)
    }

    pub(super) fn recover_turn_strategy_identity(
        &self,
        turn_ref: &str,
        execution_graph_ref: &str,
    ) -> Option<RecoveredTurnStrategyIdentity> {
        let store = self.runtime_event_store.as_ref()?;
        let session_id = self.session_id().to_string();
        let events = store
            .list_stream(&format!("session:{session_id}"))
            .map_err(|error| {
                tracing::warn!(%error, session_id, turn_ref, "failed to inspect durable strategy identity");
                error
            })
            .ok()?;
        events
            .into_iter()
            .rev()
            .filter(|event| {
                matches!(
                    event.kind.as_str(),
                    "runtime.strategy.selected"
                        | "runtime.strategy.downgraded"
                        | "runtime.strategy.early_stopped"
                        | "runtime.strategy.outcome"
                )
            })
            .find_map(|event| {
                let payload = event.payload;
                if payload.get("turn_ref").and_then(serde_json::Value::as_str) != Some(turn_ref)
                    || payload
                        .get("execution_graph_ref")
                        .and_then(serde_json::Value::as_str)
                        != Some(execution_graph_ref)
                {
                    return None;
                }
                let pattern = match payload
                    .get("selected_pattern")
                    .and_then(serde_json::Value::as_str)?
                {
                    "direct" => harness_contract::core::ExecutionPattern::Direct,
                    "explore" => harness_contract::core::ExecutionPattern::Explore,
                    "execute" => harness_contract::core::ExecutionPattern::Execute,
                    "deliberate" => harness_contract::core::ExecutionPattern::Deliberate,
                    "collaborate" => harness_contract::core::ExecutionPattern::Collaborate,
                    "supervise" => harness_contract::core::ExecutionPattern::Supervise,
                    _ => return None,
                };
                Some(RecoveredTurnStrategyIdentity {
                    decision_id: payload.get("decision_id")?.as_str()?.to_string(),
                    decision_lease: payload.get("decision_lease")?.as_str()?.to_string(),
                    revision: payload.get("decision_revision")?.as_u64()?,
                    policy_version: payload.get("policy_version")?.as_str()?.to_string(),
                    selected_candidate: serde_json::from_value(
                        payload.get("selected_candidate")?.clone(),
                    )
                    .ok()?,
                    status: serde_json::from_value(payload.get("status")?.clone()).ok()?,
                    resource_snapshot: serde_json::from_value(
                        payload.get("resource_snapshot")?.clone(),
                    )
                    .ok()?,
                    candidate_estimates: serde_json::from_value(
                        payload.get("candidate_estimates")?.clone(),
                    )
                    .ok()?,
                    collaboration_receipt: payload
                        .get("collaboration_receipt")
                        .filter(|value| !value.is_null())
                        .cloned(),
                    collaboration_obligation: payload
                        .get("collaboration_obligation")
                        .filter(|value| !value.is_null())
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .and_then(|obligation: harness_contract::strategy::CollaborationExecutionObligation| {
                            obligation.validate().ok().map(|()| obligation)
                        }),
                    focus_partition_plans: payload
                        .get("evidence_scopes")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .unwrap_or_default(),
                    pattern,
                })
            })
    }

    pub(super) fn revise_active_turn_strategy(
        &self,
        selected_candidate: harness_contract::strategy::ExecutionCandidateKind,
        pattern: harness_contract::core::ExecutionPattern,
        status: crate::execution_core::TurnStrategyDecisionStatus,
        reason: &str,
        event_kind: Option<&'static str>,
    ) -> Result<crate::execution_core::RuntimeExecutionDecision, RuntimeError> {
        let (state, previous) = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let state = guard
                .as_mut()
                .ok_or_else(|| RuntimeError::new("turn strategy revision has no owner"))?;
            let previous = state.clone();
            if state.selected_candidate == selected_candidate
                && state.decision.pattern() == pattern
                && state.status == status
                && status == crate::execution_core::TurnStrategyDecisionStatus::Running
            {
                return Ok(state.decision.clone());
            }
            state
                .revise_to_pattern(selected_candidate, pattern, status, reason)
                .map_err(RuntimeError::new)?;
            (state.clone(), previous)
        };
        if let Some(kind) = event_kind {
            if let Err(error) = self.append_turn_strategy_event(kind, &state, reason) {
                *self
                    .active_turn_strategy
                    .lock()
                    .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                    Some(previous.clone());
                self.tool_executor
                    .bind_execution_decision(previous.decision);
                return Err(error);
            }
        }
        Ok(state.decision)
    }

    pub(super) fn retarget_active_turn_strategy_for_tool_requirements(
        &self,
        selected_candidate: harness_contract::strategy::ExecutionCandidateKind,
        pattern: harness_contract::core::ExecutionPattern,
        requires_external_facts: bool,
        requires_write: bool,
        requests_parallelism: bool,
        requires_explicit_approval: bool,
        reason: &str,
    ) -> Result<crate::execution_core::RuntimeExecutionDecision, RuntimeError> {
        let (state, previous) = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let state = guard
                .as_mut()
                .ok_or_else(|| RuntimeError::new("turn strategy revision has no owner"))?;
            let previous = state.clone();
            state
                .revise_for_tool_requirements(
                    selected_candidate,
                    pattern,
                    requires_external_facts,
                    requires_write,
                    requests_parallelism,
                    crate::execution_core::TurnStrategyDecisionStatus::Running,
                    reason,
                )
                .map_err(RuntimeError::new)?;
            if requires_explicit_approval
                && state
                    .decision
                    .pattern()
                    .supports_gate(harness_contract::core::ExecutionPolicyGate::Approval)
                && !state
                    .decision
                    .strategy
                    .gates
                    .contains(&harness_contract::core::ExecutionPolicyGate::Approval)
            {
                state
                    .decision
                    .strategy
                    .gates
                    .push(harness_contract::core::ExecutionPolicyGate::Approval);
                state.decision.strategy.reasons.push(
                    "an evidence-only strategy requested mutation; explicit approval is required before delivery"
                        .to_string(),
                );
            }
            (state.clone(), previous)
        };
        if let Err(error) =
            self.append_turn_strategy_event("runtime.strategy.selected", &state, reason)
        {
            *self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                Some(previous);
            return Err(error);
        }
        Ok(state.decision)
    }

    /// Revise the one turn-owned strategy from the concrete governed tool
    /// plan. Conversation and execution-graph routes call this same method so
    /// a graph node cannot validate a later write against an earlier
    /// evidence-only strategy snapshot.
    pub(crate) fn retarget_active_turn_strategy_for_governed_plan(
        &self,
        plan: &GovernedToolPlan,
        calls: &[ModelToolCall],
    ) -> Result<crate::execution_core::RuntimeExecutionDecision, RuntimeError> {
        let current = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("tool batch has no admitted turn strategy"))?;
        let requests_team = calls.iter().any(is_runtime_team_orchestration_call);
        let has_network = plan.tasks.iter().any(|task| {
            task.safety_category == crate::tool_orchestrator::ToolSafetyCategory::Network
        });
        let has_mutation = plan.tasks.iter().any(|task| {
            !is_runtime_team_orchestration_call_name(&task.tool_name)
                && matches!(
                    task.safety_category,
                    crate::tool_orchestrator::ToolSafetyCategory::WriteLocal
                        | crate::tool_orchestrator::ToolSafetyCategory::Destructive
                )
        });
        let target_pattern = if requests_team {
            harness_contract::core::ExecutionPattern::Collaborate
        } else if has_mutation {
            harness_contract::core::ExecutionPattern::Execute
        } else {
            harness_contract::core::ExecutionPattern::Explore
        };
        let requests_parallelism = target_pattern
            == harness_contract::core::ExecutionPattern::Collaborate
            || plan
                .tasks
                .iter()
                .filter(|task| task.can_parallelize)
                .count()
                > 1;
        if !has_network
            && !has_mutation
            && !requests_parallelism
            && target_pattern != harness_contract::core::ExecutionPattern::Collaborate
        {
            return Ok(current);
        }
        let selected_candidate =
            if target_pattern == harness_contract::core::ExecutionPattern::Collaborate {
                harness_contract::strategy::ExecutionCandidateKind::Team
            } else if requests_parallelism {
                harness_contract::strategy::ExecutionCandidateKind::ParallelTools
            } else {
                harness_contract::strategy::ExecutionCandidateKind::Direct
            };
        self.retarget_active_turn_strategy_for_tool_requirements(
            selected_candidate,
            target_pattern,
            has_network,
            has_mutation,
            requests_parallelism,
            current.compile_target == crate::execution_core::RuntimeCompileTarget::EvidenceGraph
                && has_mutation,
            "provider tool batch retained the admitted decision lease",
        )
    }

    pub(crate) fn downgrade_turn_strategy(
        &self,
        candidate: harness_contract::strategy::ExecutionCandidateKind,
        reason: &str,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let understanding = self
            .active_turn_strategy()
            .map(|state| state.decision.strategy.understanding)
            .ok_or_else(|| RuntimeError::new("downgraded turn strategy has no owner"))?;
        let requires_guarded_pattern = understanding.requires_write
            || matches!(
                understanding.risk,
                harness_contract::core::TaskRisk::High | harness_contract::core::TaskRisk::Critical
            );
        let pattern = match candidate {
            harness_contract::strategy::ExecutionCandidateKind::Direct => {
                if requires_guarded_pattern {
                    harness_contract::core::ExecutionPattern::Execute
                } else {
                    harness_contract::core::ExecutionPattern::Direct
                }
            }
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools => {
                if requires_guarded_pattern {
                    harness_contract::core::ExecutionPattern::Execute
                } else {
                    harness_contract::core::ExecutionPattern::Explore
                }
            }
            harness_contract::strategy::ExecutionCandidateKind::Team => {
                harness_contract::core::ExecutionPattern::Collaborate
            }
        };
        self.revise_active_turn_strategy(
            candidate,
            pattern,
            crate::execution_core::TurnStrategyDecisionStatus::Downgraded,
            reason,
            Some("runtime.strategy.downgraded"),
        )?;
        self.active_turn_strategy()
            .ok_or_else(|| RuntimeError::new("downgraded turn strategy disappeared"))
    }

    pub(crate) fn record_turn_strategy_early_stop(&self, reason: &str) -> Result<(), RuntimeError> {
        let active = self
            .active_turn_strategy()
            .ok_or_else(|| RuntimeError::new("early stop has no turn strategy"))?;
        self.revise_active_turn_strategy(
            active.selected_candidate,
            active.decision.pattern(),
            crate::execution_core::TurnStrategyDecisionStatus::EarlyStopped,
            reason,
            Some("runtime.strategy.early_stopped"),
        )?;
        Ok(())
    }

    pub(crate) fn set_turn_strategy_focus_partitions(
        &self,
        plans: Vec<harness_contract::team::FocusPartitionPlan>,
        automatic_minimum_team_count: u8,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let (updated, previous, already_bound) = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let state = guard
                .as_mut()
                .ok_or_else(|| RuntimeError::new("focus partitions have no turn strategy owner"))?;
            let previous = state.clone();
            let focus_ids = plans
                .iter()
                .flat_map(|plan| plan.slots.iter())
                .map(|slot| slot.focus_id.clone())
                .collect::<Vec<_>>();
            let obligation = (state.selected_candidate
                == harness_contract::strategy::ExecutionCandidateKind::Team)
                .then(|| {
                    harness_contract::strategy::CollaborationExecutionObligation::for_selected_team(
                        &state.decision.strategy.understanding,
                        automatic_minimum_team_count,
                        focus_ids,
                    )
                    .map_err(RuntimeError::new)
                })
                .transpose()?;
            state.focus_partition_plans = plans;
            state.decision.collaboration_obligation = obligation;
            (state.clone(), previous, state.execution_graph_ref.is_some())
        };
        self.tool_executor
            .bind_execution_decision(updated.decision.clone());
        if already_bound {
            if let Err(error) = self.append_turn_strategy_event(
                "runtime.strategy.selected",
                &updated,
                "focus partitions and collaboration execution obligation frozen",
            ) {
                *self
                    .active_turn_strategy
                    .lock()
                    .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                    Some(previous.clone());
                self.tool_executor
                    .bind_execution_decision(previous.decision);
                return Err(error);
            }
        }
        Ok(updated)
    }

    pub(crate) fn finish_turn_strategy(
        &self,
        turn_ref: &str,
        status: crate::execution_core::TurnStrategyDecisionStatus,
        mut outcome: crate::execution_core::TurnStrategyActualOutcome,
    ) -> Result<(), RuntimeError> {
        let state = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let Some(mut state) = guard.take() else {
                return Ok(());
            };
            if state.turn_ref != turn_ref {
                *guard = Some(state);
                return Err(RuntimeError::new("turn strategy finish scope mismatch"));
            }
            if let Some(receipt) = state.collaboration_receipt.as_ref() {
                let metric = |name: &str| receipt.get(name).and_then(serde_json::Value::as_u64);
                // A Session-scoped evaluation lease already includes Team
                // children and every fallback request bound to that Session.
                // Adding the receipt a second time inflates projected usage
                // and breaks the hard budget equality gate. Production turns
                // have no evaluation lease and still merge child telemetry.
                if outcome.evaluation_token_limit == 0 {
                    outcome.input_tokens = outcome
                        .input_tokens
                        .saturating_add(metric("child_input_tokens").unwrap_or(0));
                    outcome.output_tokens = outcome
                        .output_tokens
                        .saturating_add(metric("child_output_tokens").unwrap_or(0));
                    outcome.cached_tokens = outcome
                        .cached_tokens
                        .saturating_add(metric("child_cached_tokens").unwrap_or(0));
                }
                outcome.tool_calls = outcome
                    .tool_calls
                    .saturating_add(metric("child_tool_calls").unwrap_or(0));
                outcome.duplicate_tool_calls = outcome
                    .duplicate_tool_calls
                    .saturating_add(metric("duplicate_tool_calls").unwrap_or(0));
                outcome.max_tool_concurrency_observed = outcome
                    .max_tool_concurrency_observed
                    .max(metric("max_tool_concurrency_observed").unwrap_or(0));
                outcome.parallel_tool_batches = outcome
                    .parallel_tool_batches
                    .saturating_add(metric("parallel_tool_batches").unwrap_or(0));
                let child_write_attempt_paths = receipt
                    .get("write_attempt_paths")
                    .and_then(serde_json::Value::as_array)
                    .map(|paths| {
                        paths
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                outcome
                    .write_attempt_paths
                    .extend(child_write_attempt_paths);
                outcome.write_attempt_paths.sort();
                outcome.write_attempt_paths.dedup();
                outcome.evidence_overlap_bp = metric("evidence_overlap_bp")
                    .and_then(|value| u16::try_from(value).ok())
                    .unwrap_or(outcome.evidence_overlap_bp);
                outcome.evidence_overlap_observed = receipt
                    .get("evidence_overlap_observed")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(outcome.evidence_overlap_observed);
                // Team working-state verification proves the child
                // collaboration materialized. It is not the root Goal's
                // working-state verdict and must not overwrite it here.
                outcome.actual_speedup_ratio_bp = metric("actual_speedup_ratio_bp")
                    .and_then(|value| u16::try_from(value).ok())
                    .or(outcome.actual_speedup_ratio_bp);
            }
            state.revision = state.revision.saturating_add(1);
            state.decision.decision_revision = state.revision;
            state.status = status;
            state.outcome = Some(outcome);
            state
        };
        if let Err(error) = self.append_turn_strategy_event(
            "runtime.strategy.outcome",
            &state,
            "turn terminal owner recorded actual outcome",
        ) {
            *self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? = Some(state);
            return Err(error);
        }
        self.record_canonical_outcome(&state)?;
        Ok(())
    }

    pub(super) fn append_turn_strategy_event(
        &self,
        kind: &'static str,
        state: &crate::execution_core::TurnStrategyDecisionState,
        reason: &str,
    ) -> Result<(), RuntimeError> {
        if !turn_strategy_event_kind_allowed(kind) {
            return Err(RuntimeError::new(format!(
                "unsupported durable turn strategy event kind `{kind}`"
            )));
        }
        let mut refs = vec![
            RuntimeEventRef {
                kind: "strategy_decision".to_string(),
                id: state.decision_id.clone(),
            },
            RuntimeEventRef {
                kind: "strategy_lease".to_string(),
                id: state.decision_lease.clone(),
            },
            RuntimeEventRef {
                kind: "session".to_string(),
                id: state.session_ref.clone(),
            },
            RuntimeEventRef {
                kind: "turn".to_string(),
                id: state.turn_ref.clone(),
            },
        ];
        if let Some(graph_id) = &state.execution_graph_ref {
            refs.push(RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: graph_id.clone(),
            });
        }
        let store = self
            .runtime_event_store
            .as_ref()
            .ok_or_else(|| RuntimeError::new("turn strategy event store is unavailable"))?;
        let mut input = RuntimeEventInput {
            stream_id: format!("session:{}", state.session_ref),
            // Turn-level strategy evidence belongs to the Session stream.
            // Treating it as an ExecutionGraph event makes graph discovery
            // attempt to deserialize this non-graph payload as a graph.
            scope: RuntimeEventScope::Session,
            kind: kind.to_string(),
            status: Some(turn_strategy_status_name(state.status).to_string()),
            actor: Some("conversation_runtime.strategy_owner".to_string()),
            refs,
            payload: serde_json::json!({
                "decision_id": state.decision_id,
                "decision_lease": state.decision_lease,
                "decision_revision": state.revision,
                "policy_version": state.policy_version,
                "decision_source": state.decision.strategy.source,
                "confidence": state.decision.strategy.confidence,
                "selected_candidate": state.selected_candidate,
                "selected_pattern": state.decision.pattern().as_str(),
                "candidate_estimates": state.decision.strategy.candidate_estimates,
                "selection_reasons": state.decision.strategy.reasons,
                "resource_snapshot": state.resource_snapshot,
                "execution_graph_ref": state.execution_graph_ref,
                "session_ref": state.session_ref,
                "turn_ref": state.turn_ref,
                "status": state.status,
                "reason": reason,
                "collaboration_receipt": state.collaboration_receipt,
                "collaboration_obligation": state.decision.collaboration_obligation,
                "evidence_scopes": state.focus_partition_plans,
                "outcome": state.outcome,
                "provider_selection": self.provider_selection_receipt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            }),
        };
        if let Some(binding) = self
            .cowd_bus()
            .and_then(crate::CowdEventBus::current_activity_binding)
        {
            input = input.with_activity_binding(binding).map_err(|error| {
                RuntimeError::new(format!(
                    "turn strategy activity binding is invalid: {error}"
                ))
            })?;
        }
        store.append(input).map(|_| ()).map_err(|error| {
            RuntimeError::new(format!(
                "durable turn strategy event `{kind}` append failed: {error}"
            ))
        })
    }

    pub(super) fn record_canonical_outcome(
        &self,
        state: &crate::execution_core::TurnStrategyDecisionState,
    ) -> Result<(), RuntimeError> {
        let Some(outcome) = state.outcome.as_ref() else {
            return Ok(());
        };
        let service = self
            .outcome_service
            .as_ref()
            .ok_or_else(|| RuntimeError::new("canonical outcome service is unavailable"))?;
        let completed_at_ms = now_ms();
        let terminal = match state.status {
            crate::execution_core::TurnStrategyDecisionStatus::Completed
                if outcome.failed_tool_calls > 0 =>
            {
                harness_contract::outcome::OutcomeTerminalClass::PartialFailure(format!(
                    "{}; {} tool calls failed before terminal synthesis",
                    outcome.terminal_reason, outcome.failed_tool_calls
                ))
            }
            crate::execution_core::TurnStrategyDecisionStatus::Completed => {
                harness_contract::outcome::OutcomeTerminalClass::Succeeded(
                    outcome.terminal_reason.clone(),
                )
            }
            crate::execution_core::TurnStrategyDecisionStatus::Cancelled => {
                harness_contract::outcome::OutcomeTerminalClass::Cancelled(
                    outcome.terminal_reason.clone(),
                )
            }
            crate::execution_core::TurnStrategyDecisionStatus::EarlyStopped => {
                harness_contract::outcome::OutcomeTerminalClass::PartialFailure(
                    outcome.terminal_reason.clone(),
                )
            }
            _ => harness_contract::outcome::OutcomeTerminalClass::Failed(
                outcome.terminal_reason.clone(),
            ),
        };
        let quality = outcome.quality_score_bp.map_or(
            harness_contract::outcome::OutcomeQuality::Unknown,
            |value| {
                harness_contract::outcome::OutcomeQuality::estimate(
                    value,
                    "runtime.turn_verification",
                    None,
                )
            },
        );
        let provider = self
            .active_provider_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        // A terminal Outcome remains authoritative even when a deterministic,
        // tool-only, cancelled, or pre-provider path never selected a model.
        // Such an Outcome cannot safely train provider/model-scoped strategy
        // routing, so omit only the scoped workload feedback instead of
        // inventing an "unknown" provider identity.
        let strategy_workload = provider.as_ref().map(|_| {
            StrategyWorkloadFingerprint::from_understanding(
                &state.decision.strategy.understanding,
                state.decision.strategy.understanding.requires_write,
            )
        });
        let evaluation_isolated = state.resource_snapshot.sample_source.contains("corpus=");
        let config_revision = if evaluation_isolated {
            format!(
                "{}:evaluation:{:016x}",
                self.runtime_config_revision,
                model_protocol::fingerprint::stable_hash_bytes(
                    state.resource_snapshot.sample_source.as_bytes()
                )
            )
        } else {
            self.runtime_config_revision.clone()
        };
        let canonical = harness_contract::outcome::ExecutionOutcome {
            identity: harness_contract::outcome::OutcomeIdentity {
                execution_id: format!("turn:{}", state.decision_id),
                session_id: state.session_ref.clone(),
                turn_id: state.turn_ref.clone(),
                terminal_generation: state.revision,
                paired_sample_id: None,
                task_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.task_id().map(str::to_string)),
                mission_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.mission_id().map(str::to_string)),
                agent_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.agent_run_id().map(str::to_string)),
                team_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.team_run_id().map(str::to_string)),
                execution_graph_ref: state.execution_graph_ref.clone(),
            },
            runtime: harness_contract::outcome::RuntimeIdentity {
                workspace_key: self.checkpoint_workspace_id.clone(),
                runtime_revision: env!("CARGO_PKG_VERSION").to_string(),
                config_revision,
                build: Default::default(),
            },
            provider,
            strategy: harness_contract::outcome::StrategyIdentity {
                decision_id: state.decision_id.clone(),
                policy_revision: state.policy_version.clone(),
                decision_source: format!("{:?}", state.decision.strategy.source)
                    .to_ascii_lowercase(),
                selected_candidate: state.selected_candidate,
                selected_pattern: state.decision.pattern().as_str().to_string(),
            },
            timing: harness_contract::outcome::OutcomeTiming {
                started_at_ms: completed_at_ms.saturating_sub(outcome.duration_ms),
                completed_at_ms,
                duration_ms: outcome.duration_ms,
            },
            usage: harness_contract::outcome::OutcomeUsage {
                input_tokens: Some(outcome.input_tokens),
                output_tokens: Some(outcome.output_tokens),
                cached_tokens: Some(outcome.cached_tokens),
                evaluation_tokens: outcome
                    .evaluation_budget_observed
                    .then_some(outcome.evaluation_tokens_consumed),
                tool_calls: outcome.tool_calls,
                duplicate_tool_calls: outcome.duplicate_tool_calls,
                retries: 0,
                max_observed_concurrency: outcome.max_tool_concurrency_observed,
            },
            terminal,
            quality,
            observation: harness_contract::outcome::OutcomeObservation {
                source: if evaluation_isolated {
                    "harness_eval.conversation_terminal".to_string()
                } else {
                    "runtime.conversation_terminal".to_string()
                },
                observed_at_ms: completed_at_ms,
                freshness_ms: 0,
            },
            strategy_feedback: harness_contract::outcome::OutcomeStrategyFeedback {
                workload: strategy_workload,
                verification_blocked: !outcome.working_state_verified
                    || outcome.evaluation_budget_breached,
                context_pressure: outcome.input_tokens.saturating_mul(100)
                    >= u64::from(self.model_context_window).saturating_mul(80),
                coordination_cost_ms: outcome.merge_cost_ms,
                evaluation_environment: if evaluation_isolated {
                    "harness_evaluation".to_string()
                } else {
                    "production".to_string()
                },
            },
            evidence_refs: Vec::new(),
            evidence_completeness: if outcome.working_state_verified {
                harness_contract::reality::EvidenceCompleteness::Sufficient
            } else if outcome.evidence_overlap_observed {
                harness_contract::reality::EvidenceCompleteness::Partial
            } else {
                harness_contract::reality::EvidenceCompleteness::None
            },
            schema_revision: harness_contract::outcome::OUTCOME_SCHEMA_REVISION,
        };
        service
            .record_terminal(&canonical)
            .map_err(|error| RuntimeError::new(format!("record canonical outcome: {error}")))?;
        Ok(())
    }

    pub(super) fn append_execution_runtime_event(
        &self,
        scope: RuntimeEventScope,
        kind: &'static str,
        status: Option<String>,
        mut refs: Vec<RuntimeEventRef>,
        payload: serde_json::Value,
    ) {
        let Some(store) = self.runtime_event_store.as_ref() else {
            return;
        };
        let session_id = self.session_id().to_string();
        let execution_bus = self.cowd_bus();
        if let Some(context) =
            execution_bus.and_then(crate::CowdEventBus::current_execution_context)
        {
            for (kind, id) in [
                ("execution", context.execution_id),
                ("session", context.session_id),
                ("turn", context.turn_id),
            ] {
                if !refs
                    .iter()
                    .any(|reference| reference.kind == kind && reference.id == id)
                {
                    refs.push(RuntimeEventRef {
                        kind: kind.to_string(),
                        id,
                    });
                }
            }
        }
        let mut input = RuntimeEventInput {
            stream_id: format!("session:{session_id}"),
            scope,
            kind: kind.to_string(),
            status,
            actor: Some("conversation_runtime".to_string()),
            refs,
            payload,
        };
        if scope == RuntimeEventScope::Tool && kind.starts_with("tool.invocation.") {
            if let Some(bus) = execution_bus {
                if let Some(tool_call_id) = input
                    .refs
                    .iter()
                    .find(|reference| reference.kind == "tool_call")
                    .map(|reference| reference.id.clone())
                {
                    let tool_contract_id = input
                        .payload
                        .get("tool_name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                    let Some(binding) = bus.current_tool_activity_binding(
                        &tool_call_id,
                        tool_contract_id.as_deref().unwrap_or("unknown_tool"),
                    ) else {
                        tracing::warn!(
                            session_id,
                            event_kind = kind,
                            "Tool lifecycle event rejected because no active Runtime activity owns it"
                        );
                        return;
                    };
                    match input.with_activity_binding(binding) {
                        Ok(bound) => input = bound,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                session_id,
                                event_kind = kind,
                                "Tool activity binding rejected before Runtime event append"
                            );
                            return;
                        }
                    }
                }
            } else {
                tracing::warn!(
                    session_id,
                    event_kind = kind,
                    "Tool lifecycle event rejected because no active Runtime activity owns it"
                );
                return;
            }
        } else if scope == RuntimeEventScope::Skill && kind == "skill.activation.selected" {
            if let Some(owner) =
                execution_bus.and_then(crate::CowdEventBus::current_activity_binding)
            {
                if let Some(skill_id) = input
                    .refs
                    .iter()
                    .find(|reference| reference.kind == "skill")
                    .map(|reference| reference.id.clone())
                {
                    let turn_index = input
                        .payload
                        .get("turn_index")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default();
                    let activation_id = crate::cowd_event::owned_child_activity_id(
                        &owner,
                        "skill",
                        &format!("{skill_id}:{turn_index}"),
                    );
                    let binding = harness_contract::projection::RuntimeActivityBinding {
                        root_execution_id: owner.root_execution_id.clone(),
                        session_id: owner.session_id.clone(),
                        turn_id: owner.turn_id.clone(),
                        root_task_id: owner.root_task_id.clone(),
                        task_id: owner.task_id.clone(),
                        activity_id: activation_id.clone(),
                        node_id: owner.node_id.clone(),
                        parent_activity_id: Some(owner.activity_id.clone()),
                        initiator_activity_id: Some(owner.activity_id),
                        team_run_id: owner.team_run_id,
                        agent_instance_id: owner.agent_instance_id,
                        agent_run_id: owner.agent_run_id,
                        skill_id: Some(skill_id),
                        skill_revision: input
                            .payload
                            .pointer("/invocation_evidence/version")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        skill_activation_id: Some(activation_id),
                        tool_contract_id: None,
                        tool_call_id: None,
                        approval_id: None,
                        parallel_group_id: owner.parallel_group_id,
                        revision: owner.revision,
                        fence: owner.fence,
                        generation: owner.generation,
                    };
                    match input.with_activity_binding(binding) {
                        Ok(bound) => input = bound,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                session_id,
                                event_kind = kind,
                                "Skill activity binding rejected before Runtime event append"
                            );
                            return;
                        }
                    }
                }
            } else {
                tracing::warn!(
                    session_id,
                    event_kind = kind,
                    "Skill activation rejected because no active Runtime activity owns it"
                );
                return;
            }
        }
        if let Err(error) = store.append(input) {
            tracing::warn!(%error, session_id, event_kind = kind, "execution runtime event append failed");
        }
    }

    pub(super) fn record_message_event(
        &self,
        msg: &crate::session::ConversationMessage,
        _sequence: usize,
    ) {
        // Record the message in the event log for time-travel debugging.
        if let Some(ref log) = self.event_log {
            if let Ok(mut guard) = log.lock() {
                guard.push(MessageEvent::MessageAppended {
                    message: msg.clone(),
                });
            }
        }
    }

    pub(super) async fn record_runtime_policy_decision(
        &self,
        decision: &crate::execution_core::RuntimeExecutionDecision,
        sequence: usize,
    ) {
        let requires_review = decision.modifiers().iter().any(|modifier| {
            matches!(
                modifier,
                harness_contract::core::ExecutionModifier::WithVerifier
                    | harness_contract::core::ExecutionModifier::WithReviewer
            )
        }) || decision
            .gates()
            .contains(&harness_contract::core::ExecutionPolicyGate::Approval);
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::RuntimePolicyDecision {
                summary: crate::cowd_event::RuntimePolicyDecisionSummary {
                    level: format!("{:?}", decision.complexity()),
                    score: (decision.confidence * 100.0).round() as u16,
                    recommended_profile: format!("{:?}", self.context_profile()),
                    agent_mode: decision.pattern().as_str().to_string(),
                    requires_review,
                    signal_count: decision.reasons.len(),
                },
            });
        }

        let Some(ref port) = self.session_journal_port else {
            return;
        };
        let session_id = self.session_id().to_string();
        let payload = serde_json::json!({
            "decision_id": decision.decision_id,
            "pattern": decision.pattern(),
            "complexity": decision.complexity(),
            "risk": decision.risk(),
            "confidence": decision.confidence,
            "modifiers": decision.modifiers(),
            "gates": decision.gates(),
            "collaboration_lift": decision.collaboration_lift(),
            "compile_target": decision.compile_target,
            "strategy_lease": decision.lease,
            "decision_source": decision.strategy.source,
            "requires_review": requires_review,
            "reasons": decision.reasons,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let mut event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            sequence,
            crate::RuntimeSessionEventKind::RuntimePolicyDecided,
            payload,
            created_at_ms,
        );
        event.status = Some("completed".to_string());
        if let Err(error) = port.append_event(&event).await {
            tracing::warn!(%error, session_id, sequence, "runtime policy domain event append failed");
        }
    }
}
