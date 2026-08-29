use super::*;

impl CognitiveContextManager {
    // -----------------------------------------------------------------------
    // on_turn_end
    // -----------------------------------------------------------------------

    // `run_memory_post_turn` coordinates the post-turn helpers below. Runtime
    // owns transcript compaction, so this manager only extracts memories and
    // performs drift, seed, index, and graph maintenance.
    /// Compatibility entry point for non-runtime callers. Runtime-owned
    /// post-turn work must use [`Self::extract_and_remember_for_turn`].
    pub async fn extract_and_remember(&self, messages: &[Message]) -> Result<()> {
        let turn = MemoryTurnContext::new("memory-api", "memory-api");
        self.extract_and_remember_for_turn(&turn, messages).await
    }

    /// Extract memories from one explicitly identified turn and persist them.
    ///
    /// This covers steps 0, 0b, and 11 from the full turn-end sequence:
    ///   - Heuristic extraction from conversation messages (fast, sync)
    ///   - LLM extraction queued to background worker (non-blocking)
    ///   - Persist via `orchestrator.remember_batch`
    ///   - Index large tool outputs into the sandbox
    ///   - Batch-embed new entries into the vector index
    ///
    /// Failures are logged and swallowed so they never abort the turn.
    pub async fn extract_and_remember_for_turn(
        &self,
        turn: &MemoryTurnContext,
        messages: &[Message],
    ) -> Result<()> {
        let _extract_start = Instant::now();
        // ── 0. Extract and persist memories ──────────────────────────────────
        let mut pending_embeddings: Vec<(MemoryId, String)> = Vec::new();
        if messages.len() >= 2 {
            tracing::debug!(
                messages_count = messages.len(),
                has_user = messages.iter().any(|m| matches!(m.role, MessageRole::User)),
                has_assistant = messages
                    .iter()
                    .any(|m| matches!(m.role, MessageRole::Assistant)),
                has_tool = messages.iter().any(|m| matches!(m.role, MessageRole::Tool)),
                user_content_total = messages
                    .iter()
                    .filter(|m| matches!(m.role, MessageRole::User))
                    .map(|m| m.content.len())
                    .sum::<usize>(),
                "extract_and_remember: pre-extraction state"
            );

            // ── 0a. Heuristic extraction (Passes 1-4, fast / non-blocking) ──
            let mut heuristic_entries = if self.config.extractor.enabled {
                let raw = self.extractor.extract_heuristic(messages);
                self.extractor.finalize_entries(raw)
            } else {
                Vec::new()
            };
            let batch_tag = extraction_batch_tag(turn, messages);
            let mut durable_heuristic_entries = Vec::new();
            if !heuristic_entries.is_empty() {
                canonicalize_automatic_entries(turn, &batch_tag, &mut heuristic_entries);
                tracing::info!(
                    entries_count = heuristic_entries.len(),
                    "extract_and_remember: heuristic extracted {} entries",
                    heuristic_entries.len()
                );
                let heuristic_contents = heuristic_entries
                    .iter()
                    .map(memory_embedding_text)
                    .collect::<Vec<_>>();

                match self
                    .orchestrator
                    .remember_batch_for_turn(turn, heuristic_entries.clone())
                    .await
                {
                    Ok(ids) => {
                        for (entry, id) in heuristic_entries.iter_mut().zip(ids.iter().copied()) {
                            entry.id = id;
                        }
                        pending_embeddings.extend(ids.into_iter().zip(heuristic_contents));
                        durable_heuristic_entries = heuristic_entries;
                        tracing::debug!(
                            "extract_and_remember: heuristic memories persisted successfully"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "extract_and_remember: heuristic memory persistence failed"
                        );
                    }
                }
            }

            // Queue semantic extraction for every substantive turn. It must not
            // depend on a heuristic keyword hit; otherwise L3 never receives
            // novel patterns that the fast extractor cannot recognize.
            if self.config.extractor.enabled
                && self.extractor.llm_client().is_some()
                && MemoryExtractor::should_extract(messages)
            {
                let request = BackgroundExtractionRequest {
                    turn: turn.clone(),
                    messages: messages.to_vec(),
                    heuristic_entries: durable_heuristic_entries,
                };
                self.background_extraction_state
                    .pending_requests
                    .fetch_add(1, Ordering::Relaxed);
                match self.extract_tx.send(request).await {
                    Ok(()) => {
                        self.background_extraction_state
                            .accepted_requests
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(
                            "extract_and_remember: queued messages for background LLM extraction"
                        );
                    }
                    Err(error) => {
                        self.background_extraction_state
                            .pending_requests
                            .fetch_sub(1, Ordering::Relaxed);
                        self.background_extraction_state
                            .failed_requests
                            .fetch_add(1, Ordering::Relaxed);
                        *self.background_extraction_state.last_error.lock() =
                            Some(error.to_string());
                        tracing::error!(
                            %error,
                            "extract_and_remember: background LLM extraction queue closed"
                        );
                    }
                }
            }

            // ── 0b. Index large tool outputs into sandbox ───────────────────
            let mut sandbox = self.tool_sandbox.lock();
            for msg in messages
                .iter()
                .filter(|m| matches!(m.role, MessageRole::Tool))
            {
                let call_id = msg.tool_use_id.as_deref().unwrap_or("unknown");
                let tool_name = msg.tool_name.as_deref().unwrap_or("unknown_tool");
                let threshold = self.config.tuning.sandbox_min_lines;
                if let Some(summary) =
                    sandbox.index_tool_output(call_id, tool_name, &msg.content, threshold)
                {
                    tracing::info!(
                        call_id,
                        tool_name,
                        total_lines = summary.total_lines,
                        full_size = summary.full_size_bytes,
                        "tool_sandbox: indexed large tool output"
                    );
                }
            }
        } else {
            tracing::debug!(
                messages_count = messages.len(),
                "extract_and_remember: skipped (insufficient messages)"
            );
        }

        // ── 11. Batch-embed new entries ─────────────────────────────────────
        if !pending_embeddings.is_empty() {
            match embed_memory_entries(
                &self.embedding_capability,
                &self.vector_index,
                &pending_embeddings,
                false,
            )
            .await
            {
                Ok(indexed) => {
                    tracing::info!(count = indexed, "batch embedded memory entries");
                }
                Err(error) => {
                    tracing::warn!(%error, "batch embedding failed");
                }
            }
        }

        let _extract_elapsed = _extract_start.elapsed();
        self.perf_monitor.record_extract(_extract_elapsed);
        self.invalidate_prepare_context_cache();

        Ok(())
    }

    /// Write a memory entry to the appropriate layer.
    ///
    /// If a write guard is configured, the write is checked against the
    /// guard's layer permissions. Denied writes return
    /// [`MemoryError::WriteDenied`].
    /// Persist an entry with an explicit Runtime turn. This is the production
    /// route used by [`MemoryKernel`]; ownership is never inferred from a
    /// mutable manager field.
    pub async fn remember_for_turn(
        &self,
        turn: &MemoryTurnContext,
        mut entry: MemoryEntry,
    ) -> Result<()> {
        entry
            .session_id
            .get_or_insert_with(|| turn.session_id.clone());
        entry
            .source_agent
            .get_or_insert_with(|| turn.agent_id.clone());
        entry.scope = scoped_entry_scope(turn, &entry);
        self.remember_inner(entry, Some(turn)).await
    }

    /// Persist an entry supplied by a non-runtime caller. It receives a
    /// deterministic `memory-api` identity when the caller omits ownership,
    /// rather than reading mutable process-wide state.
    pub async fn remember(&self, entry: MemoryEntry) -> Result<()> {
        self.remember_inner(entry, None).await
    }

    pub(super) async fn remember_inner(
        &self,
        mut entry: MemoryEntry,
        turn: Option<&MemoryTurnContext>,
    ) -> Result<()> {
        // CognitiveContextManager is the ordinary Runtime/API memory path;
        // it must never be a second L4 promotion route.  Runtime's
        // L4PromotionService owns the governed candidate lifecycle and calls
        // the orchestrator's typed promotion command directly.
        if entry.layer == MemoryLayer::L4 {
            return Err(MemoryError::WriteDenied {
                layer: "L4".to_string(),
                write_source: "cognitive_memory_write_requires_l4_promotion_service".to_string(),
            });
        }
        // Direct manager callers are administrative/non-runtime callers. They
        // still receive a deterministic identity instead of reviving the old
        // process-wide active state or persisting the `session_` sentinel.
        let fallback_turn = MemoryTurnContext::new(
            entry.session_id.as_deref().unwrap_or("memory-api"),
            entry.source_agent.as_deref().unwrap_or("memory-api"),
        )
        .with_project_id(match &entry.scope {
            MemoryScope::Project(project) if !project.trim().is_empty() => Some(project.clone()),
            _ => None,
        });
        let turn = turn.unwrap_or(&fallback_turn);
        entry
            .session_id
            .get_or_insert_with(|| turn.session_id.clone());
        entry
            .source_agent
            .get_or_insert_with(|| turn.agent_id.clone());
        entry.scope = scoped_entry_scope(turn, &entry);
        // Check write guard
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self
                    .write_guard
                    .as_ref()
                    .map(|g| format!("{:?}", g.source()))
                    .unwrap_or_default(),
            });
        }

        // Audit log
        if policy.requires_audit() || self.audit_log.is_some() {
            if let Some(ref log) = self.audit_log {
                let _ = log.log(&AuditEntry {
                    timestamp: Utc::now(),
                    operation: AuditOperation::Create,
                    entry_id: entry.id.to_string(),
                    layer: format!("{:?}", entry.layer),
                    source: self
                        .write_guard
                        .as_ref()
                        .map(|g| g.source())
                        .unwrap_or(WriteSource::System),
                    summary: truncate_summary(
                        &entry.content,
                        self.config.tuning.audit_truncate_len,
                    ),
                    agent_id: entry.source_agent.clone(),
                    session_id: entry.session_id.clone(),
                });
            }
        }

        self.orchestrator.remember_for_turn(turn, entry).await?;
        self.invalidate_prepare_context_cache();
        Ok(())
    }

    /// Create a user-authored memory entry through the same guarded write path
    /// used by internal memory operations.
    pub async fn create_entry(
        &self,
        layer: MemoryLayer,
        category: MemoryCategory,
        title: &str,
        content: &str,
        priority: Priority,
        tags: Vec<String>,
        scope: MemoryScope,
    ) -> Result<MemoryId> {
        let id = self
            .orchestrator
            .write(
                layer,
                category,
                title,
                content,
                priority,
                MemorySource::UserExplicit,
                tags,
                scope,
            )
            .await?;
        self.log_memory_audit(AuditOperation::Create, id.to_string(), layer, content);
        self.invalidate_prepare_context_cache();
        Ok(id)
    }

    /// Get a single memory entry by ID.
    pub async fn get_entry(&self, id: &str) -> Result<Option<crate::types::MemoryEntry>> {
        let mem_id = match uuid::Uuid::try_parse(id) {
            Ok(id) => id,
            Err(_) => return Ok(None),
        };
        self.orchestrator.recall(&mem_id).await
    }

    /// Delete a memory entry by ID.
    ///
    /// If a write guard is configured, the delete is checked against the
    /// guard's layer permissions. Note: the layer must be inferred from the
    /// entry itself; if the entry is not found, the delete is still attempted
    /// (it will simply be a no-op).
    pub async fn delete_entry(&self, id: &str) -> Result<()> {
        let mem_id = match uuid::Uuid::try_parse(id) {
            Ok(id) => id,
            Err(_) => {
                return Err(crate::MemoryError::InvalidArgument(format!(
                    "invalid memory id: {id}"
                )));
            }
        };

        // Try to look up the entry's layer for guard check
        if let Some(entry) = self.orchestrator.recall(&mem_id).await? {
            let policy = self.check_write_access(entry.layer);
            if !policy.is_allowed() {
                return Err(MemoryError::WriteDenied {
                    layer: format!("{:?}", entry.layer),
                    write_source: self
                        .write_guard
                        .as_ref()
                        .map(|g| format!("{:?}", g.source()))
                        .unwrap_or_default(),
                });
            }
            // Audit log for delete
            if policy.requires_audit() || self.audit_log.is_some() {
                if let Some(ref log) = self.audit_log {
                    let _ = log.log(&AuditEntry {
                        timestamp: Utc::now(),
                        operation: AuditOperation::Delete,
                        entry_id: id.to_string(),
                        layer: format!("{:?}", entry.layer),
                        source: self
                            .write_guard
                            .as_ref()
                            .map(|g| g.source())
                            .unwrap_or(WriteSource::System),
                        summary: truncate_summary(
                            &entry.content,
                            self.config.tuning.audit_truncate_len,
                        ),

                        agent_id: None,
                        session_id: None,
                    });
                }
            }
        }

        self.orchestrator.forget(&mem_id).await?;
        self.invalidate_prepare_context_cache();
        Ok(())
    }

    /// Update a memory entry's content, tags, and/or priority.
    pub async fn update_entry(
        &self,
        id: &str,
        content: Option<String>,
        tags: Option<Vec<String>>,
        priority: Option<crate::types::Priority>,
    ) -> Result<()> {
        let mem_id = match uuid::Uuid::try_parse(id) {
            Ok(id) => id,
            Err(_) => {
                return Err(crate::MemoryError::InvalidArgument(format!(
                    "invalid memory id: {id}"
                )));
            }
        };

        let mut entry = self
            .orchestrator
            .recall(&mem_id)
            .await?
            .ok_or_else(|| crate::MemoryError::Store(format!("entry {} not found", id)))?;

        // Write guard check
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self
                    .write_guard
                    .as_ref()
                    .map(|g| format!("{:?}", g.source()))
                    .unwrap_or_default(),
            });
        }

        if let Some(c) = content {
            entry.content = c;
        }
        if let Some(t) = tags {
            entry.tags = t;
        }
        if let Some(p) = priority {
            entry.priority = p;
        }
        entry.updated_at = chrono::Utc::now();
        entry.staleness = 0.0;

        self.orchestrator.update(&entry).await?;
        self.log_memory_audit(
            AuditOperation::Update,
            entry.id.to_string(),
            entry.layer,
            &entry.content,
        );
        self.invalidate_prepare_context_cache();
        Ok(())
    }

    /// List all layers with their entry counts.
    pub async fn list_layers(&self) -> Vec<serde_json::Value> {
        use crate::types::MemoryLayer;
        let aggregate = self
            .store_aggregate(crate::kernel::MEMORY_STALE_WARNING_THRESHOLD)
            .await
            .unwrap_or_default();
        let layers = [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
            MemoryLayer::L4,
        ];
        let mut result = Vec::new();
        for layer in layers {
            let layer_aggregate = aggregate
                .layers
                .iter()
                .find(|value| value.layer == layer)
                .cloned();
            let entry_count = layer_aggregate
                .as_ref()
                .map(|value| value.active_count)
                .unwrap_or_default();
            let retained_count = layer_aggregate
                .as_ref()
                .map(|value| value.retained_count)
                .unwrap_or_default();
            let archived_count = layer_aggregate
                .as_ref()
                .map(|value| value.archived_count)
                .unwrap_or_default();
            let (enabled, role, producer, write_mode) = match layer {
                MemoryLayer::L0 => (
                    self.config.layers.l0_enabled,
                    "stable identity and explicit global invariants",
                    "explicit user or system identity writes",
                    "explicit",
                ),
                MemoryLayer::L1 => (
                    true,
                    "high-salience working preferences and active constraints",
                    "explicit writes and current-turn preference extraction",
                    "automatic_and_explicit",
                ),
                MemoryLayer::L2 => (
                    true,
                    "project conventions, decisions, and reusable resolutions",
                    "current-turn extraction and governed imports",
                    "automatic_and_explicit",
                ),
                MemoryLayer::L3 => (
                    true,
                    "deep patterns, semantic checkpoints, and long-term references",
                    "semantic extraction and session compaction checkpoints",
                    "automatic_and_explicit",
                ),
                MemoryLayer::L4 => (
                    self.config.layers.l4_enabled,
                    "reviewed cross-agent and team knowledge",
                    "Runtime evidence-backed promotion only",
                    "governed_promotion_only",
                ),
            };
            result.push(serde_json::json!({
                "layer": format!("{layer:?}"),
                "entry_count": entry_count,
                "retained_count": retained_count,
                "archived_count": archived_count,
                "enabled": enabled,
                "role": role,
                "producer": producer,
                "write_mode": write_mode,
                "automatic_extraction": self.config.extractor.enabled
                    && matches!(layer, MemoryLayer::L1 | MemoryLayer::L2 | MemoryLayer::L3),
                "state": if !enabled {
                    "disabled"
                } else if entry_count == 0 {
                    "ready_empty"
                } else {
                    "ready"
                },
            }));
        }
        result
    }
}
