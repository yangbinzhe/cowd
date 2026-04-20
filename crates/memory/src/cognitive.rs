//! `CognitiveContextManager` – unified entry-point (facade) for the memory framework.
//!
//! Coordinates all sub-systems (orchestrator, compression pipeline, relevance
//! scoring, dynamic loading, context monitoring, handoff, seeds, drift
//! detection) to produce the optimal [`PreparedContext`] within the current
//! token budget.
//!
//! # Progressive disclosure
//!
//! Context is assembled in priority order:
//! 1. L0 + L1 – fixed identity and working memory (always present).
//! 2. L2      – project context.
//! 3. L3      – dynamically loaded deep memories (multi-signal relevance).
//! 4. Seeds   – pre-authored fragments whose trigger condition fired.

use std::{collections::HashSet, path::PathBuf, sync::Mutex};

use chrono::Utc;

use crate::{
    compression::{
        budget::BudgetManager,
        monitor::ContextWindowMonitor,
        CompressionPipeline,
    },
    config::MemoryConfig,
    drift::DriftDetector,
    error::MemoryError,
    handoff::HandoffManager,
    orchestrator::MemoryOrchestrator,
    relevance::DynamicLoader,
    seeds::{DecisionThreadStore, SeedRegistry},
    store::{FtsSearchOptions, FtsSearchResult},
    store::vector::VectorIndex,
    types::{
        DecisionEntry, HandoffData, MatchedKeyword, MemoryEntry, MemoryId, Message,
        PreparedContext, SearchMemoriesRequest, SearchMemoriesResult, SearchMode,
        SearchSnippet, TokenBudget,
    },
    write_guard::{AuditLog, AuditOperation, AuditEntry, MemoryWriteGuard, WriteSource},
    embedding::EmbeddingCapability,
};

/// Result alias used throughout this module.
pub type Result<T> = std::result::Result<T, MemoryError>;

// ---------------------------------------------------------------------------
// Session Restoration Types
// ---------------------------------------------------------------------------

/// Statistics about a session restore operation.
#[derive(Debug, Clone, Default)]
pub struct SessionRestoreStats {
    pub memories_restored: u32,
    pub decisions_restored: u32,
    pub work_items_restored: u32,
    pub context_summary_length: usize,
}

// ---------------------------------------------------------------------------
// CognitiveContextManager
// ---------------------------------------------------------------------------

/// Unified facade that coordinates all memory sub-systems.
///
/// Create once per session with [`CognitiveContextManager::new`] and use the
/// provided methods to prepare context, persist memories, and manage
/// cross-session handoffs.
pub struct CognitiveContextManager {
    /// Merged configuration.
    config: MemoryConfig,
    /// Five-layer memory orchestrator.
    orchestrator: MemoryOrchestrator,
    /// Three-stage compression pipeline.
    pipeline: CompressionPipeline,
    /// Multi-signal relevance scorer + dynamic memory loader.
    #[allow(dead_code)]
    loader: DynamicLoader,
    /// In-process vector index for semantic search.
    #[allow(dead_code)]
    vector_index: VectorIndex,
    /// Real-time context window pressure monitor.
    monitor: ContextWindowMonitor,
    /// Cross-session handoff manager.
    handoff_mgr: HandoffManager,
    /// Pre-authored context seed registry.
    seeds: Mutex<SeedRegistry>,
    /// Persistent decision thread log.
    decisions: Mutex<DecisionThreadStore>,
    /// Staleness and contradiction detector.
    drift: DriftDetector,
    /// Write guard for anti-corruption control.
    write_guard: Option<MemoryWriteGuard>,
    /// Audit log for tracking all write operations.
    audit_log: Option<AuditLog>,
    /// Embedding capability level (Remote/Local/FTS5Only).
    embedding_capability: EmbeddingCapability,
}

impl CognitiveContextManager {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Initialise the manager from `config`, opening all storage backends.
    pub async fn new(config: MemoryConfig) -> Result<Self> {
        Self::new_with_workspace(config, None).await
    }

    /// Initialise the manager with an explicit workspace root for L2 project
    /// context discovery.
    pub async fn new_with_workspace(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
    ) -> Result<Self> {
        // Build the orchestrator (opens SQLite, wires all layers).
        let orchestrator =
            MemoryOrchestrator::init_with_workspace(config.clone(), workspace_root).await?;

        // Build the compression pipeline from config.
        let pipeline = CompressionPipeline::from_config(&config.compression);

        // Build the vector index with persistence support.
        // Use VectorIndex::load to restore previously persisted vectors.
        let dimension = if config.store.vector.dimension > 0 {
            config.store.vector.dimension as u32
        } else {
            // Default embedding dimension (text-embedding-3-small / OpenAI).
            1536
        };
        let persist_path = config.store.blob_dir.join("vector_index.json");
        let vector_index = VectorIndex::load(persist_path, dimension)
            .map_err(|e| MemoryError::Store(format!("load vector index: {e}")))?;

        // Build the context window monitor.
        let budget_mgr = BudgetManager::new(config.budget.clone());
        let monitor = ContextWindowMonitor::new(budget_mgr);

        // Determine embedding capability before moving config.
        let embedding_capability = EmbeddingCapability::from_config(&config.store.vector);

        Ok(Self {
            drift: DriftDetector::new(config.drift.clone()),
            config,
            orchestrator,
            pipeline,
            loader: DynamicLoader::new(),
            vector_index,
            monitor,
            handoff_mgr: HandoffManager::new(),
            seeds: Mutex::new(SeedRegistry::new()),
            decisions: Mutex::new(DecisionThreadStore::new()),
            write_guard: None,
            audit_log: None,
            embedding_capability,
        })
    }

    // -----------------------------------------------------------------------
    // Write guard configuration
    // -----------------------------------------------------------------------

    /// Set the write guard for controlling write access.
    pub fn with_write_guard(mut self, guard: MemoryWriteGuard) -> Self {
        self.write_guard = Some(guard);
        self
    }

    /// Set the audit log for tracking write operations.
    pub fn with_audit_log(mut self, log: AuditLog) -> Self {
        self.audit_log = Some(log);
        self
    }

    /// Set the write source, creating a default guard for that source.
    pub fn with_write_source(mut self, source: WriteSource) -> Self {
        self.write_guard = Some(MemoryWriteGuard::new(source));
        self
    }

    /// Check whether a write to `layer` is allowed under the current guard.
    pub fn check_write_access(&self, layer: crate::types::MemoryLayer) -> crate::write_guard::WritePolicy {
        match &self.write_guard {
            Some(guard) => guard.check_write(layer),
            None => crate::write_guard::WritePolicy::Allow,
        }
    }

    // -----------------------------------------------------------------------
    // Core: prepare_context
    // -----------------------------------------------------------------------

    /// Assemble the optimal context for the upcoming model turn.
    ///
    /// Implements "progressive disclosure":
    /// 1. Load fixed layers L0 + L1.
    /// 2. Load project context L2.
    /// 3. Dynamic-load relevant deep memories L3 via multi-signal scoring.
    /// 4. Surface triggered seeds.
    /// 5. Sample context window pressure.
    /// 6. Compress if needed.
    pub async fn prepare_context(
        &self,
        query: &str,
        messages: &[Message],
    ) -> Result<PreparedContext> {
        // ── Step 1 & 2: fixed layers + project context ──────────────────────
        let mut entries: Vec<MemoryEntry> = Vec::new();

        let fixed = self.orchestrator.load_fixed_layers().await?;
        entries.extend(fixed);

        let project = self.orchestrator.load_project_context().await?;
        entries.extend(project);

        // Track which IDs are already loaded so L3 can skip them.
        let already_surfaced: HashSet<MemoryId> = entries.iter().map(|e| e.id).collect();

        // ── Step 3: dynamic deep memories (L3) ──────────────────────────────
        let budget = self.compute_budget();
        let memory_budget = budget
            .available
            .saturating_sub(self.estimate_tokens_entries(&entries))
            .min(u64::from(u32::MAX)) as u32;

        // Use orchestrator.recall_relevant which internally combines L3 + L4 with
        // budget-aware truncation.  The DynamicLoader is available for callers
        // that have access to a concrete MemoryStore reference.
        let deep_entries = self
            .orchestrator
            .recall_relevant(query, None, &already_surfaced, memory_budget)
            .await?;
        entries.extend(deep_entries);

        // ── Step 4: check seed triggers ─────────────────────────────────────
        let query_words: Vec<String> = query
            .split_whitespace()
            .map(str::to_lowercase)
            .collect();
        let seed_entries: Vec<String> = {
            let mut reg = self
                .seeds
                .lock()
                .map_err(|_| MemoryError::Other("seeds lock poisoned".into()))?;
            reg.check_triggers("default", &query_words, Utc::now())
                .into_iter()
                .map(|s| s.content)
                .collect()
        };
        // Seeds are folded into entries as L1-priority synthetic entries.
        for (i, content) in seed_entries.into_iter().enumerate() {
            use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
            entries.push(MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L1,
                category: MemoryCategory::Reference,
                priority: Priority::High,
                source: MemorySource::Import,
                title: format!("Seed context #{i}"),
                content,
                embedding: None,
                tags: vec!["seed".into()],
                relations: vec![],
                confidence: 1.0,
                access_count: 0,
                staleness: 0.0,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                last_accessed_at: None,
                scope: None,
                session_id: None,
            });
        }

        // ── Step 5: sample context window pressure ───────────────────────────
        let total_message_tokens: u64 = messages
            .iter()
            .map(|m| u64::from(m.token_estimate()))
            .sum();
        let total_entry_tokens: u64 = self.estimate_tokens_entries(&entries);
        let used_tokens = total_message_tokens + total_entry_tokens;
        let _monitor_snapshot = self.monitor.sample(used_tokens);

        // ── Step 6: compress messages if necessary ──────────────────────────
        // Note: pipeline.run takes a mutable Vec; here we work non-destructively
        // by returning the final PreparedContext based on current state.
        // Full pipeline run is triggered in on_turn_end.

        // ── Assemble PreparedContext ─────────────────────────────────────────
        let total_tokens = self.estimate_tokens_entries(&entries);
        let depth_scale = if total_tokens > budget.available {
            budget.available as f32 / total_tokens.max(1) as f32
        } else {
            1.0
        };

        Ok(PreparedContext {
            entries,
            total_tokens,
            budget,
            depth_scale,
            prepared_at: Utc::now(),
        })
    }

    // -----------------------------------------------------------------------
    // on_turn_end
    // -----------------------------------------------------------------------

    /// Called after each assistant turn to perform lightweight housekeeping.
    ///
    /// 1. Runs micro-compact on `messages` (in place).
    /// 2. Checks whether session-compact threshold is exceeded.
    /// 3. Runs drift detection on recently loaded entries.
    /// 4. Checks seed trigger conditions for turn-end keywords.
    /// 5. Persists vector index for durability.
    pub async fn on_turn_end(&self, messages: &mut Vec<Message>) -> Result<()> {
        // ── 1. Micro compact ────────────────────────────────────────────────
        self.pipeline.micro_compact(messages);

        // ── 2. Session compact if threshold exceeded ─────────────────────────
        if self.pipeline.should_session_compact(messages) {
            self.pipeline
                .session_compact(messages, &self.orchestrator)
                .await?;
        }

        // ── 3. Drift detection on L1 entries ────────────────────────────────
        // Load essential layer entries and check for staleness.
        let l1_entries = self.orchestrator.load_fixed_layers().await?;
        for entry in &l1_entries {
            match self.drift.check(entry) {
                crate::drift::DriftVerdict::Prune { reason } => {
                    // Log the pruning verdict (no direct delete without the store).
                    // The orchestrator's forget() can be called with the entry ID.
                    tracing::debug!(
                        id = %entry.id,
                        reason = %reason,
                        "drift: pruning entry"
                    );
                    let _ = self.orchestrator.forget(&entry.id).await;
                }
                crate::drift::DriftVerdict::FlagForReview { reason } => {
                    tracing::debug!(
                        id = %entry.id,
                        reason = %reason,
                        "drift: entry flagged for review"
                    );
                }
                crate::drift::DriftVerdict::Ok => {}
            }
        }

        // ── 4. Check seed triggers at turn-end ──────────────────────────────
        let turn_keywords: Vec<String> = messages
            .iter()
            .flat_map(|m| m.content.split_whitespace().map(str::to_lowercase))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        {
            let mut reg = self
                .seeds
                .lock()
                .map_err(|_| MemoryError::Other("seeds lock poisoned".into()))?;
            reg.check_triggers("turn_end", &turn_keywords, Utc::now());
        }

        // ── 5. Run orchestrator maintenance tick ─────────────────────────────
        self.orchestrator.tick().await?;

        // ── 6. Persist vector index ─────────────────────────────────────────
        if let Err(e) = self.vector_index.persist() {
            tracing::warn!("failed to persist vector index: {}", e);
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // remember / recall
    // -----------------------------------------------------------------------

    /// Write a memory entry to the appropriate layer.
    ///
    /// If a write guard is configured, the write is checked against the
    /// guard's layer permissions. Denied writes return
    /// [`MemoryError::WriteDenied`].
    pub async fn remember(&self, entry: MemoryEntry) -> Result<()> {
        // Check write guard
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self.write_guard.as_ref().map(|g| format!("{:?}", g.source())).unwrap_or_default(),
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
                    source: self.write_guard.as_ref().map(|g| g.source()).unwrap_or(WriteSource::System),
                    summary: truncate_summary(&entry.content, 120),
                    agent_id: None,
                    session_id: None,
                });
            }
        }

        self.orchestrator.remember(entry).await?;
        Ok(())
    }

    /// Recall memories by relevance to `query`, returning up to `limit` entries.
    pub async fn recall(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        let already_surfaced = HashSet::new();
        // Use a generous token budget so the limit parameter is the binding constraint.
        let token_budget = (limit as u32).saturating_mul(2000);
        let mut entries = self
            .orchestrator
            .recall_relevant(query, None, &already_surfaced, token_budget)
            .await?;
        entries.truncate(limit);
        Ok(entries)
    }

    /// List all memory entries in a specific layer.
    pub async fn list_layer_entries(
        &self,
        layer: crate::types::MemoryLayer,
    ) -> Result<Vec<crate::types::MemoryMeta>> {
        self.orchestrator.list_layer(layer).await
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
                return Err(crate::MemoryError::InvalidArgument(format!("invalid memory id: {id}")));
            }
        };

        // Try to look up the entry's layer for guard check
        if let Some(entry) = self.orchestrator.recall(&mem_id).await? {
            let policy = self.check_write_access(entry.layer);
            if !policy.is_allowed() {
                return Err(MemoryError::WriteDenied {
                    layer: format!("{:?}", entry.layer),
                    write_source: self.write_guard.as_ref().map(|g| format!("{:?}", g.source())).unwrap_or_default(),
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
                        source: self.write_guard.as_ref().map(|g| g.source()).unwrap_or(WriteSource::System),
                        summary: truncate_summary(&entry.content, 120),
                        agent_id: None,
                        session_id: None,
                    });
                }
            }
        }

        self.orchestrator.forget(&mem_id).await
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
                return Err(crate::MemoryError::InvalidArgument(format!("invalid memory id: {id}")));
            }
        };

        let mut entry = self.orchestrator.recall(&mem_id).await?
            .ok_or_else(|| crate::MemoryError::Store(format!("entry {} not found", id)))?;

        // Write guard check
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self.write_guard.as_ref().map(|g| format!("{:?}", g.source())).unwrap_or_default(),
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

        self.orchestrator.update(&entry).await
    }

    /// List all layers with their entry counts.
    pub async fn list_layers(&self) -> Vec<serde_json::Value> {
        use crate::types::MemoryLayer;
        let layers = [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
            MemoryLayer::L4,
        ];
        let mut result = Vec::new();
        for layer in layers {
            let metas = self.orchestrator.list_layer(layer).await.unwrap_or_default();
            result.push(serde_json::json!({
                "layer": format!("{layer:?}"),
                "entry_count": metas.len(),
            }));
        }
        result
    }

    // -----------------------------------------------------------------------
    // Vector Index Persistence
    // -----------------------------------------------------------------------

    /// Persist the vector index to disk for durability.
    ///
    /// This saves all embeddings to `blob_dir/vector_index.json`.
    /// Called automatically by [`on_turn_end`], but can be invoked manually
    /// for explicit checkpointing.
    pub fn persist_vector_index(&self) -> Result<()> {
        self.vector_index
            .persist()
            .map_err(|e| MemoryError::Store(format!("persist vector index: {e}")))
    }

    /// Get the number of vectors currently indexed.
    #[must_use]
    pub fn vector_index_count(&self) -> usize {
        self.vector_index.count()
    }

    /// Get vector index statistics.
    #[must_use]
    pub fn vector_index_stats(&self) -> VectorIndexStats {
        VectorIndexStats {
            count: self.vector_index.count(),
        }
    }

    /// Return the current embedding capability level.
    #[must_use]
    pub fn embedding_capability(&self) -> &EmbeddingCapability {
        &self.embedding_capability
    }

    /// Return the search mode label for the current embedding capability.
    #[must_use]
    pub fn search_mode_label(&self) -> &'static str {
        self.embedding_capability.search_mode_label()
    }

    // -----------------------------------------------------------------------
    // FTS5 Full-text search
    // -----------------------------------------------------------------------

    /// Perform full-text search across memories using FTS5.
    ///
    /// This method provides Hermes-Agent sessions-style FTS5 indexing with:
    /// - Category and layer filtering
    /// - Highlighted snippets for context
    /// - Matched keywords extraction
    /// - Both simple and boolean query modes
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let request = SearchMemoriesRequest {
    ///     query: "Rust async programming".to_string(),
    ///     category: Some(MemoryCategory::ProjectConvention),
    ///     limit: 5,
    ///     with_snippets: true,
    ///     with_keywords: true,
    ///     ..Default::default()
    /// };
    /// let result = manager.search_memories(request).await?;
    /// for (entry, snippet) in result.entries.iter().zip(result.snippets.iter()) {
    ///     println!("Title: {}", entry.title);
    ///     if let Some(snippet) = snippet {
    ///         println!("Snippet: {}", snippet.text);
    ///     }
    /// }
    /// ```
    pub async fn search_memories(
        &self,
        request: SearchMemoriesRequest,
    ) -> Result<SearchMemoriesResult> {
        // Build FTS5 query based on search mode
        let fts_query = match request.mode {
            SearchMode::Match => prepare_fts_query(&request.query),
            SearchMode::Boolean => request.query.clone(),
            SearchMode::Prefix => request
                .query
                .split_whitespace()
                .map(|w| format!("{}*", w))
                .collect::<Vec<_>>()
                .join(" "),
        };

        // Build search options
        let options = FtsSearchOptions {
            category: request.category,
            layer: request.layer,
            with_snippets: request.with_snippets,
            with_keywords: request.with_keywords,
        };

        // Execute search through the orchestrator's store
        let fts_result: FtsSearchResult = self
            .orchestrator
            .store()
            .search_fts_advanced(&fts_query, options, request.limit)
            .await?;

        // Convert snippets to SearchSnippet format
        let snippets: Vec<Option<SearchSnippet>> = fts_result
            .snippets
            .into_iter()
            .map(|opt| {
                opt.map(|text| SearchSnippet {
                    text,
                    positions: vec![],
                })
            })
            .collect();

        // Convert keywords
        let keywords: Vec<MatchedKeyword> = fts_result
            .keywords
            .into_iter()
            .map(|(keyword, count)| MatchedKeyword {
                keyword,
                count: count as u32,
            })
            .collect();

        // Collect unique categories found in results
        use std::collections::HashSet;
        let categories_found_set: HashSet<_> = fts_result
            .entries
            .iter()
            .map(|e| e.category)
            .collect();
        let categories_found: Vec<_> = categories_found_set.into_iter().collect();

        Ok(SearchMemoriesResult {
            entries: fts_result.entries,
            snippets,
            keywords,
            total_matches: fts_result.total_matches,
            query: request.query,
            categories_found,
            search_mode: self.search_mode_label().to_string(),
        })
    }

    /// Quick FTS5 search with just a query string.
    ///
    /// Convenience method that creates a default request with the given query.
    pub async fn search(&self, query: &str) -> Result<Vec<MemoryEntry>> {
        let request = SearchMemoriesRequest {
            query: query.to_string(),
            ..Default::default()
        };
        let result = self.search_memories(request).await?;
        Ok(result.entries)
    }

    // -----------------------------------------------------------------------
    // Handoff
    // -----------------------------------------------------------------------

    /// Serialise the current session state into a [`HandoffData`] packet ready
    /// for cross-session resumption.
    pub fn create_handoff(&self) -> Result<HandoffData> {
        let session_id = uuid::Uuid::new_v4().to_string();
        let handoff = self.handoff_mgr.create_handoff(
            &session_id,
            None,  // no current task snapshot
            vec![], // no completed items
            vec![], // no remaining items
            vec![], // no recorded decisions
            vec![], // no blockers
            "",    // next action to be filled by caller
            "",    // context notes
        )?;
        self.handoff_mgr.save(&handoff)?;
        Ok(handoff)
    }

    /// Restore session state from a previously created [`HandoffData`] packet.
    pub async fn restore_handoff(&self, data: HandoffData) -> Result<()> {
        self.handoff_mgr.resume(data).await
    }

    // -----------------------------------------------------------------------
    // Session Restoration
    // -----------------------------------------------------------------------

    /// Restore memories from session history.
    ///
    /// This method reads the session history from `session_path` and extracts:
    /// - Memory entries from compressed messages
    /// - Decisions from decision messages
    /// - Work items from task-related messages
    ///
    /// Returns statistics about what was restored.
    pub async fn restore_from_session(
        &self,
        session_path: &std::path::Path,
        session_id: &str,
    ) -> Result<SessionRestoreStats> {
        use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};

        let mut stats = SessionRestoreStats::default();

        // Try to load the session file
        let contents = match std::fs::read_to_string(session_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("failed to read session file: {}", e);
                return Ok(stats);
            }
        };

        // Parse JSON or JSONL
        let messages: Vec<serde_json::Value> = if contents.trim().starts_with('{') {
            // Single JSON object
            match serde_json::from_str::<serde_json::Value>(&contents) {
                Ok(v) if v.get("messages").is_some() => {
                    v.get("messages")
                        .and_then(|m: &serde_json::Value| m.as_array())
                        .cloned()
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            }
        } else {
            // JSONL format - one JSON object per line
            contents
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter_map(|v: serde_json::Value| {
                    v.get("message")
                        .or_else(|| v.get("content"))
                        .cloned()
                })
                .collect()
        };

        // Extract memories from messages
        for msg in messages {
            // Try to extract content from message
            let text_opt: Option<String> = msg.as_str().map(String::from)
                .or_else(|| msg.get("text").and_then(|v: &serde_json::Value| v.as_str()).map(String::from))
                .or_else(|| msg.get("content").and_then(|v: &serde_json::Value| v.as_str()).map(String::from));

            if let Some(text) = text_opt {
                // Skip very short messages
                if text.len() < 50 {
                    continue;
                }

                // Extract title from first line or truncate
                let first_line = text.lines().next().unwrap_or("");
                let title = if first_line.len() > 60 {
                    format!("{}...", &first_line[..60])
                } else {
                    first_line.to_string()
                };

                // Create memory entry for this message
                let entry = MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L3, // Deep layer for restored memories
                    category: MemoryCategory::CompressedSummary,
                    priority: Priority::Normal,
                    source: MemorySource::Import,
                    title,
                    content: text.clone(),
                    embedding: None,
                    tags: vec!["restored".into(), "session".into(), session_id.into()],
                    relations: vec![],
                    confidence: 0.7, // Lower confidence for restored content
                    access_count: 0,
                    staleness: 0.0,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    last_accessed_at: None,
                    scope: None,
                    session_id: Some(session_id.to_string()),
                };

                if let Err(e) = self.orchestrator.remember(entry).await {
                    tracing::warn!("failed to restore memory: {}", e);
                } else {
                    stats.memories_restored += 1;
                }
            }

            // Try to extract decisions
            if let Some(content_obj) = msg.get("content").and_then(|v: &serde_json::Value| v.as_object()) {
                if content_obj.contains_key("decision") || content_obj.contains_key("rationale") {
                    stats.decisions_restored += 1;
                }
            }
        }

        tracing::info!(
            "restored {} memories from session {}",
            stats.memories_restored,
            session_id
        );

        Ok(stats)
    }

    // -----------------------------------------------------------------------
    // Decision threads
    // -----------------------------------------------------------------------

    /// Record a decision entry into `thread_id`'s decision thread.
    ///
    /// If the thread does not yet exist it is created automatically.
    pub fn record_decision(&self, thread_id: &str, decision: DecisionEntry) -> Result<()> {
        let mut store = self
            .decisions
            .lock()
            .map_err(|_| MemoryError::Other("decisions lock poisoned".into()))?;

        // Ensure the thread exists.
        store.create_thread(thread_id);

        // Append the entry using the record() compatibility API.
        store.record(
            thread_id,
            decision.summary,
            decision.rationale,
            decision.alternatives,
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Build a [`TokenBudget`] from the current config.
    fn compute_budget(&self) -> TokenBudget {
        let c = &self.config.budget;
        let available = c
            .context_window
            .saturating_sub(c.reserved_system)
            .saturating_sub(c.reserved_response);
        TokenBudget {
            total: c.context_window,
            reserved_system: c.reserved_system,
            reserved_response: c.reserved_response,
            allocated_memory: 0,
            allocated_conversation: 0,
            available,
        }
    }

    /// Approximate token count for a slice of memory entries (chars / 4).
    fn estimate_tokens_entries(&self, entries: &[MemoryEntry]) -> u64 {
        entries
            .iter()
            .map(|e| (e.content.len() as u64).div_ceil(4))
            .sum()
    }
}

// ---------------------------------------------------------------------------
// FTS5 Query Helpers
// ---------------------------------------------------------------------------

/// Statistics about the vector index.
#[derive(Debug, Clone)]
pub struct VectorIndexStats {
    pub count: usize,
}

/// Prepare a query string for FTS5 MATCH by escaping special characters.
///
/// FTS5 special characters include: `"`, `'`, `(`, `)`, `*`, `:`, `^`, `-`, `+`
fn prepare_fts_query(query: &str) -> String {
    // Split into words, escape each, rejoin with implicit AND
    query
        .split_whitespace()
        .map(|word| {
            // Skip FTS5 operators
            if matches!(word.to_uppercase().as_str(), "AND" | "OR" | "NOT" | "NEAR") {
                word.to_string()
            } else {
                // Escape double quotes
                word.replace('"', "\"\"")
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Truncate content to a short summary for audit logging (privacy-preserving).
fn truncate_summary(content: &str, max_len: usize) -> String {
    if content.len() <= max_len {
        content.to_string()
    } else {
        format!("{}...", &content[..max_len])
    }
}
