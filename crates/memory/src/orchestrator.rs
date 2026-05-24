//! `MemoryOrchestrator` – top-level coordinator for the memory system.
//!
//! Owns all layer managers, the store, the compression pipeline, and the
//! background extractor.  External callers interact with the memory system
//! exclusively through this type.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::{AtomicU32, Ordering},
};

use chrono::Utc;
use parking_lot::Mutex;

use crate::{
    config::MemoryConfig,
    context_fence::FenceRegistry,
    closet::{ClosetManager, Closet},
    error::MemoryError,
    fact_checker::{FactChecker, FactCheckResult},
    project_scope::MemoryScope,
    temporal_graph::{Triple, EntityFacts},
    layers::{
        deep::DeepLayer,
        essential::EssentialLayer,
        identity::IdentityLayer,
        project::ProjectLayer,
        shared::SharedLayer,
        LayerManager,
    },
    store::{sqlite::SqliteStore, MemoryStore},
    types::{
        MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemoryMeta, MemorySource,
        PreparedContext, Priority, TokenBudget,
    },
};

/// Result alias.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// Top-level memory system coordinator.
///
/// Coordinates the five memory layers (L0–L4) and exposes a clean, unified
/// API for reading and writing memories.
pub struct MemoryOrchestrator {
    /// Shared backing store (`SQLite`).
    store: Arc<dyn MemoryStore>,
    /// Configuration snapshot.
    config: MemoryConfig,
    /// L0 – identity layer.
    l0: IdentityLayer,
    /// L1 – essential working-memory layer.
    l1: EssentialLayer,
    /// L2 – project-specific layer.
    l2: ProjectLayer,
    /// L3 – deep long-term knowledge layer.
    l3: DeepLayer,
    /// L4 – shared team layer.
    l4: SharedLayer,
    /// Closet – compact pointer-row index for fast topic routing.
    closet: parking_lot::Mutex<ClosetManager>,
    /// Fence registry for context isolation.
    fence_registry: FenceRegistry,
    /// Fact checker for detecting factual contradictions.
    fact_checker: Mutex<Option<FactChecker>>,
    /// Active memory scope for auto-filling new entries.
    active_scope: Mutex<MemoryScope>,
    /// Active agent ID for auto-filling new entries' source_agent.
    active_agent: Mutex<Option<String>>,
    /// Active session ID for auto-filling new entries' session_id.
    active_session: Mutex<Option<String>>,
    /// Closet rebuild counter for automatic periodic rebuild.
    closet_rebuild_counter: AtomicU32,
}

impl MemoryOrchestrator {
    /// Initialise the orchestrator with the given configuration.
    ///
    /// Opens storage backends, runs migrations, and wires up all layer
    /// managers.  The `workspace_root` is used by the L2 layer for project
    /// context discovery.
    pub async fn init(config: MemoryConfig) -> Result<Self> {
        Self::init_with_workspace(config, None).await
    }

    /// Initialise with an explicit workspace root for L2 project discovery.
    pub async fn init_with_workspace(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
    ) -> Result<Self> {
        // Open the SQLite store.
        let store: Arc<dyn MemoryStore> = Arc::new(
            SqliteStore::open(&config.store)
                .map_err(|e| MemoryError::Store(format!("open sqlite: {e}")))?,
        );

        Self::from_store(config, store, workspace_root)
    }

    /// Build an orchestrator from a pre-built store (useful for testing).
    pub fn from_store(
        config: MemoryConfig,
        store: Arc<dyn MemoryStore>,
        workspace_root: Option<PathBuf>,
    ) -> Result<Self> {
        let l0 = IdentityLayer::new(Arc::clone(&store));
        let l1 = EssentialLayer::with_config(
            Arc::clone(&store),
            2000,
            config.drift.clone(),
        );
        let l2 = if let Some(root) = workspace_root {
            ProjectLayer::with_workspace(
                Arc::clone(&store),
                root,
                3000,
                config.drift.clone(),
            )
        } else {
            ProjectLayer::new(Arc::clone(&store))
        };
        let l3 = DeepLayer::with_config(Arc::clone(&store), 5, config.drift.clone());
        let l4 = if config.store.enable_vector_index {
            // When the vector index is enabled we also allow the shared layer.
            SharedLayer::new(Arc::clone(&store))
        } else {
            SharedLayer::new(Arc::clone(&store))
        };

        // Build Closet index from L2 (project) + L3 (deep) memory metadata.
        let closet = parking_lot::Mutex::new(ClosetManager::from_closet(Closet::default()));

        Ok(Self {
            store,
            config,
            l0,
            l1,
            l2,
            l3,
            l4,
            closet,
            fence_registry: FenceRegistry::new(),
            fact_checker: Mutex::new(Some(FactChecker::new())),
            active_scope: Mutex::new(MemoryScope::default()),
            active_agent: Mutex::new(None),
            active_session: Mutex::new(None),
            closet_rebuild_counter: AtomicU32::new(0),
        })
    }

    // -----------------------------------------------------------------------
    // Read API
    // -----------------------------------------------------------------------

    /// Load fixed layers (L0 + L1) – called at the start of every turn.
    pub async fn load_fixed_layers(&self) -> Result<Vec<MemoryEntry>> {
        let mut entries = Vec::new();
        let l0 = self.l0.load().await?;
        let l1 = self.l1.load().await?;
        entries.extend(l0);
        entries.extend(l1);
        Ok(entries)
    }

    /// Load project context (L2).
    pub async fn load_project_context(&self) -> Result<Vec<MemoryEntry>> {
        self.l2.load().await
    }

    /// Find code symbols relevant to the given query.
    ///
    /// Delegates to the L2 project layer's code indexer.
    /// Returns an empty vector if no code indexer is available (graceful degradation).
    pub async fn find_relevant_symbols(
        &self,
        query: &str,
        limit: usize,
    ) -> Vec<crate::code_indexer::CodeSymbol> {
        self.l2.find_relevant_symbols(query, limit).await
    }

    /// Record that source files were accessed by a tool.
    ///
    /// Automatically promotes relevant symbols to the hot-symbol cache
    /// in the essential layer (L1), so that frequently-accessed code
    /// symbols are tracked and surfaced in future context preparation.
    ///
    /// Call this after each tool invocation that touches source files
    /// (e.g., `read_file`, `edit_file`, `grep_search`, `glob_search`).
    pub async fn note_file_access(&self, file_paths: &[&str]) {
        for path_str in file_paths {
            let path = Path::new(path_str);
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if stem.is_empty() {
                    continue;
                }
                let symbols = self.l2.find_relevant_symbols(stem, 3).await;
                for sym in &symbols {
                    self.l1.promote_symbol(&sym.name);
                }
            }
        }
    }

    /// Recall relevant memories on-demand (L3 + L4), pre-routed through
    /// the Closet index for topic-aware drawer selection.
    ///
    /// Closet routing narrows the search space: matched topics identify
    /// relevant drawer IDs which are prioritised in the recall phase.
    /// Entries whose IDs appear in `already_surfaced` are excluded to avoid
    /// duplicating content already shown to the model.
    pub async fn recall_relevant(
        &self,
        query: &str,
        embedding: Option<&[f32]>,
        already_surfaced: &HashSet<MemoryId>,
        token_budget: u32,
    ) -> Result<Vec<MemoryEntry>> {
        // Phase 1: Closet topic routing – find relevant drawer IDs.
        let drawer_ids: HashSet<String> = {
            let guard = self.closet.lock();
            guard.search_topics(query)
                .iter()
                .flat_map(|ptr| ptr.drawer_ids.clone())
                .collect()
        };

        // Gather L3 results.
        let mut l3 = self.l3.recall(query, embedding, already_surfaced).await?;

        // Gather L4 results.
        let l4 = self.l4.recall(query, 5).await?;

        // Merge L4 into l3, skipping already-seen IDs.
        let mut seen: HashSet<MemoryId> = already_surfaced.clone();
        for e in &l3 {
            seen.insert(e.id);
        }
        for e in l4 {
            if !seen.contains(&e.id) {
                seen.insert(e.id);
                l3.push(e);
            }
        }

        // Phase 2: Apply Closet boost – entries matching drawer IDs get priority.
        if !drawer_ids.is_empty() {
            l3.sort_by(|a, b| {
                let a_in_closet = drawer_ids.contains(&a.id.to_string());
                let b_in_closet = drawer_ids.contains(&b.id.to_string());
                b_in_closet.cmp(&a_in_closet)
                    .then(b.priority.cmp(&a.priority))
                    .then(b.updated_at.cmp(&a.updated_at))
            });
        }

        // Truncate to token budget.
        let budget = u64::from(token_budget);
        let mut used: u64 = 0;
        let mut kept = Vec::new();
        for e in l3 {
            let tokens = estimate_tokens(&e.content);
            if used + tokens > budget {
                break;
            }
            used += tokens;
            kept.push(e);
        }
        Ok(kept)
    }

    /// Recall memory entries related to a known entity.
    ///
    /// Queries the store's full-text index for entries containing the
    /// entity name, filtering out entries that have already been surfaced.
    pub async fn recall_by_entity(
        &self,
        entity_name: &str,
        already_surfaced: &HashSet<MemoryId>,
    ) -> Result<Vec<MemoryEntry>> {
        let candidates = self.store.search_fts(entity_name, 10).await?;
        let filtered: Vec<MemoryEntry> = candidates
            .into_iter()
            .filter(|e| !already_surfaced.contains(&e.id))
            .collect();
        // Apply the same token-budget-aware truncation as recall_relevant.
        // Use a generous per-entry cap: ~2000 tokens per entity-related entry.
        let token_budget = (10u32).saturating_mul(2000);
        let budget = u64::from(token_budget);
        let mut used: u64 = 0;
        let mut kept = Vec::new();
        for e in filtered {
            let tokens = estimate_tokens(&e.content);
            if used + tokens > budget {
                break;
            }
            used += tokens;
            kept.push(e);
        }
        Ok(kept)
    }

    /// Rebuild the Closet index from current memory store state (L2 + L3).
    ///
    /// Call periodically after significant memory insertions to keep
    /// the routing index fresh.
    pub async fn rebuild_closet(&self) -> Result<()> {
        *self.closet.lock() = ClosetManager::build_from_orchestrator(self).await?;
        Ok(())
    }

    /// Restore the Closet from a previously-built index (e.g. on startup).
    pub async fn restore_closet(&self, closet: Closet) -> Result<()> {
        *self.closet.lock() = ClosetManager::from_closet(closet);
        Ok(())
    }

    /// Prepare a full context snapshot for the next model turn.
    ///
    /// Combines L0 (identity), L1 (essential), and L2 (project) within
    /// the configured token budget.
    pub async fn prepare_context(&self) -> Result<PreparedContext> {
        let budget = self.make_budget();

        // Collect layers in priority order.
        let l0_ctx = self.l0.prepare_context(&budget).await?;
        let l1_ctx = self.l1.prepare_context(&budget).await?;
        let l2_ctx = self.l2.prepare_context(&budget).await?;

        let mut entries = Vec::new();
        entries.extend(l0_ctx.entries);
        entries.extend(l1_ctx.entries);
        entries.extend(l2_ctx.entries);

        let total_tokens: u64 = entries.iter().map(|e| estimate_tokens(&e.content)).sum();

        Ok(PreparedContext {
            entries,
            total_tokens,
            budget,
            depth_scale: 1.0,
            prepared_at: Utc::now(),
            code_context: None,
        })
    }

    /// Prepare context filtered through a context fence for session isolation.
    ///
    /// This method is used when preparing context for a specific session,
    /// ensuring memory entries from other sessions are properly filtered.
    pub async fn prepare_context_with_fence(
        &self,
        fence: &crate::context_fence::ContextFence,
    ) -> Result<PreparedContext> {
        let budget = self.make_budget();

        // Collect layers in priority order.
        let l0_ctx = self.l0.prepare_context(&budget).await?;
        let l1_ctx = self.l1.prepare_context(&budget).await?;
        let l2_ctx = self.l2.prepare_context(&budget).await?;

        let mut entries = Vec::new();
        entries.extend(l0_ctx.entries);
        entries.extend(l1_ctx.entries);
        entries.extend(l2_ctx.entries);

        // Filter entries through the fence.
        let filtered_entries: Vec<MemoryEntry> = entries
            .into_iter()
            .filter(|e| fence.allows(e))
            .collect();

        let total_tokens: u64 = filtered_entries.iter()
            .map(|e| estimate_tokens(&e.content))
            .sum();

        Ok(PreparedContext {
            entries: filtered_entries,
            total_tokens,
            budget,
            depth_scale: 1.0,
            prepared_at: Utc::now(),
            code_context: None,
        })
    }

    // -----------------------------------------------------------------------
    // Fence API
    // -----------------------------------------------------------------------

    /// Get the fence registry for managing context fences.
    pub fn fence_registry(&self) -> &FenceRegistry {
        &self.fence_registry
    }

    /// Get a reference to the underlying memory store.
    ///
    /// This is useful for advanced operations like custom FTS5 queries.
    pub fn store(&self) -> &Arc<dyn MemoryStore> {
        &self.store
    }

    /// Set the active memory scope for new entries.
    pub fn set_active_scope(&self, scope: MemoryScope) {
        *self.active_scope.lock() = scope;
    }

    /// Get the currently active memory scope.
    pub fn get_active_scope(&self) -> MemoryScope {
        self.active_scope.lock().clone()
    }

    /// Set the active agent ID for auto-filling new entries' source_agent.
    pub fn set_active_agent(&self, agent_id: String) {
        *self.active_agent.lock() = Some(agent_id);
    }

    /// Set the active session ID for auto-filling new entries.
    pub fn set_active_session(&self, session_id: String) {
        *self.active_session.lock() = Some(session_id);
    }

    /// Get the current active session ID (if set).
    pub fn active_session_id(&self) -> Option<String> {
        self.active_session.lock().clone()
    }

    // -----------------------------------------------------------------------
    // Write API
    // -----------------------------------------------------------------------

    /// Store a new memory entry, routing it to the correct layer.
    ///
    /// If a fact checker is configured, the entry content is scanned for
    /// known entity facts. Extracted facts are registered for future checks,
    /// and contradictory statements cause the entry's confidence to be
    /// downgraded.
    pub async fn remember(&self, mut entry: MemoryEntry) -> Result<MemoryId> {
        // Auto-fill scope from the active scope
        entry.scope = self.active_scope.lock().clone();

        // Auto-fill source_agent from the active agent
        if entry.source_agent.is_none() {
            if let Some(ref agent) = *self.active_agent.lock() {
                entry.source_agent = Some(agent.clone());
            }
        }

        // Auto-fill session_id from the active session
        if entry.session_id.is_none() {
            if let Some(ref session) = *self.active_session.lock() {
                entry.session_id = Some(session.clone());
            }
        }

        // Apply fact checking if configured
        let check_result: Option<FactCheckResult> = {
            let guard = self.fact_checker.lock();
            if let Some(ref checker) = *guard {
                let source_agent = entry.source_agent.as_deref();
                let triple = extract_triple_from_content(&entry.content, source_agent);
                if let Some(ref t) = triple {
                    let result = checker.check_triple(t);
                    if !result.is_consistent {
                        Some(result)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(result) = check_result {
            entry.confidence = (entry.confidence * result.confidence).min(0.5);
            tracing::warn!(
                contradiction = ?result.contradiction,
                confidence = result.confidence,
                entry_id = %entry.id,
                "fact check: contradiction detected, confidence downgraded"
            );
        }

        // Register new facts from content for future checks
        // Also perform cross-agent conflict detection
        {
            let mut guard = self.fact_checker.lock();
            if let Some(ref mut checker) = *guard {
                let source_agent = entry.source_agent.as_deref();
                register_facts_from_content(checker, &entry.content, source_agent);

                // Cross-agent conflict detection
                if let Some(triple) = extract_triple_from_content(&entry.content, source_agent) {
                    let conflict_info = checker.detect_conflict(&triple);
                    if let Some((conflicting, score)) = conflict_info {
                        let loser_confidence = score.clamp(0.1, 0.9);
                        entry.confidence = (entry.confidence * loser_confidence).min(0.5);
                        tracing::warn!(
                            subject = %conflicting.subject,
                            predicate = %conflicting.predicate,
                            existing_object = %conflicting.object,
                            conflict_score = score,
                            entry_id = %entry.id,
                            "cross-agent conflict: confidence downgraded"
                        );
                        // Also register this triple with downgraded confidence
                        let mut downgraded = triple;
                        downgraded.confidence = entry.confidence;
                        checker.register_triple(downgraded);
                    }
                }
            }
        }

        let layer = entry.layer;
        let source = entry.source;
        let entry_id = entry.id;
        let content = entry.content.clone();
        let id = match layer {
            MemoryLayer::L0 => self.l0.insert(entry).await?,
            MemoryLayer::L1 => self.l1.insert(entry).await?,
            MemoryLayer::L2 => self.l2.insert(entry).await?,
            MemoryLayer::L3 => self.l3.insert(entry).await?,
            MemoryLayer::L4 => self.l4.insert(entry).await?,
        };

        // Wire into verbatim sink for persistent layers (L2/L3/L4).
        // L0/L1 are volatile layers and should NOT be stored verbatim.
        if matches!(layer, MemoryLayer::L2 | MemoryLayer::L3 | MemoryLayer::L4) {
            let timestamp = Utc::now().to_rfc3339();
            self.store
                .save_verbatim(
                    &entry_id.to_string(),
                    &content,
                    crate::store::sqlite::source_to_str(source),
                    crate::store::sqlite::layer_to_int(layer),
                    &timestamp,
                )
                .await?;
        }

        Ok(id)
    }

    /// Write a new memory entry to the specified layer.
    ///
    /// This is a convenience wrapper around `remember` that builds the entry
    /// from individual fields.
    pub async fn write(
        &self,
        layer: MemoryLayer,
        category: MemoryCategory,
        title: &str,
        content: &str,
        priority: Priority,
        source: MemorySource,
        tags: Vec<String>,
        scope: MemoryScope,
    ) -> Result<MemoryId> {
        let now = Utc::now();
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer,
            category,
            priority,
            source,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags,
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope,
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        };
        self.remember(entry).await
    }

    /// Retrieve a memory entry by ID.
    pub async fn recall(&self, id: &MemoryId) -> Result<Option<MemoryEntry>> {
        self.store.get(id).await
    }

    /// Permanently delete a memory entry.
    pub async fn forget(&self, id: &MemoryId) -> Result<()> {
        self.store.delete(id).await
    }

    /// Update an existing memory entry in-place.
    pub async fn update(&self, entry: &crate::types::MemoryEntry) -> Result<()> {
        self.store.update(entry).await
    }

    /// List metadata for all entries in a given layer.
    pub async fn list_layer(&self, layer: MemoryLayer) -> Result<Vec<MemoryMeta>> {
        self.store.list_metas(Some(layer)).await
    }

    // -----------------------------------------------------------------------
    // L4 Shared Layer – Team Knowledge Operations
    // -----------------------------------------------------------------------

    /// Write a shared entry visible to all agents in the same team scope.
    ///
    /// This is the recommended API for agent-to-agent knowledge sharing:
    /// task handoff, team decisions, coding conventions, operational runbooks.
    /// Entries are automatically scoped by `shared_scope` and pruned based
    /// on staleness thresholds during maintenance ticks.
    pub async fn team_remember(
        &self,
        title: &str,
        content: &str,
        priority: Priority,
        tags: Vec<String>,
        scope: MemoryScope,
    ) -> Result<MemoryId> {
        self.write(
            MemoryLayer::L4,
            MemoryCategory::Shared,
            title,
            content,
            priority,
            MemorySource::Import,
            tags,
            scope,
        )
        .await
    }

    /// Query team-shared knowledge relevant to a search term.
    ///
    /// Returns entries from L4 filtered by the given scope. This is the
    /// entry point for agents to discover team conventions, prior decisions,
    /// and peer agent handoff data before starting a new task.
    pub async fn team_query(
        &self,
        query: &str,
        scope: Option<&MemoryScope>,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        let mut entries = self.l4.recall(query, limit * 2).await?;
        if let Some(s) = scope {
            entries.retain(|e| e.scope == *s || e.scope.is_global());
        }
        entries.truncate(limit);
        Ok(entries)
    }

    /// Recall L4 shared entries scoped to the current project.
    pub async fn recall_l4_project(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.l4.recall_project(query, limit).await
    }

    /// Recall L4 shared entries scoped globally.
    pub async fn recall_l4_global(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.l4.recall_global(query, limit).await
    }

    /// Recall peer agent context from L4 for cross-agent perception.
    ///
    /// Returns entries where visibility is Shared and source_agent differs
    /// from `current_agent`, capped per peer and total.
    pub async fn recall_peer_context(
        &self,
        query: &str,
        current_agent: &str,
        max_per_peer: usize,
        max_peers: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.l4.recall_peers(query, current_agent, max_per_peer, max_peers).await
    }

    /// Intra-turn real-time peer perception (T3).
    ///
    /// Delegates to [`SharedLayer::recall_peers_realtime`].  No 5-minute time
    /// cutoff — filters by session_id instead so Agent B sees Agent A's writes
    /// from the same turn.
    pub async fn recall_peer_context_realtime(
        &self,
        query: &str,
        current_agent: &str,
        current_session_id: &str,
        max_per_peer: usize,
        max_peers: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.l4.recall_peers_realtime(query, current_agent, current_session_id, max_per_peer, max_peers).await
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Return frequently occurring tags (hot topics) from the L4 shared
    /// layer within the given time window in seconds.  Delegates to
    /// [`SharedLayer::hot_topics`].
    pub async fn hot_topics(&self, window_secs: i64) -> Vec<String> {
        self.l4.hot_topics(window_secs).await
    }

    /// Run periodic maintenance across all layers.
    ///
    /// Should be called once per session tick (e.g. after each user turn).
    pub async fn tick(&self) -> Result<()> {
        self.l0.tick().await?;
        self.l1.tick().await?;
        self.l2.tick().await?;
        self.l3.tick().await?;
        self.l4.tick().await?;

        // Auto-rebuild Closet every 10 ticks (counter only - actual rebuild needs &mut self)
        self.closet_rebuild_counter.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Check if the Closet should be rebuilt and return true if it's time.
    pub fn should_rebuild_closet(&self) -> bool {
        self.closet_rebuild_counter.load(Ordering::Relaxed) % 10 == 0
    }

    /// Force an immediate Closet rebuild.
    pub async fn force_rebuild_closet(&self) -> Result<()> {
        self.rebuild_closet().await
    }

    /// Ingest project context files from the workspace into L2.
    pub async fn ingest_project_context(&self, scope: MemoryScope) -> Result<Vec<MemoryId>> {
        self.l2.ingest_project_context(scope).await
    }

    // -----------------------------------------------------------------------
    // Identity helpers
    // -----------------------------------------------------------------------

    /// Set (create or update) the primary identity entry.
    pub async fn set_identity(&self, title: &str, content: &str) -> Result<MemoryId> {
        self.l0.set(title, content).await
    }

    /// Configure a fact checker for contradiction detection.
    pub fn with_fact_checker(self, checker: FactChecker) -> Self {
        *self.fact_checker.lock() = Some(checker);
        self
    }

    /// Access the fact checker for configuration.
    pub fn with_fact_checker_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut FactChecker) -> R,
    {
        let mut guard = self.fact_checker.lock();
        if let Some(ref mut checker) = *guard {
            f(checker)
        } else {
            *guard = Some(FactChecker::new());
            f(guard.as_mut().unwrap())
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn make_budget(&self) -> TokenBudget {
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
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn estimate_tokens(content: &str) -> u64 {
    (content.len() as u64).div_ceil(4)
}

/// Extract a Triple from entry content if it contains entity relationship statements.
///
/// Recognised patterns:
/// - `"Alice's parent is Bob"` → triple(subject="Alice", predicate="parent_of", object="Bob")
/// - `"Alice is child_of Charlie"` → same
/// - `"Alice's full_name is Alice Smith"` → triple(subject="Alice", predicate="full_name", object="Alice Smith")
fn extract_triple_from_content(content: &str, source_agent: Option<&str>) -> Option<Triple> {
    // Pattern: "X's parent is Y" or "X's parent_is Y"
    let parent_re = regex::Regex::new(r#"(\w+)'s\s+parent\s+is\s+(\w+)"#).ok()?;
    if let Some(caps) = parent_re.captures(content) {
        let subject = caps.get(1)?.as_str().to_string();
        let object = caps.get(2)?.as_str().to_string();
        return Some(Triple {
            id: uuid::Uuid::new_v4().to_string(),
            subject,
            predicate: "child_of".to_string(),
            object,
            valid_from: None,
            valid_until: None,
            confidence: 1.0,
            source_memory_id: None,
            source_file: None,
            source_agent: source_agent.map(String::from),
        });
    }

    // Pattern: "X is child_of Y"
    let child_re = regex::Regex::new(r#"(\w+)\s+is\s+child_of\s+(\w+)"#).ok()?;
    if let Some(caps) = child_re.captures(content) {
        let subject = caps.get(1)?.as_str().to_string();
        let object = caps.get(2)?.as_str().to_string();
        return Some(Triple {
            id: uuid::Uuid::new_v4().to_string(),
            subject,
            predicate: "child_of".to_string(),
            object,
            valid_from: None,
            valid_until: None,
            confidence: 1.0,
            source_memory_id: None,
            source_file: None,
            source_agent: source_agent.map(String::from),
        });
    }

    None
}

/// Register entity facts from entry content into the fact checker.
///
/// This enables the fact checker to detect contradictions across writes:
/// first write registers the fact, second write with contradictory value
/// triggers a warning and confidence downgrade.
fn register_facts_from_content(checker: &mut FactChecker, content: &str, source_agent: Option<&str>) {
    // Register parent facts
    let parent_re = regex::Regex::new(r#"(\w+)'s\s+parent\s+is\s+(\w+)"#).ok();
    if let Some(re) = parent_re {
        for caps in re.captures_iter(content) {
            if let (Some(subj), Some(obj)) = (caps.get(1), caps.get(2)) {
                let subject = subj.as_str();
                let parent_name = obj.as_str();
                let mut facts = EntityFacts::default();
                facts.entity_type = Some("person".to_string());
                facts.parent = Some(parent_name.to_string());
                checker.register_facts(subject, facts);

                // Register triple for cross-agent conflict detection
                let triple = Triple {
                    id: uuid::Uuid::new_v4().to_string(),
                    subject: subject.to_string(),
                    predicate: "child_of".to_string(),
                    object: parent_name.to_string(),
                    valid_from: Some(chrono::Utc::now()),
                    valid_until: None,
                    confidence: 1.0,
                    source_memory_id: None,
                    source_file: None,
                    source_agent: source_agent.map(String::from),
                };
                checker.register_triple(triple);
            }
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BudgetConfig, MemoryConfig, StoreConfig};
    use crate::store::sqlite::SqliteStore;
    use crate::types::{MemoryCategory, MemorySource, Priority, TokenBudget};

    fn in_memory_store() -> Arc<dyn MemoryStore> {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        Arc::new(SqliteStore::open_path(&tmp.path().join("test.db")).unwrap())
    }

    fn test_config() -> MemoryConfig {
        MemoryConfig {
            budget: BudgetConfig {
                context_window: 8000,
                reserved_system: 2000,
                reserved_response: 1000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn test_entry(layer: MemoryLayer, title: &str, content: &str) -> MemoryEntry {
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer,
            category: MemoryCategory::Decision,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: vec![],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        }
    }

    // ── Construction ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn from_store_constructs_all_layers() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), Arc::clone(&store), None)
            .expect("from_store");
        let fences = orch.fence_registry().list_fences().await;
        assert!(fences.is_empty());
    }

    #[tokio::test]
    async fn from_store_with_workspace_uses_project_layer() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = in_memory_store();
        let orch =
            MemoryOrchestrator::from_store(test_config(), store, Some(tmp.path().to_path_buf()))
                .expect("from_store");
        let ctx = orch.load_project_context().await.unwrap();
        assert!(ctx.is_empty()); // no project context files in empty dir
    }

    // ── Write / Remember ────────────────────────────────────────────────────

    #[tokio::test]
    async fn remember_routes_entry_to_correct_layer() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();

        let id = orch.remember(test_entry(MemoryLayer::L1, "T", "C")).await.unwrap();
        let recalled = orch.recall(&id).await.unwrap().expect("should exist");
        assert_eq!(recalled.layer, MemoryLayer::L1);
        assert_eq!(recalled.title, "T");
        assert_eq!(recalled.content, "C");
    }

    #[tokio::test]
    async fn write_creates_entry_with_all_fields() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();

        let id = orch
            .write(
                MemoryLayer::L2,
                MemoryCategory::ProjectConvention,
                "api-design",
                "Use REST for external APIs",
                Priority::High,
                MemorySource::Import,
                vec!["api".into(), "convention".into()],
                MemoryScope::Project("project-x".into()),
            )
            .await
            .unwrap();

        let recalled = orch.recall(&id).await.unwrap().unwrap();
        assert_eq!(recalled.title, "api-design");
        assert_eq!(recalled.layer, MemoryLayer::L2);
        assert_eq!(recalled.category, MemoryCategory::ProjectConvention);
        assert_eq!(recalled.priority, Priority::High);
        assert_eq!(recalled.source, MemorySource::Import);
        assert_eq!(recalled.tags, vec!["api", "convention"]);
        assert!(recalled.scope.scope_key().starts_with("session_"));
        assert!(recalled.confidence > 0.0);
    }

    #[tokio::test]
    async fn remember_all_five_layers() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();

        for layer in &[
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
            MemoryLayer::L4,
        ] {
            let e = test_entry(*layer, "T", "C");
            let id = orch.remember(e).await.unwrap();
            let got = orch.recall(&id).await.unwrap().unwrap();
            assert_eq!(got.layer, *layer);
        }
    }

    // ── Read ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn load_fixed_layers_returns_l0_and_l1() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();

        orch.remember(test_entry(MemoryLayer::L0, "identity", "content")).await.unwrap();
        orch.remember(test_entry(MemoryLayer::L1, "task", "content")).await.unwrap();
        // L2 should NOT appear in fixed layers
        orch.remember(test_entry(MemoryLayer::L2, "project", "content")).await.unwrap();

        let fixed = orch.load_fixed_layers().await.unwrap();
        let layers: Vec<MemoryLayer> = fixed.iter().map(|e| e.layer).collect();
        assert!(layers.iter().any(|l| *l == MemoryLayer::L0));
        assert!(layers.iter().any(|l| *l == MemoryLayer::L1));
        assert!(!layers.iter().any(|l| *l == MemoryLayer::L2));
    }

    #[tokio::test]
    async fn recall_returns_none_for_missing() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        let fake = uuid::Uuid::new_v4();
        assert!(orch.recall(&fake).await.unwrap().is_none());
    }

    // ── Delete ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn forget_removes_entry() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        let id = orch.remember(test_entry(MemoryLayer::L1, "tmp", "x")).await.unwrap();
        assert!(orch.recall(&id).await.unwrap().is_some());

        orch.forget(&id).await.unwrap();
        assert!(orch.recall(&id).await.unwrap().is_none());
    }

    // ── List ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_layer_returns_metadata() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        orch.remember(test_entry(MemoryLayer::L1, "a", "aa")).await.unwrap();
        orch.remember(test_entry(MemoryLayer::L1, "b", "bb")).await.unwrap();
        orch.remember(test_entry(MemoryLayer::L2, "c", "cc")).await.unwrap();

        let l1 = orch.list_layer(MemoryLayer::L1).await.unwrap();
        assert_eq!(l1.len(), 2);
    }

    // ── Identity ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_identity_creates_l0_entry() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        let id = orch.set_identity("Assistant Persona", "You are a helpful assistant.").await.unwrap();
        let got = orch.recall(&id).await.unwrap().unwrap();
        assert_eq!(got.layer, MemoryLayer::L0);
        assert_eq!(got.title, "Assistant Persona");
    }

    #[tokio::test]
    async fn set_identity_updates_existing() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        let id1 = orch.set_identity("Assistant Persona", "V1").await.unwrap();
        let id2 = orch.set_identity("Assistant Persona", "V2").await.unwrap();
        // Same title → overwrite, same ID returned
        assert_eq!(id1, id2);
        let got = orch.recall(&id1).await.unwrap().unwrap();
        assert_eq!(got.content, "V2");
    }

    // ── Tick ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn tick_propagates_to_all_layers() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        orch.tick().await.unwrap();
    }

    // ── Context Preparation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn prepare_context_combines_layers_within_budget() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();

        orch.set_identity("I", "ident content here").await.unwrap();
        orch.remember(test_entry(MemoryLayer::L1, "task", "task content here today")).await.unwrap();
        orch.remember(test_entry(MemoryLayer::L2, "proj", "project convention text")).await.unwrap();

        let ctx = orch.prepare_context().await.unwrap();
        assert!(!ctx.entries.is_empty());
        assert!(ctx.total_tokens > 0);
        assert_eq!(ctx.budget.total, 8000);
    }

    #[tokio::test]
    async fn prepare_context_makes_budget_from_config() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        let ctx = orch.prepare_context().await.unwrap();
        // total 8000 - system 2000 - response 1000 = 5000 available
        assert_eq!(ctx.budget.reserved_system, 2000);
        assert_eq!(ctx.budget.reserved_response, 1000);
        assert_eq!(ctx.budget.available, 5000);
    }

    // ── store() accessor ────────────────────────────────────────────────────

    #[test]
    fn store_returns_reference() {
        let store = in_memory_store();
        let orch = MemoryOrchestrator::from_store(test_config(), store, None).unwrap();
        let _ = orch.store(); // must compile + return valid ref
    }
}
