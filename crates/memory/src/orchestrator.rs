//! `MemoryOrchestrator` – top-level coordinator for the memory system.
//!
//! Owns all layer managers, the store, the compression pipeline, and the
//! background extractor.  External callers interact with the memory system
//! exclusively through this type.

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::Arc,
};

use chrono::Utc;

use crate::{
    config::MemoryConfig,
    context_fence::FenceRegistry,
    closet::{ClosetManager, Closet},
    error::MemoryError,
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
    closet: ClosetManager,
    /// Fence registry for context isolation.
    fence_registry: FenceRegistry,
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
        let closet = ClosetManager::from_closet(Closet::default());

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
        let closet_topics = self.closet.search_topics(query);
        let drawer_ids: HashSet<String> = closet_topics
            .iter()
            .flat_map(|ptr| ptr.drawer_ids.iter().cloned())
            .collect();

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

    /// Rebuild the Closet index from current memory store state (L2 + L3).
    ///
    /// Call periodically after significant memory insertions to keep
    /// the routing index fresh.
    pub async fn rebuild_closet(&mut self) -> Result<()> {
        self.closet = ClosetManager::build_from_orchestrator(self).await?;
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

    // -----------------------------------------------------------------------
    // Write API
    // -----------------------------------------------------------------------

    /// Store a new memory entry, routing it to the correct layer.
    pub async fn remember(&self, entry: MemoryEntry) -> Result<MemoryId> {
        let id = match entry.layer {
            MemoryLayer::L0 => self.l0.insert(entry).await?,
            MemoryLayer::L1 => self.l1.insert(entry).await?,
            MemoryLayer::L2 => self.l2.insert(entry).await?,
            MemoryLayer::L3 => self.l3.insert(entry).await?,
            MemoryLayer::L4 => self.l4.insert(entry).await?,
        };
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
        scope: Option<String>,
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
        scope: Option<String>,
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
        scope: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        let mut entries = self.l4.recall(query, limit * 2).await?;
        if let Some(s) = scope {
            entries.retain(|e| e.scope.as_deref() == Some(s));
        }
        entries.truncate(limit);
        Ok(entries)
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Run periodic maintenance across all layers.
    ///
    /// Should be called once per session tick (e.g. after each user turn).
    pub async fn tick(&self) -> Result<()> {
        self.l0.tick().await?;
        self.l1.tick().await?;
        self.l2.tick().await?;
        self.l3.tick().await?;
        self.l4.tick().await?;
        Ok(())
    }

    /// Ingest project context files from the workspace into L2.
    pub async fn ingest_project_context(&self, scope: Option<String>) -> Result<Vec<MemoryId>> {
        self.l2.ingest_project_context(scope).await
    }

    // -----------------------------------------------------------------------
    // Identity helpers
    // -----------------------------------------------------------------------

    /// Set (create or update) the primary identity entry.
    pub async fn set_identity(&self, title: &str, content: &str) -> Result<MemoryId> {
        self.l0.set(title, content).await
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
            scope: None,
            session_id: None,
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
                Some("project-x".into()),
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
        assert_eq!(recalled.scope.as_deref(), Some("project-x"));
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
