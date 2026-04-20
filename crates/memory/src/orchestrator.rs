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

        Ok(Self {
            store,
            config,
            l0,
            l1,
            l2,
            l3,
            l4,
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

    /// Recall relevant memories on-demand (L3 + L4), filtered by token budget.
    ///
    /// Entries whose IDs appear in `already_surfaced` are excluded to avoid
    /// duplicating content already shown to the model.
    pub async fn recall_relevant(
        &self,
        query: &str,
        embedding: Option<&[f32]>,
        already_surfaced: &HashSet<MemoryId>,
        token_budget: u32,
    ) -> Result<Vec<MemoryEntry>> {
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
