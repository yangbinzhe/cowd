//! L4 – Shared / team-scoped layer.
//!
//! Entries here are visible to all agents within a team or organisation.
//! Typical content: shared conventions, team decisions, on-call runbooks.
//!
//! Characteristics:
//! - Optional: if `enabled` is false all operations are no-ops / empty
//! - Entries are scoped with a shared scope key (e.g. team name)
//! - Sync mechanism: re-reads entries from a shared store path on demand

use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    config::DriftConfig,
    layers::{LayerManager, Result},
    store::MemoryStore,
    types::{
        MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, PreparedContext,
        Priority, TokenBudget,
    },
};

/// Default maximum token budget for the shared layer.
const DEFAULT_MAX_TOKENS: u64 = 2000;

/// Manager for the L4 shared team layer.
pub struct SharedLayer {
    store: Arc<dyn MemoryStore>,
    enabled: bool,
    max_tokens: u64,
    /// Scope key used to tag all entries written by this layer.
    shared_scope: Option<String>,
    drift: DriftConfig,
}

impl SharedLayer {
    /// Create a disabled shared layer (all operations are no-ops).
    #[must_use] 
    pub fn disabled() -> Self {
        Self {
            store: Arc::new(NoopStore),
            enabled: false,
            max_tokens: DEFAULT_MAX_TOKENS,
            shared_scope: None,
            drift: DriftConfig::default(),
        }
    }

    /// Create an enabled shared layer backed by the given store.
    pub fn new(store: Arc<dyn MemoryStore>) -> Self {
        Self {
            store,
            enabled: true,
            max_tokens: DEFAULT_MAX_TOKENS,
            shared_scope: None,
            drift: DriftConfig::default(),
        }
    }

    /// Create with all options.
    pub fn with_config(
        store: Arc<dyn MemoryStore>,
        enabled: bool,
        shared_scope: Option<String>,
        max_tokens: u64,
        drift: DriftConfig,
    ) -> Self {
        Self {
            store,
            enabled,
            max_tokens,
            shared_scope,
            drift,
        }
    }

    /// Whether this layer is active.
    #[must_use] 
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Load all L4 entries within the token budget.
    pub async fn load(&self) -> Result<Vec<MemoryEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let entries = self.store.search_by_layer(MemoryLayer::L4).await?;
        Ok(truncate_to_budget(entries, self.max_tokens))
    }

    /// Add a shared memory entry.
    pub async fn add(
        &self,
        category: MemoryCategory,
        title: &str,
        content: &str,
        priority: Priority,
        source: MemorySource,
        tags: Vec<String>,
    ) -> Result<MemoryId> {
        if !self.enabled {
            return Ok(Uuid::nil());
        }
        let now = Utc::now();
        let entry = MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L4,
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
            scope: self.shared_scope.clone(),
            session_id: None,
        };
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    /// Recall shared entries relevant to a query via FTS.
    pub async fn recall(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let results = self.store.search_fts(query, limit * 2).await?;
        let filtered: Vec<MemoryEntry> = results
            .into_iter()
            .filter(|e| e.layer == MemoryLayer::L4)
            .take(limit)
            .collect();
        Ok(filtered)
    }

    /// Sync: re-read L4 entries from the store and update staleness.
    ///
    /// In a real multi-agent system this would pull from a remote shared store.
    /// Here it simply refreshes staleness scores for all L4 entries.
    pub async fn sync(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        // Apply a small staleness reduction on sync (freshness signal).
        let entries = self.store.search_by_layer(MemoryLayer::L4).await?;
        for mut entry in entries {
            entry.staleness = (entry.staleness - 0.01_f32).max(0.0);
            self.store.update(&entry).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// LayerManager implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LayerManager for SharedLayer {
    fn layer(&self) -> MemoryLayer {
        MemoryLayer::L4
    }

    async fn insert(&self, mut entry: MemoryEntry) -> Result<MemoryId> {
        if !self.enabled {
            return Ok(Uuid::nil());
        }
        entry.layer = MemoryLayer::L4;
        if entry.scope.is_none() {
            entry.scope = self.shared_scope.clone();
        }
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    async fn remove(&self, id: &MemoryId) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        self.store.delete(id).await
    }

    async fn prepare_context(&self, budget: &TokenBudget) -> Result<PreparedContext> {
        if !self.enabled {
            return Ok(PreparedContext {
                entries: Vec::new(),
                total_tokens: 0,
                budget: budget.clone(),
                depth_scale: 0.4,
                prepared_at: Utc::now(),
            });
        }

        let available = budget.available.min(self.max_tokens);
        let entries = self.store.search_by_layer(MemoryLayer::L4).await?;
        let kept = truncate_to_budget(entries, available);
        let used_tokens: u64 = kept.iter().map(|e| estimate_tokens(&e.content)).sum();

        Ok(PreparedContext {
            entries: kept,
            total_tokens: used_tokens,
            budget: budget.clone(),
            depth_scale: 0.4,
            prepared_at: Utc::now(),
        })
    }

    async fn tick(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let entries = self.store.search_by_layer(MemoryLayer::L4).await?;
        let decay = self.drift.staleness_decay_per_day;

        for mut entry in entries {
            entry.staleness = (entry.staleness + decay).min(1.0);

            if entry.staleness >= self.drift.prune_threshold {
                self.store.delete(&entry.id).await?;
                continue;
            }
            self.store.update(&entry).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn truncate_to_budget(mut entries: Vec<MemoryEntry>, max_tokens: u64) -> Vec<MemoryEntry> {
    entries.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then(b.updated_at.cmp(&a.updated_at))
    });

    let mut used: u64 = 0;
    let mut kept = Vec::new();
    for e in entries {
        let tokens = estimate_tokens(&e.content);
        if used + tokens > max_tokens {
            break;
        }
        used += tokens;
        kept.push(e);
    }
    kept
}

fn estimate_tokens(content: &str) -> u64 {
    (content.len() as u64).div_ceil(4)
}

// ---------------------------------------------------------------------------
// NoopStore – used when SharedLayer is disabled
// ---------------------------------------------------------------------------

use crate::types::MemoryMeta;

/// A no-op store used when the `SharedLayer` is disabled.
struct NoopStore;

#[async_trait]
impl crate::store::MemoryStore for NoopStore {
    async fn insert(&self, entry: &MemoryEntry) -> crate::store::Result<MemoryId> {
        Ok(entry.id)
    }
    async fn get(&self, _id: &MemoryId) -> crate::store::Result<Option<MemoryEntry>> {
        Ok(None)
    }
    async fn update(&self, _entry: &MemoryEntry) -> crate::store::Result<()> {
        Ok(())
    }
    async fn delete(&self, _id: &MemoryId) -> crate::store::Result<()> {
        Ok(())
    }
    async fn search_fts(&self, _query: &str, _limit: usize) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn search_fts_advanced(
        &self,
        _query: &str,
        _options: crate::store::FtsSearchOptions,
        _limit: usize,
    ) -> crate::store::Result<crate::store::FtsSearchResult> {
        Ok(crate::store::FtsSearchResult {
            entries: Vec::new(),
            snippets: Vec::new(),
            total_matches: 0,
            keywords: Vec::new(),
        })
    }
    async fn search_vector(&self, _embedding: &[f32], _limit: usize) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn search_by_layer(&self, _layer: MemoryLayer) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn search_by_category(&self, _category: MemoryCategory) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn get_meta(&self, _id: &MemoryId) -> crate::store::Result<Option<MemoryMeta>> {
        Ok(None)
    }
    async fn list_metas(&self, _layer: Option<MemoryLayer>) -> crate::store::Result<Vec<MemoryMeta>> {
        Ok(Vec::new())
    }
    async fn list_all(&self) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
}
