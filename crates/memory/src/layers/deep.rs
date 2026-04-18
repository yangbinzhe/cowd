//! L3 – Deep long-term knowledge layer.
//!
//! Accumulates distilled knowledge from previous sessions: learned patterns,
//! resolved problems, and curated reference material.  Entries survive
//! indefinitely but are subject to staleness-based pruning.
//!
//! Characteristics:
//! - Dynamic retrieval: entries are loaded on-demand via FTS + vector search
//! - No fixed token budget; the orchestrator controls how many are surfaced
//! - `tick()` applies staleness decay and prunes entries above `prune_threshold`

use async_trait::async_trait;
use chrono::Utc;
use std::{
    collections::HashSet,
    sync::Arc,
};
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

/// Default number of results returned by a recall query.
const DEFAULT_SEARCH_LIMIT: usize = 5;

/// Manager for the L3 deep knowledge layer.
pub struct DeepLayer {
    store: Arc<dyn MemoryStore>,
    search_limit: usize,
    drift: DriftConfig,
}

impl DeepLayer {
    /// Create with default search limit.
    pub fn new(store: Arc<dyn MemoryStore>) -> Self {
        Self {
            store,
            search_limit: DEFAULT_SEARCH_LIMIT,
            drift: DriftConfig::default(),
        }
    }

    /// Create with custom search limit and drift configuration.
    pub fn with_config(
        store: Arc<dyn MemoryStore>,
        search_limit: usize,
        drift: DriftConfig,
    ) -> Self {
        Self {
            store,
            search_limit,
            drift,
        }
    }

    /// On-demand recall: combines FTS search with optional vector search.
    ///
    /// Results from both searches are merged and de-duplicated.  Entries whose
    /// IDs appear in `already_surfaced` are excluded.
    pub async fn recall(
        &self,
        query: &str,
        embedding: Option<&[f32]>,
        already_surfaced: &HashSet<MemoryId>,
    ) -> Result<Vec<MemoryEntry>> {
        // 1. Full-text search.
        let fts_results = self
            .store
            .search_fts(query, self.search_limit * 2)
            .await
            .unwrap_or_default();

        // 2. Optional vector search.
        let vec_results = if let Some(emb) = embedding {
            self.store
                .search_vector(emb, self.search_limit * 2)
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // 3. Merge, restrict to L3, de-duplicate.
        let mut seen: HashSet<MemoryId> = already_surfaced.clone();
        let mut merged: Vec<MemoryEntry> = Vec::new();

        for entry in fts_results.into_iter().chain(vec_results) {
            if entry.layer != MemoryLayer::L3 {
                continue;
            }
            if seen.contains(&entry.id) {
                continue;
            }
            seen.insert(entry.id);
            merged.push(entry);
        }

        // 4. Sort by confidence (desc) then priority.
        merged.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.priority.cmp(&b.priority))
        });

        merged.truncate(self.search_limit);
        Ok(merged)
    }

    /// Store a new deep-knowledge entry.
    pub async fn store_entry(
        &self,
        title: &str,
        content: &str,
        source: MemorySource,
        tags: Vec<String>,
        scope: Option<String>,
    ) -> Result<MemoryId> {
        let now = Utc::now();
        let entry = MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::CompressedSummary,
            priority: Priority::Normal,
            source,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags,
            relations: vec![],
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope,
            session_id: None,
        };
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    /// Store a deep-knowledge entry with a specific category and priority.
    pub async fn store_entry_full(
        &self,
        title: &str,
        content: &str,
        category: MemoryCategory,
        priority: Priority,
        source: MemorySource,
        tags: Vec<String>,
        scope: Option<String>,
    ) -> Result<MemoryId> {
        let now = Utc::now();
        let entry = MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L3,
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
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }
}

// ---------------------------------------------------------------------------
// LayerManager implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LayerManager for DeepLayer {
    fn layer(&self) -> MemoryLayer {
        MemoryLayer::L3
    }

    /// Insert an entry, overriding its layer to L3.
    async fn insert(&self, mut entry: MemoryEntry) -> Result<MemoryId> {
        entry.layer = MemoryLayer::L3;
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    async fn remove(&self, id: &MemoryId) -> Result<()> {
        self.store.delete(id).await
    }

    /// Deep layer does not prepare a fixed context block.
    ///
    /// Use [`recall`] instead for on-demand retrieval.  This method returns an
    /// empty context to satisfy the trait contract.
    async fn prepare_context(&self, budget: &TokenBudget) -> Result<PreparedContext> {
        Ok(PreparedContext {
            entries: Vec::new(),
            total_tokens: 0,
            budget: budget.clone(),
            depth_scale: 0.5,
            prepared_at: Utc::now(),
        })
    }

    /// Apply staleness decay and prune entries above the prune threshold.
    async fn tick(&self) -> Result<()> {
        let entries = self.store.search_by_layer(MemoryLayer::L3).await?;
        let decay = self.drift.staleness_decay_per_day;

        for mut entry in entries {
            entry.staleness = (entry.staleness + decay).min(1.0);

            // Prune entries above the hard threshold.
            if entry.staleness >= self.drift.prune_threshold {
                self.store.delete(&entry.id).await?;
                continue;
            }

            self.store.update(&entry).await?;
        }
        Ok(())
    }
}
