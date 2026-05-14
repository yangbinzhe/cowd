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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DriftConfig;
    use crate::store::sqlite::SqliteStore;

    fn in_memory() -> Arc<dyn MemoryStore> {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        Arc::new(SqliteStore::open_path(&tmp.path().join("test.db")).unwrap())
    }

    #[tokio::test]
    async fn store_entry_creates_l3_entry() {
        let layer = DeepLayer::new(in_memory());
        let id = layer
            .store_entry("Rust patterns", "Use Result for errors", MemorySource::Compression, vec!["rust".into()], None)
            .await
            .unwrap();

        let all = layer.store.search_by_layer(MemoryLayer::L3).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, id);
        assert_eq!(all[0].layer, MemoryLayer::L3);
        assert_eq!(all[0].category, MemoryCategory::CompressedSummary);
        assert_eq!(all[0].confidence, 0.9);
    }

    #[tokio::test]
    async fn insert_overrides_layer_to_l3() {
        let layer = DeepLayer::new(in_memory());
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(), layer: MemoryLayer::L0, category: MemoryCategory::Decision,
            priority: Priority::Normal, source: MemorySource::AutoExtracted,
            title: "t".into(), content: "c".into(), embedding: None,
            tags: vec![], relations: vec![], confidence: 1.0, access_count: 0,
            staleness: 0.0, created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            last_accessed_at: None, scope: None, session_id: None,
        };
        let id = layer.insert(entry).await.unwrap();
        let all = layer.store.search_by_layer(MemoryLayer::L3).await.unwrap();
        let got = all.iter().find(|e| e.id == id).unwrap();
        assert_eq!(got.layer, MemoryLayer::L3);
    }

    #[tokio::test]
    async fn remove_deletes_entry() {
        let layer = DeepLayer::new(in_memory());
        let id = layer.store_entry("T", "C", MemorySource::Compression, vec![], None).await.unwrap();
        assert_eq!(layer.store.search_by_layer(MemoryLayer::L3).await.unwrap().len(), 1);
        layer.remove(&id).await.unwrap();
        assert_eq!(layer.store.search_by_layer(MemoryLayer::L3).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn recall_excludes_already_surfaced() {
        let layer = DeepLayer::new(in_memory());
        let id = layer.store_entry("T", "C", MemorySource::Compression, vec![], None).await.unwrap();

        let mut surf = std::collections::HashSet::new();
        surf.insert(id);
        let results = layer.recall("T", None, &surf).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn recall_respects_search_limit() {
        let layer = DeepLayer::with_config(in_memory(), 2, DriftConfig::default());
        layer.store_entry("A", "aaa", MemorySource::Compression, vec![], None).await.unwrap();
        layer.store_entry("B", "bbb", MemorySource::Compression, vec![], None).await.unwrap();
        layer.store_entry("C", "ccc", MemorySource::Compression, vec![], None).await.unwrap();

        let surf = std::collections::HashSet::new();
        let results = layer.recall("aaa", None, &surf).await.unwrap();
        assert!(results.len() <= 2);
    }

    #[tokio::test]
    async fn recall_filters_non_l3_entries() {
        let layer = DeepLayer::new(in_memory());
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(), layer: MemoryLayer::L1, category: MemoryCategory::Decision,
            priority: Priority::Normal, source: MemorySource::AutoExtracted,
            title: "L1 entry".into(), content: "some L1 content".into(), embedding: None,
            tags: vec![], relations: vec![], confidence: 1.0, access_count: 0,
            staleness: 0.0, created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            last_accessed_at: None, scope: None, session_id: None,
        };
        layer.insert(entry).await.unwrap();

        let surf = std::collections::HashSet::new();
        let results = layer.recall("L1", None, &surf).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn tick_prunes_stale_entries() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.9,
            prune_threshold: 0.5,
            ..Default::default()
        };
        let layer = DeepLayer::with_config(in_memory(), 5, drift);
        layer.store_entry("T", "C", MemorySource::Compression, vec![], None).await.unwrap();
        layer.tick().await.unwrap();
        assert_eq!(layer.store.search_by_layer(MemoryLayer::L3).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn prepare_context_returns_empty() {
        let layer = DeepLayer::new(in_memory());
        layer.store_entry("T", "C", MemorySource::Compression, vec![], None).await.unwrap();
        let budget = TokenBudget { total: 1000, reserved_system: 0, reserved_response: 0, allocated_memory: 0, allocated_conversation: 0, available: 1000 };
        let ctx = layer.prepare_context(&budget).await.unwrap();
        assert!(ctx.entries.is_empty());
    }

    #[test]
    fn layer_returns_l3() {
        assert_eq!(DeepLayer::new(in_memory()).layer(), MemoryLayer::L3);
    }
}
