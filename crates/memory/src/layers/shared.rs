//! L4 – governed durable Team knowledge.
//!
//! # Role in Multi-Agent Architecture
//!
//! L4 is not a live cross-agent message bus.  ExecutionGraph and
//! TeamWorkingState carry current-run collaboration; L4 contains only
//! evidence-backed, promoted long-term knowledge from completed governance.
//!
//! ## Key Use Cases
//!
//! - **Team conventions**: coding standards, review checklists, naming conventions
//! - **Shared decisions**: architectural decisions, API contracts, design tradeoffs
//! - **Handoffs/checkpoints**: governed durable context for later runs
//! - **Runbooks**: operational knowledge, on-call procedures, troubleshooting guides
//! - Never task progress, raw tool output, worker assignment, or live peer
//!   messages; those belong to Runtime projections.
//!
//! ## Scope Isolation
//!
//! Each entry is tagged with an optional `shared_scope` key (e.g. team name,
//! organisation ID). Agents in different scopes are isolated from each other's
//! shared memory. This enables multi-tenant deployments where Team A and Team B
//! share the same Cowd instance but have separate knowledge spaces.
//!
//! ## Characteristics
//!
//! - **Optional by default**: disabled in single-user mode; auto-enabled when
//!   team mode is activated via configuration.
//! - **Scope-keyed isolation**: entries are tagged with a team/organisation scope.
//! - **Sync mechanism**: periodic re-read from shared store ensures freshness
//!   across concurrent agent sessions.
//! - **Governed lifecycle**: `tick()` only refreshes wall-clock staleness;
//!   evidence is archived or superseded by governance rather than deleted.

use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;

use crate::{
    config::DriftConfig,
    layers::{wall_clock_staleness, LayerManager, Result},
    store::MemoryStore,
    types::{MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, PreparedContext, TokenBudget},
    MemoryScope,
};

/// Default maximum token budget for the shared layer.
const DEFAULT_MAX_TOKENS: u64 = 2000;

// ---------------------------------------------------------------------------
// SharedLayer
// ---------------------------------------------------------------------------

/// Manager for the L4 shared team layer.
pub struct SharedLayer {
    store: Arc<dyn MemoryStore>,
    enabled: bool,
    max_tokens: u64,
    /// Scope key used to tag all entries written by this layer.
    shared_scope: Option<MemoryScope>,
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
        shared_scope: Option<MemoryScope>,
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

    /// Recall L4 entries scoped to a Project scope via FTS.
    pub async fn recall_project(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let scope = self.shared_scope.clone().unwrap_or_default();
        let results = self
            .store
            .search_fts_scoped(query, &scope, limit * 2)
            .await?;
        let filtered: Vec<MemoryEntry> = results
            .into_iter()
            .filter(|e| e.layer == MemoryLayer::L4)
            .take(limit)
            .collect();
        Ok(filtered)
    }

    /// Recall L4 entries scoped to the Global scope via FTS.
    pub async fn recall_global(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let results = self
            .store
            .search_fts_scoped(query, &MemoryScope::Global, limit * 2)
            .await?;
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

    /// Return frequently occurring tags within a time window.
    ///
    /// Scans all L4 entries created within `window_secs` from now, counts tag
    /// frequencies, and returns tags ordered by frequency (descending).
    pub async fn hot_topics(&self, window_secs: i64) -> Vec<String> {
        if !self.enabled {
            return Vec::new();
        }
        let cutoff = Utc::now() - chrono::Duration::seconds(window_secs);
        let entries = self
            .store
            .search_by_layer(MemoryLayer::L4)
            .await
            .unwrap_or_default();

        let mut tag_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for entry in &entries {
            if entry.created_at >= cutoff {
                for tag in &entry.tags {
                    *tag_counts.entry(tag.clone()).or_insert(0) += 1;
                }
            }
        }

        let mut sorted: Vec<(String, usize)> = tag_counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        sorted.into_iter().map(|(tag, _)| tag).collect()
    }

    /// Internal writer used only by `MemoryOrchestrator::promote_l4` after
    /// Runtime has validated a typed promotion command.  This is deliberately
    /// not exposed through the public `LayerManager::insert` contract.
    pub(crate) async fn insert_promoted(&self, mut entry: MemoryEntry) -> Result<MemoryId> {
        if !self.enabled {
            return Err(crate::MemoryError::WriteDenied {
                layer: "L4".to_string(),
                write_source: "disabled_l4_promotion_target".to_string(),
            });
        }
        entry.layer = MemoryLayer::L4;
        if let Some(ref scope) = self.shared_scope {
            entry.scope = scope.clone();
        }
        self.store.insert(&entry).await
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

    async fn insert(&self, _entry: MemoryEntry) -> Result<MemoryId> {
        Err(crate::MemoryError::WriteDenied {
            layer: "L4".to_string(),
            write_source: "layer_manager_insert_requires_runtime_l4_promotion_service".to_string(),
        })
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
                code_context: None,
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
            code_context: None,
        })
    }

    async fn tick(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let entries = self.store.search_by_layer(MemoryLayer::L4).await?;
        let decay = self.drift.staleness_decay_per_day;

        for mut entry in entries {
            let next_staleness = wall_clock_staleness(&entry, decay);
            if (entry.staleness - next_staleness).abs() > f32::EPSILON {
                entry.staleness = next_staleness;
                self.store.update(&entry).await?;
            }
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

use crate::entity::{Entity, Triple};
use crate::types::MemoryMeta;

/// A no-op store used when the `SharedLayer` is disabled.
struct NoopStore;

impl NoopStore {
    fn unavailable<T>() -> crate::store::Result<T> {
        Err(crate::MemoryError::CapabilityUnavailable {
            capability: "shared_memory_store".to_string(),
            details: "shared layer is disabled".to_string(),
        })
    }
}

#[async_trait]
impl crate::store::MemoryStore for NoopStore {
    fn capabilities(&self) -> crate::store::MemoryStoreCapabilities {
        crate::store::MemoryStoreCapabilities {
            backend: "disabled",
            full_text_search: false,
            lexical_fallback: false,
            vector_search: false,
            code_index: false,
        }
    }
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
    async fn search_fts(
        &self,
        _query: &str,
        _limit: usize,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Self::unavailable()
    }
    async fn search_fts_scoped(
        &self,
        _query: &str,
        _scope: &crate::project_scope::MemoryScope,
        _limit: usize,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Self::unavailable()
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
    async fn search_vector(
        &self,
        _embedding: &[f32],
        _limit: usize,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Self::unavailable()
    }
    async fn search_by_layer(&self, _layer: MemoryLayer) -> crate::store::Result<Vec<MemoryEntry>> {
        Self::unavailable()
    }
    async fn search_by_category(
        &self,
        _category: MemoryCategory,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn lookup_authority_candidates(
        &self,
        _query: crate::store::AuthorityLookup,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn lookup_tagged_candidates(
        &self,
        _query: crate::store::TaggedLookup,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn lookup_fact_candidates(
        &self,
        _scope: &crate::project_scope::MemoryScope,
        _category: MemoryCategory,
        _limit: usize,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn search_semantic_checkpoints(
        &self,
        _scope: &crate::project_scope::MemoryScope,
        _query: &str,
        _limit: usize,
    ) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn scan_entries_page(
        &self,
        _cursor: crate::store::MemoryScanCursor,
        _limit: usize,
    ) -> crate::store::Result<crate::store::MemoryScanPage> {
        Ok(crate::store::MemoryScanPage {
            entries: Vec::new(),
            next: None,
        })
    }
    async fn aggregate(
        &self,
        _stale_threshold: f32,
    ) -> crate::store::Result<crate::store::MemoryStoreAggregate> {
        Ok(crate::store::MemoryStoreAggregate::default())
    }
    async fn get_meta(&self, _id: &MemoryId) -> crate::store::Result<Option<MemoryMeta>> {
        Ok(None)
    }
    async fn list_metas(
        &self,
        _layer: Option<MemoryLayer>,
    ) -> crate::store::Result<Vec<MemoryMeta>> {
        Ok(Vec::new())
    }
    async fn list_all(&self) -> crate::store::Result<Vec<MemoryEntry>> {
        Ok(Vec::new())
    }
    async fn kv_get_many(
        &self,
        _keys: &[String],
    ) -> crate::store::Result<Vec<crate::store::MemoryKeyValue>> {
        Ok(Vec::new())
    }

    async fn legacy_scope_migration_reports(
        &self,
    ) -> crate::store::Result<Vec<crate::store::sqlite::LegacyScopeMigrationReport>> {
        Self::unavailable()
    }

    async fn save_entities(&self, _entities: &[Entity]) -> crate::store::Result<()> {
        Ok(())
    }

    async fn load_entities(&self) -> crate::store::Result<Vec<Entity>> {
        Ok(Vec::new())
    }

    async fn save_triples(&self, _triples: &[Triple]) -> crate::store::Result<()> {
        Ok(())
    }

    async fn load_triples(&self) -> crate::store::Result<Vec<Triple>> {
        Ok(Vec::new())
    }

    async fn save_verbatim(
        &self,
        _id: &str,
        _content: &str,
        _source: &str,
        _layer: i32,
        _timestamp: &str,
    ) -> crate::store::Result<()> {
        Ok(())
    }

    async fn load_verbatim_by_id(
        &self,
        _id: &str,
    ) -> crate::store::Result<Option<crate::store::VerbatimEntry>> {
        Ok(None)
    }

    async fn search_verbatim_by_content(
        &self,
        _query: &str,
    ) -> crate::store::Result<Vec<crate::store::VerbatimEntry>> {
        Ok(Vec::new())
    }
    async fn list_verbatim_entries(
        &self,
    ) -> crate::store::Result<Vec<crate::store::VerbatimEntry>> {
        Self::unavailable()
    }

    async fn insert_symbol(
        &self,
        _symbol: &crate::code_indexer::CodeSymbol,
    ) -> crate::store::Result<()> {
        Self::unavailable()
    }
    async fn search_symbols(
        &self,
        _query: &str,
        _limit: usize,
    ) -> crate::store::Result<Vec<crate::code_indexer::CodeSymbol>> {
        Self::unavailable()
    }
    async fn insert_edge(
        &self,
        _edge: &crate::code_indexer::SymbolEdge,
    ) -> crate::store::Result<()> {
        Self::unavailable()
    }
    async fn get_callers(
        &self,
        _symbol_id: &str,
    ) -> crate::store::Result<Vec<crate::code_indexer::CodeSymbol>> {
        Self::unavailable()
    }
    async fn get_callees(
        &self,
        _symbol_id: &str,
    ) -> crate::store::Result<Vec<crate::code_indexer::CodeSymbol>> {
        Self::unavailable()
    }
    async fn list_all_symbols(&self) -> crate::store::Result<Vec<crate::code_indexer::CodeSymbol>> {
        Self::unavailable()
    }
    async fn list_all_edges(&self) -> crate::store::Result<Vec<crate::code_indexer::SymbolEdge>> {
        Self::unavailable()
    }
    async fn link_symbol_to_memory(
        &self,
        _symbol_id: &str,
        _memory_id: &MemoryId,
        _turn_index: Option<i32>,
        _reference_type: &str,
        _timestamp: i64,
    ) -> crate::store::Result<()> {
        Self::unavailable()
    }
    async fn find_memories_by_symbol(
        &self,
        _symbol_name: &str,
    ) -> crate::store::Result<Vec<MemoryId>> {
        Self::unavailable()
    }
    async fn list_symbol_memory_references(
        &self,
    ) -> crate::store::Result<Vec<crate::store::SymbolMemoryReference>> {
        Self::unavailable()
    }
    async fn kv_put(&self, _key: &str, _value: &str) -> crate::store::Result<()> {
        Self::unavailable()
    }
    async fn kv_get(&self, _key: &str) -> crate::store::Result<Option<String>> {
        Self::unavailable()
    }
    async fn list_key_values(&self) -> crate::store::Result<Vec<crate::store::MemoryKeyValue>> {
        Self::unavailable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DriftConfig;
    use crate::store::sqlite::SqliteStore;
    use crate::types::{MemorySource, Priority};

    fn in_memory() -> Arc<dyn MemoryStore> {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        Arc::new(SqliteStore::open_path(&tmp.path().join("test.db")).unwrap())
    }

    fn shared_entry(staleness: f32) -> MemoryEntry {
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L4,
            category: MemoryCategory::Shared,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: "T".to_string(),
            content: "C".to_string(),
            embedding: None,
            tags: Vec::new(),
            relations: Vec::new(),
            confidence: 1.0,
            access_count: 0,
            staleness,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::Shared,
        }
    }

    #[test]
    fn disabled_is_not_enabled() {
        let layer = SharedLayer::disabled();
        assert!(!layer.is_enabled());
    }

    #[test]
    fn new_is_enabled() {
        let layer = SharedLayer::new(in_memory());
        assert!(layer.is_enabled());
    }

    #[tokio::test]
    async fn load_returns_empty_when_disabled() {
        let layer = SharedLayer::disabled();
        assert!(layer.load().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn recall_returns_empty_when_disabled() {
        let layer = SharedLayer::disabled();
        assert!(layer.recall("query", 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn ordinary_insert_is_rejected_even_when_disabled() {
        let layer = SharedLayer::disabled();
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L0,
            category: MemoryCategory::Shared,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: "t".into(),
            content: "c".into(),
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
        };
        assert!(matches!(
            layer.insert(entry).await,
            Err(crate::MemoryError::WriteDenied { .. })
        ));
    }

    #[tokio::test]
    async fn governed_insert_overrides_layer_to_l4_when_enabled() {
        let layer = SharedLayer::new(in_memory());
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L0,
            category: MemoryCategory::Shared,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: "t".into(),
            content: "c".into(),
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
        };
        let id = layer.insert_promoted(entry).await.unwrap();
        let entries = layer.load().await.unwrap();
        let got = entries.iter().find(|e| e.id == id).unwrap();
        assert_eq!(got.layer, MemoryLayer::L4);
    }

    #[tokio::test]
    async fn remove_noops_when_disabled() {
        let layer = SharedLayer::disabled();
        layer.remove(&uuid::Uuid::new_v4()).await.unwrap();
    }

    #[tokio::test]
    async fn prepare_context_returns_empty_when_disabled() {
        let layer = SharedLayer::disabled();
        let budget = TokenBudget {
            total: 1000,
            reserved_system: 0,
            reserved_response: 0,
            allocated_memory: 0,
            allocated_conversation: 0,
            available: 1000,
        };
        let ctx = layer.prepare_context(&budget).await.unwrap();
        assert!(ctx.entries.is_empty());
    }

    #[tokio::test]
    async fn tick_noops_when_disabled() {
        let layer = SharedLayer::disabled();
        layer.tick().await.unwrap();
    }

    #[tokio::test]
    async fn sync_noops_when_disabled() {
        let layer = SharedLayer::disabled();
        layer.sync().await.unwrap();
    }

    #[tokio::test]
    async fn sync_reduces_staleness() {
        let layer = SharedLayer::new(in_memory());
        let id = layer.insert_promoted(shared_entry(0.0)).await.unwrap();

        layer.sync().await.unwrap();
        let entries = layer.load().await.unwrap();
        let entry = entries.iter().find(|e| e.id == id).unwrap();
        assert_eq!(entry.staleness, 0.0);
    }

    #[tokio::test]
    async fn tick_retains_shared_evidence_and_recalibrates_staleness() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.9,
            prune_threshold: 0.5,
            ..Default::default()
        };
        let layer = SharedLayer::with_config(in_memory(), true, None, 2000, drift);
        layer.insert_promoted(shared_entry(1.0)).await.unwrap();
        layer.tick().await.unwrap();
        let entries = layer.load().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].staleness < 0.001);
    }

    #[test]
    fn layer_returns_l4() {
        assert_eq!(SharedLayer::new(in_memory()).layer(), MemoryLayer::L4);
    }
}
