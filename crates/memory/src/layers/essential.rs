//! L1 – Essential working-memory layer.
//!
//! High-churn, short-lived entries that capture the most recent context:
//! current tasks, active decisions, and session-level observations.
//! Entries here are frequently evicted or compressed into L2/L3.
//!
//! Characteristics:
//! - ~2000 token budget
//! - Cross-session persistence for Critical/High priority entries
//! - Sorted by priority: Critical > High > Normal > Low
//! - `tick()` applies staleness decay and prunes stale Low-priority entries

use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    config::DriftConfig,
    layers::{LayerManager, Result},
    project_scope::MemoryScope,
    store::MemoryStore,
    types::{
        MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, PreparedContext,
        Priority, TokenBudget,
    },
};

/// Default maximum token budget for the essential layer.
const DEFAULT_MAX_TOKENS: u64 = 2000;
/// Maximum number of hot symbol slots in L1.
const MAX_HOT_SYMBOLS: usize = 5;

/// A tracked code symbol in the hot-symbol cache.
#[derive(Debug, Clone)]
pub struct HotSymbol {
    pub name: String,
    pub frequency: f32,
    pub last_referenced: i64,
}

/// Manager for the L1 essential working-memory layer.
pub struct EssentialLayer {
    store: Arc<dyn MemoryStore>,
    max_tokens: u64,
    drift: DriftConfig,
    hot_symbols: Mutex<Vec<HotSymbol>>,
}

impl EssentialLayer {
    /// Create a new `EssentialLayer` with default token budget.
    pub fn new(store: Arc<dyn MemoryStore>) -> Self {
        Self {
            store,
            max_tokens: DEFAULT_MAX_TOKENS,
            drift: DriftConfig::default(),
            hot_symbols: Mutex::new(Vec::new()),
        }
    }

    /// Create with a custom token budget and drift configuration.
    pub fn with_config(store: Arc<dyn MemoryStore>, max_tokens: u64, drift: DriftConfig) -> Self {
        Self {
            store,
            max_tokens,
            drift,
            hot_symbols: Mutex::new(Vec::new()),
        }
    }

    /// Load all L1 entries sorted by priority, truncated to `max_tokens` budget.
    pub async fn load(&self) -> Result<Vec<MemoryEntry>> {
        let entries = self.store.search_by_layer(MemoryLayer::L1).await?;
        Ok(truncate_to_budget(entries, self.max_tokens))
    }

    /// Add a new essential memory entry.
    pub async fn add(
        &self,
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
            id: Uuid::new_v4(),
            layer: MemoryLayer::L1,
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
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    /// Update the content of an existing L1 entry.
    pub async fn update(&self, id: &MemoryId, content: &str) -> Result<()> {
        if let Some(mut entry) = self.store.get(id).await? {
            entry.content = content.to_string();
            entry.updated_at = Utc::now();
            entry.staleness = 0.0; // reset staleness on update
            self.store.update(&entry).await?;
        }
        Ok(())
    }

    /// Promote a code symbol to the hot-symbol cache.
    ///
    /// Called by the background extractor when a symbol is frequently
    /// referenced.  If the symbol is already tracked its frequency is
    /// boosted; otherwise a new slot is allocated (evicting the lowest-
    /// frequency symbol if the cache is full).
    pub fn promote_symbol(&self, name: &str) {
        let mut symbols = self.hot_symbols.lock();
        let now = Utc::now().timestamp();

        // Boost existing symbol
        if let Some(sym) = symbols.iter_mut().find(|s| s.name == name) {
            sym.frequency += 1.0;
            sym.last_referenced = now;
            return;
        }

        // Evict lowest-frequency if at capacity
        if symbols.len() >= MAX_HOT_SYMBOLS {
            symbols.sort_by(|a, b| {
                a.frequency
                    .partial_cmp(&b.frequency)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            symbols.remove(0);
        }

        symbols.push(HotSymbol {
            name: name.to_string(),
            frequency: 1.0,
            last_referenced: now,
        });
    }

    /// Return the current hot symbol slots, sorted by frequency (descending).
    #[must_use]
    pub fn get_hot_symbols(&self) -> Vec<HotSymbol> {
        let symbols = self.hot_symbols.lock();
        let mut syms: Vec<HotSymbol> = symbols.clone();
        syms.sort_by(|a, b| {
            b.frequency
                .partial_cmp(&a.frequency)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        syms
    }
}

// ---------------------------------------------------------------------------
// LayerManager implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LayerManager for EssentialLayer {
    fn layer(&self) -> MemoryLayer {
        MemoryLayer::L1
    }

    /// Insert an entry, overriding its layer to L1.
    async fn insert(&self, mut entry: MemoryEntry) -> Result<MemoryId> {
        entry.layer = MemoryLayer::L1;
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    async fn remove(&self, id: &MemoryId) -> Result<()> {
        self.store.delete(id).await
    }

    /// Prepare L1 context within the given token budget.
    async fn prepare_context(&self, budget: &TokenBudget) -> Result<PreparedContext> {
        let available = budget.available.min(self.max_tokens);
        let entries = self.store.search_by_layer(MemoryLayer::L1).await?;
        let kept = truncate_to_budget(entries, available);
        let used_tokens: u64 = kept.iter().map(|e| estimate_tokens(&e.content)).sum();

        Ok(PreparedContext {
            entries: kept,
            total_tokens: used_tokens,
            budget: budget.clone(),
            depth_scale: 0.8,
            prepared_at: Utc::now(),
            code_context: None,
        })
    }

    /// Apply staleness decay and prune high-staleness Low-priority entries.
    async fn tick(&self) -> Result<()> {
        let entries = self.store.search_by_layer(MemoryLayer::L1).await?;
        let decay = self.drift.staleness_decay_per_day;

        for mut entry in entries {
            // Increase staleness.
            entry.staleness = (entry.staleness + decay).min(1.0);

            // Prune Low-priority entries that have become very stale.
            if entry.priority == Priority::Low
                && entry.staleness >= self.drift.low_priority_prune_threshold
            {
                self.store.delete(&entry.id).await?;
                continue;
            }

            // Prune Normal entries above prune_threshold.
            if entry.priority == Priority::Normal && entry.staleness >= self.drift.prune_threshold {
                self.store.delete(&entry.id).await?;
                continue;
            }

            self.store.update(&entry).await?;
        }

        // Apply frequency decay to hot symbols and evict cold ones.
        let mut symbols = self.hot_symbols.lock();
        let hot_decay = decay * 0.5; // slower decay for symbols
        symbols.retain_mut(|sym| {
            sym.frequency = (sym.frequency - hot_decay).max(0.0);
            sym.frequency > 0.1
        });

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Sort entries by priority (ascending = Critical first) then by recency,
/// and truncate to the given token budget.
fn truncate_to_budget(mut entries: Vec<MemoryEntry>, max_tokens: u64) -> Vec<MemoryEntry> {
    // Critical(0) < High(1) < Normal(2) < Low(3)
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

/// Estimate token count from content length (4 chars ≈ 1 token).
fn estimate_tokens(content: &str) -> u64 {
    (content.len() as u64).div_ceil(4)
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

    #[test]
    fn new_uses_default_max_tokens() {
        let layer = EssentialLayer::new(in_memory());
        assert_eq!(layer.max_tokens, 2000);
    }

    #[test]
    fn with_config_sets_custom_parameters() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.05,
            prune_threshold: 0.5,
            ..Default::default()
        };
        let layer = EssentialLayer::with_config(in_memory(), 500, drift);
        assert_eq!(layer.max_tokens, 500);
    }

    #[tokio::test]
    async fn add_creates_entry_with_fields() {
        let layer = EssentialLayer::new(in_memory());
        let id = layer
            .add(
                MemoryCategory::Decision,
                "Task",
                "Implement login",
                Priority::High,
                MemorySource::UserExplicit,
                vec!["auth".into()],
                MemoryScope::Session("s1".into()),
            )
            .await
            .unwrap();

        let entries = layer.load().await.unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.id, id);
        assert_eq!(e.layer, MemoryLayer::L1);
        assert_eq!(e.category, MemoryCategory::Decision);
        assert_eq!(e.priority, Priority::High);
        assert_eq!(e.tags, vec!["auth"]);
        assert_eq!(e.scope, MemoryScope::Session("s1".into()));
    }

    #[tokio::test]
    async fn update_modifies_content_and_resets_staleness() {
        let layer = EssentialLayer::new(in_memory());
        let id = layer
            .add(
                MemoryCategory::Decision,
                "T",
                "old",
                Priority::Normal,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        layer.update(&id, "new content here").await.unwrap();
        let entries = layer.load().await.unwrap();
        assert_eq!(entries[0].content, "new content here");
        assert_eq!(entries[0].staleness, 0.0);
    }

    #[tokio::test]
    async fn insert_overrides_layer_to_l1() {
        let layer = EssentialLayer::new(in_memory());
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Decision,
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
        let id = layer.insert(entry).await.unwrap();
        let loaded = layer
            .load()
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.id == id)
            .unwrap();
        assert_eq!(loaded.layer, MemoryLayer::L1);
    }

    #[tokio::test]
    async fn remove_deletes_entry() {
        let layer = EssentialLayer::new(in_memory());
        let id = layer
            .add(
                MemoryCategory::Decision,
                "T",
                "C",
                Priority::Normal,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        assert_eq!(layer.load().await.unwrap().len(), 1);
        layer.remove(&id).await.unwrap();
        assert_eq!(layer.load().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn tick_applies_staleness_decay() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.1,
            prune_threshold: 0.9,
            ..Default::default()
        };
        let layer = EssentialLayer::with_config(in_memory(), 2000, drift);
        layer
            .add(
                MemoryCategory::Decision,
                "T",
                "C",
                Priority::High,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        layer.tick().await.unwrap();
        let entries = layer.load().await.unwrap();
        assert_eq!(entries[0].staleness, 0.1);
    }

    #[tokio::test]
    async fn tick_prunes_stale_low_priority_entries() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.9,
            prune_threshold: 0.5,
            ..Default::default()
        };
        let layer = EssentialLayer::with_config(in_memory(), 2000, drift);
        layer
            .add(
                MemoryCategory::Decision,
                "T",
                "C",
                Priority::Low,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        layer.tick().await.unwrap(); // staleness now 0.9 >= 0.8 threshold
        assert!(layer.load().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn tick_keeps_high_priority_entries() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.9,
            prune_threshold: 0.5,
            ..Default::default()
        };
        let layer = EssentialLayer::with_config(in_memory(), 2000, drift);
        layer
            .add(
                MemoryCategory::Decision,
                "T",
                "C",
                Priority::High,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        layer.tick().await.unwrap();
        assert_eq!(layer.load().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn load_truncates_to_budget() {
        let layer = EssentialLayer::with_config(in_memory(), 10, DriftConfig::default());
        layer
            .add(
                MemoryCategory::Decision,
                "T",
                &"x".repeat(1000),
                Priority::Normal,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        // 1000 chars → ~250 tokens, but max_tokens is 10 → entry excluded
        let entries = layer.load().await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn load_sorts_by_priority() {
        let layer = EssentialLayer::new(in_memory());
        layer
            .add(
                MemoryCategory::Decision,
                "low",
                "x",
                Priority::Low,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        layer
            .add(
                MemoryCategory::Decision,
                "critical",
                "x",
                Priority::Critical,
                MemorySource::AutoExtracted,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();

        let entries = layer.load().await.unwrap();
        assert_eq!(entries[0].priority, Priority::Critical);
        assert_eq!(entries[1].priority, Priority::Low);
    }

    #[test]
    fn layer_returns_l1() {
        assert_eq!(EssentialLayer::new(in_memory()).layer(), MemoryLayer::L1);
    }

    // -----------------------------------------------------------------------
    // T6: Hot symbol tracking tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_hot_symbol_promotion() {
        let layer = EssentialLayer::new(in_memory());

        // Initially no hot symbols
        assert!(layer.get_hot_symbols().is_empty());

        // Promote a symbol
        layer.promote_symbol("authenticate_user");
        let hot = layer.get_hot_symbols();
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].name, "authenticate_user");
        assert_eq!(hot[0].frequency, 1.0);

        // Promote again — frequency should increase
        layer.promote_symbol("authenticate_user");
        let hot2 = layer.get_hot_symbols();
        assert_eq!(hot2.len(), 1);
        assert_eq!(hot2[0].frequency, 2.0);
    }

    #[test]
    fn test_hot_symbol_eviction() {
        let layer = EssentialLayer::new(in_memory());

        // Fill the cache (max 5 slots)
        for i in 0..6 {
            layer.promote_symbol(&format!("symbol_{i}"));
        }

        let hot = layer.get_hot_symbols();
        assert_eq!(hot.len(), 5, "should cap at 5 hot symbols");

        // The lowest-frequency symbol (symbol_0 with frequency 1.0) should be evicted
        // symbol_1 through symbol_5 should remain
        let names: Vec<&str> = hot.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"symbol_0"), "symbol_0 should be evicted");
        assert!(names.contains(&"symbol_1"), "symbol_1 should remain");
        assert!(names.contains(&"symbol_5"), "symbol_5 should remain");
    }

    #[tokio::test]
    async fn test_hot_symbol_decay_on_tick() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.4,
            prune_threshold: 0.9,
            ..Default::default()
        };
        let layer = EssentialLayer::with_config(in_memory(), 2000, drift);

        // Add a hot symbol with moderate frequency
        layer.promote_symbol("handle_request");
        layer.promote_symbol("handle_request");
        assert_eq!(layer.get_hot_symbols()[0].frequency, 2.0);

        // One tick: hot decay = 0.4 * 0.5 = 0.2 → frequency becomes 1.8
        layer.tick().await.unwrap();
        let hot = layer.get_hot_symbols();
        assert!(!hot.is_empty(), "symbol should survive one tick");
        assert!(hot[0].frequency < 2.0, "frequency should decay");
        assert!(hot[0].frequency > 1.0, "frequency decay should be moderate");
    }
}
