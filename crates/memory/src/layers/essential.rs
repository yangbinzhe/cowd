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

/// Default maximum token budget for the essential layer.
const DEFAULT_MAX_TOKENS: u64 = 2000;
/// Staleness threshold above which Low-priority entries are evicted.
const LOW_PRIORITY_PRUNE_THRESHOLD: f32 = 0.8;

/// Manager for the L1 essential working-memory layer.
pub struct EssentialLayer {
    store: Arc<dyn MemoryStore>,
    max_tokens: u64,
    drift: DriftConfig,
}

impl EssentialLayer {
    /// Create a new `EssentialLayer` with default token budget.
    pub fn new(store: Arc<dyn MemoryStore>) -> Self {
        Self {
            store,
            max_tokens: DEFAULT_MAX_TOKENS,
            drift: DriftConfig::default(),
        }
    }

    /// Create with a custom token budget and drift configuration.
    pub fn with_config(store: Arc<dyn MemoryStore>, max_tokens: u64, drift: DriftConfig) -> Self {
        Self {
            store,
            max_tokens,
            drift,
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
        scope: Option<String>,
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
                && entry.staleness >= LOW_PRIORITY_PRUNE_THRESHOLD
            {
                self.store.delete(&entry.id).await?;
                continue;
            }

            // Prune Normal entries above prune_threshold.
            if entry.priority == Priority::Normal
                && entry.staleness >= self.drift.prune_threshold
            {
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
