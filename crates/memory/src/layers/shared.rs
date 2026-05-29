//! L4 – Shared / team knowledge backbone for multi-agent collaboration.
//!
//! # Role in Multi-Agent Architecture
//!
//! L4 serves as the **cross-agent knowledge bus** in Cowd's multi-agent
//! architecture. Unlike L0-L3 which are session/agent-scoped, L4 entries are
//! visible to all agents within a team or organisation.
//!
//! ## Key Use Cases
//!
//! - **Team conventions**: coding standards, review checklists, naming conventions
//! - **Shared decisions**: architectural decisions, API contracts, design tradeoffs
//! - **Task handoff**: agent-to-agent context transfer via persistent shared entries
//! - **Runbooks**: operational knowledge, on-call procedures, troubleshooting guides
//! - **Agent orchestration**: Worker assignments, task progress tracking, completion
//!   signals visible to the orchestrator and peer agents
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
//! - **Automatic pruning**: stale entries (exceeding drift threshold) are
//!   removed during `tick()` to prevent accumulation.

use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{ MemoryScope,
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

// ---------------------------------------------------------------------------
// L4 Event Bus – push notifications for cross-agent awareness
// ---------------------------------------------------------------------------

/// An event emitted when an agent modifies L4 shared memory.
///
/// Other agents can subscribe to receive real-time notifications so they
/// become immediately aware of new shared entries without waiting for the
/// next `prepare_context` pull cycle.
#[derive(Debug, Clone)]
pub struct L4Event {
    /// Which agent performed the operation.
    pub agent_id: String,
    /// UUID string of the affected memory entry.
    pub memory_id: String,
    /// What kind of operation was performed.
    pub operation: L4Operation,
    /// Title of the memory entry.
    pub title: String,
    /// Unix timestamp in milliseconds when the event occurred.
    pub timestamp_ms: u64,
}

/// Type of L4 modification that triggered an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L4Operation {
    /// A new entry was created.
    Insert,
    /// An existing entry was modified.
    Update,
    /// An entry was removed.
    Delete,
}

/// Lightweight publish-subscribe bus for L4 memory events.
///
/// Built on `tokio::sync::broadcast`, it allows any number of subscribers
/// to receive push notifications whenever an agent writes to L4.
pub struct L4EventBus {
    tx: broadcast::Sender<L4Event>,
}

impl L4EventBus {
    /// Create a new bus with the given channel capacity.
    ///
    /// `capacity` is the maximum number of buffered events before slow
    /// receivers start lagging (lagged receivers are closed).
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe to receive all future L4 events.
    ///
    /// Returns a [`broadcast::Receiver`] that can be used to drain
    /// pending events via [`broadcast::Receiver::try_recv`].
    pub fn subscribe(&self) -> broadcast::Receiver<L4Event> {
        self.tx.subscribe()
    }

    /// Publish an L4 event to all active subscribers.
    ///
    /// If no subscribers are listening the event is silently dropped.
    /// Slow receivers that have lagged beyond the channel capacity are
    /// closed automatically by the underlying broadcast channel.
    pub fn publish(&self, event: L4Event) {
        let _ = self.tx.send(event);
    }
}

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
    /// Optional event bus for push-notifying other agents of L4 writes.
    pub event_bus: Option<Arc<L4EventBus>>,
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
            event_bus: None,
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
            event_bus: None,
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
            event_bus: None,
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
            scope: self.shared_scope.clone().unwrap_or_default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        };
        let id = self.store.insert(&entry).await?;
        // Publish push notification so other agents become immediately aware.
        if let Some(ref bus) = self.event_bus {
            bus.publish(L4Event {
                agent_id: String::new(),
                memory_id: id.to_string(),
                operation: L4Operation::Insert,
                title: title.to_string(),
                timestamp_ms: now.timestamp_millis() as u64,
            });
        }
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

    /// Recall L4 entries scoped to a Project scope via FTS.
    pub async fn recall_project(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let scope = self.shared_scope.clone().unwrap_or_default();
        let results = self.store.search_fts_scoped(query, &scope, limit * 2).await?;
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
        let results = self.store.search_fts_scoped(query, &MemoryScope::Global, limit * 2).await?;
        let filtered: Vec<MemoryEntry> = results
            .into_iter()
            .filter(|e| e.layer == MemoryLayer::L4)
            .take(limit)
            .collect();
        Ok(filtered)
    }

    /// Recall peer agent entries from L4 for cross-agent perception.
    ///
    /// Filters for entries with visibility==Shared from other agents,
    /// within a 5-minute time window. Caps at `max_per_peer` entries
    /// per agent and `max_peers` total peer agents.
    pub async fn recall_peers(
        &self,
        query: &str,
        current_agent: &str,
        max_per_peer: usize,
        max_peers: usize,
    ) -> Result<Vec<MemoryEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let cutoff = Utc::now() - chrono::Duration::minutes(5);
        let results = self.store.search_fts(query, max_peers * max_per_peer * 2).await?;
        let peer_entries: Vec<MemoryEntry> = results
            .into_iter()
            .filter(|e| {
                e.layer == MemoryLayer::L4
                    && e.visibility == crate::types::AgentVisibility::Shared
                    && e.source_agent.as_deref() != Some(current_agent)
                    && e.created_at >= cutoff
            })
            .collect();

        // Group by source_agent, cap per peer, then cap total peers.
        let mut seen_peers: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut capped = Vec::new();
        for entry in peer_entries {
            let agent_key = entry.source_agent.clone().unwrap_or_default();
            let count = seen_peers.get(&agent_key).copied().unwrap_or(0);
            if count >= max_per_peer {
                continue;
            }
            if seen_peers.len() >= max_peers && !seen_peers.contains_key(&agent_key) {
                continue;
            }
            seen_peers.insert(agent_key, count + 1);
            capped.push(entry);
        }
        Ok(capped)
    }

    /// Recall peer agent entries from L4 for intra-turn real-time perception.
    ///
    /// Unlike [`recall_peers`], this does NOT apply a 5-minute time cutoff.
    /// Instead it filters by `session_id` so entries written by Agent A in the
    /// current turn are visible to Agent B in the same turn's prepare_context.
    /// Caps at `max_per_peer` entries per agent and `max_peers` total peers.
    pub async fn recall_peers_realtime(
        &self,
        query: &str,
        current_agent: &str,
        current_session_id: &str,
        max_per_peer: usize,
        max_peers: usize,
    ) -> Result<Vec<MemoryEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let results = self.store.search_fts(query, max_peers * max_per_peer * 2).await?;
        let peer_entries: Vec<MemoryEntry> = results
            .into_iter()
            .filter(|e| {
                e.layer == MemoryLayer::L4
                    && e.visibility == crate::types::AgentVisibility::Shared
                    && e.source_agent.as_deref() != Some(current_agent)
                    // Match entries from the same session (intra-turn)
                    && e.session_id.as_deref() == Some(current_session_id)
            })
            .collect();

        // Group by source_agent, cap per peer, then cap total peers.
        let mut seen_peers: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut capped = Vec::new();
        for entry in peer_entries {
            let agent_key = entry.source_agent.clone().unwrap_or_default();
            let count = seen_peers.get(&agent_key).copied().unwrap_or(0);
            if count >= max_per_peer {
                continue;
            }
            if seen_peers.len() >= max_peers && !seen_peers.contains_key(&agent_key) {
                continue;
            }
            seen_peers.insert(agent_key, count + 1);
            capped.push(entry);
        }
        Ok(capped)
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
        let entries = self.store.search_by_layer(MemoryLayer::L4).await.unwrap_or_default();

        let mut tag_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
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
        if let Some(ref s) = self.shared_scope {
            entry.scope = s.clone();
        }
        let id = self.store.insert(&entry).await?;
        // Publish push notification so other agents become immediately aware.
        if let Some(ref bus) = self.event_bus {
            bus.publish(L4Event {
                agent_id: entry.source_agent.clone().unwrap_or_default(),
                memory_id: id.to_string(),
                operation: L4Operation::Insert,
                title: entry.title.clone(),
                timestamp_ms: Utc::now().timestamp_millis() as u64,
            });
        }
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
use crate::entity::{Entity, Triple};

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

    async fn load_verbatim_by_id(&self, _id: &str) -> crate::store::Result<Option<crate::store::VerbatimEntry>> {
        Ok(None)
    }

    async fn search_verbatim_by_content(
        &self,
        _query: &str,
    ) -> crate::store::Result<Vec<crate::store::VerbatimEntry>> {
        Ok(Vec::new())
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
    async fn add_returns_nil_when_disabled() {
        let layer = SharedLayer::disabled();
        let id = layer
            .add(MemoryCategory::Shared, "T", "C", Priority::Normal, MemorySource::AutoExtracted, vec![])
            .await
            .unwrap();
        assert_eq!(id, uuid::Uuid::nil());
    }

    #[tokio::test]
    async fn add_creates_entry_when_enabled() {
        let layer = SharedLayer::new(in_memory());
        let id = layer
            .add(MemoryCategory::Shared, "Team decision", "Use Rust", Priority::High, MemorySource::Import, vec!["lang".into()])
            .await
            .unwrap();
        assert_ne!(id, uuid::Uuid::nil());

        let entries = layer.load().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
        assert_eq!(entries[0].layer, MemoryLayer::L4);
        assert_eq!(entries[0].tags, vec!["lang"]);
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
    async fn insert_noops_when_disabled() {
        let layer = SharedLayer::disabled();
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(), layer: MemoryLayer::L0, category: MemoryCategory::Shared,
            priority: Priority::Normal, source: MemorySource::AutoExtracted,
            title: "t".into(), content: "c".into(), embedding: None,
            tags: vec![], relations: vec![], confidence: 1.0, access_count: 0,
            staleness: 0.0, created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            last_accessed_at: None, scope: MemoryScope::default(), session_id: None,
            source_agent: None, visibility: crate::types::AgentVisibility::default(),
        };
        let id = layer.insert(entry).await.unwrap();
        assert_eq!(id, uuid::Uuid::nil());
    }

    #[tokio::test]
    async fn insert_overrides_layer_to_l4_when_enabled() {
        let layer = SharedLayer::new(in_memory());
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(), layer: MemoryLayer::L0, category: MemoryCategory::Shared,
            priority: Priority::Normal, source: MemorySource::AutoExtracted,
            title: "t".into(), content: "c".into(), embedding: None,
            tags: vec![], relations: vec![], confidence: 1.0, access_count: 0,
            staleness: 0.0, created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            last_accessed_at: None, scope: MemoryScope::default(), session_id: None,
            source_agent: None, visibility: crate::types::AgentVisibility::default(),
        };
        let id = layer.insert(entry).await.unwrap();
        assert_ne!(id, uuid::Uuid::nil());
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
        let budget = TokenBudget { total: 1000, reserved_system: 0, reserved_response: 0, allocated_memory: 0, allocated_conversation: 0, available: 1000 };
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
        let id = layer
            .add(MemoryCategory::Shared, "T", "C", Priority::Normal, MemorySource::AutoExtracted, vec![])
            .await
            .unwrap();

        layer.sync().await.unwrap();
        let entries = layer.load().await.unwrap();
        let entry = entries.iter().find(|e| e.id == id).unwrap();
        assert_eq!(entry.staleness, 0.0);
    }

    #[tokio::test]
    async fn tick_prunes_stale_entries() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.9,
            prune_threshold: 0.5,
            ..Default::default()
        };
        let layer = SharedLayer::with_config(in_memory(), true, None, 2000, drift);
        layer
            .add(MemoryCategory::Shared, "T", "C", Priority::Normal, MemorySource::AutoExtracted, vec![])
            .await
            .unwrap();
        layer.tick().await.unwrap();
        assert!(layer.load().await.unwrap().is_empty());
    }

    #[test]
    fn layer_returns_l4() {
        assert_eq!(SharedLayer::new(in_memory()).layer(), MemoryLayer::L4);
    }
}
