//! Session-scoped memory visibility fences.
//!
//! This module intentionally owns only the live isolation primitive used by
//! cognitive recall and `MemoryOrchestrator`. Legacy overview/rendering and
//! alternate prompt-injection APIs were removed because no runtime caller used
//! them; Runtime's contextual packet pipeline is the sole memory injection
//! path.

use std::{collections::HashSet, sync::Arc};

use tokio::sync::RwLock;

use crate::types::{MemoryEntry, MemoryId};

/// Controls which memory entries are visible to one session-scoped recall.
#[derive(Debug, Clone)]
pub struct ContextFence {
    pub id: String,
    allowed_layers: HashSet<u8>,
    included_ids: HashSet<MemoryId>,
    excluded_ids: HashSet<MemoryId>,
    active: bool,
}

impl ContextFence {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            allowed_layers: HashSet::new(),
            included_ids: HashSet::new(),
            excluded_ids: HashSet::new(),
            active: true,
        }
    }

    #[must_use]
    pub fn allow_layers(mut self, layers: &[u8]) -> Self {
        self.allowed_layers = layers.iter().copied().collect();
        self
    }

    #[must_use]
    pub fn include_ids(mut self, ids: impl IntoIterator<Item = MemoryId>) -> Self {
        self.included_ids.extend(ids);
        self
    }

    #[must_use]
    pub fn exclude_ids(mut self, ids: impl IntoIterator<Item = MemoryId>) -> Self {
        self.excluded_ids.extend(ids);
        self
    }

    #[must_use]
    pub fn allows(&self, entry: &MemoryEntry) -> bool {
        !self.active
            || (!self.excluded_ids.contains(&entry.id)
                && (self.included_ids.contains(&entry.id)
                    || self.allowed_layers.is_empty()
                    || self.allowed_layers.contains(&(entry.layer as u8))))
    }

    pub fn activate(&mut self) {
        self.active = true;
    }

    pub fn deactivate(&mut self) {
        self.active = false;
    }

    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }
}

/// Tracks active fence identities for observability and orchestrator cleanup.
#[derive(Debug, Clone, Default)]
pub struct FenceRegistry {
    fences: Arc<RwLock<HashSet<String>>>,
}

impl FenceRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, fence: &ContextFence) {
        self.fences.write().await.insert(fence.id.clone());
    }

    pub async fn unregister(&self, fence_id: &str) {
        self.fences.write().await.remove(fence_id);
    }

    pub async fn is_registered(&self, fence_id: &str) -> bool {
        self.fences.read().await.contains(fence_id)
    }

    pub async fn list_fences(&self) -> Vec<String> {
        let mut fences = self.fences.read().await.iter().cloned().collect::<Vec<_>>();
        fences.sort();
        fences
    }
}

#[must_use]
pub fn filter_through_fence<'a>(
    entries: &'a [MemoryEntry],
    fence: &ContextFence,
) -> Vec<&'a MemoryEntry> {
    entries.iter().filter(|entry| fence.allows(entry)).collect()
}

/// Build the standard session fence. Scope matching belongs to the typed
/// `MemoryScope` filter upstream; this fence only owns layer/entry visibility.
#[must_use]
pub fn fence_from_session(
    session_id: &str,
    _scope: Option<&str>,
    layers: Option<&[u8]>,
) -> ContextFence {
    // Scope authorization is enforced by `MemoryTurnContext` after every
    // retrieval source returns. A session fence must not silently discard L3
    // long-term recall or L4 team evidence before that typed policy runs.
    ContextFence::new(format!("session:{session_id}"))
        .allow_layers(layers.unwrap_or(&[0, 1, 2, 3, 4]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryCategory, MemoryLayer, MemoryScope, MemorySource, Priority};

    fn entry(layer: MemoryLayer) -> MemoryEntry {
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: "entry".to_string(),
            content: "content".to_string(),
            embedding: None,
            tags: Vec::new(),
            relations: Vec::new(),
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: Default::default(),
        }
    }

    #[test]
    fn session_fence_preserves_isolation_and_explicit_overrides() {
        let l0 = entry(MemoryLayer::L0);
        let l3 = entry(MemoryLayer::L3);
        let fence = fence_from_session("s1", None, None).include_ids([l3.id]);
        assert!(fence.allows(&l0));
        assert!(fence.allows(&l3));

        let fence = fence.exclude_ids([l0.id]);
        assert!(!fence.allows(&l0));
    }

    #[tokio::test]
    async fn registry_tracks_live_fences_only() {
        let registry = FenceRegistry::new();
        let fence = ContextFence::new("session:s1");
        registry.register(&fence).await;
        assert!(registry.is_registered("session:s1").await);
        registry.unregister("session:s1").await;
        assert!(!registry.is_registered("session:s1").await);
    }
}
