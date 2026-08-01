use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct HotStateMetrics {
    graph_hits: AtomicU64,
    graph_misses: AtomicU64,
    graph_recoveries: AtomicU64,
    graph_publishes: AtomicU64,
    session_hits: AtomicU64,
    session_misses: AtomicU64,
    resident_bytes: AtomicU64,
    evictions: AtomicU64,
    materializer_enqueued: AtomicU64,
    materializer_coalesced: AtomicU64,
    materializer_dropped: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotStateMetricsSnapshot {
    pub graph_hits: u64,
    pub graph_misses: u64,
    pub graph_recoveries: u64,
    pub graph_publishes: u64,
    pub session_hits: u64,
    pub session_misses: u64,
    pub resident_bytes: u64,
    pub evictions: u64,
    pub materializer_enqueued: u64,
    pub materializer_coalesced: u64,
    pub materializer_dropped: u64,
}

impl HotStateMetrics {
    pub(super) fn graph_hit(&self) {
        self.graph_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn graph_miss(&self) {
        self.graph_misses.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn graph_recovered(&self) {
        self.graph_recoveries.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn graph_published(&self) {
        self.graph_publishes.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn session_hit(&self) {
        self.session_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn session_miss(&self) {
        self.session_misses.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn set_resident_bytes(&self, bytes: u64) {
        self.resident_bytes.store(bytes, Ordering::Relaxed);
    }
    pub(super) fn evicted(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn materializer_enqueued(&self) {
        self.materializer_enqueued.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn materializer_coalesced(&self) {
        self.materializer_coalesced.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn materializer_dropped(&self) {
        self.materializer_dropped.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> HotStateMetricsSnapshot {
        HotStateMetricsSnapshot {
            graph_hits: self.graph_hits.load(Ordering::Relaxed),
            graph_misses: self.graph_misses.load(Ordering::Relaxed),
            graph_recoveries: self.graph_recoveries.load(Ordering::Relaxed),
            graph_publishes: self.graph_publishes.load(Ordering::Relaxed),
            session_hits: self.session_hits.load(Ordering::Relaxed),
            session_misses: self.session_misses.load(Ordering::Relaxed),
            resident_bytes: self.resident_bytes.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            materializer_enqueued: self.materializer_enqueued.load(Ordering::Relaxed),
            materializer_coalesced: self.materializer_coalesced.load(Ordering::Relaxed),
            materializer_dropped: self.materializer_dropped.load(Ordering::Relaxed),
        }
    }
}
