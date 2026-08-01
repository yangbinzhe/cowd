use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;

use serde::{Deserialize, Serialize};

use super::{HotResidencyRegistry, HotResidentClass, HotStateMetrics};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedMaterialization {
    pub key: String,
    pub revision: u64,
    pub commit_cursor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedMaterializerHealth {
    pub pending: usize,
    pub capacity: usize,
    pub latest_commit_cursor: u64,
    pub materialized_keys: usize,
}

/// Runtime-owned bounded worker for rebuildable cursor projections. Canonical
/// journal writes never enter this queue and therefore cannot be dropped.
pub struct DerivedMaterializer {
    capacity: usize,
    sender: Mutex<Option<mpsc::SyncSender<DerivedMaterialization>>>,
    latest_enqueued: Arc<Mutex<HashMap<String, u64>>>,
    materialized: Arc<RwLock<HashMap<String, DerivedMaterialization>>>,
    pending: Arc<AtomicUsize>,
    latest_commit_cursor: Arc<AtomicU64>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    metrics: Arc<HotStateMetrics>,
}

impl DerivedMaterializer {
    pub(super) fn new(
        capacity: usize,
        metrics: Arc<HotStateMetrics>,
        residency: Arc<HotResidencyRegistry>,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<DerivedMaterialization>(capacity);
        let latest_enqueued = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
        let materialized = Arc::new(RwLock::new(HashMap::new()));
        let pending = Arc::new(AtomicUsize::new(0));
        let latest_commit_cursor = Arc::new(AtomicU64::new(0));
        let worker_latest = Arc::clone(&latest_enqueued);
        let worker_materialized = Arc::clone(&materialized);
        let worker_pending = Arc::clone(&pending);
        let worker_cursor = Arc::clone(&latest_commit_cursor);
        let worker_metrics = Arc::clone(&metrics);
        let worker_residency = Arc::clone(&residency);
        let worker = thread::Builder::new()
            .name("cowd-derived-materializer".to_string())
            .spawn(move || {
                while let Ok(item) = receiver.recv() {
                    worker_pending.fetch_sub(1, Ordering::Relaxed);
                    let key = item.key.clone();
                    let revision = item.revision;
                    let commit_cursor = item.commit_cursor;
                    let current = worker_latest
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(&key)
                        .copied()
                        .unwrap_or_default();
                    if revision < current {
                        worker_metrics.materializer_coalesced();
                        continue;
                    }
                    worker_cursor.fetch_max(commit_cursor, Ordering::Relaxed);
                    worker_materialized
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(key.clone(), item);
                    {
                        let mut latest = worker_latest
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if latest.get(&key).copied() == Some(revision) {
                            latest.remove(&key);
                        }
                    }
                    worker_residency.upsert(
                        resident_id(&key),
                        HotResidentClass::DerivedProjection,
                        key.clone(),
                        u64::try_from(key.len())
                            .unwrap_or(u64::MAX)
                            .saturating_add(24),
                        Some(commit_cursor),
                    );
                    if worker_residency.pressure_high() {
                        for candidate in worker_residency
                            .eviction_candidates(HotResidentClass::DerivedProjection)
                            .into_iter()
                            .filter(|candidate| candidate.resident_id.starts_with("materializer:"))
                        {
                            if worker_residency.resident_bytes()
                                <= worker_residency.target_low_watermark()
                            {
                                break;
                            }
                            worker_materialized
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .remove(&candidate.owner_id);
                            worker_residency.remove(&candidate.resident_id);
                        }
                    }
                }
            })
            .expect("derived materializer worker must start");
        Self {
            capacity,
            sender: Mutex::new(Some(sender)),
            latest_enqueued,
            materialized,
            pending,
            latest_commit_cursor,
            worker: Mutex::new(Some(worker)),
            metrics,
        }
    }

    pub fn enqueue(&self, item: DerivedMaterialization) -> bool {
        if self
            .materialized
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&item.key)
            .is_some_and(|materialized| materialized.revision >= item.revision)
        {
            self.metrics.materializer_coalesced();
            return true;
        }
        {
            let mut latest = self
                .latest_enqueued
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if latest
                .get(&item.key)
                .is_some_and(|revision| *revision >= item.revision)
            {
                self.metrics.materializer_coalesced();
                return true;
            }
            latest.insert(item.key.clone(), item.revision);
        }
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            self.metrics.materializer_dropped();
            return false;
        };
        self.pending.fetch_add(1, Ordering::Relaxed);
        match sender.try_send(item) {
            Ok(()) => {
                self.metrics.materializer_enqueued();
                true
            }
            Err(mpsc::TrySendError::Full(item)) => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                self.latest_enqueued
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&item.key);
                self.metrics.materializer_dropped();
                false
            }
            Err(mpsc::TrySendError::Disconnected(item)) => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                self.latest_enqueued
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&item.key);
                self.metrics.materializer_dropped();
                false
            }
        }
    }

    #[must_use]
    pub fn materialized(&self, key: &str) -> Option<DerivedMaterialization> {
        self.materialized
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    #[must_use]
    pub fn health(&self) -> DerivedMaterializerHealth {
        DerivedMaterializerHealth {
            pending: self.pending.load(Ordering::Relaxed),
            capacity: self.capacity,
            latest_commit_cursor: self.latest_commit_cursor.load(Ordering::Relaxed),
            materialized_keys: self
                .materialized
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
        }
    }
}

fn resident_id(key: &str) -> String {
    format!("materializer:{key}")
}

impl Drop for DerivedMaterializer {
    fn drop(&mut self) {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn materializer_coalesces_and_drains_on_drop() {
        let metrics = Arc::new(HotStateMetrics::default());
        let budget = Arc::new(RwLock::new(super::super::HotMemoryBudget::resolve(
            &super::super::HotStateMemoryConfig::default(),
        )));
        let residency = Arc::new(HotResidencyRegistry::new(budget, metrics.clone()));
        let materializer = DerivedMaterializer::new(8, metrics, residency);
        assert!(materializer.enqueue(DerivedMaterialization {
            key: "graph:a".to_string(),
            revision: 1,
            commit_cursor: 4,
        }));
        for _ in 0..50 {
            if materializer.materialized("graph:a").is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            materializer.materialized("graph:a").unwrap().commit_cursor,
            4
        );
    }
}
