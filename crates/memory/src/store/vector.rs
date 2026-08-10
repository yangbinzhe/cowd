//! In-process lightweight vector index for exact nearest-neighbour search.
//!
//! The index stores all vectors in a `HashMap` keyed by [`MemoryId`] and uses a
//! an exact cosine-similarity scan with a bounded top-k heap, which is appropriate
//! for the enforced corpus limit (50 000 entries).
//!
//! ## Persistence
//! The index is serialised to / deserialised from a JSON file so that it
//! survives process restarts without an external vector database.
//!
//! ```json
//! {
//!   "schema_version": 2,
//!   "generation": 42,
//!   "dimension": 1536,
//!   "vectors": [{"id": "<uuid>", "embedding": [0.1, -0.3, …]}]
//! }
//! ```

use std::{
    cmp::{Ordering as CmpOrdering, Reverse},
    collections::{BinaryHeap, HashMap, VecDeque},
    fs,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::store::sqlite::SqliteStore;
use crate::{error::MemoryError, types::MemoryId};

// ─── Serialisation envelope ───────────────────────────────────────────────────

const VECTOR_INDEX_SCHEMA_VERSION: u32 = 2;

/// Current on-disk representation. Vectors are an ordered sequence so a
/// snapshot can be streamed without cloning every embedding buffer.
#[derive(Deserialize)]
struct IndexSnapshotV2 {
    schema_version: u32,
    generation: u64,
    dimension: u32,
    vectors: Vec<IndexSnapshotEntry>,
}

#[derive(Deserialize)]
struct IndexSnapshotEntry {
    id: MemoryId,
    embedding: Vec<f32>,
}

/// Reader for the v1 map-based format. Keeping this tiny compatibility reader
/// avoids discarding a valid rebuildable index during an in-place upgrade.
#[derive(Deserialize)]
struct LegacyIndexSnapshot {
    dimension: u32,
    vectors: HashMap<MemoryId, Vec<f32>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CompatibleIndexSnapshot {
    V2(IndexSnapshotV2),
    Legacy(LegacyIndexSnapshot),
}

#[derive(Serialize)]
struct IndexSnapshotRef<'a> {
    schema_version: u32,
    generation: u64,
    dimension: u32,
    vectors: Vec<IndexSnapshotEntryRef<'a>>,
}

#[derive(Serialize)]
struct IndexSnapshotEntryRef<'a> {
    id: MemoryId,
    embedding: &'a [f32],
}

#[derive(Default)]
struct PersistenceCoordinator {
    /// Serialises only persistence work, never index reads or mutations.
    io_lock: Mutex<()>,
    persisted_generation: AtomicU64,
    failures: AtomicU64,
    last_error: Mutex<Option<String>>,
}

/// Immutable, cheap persistence view. Capturing it clones only `Arc` handles;
/// serialisation and SQLite I/O happen after the `VectorIndex` lock is released.
pub struct VectorPersistenceSnapshot {
    generation: u64,
    dimension: u32,
    vectors: Vec<(MemoryId, Arc<Vec<f32>>)>,
    persist_path: PathBuf,
    sqlite_store: Option<SqliteStore>,
    coordinator: Arc<PersistenceCoordinator>,
}

/// Operational counters required by Memory health/status projection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorRuntimeStats {
    pub count: usize,
    pub generation: u64,
    pub persisted_generation: u64,
    pub evictions: u64,
    pub persistence_failures: u64,
    pub last_persistence_error: Option<String>,
}

// ─── VectorIndex ─────────────────────────────────────────────────────────────

/// Lightweight in-process vector index with cosine-similarity search.
pub struct VectorIndex {
    /// All vectors keyed by memory ID.
    vectors: HashMap<MemoryId, Arc<Vec<f32>>>,
    /// Path used for [`persist`] / [`load`].
    persist_path: PathBuf,
    /// Expected dimensionality; validated on [`upsert`].
    dimension: u32,
    /// Maximum entries before LRU eviction kicks in.
    max_entries: usize,
    /// Insertion order for LRU eviction (front = oldest).
    insert_order: VecDeque<MemoryId>,
    /// Optional SQLite store for dual persistence (JSON + BLOB).
    sqlite_store: Option<SqliteStore>,
    /// Monotonic content generation. Every effective mutation advances it.
    generation: u64,
    /// Number of capacity evictions since this process loaded the index.
    evictions: u64,
    persistence: Arc<PersistenceCoordinator>,
}

impl VectorIndex {
    /// Create a new, empty index.
    ///
    /// `persist_path` does **not** have to exist yet; it will be created on the
    /// first call to [`persist`].
    pub fn new(persist_path: PathBuf, dimension: u32) -> Result<Self, MemoryError> {
        Ok(Self {
            vectors: HashMap::new(),
            persist_path,
            dimension,
            max_entries: 50_000,
            insert_order: VecDeque::new(),
            sqlite_store: None,
            generation: 0,
            evictions: 0,
            persistence: Arc::new(PersistenceCoordinator::default()),
        })
    }

    /// Attach a [`SqliteStore`] for dual persistence (SQLite BLOB + JSON file).
    ///
    /// When set, [`persist`] writes to both the JSON file and the SQLite
    /// `vector_embeddings` table.  [`load`] tries SQLite first, falling back
    /// to the JSON file if the table is empty.
    pub fn set_sqlite_store(&mut self, store: SqliteStore) {
        self.sqlite_store = Some(store);
    }

    /// Load a previously persisted index from disk.
    ///
    /// If a [`SqliteStore`] has been attached via [`set_sqlite_store`], tries to
    /// load from the `vector_embeddings` table first.  Falls back to the JSON
    /// file if the table is empty or no store is configured.
    ///
    /// Returns an empty index (with the given `dimension`) if neither source has
    /// data, making cold-start initialisation transparent.
    pub fn load(persist_path: PathBuf, dimension: u32) -> Result<Self, MemoryError> {
        Self::load_with_store(persist_path, dimension, None)
    }

    pub fn load_with_store(
        persist_path: PathBuf,
        dimension: u32,
        sqlite_store: Option<SqliteStore>,
    ) -> Result<Self, MemoryError> {
        let auto_dimension = dimension == 0;
        let mut idx = Self {
            vectors: HashMap::new(),
            persist_path,
            dimension,
            max_entries: 50_000,
            insert_order: VecDeque::new(),
            sqlite_store,
            generation: 0,
            evictions: 0,
            persistence: Arc::new(PersistenceCoordinator::default()),
        };

        if let Some(ref store) = idx.sqlite_store {
            match store.load_vectors_from_sqlite() {
                Ok(vectors) if !vectors.is_empty() => {
                    let dim = vectors.values().next().map_or(0, |v| v.len() as u32);
                    if auto_dimension {
                        idx.dimension = dim;
                    } else if dim != idx.dimension {
                        return Err(MemoryError::InvalidArgument(format!(
                            "dimension mismatch: index has {dim}, requested {}",
                            idx.dimension
                        )));
                    }
                    Self::validate_loaded_vectors(&vectors, idx.dimension)?;
                    idx.vectors = vectors
                        .into_iter()
                        .map(|(id, embedding)| (id, Arc::new(embedding)))
                        .collect();
                    idx.generation = store
                        .load_vector_generation_from_sqlite()
                        .unwrap_or_default();
                    idx.persistence
                        .persisted_generation
                        .store(idx.generation, Ordering::Release);
                    idx.restore_insert_order();
                    return Ok(idx);
                }
                Ok(_) => {
                    // Table exists but is empty — fall through to JSON.
                }
                Err(_) => {
                    // Table might not exist yet — fall through to JSON.
                }
            }
        }

        // JSON fallback
        match fs::read_to_string(&idx.persist_path) {
            Ok(json) => {
                let snap: CompatibleIndexSnapshot = serde_json::from_str(&json)
                    .map_err(|e| MemoryError::Store(format!("deserialise vector index: {e}")))?;
                let (snapshot_dimension, generation, vectors): (
                    u32,
                    u64,
                    HashMap<MemoryId, Arc<Vec<f32>>>,
                ) = match snap {
                    CompatibleIndexSnapshot::V2(snapshot) => (
                        {
                            if snapshot.schema_version != VECTOR_INDEX_SCHEMA_VERSION {
                                return Err(MemoryError::Store(format!(
                                    "unsupported vector index schema version {}",
                                    snapshot.schema_version
                                )));
                            }
                            snapshot.dimension
                        },
                        snapshot.generation,
                        snapshot
                            .vectors
                            .into_iter()
                            .map(|entry| (entry.id, Arc::new(entry.embedding)))
                            .collect(),
                    ),
                    CompatibleIndexSnapshot::Legacy(snapshot) => (
                        snapshot.dimension,
                        0,
                        snapshot
                            .vectors
                            .into_iter()
                            .map(|(id, embedding)| (id, Arc::new(embedding)))
                            .collect(),
                    ),
                };
                if auto_dimension {
                    idx.dimension = if vectors.is_empty() {
                        0
                    } else {
                        snapshot_dimension
                    };
                } else if snapshot_dimension != idx.dimension {
                    return Err(MemoryError::InvalidArgument(format!(
                        "dimension mismatch: index has {}, requested {}",
                        snapshot_dimension, idx.dimension
                    )));
                }
                Self::validate_loaded_arc_vectors(&vectors, idx.dimension)?;
                idx.generation = generation;
                idx.persistence
                    .persisted_generation
                    .store(generation, Ordering::Release);
                idx.vectors = vectors;
                idx.restore_insert_order();
                Ok(idx)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(idx),
            Err(e) => Err(MemoryError::Store(format!("read vector index: {e}"))),
        }
    }

    /// Persist the index to [`persist_path`] atomically.
    ///
    /// If a [`SqliteStore`] has been attached, also writes to the
    /// `vector_embeddings` table for dual persistence.
    ///
    /// Uses write-to-temp-then-rename to avoid corruption on interrupted writes.
    pub fn persist(&self) -> Result<(), MemoryError> {
        self.persistence_snapshot().persist()
    }

    /// Capture a persistence view without cloning embedding buffers.
    #[must_use]
    pub fn persistence_snapshot(&self) -> VectorPersistenceSnapshot {
        let vectors = self
            .vectors
            .iter()
            .map(|(id, embedding)| (*id, Arc::clone(embedding)))
            .collect::<Vec<_>>();
        VectorPersistenceSnapshot {
            generation: self.generation,
            dimension: self.dimension,
            vectors,
            persist_path: self.persist_path.clone(),
            sqlite_store: self.sqlite_store.clone(),
            coordinator: Arc::clone(&self.persistence),
        }
    }

    /// Persist vectors to the attached [`SqliteStore`] only (JSON file is not
    /// written by this method).
    pub fn persist_to_sqlite(&self) -> Result<(), MemoryError> {
        let store = self
            .sqlite_store
            .as_ref()
            .ok_or_else(|| MemoryError::Store("no SqliteStore configured".into()))?;
        let snapshot = self.persistence_snapshot();
        store.save_vector_snapshot_to_sqlite(&snapshot.vectors, self.dimension, snapshot.generation)
    }

    /// Load vectors from a [`SqliteStore`] into a new `VectorIndex`.
    ///
    /// The JSON `persist_path` is still required for the file-backed fallback.
    /// If the `vector_embeddings` table is empty, returns an empty index.
    pub fn load_from_sqlite(
        persist_path: PathBuf,
        dimension: u32,
        store: SqliteStore,
    ) -> Result<Self, MemoryError> {
        Self::load_with_store(persist_path, dimension, Some(store))
    }

    // ─── Mutation ─────────────────────────────────────────────────────────────

    /// Insert or replace the embedding for `id`.
    ///
    /// # Errors
    /// Returns [`MemoryError::InvalidArgument`] if the embedding length does not
    /// match [`dimension`].
    pub fn upsert(&mut self, id: MemoryId, embedding: Vec<f32>) -> Result<(), MemoryError> {
        if self.dimension == 0 {
            self.dimension = embedding.len() as u32;
        }
        self.check_dimension(&embedding)?;

        // Evict oldest entry if at capacity
        if self.vectors.len() >= self.max_entries && !self.vectors.contains_key(&id) {
            if let Some(oldest) = self.insert_order.pop_front() {
                self.vectors.remove(&oldest);
                self.evictions = self.evictions.saturating_add(1);
                tracing::debug!("LRU evicted {} from vector index", oldest);
            }
        }

        // Track insertion order for LRU
        if !self.vectors.contains_key(&id) {
            self.insert_order.push_back(id);
        }

        self.vectors.insert(id, Arc::new(embedding));
        self.advance_generation();
        Ok(())
    }

    /// Remove the entry for `id` (no-op if absent).
    pub fn remove(&mut self, id: &MemoryId) -> Result<(), MemoryError> {
        let removed = self.vectors.remove(id).is_some();
        // Clean up insert_order (linear scan, acceptable for VecDeque < 50K)
        if let Some(pos) = self.insert_order.iter().position(|x| x == id) {
            self.insert_order.remove(pos);
        }
        if removed {
            self.advance_generation();
        }
        Ok(())
    }

    /// Set the maximum number of entries (triggers immediate eviction if needed).
    pub fn set_max_entries(&mut self, max: usize) {
        self.max_entries = max;
        while self.vectors.len() > self.max_entries {
            if let Some(oldest) = self.insert_order.pop_front() {
                if self.vectors.remove(&oldest).is_some() {
                    self.evictions = self.evictions.saturating_add(1);
                    self.advance_generation();
                }
            } else {
                break;
            }
        }
    }

    // ─── Query ────────────────────────────────────────────────────────────────

    /// Return the `limit` nearest IDs ordered by descending cosine similarity.
    ///
    /// # Errors
    /// Returns [`MemoryError::InvalidArgument`] if `query` is empty or has the
    /// wrong length.
    pub fn search(&self, query: &[f32], limit: usize) -> Result<Vec<(MemoryId, f32)>, MemoryError> {
        self.search_with_filter(query, limit, &|_| true)
    }

    /// Like [`search`] but skips entries for which `filter` returns `false`.
    pub fn search_with_filter(
        &self,
        query: &[f32],
        limit: usize,
        filter: &dyn Fn(&MemoryId) -> bool,
    ) -> Result<Vec<(MemoryId, f32)>, MemoryError> {
        if query.is_empty() {
            return Err(MemoryError::InvalidArgument(
                "query embedding must not be empty".into(),
            ));
        }
        self.check_dimension(query)?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let query_norm = norm(query);
        let mut top = BinaryHeap::with_capacity(limit.saturating_add(1));
        for (id, embedding) in self.vectors.iter().filter(|(id, _)| filter(id)) {
            let candidate = ScoredMemory {
                id: *id,
                score: cosine_similarity(query, embedding, query_norm),
            };
            if top.len() < limit {
                top.push(Reverse(candidate));
            } else if top.peek().is_some_and(|Reverse(worst)| candidate > *worst) {
                let _ = top.pop();
                top.push(Reverse(candidate));
            }
        }
        let mut scored = top
            .into_iter()
            .map(|Reverse(candidate)| candidate)
            .collect::<Vec<_>>();
        scored.sort_unstable_by(|left, right| right.cmp(left));
        Ok(scored
            .into_iter()
            .map(|candidate| (candidate.id, candidate.score))
            .collect())
    }

    /// Return all entries whose cosine similarity to `embedding` is **greater
    /// than or equal to** `(1 - threshold)`, i.e. whose cosine *distance* is
    /// **at most** `threshold`.
    ///
    /// The default duplicate-detection threshold is `0.15` (cosine distance),
    /// which corresponds to a minimum similarity of `0.85`.
    ///
    /// # Errors
    /// Returns [`MemoryError::InvalidArgument`] for an empty / wrong-length
    /// embedding.
    pub fn find_duplicates(
        &self,
        embedding: &[f32],
        threshold: f32,
    ) -> Result<Vec<(MemoryId, f32)>, MemoryError> {
        if embedding.is_empty() {
            return Err(MemoryError::InvalidArgument(
                "embedding must not be empty".into(),
            ));
        }
        self.check_dimension(embedding)?;

        let min_similarity = 1.0 - threshold;
        let emb_norm = norm(embedding);

        let mut results: Vec<(MemoryId, f32)> = self
            .vectors
            .iter()
            .filter_map(|(id, stored)| {
                let sim = cosine_similarity(embedding, stored, emb_norm);
                if sim >= min_similarity {
                    Some((*id, sim))
                } else {
                    None
                }
            })
            .collect();

        results.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(results)
    }

    // ─── Statistics ───────────────────────────────────────────────────────────

    /// Number of entries currently in the index.
    #[must_use]
    pub fn count(&self) -> usize {
        self.vectors.len()
    }

    /// Whether a durable memory already has a semantic vector.
    #[must_use]
    pub fn contains(&self, id: &MemoryId) -> bool {
        self.vectors.contains_key(id)
    }

    /// Return a copy of one indexed embedding for background governance.
    ///
    /// The durable memory store remains authoritative; this accessor is used
    /// only to avoid a second remote embedding call when a freshly persisted
    /// heuristic atom is checked for semantic duplication.
    #[must_use]
    pub fn embedding(&self, id: &MemoryId) -> Option<Vec<f32>> {
        self.vectors
            .get(id)
            .map(|embedding| embedding.as_ref().clone())
    }

    /// Effective vector dimension, or zero while an automatic index is unbound.
    #[must_use]
    pub fn dimension(&self) -> u32 {
        self.dimension
    }

    /// Rebind an automatic index after the configured embedding model changes.
    ///
    /// Existing vectors cannot be compared across dimensions, so they are
    /// discarded and rebuilt from the durable memory store.
    pub fn reset_dimension(&mut self, dimension: u32) {
        let changed = !self.vectors.is_empty() || self.dimension != dimension;
        self.vectors.clear();
        self.insert_order.clear();
        self.dimension = dimension;
        if changed {
            self.advance_generation();
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    fn check_dimension(&self, v: &[f32]) -> Result<(), MemoryError> {
        let expected = self.dimension as usize;
        if v.len() != expected {
            return Err(MemoryError::InvalidArgument(format!(
                "dimension mismatch: expected {expected}, got {}",
                v.len()
            )));
        }
        if v.iter().any(|value| !value.is_finite()) {
            return Err(MemoryError::InvalidArgument(
                "embedding contains a non-finite value".into(),
            ));
        }
        Ok(())
    }

    fn advance_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }

    fn restore_insert_order(&mut self) {
        let mut ids = self.vectors.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        self.insert_order = ids.into();
    }

    fn validate_loaded_vectors(
        vectors: &HashMap<MemoryId, Vec<f32>>,
        dimension: u32,
    ) -> Result<(), MemoryError> {
        if vectors.len() > 50_000 {
            return Err(MemoryError::Store(format!(
                "vector index exceeds the 50000 entry limit: {}",
                vectors.len()
            )));
        }
        for embedding in vectors.values() {
            if embedding.len() != dimension as usize
                || embedding.iter().any(|value| !value.is_finite())
            {
                return Err(MemoryError::Store(
                    "vector index contains an invalid embedding".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_loaded_arc_vectors(
        vectors: &HashMap<MemoryId, Arc<Vec<f32>>>,
        dimension: u32,
    ) -> Result<(), MemoryError> {
        if vectors.len() > 50_000 {
            return Err(MemoryError::Store(format!(
                "vector index exceeds the 50000 entry limit: {}",
                vectors.len()
            )));
        }
        for embedding in vectors.values() {
            if embedding.len() != dimension as usize
                || embedding.iter().any(|value| !value.is_finite())
            {
                return Err(MemoryError::Store(
                    "vector index contains an invalid embedding".into(),
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn runtime_stats(&self) -> VectorRuntimeStats {
        VectorRuntimeStats {
            count: self.count(),
            generation: self.generation,
            persisted_generation: self
                .persistence
                .persisted_generation
                .load(Ordering::Acquire),
            evictions: self.evictions,
            persistence_failures: self.persistence.failures.load(Ordering::Relaxed),
            last_persistence_error: self.persistence.last_error.lock().clone(),
        }
    }
}

impl VectorPersistenceSnapshot {
    /// Persist the snapshot if it is newer than the durable generation.
    ///
    /// The dedicated I/O fence permits concurrent index reads and writes while
    /// preventing an older, slower snapshot from replacing a newer one.
    pub fn persist(&self) -> Result<(), MemoryError> {
        let _io_guard = self.coordinator.io_lock.lock();
        if self.generation
            <= self
                .coordinator
                .persisted_generation
                .load(Ordering::Acquire)
        {
            return Ok(());
        }
        let result = self.persist_inner();
        match &result {
            Ok(()) => {
                self.coordinator
                    .persisted_generation
                    .store(self.generation, Ordering::Release);
                *self.coordinator.last_error.lock() = None;
            }
            Err(error) => {
                self.coordinator.failures.fetch_add(1, Ordering::Relaxed);
                *self.coordinator.last_error.lock() = Some(error.to_string());
            }
        }
        result
    }

    fn persist_inner(&self) -> Result<(), MemoryError> {
        let parent = self.persist_path.parent().ok_or_else(|| {
            MemoryError::Store("vector index persistence path has no parent".into())
        })?;
        fs::create_dir_all(parent)
            .map_err(|error| MemoryError::Store(format!("create vector index dir: {error}")))?;

        let tmp = self.persist_path.with_extension(format!(
            "{}.{}.tmp",
            self.persist_path
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("json"),
            self.generation
        ));
        let write_result = (|| -> Result<(), MemoryError> {
            let file = fs::File::create(&tmp)
                .map_err(|error| MemoryError::Store(format!("create tmp index file: {error}")))?;
            let mut writer = BufWriter::new(file);
            let mut vectors = self
                .vectors
                .iter()
                .map(|(id, embedding)| IndexSnapshotEntryRef {
                    id: *id,
                    embedding: embedding.as_slice(),
                })
                .collect::<Vec<_>>();
            vectors.sort_unstable_by_key(|entry| entry.id);
            serde_json::to_writer(
                &mut writer,
                &IndexSnapshotRef {
                    schema_version: VECTOR_INDEX_SCHEMA_VERSION,
                    generation: self.generation,
                    dimension: self.dimension,
                    vectors,
                },
            )
            .map_err(|error| MemoryError::Store(format!("serialise vector index: {error}")))?;
            writer
                .flush()
                .map_err(|error| MemoryError::Store(format!("flush tmp index file: {error}")))?;
            writer
                .get_ref()
                .sync_all()
                .map_err(|error| MemoryError::Store(format!("fsync tmp index file: {error}")))?;

            // SQLite is updated transactionally before the canonical JSON
            // replace. Any failure therefore leaves the old JSON intact.
            if let Some(store) = &self.sqlite_store {
                store.save_vector_snapshot_to_sqlite(
                    &self.vectors,
                    self.dimension,
                    self.generation,
                )?;
            }
            fs::rename(&tmp, &self.persist_path)
                .map_err(|error| MemoryError::Store(format!("replace vector index: {error}")))?;
            fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| MemoryError::Store(format!("fsync vector index dir: {error}")))?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        write_result
    }

    #[cfg(test)]
    fn temp_path(&self) -> PathBuf {
        self.persist_path.with_extension(format!(
            "{}.{}.tmp",
            self.persist_path
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("json"),
            self.generation
        ))
    }
}

#[derive(Clone, Copy, Debug)]
struct ScoredMemory {
    id: MemoryId,
    score: f32,
}

impl PartialEq for ScoredMemory {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == CmpOrdering::Equal && self.id == other.id
    }
}

impl Eq for ScoredMemory {}

impl PartialOrd for ScoredMemory {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredMemory {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .total_cmp(&other.score)
            // With equal scores, the lower UUID is the deterministic winner.
            .then_with(|| other.id.cmp(&self.id))
    }
}

// ─── Math helpers ─────────────────────────────────────────────────────────────

#[inline]
fn norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// Cosine similarity given a pre-computed norm for `a`.
#[inline]
fn cosine_similarity(a: &[f32], b: &[f32], norm_a: f32) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_b = norm(b);
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::{sync::Arc, thread, time::Instant};
    use tempfile::TempDir;

    fn make_index(dim: u32) -> (VectorIndex, TempDir) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("vector_index.json");
        let idx = VectorIndex::new(path, dim).unwrap();
        (idx, tmp)
    }

    #[test]
    fn upsert_and_search() {
        let (mut idx, _tmp) = make_index(3);
        let id1 = MemoryId::new_v4();
        let id2 = MemoryId::new_v4();

        idx.upsert(id1, vec![1.0, 0.0, 0.0]).unwrap();
        idx.upsert(id2, vec![0.0, 1.0, 0.0]).unwrap();

        let results = idx.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results[0].0, id1);
        assert!((results[0].1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn dimension_mismatch_is_rejected() {
        let (mut idx, _tmp) = make_index(3);
        let id = MemoryId::new_v4();
        let err = idx.upsert(id, vec![1.0, 0.0]).unwrap_err();
        assert!(matches!(err, MemoryError::InvalidArgument(_)));
    }

    #[test]
    fn non_finite_vectors_are_rejected() {
        let (mut index, _tmp) = make_index(2);
        assert!(index
            .upsert(MemoryId::from_u128(1), vec![f32::NAN, 1.0])
            .is_err());
        index
            .upsert(MemoryId::from_u128(1), vec![1.0, 0.0])
            .unwrap();
        assert!(index.search(&[f32::INFINITY, 0.0], 1).is_err());
    }

    #[test]
    fn remove_entry() {
        let (mut idx, _tmp) = make_index(2);
        let id = MemoryId::new_v4();
        idx.upsert(id, vec![1.0, 0.0]).unwrap();
        assert_eq!(idx.count(), 1);
        idx.remove(&id).unwrap();
        assert_eq!(idx.count(), 0);
    }

    #[test]
    fn persist_and_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("idx.json");
        let id = MemoryId::new_v4();

        {
            let mut idx = VectorIndex::new(path.clone(), 2).unwrap();
            idx.upsert(id, vec![0.6, 0.8]).unwrap();
            idx.persist().unwrap();
        }

        let idx2 = VectorIndex::load(path, 2).unwrap();
        assert_eq!(idx2.count(), 1);
        let results = idx2.search(&[0.6, 0.8], 1).unwrap();
        assert_eq!(results[0].0, id);
    }

    #[test]
    fn load_zero_dimension_reuses_persisted_json_dimension() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("idx.json");
        let id = MemoryId::new_v4();

        {
            let mut idx = VectorIndex::new(path.clone(), 1024).unwrap();
            idx.upsert(id, vec![0.25; 1024]).unwrap();
            idx.persist().unwrap();
        }

        let idx2 = VectorIndex::load(path, 0).unwrap();
        assert_eq!(idx2.dimension, 1024);
        assert_eq!(idx2.count(), 1);
    }

    #[test]
    fn load_zero_dimension_uses_default_for_empty_index() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("idx.json");

        let mut idx = VectorIndex::load(path, 0).unwrap();
        assert_eq!(idx.dimension, 0);
        assert_eq!(idx.count(), 0);
        idx.upsert(MemoryId::new_v4(), vec![0.25; 1024]).unwrap();
        assert_eq!(idx.dimension, 1024);
    }

    #[test]
    fn find_duplicates_detects_similar() {
        let (mut idx, _tmp) = make_index(3);
        let id = MemoryId::new_v4();
        idx.upsert(id, vec![1.0, 0.01, 0.0]).unwrap();

        // Very similar vector — should be found at threshold 0.15.
        let dups = idx.find_duplicates(&[1.0, 0.0, 0.0], 0.15).unwrap();
        assert!(!dups.is_empty());
        assert_eq!(dups[0].0, id);
        assert_eq!(idx.embedding(&id), Some(vec![1.0, 0.01, 0.0]));
    }

    #[test]
    fn search_with_filter_excludes_entries() {
        let (mut idx, _tmp) = make_index(2);
        let id1 = MemoryId::new_v4();
        let id2 = MemoryId::new_v4();
        idx.upsert(id1, vec![1.0, 0.0]).unwrap();
        idx.upsert(id2, vec![1.0, 0.0]).unwrap();

        // Exclude id1 explicitly.
        let results = idx
            .search_with_filter(&[1.0, 0.0], 10, &|id| *id != id1)
            .unwrap();
        assert!(results.iter().all(|(id, _)| *id != id1));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id2);
    }

    fn reference_full_sort(
        index: &VectorIndex,
        query: &[f32],
        limit: usize,
    ) -> Vec<(MemoryId, f32)> {
        let query_norm = norm(query);
        let mut values = index
            .vectors
            .iter()
            .map(|(id, embedding)| (*id, cosine_similarity(query, embedding, query_norm)))
            .collect::<Vec<_>>();
        values.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        values.truncate(limit);
        values
    }

    #[test]
    fn bounded_top_k_matches_reference_at_50k() {
        let (mut index, _tmp) = make_index(16);
        for ordinal in 1..=50_000_u128 {
            let embedding = (0..16)
                .map(|axis| {
                    (((ordinal as u64)
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .rotate_left(axis as u32)
                        % 20_003) as f32
                        / 10_001.5)
                        - 1.0
                })
                .collect();
            index
                .upsert(MemoryId::from_u128(ordinal), embedding)
                .unwrap();
        }
        let mut bounded_micros = Vec::new();
        let mut reference_micros = Vec::new();
        for query_ordinal in 0..40 {
            let query = (0..16)
                .map(|axis| (((axis * 37 + query_ordinal * 19) % 101) as f32 / 50.0) - 1.0)
                .collect::<Vec<_>>();
            let started = Instant::now();
            let bounded = index.search(&query, 32).unwrap();
            bounded_micros.push(started.elapsed().as_micros());
            let started = Instant::now();
            let reference = reference_full_sort(&index, &query, 32);
            reference_micros.push(started.elapsed().as_micros());
            assert_eq!(bounded, reference);
        }
        bounded_micros.sort_unstable();
        reference_micros.sort_unstable();
        eprintln!(
            "R4_TOPK_50K bounded_p50_us={} bounded_p95_us={} bounded_p99_us={} full_sort_p50_us={} full_sort_p95_us={} full_sort_p99_us={} entries=50000 k=32 samples=40",
            bounded_micros[19],
            bounded_micros[37],
            bounded_micros[39],
            reference_micros[19],
            reference_micros[37],
            reference_micros[39],
        );
    }

    #[test]
    fn equal_scores_use_memory_id_as_stable_tie_break() {
        let (mut index, _tmp) = make_index(2);
        let higher = MemoryId::from_u128(2);
        let lower = MemoryId::from_u128(1);
        index.upsert(higher, vec![1.0, 0.0]).unwrap();
        index.upsert(lower, vec![1.0, 0.0]).unwrap();
        let results = index.search(&[1.0, 0.0], 2).unwrap();
        assert_eq!(results, vec![(lower, 1.0), (higher, 1.0)]);
    }

    #[test]
    fn generation_fence_rejects_stale_snapshot() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("idx.json");
        let first = MemoryId::from_u128(1);
        let second = MemoryId::from_u128(2);
        let mut index = VectorIndex::new(path.clone(), 2).unwrap();
        index.upsert(first, vec![1.0, 0.0]).unwrap();
        let stale = index.persistence_snapshot();
        index.upsert(second, vec![0.0, 1.0]).unwrap();
        let current = index.persistence_snapshot();
        current.persist().unwrap();
        stale.persist().unwrap();

        let restored = VectorIndex::load(path, 2).unwrap();
        assert_eq!(restored.count(), 2);
        assert_eq!(restored.runtime_stats().generation, 2);
    }

    #[test]
    fn failed_snapshot_preserves_previous_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("idx.json");
        let mut index = VectorIndex::new(path.clone(), 2).unwrap();
        index
            .upsert(MemoryId::from_u128(1), vec![1.0, 0.0])
            .unwrap();
        index.persist().unwrap();
        let previous = fs::read(&path).unwrap();

        index
            .upsert(MemoryId::from_u128(2), vec![0.0, 1.0])
            .unwrap();
        let snapshot = index.persistence_snapshot();
        fs::create_dir(snapshot.temp_path()).unwrap();
        assert!(snapshot.persist().is_err());
        assert_eq!(fs::read(&path).unwrap(), previous);
        assert_eq!(index.runtime_stats().persistence_failures, 1);
    }

    #[test]
    fn rwlock_allows_overlapping_readers_and_a_writer() {
        let (mut index, _tmp) = make_index(8);
        for ordinal in 1..=2_000_u128 {
            index
                .upsert(MemoryId::from_u128(ordinal), vec![ordinal as f32; 8])
                .unwrap();
        }
        let index = Arc::new(RwLock::new(index));
        let readers = (0..8)
            .map(|_| {
                let index = Arc::clone(&index);
                thread::spawn(move || {
                    for _ in 0..20 {
                        assert_eq!(index.read().search(&[1.0; 8], 8).unwrap().len(), 8);
                    }
                })
            })
            .collect::<Vec<_>>();
        let writer_index = Arc::clone(&index);
        let writer = thread::spawn(move || {
            for ordinal in 2_001..=2_050_u128 {
                writer_index
                    .write()
                    .upsert(MemoryId::from_u128(ordinal), vec![ordinal as f32; 8])
                    .unwrap();
            }
        });
        for reader in readers {
            reader.join().unwrap();
        }
        writer.join().unwrap();
        assert_eq!(index.read().count(), 2_050);
    }

    #[test]
    fn sqlite_snapshot_over_parameter_limit_remains_complete() {
        let tmp = TempDir::new().unwrap();
        let store = SqliteStore::open_path(&tmp.path().join("memory.db")).unwrap();
        let path = tmp.path().join("idx.json");
        let mut index = VectorIndex::new(path.clone(), 2).unwrap();
        index.set_sqlite_store(store.clone());
        for ordinal in 1..=1_200_u128 {
            index
                .upsert(MemoryId::from_u128(ordinal), vec![ordinal as f32, 1.0])
                .unwrap();
        }
        index.persist().unwrap();
        assert_eq!(store.load_vectors_from_sqlite().unwrap().len(), 1_200);
        for ordinal in 1..=100_u128 {
            index.remove(&MemoryId::from_u128(ordinal)).unwrap();
        }
        index.persist().unwrap();
        assert_eq!(store.load_vectors_from_sqlite().unwrap().len(), 1_100);
        let restored = VectorIndex::load_with_store(path, 2, Some(store)).unwrap();
        assert_eq!(restored.count(), 1_100);
        assert_eq!(restored.runtime_stats().generation, 1_300);
        assert_eq!(restored.runtime_stats().persisted_generation, 1_300);
    }
}
