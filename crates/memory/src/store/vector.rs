//! In-process lightweight vector index for approximate nearest-neighbour search.
//!
//! The index stores all vectors in a `HashMap` keyed by [`MemoryId`] and uses a
//! brute-force cosine-similarity scan, which is perfectly adequate for the
//! expected corpus size (<10 000 entries).
//!
//! ## Persistence
//! The index is serialised to / deserialised from a JSON file so that it
//! survives process restarts without an external vector database.
//!
//! ```json
//! {
//!   "dimension": 1536,
//!   "vectors": {
//!     "<uuid>": [0.1, -0.3, …],
//!     …
//!   }
//! }
//! ```

use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::Write,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::store::sqlite::SqliteStore;
use crate::{error::MemoryError, types::MemoryId};

// ─── Serialisation envelope ───────────────────────────────────────────────────

/// On-disk representation of the vector index.
#[derive(Serialize, Deserialize)]
struct IndexSnapshot {
    dimension: u32,
    vectors: HashMap<MemoryId, Vec<f32>>,
}

// ─── VectorIndex ─────────────────────────────────────────────────────────────

/// Lightweight in-process vector index with cosine-similarity search.
pub struct VectorIndex {
    /// All vectors keyed by memory ID.
    vectors: HashMap<MemoryId, Vec<f32>>,
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
                    idx.vectors = vectors;
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
                let snap: IndexSnapshot = serde_json::from_str(&json)
                    .map_err(|e| MemoryError::Store(format!("deserialise vector index: {e}")))?;
                if auto_dimension {
                    idx.dimension = if snap.vectors.is_empty() {
                        0
                    } else {
                        snap.dimension
                    };
                } else if snap.dimension != idx.dimension {
                    return Err(MemoryError::InvalidArgument(format!(
                        "dimension mismatch: index has {}, requested {}",
                        snap.dimension, idx.dimension
                    )));
                }
                idx.vectors = snap.vectors;
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
        // Persist to JSON (always).
        self.persist_json()?;

        // Persist to SQLite (if configured).
        if let Some(ref store) = self.sqlite_store {
            store.save_vectors_to_sqlite(&self.vectors, self.dimension)?;
        }
        Ok(())
    }

    fn persist_json(&self) -> Result<(), MemoryError> {
        if let Some(parent) = self.persist_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| MemoryError::Store(format!("create vector index dir: {e}")))?;
        }

        let snap = IndexSnapshot {
            dimension: self.dimension,
            vectors: self.vectors.clone(),
        };
        let json = serde_json::to_string(&snap)
            .map_err(|e| MemoryError::Store(format!("serialise vector index: {e}")))?;

        let tmp = self.persist_path.with_extension("json.tmp");
        {
            let mut f = fs::File::create(&tmp)
                .map_err(|e| MemoryError::Store(format!("create tmp index file: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| MemoryError::Store(format!("write tmp index file: {e}")))?;
            f.flush()
                .map_err(|e| MemoryError::Store(format!("flush tmp index file: {e}")))?;
        }
        fs::rename(&tmp, &self.persist_path)
            .map_err(|e| MemoryError::Store(format!("rename tmp index: {e}")))?;
        Ok(())
    }

    /// Persist vectors to the attached [`SqliteStore`] only (JSON file is not
    /// written by this method).
    pub fn persist_to_sqlite(&self) -> Result<(), MemoryError> {
        let store = self
            .sqlite_store
            .as_ref()
            .ok_or_else(|| MemoryError::Store("no SqliteStore configured".into()))?;
        store.save_vectors_to_sqlite(&self.vectors, self.dimension)
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
                tracing::debug!("LRU evicted {} from vector index", oldest);
            }
        }

        // Track insertion order for LRU
        if !self.vectors.contains_key(&id) {
            self.insert_order.push_back(id);
        }

        self.vectors.insert(id, embedding);
        Ok(())
    }

    /// Remove the entry for `id` (no-op if absent).
    pub fn remove(&mut self, id: &MemoryId) -> Result<(), MemoryError> {
        self.vectors.remove(id);
        // Clean up insert_order (linear scan, acceptable for VecDeque < 50K)
        if let Some(pos) = self.insert_order.iter().position(|x| x == id) {
            self.insert_order.remove(pos);
        }
        Ok(())
    }

    /// Set the maximum number of entries (triggers immediate eviction if needed).
    pub fn set_max_entries(&mut self, max: usize) {
        self.max_entries = max;
        while self.vectors.len() > self.max_entries {
            if let Some(oldest) = self.insert_order.pop_front() {
                self.vectors.remove(&oldest);
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

        let query_norm = norm(query);
        let mut scored: Vec<(MemoryId, f32)> = self
            .vectors
            .iter()
            .filter(|(id, _)| filter(id))
            .map(|(id, emb)| (*id, cosine_similarity(query, emb, query_norm)))
            .collect();

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);
        Ok(scored)
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

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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
        self.vectors.get(id).cloned()
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
        self.vectors.clear();
        self.insert_order.clear();
        self.dimension = dimension;
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
        Ok(())
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
}
