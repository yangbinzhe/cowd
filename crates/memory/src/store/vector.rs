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
    collections::HashMap,
    fs,
    io::Write,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

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
        })
    }

    /// Load a previously persisted index from disk.
    ///
    /// Returns an empty index (with the given `dimension`) if the file does not
    /// exist, making cold-start initialisation transparent.
    pub fn load(persist_path: PathBuf, dimension: u32) -> Result<Self, MemoryError> {
        match fs::read_to_string(&persist_path) {
            Ok(json) => {
                let snap: IndexSnapshot = serde_json::from_str(&json)
                    .map_err(|e| MemoryError::Store(format!("deserialise vector index: {e}")))?;
                if snap.dimension != dimension {
                    return Err(MemoryError::InvalidArgument(format!(
                        "dimension mismatch: index has {}, requested {}",
                        snap.dimension, dimension
                    )));
                }
                Ok(Self {
                    vectors: snap.vectors,
                    persist_path,
                    dimension,
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Self::new(persist_path, dimension)
            }
            Err(e) => Err(MemoryError::Store(format!("read vector index: {e}"))),
        }
    }

    /// Persist the index to [`persist_path`] atomically.
    ///
    /// Uses write-to-temp-then-rename to avoid corruption on interrupted writes.
    pub fn persist(&self) -> Result<(), MemoryError> {
        // Ensure the parent directory exists.
        if let Some(parent) = self.persist_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                MemoryError::Store(format!("create vector index dir: {e}"))
            })?;
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

    // ─── Mutation ─────────────────────────────────────────────────────────────

    /// Insert or replace the embedding for `id`.
    ///
    /// # Errors
    /// Returns [`MemoryError::InvalidArgument`] if the embedding length does not
    /// match [`dimension`].
    pub fn upsert(&mut self, id: MemoryId, embedding: Vec<f32>) -> Result<(), MemoryError> {
        self.check_dimension(&embedding)?;
        self.vectors.insert(id, embedding);
        Ok(())
    }

    /// Remove the entry for `id` (no-op if absent).
    pub fn remove(&mut self, id: &MemoryId) -> Result<(), MemoryError> {
        self.vectors.remove(id);
        Ok(())
    }

    // ─── Query ────────────────────────────────────────────────────────────────

    /// Return the `limit` nearest IDs ordered by descending cosine similarity.
    ///
    /// # Errors
    /// Returns [`MemoryError::InvalidArgument`] if `query` is empty or has the
    /// wrong length.
    pub fn search(
        &self,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<(MemoryId, f32)>, MemoryError> {
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
    fn find_duplicates_detects_similar() {
        let (mut idx, _tmp) = make_index(3);
        let id = MemoryId::new_v4();
        idx.upsert(id, vec![1.0, 0.01, 0.0]).unwrap();

        // Very similar vector — should be found at threshold 0.15.
        let dups = idx
            .find_duplicates(&[1.0, 0.0, 0.0], 0.15)
            .unwrap();
        assert!(!dups.is_empty());
        assert_eq!(dups[0].0, id);
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
