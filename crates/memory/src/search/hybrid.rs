//! Hybrid searcher – convex combination of vector similarity and BM25 scores.
//!
//! Strategy (borrowed from MemPalace):
//! 1. Vector search → top-n*3 candidates
//! 2. BM25 search → top-n*3 candidates
//! 3. Merge candidate sets
//! 4. Min-max normalise each score type to [0, 1]
//! 5. Convex combine: hybrid = 0.6 * vector + 0.4 * BM25
//! 6. Re-rank by hybrid score
//! 7. Return top-n

use crate::search::bm25::BM25Scorer;
use serde::Serialize;
use std::collections::HashMap;

/// Identifier for memory entries in search results.
pub type MemoryId = String;

/// A single search result with per-method scores.
#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub id: MemoryId,
    pub content: String,
    /// Normalised vector similarity score [0, 1].
    pub vector_score: f64,
    /// Normalised BM25 keyword score [0, 1].
    pub bm25_score: f64,
    /// Combined hybrid score.
    pub hybrid_score: f64,
    /// Which methods contributed: "vector", "bm25", or "hybrid".
    pub source: String,
}

/// Hybrid retrieval combining vector similarity with BM25 keyword ranking.
pub struct HybridSearcher {
    /// Weight for vector similarity (default 0.6).
    pub vector_weight: f64,
    /// Weight for BM25 keyword score (default 0.4).
    pub bm25_weight: f64,
    /// Over-fetch multiplier (default 3, borrowed from MemPalace).
    pub over_fetch_factor: usize,
}

impl Default for HybridSearcher {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridSearcher {
    pub fn new() -> Self {
        Self {
            vector_weight: 0.6,
            bm25_weight: 0.4,
            over_fetch_factor: 3,
        }
    }

    /// Run hybrid search combining vector and BM25 results.
    ///
    /// # Arguments
    /// * `query` - The search query string
    /// * `vector_results` - Pre-computed vector search results as (id, content, similarity)
    /// * `all_documents` - All document contents for BM25 index construction
    /// * `doc_id_map` - Mapping from BM25 document index to MemoryId
    /// * `n_results` - Number of final results to return
    pub fn search(
        &self,
        query: &str,
        vector_results: Vec<(MemoryId, String, f64)>,
        all_documents: &[String],
        doc_id_map: &[MemoryId],
        n_results: usize,
    ) -> Vec<SearchResult> {
        if all_documents.is_empty() {
            return Vec::new();
        }

        let over_fetch = n_results * self.over_fetch_factor;

        // Step 1: Vector candidates (take over_fetch from the provided results)
        let vector_candidates: Vec<_> = vector_results.into_iter().take(over_fetch).collect();

        // Step 2: BM25 candidates
        let bm25 = BM25Scorer::default_params(all_documents);
        let bm25_rankings = bm25.rank(query);
        let bm25_candidates: Vec<_> = bm25_rankings.into_iter().take(over_fetch).collect();

        // Step 3: Merge candidate sets
        // id → (vector_score, bm25_score, content)
        let mut candidates: HashMap<MemoryId, (Option<f64>, Option<f64>, String)> = HashMap::new();

        for (id, content, score) in &vector_candidates {
            candidates.insert(
                id.clone(),
                (Some(*score), None, content.clone()),
            );
        }

        for (doc_idx, bm25_score) in &bm25_candidates {
            if let Some(id) = doc_id_map.get(*doc_idx) {
                let content = all_documents
                    .get(*doc_idx)
                    .cloned()
                    .unwrap_or_default();
                candidates
                    .entry(id.clone())
                    .and_modify(|(_v, b, c)| {
                        *b = Some(*bm25_score);
                        if c.is_empty() {
                            *c = content.clone();
                        }
                    })
                    .or_insert((None, Some(*bm25_score), content));
            }
        }

        if candidates.is_empty() {
            return Vec::new();
        }

        // Step 4: Min-Max normalisation
        let vec_scores: Vec<f64> = candidates.values().filter_map(|(v, _, _)| *v).collect();
        let bm25_scores: Vec<f64> = candidates.values().filter_map(|(_, b, _)| *b).collect();

        let vec_min = vec_scores.iter().cloned().fold(f64::INFINITY, f64::min);
        let vec_max = vec_scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let bm25_min = bm25_scores.iter().cloned().fold(f64::INFINITY, f64::min);
        let bm25_max = bm25_scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        // Step 5: Convex combination
        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .map(|(id, (vec_s, bm25_s, content))| {
                let normalised_vec = normalise(vec_s, vec_min, vec_max);
                let normalised_bm25 = normalise(bm25_s, bm25_min, bm25_max);

                // When only one method returned a result, use that score directly
                let hybrid = if vec_s.is_some() && bm25_s.is_some() {
                    self.vector_weight * normalised_vec + self.bm25_weight * normalised_bm25
                } else if vec_s.is_some() {
                    normalised_vec
                } else {
                    normalised_bm25
                };

                let source = if vec_s.is_some() && bm25_s.is_some() {
                    "hybrid"
                } else if vec_s.is_some() {
                    "vector"
                } else {
                    "bm25"
                };

                SearchResult {
                    id,
                    content,
                    vector_score: normalised_vec,
                    bm25_score: normalised_bm25,
                    hybrid_score: hybrid,
                    source: source.to_string(),
                }
            })
            .collect();

        // Step 6: Sort by hybrid score descending
        results.sort_by(|a, b| {
            b.hybrid_score
                .partial_cmp(&a.hybrid_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Step 7: Return top-n
        results.into_iter().take(n_results).collect()
    }
}

/// Min-Max normalisation to [0, 1].
fn normalise(value: Option<f64>, min: f64, max: f64) -> f64 {
    match value {
        Some(v) if (max - min).abs() > f64::EPSILON => (v - min) / (max - min),
        Some(_) => 1.0,
        None => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hybrid_search_combines_scores() {
        let docs = vec![
            "Rust programming language".to_string(),
            "Python data science".to_string(),
            "Rust web development Axum".to_string(),
        ];
        let doc_ids: Vec<MemoryId> = vec![
            "id_0".to_string(),
            "id_1".to_string(),
            "id_2".to_string(),
        ];

        // Simulate vector results (Rust docs rank high)
        let vector_results = vec![
            ("id_0".to_string(), "Rust programming language".to_string(), 0.95),
            ("id_2".to_string(), "Rust web development Axum".to_string(), 0.85),
            ("id_1".to_string(), "Python data science".to_string(), 0.3),
        ];

        let searcher = HybridSearcher::new();
        let results = searcher.search("Rust programming", vector_results, &docs, &doc_ids, 3);

        assert!(!results.is_empty());
        // Results that have at least one positive contribution should score > 0
        let positive_count = results.iter().filter(|r| r.hybrid_score > 0.0).count();
        assert!(positive_count > 0, "at least one result should have a positive hybrid score");
        for r in &results {
            assert!(r.hybrid_score >= 0.0);
        }
    }

    #[test]
    fn test_hybrid_search_empty_docs() {
        let searcher = HybridSearcher::new();
        let results = searcher.search("test", vec![], &[], &[], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_normalise() {
        assert_eq!(normalise(Some(0.5), 0.0, 1.0), 0.5);
        assert_eq!(normalise(Some(0.0), 0.0, 1.0), 0.0);
        assert_eq!(normalise(Some(1.0), 0.0, 1.0), 1.0);
        assert_eq!(normalise(None, 0.0, 1.0), 0.0);
        // When min == max, return 1.0 for present values
        assert_eq!(normalise(Some(5.0), 5.0, 5.0), 1.0);
    }
}
