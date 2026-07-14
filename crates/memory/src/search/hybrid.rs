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
use parking_lot::Mutex;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

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
    /// Cached BM25 index + document hash, protected by interior mutability.
    cache: Mutex<Option<(BM25Scorer, u64)>>,
}

impl Default for HybridSearcher {
    fn default() -> Self {
        Self::new(0.6, 0.4)
    }
}

impl HybridSearcher {
    pub fn new(vector_weight: f64, bm25_weight: f64) -> Self {
        Self {
            vector_weight,
            bm25_weight,
            over_fetch_factor: 3,
            cache: Mutex::new(None),
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

        // Step 2: BM25 candidates — use cached index when documents haven't changed
        let doc_hash = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            all_documents.hash(&mut hasher);
            hasher.finish()
        };

        let bm25_rankings = {
            let mut cache = self.cache.lock();
            let rebuild = match cache.as_ref() {
                Some((_, cached_hash)) if *cached_hash == doc_hash => false,
                _ => true,
            };
            if rebuild {
                let scorer = BM25Scorer::default_params(all_documents);
                *cache = Some((scorer, doc_hash));
            }
            cache
                .as_ref()
                .map_or_else(Vec::new, |(scorer, _)| scorer.rank(query))
        };
        let bm25_candidates: Vec<_> = bm25_rankings.into_iter().take(over_fetch).collect();

        // Step 3: Merge candidate sets
        // id → (vector_score, bm25_score, content)
        let mut candidates: HashMap<MemoryId, (Option<f64>, Option<f64>, String)> = HashMap::new();

        for (id, content, score) in &vector_candidates {
            candidates.insert(id.clone(), (Some(*score), None, content.clone()));
        }

        for (doc_idx, bm25_score) in &bm25_candidates {
            if let Some(id) = doc_id_map.get(*doc_idx) {
                let content = all_documents.get(*doc_idx).cloned().unwrap_or_default();
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
        let bm25_max = bm25_scores
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);

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

    /// Reciprocal Rank Fusion (RRF) – fuses porter and trigram result lists.
    ///
    /// Each result list contributes `1/(K + rank)` to the fused score.
    /// Documents appearing in both lists receive boosted scores.
    /// K=60 follows the Cormack et al. 2009 standard.
    ///
    /// If only one result set is provided (the other is empty), returns
    /// that set as-is with its original ordering preserved.
    pub fn search_rrf(
        &self,
        _query: &str,
        porter_results: &[(MemoryId, f64)],
        trigram_results: &[(MemoryId, f64)],
    ) -> Vec<SearchResult> {
        const K: f64 = 60.0;

        if porter_results.is_empty() && trigram_results.is_empty() {
            return Vec::new();
        }

        // Single-list fallback: return as-is
        if trigram_results.is_empty() {
            return porter_results
                .iter()
                .map(|(id, score)| SearchResult {
                    id: id.clone(),
                    content: String::new(),
                    vector_score: 0.0,
                    bm25_score: 0.0,
                    hybrid_score: *score,
                    source: "porter".to_string(),
                })
                .collect();
        }
        if porter_results.is_empty() {
            return trigram_results
                .iter()
                .map(|(id, score)| SearchResult {
                    id: id.clone(),
                    content: String::new(),
                    vector_score: 0.0,
                    bm25_score: 0.0,
                    hybrid_score: *score,
                    source: "trigram".to_string(),
                })
                .collect();
        }

        // RRF fusion: accumulate 1/(K + rank + 1) from each list
        let mut scores: HashMap<MemoryId, f64> = HashMap::new();
        for (rank, (idx, _score)) in porter_results.iter().enumerate() {
            *scores.entry(idx.clone()).or_default() += 1.0 / (K + rank as f64 + 1.0);
        }
        for (rank, (idx, _score)) in trigram_results.iter().enumerate() {
            *scores.entry(idx.clone()).or_default() += 1.0 / (K + rank as f64 + 1.0);
        }

        let mut fused: Vec<SearchResult> = scores
            .into_iter()
            .map(|(id, rrf_score)| SearchResult {
                id,
                content: String::new(),
                vector_score: 0.0,
                bm25_score: 0.0,
                hybrid_score: rrf_score,
                source: "rrf".to_string(),
            })
            .collect();

        fused.sort_by(|a, b| {
            b.hybrid_score
                .partial_cmp(&a.hybrid_score)
                .unwrap_or(Ordering::Equal)
        });

        fused
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
        let doc_ids: Vec<MemoryId> =
            vec!["id_0".to_string(), "id_1".to_string(), "id_2".to_string()];

        // Simulate vector results (Rust docs rank high)
        let vector_results = vec![
            (
                "id_0".to_string(),
                "Rust programming language".to_string(),
                0.95,
            ),
            (
                "id_2".to_string(),
                "Rust web development Axum".to_string(),
                0.85,
            ),
            ("id_1".to_string(), "Python data science".to_string(), 0.3),
        ];

        let searcher = HybridSearcher::new(0.6, 0.4);
        let results = searcher.search("Rust programming", vector_results, &docs, &doc_ids, 3);

        assert!(!results.is_empty());
        // Results that have at least one positive contribution should score > 0
        let positive_count = results.iter().filter(|r| r.hybrid_score > 0.0).count();
        assert!(
            positive_count > 0,
            "at least one result should have a positive hybrid score"
        );
        for r in &results {
            assert!(r.hybrid_score >= 0.0);
        }
    }

    #[test]
    fn test_hybrid_search_empty_docs() {
        let searcher = HybridSearcher::new(0.6, 0.4);
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

    // --- RRF tests ---

    #[test]
    fn test_rrf_fusion_combines_ranks() {
        // doc_a: rank 0 in porter, rank 2 in trigram
        // doc_b: rank 1 in porter, rank 0 in trigram  → highest RRF (both lists near top)
        // doc_c: rank 2 in porter only
        // doc_d: rank 1 in trigram only
        let porter = vec![
            ("doc_a".to_string(), 0.95),
            ("doc_b".to_string(), 0.80),
            ("doc_c".to_string(), 0.60),
        ];
        let trigram = vec![
            ("doc_b".to_string(), 0.90),
            ("doc_d".to_string(), 0.70),
            ("doc_a".to_string(), 0.50),
        ];

        let searcher = HybridSearcher::new(0.6, 0.4);
        let results = searcher.search_rrf("test query", &porter, &trigram);

        assert_eq!(results.len(), 4, "should fuse to 4 unique documents");

        // doc_b: 1/(60+2) + 1/(60+1) = 1/62 + 1/61 ≈ 0.03252
        // doc_a: 1/(60+1) + 1/(60+3) = 1/61 + 1/63 ≈ 0.03226
        assert_eq!(
            results[0].id, "doc_b",
            "doc_b appears near top of both lists"
        );
        assert_eq!(
            results[1].id, "doc_a",
            "doc_a appears in both lists, rank 0 + rank 2"
        );

        assert!(results[0].hybrid_score > results[1].hybrid_score);
        assert!(results[1].hybrid_score > results[2].hybrid_score);
        assert!(results[2].hybrid_score > results[3].hybrid_score);

        // All fused results should have source "rrf"
        for r in &results {
            assert_eq!(r.source, "rrf");
            assert!(r.hybrid_score > 0.0);
        }
    }

    #[test]
    fn test_rrf_single_list_fallback_porter() {
        let porter = vec![("doc_a".to_string(), 0.95), ("doc_b".to_string(), 0.80)];

        let searcher = HybridSearcher::new(0.6, 0.4);
        let results = searcher.search_rrf("test", &porter, &[]);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].source, "porter");
        assert_eq!(results[1].source, "porter");
        // Original ordering preserved
        assert_eq!(results[0].id, "doc_a");
        assert_eq!(results[1].id, "doc_b");
        // Original scores preserved
        assert!((results[0].hybrid_score - 0.95).abs() < 1e-10);
        assert!((results[1].hybrid_score - 0.80).abs() < 1e-10);
    }

    #[test]
    fn test_rrf_single_list_fallback_trigram() {
        let trigram = vec![("doc_x".to_string(), 0.88)];

        let searcher = HybridSearcher::new(0.6, 0.4);
        let results = searcher.search_rrf("test", &[], &trigram);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].source, "trigram");
        assert_eq!(results[0].id, "doc_x");
        assert!((results[0].hybrid_score - 0.88).abs() < 1e-10);
    }

    #[test]
    fn test_rrf_empty_input() {
        let searcher = HybridSearcher::new(0.6, 0.4);
        let results = searcher.search_rrf("test", &[], &[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_rrf_boost_for_overlapping_docs() {
        // Single doc in both lists gets score from both ranks (boosted)
        let porter = vec![("doc_z".to_string(), 0.99)];
        let trigram = vec![("doc_z".to_string(), 0.99)];

        let searcher = HybridSearcher::new(0.6, 0.4);
        let results = searcher.search_rrf("test", &porter, &trigram);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "doc_z");
        assert_eq!(results[0].source, "rrf");
        // RRF score: 1/(60+1) + 1/(60+1) = 2/61 ≈ 0.03279
        let expected = 1.0 / (60.0 + 1.0) + 1.0 / (60.0 + 1.0);
        assert!((results[0].hybrid_score - expected).abs() < 1e-10);
    }

    #[test]
    fn bm25_index_cached_across_searches() {
        let searcher = HybridSearcher::new(0.5, 0.5);
        let docs = vec!["hello world".to_string(), "foo bar".to_string()];
        let doc_ids: Vec<MemoryId> = vec!["id_0".to_string(), "id_1".to_string()];

        // First search builds BM25 index
        searcher.search("hello", vec![], &docs, &doc_ids, 5);
        // Second search with same docs should use cache (no rebuild)
        searcher.search("bar", vec![], &docs, &doc_ids, 5);
        // Verify cache exists
        assert!(
            searcher.cache.lock().is_some(),
            "BM25 index should be cached after first search"
        );
    }
}
