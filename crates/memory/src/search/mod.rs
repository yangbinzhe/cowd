//! Search module – BM25 keyword search and hybrid retrieval.
//!
//! Provides Okapi BM25+ scoring (inspired by MemPalace `searcher.py`)
//! and a hybrid retrieval strategy that convex-combines vector similarity
//! with BM25 keyword scores.

pub mod bm25;
pub mod hybrid;
pub mod semantic_bridge;

pub use bm25::BM25Scorer;
pub use hybrid::{HybridSearcher, SearchResult};
pub use semantic_bridge::semantic_query_variants;
