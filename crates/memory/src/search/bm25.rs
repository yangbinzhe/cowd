//! Okapi BM25+ scorer – keyword-based document ranking.
//!
//! Parameters (k1=1.5, b=0.75) borrowed from MemPalace `searcher.py`.
//! Uses BM25+ smoothed IDF to avoid negative scores for very common terms.

use std::collections::HashMap;

/// Okapi BM25+ scorer for keyword-based document ranking.
pub struct BM25Scorer {
    k1: f64,
    b: f64,
    avgdl: f64,
    n_docs: usize,
    /// Document frequency: term → number of docs containing the term.
    df: HashMap<String, usize>,
    /// Document length: doc_index → token count.
    doc_lengths: Vec<usize>,
    /// Per-document term frequency: doc_index → { term → tf }.
    doc_tf: Vec<HashMap<String, usize>>,
    /// Original document content.
    documents: Vec<String>,
}

impl BM25Scorer {
    /// Build a BM25 index from a document collection.
    pub fn new(documents: &[String], k1: f64, b: f64) -> Self {
        let n_docs = documents.len();
        let mut df: HashMap<String, usize> = HashMap::new();
        let mut doc_lengths = Vec::with_capacity(n_docs);
        let mut doc_tf = Vec::with_capacity(n_docs);

        for doc in documents {
            let tokens = tokenize(doc);
            doc_lengths.push(tokens.len());

            let mut tf: HashMap<String, usize> = HashMap::new();
            let mut seen_terms: HashMap<String, bool> = HashMap::new();

            for token in &tokens {
                *tf.entry(token.clone()).or_insert(0) += 1;
                // DF: each term counted once per document
                if !seen_terms.contains_key(token) {
                    *df.entry(token.clone()).or_insert(0) += 1;
                    seen_terms.insert(token.clone(), true);
                }
            }
            doc_tf.push(tf);
        }

        let avgdl = if n_docs > 0 {
            doc_lengths.iter().sum::<usize>() as f64 / n_docs as f64
        } else {
            0.0
        };

        Self {
            k1,
            b,
            avgdl,
            n_docs,
            df,
            doc_lengths,
            doc_tf,
            documents: documents.to_vec(),
        }
    }

    /// Construct with default parameters (k1=1.5, b=0.75), matching MemPalace.
    pub fn default_params(documents: &[String]) -> Self {
        Self::new(documents, 1.5, 0.75)
    }

    /// Compute the BM25 score of a query against a single document.
    pub fn score(&self, query: &str, doc_index: usize) -> f64 {
        if doc_index >= self.n_docs {
            return 0.0;
        }
        let query_tokens = tokenize(query);
        let doc_len = self.doc_lengths[doc_index] as f64;
        let tf_map = &self.doc_tf[doc_index];

        let mut total_score = 0.0;
        for term in &query_tokens {
            let tf = *tf_map.get(term).unwrap_or(&0) as f64;
            let df_val = *self.df.get(term).unwrap_or(&0) as f64;

            // BM25+ smoothed IDF (avoids negative IDF)
            let idf = ((self.n_docs as f64 - df_val + 0.5) / (df_val + 0.5) + 1.0).ln();

            // TF component with length normalisation
            let numerator = tf * (self.k1 + 1.0);
            let denominator = tf + self.k1 * (1.0 - self.b + self.b * (doc_len / self.avgdl));

            if denominator > 0.0 {
                total_score += idf * (numerator / denominator);
            }
        }

        total_score
    }

    /// Rank all documents by BM25 score for the given query.
    /// Returns (doc_index, score) pairs sorted by descending score, excluding zeros.
    pub fn rank(&self, query: &str) -> Vec<(usize, f64)> {
        let mut scores: Vec<(usize, f64)> = (0..self.n_docs)
            .map(|i| (i, self.score(query, i)))
            .filter(|(_, s)| *s > 0.0)
            .collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores
    }

    /// Incrementally add a document without rebuilding the entire index.
    pub fn add_document(&mut self, doc: &str) {
        let tokens = tokenize(doc);
        let doc_len = tokens.len();

        // Update avgdl
        let total_len = self.avgdl * self.n_docs as f64 + doc_len as f64;
        self.n_docs += 1;
        self.avgdl = total_len / self.n_docs as f64;

        let mut tf: HashMap<String, usize> = HashMap::new();
        let mut seen: HashMap<String, bool> = HashMap::new();
        for token in &tokens {
            *tf.entry(token.clone()).or_insert(0) += 1;
            if !seen.contains_key(token) {
                *self.df.entry(token.clone()).or_insert(0) += 1;
                seen.insert(token.clone(), true);
            }
        }

        self.doc_lengths.push(doc_len);
        self.doc_tf.push(tf);
        self.documents.push(doc.to_string());
    }

    /// Get the number of documents in the index.
    pub fn len(&self) -> usize {
        self.n_docs
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.n_docs == 0
    }

    /// Get a reference to a document by index.
    pub fn get_document(&self, index: usize) -> Option<&str> {
        self.documents.get(index).map(|s| s.as_str())
    }
}

/// Tokenize text: lowercase, split on non-alphanumeric, filter short tokens and stop words.
pub fn tokenize(text: &str) -> Vec<String> {
    const STOP_WORDS: &[&str] = &[
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "shall", "can", "need", "dare", "ought",
        "used", "to", "of", "in", "for", "on", "with", "at", "by", "from",
        "as", "into", "through", "during", "before", "after", "above", "below",
        "between", "out", "off", "over", "under", "again", "further", "then",
        "once", "here", "there", "when", "where", "why", "how", "all", "both",
        "each", "few", "more", "most", "other", "some", "such", "no", "nor",
        "not", "only", "own", "same", "so", "than", "too", "very", "just",
        "because", "but", "and", "or", "if", "while", "although", "this",
        "that", "these", "those", "it", "its", "i", "me", "my", "we", "our",
        "you", "your", "he", "him", "his", "she", "her", "they", "them",
        "their", "what", "which", "who", "whom",
    ];

    let lower = text.to_lowercase();
    let tokens: Vec<String> = lower
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| s.len() >= 2)
        .filter(|s| !STOP_WORDS.contains(s))
        .map(|s| s.to_string())
        .collect();
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_basic_ranking() {
        let docs = vec![
            "Rust programming language for systems".to_string(),
            "Python machine learning and data science".to_string(),
            "Rust web framework Axum tutorial".to_string(),
            "Machine learning with Rust and WASM".to_string(),
        ];
        let scorer = BM25Scorer::default_params(&docs);

        let results = scorer.rank("Rust programming");
        assert!(!results.is_empty());
        // Documents containing Rust should rank first
        assert!(
            results[0].0 == 0 || results[0].0 == 2 || results[0].0 == 3,
            "Expected Rust-containing doc at top, got index {}",
            results[0].0
        );
    }

    #[test]
    fn test_bm25_exact_match_boost() {
        let docs = vec![
            "general discussion about code".to_string(),
            "how to implement BM25 search algorithm".to_string(),
        ];
        let scorer = BM25Scorer::default_params(&docs);
        let results = scorer.rank("BM25 search algorithm");
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 1, "Exact match document should rank first");
    }

    #[test]
    fn test_tokenize_stop_words() {
        let tokens = tokenize("The quick brown fox is running to the store");
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
        assert!(!tokens.contains(&"is".to_string()));
    }

    #[test]
    fn test_add_document_incremental() {
        let docs = vec!["first document".to_string()];
        let mut scorer = BM25Scorer::default_params(&docs);
        assert_eq!(scorer.len(), 1);

        scorer.add_document("second document about Rust");
        assert_eq!(scorer.len(), 2);

        let results = scorer.rank("Rust");
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 1, "Added document with 'Rust' should rank first");
    }

    #[test]
    fn test_empty_query() {
        let docs = vec!["some document".to_string()];
        let scorer = BM25Scorer::default_params(&docs);
        let results = scorer.rank("");
        assert!(results.is_empty() || results.iter().all(|(_, s)| *s == 0.0));
    }
}
