//! Lightweight semantic bridge for local recall.
//!
//! This is not a replacement for embeddings. It is a deterministic fallback
//! that expands a small set of high-value concept families when FTS/vector
//! search returns too little. The goal is to keep recall useful in offline
//! mode without adding remote latency or model dependencies.

const CONCEPT_CLUSTERS: &[(&[&str], &[&str])] = &[
    (
        &["machine", "vision", "visual", "image", "inference"],
        &[
            "neural network",
            "convolutional",
            "pixel",
            "classification",
            "pattern detection",
        ],
    ),
    (
        &[
            "auth",
            "authentication",
            "login",
            "credential",
            "credentials",
        ],
        &[
            "authenticate",
            "authorization",
            "token",
            "session",
            "middleware",
        ],
    ),
    (
        &["database", "db", "sql", "sqlite", "postgres"],
        &["query", "index", "transaction", "migration", "storage"],
    ),
    (
        &["context", "prompt", "cache", "kv"],
        &[
            "stable head",
            "runtime header",
            "dynamic tail",
            "token budget",
        ],
    ),
    (
        &["agent", "collaboration", "delegate", "delegation"],
        &["peer", "sub agent", "workgraph", "evidence", "conflict"],
    ),
];

pub fn semantic_query_variants(query: &str, max_variants: usize) -> Vec<String> {
    let normalized = normalize(query);
    if normalized.is_empty() || max_variants == 0 {
        return Vec::new();
    }

    let mut variants = Vec::new();
    for (triggers, expansions) in CONCEPT_CLUSTERS {
        if triggers
            .iter()
            .any(|trigger| normalized.split_whitespace().any(|term| term == *trigger))
        {
            for expansion in *expansions {
                if variants.len() >= max_variants {
                    return variants;
                }
                if !normalized.contains(expansion) && !variants.iter().any(|v| v == expansion) {
                    variants.push((*expansion).to_string());
                }
            }
        }
    }
    variants
}

fn normalize(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_bridge_expands_machine_vision_query() {
        let variants = semantic_query_variants("machine vision inference", 8);

        assert!(variants.contains(&"neural network".to_string()));
        assert!(variants.contains(&"classification".to_string()));
        assert!(variants.len() <= 8);
    }

    #[test]
    fn semantic_bridge_is_bounded_and_empty_for_unknown_terms() {
        assert!(semantic_query_variants("zzzz unknown", 4).is_empty());
        assert_eq!(semantic_query_variants("auth login database", 3).len(), 3);
    }
}
