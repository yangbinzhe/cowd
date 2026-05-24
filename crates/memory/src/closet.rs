//! Closet: Compact pointer-row index layer for fast topic routing.
//!
//! Borrowed from MemPalace closet concept: short pointer rows that route
//! queries to relevant "drawers" (memory entries) without reading full data.
//! Ranking boost applied based on closet position (top position = highest boost).

use serde::{Deserialize, Serialize};

/// Closet rank boosts (borrowed from MemPalace: [0.40, 0.25, 0.15, 0.08, 0.04]).
pub const RANK_BOOSTS: [f64; 5] = [0.40, 0.25, 0.15, 0.08, 0.04];

/// Maximum characters per pointer row (borrowed from MemPalace: 1500).
pub const CHAR_LIMIT: usize = 1500;

/// Character window for extraction (borrowed from MemPalace: 5000).
pub const EXTRACT_WINDOW: usize = 5000;

/// What kind of entity a closet pointer references.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PointerKind {
    /// Points to a text memory entry.
    Memory,
    /// Points to a code symbol.
    CodeSymbol,
}

/// Identifier for a code symbol in the closet.
pub type CodeSymbolId = String;

/// A single pointer row in the closet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosetPointer {
    /// Topic keyword or phrase.
    pub topic: String,
    /// Associated entity names.
    pub entities: Vec<String>,
    /// IDs of memory entries ("drawers") this pointer references.
    pub drawer_ids: Vec<String>,
    /// Relevance score for ranking.
    pub relevance_score: f64,
    /// What kind of entity this pointer references.
    pub kind: PointerKind,
    /// Display name of the code symbol (when kind is CodeSymbol).
    pub symbol_name: Option<String>,
    /// Unique identifier for the code symbol (when kind is CodeSymbol).
    pub symbol_id: Option<CodeSymbolId>,
}

/// The Closet index: a collection of pointer rows for fast topic routing.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Closet {
    pub pointers: Vec<ClosetPointer>,
}

impl Closet {
    /// Build a closet from memory entries.
    /// Each entry's keywords become topics; entries become drawer references.
    pub fn build(entries: &[ClosetEntry]) -> Self {
        let mut pointers: Vec<ClosetPointer> = Vec::new();

        for entry in entries {
            let keywords = extract_keywords(&entry.content);
            for kw in keywords {
                // Check if topic already exists
                if let Some(ptr) = pointers.iter_mut().find(|p| p.topic == kw) {
                    if !ptr.drawer_ids.contains(&entry.id) {
                        ptr.drawer_ids.push(entry.id.clone());
                    }
                    // Add entities from the entry
                    for e in &entry.entities {
                        if !ptr.entities.contains(e) {
                            ptr.entities.push(e.clone());
                        }
                    }
                    ptr.relevance_score += 0.1;
                } else {
                    let ptr = ClosetPointer {
                        topic: kw,
                        entities: entry.entities.clone(),
                        drawer_ids: vec![entry.id.clone()],
                        relevance_score: 0.5,
                        kind: PointerKind::Memory,
                        symbol_name: None,
                        symbol_id: None,
                    };
                    pointers.push(ptr);
                }
            }
        }

        // Sort by relevance (descending)
        pointers.sort_by(|a, b| b.relevance_score.partial_cmp(&a.relevance_score).unwrap_or(std::cmp::Ordering::Equal));

        // Apply char limit
        let mut total_chars = 0usize;
        pointers.retain(|p| {
            let row_len = p.topic.len() + p.entities.join(",").len() + p.drawer_ids.join(",").len();
            if total_chars + row_len <= CHAR_LIMIT {
                total_chars += row_len;
                true
            } else {
                false
            }
        });

        Self { pointers }
    }

    /// Search the closet for drawer IDs matching the query, with rank boosts.
    pub fn search(&self, query: &str, top_n: usize) -> Vec<(String, f64)> {
        let query_lower = query.to_lowercase();
        let query_tokens: Vec<&str> = query_lower.split_whitespace().collect();

        let mut scored: Vec<(String, f64)> = Vec::new();

        for (position, ptr) in self.pointers.iter().enumerate() {
            let topic_lower = ptr.topic.to_lowercase();

            // Check if any query token matches the topic or entities
            let matches = query_tokens.iter().any(|t| topic_lower.contains(t))
                || ptr.entities.iter().any(|e| {
                    let e_lower = e.to_lowercase();
                    query_tokens.iter().any(|t| e_lower.contains(t))
                });

            if matches {
                let boost = RANK_BOOSTS.get(position).copied().unwrap_or(0.01);
                for drawer_id in &ptr.drawer_ids {
                    // Find existing or add new
                    if let Some(existing) = scored.iter_mut().find(|(id, _)| id == drawer_id) {
                        existing.1 = existing.1.max(ptr.relevance_score + boost);
                    } else {
                        scored.push((drawer_id.clone(), ptr.relevance_score + boost));
                    }
                }
            }
        }

        // Sort by score descending
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_n);
        scored
    }

    /// Get the number of pointer rows.
    pub fn len(&self) -> usize {
        self.pointers.len()
    }

    /// Check if the closet is empty.
    pub fn is_empty(&self) -> bool {
        self.pointers.is_empty()
    }
}

/// Minimal entry representation for closet building.
#[derive(Debug, Clone)]
pub struct ClosetEntry {
    pub id: String,
    /// Human-readable title (used for display).
    pub title: String,
    /// Full text content (used for keyword extraction).
    pub content: String,
    /// Named entities associated with this entry.
    pub entities: Vec<String>,
}

// ─── ClosetManager ───────────────────────────────────────────────────────────

use crate::orchestrator::MemoryOrchestrator;
use crate::types::MemoryLayer;

/// High-level manager that builds a [`Closet`] from the memory orchestrator
/// and provides topic-oriented query methods.
///
/// # Example
///
/// ```rust,ignore
/// let manager = ClosetManager::build_from_orchestrator(&orchestrator).await?;
/// for topic in manager.list_topics() {
///     println!("{} ({} drawers)", topic.topic, topic.drawer_ids.len());
/// }
/// ```
pub struct ClosetManager {
    closet: Closet,
}

impl ClosetManager {
    /// Build a [`Closet`] index from L2 (project) and L3 (deep) memory layers.
    ///
    /// Loads the **full content** of each entry so that keyword extraction
    /// operates on meaningful text rather than bare metadata.
    pub async fn build_from_orchestrator(
        orchestrator: &MemoryOrchestrator,
    ) -> Result<Self, crate::error::MemoryError> {
        let l2_metas = orchestrator.list_layer(MemoryLayer::L2).await?;
        let l3_metas = orchestrator.list_layer(MemoryLayer::L3).await?;

        let mut entries: Vec<ClosetEntry> = Vec::new();
        for meta in l2_metas.into_iter().chain(l3_metas) {
            let Some(full_entry) = orchestrator.recall(&meta.id).await? else {
                continue;
            };
            entries.push(ClosetEntry {
                id: meta.id.to_string(),
                title: meta.title.clone(),
                content: full_entry.content,
                entities: meta.tags,
            });
        }

        Ok(Self {
            closet: Closet::build(&entries),
        })
    }

    /// Create a [`ClosetManager`] from an already-built [`Closet`].
    #[must_use]
    pub fn from_closet(closet: Closet) -> Self {
        Self { closet }
    }

    /// List all topic pointers ordered by relevance score (descending).
    #[must_use]
    pub fn list_topics(&self) -> Vec<&ClosetPointer> {
        self.closet.pointers.iter().collect()
    }

    /// Get pointers whose topic or entities match `query` (case-insensitive
    /// substring). Results are sorted by relevance score.
    #[must_use]
    pub fn search_topics(&self, query: &str) -> Vec<&ClosetPointer> {
        let q = query.to_lowercase();
        let mut matched: Vec<&ClosetPointer> = self
            .closet
            .pointers
            .iter()
            .filter(|p| {
                p.topic.to_lowercase().contains(&q)
                    || p.entities.iter().any(|e| e.to_lowercase().contains(&q))
            })
            .collect();
        matched.sort_by(|a, b| {
            b.relevance_score
                .partial_cmp(&a.relevance_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        matched
    }

    /// Get all pointers for a specific topic (case-insensitive substring match).
    #[must_use]
    pub fn get_pointers_for_topic(&self, topic: &str) -> Vec<&ClosetPointer> {
        let t = topic.to_lowercase();
        let mut matched: Vec<&ClosetPointer> = self
            .closet
            .pointers
            .iter()
            .filter(|p| p.topic.to_lowercase().contains(&t))
            .collect();
        matched.sort_by(|a, b| {
            b.relevance_score
                .partial_cmp(&a.relevance_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        matched
    }

    /// Add a code-symbol pointer to the closet.
    ///
    /// The pointer is boosted by the given `boost` amount so that frequently
    /// referenced symbols rise to the top of search results.
    pub fn add_code_pointer(&mut self, symbol_name: &str, drawer_key: &str, boost: f32) {
        let topic = symbol_name.to_lowercase();
        let boost_f64 = f64::from(boost);

        // Check if a pointer for this symbol already exists.
        if let Some(ptr) = self.closet.pointers.iter_mut().find(|p| {
            p.kind == PointerKind::CodeSymbol && p.symbol_name.as_deref() == Some(symbol_name)
        }) {
            ptr.relevance_score += boost_f64;
            return;
        }

        // Create a new code-symbol pointer.
        let ptr = ClosetPointer {
            topic: topic.clone(),
            entities: vec![symbol_name.to_string()],
            drawer_ids: vec![drawer_key.to_string()],
            relevance_score: f64::from(boost),
            kind: PointerKind::CodeSymbol,
            symbol_name: Some(symbol_name.to_string()),
            symbol_id: Some(symbol_name.to_string()),
        };

        self.closet.pointers.push(ptr);
        // Sort by relevance
        self.closet.pointers.sort_by(|a, b| {
            b.relevance_score
                .partial_cmp(&a.relevance_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Look up a code symbol by name in the closet.
    ///
    /// Returns the symbol ID if a matching code-symbol pointer exists.
    #[must_use]
    pub fn lookup_code_symbol(&self, name: &str) -> Option<CodeSymbolId> {
        self.closet
            .pointers
            .iter()
            .find(|p| {
                p.kind == PointerKind::CodeSymbol
                    && p.symbol_name.as_deref() == Some(name)
            })
            .and_then(|p| p.symbol_id.clone())
    }

    /// Return the total number of pointer rows in the closet.
    #[must_use]
    pub fn topic_count(&self) -> usize {
        self.closet.pointers.len()
    }

    /// Return `true` if the closet contains no pointers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.closet.is_empty()
    }

    /// Get a reference to the inner [`Closet`].
    #[must_use]
    pub fn closet(&self) -> &Closet {
        &self.closet
    }
}

/// Extract keyword tokens from content for closet indexing.
fn extract_keywords(content: &str) -> Vec<String> {
    let stop_words: &[&str] = &[
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "shall", "can", "to", "of", "in", "for",
        "on", "with", "at", "by", "from", "as", "into", "through", "during",
        "before", "after", "above", "below", "between", "out", "off", "over",
        "under", "again", "further", "then", "once", "and", "but", "or", "nor",
        "not", "so", "yet", "both", "either", "neither", "each", "every",
        "all", "any", "few", "more", "most", "other", "some", "such", "no",
        "only", "own", "same", "than", "too", "very", "just", "because",
        "this", "that", "these", "those", "it", "its", "i", "me", "my",
        "we", "us", "our", "you", "your", "he", "him", "his", "she", "her",
        "they", "them", "their", "what", "which", "who", "whom", "how",
        "when", "where", "why", "if", "about", "up", "there", "also",
        "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都",
        "一", "一个", "上", "也", "很", "到", "说", "要", "去", "你",
        "会", "着", "没有", "看", "好", "自己", "这",
    ];

    let lower = content.to_lowercase();
    let tokens: Vec<String> = lower
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty() && s.len() >= 2)
        .map(|s| s.to_string())
        .filter(|s| !stop_words.contains(&s.as_str()))
        .take(20)
        .collect();

    // Deduplicate
    let mut seen = std::collections::HashSet::new();
    tokens
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_closet_build_and_search() {
        let entries = vec![
            ClosetEntry {
                id: "e1".to_string(),
                title: "React Frontend".to_string(),
                content: "Using React framework for the frontend project".to_string(),
                entities: vec!["React".to_string()],
            },
            ClosetEntry {
                id: "e2".to_string(),
                title: "Rust Backend".to_string(),
                content: "Rust backend with Axum web framework".to_string(),
                entities: vec!["Rust".to_string(), "Axum".to_string()],
            },
        ];

        let closet = Closet::build(&entries);
        assert!(!closet.is_empty());

        let results = closet.search("React", 5);
        assert!(!results.is_empty());
        assert!(results[0].0 == "e1");
    }

    #[test]
    fn test_closet_rank_boost() {
        let entries = vec![
            ClosetEntry {
                id: "e1".to_string(),
                title: "React Components".to_string(),
                content: "React frontend components".to_string(),
                entities: vec!["React".to_string()],
            },
            ClosetEntry {
                id: "e2".to_string(),
                title: "React State".to_string(),
                content: "React state management patterns".to_string(),
                entities: vec!["React".to_string()],
            },
        ];

        let closet = Closet::build(&entries);
        let results = closet.search("React", 5);

        // Top results should have higher boosts
        if results.len() >= 2 {
            assert!(results[0].1 >= results[1].1);
        }
    }

    #[test]
    fn test_extract_keywords() {
        let kws = extract_keywords("Using React framework for the frontend project");
        assert!(kws.contains(&"react".to_string()));
        assert!(kws.contains(&"framework".to_string()));
        assert!(!kws.contains(&"the".to_string())); // stop word
    }

    // -----------------------------------------------------------------------
    // T6: Hot symbol tracking — code pointer tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_add_code_pointer_and_lookup() {
        let mut manager = ClosetManager::from_closet(Closet::default());

        manager.add_code_pointer("authenticate_user", "src/auth.rs:10", 0.8);
        manager.add_code_pointer("TokenManager", "src/auth.rs:25", 0.6);

        assert_eq!(manager.topic_count(), 2);

        let lookup = manager.lookup_code_symbol("authenticate_user");
        assert!(lookup.is_some());
        assert_eq!(lookup.unwrap(), "authenticate_user");

        let missing = manager.lookup_code_symbol("nonexistent_fn");
        assert!(missing.is_none());
    }

    #[test]
    fn test_hot_symbol_promotion() {
        let mut manager = ClosetManager::from_closet(Closet::default());

        // Add a symbol with initial boost
        manager.add_code_pointer("handle_request", "src/server.rs:42", 0.5);

        // Verify initial score
        let ptrs = manager.search_topics("handle_request");
        assert_eq!(ptrs.len(), 1);
        let initial_score = ptrs[0].relevance_score;

        // Boost the same symbol again (simulating frequent reference)
        manager.add_code_pointer("handle_request", "src/server.rs:42", 0.4);

        let ptrs2 = manager.search_topics("handle_request");
        assert_eq!(ptrs2.len(), 1);
        assert!(
            ptrs2[0].relevance_score > initial_score,
            "score should increase after boost"
        );
    }

    #[test]
    fn test_quick_lookup() {
        let mut manager = ClosetManager::from_closet(Closet::default());

        manager.add_code_pointer("UserService", "src/services.rs:15", 0.9);
        manager.add_code_pointer("handle_auth", "src/auth.rs:30", 0.5);

        // lookup_code_symbol should return the exact match
        assert_eq!(
            manager.lookup_code_symbol("UserService"),
            Some("UserService".to_string())
        );
        assert_eq!(
            manager.lookup_code_symbol("handle_auth"),
            Some("handle_auth".to_string())
        );

        // Closet pointers should have CodeSymbol kind
        let ptrs = manager.search_topics("UserService");
        assert_eq!(ptrs[0].kind, PointerKind::CodeSymbol);
        assert_eq!(ptrs[0].symbol_name.as_deref(), Some("UserService"));
    }
}
