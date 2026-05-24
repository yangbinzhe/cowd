//! BM25-based session resume – ranks entries by relevance to session context.
//!
//! Provides a lightweight `SessionResume` struct that indexes memory entries
//! with BM25 and can retrieve the most relevant ones for a given session,
//! falling back to FTS5 when the BM25 index is empty.

use crate::error::MemoryError;
use crate::search::BM25Scorer;
use crate::store::MemoryStore;
use crate::types::MemoryEntry;

/// Lightweight BM25-based session resumer.
///
/// Builds an internal BM25 index from all provided entries and can rank
/// session-tagged entries by their relevance to the session context.
pub struct SessionResume {
    entries: Vec<MemoryEntry>,
    bm25: BM25Scorer,
}

impl SessionResume {
    /// Build a resumer from a collection of memory entries.
    ///
    /// Each entry's content is tokenized and indexed for BM25 scoring.
    #[must_use]
    pub fn new(entries: Vec<MemoryEntry>) -> Self {
        let docs: Vec<String> = entries.iter().map(|e| e.content.clone()).collect();
        let bm25 = BM25Scorer::default_params(&docs);
        Self { entries, bm25 }
    }

    /// Rank entries by relevance to a session.
    ///
    /// Filters entries whose `tags` contain `session_id`, builds a query from
    /// their titles, ranks all indexed entries via BM25, and returns the top
    /// `limit` results.
    ///
    /// Falls back to FTS5 (via `store`) when the BM25 index has no documents.
    pub async fn resume_recent(
        &self,
        session_id: &str,
        store: Option<&dyn MemoryStore>,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, MemoryError> {
        if self.bm25.is_empty() {
            if let Some(store) = store {
                return store.search_fts(session_id, limit).await;
            }
            return Ok(Vec::new());
        }

        let session_titles: Vec<&str> = self
            .entries
            .iter()
            .filter(|e| e.tags.iter().any(|t| t == session_id))
            .map(|e| e.title.as_str())
            .collect();

        if session_titles.is_empty() {
            return Ok(Vec::new());
        }

        let query = session_titles.join(" ");
        let ranked = self.bm25.rank(&query);

        let results: Vec<MemoryEntry> = ranked
            .into_iter()
            .take(limit)
            .filter_map(|(idx, _)| self.entries.get(idx).cloned())
            .collect();

        Ok(results)
    }

    /// Number of entries indexed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MemoryCategory, MemoryLayer, Priority, MemorySource};
    use crate::MemoryScope;
    use uuid::Uuid;

    fn make_entry(id: &str, title: &str, content: &str, tags: Vec<&str>) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::parse_str(id).unwrap(),
            layer: MemoryLayer::L2,
            category: MemoryCategory::ProjectConvention,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: tags.into_iter().map(|s| s.to_string()).collect(),
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            scope: MemoryScope::default(),
            session_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        }
    }

    #[test]
    fn test_resume_ranks_session_entries() {
        let entries = vec![
            make_entry(
                "00000000-0000-0000-0000-000000000001",
                "Rust async patterns",
                "Using tokio for async Rust programming with async/await",
                vec!["session-1"],
            ),
            make_entry(
                "00000000-0000-0000-0000-000000000002",
                "Python data science",
                "Machine learning with Python and scikit-learn",
                vec!["session-2"],
            ),
            make_entry(
                "00000000-0000-0000-0000-000000000003",
                "Rust error handling",
                "Error handling patterns in Rust using Result and thiserror",
                vec!["session-1"],
            ),
        ];

        let resume = SessionResume::new(entries.clone());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let results = rt.block_on(resume.resume_recent("session-1", None, 5)).unwrap();

        assert!(!results.is_empty());
        // Session-1 entries should rank highest
        let ids: Vec<String> = results.iter().map(|e| e.id.to_string()).collect();
        assert!(ids.contains(&"00000000-0000-0000-0000-000000000001".to_string()));
        assert!(ids.contains(&"00000000-0000-0000-0000-000000000003".to_string()));
    }

    #[test]
    fn test_resume_unknown_session_returns_empty() {
        let entries = vec![make_entry(
            "00000000-0000-0000-0000-000000000001",
            "Test",
            "Some content",
            vec!["session-1"],
        )];

        let resume = SessionResume::new(entries);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let results = rt.block_on(resume.resume_recent("nonexistent", None, 5)).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_resume_empty_index_returns_empty() {
        let resume = SessionResume::new(vec![]);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let results = rt.block_on(resume.resume_recent("session-1", None, 5)).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_resume_respects_limit() {
        let mut entries = Vec::new();
        for i in 0..10u32 {
            let id = Uuid::new_v4();
            entries.push(make_entry(
                &id.to_string(),
                &format!("Entry {i}"),
                &format!("Content for entry number {i}"),
                vec!["session-1"],
            ));
        }

        let resume = SessionResume::new(entries);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let results = rt.block_on(resume.resume_recent("session-1", None, 3)).unwrap();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_len_and_is_empty() {
        let entries = vec![make_entry(
            "00000000-0000-0000-0000-000000000001",
            "Test",
            "Content",
            vec![],
        )];
        let resume = SessionResume::new(entries);
        assert_eq!(resume.len(), 1);
        assert!(!resume.is_empty());

        let empty = SessionResume::new(vec![]);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }
}
