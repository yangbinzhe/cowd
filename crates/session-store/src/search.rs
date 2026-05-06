use rusqlite::params;

use crate::error::SessionStoreError;
use crate::store::SessionStore;

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub session_id: String,
    pub message_id: i64,
    pub content_snippet: String,
    pub role: String,
    pub timestamp: f64,
    pub session_title: Option<String>,
}

fn prepare_fts_query(query: &str) -> String {
    query
        .split_whitespace()
        .filter(|word| !word.is_empty())
        .map(|word| {
            let escaped = word.replace('"', "\"\"");
            format!("\"{escaped}\"")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn row_to_search_result(row: &rusqlite::Row) -> rusqlite::Result<SearchResult> {
    Ok(SearchResult {
        session_id: row.get(0)?,
        message_id: row.get(1)?,
        content_snippet: row.get::<_, String>(2).unwrap_or_default(),
        role: row.get(3)?,
        timestamp: row.get(4)?,
        session_title: row.get(5)?,
    })
}

impl SessionStore {
    pub fn search_messages(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>, SessionStoreError> {
        let fts_query = prepare_fts_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }
        let sql = "SELECT m.session_id, m.id, snippet(messages_fts, 2, '<b>', '</b>', '...', 40) as snip,
                          m.role, m.timestamp, s.title as session_title
                   FROM messages_fts fts
                   JOIN messages m ON m.id = fts.rowid
                   LEFT JOIN sessions s ON s.id = m.session_id
                   WHERE messages_fts MATCH ?1
                   ORDER BY rank LIMIT ?2";
        let mut stmt = self.conn().prepare(sql)?;
        let rows = stmt.query_map(params![fts_query, limit as i64], row_to_search_result)?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn search_in_session(&self, session_id: &str, query: &str, limit: usize) -> Result<Vec<SearchResult>, SessionStoreError> {
        let fts_query = prepare_fts_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }
        let sql = "SELECT m.session_id, m.id, snippet(messages_fts, 2, '<b>', '</b>', '...', 40) as snip,
                          m.role, m.timestamp, s.title as session_title
                   FROM messages_fts fts
                   JOIN messages m ON m.id = fts.rowid
                   LEFT JOIN sessions s ON s.id = m.session_id
                   WHERE messages_fts MATCH ?1 AND m.session_id = ?2
                   ORDER BY rank LIMIT ?3";
        let mut stmt = self.conn().prepare(sql)?;
        let rows = stmt.query_map(params![fts_query, session_id, limit as i64], row_to_search_result)?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}
