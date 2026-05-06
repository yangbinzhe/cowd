//! Tool output sandbox — in-memory FTS5 index + summary replacement.
//!
//! When a tool output exceeds a configurable token threshold, the output is
//! split into chunks and indexed in an in-memory SQLite FTS5 table. A compact
//! summary replaces the raw output in the conversation context, and the model
//! can later retrieve specific chunks via `/sandbox-search`.
//!
//! Inspired by context-mode's "batch→index→search→inject" pipeline.

use rusqlite::{params, Connection};
use std::collections::HashMap;

/// A snippet of indexed tool output returned by a search query.
#[derive(Debug, Clone)]
pub struct SearchSnippet {
    /// Starting line number (1-based).
    pub line_start: usize,
    /// Ending line number (1-based, inclusive).
    pub line_end: usize,
    /// The matching chunk content.
    pub content: String,
}

/// Summary generated when a large tool output is sandboxed.
#[derive(Debug, Clone)]
pub struct ToolOutputSummary {
    /// Total size of the original output in bytes.
    pub full_size_bytes: usize,
    /// Total number of lines in the original output.
    pub total_lines: usize,
    /// First few lines of the output.
    pub sample_head: String,
    /// Last few lines of the output.
    pub sample_tail: String,
    /// Frequently occurring keywords extracted from the output.
    pub keyword_highlights: Vec<String>,
    /// Hint telling the model how to search within the sandbox.
    pub search_hint: String,
}

/// In-memory FTS5 sandbox for large tool outputs.
///
/// Each [`ConversationRuntime`] instance owns one sandbox. Indexed outputs
/// are automatically discarded when the runtime is dropped.
pub struct ToolOutputSandbox {
    conn: Connection,
}

impl ToolOutputSandbox {
    /// Create a new sandbox with an in-memory SQLite connection and an FTS5
    /// virtual table.
    ///
    /// # Errors
    ///
    /// Returns `rusqlite::Error` if the connection or table creation fails.
    pub fn new() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS tool_output_fts \
             USING fts5(call_id, line_range, content, tokenize='porter unicode61');",
        )?;
        Ok(Self { conn })
    }

    /// Index a tool output.
    ///
    /// If the estimated token count of `output` is below `threshold_tokens`,
    /// returns `None` (the output is small enough to keep in context).
    /// Otherwise the output is chunked (50 lines per chunk) and inserted into
    /// the FTS5 index, and a [`ToolOutputSummary`] is returned.
    ///
    /// The caller should replace the raw output with the summary text.
    #[must_use]
    pub fn index_tool_output(
        &mut self,
        tool_call_id: &str,
        _tool_name: &str,
        output: &str,
        threshold_tokens: usize,
    ) -> Option<ToolOutputSummary> {
        // Rough token estimate: 1 token ≈ 4 characters.
        let token_estimate = output.len() / 4;
        if token_estimate < threshold_tokens {
            return None;
        }

        let lines: Vec<&str> = output.lines().collect();
        let total_lines = lines.len();
        let full_size_bytes = output.len();

        // Chunk by 50 lines and insert into FTS5.
        let chunk_size = 50;
        if let Ok(tx) = self.conn.transaction() {
            for chunk_start in (0..total_lines).step_by(chunk_size) {
                let chunk_end = (chunk_start + chunk_size).min(total_lines);
                let chunk_content: String = lines[chunk_start..chunk_end].join("\n");
                let line_range = format!("L{}-L{}", chunk_start + 1, chunk_end);
                let _ = tx.execute(
                    "INSERT INTO tool_output_fts(call_id, line_range, content) VALUES (?1, ?2, ?3)",
                    params![tool_call_id, line_range, chunk_content],
                );
            }
            let _ = tx.commit();
        }

        // Extract keyword highlights (top 10 by frequency).
        let keywords = extract_keywords(output, 10);

        // Sample head and tail.
        let head_sample: Vec<&str> = lines.iter().take(3).copied().collect();
        let tail_sample: Vec<&str> = lines.iter().rev().take(3).copied().collect();

        Some(ToolOutputSummary {
            full_size_bytes,
            total_lines,
            sample_head: head_sample.join("\n"),
            sample_tail: tail_sample.into_iter().rev().collect::<Vec<_>>().join("\n"),
            keyword_highlights: keywords,
            search_hint: format!(
                "Output indexed ({} lines, {} bytes). \
                 Use /sandbox-search {} <query> to locate specific content.",
                total_lines, full_size_bytes, tool_call_id
            ),
        })
    }

    /// Search the FTS5 index for chunks matching `query` within the output of
    /// the given `tool_call_id`.
    ///
    /// Returns up to `limit` [`SearchSnippet`]s ordered by FTS5 relevance.
    #[must_use]
    pub fn search(
        &self,
        tool_call_id: &str,
        query: &str,
        limit: usize,
    ) -> Vec<SearchSnippet> {
        let sql = "SELECT line_range, content FROM tool_output_fts \
                   WHERE call_id = ?1 AND content MATCH ?2 \
                   LIMIT ?3";
        let mut stmt = match self.conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return vec![],
        };

        let rows = stmt.query_map(params![tool_call_id, query, limit as i64], |row| {
            let line_range: String = row.get(0)?;
            let content: String = row.get(1)?;
            // Parse "L{start}-L{end}" format.
            let parts: Vec<&str> = line_range.trim_start_matches('L').split("-L").collect();
            let start: usize = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
            let end: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(start);
            Ok(SearchSnippet {
                line_start: start,
                line_end: end,
                content,
            })
        });

        match rows {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => vec![],
        }
    }

    /// Remove all indexed entries for the given tool call ID.
    pub fn clear(&self, tool_call_id: &str) {
        let _ = self.conn.execute(
            "DELETE FROM tool_output_fts WHERE call_id = ?1",
            params![tool_call_id],
        );
    }

    /// Remove all indexed entries (reset the sandbox).
    pub fn clear_all(&self) {
        let _ = self.conn.execute("DELETE FROM tool_output_fts", []);
    }

    /// Delete oldest entries when count exceeds limit (LRU).
    pub fn clear_oldest(&self, max_entries: usize) {
        let count: i64 = self.conn.query_row("SELECT COUNT(DISTINCT call_id) FROM tool_output_fts", [], |r| r.get(0)).unwrap_or(0);
        if count as usize > max_entries {
            let excess = count as usize - max_entries;
            let _ = self.conn.execute(
                "DELETE FROM tool_output_fts WHERE call_id IN (SELECT call_id FROM tool_output_fts GROUP BY call_id ORDER BY MIN(rowid) LIMIT ?1)",
                rusqlite::params![excess as i64],
            );
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Extract up to `top_n` most frequent keywords from `text`, filtering out
/// common English stop words and short tokens.
fn extract_keywords(text: &str, top_n: usize) -> Vec<String> {
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
        "this", "that", "these", "those", "it", "its",
    ];

    let mut freq: HashMap<&str, usize> = HashMap::new();
    for word in text.split(|c: char| !c.is_alphanumeric() && c != '_') {
        let w = word.trim();
        if w.len() < 3 || stop_words.contains(&w.to_lowercase().as_str()) {
            continue;
        }
        *freq.entry(w).or_insert(0) += 1;
    }

    let mut sorted: Vec<_> = freq.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    sorted.truncate(top_n);
    sorted.into_iter().map(|(w, _)| w.to_string()).collect()
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn below_threshold_returns_none() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let result = sandbox.index_tool_output("call_1", "bash", "hello world", 100);
        assert!(result.is_none());
    }

    #[test]
    fn above_threshold_returns_summary() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        // ~5000 tokens worth of content.
        let large_output = "line of data\n".repeat(5000);
        let result = sandbox.index_tool_output("call_1", "bash", &large_output, 1000);
        assert!(result.is_some());
        let summary = result.unwrap();
        assert!(summary.total_lines > 0);
        assert!(summary.full_size_bytes > 0);
        assert!(!summary.search_hint.is_empty());
        assert!(!summary.sample_head.is_empty());
    }

    #[test]
    fn search_finds_indexed_content() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let output = "error: config_parse_failed at line 42\n\
                      error: invalid_field at line 100\n\
                      info: config loaded successfully\n\
                      warning: deprecated_option at line 200\n\
                      error: timeout at line 300";
        sandbox.index_tool_output("call_x", "bash", output, 10);
        
        let results = sandbox.search("call_x", "error", 5);
        assert!(!results.is_empty(), "should find 'error' in indexed content");
    }

    #[test]
    fn search_unknown_call_id_returns_empty() {
        let sandbox = ToolOutputSandbox::new().unwrap();
        let results = sandbox.search("nonexistent", "error", 5);
        assert!(results.is_empty());
    }

    #[test]
    fn clear_removes_entries() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let output = "error: something went wrong\n".repeat(200);
        let _ = sandbox.index_tool_output("call_z", "bash", &output, 10);

        // Should find before clear.
        assert!(!sandbox.search("call_z", "error", 5).is_empty());

        sandbox.clear("call_z");
        assert!(sandbox.search("call_z", "error", 5).is_empty());
    }

    #[test]
    fn extract_keywords_filters_stop_words() {
        let text = "the quick brown fox jumps over the lazy dog. the fox is quick.";
        let kws = extract_keywords(text, 5);
        // "the" and "is" should be filtered.
        assert!(!kws.contains(&"the".to_string()));
        assert!(!kws.contains(&"is".to_string()));
        // "quick" and "fox" should appear.
        let joined = kws.join(" ");
        assert!(joined.contains("quick") || joined.contains("fox"),
            "expected 'quick' or 'fox' in keywords: {joined}");
    }
}
