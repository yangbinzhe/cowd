//! Tool output sandbox — derived in-memory FTS5 index over tool evidence.
//!
//! This index is never the lifecycle truth. Durable chunks carry a canonical
//! evidence reference from the session ledger; a failed durable write may
//! instead create an explicitly ephemeral active-runtime entry. Neither form
//! is replaced by the index, and ephemeral entries are never advertised as
//! restart-safe evidence.
//!
//! Inspired by context-mode's "batch→index→search→inject" pipeline.

use rusqlite::{params, Connection};
use std::collections::HashMap;

use crate::types::CanonicalRawEvidence;

/// A snippet of indexed tool output returned by a search query.
#[derive(Debug, Clone)]
pub struct SearchSnippet {
    /// Canonical durable evidence identifier.
    pub evidence_ref: String,
    /// Hash of the complete canonical raw payload.
    pub content_hash: String,
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
             USING fts5(call_id, evidence_ref UNINDEXED, content_hash UNINDEXED, \
                        line_range, content, tokenize='porter unicode61');",
        )?;
        Ok(Self { conn })
    }

    /// Index a tool output.
    ///
    /// If the line count of `output` is below `threshold_min_lines`,
    /// returns `None` (the output is small enough to keep in context).
    /// Otherwise the output is chunked (50 lines per chunk) and inserted into
    /// the FTS5 index, and a [`ToolOutputSummary`] is returned.
    ///
    /// This compatibility entry point deliberately refuses to build an orphan
    /// index. Call [`Self::index_tool_output_with_evidence`] after canonical raw
    /// persistence has returned a durable receipt.
    #[must_use]
    pub fn index_tool_output(
        &mut self,
        _tool_call_id: &str,
        _tool_name: &str,
        _output: &str,
        _threshold_min_lines: usize,
    ) -> Option<ToolOutputSummary> {
        None
    }

    /// Index a canonical raw tool output after its durable write has completed.
    #[must_use]
    pub fn index_tool_output_with_evidence(
        &mut self,
        tool_call_id: &str,
        _tool_name: &str,
        output: &str,
        threshold_min_lines: usize,
        evidence: &CanonicalRawEvidence,
    ) -> Option<ToolOutputSummary> {
        if !evidence.is_durable() || evidence.access.bytes != output.len() as u64 {
            return None;
        }
        self.index_tool_output_with_metadata(
            tool_call_id,
            output,
            threshold_min_lines,
            &evidence.access.evidence_ref.id,
            &evidence.access.sha256,
            &evidence.access.retrieval_selector,
        )
    }

    /// Index an output retained only by the active Runtime instance. This is
    /// deliberately separate from canonical evidence: callers must never
    /// publish its reference as durable or claim it survives a restart.
    #[must_use]
    pub fn index_tool_output_ephemeral(
        &mut self,
        tool_call_id: &str,
        output: &str,
        threshold_min_lines: usize,
        evidence_ref: &str,
        content_hash: &str,
    ) -> Option<ToolOutputSummary> {
        self.index_tool_output_with_metadata(
            tool_call_id,
            output,
            threshold_min_lines,
            evidence_ref,
            content_hash,
            &format!("runtime-memory://tool-output/{tool_call_id}"),
        )
    }

    fn index_tool_output_with_metadata(
        &mut self,
        tool_call_id: &str,
        output: &str,
        threshold_min_lines: usize,
        evidence_ref: &str,
        content_hash: &str,
        retrieval_selector: &str,
    ) -> Option<ToolOutputSummary> {
        let lines: Vec<&str> = output.lines().collect();
        if lines.len() < threshold_min_lines && output.chars().count() < 16_000 {
            return None;
        }

        let total_lines = lines.len();
        let full_size_bytes = output.len();

        // Chunk by 50 lines and insert into FTS5.
        let chunk_size = 50;
        if let Ok(tx) = self.conn.transaction() {
            if total_lines < threshold_min_lines {
                let chars = output.chars().collect::<Vec<_>>();
                for (chunk_index, chunk) in chars.chunks(8_000).enumerate() {
                    let chunk_content = chunk.iter().collect::<String>();
                    let line_range = format!(
                        "C{}-C{}",
                        chunk_index * 8_000,
                        (chunk_index * 8_000) + chunk.len()
                    );
                    let _ = tx.execute(
                        "INSERT INTO tool_output_fts(call_id, evidence_ref, content_hash, line_range, content) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![tool_call_id, evidence_ref, content_hash, line_range, chunk_content],
                    );
                }
            } else {
                for chunk_start in (0..total_lines).step_by(chunk_size) {
                    let chunk_end = (chunk_start + chunk_size).min(total_lines);
                    let chunk_content: String = lines[chunk_start..chunk_end].join("\n");
                    let line_range = format!("L{}-L{}", chunk_start + 1, chunk_end);
                    let _ = tx.execute(
                        "INSERT INTO tool_output_fts(call_id, evidence_ref, content_hash, line_range, content) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![tool_call_id, evidence_ref, content_hash, line_range, chunk_content],
                    );
                }
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
                 Use evidence_retrieve with evidence_ref {} and selector {}.",
                total_lines, full_size_bytes, evidence_ref, retrieval_selector
            ),
        })
    }

    /// Search the FTS5 index for chunks matching `query` within the output of
    /// the given `tool_call_id`.
    ///
    /// Returns up to `limit` [`SearchSnippet`]s ordered by FTS5 relevance.
    #[must_use]
    pub fn search(&self, tool_call_id: &str, query: &str, limit: usize) -> Vec<SearchSnippet> {
        let sql = "SELECT evidence_ref, content_hash, line_range, content FROM tool_output_fts \
                   WHERE call_id = ?1 AND content MATCH ?2 \
                   LIMIT ?3";
        let mut stmt = match self.conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return vec![],
        };

        let rows = stmt.query_map(params![tool_call_id, query, limit as i64], |row| {
            let evidence_ref: String = row.get(0)?;
            let content_hash: String = row.get(1)?;
            let line_range: String = row.get(2)?;
            let content: String = row.get(3)?;
            let (start, end) = parse_range(&line_range);
            Ok(SearchSnippet {
                evidence_ref,
                content_hash,
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

    /// Read the first indexed chunks for an evidence reference without an FTS query.
    #[must_use]
    pub fn read(&self, tool_call_id: &str, limit: usize) -> Vec<SearchSnippet> {
        let mut stmt = match self.conn.prepare(
            "SELECT evidence_ref, content_hash, line_range, content FROM tool_output_fts \
             WHERE call_id = ?1 ORDER BY rowid LIMIT ?2",
        ) {
            Ok(statement) => statement,
            Err(_) => return vec![],
        };
        let rows = stmt.query_map(params![tool_call_id, limit as i64], |row| {
            let evidence_ref: String = row.get(0)?;
            let content_hash: String = row.get(1)?;
            let range: String = row.get(2)?;
            let content: String = row.get(3)?;
            let (line_start, line_end) = parse_range(&range);
            Ok(SearchSnippet {
                evidence_ref,
                content_hash,
                line_start,
                line_end,
                content,
            })
        });
        rows.map(|mapped| mapped.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    /// Search across ALL indexed tool outputs (not restricted to a specific call_id).
    /// Returns matching snippets ordered by FTS5 relevance.
    #[must_use]
    pub fn search_all(&self, query: &str, limit: usize) -> Vec<SearchSnippet> {
        let sql = "SELECT evidence_ref, content_hash, line_range, content FROM tool_output_fts \
                   WHERE content MATCH ?1 \
                   LIMIT ?2";
        let mut stmt = match self.conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        let rows = stmt.query_map(params![query, limit as i64], |row| {
            let evidence_ref: String = row.get(0)?;
            let content_hash: String = row.get(1)?;
            let line_range: String = row.get(2)?;
            let content: String = row.get(3)?;
            let (start, end) = parse_range(&line_range);
            Ok(SearchSnippet {
                evidence_ref,
                content_hash,
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

    /// Return total count of indexed tool output entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.conn
            .query_row("SELECT count(*) FROM tool_output_fts", [], |row| {
                row.get::<_, i64>(0)
            })
            .map(|n| n as usize)
            .unwrap_or(0)
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
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(DISTINCT call_id) FROM tool_output_fts",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if count as usize > max_entries {
            let excess = count as usize - max_entries;
            let _ = self.conn.execute(
                "DELETE FROM tool_output_fts WHERE call_id IN (SELECT call_id FROM tool_output_fts GROUP BY call_id ORDER BY MIN(rowid) LIMIT ?1)",
                rusqlite::params![excess as i64],
            );
        }
    }
}

fn parse_range(range: &str) -> (usize, usize) {
    let normalized = range
        .trim_start_matches('L')
        .trim_start_matches('C')
        .replace("-L", "-")
        .replace("-C", "-");
    let mut parts = normalized.split('-');
    let start = parts
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let end = parts
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(start);
    (start, end)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Extract up to `top_n` most frequent keywords from `text`, filtering out
/// common English stop words and short tokens.
fn extract_keywords(text: &str, top_n: usize) -> Vec<String> {
    let stop_words: &[&str] = &[
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
        "do", "does", "did", "will", "would", "could", "should", "may", "might", "shall", "can",
        "to", "of", "in", "for", "on", "with", "at", "by", "from", "as", "into", "through",
        "during", "before", "after", "above", "below", "between", "out", "off", "over", "under",
        "again", "further", "then", "once", "and", "but", "or", "nor", "not", "so", "yet", "both",
        "either", "neither", "each", "every", "all", "any", "few", "more", "most", "other", "some",
        "such", "no", "only", "own", "same", "than", "too", "very", "just", "because", "this",
        "that", "these", "those", "it", "its",
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
    use harness_contract::{context::EvidenceAccessRef, reality::EvidenceRef};

    fn receipt(id: &str, output: &str) -> CanonicalRawEvidence {
        CanonicalRawEvidence::new(
            EvidenceAccessRef::durable(
                EvidenceRef::durable(id),
                format!("sha256:{id}"),
                output.len() as u64,
                "text/plain",
                format!("retrieve {id}"),
                "session:test",
            ),
            "preview",
        )
    }

    #[test]
    fn below_threshold_returns_none() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let result = sandbox.index_tool_output("call_1", "bash", "hello world", 100);
        assert!(result.is_none());
    }

    #[test]
    fn large_output_without_durable_receipt_is_not_orphan_indexed() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let output = "uncommitted raw output\n".repeat(1_000);
        assert!(sandbox
            .index_tool_output("pending-call", "bash", &output, 10)
            .is_none());
        assert_eq!(sandbox.entry_count(), 0);
    }

    #[test]
    fn active_runtime_can_index_ephemeral_output_without_claiming_durability() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let output = "transient_evidence_marker\n".repeat(120);
        let summary = sandbox.index_tool_output_ephemeral(
            "ephemeral-1",
            &output,
            10,
            "tool-raw-ephemeral-1",
            "ephemeral:hash",
        );

        assert!(summary.is_some());
        assert!(sandbox.entry_count() > 0);
        let found = sandbox.search("ephemeral-1", "transient_evidence_marker", 1);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].evidence_ref, "tool-raw-ephemeral-1");
    }

    #[test]
    fn above_threshold_returns_summary() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        // ~5000 tokens worth of content.
        let large_output = "line of data\n".repeat(5000);
        let result = sandbox.index_tool_output_with_evidence(
            "call_1",
            "bash",
            &large_output,
            1000,
            &receipt("raw-1", &large_output),
        );
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
        let _ = sandbox.index_tool_output_with_evidence(
            "call_x",
            "bash",
            output,
            1,
            &receipt("raw-x", output),
        );

        let results = sandbox.search("call_x", "error", 5);
        assert!(
            !results.is_empty(),
            "should find 'error' in indexed content"
        );
        assert_eq!(results[0].evidence_ref, "raw-x");
        assert_eq!(results[0].content_hash, "sha256:raw-x");
    }

    #[test]
    fn search_unknown_call_id_returns_empty() {
        let sandbox = ToolOutputSandbox::new().unwrap();
        let results = sandbox.search("nonexistent", "error", 5);
        assert!(results.is_empty());
    }

    #[test]
    fn large_single_line_json_is_indexed_and_readable() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let output = format!(r#"{{"records":["{}"]}}"#, "important-value,".repeat(2_000));
        let summary = sandbox.index_tool_output_with_evidence(
            "evidence-json",
            "query",
            &output,
            100,
            &receipt("raw-json", &output),
        );
        assert!(summary.is_some());
        assert!(!sandbox.read("evidence-json", 1).is_empty());
        assert!(!sandbox.search("evidence-json", "important", 1).is_empty());
    }

    #[test]
    fn clear_removes_entries() {
        let mut sandbox = ToolOutputSandbox::new().unwrap();
        let output = "error: something went wrong\n".repeat(200);
        let _ = sandbox.index_tool_output_with_evidence(
            "call_z",
            "bash",
            &output,
            10,
            &receipt("raw-z", &output),
        );

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
        assert!(
            joined.contains("quick") || joined.contains("fox"),
            "expected 'quick' or 'fox' in keywords: {joined}"
        );
    }
}
