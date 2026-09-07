//! Tool output sandbox — derived process-local index over tool evidence.
//!
//! This index is never the lifecycle truth. Durable chunks carry a canonical
//! evidence reference from the session ledger; a failed durable write may
//! instead create an explicitly ephemeral active-runtime entry. Neither form
//! is replaced by the index, and ephemeral entries are never advertised as
//! restart-safe evidence.
//!
//! Inspired by context-mode's "batch→index→search→inject" pipeline.

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
    chunks: parking_lot::Mutex<Vec<IndexedChunk>>,
}

#[derive(Debug, Clone)]
struct IndexedChunk {
    call_id: String,
    snippet: SearchSnippet,
}

impl ToolOutputSandbox {
    /// Create an isolated, process-local derived index.
    pub fn new() -> Result<Self, std::convert::Infallible> {
        Ok(Self {
            chunks: parking_lot::Mutex::new(Vec::new()),
        })
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

        // Chunk by 50 lines and insert into the derived index.
        let chunk_size = 50;
        let mut indexed = self.chunks.lock();
        if total_lines < threshold_min_lines {
            let chars = output.chars().collect::<Vec<_>>();
            for (chunk_index, chunk) in chars.chunks(8_000).enumerate() {
                indexed.push(indexed_chunk(
                    tool_call_id,
                    evidence_ref,
                    content_hash,
                    chunk_index * 8_000,
                    (chunk_index * 8_000) + chunk.len(),
                    chunk.iter().collect(),
                ));
            }
        } else {
            for chunk_start in (0..total_lines).step_by(chunk_size) {
                let chunk_end = (chunk_start + chunk_size).min(total_lines);
                indexed.push(indexed_chunk(
                    tool_call_id,
                    evidence_ref,
                    content_hash,
                    chunk_start + 1,
                    chunk_end,
                    lines[chunk_start..chunk_end].join("\n"),
                ));
            }
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
        search_chunks(&self.chunks.lock(), Some(tool_call_id), query, limit)
    }

    /// Read the first indexed chunks for an evidence reference without an FTS query.
    #[must_use]
    pub fn read(&self, tool_call_id: &str, limit: usize) -> Vec<SearchSnippet> {
        self.chunks
            .lock()
            .iter()
            .filter(|chunk| chunk.call_id == tool_call_id)
            .take(limit)
            .map(|chunk| chunk.snippet.clone())
            .collect()
    }

    /// Search across ALL indexed tool outputs (not restricted to a specific call_id).
    /// Returns matching snippets ordered by FTS5 relevance.
    #[must_use]
    pub fn search_all(&self, query: &str, limit: usize) -> Vec<SearchSnippet> {
        search_chunks(&self.chunks.lock(), None, query, limit)
    }

    /// Return total count of indexed tool output entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.chunks.lock().len()
    }

    /// Remove all indexed entries for the given tool call ID.
    pub fn clear(&self, tool_call_id: &str) {
        self.chunks
            .lock()
            .retain(|chunk| chunk.call_id != tool_call_id);
    }

    /// Remove all indexed entries (reset the sandbox).
    pub fn clear_all(&self) {
        self.chunks.lock().clear();
    }

    /// Delete oldest entries when count exceeds limit (LRU).
    pub fn clear_oldest(&self, max_entries: usize) {
        let mut chunks = self.chunks.lock();
        let mut call_ids = Vec::new();
        for chunk in chunks.iter() {
            if !call_ids.contains(&chunk.call_id) {
                call_ids.push(chunk.call_id.clone());
            }
        }
        if call_ids.len() > max_entries {
            let remove = &call_ids[..call_ids.len() - max_entries];
            chunks.retain(|chunk| !remove.contains(&chunk.call_id));
        }
    }
}

fn indexed_chunk(
    call_id: &str,
    evidence_ref: &str,
    content_hash: &str,
    line_start: usize,
    line_end: usize,
    content: String,
) -> IndexedChunk {
    IndexedChunk {
        call_id: call_id.to_string(),
        snippet: SearchSnippet {
            evidence_ref: evidence_ref.to_string(),
            content_hash: content_hash.to_string(),
            line_start,
            line_end,
            content,
        },
    }
}

fn search_chunks(
    chunks: &[IndexedChunk],
    call_id: Option<&str>,
    query: &str,
    limit: usize,
) -> Vec<SearchSnippet> {
    let terms: Vec<String> = query
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter(|term| {
            !term.is_empty() && !matches!(term.to_ascii_uppercase().as_str(), "OR" | "AND" | "NOT")
        })
        .map(str::to_lowercase)
        .collect();
    chunks
        .iter()
        .filter(|chunk| call_id.is_none_or(|expected| chunk.call_id == expected))
        .filter(|chunk| {
            let content = chunk.snippet.content.to_lowercase();
            terms.is_empty() || terms.iter().any(|term| content.contains(term))
        })
        .take(limit)
        .map(|chunk| chunk.snippet.clone())
        .collect()
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
