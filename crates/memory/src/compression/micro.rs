//! Stage 1 – Micro-entry compression.
//!
//! Runs after every assistant turn.  Applies three lightweight transforms
//! to keep the in-memory message list compact:
//!
//! 1. **Tool-result truncation** – oversized tool outputs are trimmed to a
//!    head + tail slice with a `…[truncated]…` marker.
//! 2. **Time-decay** – older tool results are progressively shortened by
//!    applying an exponential decay to their visible length.
//! 3. **Duplicate-read merging** – when the same file is read multiple times,
//!    only the most recent result is kept at full length.

use std::collections::HashMap;

use crate::{
    compression::Result,
    config::CompressionConfig,
    types::{MemoryEntry, Message},
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tunables for the micro-compaction stage.
#[derive(Debug, Clone)]
pub struct MicroCompactConfig {
    /// Maximum characters to keep in a single tool result before truncation.
    pub tool_result_max_chars: usize,
    /// Exponential decay factor per turn of age.  Values in `(0.0, 1.0)`.
    pub time_decay_factor: f64,
    /// Number of most-recent messages to leave completely untouched.
    pub preserve_recent: usize,
}

impl Default for MicroCompactConfig {
    fn default() -> Self {
        Self {
            tool_result_max_chars: 8_000,
            time_decay_factor: 0.9,
            preserve_recent: 6,
        }
    }
}

impl MicroCompactConfig {
    /// Build from the global compression config.
    #[must_use]
    pub fn from_config(_config: &CompressionConfig) -> Self {
        // Additional per-field extraction can be added once CompressionConfig
        // grows micro-specific knobs.
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// Compactor
// ---------------------------------------------------------------------------

/// Stage-1 (micro) compactor.
pub struct MicroCompactor {
    config: MicroCompactConfig,
}

impl MicroCompactor {
    /// Create with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: MicroCompactConfig::default(),
        }
    }

    /// Create from the global compression config.
    #[must_use]
    pub fn from_config(config: &CompressionConfig) -> Self {
        Self {
            config: MicroCompactConfig::from_config(config),
        }
    }

    // -----------------------------------------------------------------------
    // Public entry point
    // -----------------------------------------------------------------------

    /// Run all micro-compaction transforms on `messages` in-place.
    pub fn compact(&self, messages: &mut Vec<Message>) {
        self.truncate_tool_results(messages);
        self.apply_time_decay(messages);
        self.merge_duplicates(messages);
    }

    // -----------------------------------------------------------------------
    // Transform 1 – Tool result truncation
    // -----------------------------------------------------------------------

    fn truncate_tool_results(&self, messages: &mut Vec<Message>) {
        let max = self.config.tool_result_max_chars;
        for msg in messages.iter_mut() {
            if !msg.is_tool_result() {
                continue;
            }
            if msg.content.len() > max {
                msg.content = truncate_content(&msg.content, max);
            } else if let Some(ref tool_name) = msg.tool_name {
                let summary = summarize_tool_output(tool_name, &msg.content);
                if summary.len() < msg.content.len() && msg.content.len() > 50 {
                    msg.content = summary;
                } else if summary.len() >= msg.content.len() && msg.content.len() > 2000 {
                    msg.content = truncate_content(&msg.content, 2000);
                }
            } else if msg.content.len() > 2000 {
                msg.content = truncate_content(&msg.content, 2000);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Transform 2 – Time decay
    // -----------------------------------------------------------------------

    fn apply_time_decay(&self, messages: &mut Vec<Message>) {
        let total = messages.len();
        let preserve = self.config.preserve_recent.min(total);
        let decay = self.config.time_decay_factor;
        let max = self.config.tool_result_max_chars;

        // Work on older messages only (everything before the preserve window).
        let cutoff = total.saturating_sub(preserve);
        for (i, msg) in messages.iter_mut().enumerate() {
            if i >= cutoff {
                break; // inside the preserve window
            }
            if !msg.is_tool_result() {
                continue;
            }
            let turns_ago = (total - 1 - i) as u32;
            let age_factor = decay.powi(turns_ago as i32);
            let allowed = ((max as f64) * age_factor).max(256.0) as usize;
            if msg.content.len() > allowed {
                msg.content = truncate_content(&msg.content, allowed);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Transform 3 – Duplicate-read merging
    // -----------------------------------------------------------------------

    /// Remove older duplicate reads of the same file/tool, keeping only the
    /// most recent occurrence of each `(tool_name, content_key)` pair.
    fn merge_duplicates(&self, messages: &mut Vec<Message>) {
        // First pass: build a map from (tool_name, key) → last seen index.
        let mut last_seen: HashMap<(String, String), usize> = HashMap::new();
        for (i, msg) in messages.iter().enumerate() {
            if let (Some(tool_name), true) = (&msg.tool_name, msg.is_tool_result()) {
                // Use the first 200 chars as a deduplication key (file path
                // or command string is typically at the start of the content).
                let key = msg.content.chars().take(200).collect::<String>();
                last_seen.insert((tool_name.clone(), key), i);
            }
        }

        // Second pass: blank out duplicated earlier results.
        for (i, msg) in messages.iter_mut().enumerate() {
            if msg.pinned || !msg.is_tool_result() {
                continue;
            }
            if let Some(tool_name) = &msg.tool_name {
                let key = msg.content.chars().take(200).collect::<String>();
                if let Some(&last_idx) = last_seen.get(&(tool_name.clone(), key)) {
                    if last_idx > i {
                        // This is an older duplicate – replace content with a
                        // minimal placeholder.
                        msg.content = format!("[duplicate – see turn {last_idx}]");
                    }
                }
            }
        }
    }
}

impl Default for MicroCompactor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Legacy MemoryEntry-based API (kept for backward compatibility with mod.rs)
// ---------------------------------------------------------------------------

/// Micro-compression stage (`MemoryEntry` variant).
///
/// Preserved from the original skeleton so that existing call-sites are not
/// broken.  The heavy lifting is done by [`MicroCompactor`].
pub struct MicroCompressor {
    /// Trigger compression once this many entries have accumulated.
    pub threshold: usize,
}

impl MicroCompressor {
    #[must_use]
    pub fn new(threshold: usize) -> Self {
        Self { threshold }
    }

    /// Compress `entries` if the threshold has been reached.
    pub async fn compress(&self, entries: Vec<MemoryEntry>) -> Result<Vec<MemoryEntry>> {
        if entries.len() < self.threshold {
            return Ok(entries);
        }
        // Simple staleness-based eviction: sort by staleness descending and
        // keep the least-stale half.
        let mut entries = entries;
        entries.sort_by(|a, b| {
            a.staleness
                .partial_cmp(&b.staleness)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let keep = (entries.len() / 2).max(1);
        entries.truncate(keep);
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncate `text` to at most `max_chars`, keeping the first `max/2` and last
/// `max/4` characters with a `\n…[truncated]…\n` marker in between.
fn truncate_content(text: &str, max_chars: usize) -> String {
    if text.len() <= max_chars {
        return text.to_owned();
    }
    let head_len = max_chars / 2;
    let tail_len = max_chars / 4;
    // Walk char boundaries to avoid splitting multi-byte sequences.
    let head_end = text
        .char_indices()
        .nth(head_len)
        .map_or(text.len(), |(i, _)| i);
    let tail_start = text
        .char_indices()
        .rev()
        .nth(tail_len.saturating_sub(1))
        .map_or(0, |(i, _)| i);

    format!(
        "{}\n…[truncated]…\n{}",
        &text[..head_end],
        &text[tail_start..]
    )
}

/// Smart summarization of tool output based on tool type.
///
/// Returns a concise one-line summary (<200 chars) that captures the key
/// information (exit code, file path, match count, URL, etc.) for known tool
/// types.  For unknown tools, returns the full content unchanged (caller will
/// fall back to generic truncation).
///
/// Match order matters: more-specific prefixes (browser_, lsp_, web_search)
/// are checked before broader patterns (search, read, write) to avoid false
/// matches.
fn summarize_tool_output(tool_name: &str, content: &str) -> String {
    let content_len = content.len();
    let line_count = content.lines().count().max(1);

    match tool_name {
        // ── browser_* tools (before "search" to avoid grep arm) ──────────
        n if n.starts_with("browser_") => {
            let url_re = regex::Regex::new(r#""url"\s*:\s*"([^"]+)""#).ok();
            let url = url_re
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str());
            if let Some(url) = url {
                format!("[browser] {url} ({content_len} chars)")
            } else {
                format!("[browser] ({content_len} chars)")
            }
        }

        // ── web_search / web_extract (before grep/search arm) ────────────
        n if n.contains("web_search") => {
            let query_re = regex::Regex::new(r#""query"\s*:\s*"([^"]+)""#).ok();
            let query = query_re
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("?");
            let short_q = if query.len() > 60 {
                format!("{}...", &query[..57])
            } else {
                query.to_string()
            };
            format!("[web_search] '{short_q}' ({content_len} chars)")
        }
        n if n.contains("web_extract") => {
            format!("[web_extract] ({content_len} chars)")
        }

        // ── lsp_* tools ──────────────────────────────────────────────────
        n if n.starts_with("lsp_") => {
            if n.contains("diagnostics") {
                format!("[lsp_diagnostics] {line_count} results")
            } else if n.contains("references") || n.contains("find_references") {
                format!("[lsp_find_references] {line_count} references")
            } else if n.contains("definition") {
                format!("[lsp_goto_definition] {line_count} results")
            } else if n.contains("rename") {
                format!("[lsp_rename] {line_count} results")
            } else if n.contains("symbols") {
                format!("[lsp_symbols] {line_count} results")
            } else {
                format!("[lsp] ({content_len} chars)")
            }
        }

        // ── execute_code ─────────────────────────────────────────────────
        n if n.contains("execute_code") => {
            format!("[execute_code] {line_count} lines output ({content_len} chars)")
        }

        // ── patch ────────────────────────────────────────────────────────
        n if n.contains("patch") => {
            let path_re = regex::Regex::new(r#""(?:path|file)"\s*:\s*"([^"]+)""#).ok();
            let path = path_re
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("?");
            format!("[patch] {path} ({content_len} chars)")
        }

        // ── terminal / bash ──────────────────────────────────────────────
        n if n.contains("terminal") || n.contains("bash") => {
            let exit_regex = regex::Regex::new(r#""exit_code"\s*:\s*(-?\d+)"#).ok();
            let exit_code = exit_regex
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("?");
            format!("[terminal] exit {}, {} lines output", exit_code, line_count)
        }

        // ── read_file / cat ──────────────────────────────────────────────
        n if n.contains("read") || n.contains("cat") => {
            let path_regex = regex::Regex::new(r#""(?:path|file)"\s*:\s*"([^"]+)""#).ok();
            let path = path_regex
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("");
            if !path.is_empty() {
                format!(
                    "[read_file] read {} ({} chars, {} lines)",
                    path, content_len, line_count
                )
            } else {
                format!("[read_file] ({} chars, {} lines)", content_len, line_count)
            }
        }

        // ── write_file (improved — path extraction) ──────────────────────
        n if n.contains("write") => {
            let path_regex = regex::Regex::new(r#""(?:path|file)"\s*:\s*"([^"]+)""#).ok();
            let path = path_regex
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("");
            if !path.is_empty() {
                format!("[write_file] wrote {path} ({line_count} lines)")
            } else {
                format!("[write_file] wrote {line_count} lines")
            }
        }

        // ── grep / search (after web_search to avoid conflict) ───────────
        n if n.contains("grep") || n.contains("search") => {
            let pattern = regex::Regex::new(r#""pattern"\s*:\s*"([^"]+)""#).ok();
            let pat = pattern
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("?");
            format!("[search] '{}' -> {} matches", pat, line_count / 2)
        }

        // ── delegate_task (improved — subagent type) ─────────────────────
        n if n.contains("delegate") => {
            let subagent_re = regex::Regex::new(r#""subagent_type"\s*:\s*"([^"]+)""#).ok();
            let subagent = subagent_re
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("task");
            let goal = regex::Regex::new(r#""goal"\s*:\s*"([^"]+)""#).ok();
            let g = goal
                .and_then(|re| re.captures(content))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("?");
            let short_goal = if g.len() > 60 {
                format!("{}...", &g[..57])
            } else {
                g.to_string()
            };
            format!("[delegate:{subagent}] '{short_goal}' ({content_len} chars)")
        }

        // ── skill_view / skills_list ─────────────────────────────────────
        n if n.contains("skill") => {
            format!("[skill] ({content_len} chars)")
        }

        // ── unknown tool — return unchanged ──────────────────────────────
        _ => content.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summarize_terminal_with_exit_code() {
        let content = r#"{"exit_code": 1, "output": "test failed"}"#;
        let result = summarize_tool_output("terminal", content);
        assert!(
            result.contains("exit 1"),
            "Should contain exit code, got: {}",
            result
        );
        assert!(result.contains("lines"), "Should contain line count");
        assert!(result.len() < content.len(), "Should be compressed");
    }

    #[test]
    fn test_summarize_read_file_with_path() {
        let content = r#"{"path": "src/main.rs", "content": "fn main() {}"}"#;
        let result = summarize_tool_output("read_file", content);
        assert!(result.contains("src/main.rs"), "Should contain file path");
    }

    #[test]
    fn test_summarize_unknown_tool_unchanged() {
        let content = "some long output data here";
        let result = summarize_tool_output("custom_tool", content);
        assert_eq!(result, content, "Unknown tool output should be unchanged");
    }
}
