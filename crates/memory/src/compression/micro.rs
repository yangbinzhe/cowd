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
                        msg.content =
                            format!("[duplicate – see turn {last_idx}]");
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
