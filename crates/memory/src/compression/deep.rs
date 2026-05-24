//! Stage 3 – Deep / cross-session compression.
//!
//! Triggered when session-compaction has already run but the message list is
//! still above the budget.  Applies the most aggressive strategy available:
//!
//! 1. Optionally merges a *previous* summary with the incremental content
//!    (iterative summary update).
//! 2. Compresses all non-pinned messages into a single deep summary.
//! 3. Writes all extractable knowledge to the appropriate memory layers.
//! 4. Rebuilds the message list as `[deep_summary] + 2 most-recent messages`.
//!
//! LLM-backed summarisation is used when available and falls back to
//! a structured template when no LLM is configured.

use std::sync::Arc;

use chrono::Utc;

use crate::{
    MemoryScope,
    compression::{
        llm_summarizer::LlmSummarizer,
        Result,
    },
    config::CompressionConfig,
    orchestrator::MemoryOrchestrator,
    types::{
        CompactionResult, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Message,
        MessageRole, Priority,
    },
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tunables for the deep-compaction stage.
#[derive(Debug, Clone)]
pub struct DeepCompactConfig {
    /// Aggressiveness in `[0.0, 1.0]`; higher = more aggressive pruning.
    pub aggressiveness: f32,
    /// Number of most-recent messages to keep verbatim (minimum 2).
    pub preserve_recent: usize,
    /// Maximum characters for the generated deep summary.
    pub max_summary_chars: usize,
}

impl Default for DeepCompactConfig {
    fn default() -> Self {
        Self {
            aggressiveness: 0.8,
            preserve_recent: 2,
            max_summary_chars: 6_000,
        }
    }
}

impl DeepCompactConfig {
    #[must_use]
    pub fn from_config(config: &CompressionConfig) -> Self {
        Self {
            aggressiveness: config.aggressiveness,
            ..Self::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Compactor
// ---------------------------------------------------------------------------

/// Stage-3 (deep) compactor.
pub struct DeepCompactor {
    config: DeepCompactConfig,
    /// Optional LLM summariser for semantic summary generation.
    llm_summarizer: Option<Arc<dyn LlmSummarizer>>,
}

impl DeepCompactor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: DeepCompactConfig::default(),
            llm_summarizer: None,
        }
    }

    #[must_use]
    pub fn from_config(config: &CompressionConfig) -> Self {
        Self {
            config: DeepCompactConfig::from_config(config),
            llm_summarizer: None,
        }
    }

    /// Attach an LLM summariser for semantic summary generation.
    #[must_use]
    pub fn with_llm_summarizer(mut self, summarizer: Arc<dyn LlmSummarizer>) -> Self {
        self.llm_summarizer = Some(summarizer);
        self
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Execute deep compaction.
    ///
    /// If `previous_summary` is supplied the new summary is built by
    /// iteratively merging the incremental content on top of it.
    pub async fn compact(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
        previous_summary: Option<&str>,
    ) -> Result<CompactionResult> {
        let tokens_before: u32 = estimate_tokens(messages);

        // Split into the preserve window and everything else.
        let preserve = self.config.preserve_recent.max(2).min(messages.len());
        let split_at = messages.len().saturating_sub(preserve);
        let recent: Vec<Message> = messages.drain(split_at..).collect();
        let body: Vec<Message> = std::mem::take(messages);

        // Build the deep summary (with optional iterative merge).
        let summary = self.build_deep_summary(&body, previous_summary).await;
        let summary_tokens = (summary.len() as u32).div_ceil(4);

        // Persist all extractable information.
        let mut memories_extracted: u32 = 0;

        // --- Write the deep summary to L3 ---
        orchestrator
            .write(
                MemoryLayer::L3,
                MemoryCategory::CompressedSummary,
                &format!("Deep summary – {}", Utc::now().format("%Y-%m-%d %H:%M")),
                &summary,
                Priority::High,
                MemorySource::Compression,
                vec!["compression".into(), "deep-summary".into()],
                MemoryScope::default(),
            )
            .await
            .map_err(|e| crate::error::MemoryError::Compression(e.to_string()))?;
        memories_extracted += 1;

        // --- Extract and persist decisions ---
        for decision in extract_decisions(&body) {
            orchestrator
                .write(
                    MemoryLayer::L2,
                    MemoryCategory::Decision,
                    &format!("Deep decision: {}", &decision[..decision.len().min(80)]),
                    &decision,
                    Priority::High,
                    MemorySource::Compression,
                    vec!["compression".into(), "deep-decision".into()],
                    MemoryScope::default(),
                )
                .await
                .map_err(|e| crate::error::MemoryError::Compression(e.to_string()))?;
            memories_extracted += 1;
        }

        // Rebuild message list: [deep_summary_msg] + recent (max 2).
        let summary_msg = Message {
            turn_index: 0,
            role: MessageRole::User,
            content: format!(
                "[SYSTEM: Full conversation history compressed by DeepCompactor]\n\n{summary}"
            ),
            tool_use_id: None,
            tool_name: None,
            pinned: true,
        };
        let recent_trimmed: Vec<Message> = recent
            .into_iter()
            .rev()
            .take(self.config.preserve_recent.max(2))
            .rev()
            .enumerate()
            .map(|(i, mut m)| {
                m.turn_index = i + 1;
                m
            })
            .collect();

        *messages = std::iter::once(summary_msg).chain(recent_trimmed).collect();

        let tokens_after: u32 = estimate_tokens(messages);

        Ok(CompactionResult {
            tokens_before,
            tokens_after,
            memories_extracted,
            summary_tokens,
        })
    }

    // -----------------------------------------------------------------------
    // Summary generation
    // -----------------------------------------------------------------------

    /// Build a deep summary, optionally incorporating a previous summary.
    ///
    /// When an LLM summariser is available, it is used to generate a semantic
    /// summary. On failure (or when no summariser is configured), the method
    /// falls back to the template-based heuristic.
    async fn build_deep_summary(&self, messages: &[Message], previous_summary: Option<&str>) -> String {
        // Try LLM summariser first
        if let Some(ref summarizer) = self.llm_summarizer {
            let content: String = messages
                .iter()
                .map(|m| format!("[{}]: {}", m.role, m.content))
                .collect::<Vec<_>>()
                .join("\n");

            let prompt = match previous_summary {
                Some(prev) => format!(
                    "You are merging a previous deep summary with new conversation content. \
                     Produce a single unified deep summary that preserves all key decisions, \
                     open questions, and important details from both.\n\n\
                     Previous summary:\n{}\n\n\
                     Merge the new content into this summary, updating and expanding as needed.",
                    prev
                ),
                None => "Generate a comprehensive deep compression summary of the following \
                          conversation. Include all key decisions, open questions, important \
                          code changes, and a content digest. Be thorough and preserve all \
                          critical information.".to_string(),
            };

            match summarizer.summarize(&prompt, &content).await {
                Ok(summary) if !summary.trim().is_empty() => {
                    tracing::debug!("LLM deep summary generated ({} chars)", summary.len());
                    return format!(
                        "## Deep Compression Summary (LLM)\n\n{}\n\n---\n*Generated by DeepCompactor with LLM.*",
                        summary
                    );
                }
                Ok(_) => {
                    tracing::warn!("LLM returned empty deep summary, falling back to template");
                }
                Err(e) => {
                    tracing::warn!("LLM deep summarisation failed, falling back to template: {}", e);
                }
            }
        }

        // Fallback: template-based heuristic
        self.build_deep_summary_template(messages, previous_summary)
    }

    /// Template-based heuristic deep summary generation (fallback).
    fn build_deep_summary_template(&self, messages: &[Message], previous_summary: Option<&str>) -> String {
        let incremental = self.generate_incremental_summary(messages);

        match previous_summary {
            None => self.format_deep_summary(&incremental, None),
            Some(prev) => {
                // Iterative update: combine previous summary with new content.
                let merged = self.merge_summaries(prev, &incremental);
                self.format_deep_summary(&merged, Some(prev))
            }
        }
    }

    fn generate_incremental_summary(&self, messages: &[Message]) -> IncrementalSummary {
        // Count message statistics.
        let total_turns = messages.len();
        let user_turns = messages
            .iter()
            .filter(|m| matches!(m.role, MessageRole::User))
            .count();
        let assistant_turns = messages
            .iter()
            .filter(|m| matches!(m.role, MessageRole::Assistant))
            .count();
        let tool_calls = messages.iter().filter(|m| m.is_tool_result()).count();

        // Collect a brief content digest.
        let content_digest: String = messages
            .iter()
            .filter(|m| matches!(m.role, MessageRole::User | MessageRole::Assistant))
            .flat_map(|m| m.content.chars().take(200))
            .take(1500)
            .collect();

        let decisions = extract_decisions(messages);
        let open_questions = extract_questions(messages);

        IncrementalSummary {
            total_turns,
            user_turns,
            assistant_turns,
            tool_calls,
            content_digest,
            decisions,
            open_questions,
        }
    }

    fn merge_summaries(&self, previous: &str, incremental: &IncrementalSummary) -> IncrementalSummary {
        // Blend previous-summary decisions with newly discovered ones.
        let mut merged_decisions: Vec<String> = extract_decisions_from_text(previous);
        for d in &incremental.decisions {
            if !merged_decisions.contains(d) {
                merged_decisions.push(d.clone());
            }
        }
        // Keep up to 20 decisions.
        merged_decisions.truncate(20);

        let mut merged_questions = incremental.open_questions.clone();
        merged_questions.truncate(10);

        // Combine content digests.
        let combined_digest = format!(
            "**Previous context:**\n{}\n\n**New activity:**\n{}",
            &previous[..previous.len().min(800)],
            &incremental.content_digest[..incremental.content_digest.len().min(800)]
        );

        IncrementalSummary {
            total_turns: incremental.total_turns,
            user_turns: incremental.user_turns,
            assistant_turns: incremental.assistant_turns,
            tool_calls: incremental.tool_calls,
            content_digest: combined_digest,
            decisions: merged_decisions,
            open_questions: merged_questions,
        }
    }

    fn format_deep_summary(
        &self,
        summary: &IncrementalSummary,
        previous_summary: Option<&str>,
    ) -> String {
        let is_iterative = previous_summary.is_some();
        let mode = if is_iterative { "iterative update" } else { "initial deep compression" };

        let decisions_text = if summary.decisions.is_empty() {
            "No key decisions identified.".into()
        } else {
            summary
                .decisions
                .iter()
                .map(|d| format!("- {d}"))
                .collect::<Vec<_>>()
                .join("\n")
        };

        let questions_text = if summary.open_questions.is_empty() {
            "No open questions identified.".into()
        } else {
            summary
                .open_questions
                .iter()
                .map(|q| format!("- {q}"))
                .collect::<Vec<_>>()
                .join("\n")
        };

        format!(
            r"## Deep Compression Summary ({mode})

**Statistics:** {total} turns ({user} user / {assistant} assistant / {tools} tool calls)

### Key Decisions
{decisions}

### Open Questions
{questions}

### Content Digest
{digest}

---
*Generated by DeepCompactor (aggressiveness={aggressiveness:.2}, template fallback).*
",
            mode = mode,
            total = summary.total_turns,
            user = summary.user_turns,
            assistant = summary.assistant_turns,
            tools = summary.tool_calls,
            decisions = decisions_text,
            questions = questions_text,
            digest = &summary.content_digest[..summary.content_digest.len().min(self.config.max_summary_chars)],
            aggressiveness = self.config.aggressiveness,
        )
    }
}

impl Default for DeepCompactor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Legacy MemoryEntry-based API (kept for backward compatibility)
// ---------------------------------------------------------------------------

/// Deep (LLM-assisted) compression stage (`MemoryEntry` variant).
pub struct DeepCompressor {
    /// Aggressiveness factor in `[0.0, 1.0]`.
    pub aggressiveness: f32,
}

impl DeepCompressor {
    #[must_use]
    pub fn new(aggressiveness: f32) -> Self {
        Self { aggressiveness }
    }

    /// Distil `entries` into a smaller, higher-signal set.
    pub async fn compress(&self, entries: Vec<MemoryEntry>) -> Result<Vec<MemoryEntry>> {
        if entries.is_empty() {
            return Ok(entries);
        }
        // Heuristic: sort by (priority desc, staleness asc) and keep the top
        // fraction determined by (1 - aggressiveness).
        let keep_ratio = 1.0 - self.aggressiveness.clamp(0.0, 1.0);
        let keep = ((entries.len() as f32 * keep_ratio).ceil() as usize).max(1);

        let mut sorted = entries;
        sorted.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| {
                    a.staleness
                        .partial_cmp(&b.staleness)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });
        sorted.truncate(keep);
        Ok(sorted)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

struct IncrementalSummary {
    total_turns: usize,
    user_turns: usize,
    assistant_turns: usize,
    tool_calls: usize,
    content_digest: String,
    decisions: Vec<String>,
    open_questions: Vec<String>,
}

fn estimate_tokens(messages: &[Message]) -> u32 {
    super::token_estimation::estimate_tokens_messages(messages)
}

fn extract_decisions(messages: &[Message]) -> Vec<String> {
    let keywords = [
        "decided", "chosen", "agreed", "will use", "we should", "let's use",
    ];
    let mut decisions = Vec::new();
    for msg in messages {
        for line in msg.content.lines() {
            let lower = line.to_lowercase();
            if keywords.iter().any(|kw| lower.contains(kw)) {
                let trimmed = line.trim().to_string();
                if trimmed.len() > 10 {
                    decisions.push(trimmed);
                }
            }
        }
    }
    decisions.dedup();
    decisions.truncate(20);
    decisions
}

fn extract_decisions_from_text(text: &str) -> Vec<String> {
    let keywords = [
        "decided", "chosen", "agreed", "will use", "we should", "let's use",
    ];
    let mut decisions = Vec::new();
    for line in text.lines() {
        let lower = line.to_lowercase();
        if keywords.iter().any(|kw| lower.contains(kw)) {
            let trimmed = line.trim().to_string();
            if trimmed.len() > 10 {
                decisions.push(trimmed);
            }
        }
    }
    decisions.dedup();
    decisions
}

fn extract_questions(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|m| {
            m.content
                .lines()
                .find(|l| l.trim_end().ends_with('?'))
                .map(|l| l.trim().to_string())
        })
        .take(10)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Message, MessageRole};

    fn msg(role: MessageRole, content: &str) -> Message {
        Message { turn_index: 0, role, content: content.into(), tool_use_id: None, tool_name: None, pinned: false }
    }

    #[test]
    fn default_config_parameters() {
        let cfg = DeepCompactConfig::default();
        assert_eq!(cfg.aggressiveness, 0.8);
        assert_eq!(cfg.preserve_recent, 2);
        assert_eq!(cfg.max_summary_chars, 6000);
    }

    #[test]
    fn from_config_reads_aggressiveness() {
        let cc = CompressionConfig { aggressiveness: 0.5, ..Default::default() };
        let cfg = DeepCompactConfig::from_config(&cc);
        assert_eq!(cfg.aggressiveness, 0.5);
    }

    #[test]
    fn new_has_no_llm() {
        let compactor = DeepCompactor::new();
        assert!(compactor.llm_summarizer.is_none());
    }

    #[test]
    fn with_llm_summarizer_attaches() {
        let summarizer = Arc::new(crate::compression::llm_summarizer::NoOpSummarizer);
        let compactor = DeepCompactor::new().with_llm_summarizer(summarizer);
        assert!(compactor.llm_summarizer.is_some());
    }

    #[test]
    fn build_deep_summary_template_produces_output() {
        let compactor = DeepCompactor::new();
        let messages = vec![
            msg(MessageRole::User, "We decided to refactor the auth module"),
            msg(MessageRole::Assistant, "I'll start working on that now"),
        ];
        let summary = compactor.build_deep_summary_template(&messages, None);
        assert!(!summary.is_empty());
        assert!(summary.contains("Deep Compression Summary"));
    }

    #[test]
    fn estimate_tokens_counts_reasonably() {
        let messages = vec![msg(MessageRole::User, "hello world")];
        let tokens = estimate_tokens(&messages);
        assert!(tokens > 0);
    }
}
