//! Stage 2 – Session-level compression.
//!
//! Triggered when the estimated token count of the in-flight message list
//! exceeds a configurable threshold.  The compactor:
//!
//! 1. Splits the list into *old* messages and a *recent* preserve window.
//! 2. Generates a structured 9-section summary from the old messages.
//! 3. Extracts key decisions / code changes and writes them to L2.
//! 4. Writes the summary itself to L3.
//! 5. Rebuilds the message list as `[summary_message, …recent…]`.

use std::sync::Arc;

use chrono::Utc;

use crate::{
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

/// Tunables for the session-compaction stage.
#[derive(Debug, Clone)]
pub struct SessionCompactConfig {
    /// Fire session compaction when the estimated token count exceeds this.
    pub threshold_tokens: u32,
    /// Number of most-recent messages to keep verbatim after compaction.
    pub preserve_recent: usize,
    /// Minimum entries to compress (don't fire if history is tiny).
    pub min_messages_to_compact: usize,
}

impl Default for SessionCompactConfig {
    fn default() -> Self {
        Self {
            threshold_tokens: 40_000,
            preserve_recent: 10,
            min_messages_to_compact: 4,
        }
    }
}

impl SessionCompactConfig {
    #[must_use]
    pub fn from_config(config: &CompressionConfig) -> Self {
        let mut cfg = Self::default();
        // The global session_threshold is expressed in number-of-summaries;
        // we map it lightly to the token threshold here.
        cfg.min_messages_to_compact = config.session_threshold;
        cfg
    }
}

// ---------------------------------------------------------------------------
// Compactor
// ---------------------------------------------------------------------------

/// Stage-2 (session) compactor.
pub struct SessionCompactor {
    config: SessionCompactConfig,
    /// Optional LLM summariser for semantic summary generation.
    llm_summarizer: Option<Arc<dyn LlmSummarizer>>,
}

impl SessionCompactor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: SessionCompactConfig::default(),
            llm_summarizer: None,
        }
    }

    #[must_use]
    pub fn from_config(config: &CompressionConfig) -> Self {
        Self {
            config: SessionCompactConfig::from_config(config),
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

    /// Return `true` when session compaction should be triggered.
    #[must_use]
    pub fn should_compact(&self, messages: &[Message]) -> bool {
        let total = self.estimate_tokens(messages);
        total > self.config.threshold_tokens
            && messages.len() >= self.config.min_messages_to_compact
    }

    /// Execute session compaction.
    ///
    /// Splits `messages` into old + recent, summarises the old portion,
    /// persists key information to the memory layers, and replaces the
    /// message list with `[summary_msg, …recent…]`.
    pub async fn compact(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
    ) -> Result<CompactionResult> {
        let tokens_before: u32 = self.estimate_tokens(messages);

        let (old_messages, recent) = self.split_messages(messages.clone());

        // Generate structured summary (tries LLM, falls back to template).
        let summary = self.generate_summary(&old_messages).await;
        let summary_tokens = (summary.len() as u32).div_ceil(4);

        // Extract decisions and write to L2.
        let decisions = self.extract_decisions(&old_messages);
        let mut memories_extracted: u32 = 0;
        for decision in &decisions {
            orchestrator
                .write(
                    MemoryLayer::L2,
                    MemoryCategory::Decision,
                    &format!("Decision: {}", &decision[..decision.len().min(80)]),
                    decision,
                    Priority::Normal,
                    MemorySource::Compression,
                    vec!["compression".into(), "decision".into()],
                    None,
                )
                .await
                .map_err(|e| crate::error::MemoryError::Compression(e.to_string()))?;
            memories_extracted += 1;
        }

        // Write full summary to L3.
        orchestrator
            .write(
                MemoryLayer::L3,
                MemoryCategory::CompressedSummary,
                &format!("Session summary – {}", Utc::now().format("%Y-%m-%d %H:%M")),
                &summary,
                Priority::Normal,
                MemorySource::Compression,
                vec!["compression".into(), "session-summary".into()],
                None,
            )
            .await
            .map_err(|e| crate::error::MemoryError::Compression(e.to_string()))?;
        memories_extracted += 1;

        // Rebuild message list.
        *messages = self.rebuild_messages(summary.clone(), recent);

        let tokens_after: u32 = self.estimate_tokens(messages);

        Ok(CompactionResult {
            tokens_before,
            tokens_after,
            memories_extracted,
            summary_tokens,
        })
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Approximate token count using the improved estimator.
    #[must_use] 
    pub fn estimate_tokens(&self, messages: &[Message]) -> u32 {
        super::token_estimation::estimate_tokens_messages(messages)
    }

    /// Split into `(old, recent)` where `recent` contains the last
    /// `preserve_recent` messages.
    fn split_messages(&self, messages: Vec<Message>) -> (Vec<Message>, Vec<Message>) {
        let total = messages.len();
        let recent_count = self.config.preserve_recent.min(total);
        let split_at = total - recent_count;
        let recent = messages[split_at..].to_vec();
        let old = messages[..split_at].to_vec();
        (old, recent)
    }

    /// Generate a structured summary from a set of messages.
    ///
    /// When an LLM summariser is available, it is used to generate a semantic
    /// summary.  On failure (or when no summariser is configured), the method
    /// falls back to the template-based heuristic.
    async fn generate_summary(&self, messages: &[Message]) -> String {
        // Try LLM summariser first
        if let Some(ref summarizer) = self.llm_summarizer {
            let content: String = messages
                .iter()
                .map(|m| format!("[{}]: {}", m.role, m.content))
                .collect::<Vec<_>>()
                .join("\n");

            let prompt = "Generate a comprehensive 9-section summary of the following \
                           conversation. Include: 1) Context, 2) Key Decisions, 3) Code Changes, \
                           4) Errors Fixed, 5) Patterns Discovered, 6) User Preferences, \
                           7) Open Questions, 8) Current State, 9) Next Steps. \
                           Be thorough and preserve all important details.";

            match summarizer.summarize(prompt, &content).await {
                Ok(summary) if !summary.trim().is_empty() => {
                    tracing::debug!("LLM session summary generated ({} chars)", summary.len());
                    return format!(
                        "## Compressed Session Summary (LLM)\n\n{}\n\n---\n*Generated by SessionCompactor with LLM.*",
                        summary
                    );
                }
                Ok(_) => {
                    tracing::warn!("LLM returned empty summary, falling back to template");
                }
                Err(e) => {
                    tracing::warn!("LLM summarisation failed, falling back to template: {}", e);
                }
            }
        }

        // Fallback: template-based heuristic
        self.generate_summary_template(messages)
    }

    /// Template-based heuristic summary generation (fallback).
    fn generate_summary_template(&self, messages: &[Message]) -> String {
        let context = self.extract_context(messages);
        let decisions_text = self.extract_decisions(messages).join("\n- ");
        let code_changes = self.extract_code_changes(messages);
        let errors_fixed = self.extract_errors_fixed(messages);
        let patterns = self.extract_patterns(messages);
        let preferences = self.extract_preferences(messages);
        let questions = self.extract_questions(messages);
        let current_state = self.infer_current_state(messages);
        let next_steps = self.infer_next_steps(messages);

        let decisions_section = if decisions_text.is_empty() {
            "No key decisions recorded.".into()
        } else {
            format!("- {decisions_text}")
        };

        format!(
            r"## Compressed Session Summary

### 1. Context
{context}

### 2. Key Decisions
{decisions_section}

### 3. Code Changes
{code_changes}

### 4. Errors Fixed
{errors_fixed}

### 5. Patterns Discovered
{patterns}

### 6. User Preferences
{preferences}

### 7. Open Questions
{questions}

### 8. Current State
{current_state}

### 9. Next Steps
{next_steps}

---
*Generated by SessionCompactor (template fallback).*
"
        )
    }

    fn extract_context(&self, messages: &[Message]) -> String {
        // Use the first non-empty user or assistant message as context seed.
        let snippet = messages
            .iter()
            .find(|m| {
                matches!(m.role, MessageRole::User | MessageRole::Assistant)
                    && !m.content.trim().is_empty()
            })
            .map(|m| {
                let s: String = m.content.chars().take(300).collect();
                s
            })
            .unwrap_or_default();
        if snippet.is_empty() {
            "No context available.".into()
        } else {
            snippet
        }
    }

    fn extract_decisions(&self, messages: &[Message]) -> Vec<String> {
        // Heuristic: lines containing "decided", "chosen", "agreed", "will use"
        let keywords = ["decided", "chosen", "agreed", "will use", "we should", "let's use"];
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
        decisions
    }

    fn extract_code_changes(&self, messages: &[Message]) -> String {
        // Count tool calls that look like file writes.
        let write_ops = messages
            .iter()
            .filter(|m| {
                m.is_tool_result()
                    && m.tool_name
                        .as_deref()
                        .is_some_and(|n| {
                            n.contains("write")
                                || n.contains("edit")
                                || n.contains("create")
                                || n.contains("replace")
                        })
            })
            .count();
        if write_ops == 0 {
            "No file changes detected.".into()
        } else {
            format!("{write_ops} file write/edit operation(s) performed.")
        }
    }

    fn extract_errors_fixed(&self, messages: &[Message]) -> String {
        let error_count = messages
            .iter()
            .filter(|m| {
                m.content.to_lowercase().contains("error")
                    || m.content.to_lowercase().contains("fix")
                    || m.content.to_lowercase().contains("resolved")
            })
            .count();
        if error_count == 0 {
            "No errors recorded.".into()
        } else {
            format!("Approximately {error_count} message(s) mention errors or fixes.")
        }
    }

    fn extract_patterns(&self, messages: &[Message]) -> String {
        // Heuristic: look for repeated keywords/phrases across messages
        let mut keyword_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for msg in messages {
            for word in msg.content.split_whitespace() {
                let w = word.to_lowercase();
                if w.len() > 4 {
                    *keyword_counts.entry(w).or_insert(0) += 1;
                }
            }
        }
        let repeated: Vec<_> = keyword_counts
            .iter()
            .filter(|(_, &count)| count >= 3)
            .map(|(k, &c)| format!("{} ({}x)", k, c))
            .collect();
        if repeated.is_empty() {
            "No clear patterns detected (LLM analysis recommended for deeper insight).".into()
        } else {
            format!("Frequent terms: {}", repeated.join(", "))
        }
    }

    fn extract_preferences(&self, messages: &[Message]) -> String {
        let keywords = ["prefer", "like", "always", "never", "use", "don't use", "avoid", "recommend"];
        let mut prefs = Vec::new();
        for msg in messages {
            for line in msg.content.lines() {
                let lower = line.to_lowercase();
                if keywords.iter().any(|kw| lower.contains(kw)) {
                    let trimmed = line.trim().to_string();
                    if trimmed.len() > 10 && trimmed.len() < 200 {
                        prefs.push(trimmed);
                    }
                }
            }
        }
        prefs.dedup();
        if prefs.is_empty() {
            "No explicit preferences detected (LLM analysis recommended for deeper insight).".into()
        } else {
            prefs.join("\n- ")
        }
    }

    fn extract_questions(&self, messages: &[Message]) -> String {
        let questions: Vec<_> = messages
            .iter()
            .filter_map(|m| {
                m.content
                    .lines()
                    .find(|l| l.trim_end().ends_with('?'))
                    .map(|l| format!("- {}", l.trim()))
            })
            .take(5)
            .collect();
        if questions.is_empty() {
            "No open questions identified.".into()
        } else {
            questions.join("\n")
        }
    }

    fn infer_current_state(&self, messages: &[Message]) -> String {
        // Use the last non-empty assistant message as the current-state proxy.
        messages
            .iter()
            .rev()
            .find(|m| matches!(m.role, MessageRole::Assistant) && !m.content.trim().is_empty()).map_or_else(|| "State unknown.".into(), |m| {
                let s: String = m.content.chars().take(500).collect();
                s
            })
    }

    fn infer_next_steps(&self, messages: &[Message]) -> String {
        let keywords = ["next", "todo", "will", "should", "plan to", "going to"];
        let steps: Vec<_> = messages
            .iter()
            .rev()
            .take(10)
            .flat_map(|m| m.content.lines().map(str::to_owned).collect::<Vec<_>>())
            .filter(|line| {
                let lower = line.to_lowercase();
                keywords.iter().any(|kw| lower.contains(kw))
            })
            .take(5)
            .map(|l| format!("- {}", l.trim()))
            .collect();
        if steps.is_empty() {
            "No next steps identified.".into()
        } else {
            steps.join("\n")
        }
    }

    /// Rebuild the message list as [`summary_message`] + recent.
    fn rebuild_messages(&self, summary: String, recent: Vec<Message>) -> Vec<Message> {
        let summary_msg = Message {
            turn_index: 0,
            role: MessageRole::User,
            content: format!(
                "[SYSTEM: Previous conversation compressed by SessionCompactor]\n\n{summary}"
            ),
            tool_use_id: None,
            tool_name: None,
            pinned: true,
        };
        let mut result = vec![summary_msg];
        for (i, mut msg) in recent.into_iter().enumerate() {
            msg.turn_index = i + 1;
            result.push(msg);
        }
        result
    }
}

impl Default for SessionCompactor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Legacy MemoryEntry-based API (kept for backward compatibility)
// ---------------------------------------------------------------------------

/// Session-level compression stage (`MemoryEntry` variant).
pub struct SessionCompressor {
    /// Minimum number of session summaries before triggering stage-2.
    pub threshold: usize,
}

impl SessionCompressor {
    #[must_use]
    pub fn new(threshold: usize) -> Self {
        Self { threshold }
    }

    /// Summarise `session_entries` into a single compressed entry.
    pub async fn compress(&self, session_entries: Vec<MemoryEntry>) -> Result<MemoryEntry> {
        let now = Utc::now();
        let combined = session_entries
            .iter()
            .map(|e| format!("## {}\n{}", e.title, e.content))
            .collect::<Vec<_>>()
            .join("\n\n---\n\n");

        let summary_content = format!(
            "## Session Summary\n\
             *Compressed from {} entries.*\n\n\
             {}\n\n\
             ---\n\
             *Generated by SessionCompactor (template fallback).*",
            session_entries.len(),
            &combined[..combined.len().min(4000)]
        );

        Ok(MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: crate::types::MemoryLayer::L3,
            category: crate::types::MemoryCategory::CompressedSummary,
            priority: Priority::Normal,
            source: MemorySource::Compression,
            title: format!("Session summary – {}", now.format("%Y-%m-%d %H:%M")),
            content: summary_content,
            embedding: None,
            tags: vec!["session-summary".into(), "compression".into()],
            relations: vec![],
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: None,
            session_id: None,
        })
    }
}
