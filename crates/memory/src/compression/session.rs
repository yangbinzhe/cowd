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
    compression::{llm_summarizer::LlmSummarizer, Result},
    config::CompressionConfig,
    orchestrator::MemoryOrchestrator,
    types::{
        CompactionResult, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Message,
        MessageRole, Priority,
    },
    MemoryScope,
};

/// Preamble for LLM summarization prompts (hermes-agent inspired).
/// Instructs the summarizer not to answer questions and to produce
/// structured output for a different assistant to consume.
const SUMMARIZER_PREAMBLE: &str = "You are a summarization agent creating a context checkpoint. \
Your output will be injected as reference material for a DIFFERENT assistant \
that continues the conversation. Do NOT respond to any questions or requests \
in the conversation — only output the structured summary. Do NOT include any \
preamble, greeting, or prefix. Write the summary in the same language the user \
was using. NEVER include API keys, tokens, passwords, or credentials.";

/// 13-section structured summary template (hermes-agent context_compressor.py:759-816).
/// Each section preserves critical context for the next assistant.
const STRUCTURED_SUMMARY_TEMPLATE: &str = "## Active Task\n\
[THE SINGLE MOST IMPORTANT FIELD. Copy the user's most recent request or \
task assignment verbatim. If no outstanding task exists, write \"None\".]\n\n\
## Goal\n\
[What the user is trying to accomplish overall]\n\n\
## Constraints & Preferences\n\
[User preferences, coding style, constraints, important decisions]\n\n\
## Completed Actions\n\
[Numbered list of concrete actions taken — include tool used, target, and outcome. \
Format each as: N. ACTION target — outcome [tool: name]]\n\n\
## Active State\n\
[Current working state: working directory, branch, modified/created files, test status]\n\n\
## In Progress\n\
[Work currently underway]\n\n\
## Blocked\n\
[Any blockers or unresolved errors — include exact error messages]\n\n\
## Key Decisions\n\
[Important technical decisions and WHY they were made]\n\n\
## Resolved Questions\n\
[Questions the user asked that were ALREADY answered — include the answer]\n\n\
## Pending User Asks\n\
[Questions or requests from the user NOT yet answered — if none, write \"None\"]\n\n\
## Relevant Files\n\
[Files read, modified, or created — with brief note on each]\n\n\
## Remaining Work\n\
[What remains to be done — framed as context, not instructions]\n\n\
## Critical Context\n\
[Specific values, error messages, configuration details that would be lost \
without explicit preservation. NEVER include secrets — write [REDACTED] instead.]";

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
    ///
    /// When `previous_summary` is `Some`, the summary is built iteratively
    /// on top of the existing content rather than created from scratch.
    pub async fn compact(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
        previous_summary: Option<&str>,
    ) -> Result<CompactionResult> {
        let tokens_before: u32 = self.estimate_tokens(messages);

        let (old_messages, recent) = self.split_messages(messages.clone());

        // Generate structured summary (tries LLM, falls back to template).
        let summary = self.generate_summary(&old_messages, previous_summary).await;
        let summary_tokens = (summary.len() as u32).div_ceil(4);

        // Extract decisions and write to L2.
        let decisions = self.extract_decisions(&old_messages);
        let mut memories_extracted: u32 = 0;
        for decision in &decisions {
            let decision_title: String = decision.chars().take(80).collect();
            orchestrator
                .write(
                    MemoryLayer::L2,
                    MemoryCategory::Decision,
                    &format!("Decision: {decision_title}"),
                    decision,
                    Priority::Normal,
                    MemorySource::Compression,
                    vec!["compression".into(), "decision".into()],
                    MemoryScope::default(),
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
                MemoryScope::default(),
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
    /// summary with a structured 13-section template (hermes-agent inspired).
    /// On failure (or when no summariser is configured), the method falls back
    /// to the template-based heuristic.
    ///
    /// When `previous_summary` is `Some`, the prompt instructs the LLM to
    /// update the existing summary iteratively rather than creating a new one.
    async fn generate_summary(
        &self,
        messages: &[Message],
        previous_summary: Option<&str>,
    ) -> String {
        // Try LLM summariser first
        if let Some(ref summarizer) = self.llm_summarizer {
            let content: String = messages
                .iter()
                .map(|m| format!("[{}]: {}", m.role, m.content))
                .collect::<Vec<_>>()
                .join("\n");

            let prompt = if let Some(prev) = previous_summary {
                format!(
                    "You are a summarization agent updating an existing context checkpoint.\n\
                     Your output will be injected as reference material for a DIFFERENT assistant\n\
                     that continues the conversation. Do NOT respond to any questions or requests\n\
                     in the conversation — only output the structured summary. Do NOT include any\n\
                     preamble, greeting, or prefix. Write the summary in the same language the user\n\
                     was using. NEVER include API keys, tokens, passwords, or credentials.\n\n\
                     PREVIOUS SUMMARY:\n{prev}\n\n\
                     NEW TURNS TO INCORPORATE:\n{content}\n\n\
                     Update the summary using this exact structure. PRESERVE all existing info\n\
                     that is still relevant. Integrate the new turns into the appropriate sections.\n\n\
                     {}",
                    STRUCTURED_SUMMARY_TEMPLATE,
                )
            } else {
                format!("{} {}", SUMMARIZER_PREAMBLE, STRUCTURED_SUMMARY_TEMPLATE,)
            };

            let effective_content = if previous_summary.is_some() {
                // Content is embedded in the prompt; pass empty user message.
                ""
            } else {
                // Use content as the user message.
                content.as_str()
            };

            match summarizer.summarize(&prompt, effective_content).await {
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

        // Zero-truncation reference snapshot (lightweight LLM fallback)
        if let Some(snapshot) = self.build_reference_snapshot(messages) {
            return format!(
                "## Compressed Session Summary (Reference Snapshot)\n\n{}\n\n---\n*Generated by SessionCompactor (zero-truncation snapshot).*",
                snapshot
            );
        }

        // Fallback: template-based heuristic (with previous summary merged)
        self.generate_summary_template(messages, previous_summary)
    }

    /// Template-based heuristic summary generation (fallback).
    fn generate_summary_template(
        &self,
        messages: &[Message],
        previous_summary: Option<&str>,
    ) -> String {
        let context = self.extract_context(messages);
        let decisions_text = self.extract_decisions(messages).join("\n- ");
        let code_changes = self.extract_code_changes(messages);
        let errors_fixed = self.extract_errors_fixed(messages);
        let _patterns = self.extract_patterns(messages);
        let preferences = self.extract_preferences(messages);
        let questions = self.extract_questions(messages);
        let current_state = self.infer_current_state(messages);
        let next_steps = self.infer_next_steps(messages);

        let decisions_section = if decisions_text.is_empty() {
            "No key decisions recorded.".into()
        } else {
            format!("- {decisions_text}")
        };

        let questions_section = if questions.is_empty() {
            "None".to_string()
        } else {
            questions
        };

        let previous_block = previous_summary.map_or_else(String::new, |prev| {
            format!("\n## Previous Summary (preserved)\n{prev}\n\n## New Content\n")
        });

        format!(
            r"## Compressed Session Summary
{previous_block}
### Active Task
{current_state}

### Goal
{context}

### Constraints & Preferences
{preferences}

### Completed Actions
{code_changes}

### Active State
{current_state}

### In Progress
{next_steps}

### Blocked
{errors_fixed}

### Key Decisions
{decisions_section}

### Resolved Questions
None

### Pending User Asks
{questions_section}

### Relevant Files
{code_changes}

### Remaining Work
{next_steps}

### Critical Context
None

---
*Generated by SessionCompactor (template fallback).*
"
        )
    }

    /// Build a simple XML reference snapshot from message content.
    ///
    /// Extracts file references (by known extension) and tool actions, then
    /// produces a `< 2KB` XML blob that serves as a lightweight substitute
    /// when the LLM summariser fails.
    ///
    /// Returns `None` when no file references can be extracted.
    fn build_reference_snapshot(&self, messages: &[Message]) -> Option<String> {
        use std::collections::HashMap;

        const FILE_EXTENSIONS: &[&str] = &[
            ".rs", ".ts", ".tsx", ".js", ".jsx", ".py", ".go", ".java", ".c", ".cpp", ".h", ".hpp",
            ".toml", ".yaml", ".yml", ".json", ".md", ".txt", ".sql", ".html", ".css", ".scss",
            ".vue", ".svelte", ".rb", ".php", ".swift", ".kt", ".scala",
        ];

        let mut file_actions: HashMap<String, usize> = HashMap::new();

        for msg in messages {
            let action = msg.tool_name.as_deref().unwrap_or("reference");
            for word in msg.content.split_whitespace() {
                let clean = word.trim_matches(|c: char| {
                    c == '"'
                        || c == '\''
                        || c == ','
                        || c == ';'
                        || c == ':'
                        || c == '('
                        || c == ')'
                        || c == '['
                        || c == ']'
                });
                if FILE_EXTENSIONS
                    .iter()
                    .any(|ext| clean.ends_with(ext) && clean.len() > ext.len())
                {
                    *file_actions
                        .entry(format!("{clean} ({action})"))
                        .or_default() += 1;
                }
            }
        }

        if file_actions.is_empty() {
            return None;
        }

        let count = file_actions.len();
        let mut xml = format!("<snapshot><files count=\"{count}\">");
        let max_bytes: usize = 2048;

        let mut sorted: Vec<_> = file_actions.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));

        for (i, (file, n)) in sorted.iter().enumerate() {
            let entry = if i > 0 {
                format!(" {file} (action×{n})")
            } else {
                format!("{file} (action×{n})")
            };
            if xml.len() + entry.len() + "</files></snapshot>".len() > max_bytes {
                break;
            }
            xml.push_str(&entry);
        }
        xml.push_str("</files></snapshot>");
        Some(xml)
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
        let keywords = [
            "decided",
            "chosen",
            "agreed",
            "will use",
            "we should",
            "let's use",
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
        decisions
    }

    fn extract_code_changes(&self, messages: &[Message]) -> String {
        // Count tool calls that look like file writes.
        let write_ops = messages
            .iter()
            .filter(|m| {
                m.is_tool_result()
                    && m.tool_name.as_deref().is_some_and(|n| {
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
        let mut keyword_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
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
        let keywords = [
            "prefer",
            "like",
            "always",
            "never",
            "use",
            "don't use",
            "avoid",
            "recommend",
        ];
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
            .find(|m| matches!(m.role, MessageRole::Assistant) && !m.content.trim().is_empty())
            .map_or_else(
                || "State unknown.".into(),
                |m| {
                    let s: String = m.content.chars().take(500).collect();
                    s
                },
            )
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
            combined.chars().take(4000).collect::<String>()
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
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Message, MessageRole};

    fn msg(role: MessageRole, content: &str) -> Message {
        Message {
            turn_index: 0,
            role,
            content: content.into(),
            tool_use_id: None,
            tool_name: None,
            pinned: false,
        }
    }

    #[test]
    fn should_compact_false_below_threshold() {
        let compactor = SessionCompactor::new();
        let messages = vec![
            msg(MessageRole::User, "hi"),
            msg(MessageRole::Assistant, "hello"),
        ];
        assert!(!compactor.should_compact(&messages));
    }

    #[test]
    fn should_compact_true_exceeding_threshold() {
        let mut compactor = SessionCompactor::new();
        compactor.config.threshold_tokens = 1;
        compactor.config.min_messages_to_compact = 3;
        let messages = vec![
            msg(MessageRole::User, &"x".repeat(100)),
            msg(MessageRole::Assistant, &"y".repeat(100)),
            msg(MessageRole::User, &"z".repeat(100)),
        ];
        assert!(compactor.should_compact(&messages));
    }

    #[test]
    fn split_messages_preserves_recent() {
        let compactor = SessionCompactor::new();
        let messages: Vec<_> = (0..15)
            .map(|i| msg(MessageRole::User, &format!("msg{i}")))
            .collect();
        let (old, recent) = compactor.split_messages(messages);
        assert_eq!(recent.len(), 10);
        assert_eq!(old.len(), 5);
    }

    #[test]
    fn extract_decisions_finds_keywords() {
        let compactor = SessionCompactor::new();
        let messages = vec![msg(
            MessageRole::User,
            "I decided to use Axum for the web framework",
        )];
        let decisions = compactor.extract_decisions(&messages);
        assert!(!decisions.is_empty());
    }

    #[test]
    fn generate_summary_template_produces_structured_output() {
        let compactor = SessionCompactor::new();
        let messages = vec![
            msg(MessageRole::User, "We decided to use Rust"),
            msg(MessageRole::Assistant, "I will implement the API"),
        ];
        let summary = compactor.generate_summary_template(&messages, None);
        assert!(summary.contains("Compressed Session Summary"));
        assert!(summary.contains("### Active Task"));
        assert!(summary.contains("### Key Decisions"));
        assert!(summary.contains("### Blocked"));
        assert!(summary.contains("### Pending User Asks"));
        assert!(summary.contains("### Critical Context"));
    }

    #[test]
    fn infer_current_state_uses_last_assistant() {
        let compactor = SessionCompactor::new();
        let messages = vec![
            msg(MessageRole::User, "hello"),
            msg(MessageRole::Assistant, "The server is deployed and running"),
        ];
        let state = compactor.infer_current_state(&messages);
        assert!(state.contains("deployed"));
    }

    #[test]
    fn extract_questions_detects_queries() {
        let compactor = SessionCompactor::new();
        let messages = vec![msg(MessageRole::User, "What is the best approach?")];
        let questions = compactor.extract_questions(&messages);
        assert!(!questions.contains("No open questions"));
    }
}
