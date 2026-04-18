//! Background memory extractor.
//!
//! Runs as a Tokio task, analysing recent conversation history and
//! automatically persisting noteworthy observations into the memory store.
//!
//! # Extraction strategy
//!
//! The extractor is **heuristic-only** – it does not require an LLM call.
//! It applies four independent passes over the message slice:
//!
//! 1. **Preference pass** – scans user messages for natural-language preference
//!    signals ("I like", "please always", "never", "don't", …) and records
//!    them as [`MemoryCategory::UserPreference`] / [`MemoryLayer::L1`].
//!
//! 2. **Decision pass** – scans assistant messages for decision language
//!    ("I've decided", "we'll use", "the approach is", …) and records them as
//!    [`MemoryCategory::Decision`] / [`MemoryLayer::L2`].
//!
//! 3. **Error-fix pass** – detects error→fix sequences (tool result with an
//!    error immediately followed by a corrective assistant turn) and records
//!    the resolution as [`MemoryCategory::Reference`] / [`MemoryLayer::L2`].
//!
//! 4. **Pattern pass** – counts repeated tool invocations and records
//!    frequently-used commands as [`MemoryCategory::ProjectConvention`] /
//!    [`MemoryLayer::L2`].
//!
//! # Derivation-exclusion principle
//!
//! Only information that **cannot** be re-derived by re-running a tool is
//! persisted.  Raw file contents, search results, and command outputs are
//! explicitly skipped.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use chrono::Utc;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use uuid::Uuid;

use crate::{
    config::ExtractorConfig,
    error::MemoryError,
    store::MemoryStore,
    types::{
        MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Message, MessageRole, Priority,
    },
};

/// Result alias used throughout this module.
pub type Result<T> = std::result::Result<T, MemoryError>;

// ---------------------------------------------------------------------------
// Constants – preference-signal keywords (lower-case)
// ---------------------------------------------------------------------------

/// English and Chinese preference-signal fragments scanned in user messages.
const PREF_SIGNALS: &[&str] = &[
    // English
    "i like",
    "i prefer",
    "i always",
    "i usually",
    "please always",
    "please never",
    "always use",
    "never use",
    "don't use",
    "do not use",
    "stop using",
    "i want",
    "i need",
    "make sure",
    "ensure that",
    "remember that",
    "keep in mind",
    // Chinese
    "我喜欢",
    "我偏好",
    "我希望",
    "请总是",
    "请始终",
    "不要",
    "别再",
    "请不要",
    "记住",
    "确保",
    "每次都",
    "我需要",
];

/// English and Chinese decision-signal fragments scanned in assistant messages.
const DECISION_SIGNALS: &[&str] = &[
    // English
    "i've decided",
    "i have decided",
    "we'll use",
    "we will use",
    "the approach is",
    "the solution is",
    "the strategy is",
    "i'll go with",
    "i will go with",
    "chosen approach",
    "decision:",
    "decided to",
    "opted for",
    // Chinese
    "决定使用",
    "选择了",
    "方案是",
    "我们将使用",
    "决策：",
    "采用",
    "最终方案",
];

/// Fragments in tool-result content that indicate an error occurred.
const ERROR_SIGNALS: &[&str] = &[
    "error:",
    "error ",
    "failed",
    "failure",
    "exception",
    "traceback",
    "panicked",
    "cannot",
    "could not",
    "not found",
    "permission denied",
    "錯誤",
    "失败",
    "异常",
];

// ---------------------------------------------------------------------------
// MemoryExtractor
// ---------------------------------------------------------------------------

/// Background task that extracts memories from the conversation stream.
///
/// Create with [`MemoryExtractor::new`] and either call [`MemoryExtractor::extract`]
/// directly for unit tests / synchronous use, or spawn a background task with
/// [`MemoryExtractor::spawn_background`] / [`MemoryExtractor::spawn`].
pub struct MemoryExtractor {
    config: ExtractorConfig,
    /// Guards against concurrent extraction runs.
    running: Arc<AtomicBool>,
}

impl MemoryExtractor {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new extractor from `config`.
    #[must_use]
    pub fn new(config: ExtractorConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Return `true` when the message slice is worth extracting from.
    ///
    /// Skips trivial conversations (pure Q&A with no tool activity or fewer
    /// than two turns) to avoid persisting noise.
    #[must_use]
    pub fn should_extract(messages: &[Message]) -> bool {
        // Need at least two messages.
        if messages.len() < 2 {
            return false;
        }

        // At least one user message and one assistant message.
        let has_user = messages
            .iter()
            .any(|m| m.role == MessageRole::User);
        let has_assistant = messages
            .iter()
            .any(|m| m.role == MessageRole::Assistant);

        if !has_user || !has_assistant {
            return false;
        }

        // Consider worth extracting if there is at least one tool call/result
        // OR the total user-content length exceeds a threshold (200 chars).
        let has_tool_activity = messages
            .iter()
            .any(|m| m.role == MessageRole::Tool || m.tool_use_id.is_some());

        let user_content_len: usize = messages
            .iter()
            .filter(|m| m.role == MessageRole::User)
            .map(|m| m.content.len())
            .sum();

        has_tool_activity || user_content_len > 200
    }

    /// Extract meaningful [`MemoryEntry`] items from `messages`.
    ///
    /// Returns up to `config.batch_size` entries, each with a confidence score
    /// at or above `config.min_confidence`.  Entries are de-duplicated by
    /// title before returning.
    pub fn extract(&self, messages: &[Message]) -> Result<Vec<MemoryEntry>> {
        if !Self::should_extract(messages) {
            return Ok(Vec::new());
        }

        let mut entries: Vec<MemoryEntry> = Vec::new();

        entries.extend(self.extract_preferences(messages));
        entries.extend(self.extract_decisions(messages));
        entries.extend(self.extract_error_fixes(messages));
        entries.extend(self.extract_patterns(messages));

        // Filter by minimum confidence.
        entries.retain(|e| e.confidence >= self.config.min_confidence);

        // De-duplicate by (normalised) title.
        let mut seen_titles: HashMap<String, ()> = HashMap::new();
        entries.retain(|e| {
            let key = e.title.to_lowercase();
            if let std::collections::hash_map::Entry::Vacant(e) = seen_titles.entry(key) {
                e.insert(());
                true
            } else {
                false
            }
        });

        // Respect the configured batch size.
        entries.truncate(self.config.batch_size);

        Ok(entries)
    }

    /// Spawn a **one-shot** background extraction task.
    ///
    /// Uses an [`AtomicBool`] guard so only one extraction runs at a time.
    /// After extraction the resulting entries are written to `store`.
    ///
    /// Returns a [`JoinHandle`] that resolves to the list of extracted entries,
    /// or an error if extraction or storage failed.
    pub fn spawn_background(
        &self,
        messages: Vec<Message>,
        store: Arc<dyn MemoryStore>,
    ) -> JoinHandle<Result<Vec<MemoryEntry>>> {
        let config = self.config.clone();
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            // Mutex-style guard: if already running, skip this cycle.
            if running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                tracing::debug!("memory extractor: skipping – another extraction is running");
                return Ok(Vec::new());
            }

            // Ensure the flag is cleared even on early return / panic.
            let _guard = RunningGuard(&running);

            let extractor = MemoryExtractor::new(config);
            let entries = extractor.extract(&messages)?;

            // Persist each entry to the store.
            let mut persisted: Vec<MemoryEntry> = Vec::with_capacity(entries.len());
            for entry in entries {
                match store.insert(&entry).await {
                    Ok(_id) => {
                        tracing::debug!(
                            title = %entry.title,
                            layer = ?entry.layer,
                            category = ?entry.category,
                            "memory extractor: persisted entry"
                        );
                        persisted.push(entry);
                    }
                    Err(e) => {
                        tracing::warn!(
                            title = %entry.title,
                            error = %e,
                            "memory extractor: failed to persist entry"
                        );
                    }
                }
            }

            Ok(persisted)
        })
    }

    /// Spawn the extractor as a **polling** background Tokio task.
    ///
    /// This variant is used when the extractor owns the message buffer
    /// internally (e.g. wired into a live session).  It ticks at the
    /// configured `poll_interval_secs` and calls [`Self::poll`] each cycle.
    ///
    /// Returns a `JoinHandle` that the caller can await or abort.
    #[must_use] 
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(self.config.poll_interval_secs));
            loop {
                ticker.tick().await;
                if let Err(e) = self.poll().await {
                    tracing::warn!("memory extractor poll error: {e}");
                }
            }
        })
    }

    // -----------------------------------------------------------------------
    // Private – polling
    // -----------------------------------------------------------------------

    /// Single extraction poll cycle (no-op placeholder for the polling path).
    ///
    /// In a fully integrated deployment this would obtain the current message
    /// buffer from a shared state handle.  For now it is a no-op that the
    /// polling `spawn` variant calls periodically.
    async fn poll(&self) -> Result<()> {
        // NOTE: The poll-based flow requires access to a shared message buffer
        // and MemoryStore, both of which are not part of the extractor struct.
        // Callers that need live polling should wire those in via a custom
        // wrapper or use `spawn_background` per-turn instead.
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private – extraction passes
    // -----------------------------------------------------------------------

    /// Pass 1 – user preference extraction.
    fn extract_preferences(&self, messages: &[Message]) -> Vec<MemoryEntry> {
        let mut entries = Vec::new();

        for msg in messages.iter().filter(|m| m.role == MessageRole::User) {
            let lower = msg.content.to_lowercase();

            for signal in PREF_SIGNALS {
                if let Some(pos) = lower.find(signal) {
                    // Extract the remainder of the sentence (up to 200 chars).
                    let snippet = Self::extract_sentence(&msg.content, pos, 200);
                    if snippet.len() < 10 {
                        continue;
                    }

                    let title = format!("User preference: {}", Self::truncate(&snippet, 60));
                    entries.push(Self::build_entry(
                        title,
                        snippet,
                        MemoryLayer::L1,
                        MemoryCategory::UserPreference,
                        Priority::High,
                        0.85,
                        vec!["preference".into(), "user".into()],
                    ));
                    // Only extract one preference signal per message to avoid
                    // flooding the store with near-duplicate entries.
                    break;
                }
            }
        }

        entries
    }

    /// Pass 2 – decision extraction from assistant messages.
    fn extract_decisions(&self, messages: &[Message]) -> Vec<MemoryEntry> {
        let mut entries = Vec::new();

        for msg in messages.iter().filter(|m| m.role == MessageRole::Assistant) {
            let lower = msg.content.to_lowercase();

            for signal in DECISION_SIGNALS {
                if let Some(pos) = lower.find(signal) {
                    let snippet = Self::extract_sentence(&msg.content, pos, 250);
                    if snippet.len() < 15 {
                        continue;
                    }

                    let title = format!("Decision: {}", Self::truncate(&snippet, 60));
                    entries.push(Self::build_entry(
                        title,
                        snippet,
                        MemoryLayer::L2,
                        MemoryCategory::Decision,
                        Priority::High,
                        0.75,
                        vec!["decision".into()],
                    ));
                    break;
                }
            }
        }

        entries
    }

    /// Pass 3 – error-fix sequence detection.
    ///
    /// Looks for a tool-result message containing an error signal immediately
    /// followed by an assistant message that likely contains the fix.
    fn extract_error_fixes(&self, messages: &[Message]) -> Vec<MemoryEntry> {
        let mut entries = Vec::new();

        for window in messages.windows(2) {
            let (prev, next) = (&window[0], &window[1]);

            // Pattern: tool result with error → assistant response.
            if prev.role != MessageRole::Tool || next.role != MessageRole::Assistant {
                continue;
            }

            let error_lower = prev.content.to_lowercase();
            let has_error = ERROR_SIGNALS.iter().any(|s| error_lower.contains(s));
            if !has_error {
                continue;
            }

            // Extract the tool name and a brief error description.
            let tool_name = prev
                .tool_name
                .as_deref()
                .unwrap_or("unknown_tool");

            // Summarise: first 120 chars of the error content.
            let error_summary = Self::truncate(&prev.content, 120);
            let fix_summary = Self::truncate(&next.content, 200);

            let content = format!(
                "**Tool**: `{tool_name}`\n\
                 **Error**: {error_summary}\n\
                 **Fix applied**: {fix_summary}"
            );

            let title = format!(
                "Error fix: {} – {}",
                tool_name,
                Self::truncate(&error_summary, 40)
            );

            entries.push(Self::build_entry(
                title,
                content,
                MemoryLayer::L2,
                MemoryCategory::Reference,
                Priority::Normal,
                0.70,
                vec!["error".into(), "fix".into(), tool_name.into()],
            ));
        }

        entries
    }

    /// Pass 4 – repeated tool-usage pattern detection.
    ///
    /// If the same tool is called 3 or more times in a conversation it is
    /// likely a project convention worth remembering.
    fn extract_patterns(&self, messages: &[Message]) -> Vec<MemoryEntry> {
        let mut tool_counts: HashMap<String, usize> = HashMap::new();

        for msg in messages {
            if let Some(name) = &msg.tool_name {
                *tool_counts.entry(name.clone()).or_insert(0) += 1;
            }
            // Also count tool-use markers embedded in assistant messages
            // (e.g. `<tool_use>read_file</tool_use>`).
            if msg.role == MessageRole::Assistant {
                // Simple heuristic: count occurrences of known tool patterns.
                for keyword in &["read_file", "write_file", "run_bash", "search_code"] {
                    let count = msg
                        .content
                        .to_lowercase()
                        .matches(keyword)
                        .count();
                    if count > 0 {
                        *tool_counts
                            .entry((*keyword).to_string())
                            .or_insert(0) += count;
                    }
                }
            }
        }

        let mut entries = Vec::new();

        for (tool, count) in &tool_counts {
            if *count < 3 {
                continue;
            }

            let title = format!("Frequent tool usage: `{tool}` ({count}×)");
            let content = format!(
                "The tool `{tool}` was invoked {count} times in this session, \
                 indicating it is a regularly used operation."
            );

            entries.push(Self::build_entry(
                title,
                content,
                MemoryLayer::L2,
                MemoryCategory::ProjectConvention,
                Priority::Low,
                0.65,
                vec!["pattern".into(), "tool".into(), tool.clone()],
            ));
        }

        entries
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Build a complete [`MemoryEntry`] with sensible defaults.
    fn build_entry(
        title: String,
        content: String,
        layer: MemoryLayer,
        category: MemoryCategory,
        priority: Priority,
        confidence: f32,
        tags: Vec<String>,
    ) -> MemoryEntry {
        let now = Utc::now();
        MemoryEntry {
            id: Uuid::new_v4(),
            layer,
            category,
            priority,
            source: MemorySource::AutoExtracted,
            title,
            content,
            embedding: None,
            tags,
            relations: vec![],
            confidence,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: None,
            session_id: None,
        }
    }

    /// Extract a sentence-like snippet from `text` starting at byte offset
    /// `start`, up to `max_len` characters.  Tries to end at a sentence
    /// boundary (`.`, `!`, `?`, `\n`).
    fn extract_sentence(text: &str, start: usize, max_len: usize) -> String {
        // Guard against non-char-boundary start (can happen with multi-byte).
        let safe_start = text
            .char_indices()
            .map(|(i, _)| i).rfind(|&i| i <= start)
            .unwrap_or(0);

        let slice = &text[safe_start..];
        let capped: String = slice.chars().take(max_len).collect();

        // Try to trim to the last sentence boundary.
        for ch in ['\n', '.', '!', '?'] {
            if let Some(pos) = capped.rfind(ch) {
                if pos > max_len / 3 {
                    return capped[..=pos].trim().to_string();
                }
            }
        }

        capped.trim().to_string()
    }

    /// Truncate `s` to at most `max_chars` characters, appending `…` if cut.
    fn truncate(s: &str, max_chars: usize) -> String {
        let chars: Vec<char> = s.chars().collect();
        if chars.len() <= max_chars {
            s.to_string()
        } else {
            let mut out: String = chars[..max_chars].iter().collect();
            out.push('…');
            out
        }
    }
}

// ---------------------------------------------------------------------------
// RAII guard – clears `running` flag when dropped.
// ---------------------------------------------------------------------------

struct RunningGuard<'a>(&'a AtomicBool);

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExtractorConfig;
    use crate::types::{Message, MessageRole};

    fn default_extractor() -> MemoryExtractor {
        MemoryExtractor::new(ExtractorConfig {
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.5,
        })
    }

    fn make_messages() -> Vec<Message> {
        vec![
            Message::user("I prefer using tabs for indentation, please always use tabs."),
            Message::assistant(
                "Understood. I've decided we'll use tabs for all Rust files in this project.",
            ),
            {
                let mut m = Message::tool_result(
                    "tool-1",
                    "run_bash",
                    "error: cargo build failed\nsome compilation error",
                );
                m.role = MessageRole::Tool;
                m
            },
            Message::assistant(
                "The compilation error was due to a missing semicolon. I've fixed it by adding \
                 the semicolon on line 42.",
            ),
        ]
    }

    #[test]
    fn should_extract_returns_false_for_empty() {
        assert!(!MemoryExtractor::should_extract(&[]));
    }

    #[test]
    fn should_extract_returns_true_for_tool_activity() {
        let msgs = make_messages();
        assert!(MemoryExtractor::should_extract(&msgs));
    }

    #[test]
    fn extract_yields_preference_entry() {
        let ex = default_extractor();
        let msgs = make_messages();
        let entries = ex.extract(&msgs).unwrap();
        let pref = entries
            .iter()
            .find(|e| e.category == MemoryCategory::UserPreference);
        assert!(pref.is_some(), "expected a UserPreference entry");
    }

    #[test]
    fn extract_yields_decision_entry() {
        let ex = default_extractor();
        let msgs = make_messages();
        let entries = ex.extract(&msgs).unwrap();
        let dec = entries
            .iter()
            .find(|e| e.category == MemoryCategory::Decision);
        assert!(dec.is_some(), "expected a Decision entry");
    }

    #[test]
    fn extract_yields_error_fix_entry() {
        let ex = default_extractor();
        let msgs = make_messages();
        let entries = ex.extract(&msgs).unwrap();
        let fix = entries
            .iter()
            .find(|e| e.category == MemoryCategory::Reference && e.tags.contains(&"fix".into()));
        assert!(fix.is_some(), "expected an error-fix Reference entry");
    }

    #[test]
    fn extract_empty_for_trivial_conversation() {
        let ex = default_extractor();
        let msgs = vec![
            Message::user("hi"),
            Message::assistant("hello"),
        ];
        let entries = ex.extract(&msgs).unwrap();
        // Should_extract returns false → empty result.
        assert!(entries.is_empty());
    }

    #[test]
    fn confidence_filter_applied() {
        let ex = MemoryExtractor::new(ExtractorConfig {
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.99, // impossibly high
        });
        let msgs = make_messages();
        let entries = ex.extract(&msgs).unwrap();
        assert!(entries.is_empty(), "all entries should be filtered out");
    }
}
