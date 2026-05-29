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

use crate::{ MemoryScope,
    compression::llm_summarizer::LlmSummarizer,
    config::ExtractorConfig,
    error::MemoryError,
    splitter,
    store::MemoryStore,
    types::{
        AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Message,
        MessageRole, Priority,
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
    /// Optional LLM client for Pass 5 extraction enhancement.
    llm_client: Option<Arc<dyn LlmSummarizer>>,
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
            llm_client: None,
        }
    }

    /// Attach an LLM summariser for Pass 5 extraction enhancement.
    #[must_use]
    pub fn with_llm(mut self, llm: Arc<dyn LlmSummarizer>) -> Self {
        self.llm_client = Some(llm);
        self
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Return `true` when the message slice is worth extracting from.
    ///
    /// Skips trivial conversations (insufficient turns or content) to avoid
    /// persisting noise. Tool activity alone is not a positive signal — only
    /// substantive user and assistant text content triggers extraction.
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

        // Consider worth extracting based on total substantive text content
        // (user + assistant). Tool activity is NOT a positive signal because
        // tool execution data is machine-optimised and can be re-derived by
        // re-running the tool.
        let user_content_len: usize = messages
            .iter()
            .filter(|m| m.role == MessageRole::User)
            .map(|m| m.content.len())
            .sum();
        let assistant_content_len: usize = messages
            .iter()
            .filter(|m| m.role == MessageRole::Assistant)
            .map(|m| m.content.len())
            .sum();

        // Threshold: 50 chars covers most daily conversations.
        user_content_len + assistant_content_len >= 50
    }

    /// Return a reference to the LLM client, if one is configured.
    pub fn llm_client(&self) -> Option<&Arc<dyn LlmSummarizer>> {
        self.llm_client.as_ref()
    }

    /// Run the four heuristic extraction passes over `messages` (only).
    ///
    /// Returns raw entries **without** confidence filtering, de-duplication,
    /// or `batch_size` truncation.  Call [`finalize_entries`] afterwards to
    /// apply those post-processing steps.
    pub fn extract_heuristic(&self, messages: &[Message]) -> Vec<MemoryEntry> {
        if !Self::should_extract(messages) {
            return Vec::new();
        }
        let chunked = Self::chunk_large_messages(messages);
        let mut entries = Vec::new();
        entries.extend(self.extract_preferences(&chunked));
        entries.extend(self.extract_decisions(&chunked));
        entries.extend(self.extract_error_fixes(&chunked));
        entries.extend(self.extract_patterns(&chunked));
        entries
    }

    /// Finalise a batch of entries: filter by `min_confidence`, de-duplicate
    /// by normalised title, and truncate to `batch_size`.
    pub fn finalize_entries(&self, mut entries: Vec<MemoryEntry>) -> Vec<MemoryEntry> {
        entries.retain(|e| e.confidence >= self.config.min_confidence);
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
        entries.truncate(self.config.batch_size);
        entries
    }

    /// Extract meaningful [`MemoryEntry`] items from `messages`.
    ///
    /// Returns up to `config.batch_size` entries, each with a confidence score
    /// at or above `config.min_confidence`.  Entries are de-duplicated by
    /// title before returning.
    ///
    /// When an LLM client is attached via [`with_llm`], a fifth pass calls the
    /// LLM for deeper extraction; on failure the heuristic results are used
    /// as-is.
    pub async fn extract(&self, messages: &[Message]) -> Result<Vec<MemoryEntry>> {
        let mut entries = self.extract_heuristic(messages);

        // Pass 5 – LLM-enhanced extraction (optional)
        if self.llm_client.is_some() {
            let chunked = Self::chunk_large_messages(messages);
            match self.llm_extract(&chunked).await {
                Ok(llm_entries) => {
                    tracing::info!(
                        count = llm_entries.len(),
                        "LLM Pass 5 extracted {} entries",
                        llm_entries.len()
                    );
                    entries = self.merge_entries(entries, llm_entries);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "LLM Pass 5 extraction failed, continuing with heuristic-only results"
                    );
                }
            }
        }

        Ok(self.finalize_entries(entries))
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
            let entries = extractor.extract(&messages).await?;

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

    /// Split messages > 2000 chars into smaller chunks for better extraction precision.
    fn chunk_large_messages(messages: &[Message]) -> Vec<Message> {
        let mut out = Vec::with_capacity(messages.len());
        for msg in messages {
            if msg.content.len() > 2000 {
                let chunks = splitter::semantic_split(&msg.content, 1500);
                for chunk in chunks {
                    let mut chunk_msg = msg.clone();
                    chunk_msg.content = chunk;
                    out.push(chunk_msg);
                }
            } else {
                out.push(msg.clone());
            }
        }
        out
    }

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
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
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

    // -------------------------------------------------------------------
    // Pass 5 – LLM-enhanced extraction
    // -------------------------------------------------------------------

    /// Run the LLM extraction pass over the given messages.
    pub async fn llm_extract(&self, messages: &[Message]) -> Result<Vec<MemoryEntry>> {
        let prompt = Self::build_extraction_prompt();
        let content_text = Self::format_messages_for_llm(messages);

        let llm = self
            .llm_client
            .as_ref()
            .ok_or_else(|| MemoryError::Other("LLM client not configured".into()))?;

        let response = llm
            .summarize(&prompt, &content_text)
            .await
            .map_err(|e| MemoryError::Other(format!("LLM extraction failed: {e}")))?;

        let trimmed = response.trim();
        // Strip markdown fences if present.
        let json_str = if trimmed.starts_with("```") {
            trimmed
                .trim_start_matches("```json")
                .trim_start_matches("```")
                .trim_end_matches("```")
                .trim()
        } else {
            trimmed
        };

        let llm_entries: Vec<LlmExtractedEntry> = serde_json::from_str(json_str).map_err(|e| {
            tracing::warn!(
                error = %e,
                raw_preview = %json_str.chars().take(400).collect::<String>(),
                "LLM extraction: failed to parse JSON response",
            );
            MemoryError::Other(format!("LLM JSON parse error: {e}"))
        })?;

        Ok(llm_entries
            .into_iter()
            .map(LlmExtractedEntry::into_memory_entry)
            .collect())
    }

    /// Build the extraction prompt instructing the LLM to return a JSON array.
    fn build_extraction_prompt() -> String {
        r#"You are a memory extraction system. Analyze the following conversation and extract key memories as a JSON array.

Each memory entry must have these fields:
- "title": A short, clear title (max 80 characters)
- "content": Detailed description of the memory
- "category": One of "UserPreference", "Decision", "Reference", "ProjectConvention", "ProjectKnowledge", "CompressedSummary", "Shared"
- "layer": One of "L1" (critical identity/preferences), "L2" (project context/decisions), "L3" (reference/patterns)
- "priority": One of "High", "Normal", "Low"
- "confidence": A float from 0.0 to 1.0
- "tags": Array of short keyword strings

Extraction guidelines:
- Extract user preferences and coding style preferences as "UserPreference" / "L1"
- Extract architectural decisions and approach choices as "Decision" / "L2"
- Extract error resolutions, fixes, and workarounds as "Reference" / "L2"
- Extract repeated patterns and conventions as "ProjectConvention" / "L2"
- Extract project facts and entity knowledge as "ProjectKnowledge" / "L2"
- Extract summaries and compressed content as "CompressedSummary" / "L3"
- Extract cross-agent shared knowledge as "Shared" / "L4"
- L1 entries should have confidence >= 0.8; L2/L3 >= 0.6

Only extract genuinely new and useful information. Skip trivial, obvious, or redundant content.
Return ONLY the JSON array, no other text or explanation."#
            .to_string()
    }

    /// Format messages into a text block suitable for LLM consumption.
    ///
    /// Tool output is truncated to 500 chars to keep the prompt manageable.
    fn format_messages_for_llm(messages: &[Message]) -> String {
        let mut out = String::new();
        for msg in messages {
            let role_label = match msg.role {
                MessageRole::User => "User",
                MessageRole::Assistant => "Assistant",
                MessageRole::Tool => {
                    let name = msg.tool_name.as_deref().unwrap_or("unknown");
                    out.push_str(&format!(
                        "[Tool {}]: {}\n",
                        name,
                        Self::truncate(&msg.content, 500),
                    ));
                    continue;
                }
                MessageRole::System => continue,
            };
            out.push_str(&format!("[{role_label}]: {}\n", msg.content));
        }
        out
    }

    /// Merge heuristic entries with LLM entries, keeping the highest-confidence
    /// version of each title and respecting `batch_size`.
    fn merge_entries(
        &self,
        heuristic: Vec<MemoryEntry>,
        llm: Vec<MemoryEntry>,
    ) -> Vec<MemoryEntry> {
        let mut merged = heuristic;
        merged.extend(llm);

        // Deduplicate by normalised title, preferring higher confidence.
        let mut seen: HashMap<String, (usize, f32)> = HashMap::new();
        let mut i = 0;
        while i < merged.len() {
            let key = merged[i].title.to_lowercase();
            if let Some(&(prev_idx, prev_conf)) = seen.get(&key) {
                if merged[i].confidence > prev_conf {
                    merged.swap_remove(prev_idx);
                    // Rebuild the index after mutation.
                    seen.clear();
                    i = 0;
                    continue;
                }
                merged.swap_remove(i);
                continue;
            }
            seen.insert(key, (i, merged[i].confidence));
            i += 1;
        }

        merged.truncate(self.config.batch_size);
        merged
    }
}

// ---------------------------------------------------------------------------
// LlmExtractedEntry – deserialisation helper for LLM JSON responses
// ---------------------------------------------------------------------------

/// Simplified entry deserialised from the LLM JSON response.
#[derive(Debug, serde::Deserialize)]
struct LlmExtractedEntry {
    title: String,
    content: String,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    layer: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    confidence: Option<f32>,
    #[serde(default)]
    tags: Option<Vec<String>>,
}

impl LlmExtractedEntry {
    fn into_memory_entry(self) -> MemoryEntry {
        let category = self
            .category
            .as_deref()
            .and_then(|c| match c {
                "UserPreference" => Some(MemoryCategory::UserPreference),
                "Decision" => Some(MemoryCategory::Decision),
                "Reference" => Some(MemoryCategory::Reference),
                "ProjectConvention" => Some(MemoryCategory::ProjectConvention),
                "ProjectKnowledge" => Some(MemoryCategory::ProjectKnowledge),
                "CompressedSummary" => Some(MemoryCategory::CompressedSummary),
                "Shared" => Some(MemoryCategory::Shared),
                _ => None,
            })
            .unwrap_or(MemoryCategory::Reference);

        let layer = self
            .layer
            .as_deref()
            .and_then(|l| match l {
                "L1" => Some(MemoryLayer::L1),
                "L2" => Some(MemoryLayer::L2),
                "L3" => Some(MemoryLayer::L3),
                _ => None,
            })
            .unwrap_or(MemoryLayer::L2);

        let priority = self
            .priority
            .as_deref()
            .and_then(|p| match p {
                "High" => Some(Priority::High),
                "Normal" => Some(Priority::Normal),
                "Low" => Some(Priority::Low),
                _ => None,
            })
            .unwrap_or(Priority::Normal);

        let now = Utc::now();
        MemoryEntry {
            id: Uuid::new_v4(),
            layer,
            category,
            priority,
            source: MemorySource::AutoExtracted,
            title: self.title,
            content: self.content,
            embedding: None,
            tags: self.tags.unwrap_or_default(),
            relations: vec![],
            confidence: self.confidence.unwrap_or(0.7),
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::default(),
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
    fn should_extract_returns_true_for_substantive_content() {
        let msgs = make_messages();
        assert!(MemoryExtractor::should_extract(&msgs));
    }

    #[test]
    fn should_extract_returns_false_when_only_tool_activity() {
        // Tool-only messages should NOT trigger extraction — the content
        // is machine-optimised and can be re-derived.
        let msgs = vec![
            Message::user(""),
            Message::assistant(""),
            {
                let mut m = Message::tool_result("t1", "bash", "ok");
                m.role = MessageRole::Tool;
                m
            },
        ];
        assert!(
            !MemoryExtractor::should_extract(&msgs),
            "tool-only messages should not trigger extraction"
        );
    }

    #[tokio::test]
    async fn extract_yields_preference_entry() {
        let ex = default_extractor();
        let msgs = make_messages();
        let entries = ex.extract(&msgs).await.unwrap();
        let pref = entries
            .iter()
            .find(|e| e.category == MemoryCategory::UserPreference);
        assert!(pref.is_some(), "expected a UserPreference entry");
    }

    #[tokio::test]
    async fn extract_yields_decision_entry() {
        let ex = default_extractor();
        let msgs = make_messages();
        let entries = ex.extract(&msgs).await.unwrap();
        let dec = entries
            .iter()
            .find(|e| e.category == MemoryCategory::Decision);
        assert!(dec.is_some(), "expected a Decision entry");
    }

    #[tokio::test]
    async fn extract_yields_error_fix_entry() {
        let ex = default_extractor();
        let msgs = make_messages();
        let entries = ex.extract(&msgs).await.unwrap();
        let fix = entries
            .iter()
            .find(|e| e.category == MemoryCategory::Reference && e.tags.contains(&"fix".into()));
        assert!(fix.is_some(), "expected an error-fix Reference entry");
    }

    #[tokio::test]
    async fn extract_empty_for_trivial_conversation() {
        let ex = default_extractor();
        let msgs = vec![
            Message::user("hi"),
            Message::assistant("hello"),
        ];
        let entries = ex.extract(&msgs).await.unwrap();
        // Should_extract returns false → empty result.
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn confidence_filter_applied() {
        let ex = MemoryExtractor::new(ExtractorConfig {
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.99, // impossibly high
        });
        let msgs = make_messages();
        let entries = ex.extract(&msgs).await.unwrap();
        assert!(entries.is_empty(), "all entries should be filtered out");
    }
}
