//! Compression pipeline for the memory system.
//!
//! The pipeline consists of three sequential stages:
//! - **Stage 1** (`micro`): per-turn lightweight compression (tool truncation,
//!   time decay, duplicate merging).
//! - **Stage 2** (`session`): token-threshold triggered medium compression
//!   (structured summary + L2/L3 memory extraction).
//! - **Stage 3** (`deep`): extreme-pressure heavy compression (iterative
//!   summary update + all-content distillation).
//!
//! Supporting modules handle token-budget tracking (`budget`), circuit-breaker
//! / recursion guards (`guard`), and real-time context monitoring (`monitor`).

pub mod budget;
pub mod deep;
pub mod guard;
pub mod llm_summarizer;
pub mod micro;
pub mod monitor;
pub mod quality;
pub mod session;
pub mod token_estimation;

use crate::{
    compression::{
        deep::DeepCompactor,
        guard::CompressionGuard,
        llm_summarizer::{LlmSummarizer, OpenAiSummarizer},
        micro::MicroCompactor,
        session::SessionCompactor,
    },
    config::CompressionConfig,
    error::MemoryError,
    orchestrator::MemoryOrchestrator,
    types::{CompactionResult, Message, PreparedContext},
};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

/// Result alias for compression operations.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// Code markers used to estimate how "code-heavy" a set of messages is.
const CODE_MARKERS: &[&str] = &[
    "fn ", "let ", "impl ", "pub ", "use ", "struct ", "enum ", "mod ", "def ", "class ",
    "import ", "const ", "trait ", "async fn", "=>", "->", "::", "();",
];

/// Estimate the ratio of code lines to total lines in the given messages.
///
/// Returns a value in `[0.0, 1.0]`. Values above 0.5 suggest that AAAK
/// (lossless, entity-aware) compression would be more effective than the
/// default micro-compaction.
fn code_ratio(messages: &[Message]) -> f32 {
    let content: String = messages
        .iter()
        .map(|m| m.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let total_lines = content.lines().count().max(1);
    let code_lines = content
        .lines()
        .filter(|line| CODE_MARKERS.iter().any(|m| line.contains(m)))
        .count();
    code_lines as f32 / total_lines as f32
}

// ---------------------------------------------------------------------------
// CompressionPipeline
// ---------------------------------------------------------------------------

/// The top-level compression pipeline.
///
/// Exposes stage-specific entry points as well as a convenience `run` method
/// that executes all configured stages in order.
pub struct CompressionPipeline {
    /// Stage-1 micro compactor.
    micro: MicroCompactor,
    /// Stage-2 session compactor.
    session: SessionCompactor,
    /// Stage-3 deep compactor.
    deep: DeepCompactor,
    /// Recursion / circuit-breaker guard.
    guard: CompressionGuard,
    /// Whether stage-3 deep compression is enabled (requires an LLM call).
    pub enable_deep: bool,
    /// Previous summary for iterative update.
    pub previous_summary: StdMutex<Option<String>>,
    /// Savings percentage from last compaction (anti-thrashing).
    pub last_compression_savings_pct: StdMutex<f32>,
    /// Consecutive ineffective compressions (anti-thrashing).
    pub ineffective_compression_count: StdMutex<u32>,
    /// Cooldown timestamp for LLM summary failures.
    pub summary_cooldown_until: StdMutex<Option<std::time::Instant>>,
}

impl CompressionPipeline {
    /// Create a new pipeline with default settings.
    #[must_use]
    pub fn new(enable_deep: bool) -> Self {
        Self {
            micro: MicroCompactor::new(),
            session: SessionCompactor::new(),
            deep: DeepCompactor::new(),
            guard: CompressionGuard::new(),
            enable_deep,
            previous_summary: StdMutex::new(None),
            last_compression_savings_pct: StdMutex::new(100.0),
            ineffective_compression_count: StdMutex::new(0),
            summary_cooldown_until: StdMutex::new(None),
        }
    }

    /// Create from the global compression config.
    #[must_use]
    pub fn from_config(config: &CompressionConfig) -> Self {
        // Create LLM summarizer if configured
        let llm_summarizer: Option<Arc<dyn LlmSummarizer>> = if config.llm.is_configured() {
            let summarizer = OpenAiSummarizer::new(
                config.llm.api_url.clone(),
                config.llm.api_key.clone(),
                config.llm.model.clone(),
            );
            tracing::info!("LLM summarization enabled: {}", config.llm.model);
            Some(Arc::new(summarizer))
        } else {
            tracing::debug!("LLM summarization not configured, using template fallback");
            None
        };

        let mut pipeline = Self {
            micro: MicroCompactor::from_config(config),
            session: SessionCompactor::from_config(config),
            deep: DeepCompactor::from_config(config),
            guard: CompressionGuard::new(),
            enable_deep: config.enable_deep_compression,
            previous_summary: StdMutex::new(None),
            last_compression_savings_pct: StdMutex::new(100.0),
            ineffective_compression_count: StdMutex::new(0),
            summary_cooldown_until: StdMutex::new(None),
        };

        // Attach LLM summarizer to compactors that support it
        if let Some(ref summarizer) = llm_summarizer {
            pipeline.session = pipeline.session.with_llm_summarizer(summarizer.clone());
            pipeline.deep = pipeline.deep.with_llm_summarizer(summarizer.clone());
        }

        pipeline
    }

    // -----------------------------------------------------------------------
    // Stage 1 – Micro compact
    // -----------------------------------------------------------------------

    /// Execute Stage-1 (micro) compression in-place.
    ///
    /// Should be called after every assistant turn.
    pub fn micro_compact(&self, messages: &mut Vec<Message>) {
        // Micro compaction is lightweight enough that we don't need the guard.
        self.micro.compact(messages);
    }

    // -----------------------------------------------------------------------
    // AAAK code-mode compact
    // -----------------------------------------------------------------------

    /// Compress messages using AAAK (Adaptive Abbreviation with Association Knowledge).
    ///
    /// AAAK is an entity-aware, lossless compression format that excels on
    /// code-heavy content. It extracts repeated entities (function names, file
    /// paths, variable names) and replaces them with short abbreviations while
    /// preserving full decompressibility.
    ///
    /// When `code_ratio(messages) > 0.5`, this method is preferred over
    /// `micro_compact` because the entity repetition in code yields much
    /// higher compression ratios.
    pub fn aaak_compact(&self, messages: &mut Vec<Message>) {
        let mut compressor = crate::aaak_compression::AaakCompressor::default_compressor();
        for msg in messages.iter_mut() {
            if msg.pinned {
                continue;
            }
            let output = compressor.compress_auto(&msg.content);
            // Only replace content if compression actually reduced size
            // and the result is lossless (code mode).
            if !output.lossy && output.content.len() < msg.content.len() {
                msg.content = output.content;
            }
        }
    }

    /// Returns `true` when the message content is code-heavy enough to
    /// benefit from AAAK compression rather than the default micro stage.
    #[must_use]
    pub fn should_use_aaak(messages: &[Message]) -> bool {
        code_ratio(messages) > 0.5
    }

    // -----------------------------------------------------------------------
    // Stage 2 – Session compact
    // -----------------------------------------------------------------------

    /// Return `true` when Stage-2 (session) compression should be triggered.
    #[must_use]
    pub fn should_session_compact(&self, messages: &[Message]) -> bool {
        // Anti-thrashing: skip if too many recent compressions were ineffective
        if *self
            .ineffective_compression_count
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            >= 2
        {
            return false;
        }
        // Cooldown: skip if LLM summarizer recently failed
        if let Some(cooldown) = *self
            .summary_cooldown_until
            .lock()
            .unwrap_or_else(|e| e.into_inner())
        {
            if std::time::Instant::now() < cooldown {
                return false;
            }
        }
        !self.guard.is_open() && self.session.should_compact(messages)
    }

    /// Execute Stage-2 (session) compression.
    ///
    /// Splits the message list, generates a structured summary, writes key
    /// decisions to L2 and the summary to L3, then replaces the list with
    /// `[summary_message, …recent…]`.
    ///
    /// When a previous summary exists in `self.previous_summary`, it is passed
    /// to the compactor so the new summary is built iteratively on top of it.
    /// After successful compaction, the field is updated with the new summary.
    pub async fn session_compact(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
    ) -> Result<CompactionResult> {
        let _scope = self.guard.enter()?;
        let prev = self
            .previous_summary
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let result = self
            .session
            .compact(messages, orchestrator, prev.as_deref())
            .await?;

        // Store the new summary for future iterative updates.
        // The first message after compaction is the pinned summary message.
        if let Some(summary_msg) = messages.first() {
            if summary_msg.pinned {
                *self
                    .previous_summary
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(summary_msg.content.clone());
            }
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Stage 3 – Deep compact
    // -----------------------------------------------------------------------

    /// Execute Stage-3 (deep) compression.
    ///
    /// Should be called when Stage-2 compression was insufficient.
    /// If `previous_summary` is supplied the new summary is built by
    /// iteratively merging incremental content on top of it.
    pub async fn deep_compact(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
    ) -> Result<CompactionResult> {
        if !self.enable_deep {
            return Ok(CompactionResult::default());
        }
        let _scope = self.guard.enter()?;
        self.deep.compact(messages, orchestrator, None).await
    }

    /// Execute Stage-3 compression with an existing summary to update
    /// iteratively.
    pub async fn deep_compact_iterative(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
        previous_summary: &str,
    ) -> Result<CompactionResult> {
        if !self.enable_deep {
            return Ok(CompactionResult::default());
        }
        let _scope = self.guard.enter()?;
        self.deep
            .compact(messages, orchestrator, Some(previous_summary))
            .await
    }

    // -----------------------------------------------------------------------
    // Full pipeline run
    // -----------------------------------------------------------------------

    /// Run all compression stages in order, returning an updated context.
    ///
    /// 1. Micro-compact the messages.
    /// 2. If session threshold is exceeded, run session compact.
    /// 3. If still above budget and deep compression is enabled, run deep.
    ///
    /// Returns a [`PreparedContext`] suitable for injection into the model
    /// prompt.
    pub async fn run(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
    ) -> Result<PreparedContext> {
        // Stage 1 – choose AAAK for code-heavy content, micro otherwise.
        if Self::should_use_aaak(messages) {
            self.aaak_compact(messages);
        } else {
            self.micro_compact(messages);
        }

        // Stage 2 – only if needed.
        if self.should_session_compact(messages) {
            self.session_compact(messages, orchestrator).await?;
        }

        // Stage 3 – only if still needed and enabled.
        if self.enable_deep && self.session.should_compact(messages) {
            self.deep_compact(messages, orchestrator).await?;
        }

        // Build a PreparedContext from the remaining messages.
        let total_tokens: u64 = messages
            .iter()
            .map(|m| token_estimation::estimate_tokens_text(&m.content) as u64)
            .sum();

        let budget = crate::types::TokenBudget {
            total: 200_000,
            reserved_system: 10_000,
            reserved_response: 8_000,
            allocated_memory: total_tokens,
            allocated_conversation: total_tokens,
            available: 200_000u64
                .saturating_sub(10_000)
                .saturating_sub(8_000)
                .saturating_sub(total_tokens),
        };

        Ok(PreparedContext {
            entries: vec![], // Message-based context; memory entries fetched separately.
            total_tokens,
            budget,
            depth_scale: 1.0,
            prepared_at: chrono::Utc::now(),
            code_context: None,
        })
    }
}

impl Default for CompressionPipeline {
    fn default() -> Self {
        Self::new(true)
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
            content: content.to_string(),
            tool_use_id: None,
            tool_name: None,
            pinned: false,
        }
    }

    fn tool_msg(tool_name: &str, content: &str) -> Message {
        Message {
            turn_index: 0,
            role: MessageRole::Tool,
            content: content.to_string(),
            tool_use_id: Some("call_1".to_string()),
            tool_name: Some(tool_name.to_string()),
            pinned: false,
        }
    }

    // ===================================================================
    // Task 9: Iterative summary update tests
    // These verify that micro_compact preserves content across calls.
    // Currently no iterative logic exists, so the second compaction
    // loses info from the first. The test asserts that it SHOULD NOT.
    // ===================================================================

    #[test]
    fn test_iterative_summary_preserves_previous_info() {
        let pipeline = CompressionPipeline::new(true);
        let mut msgs = vec![
            msg(MessageRole::User, "Initial: set up the project"),
            msg(MessageRole::Assistant, "Created project with Cargo.toml"),
        ];

        let _len_before = msgs.iter().map(|m| m.content.len()).sum::<usize>();
        pipeline.micro_compact(&mut msgs);

        // Add more conversation
        msgs.push(msg(MessageRole::User, "Add logging support"));
        msgs.push(msg(
            MessageRole::Assistant,
            "Added log4rs with rolling file appender",
        ));

        let _len_after = msgs.iter().map(|m| m.content.len()).sum::<usize>();
        pipeline.micro_compact(&mut msgs);

        // RED: the first compaction's content should influence the second.
        // Currently each micro_compact is independent, so the content from
        // the first compaction is lost. After iterative update, the second
        // compaction should preserve "Cargo.toml" from the first exchange.
        // This test FAILS because no previous_summary mechanism exists.
        let all_content: String = msgs.iter().map(|m| m.content.as_str()).collect();
        assert!(
            all_content.contains("Cargo.toml"),
            "RED: Iterative compaction should preserve 'Cargo.toml' from first turn. \
             Without previous_summary, the first compaction loses this info."
        );
    }

    #[test]
    fn test_second_compaction_content_not_lost() {
        let pipeline = CompressionPipeline::new(true);
        let mut msgs = vec![
            msg(MessageRole::User, "Implement auth module"),
            msg(
                MessageRole::Assistant,
                "Added JWT-based authentication with refresh tokens",
            ),
        ];

        pipeline.micro_compact(&mut msgs);

        // Second exchange
        msgs.push(msg(MessageRole::User, "Add rate limiting"));
        msgs.push(msg(
            MessageRole::Assistant,
            "Added token bucket rate limiter",
        ));

        pipeline.micro_compact(&mut msgs);

        // RED: "JWT" from the first exchange should survive to the second compaction
        let all_content: String = msgs.iter().map(|m| m.content.as_str()).collect();
        assert!(
            all_content.contains("JWT"),
            "RED: 'JWT' from the first turn should survive two compactions. \
             Without iterative updates, content from earlier turns is lost."
        );
    }

    // ===================================================================
    // V2: tool output remains lossless until canonical raw durability exists.
    // ===================================================================

    #[test]
    fn test_terminal_output_without_durable_ref_is_preserved() {
        use crate::compression::micro::MicroCompactor;
        let compactor = MicroCompactor::new();

        let terminal_output = concat!(
            r#"{"exit_code": 1, "output": "test failed at line 42"}"#,
            "\n",
            "error: assertion failed\n",
            "     at src/main.rs:42\n",
        );
        let mut msgs = vec![
            msg(MessageRole::Assistant, "Running tests..."),
            tool_msg("terminal", terminal_output),
            msg(MessageRole::Assistant, "Tests failed, fixing"),
        ];

        compactor.compact(&mut msgs);

        let tool_content = &msgs[1].content;
        assert_eq!(tool_content, terminal_output);
    }

    #[test]
    fn test_read_file_output_summarized_with_path_context() {
        use crate::compression::micro::MicroCompactor;
        let compactor = MicroCompactor::new();

        let read_content = "fn main() {\n    let x = 42;\n    println!(\"{}\", x);\n}\n";
        let mut msgs = vec![
            msg(MessageRole::Assistant, "Reading src/main.rs..."),
            tool_msg("read_file", read_content),
            msg(MessageRole::Assistant, "Found the main function"),
        ];

        compactor.compact(&mut msgs);

        let tool_content = &msgs[1].content;
        assert!(
            tool_content.len() <= read_content.len(),
            "Read file output should at minimum not grow"
        );
    }

    #[test]
    fn test_unknown_tool_without_durable_ref_is_preserved() {
        use crate::compression::micro::MicroCompactor;
        let compactor = MicroCompactor::new();

        let long_content = "result data: ".to_string() + &"x".repeat(3000);
        let mut msgs = vec![
            msg(MessageRole::Assistant, "Running custom transform..."),
            tool_msg("custom_tool", &long_content),
            msg(MessageRole::Assistant, "Done"),
        ];

        compactor.compact(&mut msgs);

        let tool_content = &msgs[1].content;
        assert_eq!(tool_content, &long_content);
    }
}
