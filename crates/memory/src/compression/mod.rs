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
        llm_summarizer::{OpenAiSummarizer, LlmSummarizer},
        micro::MicroCompactor,
        session::SessionCompactor,
    },
    config::CompressionConfig,
    error::MemoryError,
    orchestrator::MemoryOrchestrator,
    types::{CompactionResult, Message, PreparedContext},
};
use std::sync::Arc;

/// Result alias for compression operations.
pub type Result<T> = std::result::Result<T, MemoryError>;

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
            tracing::info!(
                "LLM summarization enabled: {}",
                config.llm.model
            );
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
    // Stage 2 – Session compact
    // -----------------------------------------------------------------------

    /// Return `true` when Stage-2 (session) compression should be triggered.
    #[must_use]
    pub fn should_session_compact(&self, messages: &[Message]) -> bool {
        !self.guard.is_open() && self.session.should_compact(messages)
    }

    /// Execute Stage-2 (session) compression.
    ///
    /// Splits the message list, generates a structured summary, writes key
    /// decisions to L2 and the summary to L3, then replaces the list with
    /// `[summary_message, …recent…]`.
    pub async fn session_compact(
        &self,
        messages: &mut Vec<Message>,
        orchestrator: &MemoryOrchestrator,
    ) -> Result<CompactionResult> {
        let _scope = self.guard.enter()?;
        self.session.compact(messages, orchestrator).await
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
        // Stage 1 – always run.
        self.micro_compact(messages);

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
        })
    }
}

impl Default for CompressionPipeline {
    fn default() -> Self {
        Self::new(true)
    }
}
