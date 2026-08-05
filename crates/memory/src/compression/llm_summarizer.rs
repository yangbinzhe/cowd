//! LLM-backed summarisation for the compression pipeline.
//!
//! Defines the [`LlmSummarizer`] port used by Memory. Provider protocol,
//! transport, retries, and credentials are deliberately supplied by the host
//! Runtime; Memory never opens a model connection by itself.

use async_trait::async_trait;

// ---------------------------------------------------------------------------
// LlmSummarizer trait
// ---------------------------------------------------------------------------

/// Interface for LLM-powered summarisation.
///
/// Implementors provide a single `summarize` method that takes a system prompt
/// and the content to summarise, returning the generated text.  This keeps the
/// trait lean while allowing callers to craft domain-specific prompts.
#[async_trait]
pub trait LlmSummarizer: Send + Sync {
    /// Generate a summary of `content` guided by `prompt`.
    async fn summarize(&self, prompt: &str, content: &str) -> Result<String, LlmSummarizerError>;

    /// Extract structured decisions from content.
    async fn extract_decisions(&self, content: &str) -> Result<Vec<String>, LlmSummarizerError> {
        // Default: use summarize with a decisions-specific prompt.
        let prompt = "Extract all key decisions from the following conversation. \
                       Return each decision as a bullet point, one per line. \
                       If no decisions are found, return an empty response.";
        let result = self.summarize(prompt, content).await?;
        Ok(result
            .lines()
            .map(|l| l.trim_start_matches("- ").trim().to_string())
            .filter(|l| !l.is_empty())
            .collect())
    }

    /// Extract behavioural patterns from content.
    async fn extract_patterns(&self, content: &str) -> Result<Vec<String>, LlmSummarizerError> {
        let prompt = "Identify recurring patterns, preferences, or habits in the \
                       following conversation. Return each as a bullet point, one per line. \
                       If none found, return an empty response.";
        let result = self.summarize(prompt, content).await?;
        Ok(result
            .lines()
            .map(|l| l.trim_start_matches("- ").trim().to_string())
            .filter(|l| !l.is_empty())
            .collect())
    }
}

/// Error type for LLM summariser operations.
#[derive(Debug, thiserror::Error)]
pub enum LlmSummarizerError {
    #[error("provider execution failed: {0}")]
    Provider(String),
    #[error("no content in response")]
    EmptyResponse,
    #[error("configuration error: {0}")]
    Config(String),
}

// ---------------------------------------------------------------------------
// NoOpSummarizer (always fails, forcing fallback)
// ---------------------------------------------------------------------------

/// A summariser that always returns an error, useful as a placeholder
/// when no LLM is configured.
pub struct NoOpSummarizer;

#[async_trait]
impl LlmSummarizer for NoOpSummarizer {
    async fn summarize(&self, _prompt: &str, _content: &str) -> Result<String, LlmSummarizerError> {
        Err(LlmSummarizerError::Config(
            "no LLM summariser configured".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_summarizer_always_fails() {
        let summarizer = NoOpSummarizer;
        let result = summarizer.summarize("test", "content").await;
        assert!(result.is_err());
    }
}
