//! LLM-backed summarisation for the compression pipeline.
//!
//! Defines the [`LlmSummarizer`] trait and an OpenAI-compatible implementation
//! ([`OpenAiSummarizer`]).  When an LLM summariser is available the
//! session/deep compactors use it to generate semantic summaries; when it is
//! absent they fall back to the template-based heuristic.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
    #[error("no content in response")]
    EmptyResponse,
    #[error("configuration error: {0}")]
    Config(String),
}

// ---------------------------------------------------------------------------
// OpenAiSummarizer
// ---------------------------------------------------------------------------

/// OpenAI-compatible summariser that calls a chat-completions endpoint.
pub struct OpenAiSummarizer {
    client: reqwest::Client,
    api_url: String,
    api_key: String,
    model: String,
}

impl OpenAiSummarizer {
    /// Create a new summariser.
    ///
    /// `api_url` should be the base URL (e.g. `https://api.openai.com/v1`).
    pub fn new(api_url: String, api_key: String, model: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_url,
            api_key,
            model,
        }
    }
}

/// A single chat message in the OpenAI format.
#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

/// The request body for a chat completions call.
#[derive(Debug, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    temperature: f32,
}

/// The response body from a chat completions call.
#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Debug, Deserialize)]
struct ChatChoiceMessage {
    content: Option<String>,
}

#[async_trait]
impl LlmSummarizer for OpenAiSummarizer {
    async fn summarize(&self, prompt: &str, content: &str) -> Result<String, LlmSummarizerError> {
        let url = format!("{}/chat/completions", self.api_url.trim_end_matches('/'));

        let request = ChatRequest {
            model: self.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: prompt.to_string(),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: content.to_string(),
                },
            ],
            max_tokens: 2048,
            temperature: 0.3,
        };

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = response.status().as_u16();

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(LlmSummarizerError::Api {
                status,
                message: body,
            });
        }

        let chat_response: ChatResponse = response.json().await?;

        chat_response
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .filter(|s| !s.trim().is_empty())
            .ok_or(LlmSummarizerError::EmptyResponse)
    }
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

    #[test]
    fn openai_summarizer_construction() {
        let summarizer = OpenAiSummarizer::new(
            "https://api.openai.com/v1".to_string(),
            "sk-test".to_string(),
            "gpt-4o-mini".to_string(),
        );
        assert_eq!(summarizer.model, "gpt-4o-mini");
    }
}
