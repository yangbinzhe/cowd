//! Remote embedding client for vector index support.
//!
//! Implements an OpenAI-compatible embedding API client that supports
//! auto-detecting the embedding dimension on the first call.  When the
//! remote API is unavailable or not configured the client degrades
//! gracefully and returns an error so callers can skip embedding.
//!
//! # Supported backends
//! Any API that speaks the OpenAI embeddings wire format works out of the box:
//! - OpenAI (`https://api.openai.com/v1/embeddings`)
//! - Azure OpenAI
//! - Ollama (`http://localhost:11434/api/embeddings` or compatible)
//! - vLLM, LocalAI, LM Studio, etc.
//!
//! # Environment variables
//! - `CC_VECTOR_API_KEY` – overrides [`VectorConfig::api_key`] at runtime.
//! - `CC_MEMORY_VECTOR_MODEL` – overrides [`VectorConfig::model`].
//! - `CC_MEMORY_VECTOR_API_URL` – overrides [`VectorConfig::api_url`].

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::{config::VectorConfig, error::MemoryError};

// ─── EmbeddingCapability ─────────────────────────────────────────────────────

/// Capability level for the embedding/search subsystem.
///
/// Determines which search strategies are available:
/// - `Remote` – full semantic search via a remote embedding API
/// - `Local` – local model-based embedding (reserved for future use)
/// - `Fts5Only` – keyword-only search via SQLite FTS5, no vector support
pub enum EmbeddingCapability {
    /// Remote embedding API (e.g. OpenAI, Ollama, vLLM).
    Remote { client: EmbeddingClient },
    /// Local embedding model (reserved for future use).
    Local { model_path: String },
    /// FTS5 keyword search only — no vector embeddings.
    Fts5Only,
}

impl std::fmt::Debug for EmbeddingCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Remote { .. } => write!(f, "Remote {{ client: EmbeddingClient }}"),
            Self::Local { model_path } => write!(f, "Local {{ model_path: {model_path} }}"),
            Self::Fts5Only => write!(f, "Fts5Only"),
        }
    }
}

impl EmbeddingCapability {
    /// Return the effective search mode for this capability.
    pub fn search_mode_label(&self) -> &'static str {
        match self {
            Self::Remote { .. } => "semantic",
            Self::Local { .. } => "local",
            Self::Fts5Only => "keyword",
        }
    }

    /// Return true if this capability supports vector/semantic search.
    pub fn supports_semantic(&self) -> bool {
        matches!(self, Self::Remote { .. } | Self::Local { .. })
    }

    /// Construct from a VectorConfig: Remote if configured, Fts5Only otherwise.
    pub fn from_config(config: &VectorConfig) -> Self {
        if config.enabled && !config.model.is_empty() && !config.api_url.is_empty() {
            Self::Remote {
                client: EmbeddingClient::new(config.clone()),
            }
        } else {
            Self::Fts5Only
        }
    }
}

// ─── Wire types ──────────────────────────────────────────────────────────────

/// Request body sent to an OpenAI-compatible embeddings endpoint.
#[derive(Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
}

/// Single embedding item in the response.
#[derive(Deserialize)]
struct EmbedItem {
    embedding: Vec<f32>,
    // `index` is part of the wire format but we sort by arrival order.
    index: usize,
}

/// Top-level response body from an OpenAI-compatible embeddings endpoint.
#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedItem>,
}

// ─── EmbeddingClient ─────────────────────────────────────────────────────────

/// Client for a remote OpenAI-compatible embeddings API.
///
/// Wraps [`reqwest::Client`] and handles batching, dimension auto-detection
/// and graceful degradation when the remote service is unavailable.
pub struct EmbeddingClient {
    http: reqwest::Client,
    config: VectorConfig,
    /// Detected (or configured) embedding dimension, populated lazily.
    detected_dimension: Arc<RwLock<Option<usize>>>,
}

impl EmbeddingClient {
    /// Build a client from `config`.
    ///
    /// Environment variables are applied here so they take precedence over
    /// the static configuration file values:
    /// - `CC_VECTOR_API_KEY` → `api_key`
    /// - `CC_MEMORY_VECTOR_MODEL` → `model`
    /// - `CC_MEMORY_VECTOR_API_URL` → `api_url`
    pub fn new(mut config: VectorConfig) -> Self {
        // Apply environment variable overrides.
        if let Ok(key) = std::env::var("COWD_VECTOR_API_KEY") {
            if !key.is_empty() {
                config.api_key = key;
            }
        }
        if let Ok(model) = std::env::var("COWD_MEMORY_VECTOR_MODEL") {
            if !model.is_empty() {
                config.model = model;
            }
        }
        if let Ok(url) = std::env::var("COWD_MEMORY_VECTOR_API_URL") {
            if !url.is_empty() {
                config.api_url = url;
            }
        }

        // If dimension was configured statically, pre-populate the detected value.
        let detected_dimension = if config.dimension > 0 {
            Arc::new(RwLock::new(Some(config.dimension)))
        } else {
            Arc::new(RwLock::new(None))
        };

        let timeout = Duration::from_secs(config.timeout_secs);
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_default();

        Self {
            http,
            config,
            detected_dimension,
        }
    }

    /// Returns `true` when a remote endpoint has been configured (non-empty
    /// `model` **and** `api_url`).
    ///
    /// Note: this does **not** perform a connectivity check.
    #[must_use]
    pub fn is_remote_available(&self) -> bool {
        self.config.enabled
            && !self.config.model.is_empty()
            && !self.config.api_url.is_empty()
    }

    /// Embed a batch of texts, returning one vector per input string.
    ///
    /// The vectors in the returned `Vec` correspond to the input texts in the
    /// same order.  The function automatically chunks the input into batches
    /// no larger than [`VectorConfig::batch_size`].
    ///
    /// # Errors
    /// Returns [`MemoryError::Store`] when the remote API returns an error or
    /// is not reachable, or when the remote service is not configured.
    pub async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, MemoryError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        if !self.is_remote_available() {
            return Err(MemoryError::Store(
                "remote embedding not configured (model or api_url is empty)".into(),
            ));
        }

        let batch_size = self.config.batch_size.max(1);
        let mut results: Vec<(usize, Vec<f32>)> = Vec::with_capacity(texts.len());

        for (batch_idx, chunk) in texts.chunks(batch_size).enumerate() {
            let offset = batch_idx * batch_size;
            let mut batch_results = self.embed_batch(chunk).await?;
            for item in &mut batch_results {
                item.0 += offset;
            }
            results.extend(batch_results);
        }

        // Sort by original index to restore input order.
        results.sort_by_key(|(idx, _)| *idx);

        // Update detected dimension from first result.
        if let Some((_, vec)) = results.first() {
            let dim = vec.len();
            let mut guard = self.detected_dimension.write().await;
            if guard.is_none() {
                *guard = Some(dim);
                debug!(dimension = dim, "auto-detected embedding dimension");
            }
        }

        Ok(results.into_iter().map(|(_, v)| v).collect())
    }

    /// Embed a single text string.
    ///
    /// Convenience wrapper around [`embed`].
    pub async fn embed_one(&self, text: &str) -> Result<Vec<f32>, MemoryError> {
        let mut results = self.embed(&[text]).await?;
        results
            .pop()
            .ok_or_else(|| MemoryError::Store("embedding API returned empty result".into()))
    }

    /// Auto-detect the embedding dimension by sending a minimal probe request.
    ///
    /// Stores the result internally and returns it.  Subsequent calls to
    /// [`embed`] will use the detected dimension for validation.
    ///
    /// # Errors
    /// Propagates any error from the underlying API call.
    pub async fn detect_dimension(&self) -> Result<usize, MemoryError> {
        // Return cached value if already known.
        {
            let guard = self.detected_dimension.read().await;
            if let Some(dim) = *guard {
                return Ok(dim);
            }
        }

        // Send a minimal probe.
        let probe = self.embed_one("dimension probe").await?;
        let dim = probe.len();

        {
            let mut guard = self.detected_dimension.write().await;
            *guard = Some(dim);
        }

        debug!(dimension = dim, "embedding dimension detected via probe");
        Ok(dim)
    }

    /// Return the currently known embedding dimension, if any.
    ///
    /// Returns `None` if the dimension has not yet been detected.
    pub async fn known_dimension(&self) -> Option<usize> {
        *self.detected_dimension.read().await
    }

    // ─── Internal helpers ─────────────────────────────────────────────────

    /// Send one batch (≤ `batch_size` items) to the API and return
    /// `(original_index, embedding)` pairs in API response order.
    async fn embed_batch(
        &self,
        texts: &[&str],
    ) -> Result<Vec<(usize, Vec<f32>)>, MemoryError> {
        let body = EmbedRequest {
            model: &self.config.model,
            input: texts,
        };

        let mut req = self
            .http
            .post(&self.config.api_url)
            .header("Content-Type", "application/json");

        // Attach the Bearer token when an API key is configured.
        if !self.config.api_key.is_empty() {
            req = req.header(
                "Authorization",
                format!("Bearer {}", self.config.api_key),
            );
        }

        let response = req.json(&body).send().await.map_err(|e| {
            warn!(error = %e, url = %self.config.api_url, "embedding API request failed");
            MemoryError::Store(format!("embedding API request failed: {e}"))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body_text = response.text().await.unwrap_or_default();
            warn!(
                status = %status,
                body = %body_text,
                "embedding API returned non-2xx status"
            );
            return Err(MemoryError::Store(format!(
                "embedding API error {status}: {body_text}"
            )));
        }

        let embed_resp: EmbedResponse = response.json().await.map_err(|e| {
            MemoryError::Store(format!("embedding API response parse error: {e}"))
        })?;

        if embed_resp.data.is_empty() {
            return Err(MemoryError::Store(
                "embedding API returned empty data array".into(),
            ));
        }

        let pairs = embed_resp
            .data
            .into_iter()
            .map(|item| (item.index, item.embedding))
            .collect();

        Ok(pairs)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::VectorConfig;

    fn unconfigured_client() -> EmbeddingClient {
        EmbeddingClient::new(VectorConfig::default())
    }

    fn configured_client() -> EmbeddingClient {
        EmbeddingClient::new(VectorConfig {
            enabled: true,
            model: "test-model".into(),
            api_url: "http://localhost:9999/v1/embeddings".into(),
            api_key: String::new(),
            dimension: 4,
            timeout_secs: 5,
            batch_size: 2,
        })
    }

    #[test]
    fn is_remote_available_false_when_unconfigured() {
        let client = unconfigured_client();
        assert!(!client.is_remote_available());
    }

    #[test]
    fn is_remote_available_true_when_configured() {
        let client = configured_client();
        assert!(client.is_remote_available());
    }

    #[tokio::test]
    async fn embed_empty_returns_empty() {
        let client = unconfigured_client();
        // embed on unconfigured should error, but empty input is returned early.
        let result = client.embed(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn embed_returns_error_when_not_configured() {
        let client = unconfigured_client();
        let err = client.embed(&["hello"]).await.unwrap_err();
        assert!(matches!(err, MemoryError::Store(_)));
    }

    #[tokio::test]
    async fn known_dimension_reflects_static_config() {
        let client = configured_client();
        // dimension = 4 was set in config, should be pre-populated.
        assert_eq!(client.known_dimension().await, Some(4));
    }

    #[tokio::test]
    async fn known_dimension_none_when_zero() {
        let client = EmbeddingClient::new(VectorConfig {
            enabled: true,
            model: "m".into(),
            api_url: "http://x".into(),
            dimension: 0, // auto-detect
            ..VectorConfig::default()
        });
        assert_eq!(client.known_dimension().await, None);
    }
}
