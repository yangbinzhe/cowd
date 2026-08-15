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

use std::future::Future;
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
#[derive(Clone)]
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
#[derive(Clone)]
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
        self.config.enabled && !self.config.model.is_empty() && !self.config.api_url.is_empty()
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

        for chunk in texts.chunks(batch_size) {
            results.extend(self.embed_chunk_with_adaptive_batch(chunk).await?);
        }

        // Sort by original index to restore input order.
        results.sort_by_key(|(idx, _)| *idx);

        // Update detected dimension from first result.
        if let Some((_, vec)) = results.first() {
            let dim = vec.len();
            let mut guard = self.detected_dimension.write().await;
            match *guard {
                Some(expected) if expected != dim => {
                    return Err(MemoryError::InvalidArgument(format!(
                        "embedding dimension mismatch: configured {expected}, provider returned {dim}"
                    )));
                }
                Some(_) => {}
                None => {
                    *guard = Some(dim);
                    debug!(dimension = dim, "auto-detected embedding dimension");
                }
            }
        }

        Ok(results.into_iter().map(|(_, v)| v).collect())
    }

    /// Embed one chunk with provider-driven adaptive batch halving.
    ///
    /// Some providers reject batches above their own undocumented limits with
    /// HTTP 400 and a message such as "batch size is invalid". Instead of
    /// failing the whole vector reconciliation, halve the batch recursively
    /// until the provider accepts it (or the batch is a single item).
    async fn embed_chunk_with_adaptive_batch(
        &self,
        chunk: &[&str],
    ) -> Result<Vec<(usize, Vec<f32>)>, MemoryError> {
        // Explicit work stack avoids recursive async (boxing) while keeping
        // input order deterministic.
        let mut pending = vec![(0usize, chunk.to_vec())];
        let mut results: Vec<(usize, Vec<f32>)> = Vec::new();
        while let Some((base, batch)) = pending.pop() {
            match self.embed_batch(&batch).await {
                Ok(raw) => {
                    for (index, (_, vector)) in raw.into_iter().enumerate() {
                        results.push((base + index, vector));
                    }
                }
                Err(error) if is_batch_size_rejection(&error) && batch.len() > 1 => {
                    let half = batch.len() / 2;
                    warn!(
                        len = batch.len(),
                        "embedding provider rejected batch size; halving to {half}"
                    );
                    pending.push((base + half, batch[half..].to_vec()));
                    pending.push((base, batch[..half].to_vec()));
                }
                Err(error) => return Err(error),
            }
        }
        results.sort_by_key(|(index, _)| *index);
        Ok(results)
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
    ///
    /// Retries up to [`EMBED_MAX_RETRIES`] times with exponential backoff on
    /// transient HTTP failures.
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<(usize, Vec<f32>)>, MemoryError> {
        let model = self.config.model.clone();
        let api_url = self.config.api_url.clone();
        let api_key = self.config.api_key.clone();
        let http = self.http.clone();

        retry_with_backoff(EMBED_MAX_RETRIES, EMBED_RETRY_BASE_DELAY_MS, move || {
            let model = model.clone();
            let api_url = api_url.clone();
            let api_key = api_key.clone();
            let http = http.clone();
            let texts = texts.to_vec();
            async move {
                let body = EmbedRequest {
                    model: &model,
                    input: &texts,
                };

                let mut req = http
                    .post(&api_url)
                    .header("Content-Type", "application/json");

                if !api_key.is_empty() {
                    req = req.header("Authorization", format!("Bearer {api_key}"));
                }

                let response = req.json(&body).send().await.map_err(|e| {
                    warn!(error = %e, url = %api_url, "embedding API request failed (will retry)");
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

                Ok(embed_resp
                    .data
                    .into_iter()
                    .map(|item| (item.index, item.embedding))
                    .collect())
            }
        })
        .await
    }
}

// ─── Retry constants & utility ─────────────────────────────────────────────

/// Number of retry attempts for transient embedding API failures.
const EMBED_MAX_RETRIES: u32 = 3;
/// Base delay in milliseconds for exponential backoff.
const EMBED_RETRY_BASE_DELAY_MS: u64 = 500;

/// Detect provider batch-limit rejections so the caller can halve the batch
/// instead of failing the whole vector reconciliation.
fn is_batch_size_rejection(error: &MemoryError) -> bool {
    let MemoryError::Store(message) = error else {
        return false;
    };
    let lowered = message.to_ascii_lowercase();
    lowered.contains("batch size")
        && (lowered.contains("invalid")
            || lowered.contains("not larger than")
            || lowered.contains("too large")
            || lowered.contains("maximum"))
}

/// Retry a fallible async operation with exponential backoff.
///
/// Calls `operation` up to `max_retries` times. Delays between retries follow
/// `base_delay_ms * 2^attempt`.
async fn retry_with_backoff<F, Fut, T>(
    max_retries: u32,
    base_delay_ms: u64,
    operation: F,
) -> Result<T, MemoryError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, MemoryError>>,
{
    let mut last_error = None;
    for attempt in 0..max_retries {
        match operation().await {
            Ok(val) => return Ok(val),
            // A provider-declared batch limit is deterministic. Retrying the
            // same oversized payload only adds backoff and duplicate 400s;
            // return immediately so the adaptive caller can split it.
            Err(e) if is_batch_size_rejection(&e) => return Err(e),
            Err(e) => {
                last_error = Some(e);
                if attempt + 1 < max_retries {
                    let delay = base_delay_ms * (1 << attempt);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }
    match last_error {
        Some(error) => Err(error),
        None => Err(MemoryError::InvalidArgument(
            "embedding retry count must be greater than zero".to_string(),
        )),
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

    #[test]
    fn batch_rejection_detection_matches_provider_400_shape() {
        assert!(is_batch_size_rejection(&MemoryError::Store(
            "embedding API error 400 Bad Request: batch size is invalid, it should not be larger than 20"
                .into()
        )));
        assert!(is_batch_size_rejection(&MemoryError::Store(
            "embedding API error 400: batch size too large, maximum 20".into()
        )));
        assert!(!is_batch_size_rejection(&MemoryError::Store(
            "embedding API error 429 rate limited".into()
        )));
        assert!(!is_batch_size_rejection(&MemoryError::InvalidArgument(
            "dimension mismatch".into()
        )));
    }

    #[tokio::test]
    async fn deterministic_batch_rejection_skips_redundant_backoff_retries() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let error = retry_with_backoff(3, 0, move || {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {
                Err::<(), _>(MemoryError::Store(
                    "embedding API error 400: batch size is invalid, it should not be larger than 10"
                        .into(),
                ))
            }
        })
        .await
        .expect_err("oversized batch is returned to the adaptive splitter");

        assert!(is_batch_size_rejection(&error));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
