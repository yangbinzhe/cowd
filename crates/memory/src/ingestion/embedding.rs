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

use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
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
    /// Bounded request fingerprints rejected deterministically by the provider.
    /// Clones share this state so a turn cannot repeat the same invalid call.
    deterministic_failures: Arc<RwLock<Vec<(u64, String)>>>,
    /// Per-payload single-flight gates close the race between a first 400 and
    /// concurrent identical callers. Weak entries disappear after the callers.
    request_gates: Arc<AsyncMutex<HashMap<u64, Weak<AsyncMutex<()>>>>>,
    /// Provider-observed upper bound for this configured route/model revision.
    /// Clones share it; a newly configured client starts a fresh capability.
    learned_batch_cap: Arc<AtomicUsize>,
    /// Authentication failure is route-wide, not a malformed text payload.
    /// A replacement configured client is the explicit credential revision.
    credentials_rejected: Arc<AtomicBool>,
    /// Selected durable store owns successful, not-yet-consumed segments.
    progress_store: Option<Arc<dyn crate::store::MemoryStore>>,
}

/// Completed original inputs remain usable even if a later provider batch fails.
pub(crate) struct PartialEmbeddings {
    pub vectors: Vec<Option<Vec<f32>>>,
    pub error: Option<MemoryError>,
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
        if let Ok(limit) = std::env::var("COWD_MEMORY_VECTOR_MAX_INPUT_TOKENS") {
            if let Ok(limit) = limit.parse::<usize>() {
                config.max_input_tokens = limit;
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

        let initial_batch_cap = config.batch_size.max(1);
        Self {
            http,
            config,
            detected_dimension,
            deterministic_failures: Arc::new(RwLock::new(Vec::new())),
            request_gates: Arc::new(AsyncMutex::new(HashMap::new())),
            learned_batch_cap: Arc::new(AtomicUsize::new(initial_batch_cap)),
            credentials_rejected: Arc::new(AtomicBool::new(false)),
            progress_store: None,
        }
    }

    /// Retain successful segments in the selected store when a later batch
    /// fails. Recreating a client with the same configuration resumes them.
    /// Successful ordinary `embed` calls acknowledge their own checkpoints.
    pub fn with_progress_store(mut self, store: Arc<dyn crate::store::MemoryStore>) -> Self {
        self.progress_store = Some(store);
        self
    }

    fn progress_key(&self, text: &str) -> String {
        // Length-delimited canonical encoding prevents field-boundary collisions.
        // Only the digest, never the credential or source text, enters the key.
        let identity = serde_json::to_vec(&(
            "embedding-segment.v1",
            &self.config.api_url,
            &self.config.model,
            &self.config.api_key,
            self.config.dimension,
            self.effective_max_input_tokens(),
            text,
        ))
        .expect("embedding identity is serializable");
        format!("embedding-progress:v1:{:x}", Sha256::digest(identity))
    }

    pub(crate) fn configuration_identity(&self) -> String {
        self.progress_key("")
    }

    async fn validate_vectors(&self, vectors: &[Vec<f32>]) -> Result<(), MemoryError> {
        let mut dimension = self.detected_dimension.write().await;
        for vector in vectors {
            if vector.is_empty() || vector.iter().any(|value| !value.is_finite()) {
                return Err(MemoryError::InvalidArgument(
                    "embedding vector must be finite and nonempty".into(),
                ));
            }
            match *dimension {
                Some(expected) if expected != vector.len() => {
                    return Err(MemoryError::InvalidArgument(format!(
                        "embedding dimension mismatch: configured {expected}, provider returned {}",
                        vector.len()
                    )))
                }
                None => *dimension = Some(vector.len()),
                _ => {}
            }
        }
        Ok(())
    }

    /// Called only after the downstream vector artifact has been persisted.
    pub(crate) async fn acknowledge_embeddings(&self, texts: &[&str]) -> Result<(), MemoryError> {
        if let Some(store) = &self.progress_store {
            for input in prepare_embedding_inputs(texts, self.effective_max_input_tokens())? {
                store.kv_delete(&self.progress_key(&input.text)).await?;
            }
        }
        Ok(())
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
        let partial = self.embed_partial(texts).await?;
        if let Some(error) = partial.error {
            return Err(error);
        }
        let vectors = partial
            .vectors
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| MemoryError::Store("embedding result is incomplete".into()))?;
        // Ordinary query callers consume the result directly. Ingestion uses
        // embed_partial and acknowledges only after its durable index write.
        self.acknowledge_embeddings(texts).await?;
        Ok(vectors)
    }

    pub(crate) async fn embed_partial(
        &self,
        texts: &[&str],
    ) -> Result<PartialEmbeddings, MemoryError> {
        if texts.is_empty() {
            return Ok(PartialEmbeddings {
                vectors: Vec::new(),
                error: None,
            });
        }
        if !self.is_remote_available() {
            return Err(MemoryError::Store(
                "remote embedding not configured (model or api_url is empty)".into(),
            ));
        }

        let prepared = prepare_embedding_inputs(texts, self.effective_max_input_tokens())?;
        let mut segment_vectors: Vec<Option<Vec<f32>>> = vec![None; prepared.len()];
        if let Some(store) = &self.progress_store {
            let keys = prepared
                .iter()
                .map(|input| self.progress_key(&input.text))
                .collect::<Vec<_>>();
            let restored = store
                .kv_get_many(&keys)
                .await?
                .into_iter()
                .map(|record| (record.key, record.value))
                .collect::<HashMap<_, _>>();
            for (index, key) in keys.iter().enumerate() {
                if let Some(raw) = restored.get(key) {
                    let vector: Vec<f32> = serde_json::from_str(raw).map_err(|error| {
                        MemoryError::Store(format!("invalid embedding checkpoint: {error}"))
                    })?;
                    self.validate_vectors(std::slice::from_ref(&vector)).await?;
                    segment_vectors[index] = Some(vector);
                }
            }
        }
        let batch_size = self.learned_batch_cap.load(Ordering::Acquire).max(1);
        let missing = prepared
            .iter()
            .enumerate()
            .filter(|(index, _)| segment_vectors[*index].is_none())
            .map(|(index, input)| (index, input.text.as_str()))
            .collect::<Vec<_>>();
        let mut failure = None;
        for chunk in missing.chunks(batch_size) {
            let refs = chunk.iter().map(|(_, text)| *text).collect::<Vec<_>>();
            let (vectors, error) = self.embed_chunk_with_adaptive_batch(&refs).await;
            for (index, vector) in vectors {
                segment_vectors[chunk[index].0] = Some(vector);
            }
            if error.is_some() {
                failure = error;
                break;
            }
        }
        let mut results = vec![None; texts.len()];
        for (input_index, result) in results.iter_mut().enumerate() {
            let segments = prepared
                .iter()
                .enumerate()
                .filter(|(_, input)| input.original_index == input_index)
                .map(|(index, _)| segment_vectors[index].clone())
                .collect::<Option<Vec<_>>>();
            if let Some(segments) = segments {
                let single = prepare_embedding_inputs(
                    &[texts[input_index]],
                    self.effective_max_input_tokens(),
                )?;
                *result = coalesce_embedding_segments(1, &single, segments)?.pop();
            }
        }
        Ok(PartialEmbeddings {
            vectors: results,
            error: failure,
        })
    }

    fn effective_max_input_tokens(&self) -> usize {
        if self.config.max_input_tokens > 0 {
            return self.config.max_input_tokens;
        }
        inferred_model_input_limit(&self.config.model)
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
    ) -> (Vec<(usize, Vec<f32>)>, Option<MemoryError>) {
        // Explicit work stack avoids recursive async (boxing) while keeping
        // input order deterministic.
        let cap = self.learned_batch_cap.load(Ordering::Acquire).max(1);
        let mut pending = chunk
            .chunks(cap)
            .enumerate()
            .rev()
            .map(|(index, batch)| (index.saturating_mul(cap), batch.to_vec()))
            .collect::<Vec<_>>();
        let mut results: Vec<(usize, Vec<f32>)> = Vec::new();
        while let Some((base, batch)) = pending.pop() {
            match self.embed_batch(&batch).await {
                Ok(raw) => {
                    if let Err(error) = self
                        .validate_vectors(
                            &raw.iter()
                                .map(|(_, vector)| vector.clone())
                                .collect::<Vec<_>>(),
                        )
                        .await
                    {
                        return (results, Some(error));
                    }
                    for (index, (_, vector)) in raw.into_iter().enumerate() {
                        if let Some(store) = &self.progress_store {
                            let value = serde_json::to_string(&vector)
                                .expect("finite embedding is serializable");
                            if let Err(error) =
                                store.kv_put(&self.progress_key(batch[index]), &value).await
                            {
                                return (results, Some(error));
                            }
                        }
                        results.push((base + index, vector));
                    }
                }
                Err(error) if is_batch_size_rejection(&error) && batch.len() > 1 => {
                    let half = batch.len() / 2;
                    self.learned_batch_cap
                        .fetch_min(half.max(1), Ordering::AcqRel);
                    warn!(
                        len = batch.len(),
                        "embedding provider rejected batch size; halving to {half}"
                    );
                    pending.push((base + half, batch[half..].to_vec()));
                    pending.push((base, batch[..half].to_vec()));
                }
                Err(error) => return (results, Some(error)),
            }
        }
        results.sort_by_key(|(index, _)| *index);
        (results, None)
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
        let fingerprint = embedding_request_fingerprint(&self.config.model, texts);
        let request_gate = {
            let mut gates = self.request_gates.lock().await;
            gates.retain(|_, gate| gate.strong_count() > 0);
            if let Some(gate) = gates.get(&fingerprint).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(AsyncMutex::new(()));
                gates.insert(fingerprint, Arc::downgrade(&gate));
                gate
            }
        };
        let _request_guard = request_gate.lock().await;
        if self.credentials_rejected.load(Ordering::Acquire) {
            return Err(MemoryError::EmbeddingCredentialsRequired);
        }
        if let Some((_, message)) = self
            .deterministic_failures
            .read()
            .await
            .iter()
            .find(|(cached, _)| *cached == fingerprint)
        {
            return Err(MemoryError::Store(message.clone()));
        }

        let model = self.config.model.clone();
        let api_url = self.config.api_url.clone();
        let api_key = self.config.api_key.clone();
        let http = self.http.clone();
        let credentials_rejected = self.credentials_rejected.clone();

        let result = retry_with_backoff(EMBED_MAX_RETRIES, EMBED_RETRY_BASE_DELAY_MS, move || {
            let model = model.clone();
            let api_url = api_url.clone();
            let api_key = api_key.clone();
            let http = http.clone();
            let credentials_rejected = credentials_rejected.clone();
            let texts = texts.to_vec();
            async move {
                if credentials_rejected.load(Ordering::Acquire) {
                    return Err(MemoryError::EmbeddingCredentialsRequired);
                }
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
                    if status == reqwest::StatusCode::UNAUTHORIZED {
                        credentials_rejected.store(true, Ordering::Release);
                        // Authentication response bodies may echo credentials;
                        // expose only the typed state, never their raw text.
                        return Err(MemoryError::EmbeddingCredentialsRequired);
                    }
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

                let mut data = embed_resp
                    .data
                    .into_iter()
                    .map(|item| (item.index, item.embedding))
                    .collect::<Vec<_>>();
                data.sort_by_key(|(index, _)| *index);
                if data.len() != texts.len()
                    || data
                        .iter()
                        .enumerate()
                        .any(|(expected, (actual, _))| expected != *actual)
                {
                    return Err(MemoryError::Store(format!(
                        "embedding API returned invalid indexes: expected 0..{}, got {:?}",
                        texts.len(),
                        data.iter().map(|(index, _)| *index).collect::<Vec<_>>()
                    )));
                }
                Ok(data)
            }
        })
        .await;

        if let Err(error) = &result {
            if is_deterministic_request_rejection(error) {
                let mut failures = self.deterministic_failures.write().await;
                if !failures.iter().any(|(cached, _)| *cached == fingerprint) {
                    if failures.len() >= DETERMINISTIC_FAILURE_CACHE_CAPACITY {
                        failures.remove(0);
                    }
                    if let MemoryError::Store(message) = error {
                        failures.push((fingerprint, message.clone()));
                    }
                }
            }
        }
        result
    }
}

#[derive(Debug)]
struct PreparedEmbeddingInput {
    original_index: usize,
    text: String,
    token_weight: usize,
}

/// Providers expose a per-input limit, while many OpenAI-compatible APIs omit
/// that capability from discovery. Known model families use their documented
/// bound; unknown providers get a conservative limit that can be overridden by
/// `max_input_tokens` or `COWD_MEMORY_VECTOR_MAX_INPUT_TOKENS`.
fn inferred_model_input_limit(model: &str) -> usize {
    let model = model.to_ascii_lowercase();
    if model.contains("text-embedding-v4")
        || model.contains("bge-m3")
        || model.contains("multilingual-e5")
    {
        8192
    } else if model.contains("text-embedding-3") || model.contains("text-embedding-ada") {
        8191
    } else {
        2048
    }
}

/// Conservative local estimate used specifically as a provider safety fence.
/// It combines the crate's semantic estimator with UTF-8 density so long
/// unbroken strings and non-ASCII text cannot be severely underestimated.
#[cfg(test)]
fn conservative_embedding_tokens(text: &str) -> usize {
    text.chars().map(conservative_embedding_char_tokens).sum()
}

fn conservative_embedding_char_tokens(ch: char) -> usize {
    if ch.is_ascii() {
        // One unit per ASCII scalar is intentionally conservative for BPE,
        // punctuation and adversarial unbroken text.
        1
    } else {
        // Multi-byte scalars (including emoji) can expand into multiple BPE
        // tokens. Charging half their UTF-8 width plus request headroom is a
        // safe local fence without coupling Memory to one provider tokenizer.
        ch.len_utf8().div_ceil(2)
    }
}

fn prepare_embedding_inputs(
    texts: &[&str],
    max_input_tokens: usize,
) -> Result<Vec<PreparedEmbeddingInput>, MemoryError> {
    let max_input_tokens = max_input_tokens.max(1);
    let reserve = if max_input_tokens > 128 {
        (max_input_tokens / 16).max(32)
    } else {
        1
    };
    let chunk_budget = max_input_tokens.saturating_sub(reserve).max(1);
    let mut prepared = Vec::new();

    for (original_index, text) in texts.iter().enumerate() {
        if text.trim().is_empty() {
            return Err(MemoryError::InvalidArgument(format!(
                "embedding input {original_index} is empty"
            )));
        }

        let mut current = String::new();
        let mut current_tokens = 0usize;
        for ch in text.chars() {
            let char_tokens = conservative_embedding_char_tokens(ch);
            if current_tokens.saturating_add(char_tokens) > chunk_budget {
                if !current.trim().is_empty() {
                    prepared.push(PreparedEmbeddingInput {
                        original_index,
                        text: std::mem::take(&mut current),
                        token_weight: current_tokens,
                    });
                } else {
                    current.clear();
                }
                current.push(ch);
                current_tokens = char_tokens;
                if current_tokens > chunk_budget {
                    return Err(MemoryError::InvalidArgument(format!(
                        "embedding input {original_index} contains a token larger than the configured limit"
                    )));
                }
            } else {
                current.push(ch);
                current_tokens += char_tokens;
            }
        }
        if !current.trim().is_empty() {
            prepared.push(PreparedEmbeddingInput {
                original_index,
                text: current,
                token_weight: current_tokens,
            });
        }
    }

    Ok(prepared)
}

fn coalesce_embedding_segments(
    original_count: usize,
    prepared: &[PreparedEmbeddingInput],
    vectors: Vec<Vec<f32>>,
) -> Result<Vec<Vec<f32>>, MemoryError> {
    let dimension = vectors.first().map(Vec::len).unwrap_or_default();
    if dimension == 0 || vectors.iter().any(|vector| vector.len() != dimension) {
        return Err(MemoryError::InvalidArgument(
            "embedding API returned empty or inconsistent vector dimensions".to_string(),
        ));
    }
    let mut grouped = vec![Vec::<(usize, Vec<f32>)>::new(); original_count];
    for (input, vector) in prepared.iter().zip(vectors) {
        grouped[input.original_index].push((input.token_weight.max(1), vector));
    }

    grouped
        .into_iter()
        .enumerate()
        .map(|(original_index, mut segments)| {
            if segments.is_empty() {
                return Err(MemoryError::Store(format!(
                    "embedding input {original_index} produced no vector"
                )));
            }
            if segments.len() == 1 {
                if let Some((_, vector)) = segments.pop() {
                    return Ok(vector);
                }
                return Err(MemoryError::Store(format!(
                    "embedding input {original_index} lost its only vector"
                )));
            }
            let total_weight = segments.iter().map(|(weight, _)| *weight).sum::<usize>() as f32;
            let mut pooled = vec![0.0f32; dimension];
            for (weight, vector) in segments {
                let weight = weight as f32 / total_weight;
                for (target, value) in pooled.iter_mut().zip(vector) {
                    *target += value * weight;
                }
            }
            let norm = pooled.iter().map(|value| value * value).sum::<f32>().sqrt();
            if norm > f32::EPSILON {
                for value in &mut pooled {
                    *value /= norm;
                }
            }
            Ok(pooled)
        })
        .collect()
}

fn embedding_request_fingerprint(model: &str, texts: &[&str]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    model.hash(&mut hasher);
    texts.len().hash(&mut hasher);
    for text in texts {
        text.len().hash(&mut hasher);
        text.hash(&mut hasher);
    }
    hasher.finish()
}

// ─── Retry constants & utility ─────────────────────────────────────────────

/// Number of retry attempts for transient embedding API failures.
const EMBED_MAX_RETRIES: u32 = 3;
/// Base delay in milliseconds for exponential backoff.
const EMBED_RETRY_BASE_DELAY_MS: u64 = 500;
/// Negative-cache bound keeps invalid payload memory finite per client.
const DETERMINISTIC_FAILURE_CACHE_CAPACITY: usize = 256;

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

/// A malformed embedding payload is deterministic. Retrying the exact same
/// request consumes quota and cannot change the result. Rate limits and server
/// failures remain transient and continue through the bounded retry policy.
fn is_deterministic_request_rejection(error: &MemoryError) -> bool {
    let MemoryError::Store(message) = error else {
        return false;
    };
    message
        .to_ascii_lowercase()
        .contains("embedding api error 400")
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
            Err(e)
                if is_deterministic_request_rejection(&e)
                    || matches!(e, MemoryError::EmbeddingCredentialsRequired) =>
            {
                return Err(e)
            }
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
pub(crate) mod tests {
    use super::*;
    use crate::config::VectorConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub(crate) async fn partial_server() -> (
        String,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/embeddings", listener.local_addr().unwrap());
        let fail = Arc::new(AtomicBool::new(true));
        let inputs = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = fail.clone();
        let observed_inputs = inputs.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request: serde_json::Value =
                    serde_json::from_slice(&read_request_body(&mut socket).await).unwrap();
                let texts = request["input"].as_array().unwrap();
                let (status, body) = if texts.len() > 1 {
                    (
                        "400 Bad Request",
                        serde_json::json!({"error": "batch size is invalid"}),
                    )
                } else if observed_fail.load(Ordering::Acquire)
                    && texts[0].as_str().unwrap().contains("fail")
                {
                    (
                        "400 Bad Request",
                        serde_json::json!({"error": "invalid input"}),
                    )
                } else {
                    observed_inputs
                        .lock()
                        .unwrap()
                        .push(texts[0].as_str().unwrap().to_owned());
                    (
                        "200 OK",
                        serde_json::json!({"data": [{"index": 0, "embedding": [1.0, 2.0]}]}),
                    )
                };
                let body = body.to_string();
                socket.write_all(format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        (url, fail, inputs, server)
    }

    #[tokio::test]
    async fn adaptive_partial_success_survives_client_restart_until_consumer_acknowledges() {
        let (url, fail, inputs, server) = partial_server().await;
        let store: Arc<dyn crate::store::MemoryStore> =
            Arc::new(crate::store::EphemeralMemoryStore::default());
        let config = VectorConfig {
            enabled: true,
            api_url: url,
            model: "partial".into(),
            dimension: 2,
            batch_size: 2,
            ..Default::default()
        };
        let client = EmbeddingClient::new(config.clone()).with_progress_store(store.clone());
        let first = client.embed_partial(&["good", "fail"]).await.unwrap();
        assert!(first.error.is_some());
        assert_eq!(first.vectors, vec![Some(vec![1.0, 2.0]), None]);
        assert_eq!(*inputs.lock().unwrap(), vec!["good"]);
        assert_eq!(store.list_key_values().await.unwrap().len(), 1);
        drop(client);
        fail.store(false, Ordering::Release);
        let restored = EmbeddingClient::new(config.clone()).with_progress_store(store.clone());
        let second = restored.embed_partial(&["good", "fail"]).await.unwrap();
        assert!(second.error.is_none());
        assert!(second.vectors.iter().all(Option::is_some));
        assert_eq!(*inputs.lock().unwrap(), vec!["good", "fail"]);
        assert_eq!(store.list_key_values().await.unwrap().len(), 2);
        // A different credential/model/content cannot consume an old checkpoint.
        let changed = EmbeddingClient::new(VectorConfig {
            api_key: "fixture-new-credential".into(),
            ..config
        })
        .with_progress_store(store.clone());
        changed.embed_partial(&["good"]).await.unwrap();
        assert_eq!(inputs.lock().unwrap().len(), 3);
        assert!(!store
            .list_key_values()
            .await
            .unwrap()
            .iter()
            .any(|record| record.key.contains("fixture-new-credential")
                || record.value.contains("fixture-new-credential")));
        restored
            .acknowledge_embeddings(&["good", "fail"])
            .await
            .unwrap();
        changed.acknowledge_embeddings(&["good"]).await.unwrap();
        assert!(store.list_key_values().await.unwrap().is_empty());
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn partial_segment_is_not_published_as_complete_input_and_corruption_fails_closed() {
        let (url, _, inputs, server) = partial_server().await;
        let store: Arc<dyn crate::store::MemoryStore> =
            Arc::new(crate::store::EphemeralMemoryStore::default());
        let client = EmbeddingClient::new(VectorConfig {
            enabled: true,
            api_url: url,
            model: "segmented".into(),
            dimension: 2,
            batch_size: 2,
            max_input_tokens: 5,
            ..Default::default()
        })
        .with_progress_store(store.clone());
        let partial = client.embed_partial(&["goodfail"]).await.unwrap();
        assert!(partial.error.is_some());
        assert_eq!(partial.vectors, vec![None]);
        assert!(!inputs.lock().unwrap().is_empty());
        store
            .kv_put(&client.progress_key("good"), "[1.0]")
            .await
            .unwrap();
        assert!(client.embed_partial(&["good"]).await.is_err());
        server.abort();
        let _ = server.await;
    }

    async fn read_request_body(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut scratch = [0u8; 4096];
        loop {
            let read = stream.read(&mut scratch).await.expect("read request");
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&scratch[..read]);
            let Some(header_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let body_start = header_end + 4;
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or_default();
            if bytes.len() >= body_start + content_length {
                return bytes[body_start..body_start + content_length].to_vec();
            }
        }
        Vec::new()
    }

    async fn spawn_embedding_server(
        reject: bool,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<serde_json::Value>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock embedding server");
        let address = listener.local_addr().expect("mock server address");
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed_calls = Arc::clone(&calls);
        let observed_requests = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let body = read_request_body(&mut stream).await;
                observed_calls.fetch_add(1, Ordering::SeqCst);
                let request: serde_json::Value =
                    serde_json::from_slice(&body).expect("embedding request JSON");
                observed_requests
                    .lock()
                    .expect("request recorder")
                    .push(request.clone());
                let (status, response) = if reject {
                    (
                        "400 Bad Request",
                        serde_json::json!({"error": {"message": "invalid input payload"}}),
                    )
                } else {
                    let inputs = request["input"].as_array().expect("embedding inputs");
                    let data = inputs
                        .iter()
                        .enumerate()
                        .map(|(index, input)| {
                            let length = input.as_str().expect("string input").len() as f32;
                            serde_json::json!({"index": index, "embedding": [length, 1.0]})
                        })
                        .collect::<Vec<_>>();
                    ("200 OK", serde_json::json!({"data": data}))
                };
                let response = response.to_string();
                let wire = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                    response.len()
                );
                stream
                    .write_all(wire.as_bytes())
                    .await
                    .expect("write mock response");
            }
        });
        (
            format!("http://{address}/v1/embeddings"),
            calls,
            requests,
            task,
        )
    }

    async fn spawn_batch_limited_server(
        max_batch: usize,
    ) -> (String, Arc<Mutex<Vec<usize>>>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind limited embedding server");
        let address = listener.local_addr().expect("server address");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let batches = Arc::clone(&observed);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let body = read_request_body(&mut stream).await;
                let request: serde_json::Value =
                    serde_json::from_slice(&body).expect("embedding request");
                let inputs = request["input"].as_array().expect("input array");
                batches.lock().expect("batch recorder").push(inputs.len());
                let (status, response) = if inputs.len() > max_batch {
                    (
                        "400 Bad Request",
                        serde_json::json!({"error":{"message":format!("batch size too large, maximum {max_batch}")}}),
                    )
                } else {
                    let data = inputs
                        .iter()
                        .enumerate()
                        .map(|(index, _)| serde_json::json!({"index":index,"embedding":[1.0,2.0]}))
                        .collect::<Vec<_>>();
                    ("200 OK", serde_json::json!({"data":data}))
                };
                let response = response.to_string();
                let wire = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                    response.len()
                );
                stream.write_all(wire.as_bytes()).await.expect("response");
            }
        });
        (format!("http://{address}/v1/embeddings"), observed, task)
    }

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
            max_input_tokens: 0,
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

    #[tokio::test]
    async fn authentication_failure_waits_for_new_credentials_without_batch_learning_or_retry() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                read_request_body(&mut stream).await;
                observed.fetch_add(1, Ordering::SeqCst);
                stream.write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await.unwrap();
            }
        });
        let config = VectorConfig {
            enabled: true,
            model: "credential-route".into(),
            api_url: format!("http://{address}/embeddings"),
            dimension: 2,
            batch_size: 10,
            ..VectorConfig::default()
        };
        let client = EmbeddingClient::new(config.clone());
        for texts in [&["a", "b"][..], &["different"]] {
            assert!(matches!(
                client.clone().embed(texts).await,
                Err(MemoryError::EmbeddingCredentialsRequired)
            ));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "same route waits rather than replaying 401"
        );
        assert_eq!(client.learned_batch_cap.load(Ordering::Acquire), 10);
        assert!(client.deterministic_failures.read().await.is_empty());
        let replacement = EmbeddingClient::new(VectorConfig {
            api_key: "synthetic-updated-credential".into(),
            ..config
        });
        assert!(matches!(
            replacement.embed(&["a"]).await,
            Err(MemoryError::EmbeddingCredentialsRequired)
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "new configuration may verify its credential once"
        );
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn transient_rate_limit_and_server_errors_keep_the_bounded_retry_policy() {
        for status in [429, 500, 503] {
            let calls = AtomicUsize::new(0);
            let result = retry_with_backoff(3, 0, || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call < 2 {
                        Err(MemoryError::Store(format!("embedding API error {status}")))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
            assert!(result.is_ok());
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }
    }

    #[tokio::test]
    async fn provider_batch_cap_is_learned_once_and_reused_by_clones() {
        let (url, batches, server) = spawn_batch_limited_server(2).await;
        let client = EmbeddingClient::new(VectorConfig {
            enabled: true,
            model: "route-revision-a".into(),
            api_url: url,
            dimension: 2,
            batch_size: 4,
            max_input_tokens: 32,
            ..VectorConfig::default()
        });
        let inputs = ["a", "b", "c", "d", "e", "f", "g", "h"];
        assert_eq!(client.embed(&inputs).await.expect("first request").len(), 8);
        let clone = client.clone();
        assert_eq!(clone.embed(&inputs).await.expect("warm request").len(), 8);

        let observed = batches.lock().expect("batch recorder").clone();
        assert_eq!(observed.iter().filter(|size| **size > 2).count(), 1);
        assert_eq!(client.learned_batch_cap.load(Ordering::Acquire), 2);
        server.abort();
    }

    #[test]
    fn model_limit_and_chunk_preparation_are_conservative_and_ordered() {
        assert_eq!(inferred_model_input_limit("text-embedding-v4"), 8192);
        assert_eq!(inferred_model_input_limit("text-embedding-3-small"), 8191);
        assert_eq!(inferred_model_input_limit("unknown-private-model"), 2048);

        let prepared = prepare_embedding_inputs(&["abcdefghijklmnopqrst", "中文🙂"], 8)
            .expect("prepare bounded inputs");
        assert!(prepared.len() > 2);
        assert!(prepared
            .windows(2)
            .all(|pair| pair[0].original_index <= pair[1].original_index));
        assert!(prepared.iter().all(|input| {
            !input.text.trim().is_empty() && conservative_embedding_tokens(&input.text) <= 7
        }));
        assert!(matches!(
            prepare_embedding_inputs(&["  \n"], 8),
            Err(MemoryError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn overlong_inputs_are_chunked_before_http_and_coalesced_to_one_vector() {
        let (url, calls, requests, server) = spawn_embedding_server(false).await;
        let client = EmbeddingClient::new(VectorConfig {
            enabled: true,
            model: "test-embedding".into(),
            api_url: url,
            dimension: 2,
            batch_size: 20,
            max_input_tokens: 8,
            ..VectorConfig::default()
        });

        let vectors = client
            .embed(&["abcdefghijklmnopqrst", "中文🙂"])
            .await
            .expect("chunked embedding succeeds");
        assert_eq!(vectors.len(), 2, "one vector remains per original input");
        assert_eq!(vectors[0].len(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for request in requests.lock().expect("request recorder").iter() {
            for input in request["input"].as_array().expect("input array") {
                let text = input.as_str().expect("string input");
                assert!(!text.trim().is_empty());
                assert!(conservative_embedding_tokens(text) <= 7);
            }
        }
        server.abort();
    }

    #[tokio::test]
    async fn empty_input_and_cached_deterministic_400_never_repeat_external_work() {
        let (url, calls, _, server) = spawn_embedding_server(true).await;
        let client = EmbeddingClient::new(VectorConfig {
            enabled: true,
            model: "test-embedding".into(),
            api_url: url,
            max_input_tokens: 32,
            ..VectorConfig::default()
        });

        assert!(matches!(
            client.embed(&["   "]).await,
            Err(MemoryError::InvalidArgument(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let (first, concurrent_duplicate) = tokio::join!(
            client.embed(&["invalid payload"]),
            client.embed(&["invalid payload"])
        );
        assert!(first.is_err());
        assert!(concurrent_duplicate.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(client.embed(&["invalid payload"]).await.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "same deterministic payload is served by the negative cache"
        );
        assert!(client.embed(&["different invalid payload"]).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }
}
