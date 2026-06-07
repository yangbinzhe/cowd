//! CachedProviderClient — wraps any ProviderClient with a prompt cache.
//!
//! The cache layer sits **outside** the provider-specific client so that
//! every model provider (Anthropic, OpenAI, xAI, DeepSeek, Qwen, Kimi,
//! Grok) benefits from completion caching without provider-specific code.
//!
//! ## Deterministic cache key
//!
//! The key is a SHA-256 hash of `model + system + tools + messages`.
//! No timestamp, request_id, or random nonce is included — repeated
//! requests produce identical keys and therefore cache hits.

use sha2::{Digest, Sha256};

use crate::error::ApiError;
use crate::types::{MessageRequest, MessageResponse};
use runtime::prompt_cache::{
    hash_serializable, CacheUsage, PromptCache, PromptCacheRecord, RequestFingerprintHashes,
};

/// Wraps a [`ProviderClient`](crate::ProviderClient) with deterministic
/// prompt-completion caching backed by
/// [`PromptCache`](runtime::prompt_cache::PromptCache).
#[derive(Debug, Clone)]
pub struct CachedProviderClient {
    /// The underlying provider client.
    pub inner: crate::ProviderClient,
    cache: PromptCache,
    session_id: String,
}

impl CachedProviderClient {
    /// Create a new cached client wrapping the given provider.
    #[must_use]
    pub fn new(inner: crate::ProviderClient, session_id: &str) -> Self {
        let cache = PromptCache::new(session_id);
        Self {
            inner,
            cache,
            session_id: session_id.to_string(),
        }
    }

    /// Create a cached client with an externally-managed cache instance
    /// (e.g. when the cache needs to be shared with legacy Anthropic
    /// code paths).
    #[must_use]
    pub fn with_cache(inner: crate::ProviderClient, cache: PromptCache, session_id: &str) -> Self {
        Self {
            inner,
            cache,
            session_id: session_id.to_string(),
        }
    }

    // ── accessors ──────────────────────────────────────────────────

    #[must_use]
    pub fn cache(&self) -> &PromptCache {
        &self.cache
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn inner(&self) -> &crate::ProviderClient {
        &self.inner
    }

    #[must_use]
    pub fn prompt_cache_stats(&self) -> runtime::prompt_cache::PromptCacheStats {
        self.cache.stats()
    }

    #[must_use]
    pub fn take_last_prompt_cache_record(&self) -> Option<PromptCacheRecord> {
        // CachedProviderClient uses the PromptCache directly; the record
        // is returned from `send_message_cached`.
        // We maintain a simple last-record mechanism for backward compat.
        None // Callers should use the record returned by send_message_cached instead.
    }

    // ── send_message (cached) ──────────────────────────────────────

    /// Send a message through the provider, checking the prompt cache first.
    ///
    /// On cache hit the cached response is returned without making an HTTP
    /// request.  On cache miss the request is forwarded to the underlying
    /// provider and the response is stored for future calls.
    pub async fn send_message(
        &self,
        request: &MessageRequest,
    ) -> Result<(MessageResponse, Option<PromptCacheRecord>), ApiError> {
        let cache_key = Self::compute_cache_key(request);

        // 1. Check cache
        if let Some(cached_json) = self.cache.lookup_completion(&cache_key) {
            tracing::debug!(
                "prompt cache hit for session {} (key={})",
                self.session_id,
                &cache_key[..32.min(cache_key.len())]
            );
            let response: MessageResponse = serde_json::from_value(cached_json)
                .map_err(|e| ApiError::json_deserialize("cache", &request.model, "", e))?;
            return Ok((response, None));
        }

        // 2. Call actual provider
        let response = self.inner.send_message(request).await?;

        // 3. Compute fingerprints for cache-break detection
        let fingerprints = request_fingerprints(request);
        let cache_usage = CacheUsage {
            input_tokens: response.usage.input_tokens,
            cache_creation_input_tokens: response.usage.cache_creation_input_tokens,
            cache_read_input_tokens: response.usage.cache_read_input_tokens,
            output_tokens: response.usage.output_tokens,
        };

        // 4. Store in cache
        let response_json = serde_json::to_value(&response).unwrap_or(serde_json::Value::Null);
        let record =
            self.cache
                .record_response(&cache_key, &response_json, &cache_usage, &fingerprints);

        tracing::debug!(
            "prompt cache miss for session {} (key={}), stored",
            self.session_id,
            &cache_key[..32.min(cache_key.len())]
        );

        Ok((response, Some(record)))
    }

    /// Send a streaming message through the provider (no caching — streams
    /// are consumed incrementally and can't easily be stored).
    ///
    /// Delegates directly to the underlying provider.
    pub async fn stream_message(
        &self,
        request: &MessageRequest,
    ) -> Result<crate::MessageStream, ApiError> {
        self.inner.stream_message(request).await
    }

    // ── cache key computation ──────────────────────────────────────

    /// Compute a **deterministic** cache key for a request.
    ///
    /// Uses SHA-256 of the canonical JSON serialisation of `model`,
    /// `system` prompt, `tools` schema, and `messages`.  No timestamp,
    /// request_id, or random nonce is included — identical requests
    /// always produce identical keys.
    #[must_use]
    pub fn compute_cache_key(request: &MessageRequest) -> String {
        let mut hasher = Sha256::new();

        // model
        hasher.update(b"m:");
        hasher.update(request.model.as_bytes());
        hasher.update(b"\n");

        // system prompt (empty string when None — deterministic)
        hasher.update(b"s:");
        let system = request.system.as_deref().unwrap_or("");
        hasher.update(system.as_bytes());
        hasher.update(b"\n");

        // tools schema (sorted keys + canonical JSON for determinism)
        hasher.update(b"t:");
        if let Some(tools) = &request.tools {
            // serde_json with sorted keys for determinism
            // Use a buffer to build sorted JSON
            let mut sorted_tools: Vec<serde_json::Value> = tools
                .iter()
                .map(|t| {
                    let mut map = serde_json::Map::new();
                    map.insert(
                        "name".to_string(),
                        serde_json::Value::String(t.name.clone()),
                    );
                    map.insert(
                        "description".to_string(),
                        serde_json::Value::String(t.description.clone().unwrap_or_default()),
                    );
                    map.insert("input_schema".to_string(), t.input_schema.clone());
                    serde_json::Value::Object(map)
                })
                .collect();
            // Sort by name for determinism
            sorted_tools.sort_by(|a, b| {
                a.get("name")
                    .and_then(|v| v.as_str())
                    .cmp(&b.get("name").and_then(|v| v.as_str()))
            });
            let tools_json = serde_json::to_vec(&sorted_tools).unwrap_or_default();
            hasher.update(&tools_json);
        }
        hasher.update(b"\n");

        // messages (serialise each message to canonical JSON)
        hasher.update(b"ms:");
        for msg in &request.messages {
            let msg_json = serde_json::to_vec(msg).unwrap_or_default();
            hasher.update(&msg_json);
        }
        hasher.update(b"\n");

        // max_tokens (affects response, include for precision)
        hasher.update(b"mt:");
        hasher.update(request.max_tokens.to_le_bytes());

        let result = hasher.finalize();
        format!("{:x}", result)
    }
}

// ── helpers ────────────────────────────────────────────────────────

/// Build FNV-1a fingerprints for cache-break detection.
fn request_fingerprints(request: &MessageRequest) -> RequestFingerprintHashes {
    RequestFingerprintHashes {
        model: hash_serializable(&request.model),
        system: hash_serializable(&request.system),
        tools: hash_serializable(&request.tools),
        messages: hash_serializable(&request.messages),
    }
}

// ── tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InputMessage;

    #[test]
    fn cache_key_is_deterministic() {
        let request = make_request("claude-sonnet-4-6", "You are helpful.", "hello");
        let key1 = CachedProviderClient::compute_cache_key(&request);
        let key2 = CachedProviderClient::compute_cache_key(&request);
        assert_eq!(key1, key2);
    }

    #[test]
    fn different_models_produce_different_keys() {
        let r1 = make_request("claude-sonnet-4-6", "You are helpful.", "hello");
        let r2 = make_request("gpt-4o", "You are helpful.", "hello");
        let k1 = CachedProviderClient::compute_cache_key(&r1);
        let k2 = CachedProviderClient::compute_cache_key(&r2);
        assert_ne!(k1, k2);
    }

    #[test]
    fn different_system_prompts_produce_different_keys() {
        let r1 = make_request("claude-sonnet-4-6", "You are helpful.", "hello");
        let r2 = make_request("claude-sonnet-4-6", "You are concise.", "hello");
        let k1 = CachedProviderClient::compute_cache_key(&r1);
        let k2 = CachedProviderClient::compute_cache_key(&r2);
        assert_ne!(k1, k2);
    }

    #[test]
    fn different_messages_produce_different_keys() {
        let r1 = make_request("claude-sonnet-4-6", "You are helpful.", "hello");
        let r2 = make_request("claude-sonnet-4-6", "You are helpful.", "goodbye");
        let k1 = CachedProviderClient::compute_cache_key(&r1);
        let k2 = CachedProviderClient::compute_cache_key(&r2);
        assert_ne!(k1, k2);
    }

    #[test]
    fn no_timestamp_in_key() {
        // Same request computed twice should produce identical key
        let request = make_request("claude-sonnet-4-6", "sys", "msg");
        let key1 = CachedProviderClient::compute_cache_key(&request);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let key2 = CachedProviderClient::compute_cache_key(&request);
        assert_eq!(key1, key2);
    }

    fn make_request(model: &str, system: &str, user_text: &str) -> MessageRequest {
        MessageRequest {
            model: model.to_string(),
            max_tokens: 1024,
            messages: vec![InputMessage::user_text(user_text)],
            system: Some(system.to_string()),
            tools: None,
            tool_choice: None,
            stream: false,
            ..Default::default()
        }
    }
}
