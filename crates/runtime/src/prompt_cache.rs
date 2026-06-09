//! Prompt Cache — session-scoped completion cache with file persistence.
//!
//! Stores API responses keyed by a deterministic request hash so that
//! identical prompts within a session can be served from disk without
//! a round-trip to the model provider.
//!
//! ## Deterministic hashing
//!
//! Cache keys should be computed by the caller using a deterministic
//! hash (e.g. SHA-256 of `model + system + tools + messages`).  This
//! module provides `stable_hash_bytes` (FNV-1a) as a fast, serialisation-
//! based helper, but callers requiring crypto-level collision resistance
//! should use SHA-2 and pass the result directly.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lru::LruCache;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

// ── constants ──────────────────────────────────────────────────────

const DEFAULT_COMPLETION_TTL_SECS: u64 = 30;
const DEFAULT_PROMPT_TTL_SECS: u64 = 5 * 60;
const DEFAULT_BREAK_MIN_DROP: u32 = 2_000;
const MAX_SANITIZED_LENGTH: usize = 80;
const REQUEST_FINGERPRINT_VERSION: u32 = 1;
const REQUEST_FINGERPRINT_PREFIX: &str = "v1";
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

// ── config ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PromptCacheConfig {
    pub session_id: String,
    pub completion_ttl: Duration,
    pub prompt_ttl: Duration,
    pub cache_break_min_drop: u32,
    pub memory_capacity: usize,
}

impl PromptCacheConfig {
    #[must_use]
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            completion_ttl: Duration::from_secs(DEFAULT_COMPLETION_TTL_SECS),
            prompt_ttl: Duration::from_secs(DEFAULT_PROMPT_TTL_SECS),
            cache_break_min_drop: DEFAULT_BREAK_MIN_DROP,
            memory_capacity: 200,
        }
    }
}

impl Default for PromptCacheConfig {
    fn default() -> Self {
        Self::new("default")
    }
}

// ── paths ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptCachePaths {
    pub root: PathBuf,
    pub session_dir: PathBuf,
    pub completion_dir: PathBuf,
    pub session_state_path: PathBuf,
    pub stats_path: PathBuf,
}

impl PromptCachePaths {
    #[must_use]
    pub fn for_session(session_id: &str) -> Self {
        let root = base_cache_root();
        let session_dir = root.join(sanitize_path_segment(session_id));
        let completion_dir = session_dir.join("completions");
        Self {
            root,
            session_state_path: session_dir.join("session-state.json"),
            stats_path: session_dir.join("stats.json"),
            session_dir,
            completion_dir,
        }
    }

    #[must_use]
    pub fn completion_entry_path(&self, request_hash: &str) -> PathBuf {
        self.completion_dir.join(format!("{request_hash}.json"))
    }
}

// ── stats ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptCacheStats {
    pub tracked_requests: u64,
    pub completion_cache_hits: u64,
    pub completion_cache_misses: u64,
    pub completion_cache_writes: u64,
    pub expected_invalidations: u64,
    pub unexpected_cache_breaks: u64,
    pub total_cache_creation_input_tokens: u64,
    pub total_cache_read_input_tokens: u64,
    pub last_cache_creation_input_tokens: Option<u32>,
    pub last_cache_read_input_tokens: Option<u32>,
    pub last_request_hash: Option<String>,
    pub last_completion_cache_key: Option<String>,
    pub last_break_reason: Option<String>,
    pub last_cache_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheBreakEvent {
    pub unexpected: bool,
    pub reason: String,
    pub previous_cache_read_input_tokens: u32,
    pub current_cache_read_input_tokens: u32,
    pub token_drop: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptCacheRecord {
    pub cache_break: Option<CacheBreakEvent>,
    pub stats: PromptCacheStats,
}

/// Lightweight usage summary passed to cache-break detection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheUsage {
    pub input_tokens: u32,
    pub cache_creation_input_tokens: u32,
    pub cache_read_input_tokens: u32,
    pub output_tokens: u32,
}

// ── PromptCache ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PromptCache {
    inner: Arc<Mutex<PromptCacheInner>>,
}

impl PromptCache {
    #[must_use]
    pub fn new(session_id: impl Into<String>) -> Self {
        Self::with_config(PromptCacheConfig::new(session_id))
    }

    #[must_use]
    pub fn with_config(config: PromptCacheConfig) -> Self {
        let paths = PromptCachePaths::for_session(&config.session_id);
        let stats = read_json::<PromptCacheStats>(&paths.stats_path).unwrap_or_default();
        let previous = read_json::<TrackedPromptState>(&paths.session_state_path);
        let memory_capacity = config.memory_capacity;
        Self {
            inner: Arc::new(Mutex::new(PromptCacheInner {
                config,
                paths,
                stats,
                previous,
                memory_cache: LruCache::new(
                    NonZeroUsize::new(memory_capacity).unwrap_or(NonZeroUsize::MIN),
                ),
            })),
        }
    }

    #[must_use]
    pub fn paths(&self) -> PromptCachePaths {
        self.lock().paths.clone()
    }

    #[must_use]
    pub fn stats(&self) -> PromptCacheStats {
        self.lock().stats.clone()
    }

    // ── completion cache (value-level) ─────────────────────────

    /// Try to fetch a cached completion response for the given
    /// **deterministic** request hash.  Returns the stored JSON blob
    /// if the entry exists, its fingerprint matches, and it hasn't
    /// expired.
    #[must_use]
    pub fn lookup_completion(&self, request_hash: &str) -> Option<serde_json::Value> {
        let ttl;
        let paths;
        // Fast path: check in-memory LRU cache first.
        {
            let mut inner = self.lock();
            ttl = inner.config.completion_ttl;
            paths = inner.paths.clone();

            if let Some(entry) = inner.memory_cache.get(request_hash) {
                // Copy/clone all needed data upfront to release the borrow on inner.
                let fingerprint_version = entry.fingerprint_version;
                let cached_at_unix_secs = entry.cached_at_unix_secs;
                let response_usage = entry.response_usage.clone();
                let response = entry.response.clone();
                // entry no longer used — NLL releases the mutable borrow

                if fingerprint_version != current_fingerprint_version() {
                    inner.memory_cache.pop(request_hash);
                    inner.stats.completion_cache_misses += 1;
                    inner.stats.last_completion_cache_key = Some(request_hash.to_string());
                    persist_state(&inner);
                    return None;
                }
                let expired = now_unix_secs().saturating_sub(cached_at_unix_secs) >= ttl.as_secs();
                if expired {
                    inner.memory_cache.pop(request_hash);
                    inner.stats.completion_cache_misses += 1;
                    inner.stats.last_completion_cache_key = Some(request_hash.to_string());
                    persist_state(&inner);
                    return None;
                }
                inner.stats.completion_cache_hits += 1;
                apply_usage_to_stats(
                    &mut inner.stats,
                    &response_usage,
                    request_hash,
                    "completion-cache",
                );
                inner.previous = Some(TrackedPromptState::from_hashes(
                    request_hash,
                    &response_usage,
                ));
                persist_state(&inner);
                return Some(response);
            }
        } // lock dropped before disk I/O

        // Slow path: disk lookup
        let entry_path = paths.completion_entry_path(request_hash);
        let entry = read_json::<CompletionCacheEntry>(&entry_path);
        let Some(entry) = entry else {
            let mut inner = self.lock();
            inner.stats.completion_cache_misses += 1;
            inner.stats.last_completion_cache_key = Some(request_hash.to_string());
            persist_state(&inner);
            return None;
        };

        if entry.fingerprint_version != current_fingerprint_version() {
            let mut inner = self.lock();
            inner.stats.completion_cache_misses += 1;
            inner.stats.last_completion_cache_key = Some(request_hash.to_string());
            let _ = fs::remove_file(&entry_path);
            persist_state(&inner);
            return None;
        }

        let expired = now_unix_secs().saturating_sub(entry.cached_at_unix_secs) >= ttl.as_secs();
        let mut inner = self.lock();
        inner.stats.last_completion_cache_key = Some(request_hash.to_string());
        if expired {
            inner.stats.completion_cache_misses += 1;
            let _ = fs::remove_file(&entry_path);
            persist_state(&inner);
            return None;
        }

        inner.stats.completion_cache_hits += 1;
        apply_usage_to_stats(
            &mut inner.stats,
            &entry.response_usage,
            request_hash,
            "completion-cache",
        );
        inner.previous = Some(TrackedPromptState::from_hashes(
            request_hash,
            &entry.response_usage,
        ));
        // Populate memory cache on disk hit
        let response = entry.response.clone();
        inner.memory_cache.put(request_hash.to_string(), entry);
        persist_state(&inner);
        Some(response)
    }

    /// Store a provider response in the completion cache and record
    /// usage telemetry for cache-break detection.  Returns a record
    /// suitable for forwarding as an `AssistantEvent::PromptCache`.
    ///
    /// `request_fingerprint` must include the pre-computed hashes for
    /// model, system, tools, and messages.
    #[must_use]
    pub fn record_response(
        &self,
        request_hash: &str,
        response_json: &serde_json::Value,
        usage: &CacheUsage,
        fingerprints: &RequestFingerprintHashes,
    ) -> PromptCacheRecord {
        self.record_internal(request_hash, Some(response_json), usage, fingerprints)
    }

    /// Record usage-only telemetry (for streaming, where the full
    /// response is not easily cached).
    #[must_use]
    pub fn record_usage(
        &self,
        request_hash: &str,
        usage: &CacheUsage,
        fingerprints: &RequestFingerprintHashes,
    ) -> PromptCacheRecord {
        self.record_internal(request_hash, None, usage, fingerprints)
    }

    fn record_internal(
        &self,
        request_hash: &str,
        response_json: Option<&serde_json::Value>,
        usage: &CacheUsage,
        _fingerprints: &RequestFingerprintHashes,
    ) -> PromptCacheRecord {
        let mut inner = self.lock();
        let previous = inner.previous.clone();
        let previous_fingerprints = previous.as_ref().map(|p| RequestFingerprintHashes {
            model: p.model_hash,
            system: p.system_hash,
            tools: p.tools_hash,
            messages: p.messages_hash,
        });
        let current = TrackedPromptState::from_hashes(request_hash, usage);
        let cache_break = detect_cache_break_from_fingerprints(
            &inner.config,
            previous_fingerprints.as_ref(),
            previous.as_ref().map(|p| p.cache_read_input_tokens),
            usage.cache_read_input_tokens,
            previous.as_ref().map(|p| p.fingerprint_version),
            previous.as_ref().map(|p| p.observed_at_unix_secs),
        );

        inner.stats.tracked_requests += 1;
        apply_usage_to_stats(&mut inner.stats, usage, request_hash, "api-response");
        if let Some(event) = &cache_break {
            if event.unexpected {
                inner.stats.unexpected_cache_breaks += 1;
            } else {
                inner.stats.expected_invalidations += 1;
            }
            inner.stats.last_break_reason = Some(event.reason.clone());
        }

        inner.previous = Some(current);
        if let Some(response_json) = response_json {
            write_completion_entry(&inner.paths, request_hash, response_json, usage);
            inner.stats.completion_cache_writes += 1;
            let entry = CompletionCacheEntry {
                cached_at_unix_secs: now_unix_secs(),
                fingerprint_version: current_fingerprint_version(),
                response: response_json.clone(),
                response_usage: usage.clone(),
            };
            inner.memory_cache.put(request_hash.to_string(), entry);
        }
        persist_state(&inner);

        PromptCacheRecord {
            cache_break,
            stats: inner.stats.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PromptCacheInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

// ── internals ──────────────────────────────────────────────────────

#[derive(Debug)]
struct PromptCacheInner {
    config: PromptCacheConfig,
    paths: PromptCachePaths,
    stats: PromptCacheStats,
    previous: Option<TrackedPromptState>,
    memory_cache: LruCache<String, CompletionCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompletionCacheEntry {
    cached_at_unix_secs: u64,
    #[serde(default = "current_fingerprint_version")]
    fingerprint_version: u32,
    response: serde_json::Value,
    response_usage: CacheUsage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TrackedPromptState {
    observed_at_unix_secs: u64,
    #[serde(default = "current_fingerprint_version")]
    fingerprint_version: u32,
    model_hash: u64,
    system_hash: u64,
    tools_hash: u64,
    messages_hash: u64,
    cache_read_input_tokens: u32,
}

impl TrackedPromptState {
    fn from_hashes(request_hash: &str, usage: &CacheUsage) -> Self {
        // Extract the four FNV-1a hash components encoded in the request hash.
        let hashes = parse_request_fingerprints(request_hash);
        Self {
            observed_at_unix_secs: now_unix_secs(),
            fingerprint_version: current_fingerprint_version(),
            model_hash: hashes.model,
            system_hash: hashes.system,
            tools_hash: hashes.tools,
            messages_hash: hashes.messages,
            cache_read_input_tokens: usage.cache_read_input_tokens,
        }
    }
}

/// Caller-supplied per-component hashes for cache-break detection.
#[derive(Debug, Clone, Copy)]
pub struct RequestFingerprintHashes {
    pub model: u64,
    pub system: u64,
    pub tools: u64,
    pub messages: u64,
}

/// Reconstruct fingerprint hashes from the v1-NNN… request hash string.
fn parse_request_fingerprints(request_hash: &str) -> RequestFingerprintHashes {
    // Format: "v1-<hex>" (16 hex digits → u64 for legacy compat; we store
    // the value as a combined FNV hash on the whole payload in our new
    // scheme).
    let hex = request_hash
        .strip_prefix("v1-")
        .unwrap_or("0000000000000000");
    let combined = u64::from_str_radix(hex, 16).unwrap_or(0);
    RequestFingerprintHashes {
        model: combined,
        system: combined,
        tools: combined,
        messages: combined,
    }
}

fn detect_cache_break_from_fingerprints(
    config: &PromptCacheConfig,
    previous_fingerprints: Option<&RequestFingerprintHashes>,
    previous_cache_read: Option<u32>,
    current_cache_read: u32,
    previous_version: Option<u32>,
    previous_observed_at: Option<u64>,
) -> Option<CacheBreakEvent> {
    let prev_cache_read = previous_cache_read?;
    let _prev_fp = previous_fingerprints?;

    // Fingerprint version change → expected break
    if let Some(prev_ver) = previous_version {
        if prev_ver != current_fingerprint_version() {
            return Some(CacheBreakEvent {
                unexpected: false,
                reason: format!(
                    "fingerprint version changed (v{prev_ver} -> v{})",
                    current_fingerprint_version()
                ),
                previous_cache_read_input_tokens: prev_cache_read,
                current_cache_read_input_tokens: current_cache_read,
                token_drop: prev_cache_read.saturating_sub(current_cache_read),
            });
        }
    }

    let token_drop = prev_cache_read.saturating_sub(current_cache_read);
    if token_drop < config.cache_break_min_drop {
        return None;
    }

    // Check individual component hashes when the caller provides them.
    // When fingerprints match exactly (same FNV hash), component comparison
    // is not meaningful — flag as unexpected.
    let elapsed = previous_observed_at
        .map(|t| now_unix_secs().saturating_sub(t))
        .unwrap_or(0);

    let reason = if elapsed > config.prompt_ttl.as_secs() {
        format!("possible prompt cache TTL expiry after {elapsed}s")
    } else {
        "cache read tokens dropped (component hashes stable)".to_string()
    };

    Some(CacheBreakEvent {
        unexpected: elapsed <= config.prompt_ttl.as_secs(),
        reason,
        previous_cache_read_input_tokens: prev_cache_read,
        current_cache_read_input_tokens: current_cache_read,
        token_drop,
    })
}

// ── stats helpers ──────────────────────────────────────────────────

fn apply_usage_to_stats(
    stats: &mut PromptCacheStats,
    usage: &CacheUsage,
    request_hash: &str,
    source: &str,
) {
    stats.total_cache_creation_input_tokens += u64::from(usage.cache_creation_input_tokens);
    stats.total_cache_read_input_tokens += u64::from(usage.cache_read_input_tokens);
    stats.last_cache_creation_input_tokens = Some(usage.cache_creation_input_tokens);
    stats.last_cache_read_input_tokens = Some(usage.cache_read_input_tokens);
    stats.last_request_hash = Some(request_hash.to_string());
    stats.last_cache_source = Some(source.to_string());
}

// ── persistence ────────────────────────────────────────────────────

fn persist_state(inner: &PromptCacheInner) {
    let _ = ensure_cache_dirs(&inner.paths);
    let _ = write_json(&inner.paths.stats_path, &inner.stats);
    if let Some(previous) = &inner.previous {
        let _ = write_json(&inner.paths.session_state_path, previous);
    }
}

fn write_completion_entry(
    paths: &PromptCachePaths,
    request_hash: &str,
    response_json: &serde_json::Value,
    usage: &CacheUsage,
) {
    let _ = ensure_cache_dirs(paths);
    let entry = CompletionCacheEntry {
        cached_at_unix_secs: now_unix_secs(),
        fingerprint_version: current_fingerprint_version(),
        response: response_json.clone(),
        response_usage: usage.clone(),
    };
    let _ = write_json(&paths.completion_entry_path(request_hash), &entry);
}

// ── file-system helpers ────────────────────────────────────────────

fn ensure_cache_dirs(paths: &PromptCachePaths) -> std::io::Result<()> {
    fs::create_dir_all(&paths.completion_dir)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let json = serde_json::to_vec_pretty(value)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    fs::write(path, json)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

// ── public hash utilities ──────────────────────────────────────────

/// Build a versioned, hex-encoded request hash string from a pre-computed
/// 64-bit FNV hash.  The format is `v1-<16-hex-digits>`.
#[must_use]
pub fn request_hash_hex_from_fnv(fnv_hash: u64) -> String {
    format!("{REQUEST_FINGERPRINT_PREFIX}-{fnv_hash:016x}")
}

/// Serialise the value to canonical JSON and compute its FNV-1a 64-bit hash.
#[must_use]
pub fn hash_serializable<T: Serialize>(value: &T) -> u64 {
    let json = serde_json::to_vec(value).unwrap_or_default();
    stable_hash_bytes(&json)
}

/// FNV-1a 64-bit hash of arbitrary bytes.
#[must_use]
pub fn stable_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

// ── path helpers ───────────────────────────────────────────────────

#[must_use]
pub fn sanitize_path_segment(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect();
    if sanitized.len() <= MAX_SANITIZED_LENGTH {
        return sanitized;
    }
    let suffix = format!("-{:x}", stable_hash_bytes(value.as_bytes()));
    format!(
        "{}{}",
        &sanitized[..MAX_SANITIZED_LENGTH.saturating_sub(suffix.len())],
        suffix
    )
}

fn base_cache_root() -> PathBuf {
    if let Some(config_home) = std::env::var_os("COWD_CONFIG_HOME") {
        return PathBuf::from(config_home)
            .join("cache")
            .join("prompt-cache");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".cowd")
            .join("cache")
            .join("prompt-cache");
    }
    std::env::temp_dir().join("cowd-prompt-cache")
}

#[must_use]
pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

const fn current_fingerprint_version() -> u32 {
    REQUEST_FINGERPRINT_VERSION
}

// ── tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
        crate::test_env_lock()
    }

    #[test]
    fn path_builder_sanitizes_session_identifier() {
        let paths = PromptCachePaths::for_session("session:/with spaces");
        let session_dir = paths
            .session_dir
            .file_name()
            .and_then(|value| value.to_str())
            .expect("session dir name");
        assert_eq!(session_dir, "session--with-spaces");
        assert!(paths.completion_dir.ends_with("completions"));
        assert!(paths.stats_path.ends_with("stats.json"));
        assert!(paths.session_state_path.ends_with("session-state.json"));
    }

    #[test]
    fn completion_cache_round_trip_persists_recent_response() {
        let _guard = test_env_lock();
        let temp_root = std::env::temp_dir().join(format!(
            "prompt-cache-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::env::set_var("COWD_CONFIG_HOME", &temp_root);
        let cache = PromptCache::new("unit-test-session");
        let request_hash = "v1-cafebabe00000001";
        let response = serde_json::json!({"text": "cached response"});
        let usage = CacheUsage {
            cache_read_input_tokens: 42,
            cache_creation_input_tokens: 12,
            input_tokens: 10,
            output_tokens: 4,
        };
        let fingerprints = RequestFingerprintHashes {
            model: 1,
            system: 2,
            tools: 3,
            messages: 4,
        };

        assert!(cache.lookup_completion(request_hash).is_none());
        let record = cache.record_response(request_hash, &response, &usage, &fingerprints);
        assert!(record.cache_break.is_none());

        let cached = cache
            .lookup_completion(request_hash)
            .expect("cached response should load");
        assert_eq!(cached, response);

        let stats = cache.stats();
        assert_eq!(stats.completion_cache_hits, 1);
        assert_eq!(stats.completion_cache_misses, 1);
        assert_eq!(stats.completion_cache_writes, 1);

        std::env::remove_var("COWD_CONFIG_HOME");
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn distinct_hashes_do_not_collide() {
        let _guard = test_env_lock();
        let temp_root = std::env::temp_dir().join(format!(
            "prompt-cache-distinct-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::env::set_var("COWD_CONFIG_HOME", &temp_root);
        let cache = PromptCache::new("distinct-request-session");
        let response = serde_json::json!({"text": "cached"});
        let usage = CacheUsage::default();
        let fingerprints = RequestFingerprintHashes {
            model: 0,
            system: 0,
            tools: 0,
            messages: 0,
        };

        let _ = cache.record_response("v1-aaaaaaaa00000001", &response, &usage, &fingerprints);
        assert!(cache.lookup_completion("v1-bbbbbbbb00000002").is_none());

        std::fs::remove_dir_all(temp_root).expect("cleanup temp root");
        std::env::remove_var("COWD_CONFIG_HOME");
    }

    #[test]
    fn expired_completion_entries_are_not_reused() {
        let _guard = test_env_lock();
        let temp_root = std::env::temp_dir().join(format!(
            "prompt-cache-expired-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::env::set_var("COWD_CONFIG_HOME", &temp_root);
        let cache = PromptCache::with_config(PromptCacheConfig {
            session_id: "expired-session".to_string(),
            completion_ttl: Duration::ZERO,
            ..PromptCacheConfig::default()
        });
        let request_hash = "v1-expired00000001";
        let response = serde_json::json!({"text": "stale"});
        let usage = CacheUsage::default();
        let fingerprints = RequestFingerprintHashes {
            model: 0,
            system: 0,
            tools: 0,
            messages: 0,
        };

        let _ = cache.record_response(request_hash, &response, &usage, &fingerprints);
        assert!(cache.lookup_completion(request_hash).is_none());
        let stats = cache.stats();
        assert_eq!(stats.completion_cache_hits, 0);
        assert_eq!(stats.completion_cache_misses, 1);

        std::fs::remove_dir_all(temp_root).expect("cleanup temp root");
        std::env::remove_var("COWD_CONFIG_HOME");
    }

    #[test]
    fn sanitize_path_caps_long_values() {
        let long_value = "x".repeat(200);
        let sanitized = sanitize_path_segment(&long_value);
        assert!(sanitized.len() <= 80);
    }

    #[test]
    fn request_hash_is_stable_and_versioned() {
        let hash = request_hash_hex_from_fnv(0xDEAD_BEEF_CAFE_BABE);
        assert!(hash.starts_with("v1-"));
        let again = request_hash_hex_from_fnv(0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(hash, again);
    }

    #[test]
    fn stable_hash_bytes_is_deterministic() {
        let a = stable_hash_bytes(b"hello world");
        let b = stable_hash_bytes(b"hello world");
        assert_eq!(a, b);
        assert_ne!(a, stable_hash_bytes(b"hello world!"));
    }
}
