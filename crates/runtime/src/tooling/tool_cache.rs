//! Small in-process cache for idempotent read-only tool results.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub invalidations: usize,
    pub entries: usize,
    pub epoch: u64,
    #[serde(rename = "scopeEpochs")]
    pub scope_epochs: usize,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    epoch: u64,
    scope_epoch: u64,
    scope: String,
    value: String,
}

#[derive(Default)]
struct ToolCacheState {
    epoch: u64,
    hits: usize,
    misses: usize,
    invalidations: usize,
    entries: HashMap<String, CacheEntry>,
    scope_epochs: HashMap<String, u64>,
}

static TOOL_CACHE: OnceLock<Mutex<ToolCacheState>> = OnceLock::new();

#[must_use]
pub fn get_cached_tool_result(tool_name: &str, input: &str) -> Option<String> {
    get_cached_tool_result_scoped(tool_name, input, "*")
}

#[must_use]
pub fn get_cached_tool_result_scoped(tool_name: &str, input: &str, scope: &str) -> Option<String> {
    let key = cache_key(tool_name, input);
    let mut guard = cache_state().lock().ok()?;
    let epoch = guard.epoch;
    let scope_epoch = guard.scope_epoch(scope);
    let value = guard
        .entries
        .get(&key)
        .filter(|entry| entry.epoch == epoch)
        .filter(|entry| entry.scope == scope && entry.scope_epoch == scope_epoch)
        .map(|entry| entry.value.clone());
    if value.is_some() {
        guard.hits += 1;
    } else {
        guard.misses += 1;
    }
    value
}

pub fn put_cached_tool_result(tool_name: &str, input: &str, value: &str) {
    put_cached_tool_result_scoped(tool_name, input, "*", value);
}

pub fn put_cached_tool_result_scoped(tool_name: &str, input: &str, scope: &str, value: &str) {
    let key = cache_key(tool_name, input);
    if let Ok(mut guard) = cache_state().lock() {
        let epoch = guard.epoch;
        let scope_epoch = guard.scope_epoch(scope);
        guard.entries.insert(
            key,
            CacheEntry {
                epoch,
                scope_epoch,
                scope: scope.to_string(),
                value: value.to_string(),
            },
        );
    }
}

pub fn invalidate_tool_cache_scope(scope: &str) {
    if let Ok(mut guard) = cache_state().lock() {
        let next_epoch = guard.scope_epoch(scope).saturating_add(1);
        guard.scope_epochs.insert(scope.to_string(), next_epoch);
        guard.invalidations = guard.invalidations.saturating_add(1);
    }
}

pub fn invalidate_tool_cache() {
    if let Ok(mut guard) = cache_state().lock() {
        guard.epoch = guard.epoch.saturating_add(1);
        guard.invalidations = guard.invalidations.saturating_add(1);
        guard.entries.clear();
    }
}

#[must_use]
pub fn tool_cache_stats() -> ToolCacheStats {
    let guard = cache_state()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ToolCacheStats {
        hits: guard.hits,
        misses: guard.misses,
        invalidations: guard.invalidations,
        entries: guard.entries.len(),
        epoch: guard.epoch,
        scope_epochs: guard.scope_epochs.len(),
    }
}

pub fn reset_tool_cache_for_tests() {
    if let Ok(mut guard) = cache_state().lock() {
        *guard = ToolCacheState::default();
    }
}

fn cache_state() -> &'static Mutex<ToolCacheState> {
    TOOL_CACHE.get_or_init(|| Mutex::new(ToolCacheState::default()))
}

impl ToolCacheState {
    fn scope_epoch(&self, scope: &str) -> u64 {
        self.scope_epochs.get(scope).copied().unwrap_or_default()
    }
}

fn cache_key(tool_name: &str, input: &str) -> String {
    let cwd = std::env::current_dir()
        .ok()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!("v1::{cwd}::{tool_name}::{}", canonical_json(input))
}

fn canonical_json(input: &str) -> String {
    serde_json::from_str::<serde_json::Value>(input)
        .and_then(|value| serde_json::to_string(&value))
        .unwrap_or_else(|_| input.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn cache_hits_then_invalidates() {
        let _guard = test_lock();
        reset_tool_cache_for_tests();
        assert!(get_cached_tool_result("read_file", r#"{"path":"a"}"#).is_none());
        put_cached_tool_result("read_file", r#"{"path":"a"}"#, "ok");
        assert_eq!(
            get_cached_tool_result("read_file", r#"{"path":"a"}"#).as_deref(),
            Some("ok")
        );
        invalidate_tool_cache();
        assert!(get_cached_tool_result("read_file", r#"{"path":"a"}"#).is_none());
        let stats = tool_cache_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.invalidations, 1);
    }

    #[test]
    fn scoped_cache_invalidation_only_expires_matching_scope() {
        let _guard = test_lock();
        reset_tool_cache_for_tests();
        put_cached_tool_result_scoped("read_file", r#"{"path":"a"}"#, "file:a", "a");
        put_cached_tool_result_scoped("read_file", r#"{"path":"b"}"#, "file:b", "b");

        invalidate_tool_cache_scope("file:a");

        assert!(get_cached_tool_result_scoped("read_file", r#"{"path":"a"}"#, "file:a").is_none());
        assert_eq!(
            get_cached_tool_result_scoped("read_file", r#"{"path":"b"}"#, "file:b").as_deref(),
            Some("b")
        );
        let stats = tool_cache_stats();
        assert_eq!(stats.invalidations, 1);
        assert_eq!(stats.scope_epochs, 1);
        reset_tool_cache_for_tests();
    }
}
