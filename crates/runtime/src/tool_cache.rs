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
}

#[derive(Debug, Clone)]
struct CacheEntry {
    epoch: u64,
    value: String,
}

#[derive(Default)]
struct ToolCacheState {
    epoch: u64,
    hits: usize,
    misses: usize,
    invalidations: usize,
    entries: HashMap<String, CacheEntry>,
}

static TOOL_CACHE: OnceLock<Mutex<ToolCacheState>> = OnceLock::new();

#[must_use]
pub fn get_cached_tool_result(tool_name: &str, input: &str) -> Option<String> {
    let key = cache_key(tool_name, input);
    let mut guard = cache_state().lock().ok()?;
    let epoch = guard.epoch;
    let value = guard
        .entries
        .get(&key)
        .filter(|entry| entry.epoch == epoch)
        .map(|entry| entry.value.clone());
    if value.is_some() {
        guard.hits += 1;
    } else {
        guard.misses += 1;
    }
    value
}

pub fn put_cached_tool_result(tool_name: &str, input: &str, value: &str) {
    let key = cache_key(tool_name, input);
    if let Ok(mut guard) = cache_state().lock() {
        let epoch = guard.epoch;
        guard.entries.insert(
            key,
            CacheEntry {
                epoch,
                value: value.to_string(),
            },
        );
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

    #[test]
    fn cache_hits_then_invalidates() {
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
}
