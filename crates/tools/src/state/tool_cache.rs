//! Workspace-scoped cache owned by [`crate::ToolHost`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub invalidations: usize,
    pub entries: usize,
    pub resident_bytes: usize,
    pub evictions: usize,
    pub expired: usize,
    pub oversized_rejections: usize,
    pub epoch: u64,
    #[serde(rename = "scopeEpochs")]
    pub scope_epochs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    workspace_id: String,
    scope: String,
    tool_name: String,
    canonical_input: String,
    schema_revision: u64,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    epoch: u64,
    scope_epoch: u64,
    value: String,
    resident_bytes: usize,
    last_access: u64,
    expires_at: Instant,
}

struct ToolCacheState {
    epoch: u64,
    hits: usize,
    misses: usize,
    invalidations: usize,
    resident_bytes: usize,
    evictions: usize,
    expired: usize,
    oversized_rejections: usize,
    access_clock: u64,
    entries: HashMap<CacheKey, CacheEntry>,
    scope_epochs: HashMap<String, u64>,
}

impl Default for ToolCacheState {
    fn default() -> Self {
        Self {
            epoch: 0,
            hits: 0,
            misses: 0,
            invalidations: 0,
            resident_bytes: 0,
            evictions: 0,
            expired: 0,
            oversized_rejections: 0,
            access_clock: 0,
            entries: HashMap::new(),
            scope_epochs: HashMap::new(),
        }
    }
}

const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_ENTRY_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_TTL: Duration = Duration::from_secs(300);

/// One cache instance belongs to one `ToolHost`; it never reads process state.
#[derive(Default)]
pub struct ToolCache {
    state: Mutex<ToolCacheState>,
}

impl std::fmt::Debug for ToolCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolCache")
            .field("stats", &self.stats())
            .finish()
    }
}

impl ToolCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(
        &self,
        workspace_id: &str,
        scope: &str,
        tool_name: &str,
        input: &str,
        schema_revision: u64,
    ) -> Option<String> {
        let key = cache_key(workspace_id, scope, tool_name, input, schema_revision);
        let mut state = self.state.lock().ok()?;
        let epoch = state.epoch;
        let scope_epoch = state.scope_epoch(scope);
        let now = Instant::now();
        let stale = state.entries.get(&key).is_some_and(|entry| {
            entry.expires_at <= now || entry.epoch != epoch || entry.scope_epoch != scope_epoch
        });
        if stale {
            if let Some(entry) = state.entries.remove(&key) {
                state.resident_bytes = state.resident_bytes.saturating_sub(entry.resident_bytes);
                if entry.expires_at <= now {
                    state.expired = state.expired.saturating_add(1);
                }
            }
        }
        let valid = state
            .entries
            .get(&key)
            .is_some_and(|entry| entry.epoch == epoch && entry.scope_epoch == scope_epoch);
        let value = if valid {
            state.access_clock = state.access_clock.saturating_add(1);
            let access = state.access_clock;
            state.entries.get_mut(&key).map(|entry| {
                entry.last_access = access;
                entry.value.clone()
            })
        } else {
            None
        };
        if value.is_some() {
            state.hits = state.hits.saturating_add(1);
        } else {
            state.misses = state.misses.saturating_add(1);
        }
        value
    }

    pub fn put(
        &self,
        workspace_id: &str,
        scope: &str,
        tool_name: &str,
        input: &str,
        schema_revision: u64,
        value: &str,
    ) {
        let key = cache_key(workspace_id, scope, tool_name, input, schema_revision);
        if let Ok(mut state) = self.state.lock() {
            let resident_bytes = cache_key_bytes(&key).saturating_add(value.len());
            if resident_bytes > DEFAULT_MAX_ENTRY_BYTES {
                state.oversized_rejections = state.oversized_rejections.saturating_add(1);
                return;
            }
            let epoch = state.epoch;
            let scope_epoch = state.scope_epoch(scope);
            state.access_clock = state.access_clock.saturating_add(1);
            let last_access = state.access_clock;
            if let Some(previous) = state.entries.remove(&key) {
                state.resident_bytes = state.resident_bytes.saturating_sub(previous.resident_bytes);
            }
            state.resident_bytes = state.resident_bytes.saturating_add(resident_bytes);
            state.entries.insert(
                key,
                CacheEntry {
                    epoch,
                    scope_epoch,
                    value: value.to_string(),
                    resident_bytes,
                    last_access,
                    expires_at: Instant::now() + DEFAULT_TTL,
                },
            );
            while state.resident_bytes > DEFAULT_MAX_BYTES {
                let Some(oldest) = state
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_access)
                    .map(|(key, _)| key.clone())
                else {
                    break;
                };
                if let Some(entry) = state.entries.remove(&oldest) {
                    state.resident_bytes =
                        state.resident_bytes.saturating_sub(entry.resident_bytes);
                    state.evictions = state.evictions.saturating_add(1);
                }
            }
        }
    }

    pub fn invalidate_scope(&self, scope: &str) {
        if let Ok(mut state) = self.state.lock() {
            let next_epoch = state.scope_epoch(scope).saturating_add(1);
            state.scope_epochs.insert(scope.to_string(), next_epoch);
            let removed = state
                .entries
                .keys()
                .filter(|key| key.scope == scope)
                .cloned()
                .collect::<Vec<_>>();
            for key in removed {
                if let Some(entry) = state.entries.remove(&key) {
                    state.resident_bytes =
                        state.resident_bytes.saturating_sub(entry.resident_bytes);
                }
            }
            state.invalidations = state.invalidations.saturating_add(1);
        }
    }

    pub fn invalidate_all(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.epoch = state.epoch.saturating_add(1);
            state.invalidations = state.invalidations.saturating_add(1);
            state.entries.clear();
            state.scope_epochs.clear();
            state.resident_bytes = 0;
        }
    }

    #[must_use]
    pub fn stats(&self) -> ToolCacheStats {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ToolCacheStats {
            hits: state.hits,
            misses: state.misses,
            invalidations: state.invalidations,
            entries: state.entries.len(),
            resident_bytes: state.resident_bytes,
            evictions: state.evictions,
            expired: state.expired,
            oversized_rejections: state.oversized_rejections,
            epoch: state.epoch,
            scope_epochs: state.scope_epochs.len(),
        }
    }

    #[cfg(test)]
    pub fn reset(&self) {
        if let Ok(mut state) = self.state.lock() {
            *state = ToolCacheState::default();
        }
    }
}

fn cache_key_bytes(key: &CacheKey) -> usize {
    std::mem::size_of::<CacheKey>()
        .saturating_add(key.workspace_id.len())
        .saturating_add(key.scope.len())
        .saturating_add(key.tool_name.len())
        .saturating_add(key.canonical_input.len())
}

impl ToolCacheState {
    fn scope_epoch(&self, scope: &str) -> u64 {
        self.scope_epochs.get(scope).copied().unwrap_or_default()
    }
}

fn cache_key(
    workspace_id: &str,
    scope: &str,
    tool_name: &str,
    input: &str,
    schema_revision: u64,
) -> CacheKey {
    CacheKey {
        workspace_id: workspace_id.to_string(),
        scope: scope.to_string(),
        tool_name: tool_name.to_string(),
        canonical_input: canonical_json(input),
        schema_revision,
    }
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
    fn instances_and_workspaces_are_isolated() {
        let first = ToolCache::new();
        let second = ToolCache::new();
        first.put(
            "workspace-a",
            "file:a",
            "read_file",
            r#"{"path":"a"}"#,
            1,
            "a",
        );

        assert_eq!(
            first
                .get("workspace-a", "file:a", "read_file", r#"{"path":"a"}"#, 1)
                .as_deref(),
            Some("a")
        );
        assert!(first
            .get("workspace-b", "file:a", "read_file", r#"{"path":"a"}"#, 1)
            .is_none());
        assert!(second
            .get("workspace-a", "file:a", "read_file", r#"{"path":"a"}"#, 1)
            .is_none());
    }

    #[test]
    fn scope_and_schema_revision_invalidate_reads() {
        let cache = ToolCache::new();
        cache.put("workspace", "file:a", "read_file", "{}", 7, "old");
        cache.invalidate_scope("file:a");
        assert!(cache
            .get("workspace", "file:a", "read_file", "{}", 7)
            .is_none());

        cache.put("workspace", "file:a", "read_file", "{}", 7, "old");
        assert!(cache
            .get("workspace", "file:a", "read_file", "{}", 8)
            .is_none());
    }

    #[test]
    fn oversized_results_are_not_retained() {
        let cache = ToolCache::new();
        let oversized = "x".repeat(DEFAULT_MAX_ENTRY_BYTES + 1);
        cache.put("workspace", "scope", "read_file", "{}", 1, &oversized);
        assert!(cache
            .get("workspace", "scope", "read_file", "{}", 1)
            .is_none());
        assert_eq!(cache.stats().oversized_rejections, 1);
        assert_eq!(cache.stats().resident_bytes, 0);
    }
}
