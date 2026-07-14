//! Workspace-scoped cache owned by [`crate::ToolHost`].

use std::collections::HashMap;
use std::sync::Mutex;

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
}

#[derive(Default)]
struct ToolCacheState {
    epoch: u64,
    hits: usize,
    misses: usize,
    invalidations: usize,
    entries: HashMap<CacheKey, CacheEntry>,
    scope_epochs: HashMap<String, u64>,
}

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
        let value = state
            .entries
            .get(&key)
            .filter(|entry| entry.epoch == epoch && entry.scope_epoch == scope_epoch)
            .map(|entry| entry.value.clone());
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
            let epoch = state.epoch;
            let scope_epoch = state.scope_epoch(scope);
            state.entries.insert(
                key,
                CacheEntry {
                    epoch,
                    scope_epoch,
                    value: value.to_string(),
                },
            );
        }
    }

    pub fn invalidate_scope(&self, scope: &str) {
        if let Ok(mut state) = self.state.lock() {
            let next_epoch = state.scope_epoch(scope).saturating_add(1);
            state.scope_epochs.insert(scope.to_string(), next_epoch);
            state.invalidations = state.invalidations.saturating_add(1);
        }
    }

    pub fn invalidate_all(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.epoch = state.epoch.saturating_add(1);
            state.invalidations = state.invalidations.saturating_add(1);
            state.entries.clear();
            state.scope_epochs.clear();
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
}
