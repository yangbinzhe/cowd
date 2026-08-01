use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

use crate::runtime_entry::GatewayRuntimeEntry;

/// Callback trait for session lifecycle events.
///
/// Implementors receive notifications when sessions are registered or
/// removed from the `HotSessionPool` registry.
pub trait SessionLifecycle: Send + Sync {
    fn register(&self, id: &str);
    fn unregister(&self, id: &str);
}

#[allow(clippy::type_complexity)]
type SessionEntry = Arc<Mutex<GatewayRuntimeEntry>>;

/// Thread-safe registry of active session runtimes.
///
/// Each session is identified by a string key and maps to a built runtime
/// wrapped in `Arc<Mutex<GatewayRuntimeEntry>>`, allowing exclusive mutable access
/// across async tasks without TOCTOU races.
pub struct HotSessionPool {
    sessions: Arc<RwLock<HashMap<String, SessionEntry>>>,
    max_sessions: Option<usize>,
    lifecycle: Option<Arc<dyn SessionLifecycle>>,
}

impl HotSessionPool {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_sessions: None,
            lifecycle: None,
        }
    }

    /// Create a new registry with a custom session limit.
    #[must_use]
    pub fn with_max_sessions(max: usize) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_sessions: Some(max.max(1)),
            lifecycle: None,
        }
    }

    /// Attach a [`SessionLifecycle`] observer.
    #[must_use]
    pub fn with_lifecycle(mut self, lifecycle: Arc<dyn SessionLifecycle>) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }

    /// Look up a session by its ID. Returns `None` if the session is not registered.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<SessionEntry> {
        self.sessions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// Return the maximum number of concurrent sessions.
    #[must_use]
    pub fn max_sessions(&self) -> Option<usize> {
        self.max_sessions
    }

    /// Register a built runtime under the given session ID.
    /// Returns an error if the session limit has been reached and the ID is new.
    /// Returns `Ok(previous_entry)` if a session with this ID already existed (replaced).
    pub fn register(
        &self,
        id: String,
        runtime: GatewayRuntimeEntry,
    ) -> Result<Option<SessionEntry>, String> {
        let mut map = self
            .sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .max_sessions
            .is_some_and(|maximum| map.len() >= maximum)
            && !map.contains_key(&id)
        {
            return Err(format!(
                "max sessions limit ({}) reached",
                self.max_sessions.unwrap_or_default()
            ));
        }
        if let Some(ref lifecycle) = self.lifecycle {
            lifecycle.register(&id);
        }
        Ok(map.insert(id, Arc::new(Mutex::new(runtime))))
    }

    /// Return all registered session IDs, sorted.
    #[must_use]
    pub fn list(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .sessions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
        ids.sort();
        ids
    }

    /// Remove and drop a session by ID. Returns the removed runtime, if any.
    pub fn remove(&self, id: &str) -> Option<SessionEntry> {
        if let Some(ref lifecycle) = self.lifecycle {
            lifecycle.unregister(id);
        }
        self.sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id)
    }
}

impl Default for HotSessionPool {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime() -> crate::runtime_entry::GatewayRuntimeEntry {
        crate::runtime_entry::GatewayRuntimeEntry::test_runtime_entry()
    }

    #[test]
    fn new_creates_empty_registry() {
        let sessions = HotSessionPool::new();
        assert!(sessions.list().is_empty());
    }

    #[test]
    fn register_and_get() {
        let sessions = HotSessionPool::new();
        let rt = test_runtime();
        sessions.register("sess-1".into(), rt).unwrap();
        let entry = sessions.get("sess-1");
        assert!(entry.is_some(), "should find registered session");
    }

    #[test]
    fn list_returns_sorted_ids() {
        let sessions = HotSessionPool::new();
        sessions.register("b".into(), test_runtime()).unwrap();
        sessions.register("a".into(), test_runtime()).unwrap();
        sessions.register("c".into(), test_runtime()).unwrap();
        let ids = sessions.list();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn remove_drops_session() {
        let sessions = HotSessionPool::new();
        sessions.register("sess-1".into(), test_runtime()).unwrap();
        let removed = sessions.remove("sess-1");
        assert!(removed.is_some(), "remove should return the entry");
        assert!(
            sessions.get("sess-1").is_none(),
            "should be gone after remove"
        );
    }

    #[test]
    fn remove_nonexistent_returns_none() {
        let sessions = HotSessionPool::new();
        assert!(sessions.remove("no-such-session").is_none());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let sessions = HotSessionPool::new();
        assert!(sessions.get("no-such-session").is_none());
    }

    #[test]
    fn register_overwrite() {
        let sessions = HotSessionPool::new();
        sessions.register("sess-1".into(), test_runtime()).unwrap();
        let prev = sessions.register("sess-1".into(), test_runtime()).unwrap();
        assert!(prev.is_some(), "overwrite should return the previous entry");
    }
}
