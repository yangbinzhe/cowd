use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

use crate::BuiltRuntime;

/// Callback trait for session lifecycle events.
///
/// Implementors receive notifications when sessions are registered or
/// removed from the `ActiveSessions` registry.
pub trait SessionLifecycle: Send + Sync {
    fn register(&self, id: &str);
    fn unregister(&self, id: &str);
}

/// Default maximum number of concurrent sessions.
const DEFAULT_MAX_SESSIONS: usize = 100;

#[allow(clippy::type_complexity)]
type SessionEntry = Arc<Mutex<BuiltRuntime>>;

/// Thread-safe registry of active session runtimes.
///
/// Each session is identified by a string key and maps to a built runtime
/// wrapped in `Arc<Mutex<BuiltRuntime>>`, allowing exclusive mutable access
/// across async tasks without TOCTOU races.
pub struct ActiveSessions {
    sessions: Arc<RwLock<HashMap<String, SessionEntry>>>,
    max_sessions: usize,
    lifecycle: Option<Arc<dyn SessionLifecycle>>,
}

impl ActiveSessions {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_sessions: DEFAULT_MAX_SESSIONS,
            lifecycle: None,
        }
    }

    /// Create a new registry with a custom session limit.
    #[must_use]
    pub fn with_max_sessions(max: usize) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_sessions: max,
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
    pub fn max_sessions(&self) -> usize {
        self.max_sessions
    }

    /// Register a built runtime under the given session ID.
    /// Returns an error if the session limit has been reached and the ID is new.
    /// Returns `Ok(previous_entry)` if a session with this ID already existed (replaced).
    pub fn register(
        &self,
        id: String,
        runtime: BuiltRuntime,
    ) -> Result<Option<SessionEntry>, String> {
        let mut map = self
            .sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if map.len() >= self.max_sessions && !map.contains_key(&id) {
            return Err(format!(
                "max sessions limit ({}) reached",
                self.max_sessions
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

impl Default for ActiveSessions {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_runtime() -> crate::BuiltRuntime {
        let session = runtime::Session::new();
        crate::build_runtime(
            session,
            "test-session",
            "test-model".to_string(),
            vec![],
            false,
            false,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
        .expect("dummy runtime should build")
    }

    #[test]
    fn new_creates_empty_registry() {
        let sessions = ActiveSessions::new();
        assert!(sessions.list().is_empty());
    }

    #[test]
    fn register_and_get() {
        let sessions = ActiveSessions::new();
        let rt = dummy_runtime();
        sessions.register("sess-1".into(), rt).unwrap();
        let entry = sessions.get("sess-1");
        assert!(entry.is_some(), "should find registered session");
    }

    #[test]
    fn list_returns_sorted_ids() {
        let sessions = ActiveSessions::new();
        sessions.register("b".into(), dummy_runtime()).unwrap();
        sessions.register("a".into(), dummy_runtime()).unwrap();
        sessions.register("c".into(), dummy_runtime()).unwrap();
        let ids = sessions.list();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn remove_drops_session() {
        let sessions = ActiveSessions::new();
        sessions.register("sess-1".into(), dummy_runtime()).unwrap();
        let removed = sessions.remove("sess-1");
        assert!(removed.is_some(), "remove should return the entry");
        assert!(
            sessions.get("sess-1").is_none(),
            "should be gone after remove"
        );
    }

    #[test]
    fn remove_nonexistent_returns_none() {
        let sessions = ActiveSessions::new();
        assert!(sessions.remove("no-such-session").is_none());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let sessions = ActiveSessions::new();
        assert!(sessions.get("no-such-session").is_none());
    }

    #[test]
    fn register_overwrite() {
        let sessions = ActiveSessions::new();
        sessions.register("sess-1".into(), dummy_runtime()).unwrap();
        let prev = sessions.register("sess-1".into(), dummy_runtime()).unwrap();
        assert!(prev.is_some(), "overwrite should return the previous entry");
    }
}
