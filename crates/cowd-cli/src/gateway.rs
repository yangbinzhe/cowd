use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

use crate::BuiltRuntime;

#[allow(clippy::type_complexity)]
type SessionEntry = Arc<Mutex<BuiltRuntime>>;

/// Thread-safe registry of active session runtimes.
///
/// Each session is identified by a string key and maps to a built runtime
/// wrapped in `Arc<Mutex<BuiltRuntime>>`, allowing exclusive mutable access
/// across async tasks without TOCTOU races.
pub struct ActiveSessions {
    sessions: Arc<RwLock<HashMap<String, SessionEntry>>>,
}

impl ActiveSessions {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
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

    /// Register a built runtime under the given session ID.
    /// Returns the previous entry if one already existed.
    pub fn register(
        &self,
        id: String,
        runtime: BuiltRuntime,
    ) -> Option<SessionEntry> {
        self.sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, Arc::new(Mutex::new(runtime)))
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
