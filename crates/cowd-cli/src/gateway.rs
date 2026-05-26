use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::BuiltRuntime;

/// Thread-safe registry of active session runtimes.
///
/// Each session is identified by a string key and maps to a built runtime
/// (`Arc<BuiltRuntime>`), allowing shared ownership across threads.
///
/// Before calling [`get_or_create`](ActiveSessions::get_or_create), register
/// a session via [`register`](ActiveSessions::register).
pub struct ActiveSessions {
    sessions: Arc<RwLock<HashMap<String, Arc<BuiltRuntime>>>>,
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
    pub fn get(&self, id: &str) -> Option<Arc<BuiltRuntime>> {
        self.sessions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// Return the session for `id`, registering a new entry if none exists.
    ///
    /// # Panics
    ///
    /// Panics if the session is not yet registered. BuiltRuntime requires
    /// complex initialisation (provider client, tool executor, plugins, MCP)
    /// that cannot be performed lazily from an ID alone. Use [`register`](Self::register)
    /// to add sessions before calling this method.
    #[must_use]
    pub fn get_or_create(&self, id: &str) -> Arc<BuiltRuntime> {
        {
            let guard = self
                .sessions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(runtime) = guard.get(id) {
                return runtime.clone();
            }
        }

        panic!(
            "session {id:?} not registered in ActiveSessions; \
             call register() first, then get() or get_or_create()"
        );
    }

    /// Register a built runtime under the given session ID.
    /// Returns the previous entry if one already existed.
    pub fn register(
        &self,
        id: String,
        runtime: BuiltRuntime,
    ) -> Option<Arc<BuiltRuntime>> {
        self.sessions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, Arc::new(runtime))
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
    pub fn remove(&self, id: &str) -> Option<Arc<BuiltRuntime>> {
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
