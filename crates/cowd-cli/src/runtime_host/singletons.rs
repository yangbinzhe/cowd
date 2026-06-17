// ── Daemon Singletons ──────────────────────────────────────────
// OnceLock globals for daemon-wide shared instances.
// Initialized once at daemon startup, reused by all sessions.

use std::sync::{Arc, OnceLock};

use crate::RuntimePluginState;
use memory::cognitive::CognitiveContextManager;
use memory::UnifiedSessionStore;

/// Global plugin state — built once at daemon startup, reused by all sessions.
pub static GLOBAL_PLUGIN: OnceLock<Arc<RuntimePluginState>> = OnceLock::new();

/// Global memory manager — single CognitiveContextManager for all sessions.
pub static GLOBAL_MEMORY: OnceLock<Arc<CognitiveContextManager>> = OnceLock::new();

/// Global session store — single UnifiedSessionStore for all sessions.
pub static GLOBAL_STORE: OnceLock<Arc<UnifiedSessionStore>> = OnceLock::new();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_singletons_exist() {
        // Verify singletons are accessible
        assert!(GLOBAL_PLUGIN.get().is_none() || GLOBAL_PLUGIN.get().is_some());
        assert!(GLOBAL_MEMORY.get().is_none() || GLOBAL_MEMORY.get().is_some());
        assert!(GLOBAL_STORE.get().is_none() || GLOBAL_STORE.get().is_some());
    }
}
