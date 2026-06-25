// P2: SharedMemoryManager — cross-session memory synchronization.
// Allows multiple concurrent sessions within the same profile to share
// extracted memories, context, and knowledge graph updates.

use crate::cognitive::CognitiveContextManager;
use crate::config::MemoryConfig;
use std::sync::{Arc, RwLock};

/// Singleton wrapper that enables cross-session memory sharing.
/// All sessions within the same process share one CognitiveContextManager.
pub struct SharedMemoryManager {
    inner: RwLock<Option<Arc<CognitiveContextManager>>>,
}

impl SharedMemoryManager {
    /// Create a new (uninitialized) shared manager.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }

    /// Initialize the shared memory backend. Safe to call multiple times;
    /// subsequent calls return the existing instance.
    pub async fn init(&self, config: MemoryConfig) -> Result<Arc<CognitiveContextManager>, String> {
        {
            let existing = self.inner.read().unwrap_or_else(|poisoned| {
                tracing::warn!("shared memory RwLock poisoned; recovering");
                poisoned.into_inner()
            });
            if let Some(ref mgr) = *existing {
                return Ok(Arc::clone(mgr));
            }
        }
        let mgr = CognitiveContextManager::new(config)
            .await
            .map_err(|e| format!("shared memory init: {e}"))?;
        let arc = Arc::new(mgr);
        *self.inner.write().unwrap_or_else(|poisoned| {
            tracing::warn!("shared memory RwLock poisoned; recovering");
            poisoned.into_inner()
        }) = Some(Arc::clone(&arc));
        Ok(arc)
    }

    /// Get the current shared manager, if initialized.
    pub fn get(&self) -> Option<Arc<CognitiveContextManager>> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("shared memory RwLock poisoned; recovering");
                poisoned.into_inner()
            })
            .clone()
    }
}

impl Default for SharedMemoryManager {
    fn default() -> Self {
        Self::new()
    }
}

// Global singleton for cross-session sharing within the same process.
// Sessions access this via SharedMemoryManager::global().
static GLOBAL_SHARED: std::sync::LazyLock<SharedMemoryManager> =
    std::sync::LazyLock::new(SharedMemoryManager::default);

impl SharedMemoryManager {
    /// Access the process-wide shared memory manager.
    pub fn global() -> &'static SharedMemoryManager {
        &GLOBAL_SHARED
    }
}
