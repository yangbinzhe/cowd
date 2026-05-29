use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::SystemTime;

/// Logical cache layer corresponding to a memory layer.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub enum CacheLayer {
    L0,
    L1,
    L2,
    L3,
    L4,
}

impl CacheLayer {
    /// All five layers in priority order (L0 = highest).
    pub fn all() -> [CacheLayer; 5] {
        [CacheLayer::L0, CacheLayer::L1, CacheLayer::L2, CacheLayer::L3, CacheLayer::L4]
    }
}

/// Per-layer cached prompt fragment and the memory count it was built from.
struct CachedPrompt {
    /// Formatted text fragment for this layer's entries.
    prompt: Vec<String>,
    /// Number of memory entries in this layer when the fragment was built.
    memory_count: usize,
}

/// Per-layer prompt cache with global invalidation for config/identity changes.
///
/// Each cache layer tracks its own `memory_count`. The cache is only invalidated
/// when a layer's entry count changes — L0 never changes but gets rebuilt when L3
/// adds entries is exactly the problem this solves.
pub struct CachedSystemPrompt {
    inner: RwLock<CacheInner>,
}

struct CacheInner {
    /// Per-layer cached fragments.
    layers: HashMap<CacheLayer, CachedPrompt>,
    /// Config file path (checked periodically for mtime changes).
    config_path: PathBuf,
    /// Identity file path (checked periodically for mtime changes).
    identity_path: PathBuf,
    /// Cached mtime of the config file.
    config_mtime: Option<SystemTime>,
    /// Cached mtime of the identity file.
    identity_mtime: Option<SystemTime>,
    /// Number of turns since the last global rebuild.
    turns_since_rebuild: u32,
    /// How often (in turns) to check file mtimes.
    check_interval: u32,
    /// Max turns before forcing a rebuild (safety dead-man switch).
    max_age: u32,
}

impl CachedSystemPrompt {
    pub fn new(config_path: PathBuf, identity_path: PathBuf) -> Self {
        let check_interval: u32 = std::env::var("COWD_PROMPT_CACHE_CHECK_INTERVAL")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(5);
        let max_age: u32 = std::env::var("COWD_PROMPT_CACHE_MAX_AGE")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(50);

        let mut layers = HashMap::new();
        for &layer in &CacheLayer::all() {
            layers.insert(layer, CachedPrompt {
                prompt: Vec::new(),
                memory_count: 0,
            });
        }

        Self {
            inner: RwLock::new(CacheInner {
                layers,
                config_path,
                identity_path,
                config_mtime: None,
                identity_mtime: None,
                turns_since_rebuild: 0,
                check_interval,
                max_age,
            }),
        }
    }

    /// Check global invalidation conditions (file mtime changes, max age).
    ///
    /// Must be called **once per turn**, before any per-layer [`needs_rebuild`]
    /// checks. Returns `true` when a full flush should be signalled (caller
    /// should rebuild all layers).
    pub fn check_global(&self) -> bool {
        let mut inner = self.inner.write().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in check_global, recovering");
            poisoned.into_inner()
        });
        inner.turns_since_rebuild += 1;

        if inner.turns_since_rebuild % inner.check_interval == 0 {
            let cfg_changed = {
                let path = inner.config_path.clone();
                check_file_changed(&path, &mut inner.config_mtime)
            };
            let id_changed = {
                let path = inner.identity_path.clone();
                check_file_changed(&path, &mut inner.identity_mtime)
            };
            if cfg_changed || id_changed {
                return true;
            }
        }

        if inner.turns_since_rebuild >= inner.max_age {
            return true;
        }

        false
    }

    /// Check whether a specific layer's cached prompt is stale.
    ///
    /// Returns `true` when the layer has never been built, or when its cached
    /// `memory_count` differs from `current_memory_count`.
    pub fn needs_rebuild(&self, layer: CacheLayer, current_memory_count: usize) -> bool {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in needs_rebuild, recovering");
            poisoned.into_inner()
        });
        let cached = inner.layers.get(&layer)
            .expect("CacheLayer::all() guarantees every variant is initialised");
        cached.prompt.is_empty() || cached.memory_count != current_memory_count
    }

    /// Rebuild (or initialise) the cached prompt for a single layer.
    ///
    /// Other layers are **not** affected.
    pub fn rebuild_layer(&self, layer: CacheLayer, prompt: Vec<String>, memory_count: usize) {
        let mut inner = self.inner.write().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in rebuild_layer, recovering");
            poisoned.into_inner()
        });
        if let Some(cached) = inner.layers.get_mut(&layer) {
            cached.prompt = prompt;
            cached.memory_count = memory_count;
        }
    }

    /// Return the cached prompt fragment for a single layer, or an empty vec
    /// if the layer was never built.
    pub fn get_layer(&self, layer: CacheLayer) -> Vec<String> {
        self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in get_layer, recovering");
            poisoned.into_inner()
        })
        .layers
        .get(&layer)
        .map(|c| c.prompt.clone())
        .unwrap_or_default()
    }

    /// Return the cached memory count for a layer.
    pub fn layer_memory_count(&self, layer: CacheLayer) -> usize {
        self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in layer_memory_count, recovering");
            poisoned.into_inner()
        })
        .layers
        .get(&layer)
        .map(|c| c.memory_count)
        .unwrap_or(0)
    }

    /// Compose the full system prompt from the base prompt plus all cached
    /// per-layer fragments.
    ///
    /// Each layer's fragment is prepended in priority order (L0 first).
    /// Layers with no cached content are silently skipped.
    pub fn get_composed(&self, base_prompt: &[String]) -> Vec<String> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in get_composed, recovering");
            poisoned.into_inner()
        });

        let mut result = base_prompt.to_vec();
        for layer in CacheLayer::all().iter().rev() {
            if let Some(cached) = inner.layers.get(layer) {
                if !cached.prompt.is_empty() {
                    for line in cached.prompt.iter().rev() {
                        result.insert(0, line.clone());
                    }
                }
            }
        }
        result
    }
}

fn check_file_changed(path: &std::path::Path, cached_mtime: &mut Option<SystemTime>) -> bool {
    let current = std::fs::metadata(path).ok().and_then(|m| m.modified().ok());
    let changed = current != *cached_mtime;
    *cached_mtime = current;
    changed
}
