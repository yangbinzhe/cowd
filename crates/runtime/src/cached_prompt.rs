use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;

pub struct CachedSystemPrompt {
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    cached_prompt: Vec<String>,
    config_path: PathBuf,
    identity_path: PathBuf,
    config_mtime: Option<SystemTime>,
    identity_mtime: Option<SystemTime>,
    memory_high_count: usize,
    turns_since_rebuild: u32,
    check_interval: u32,
    max_age: u32,
    memory_delta_threshold: usize,
}

impl CachedSystemPrompt {
    pub fn new(config_path: PathBuf, identity_path: PathBuf) -> Self {
        let check_interval: u32 = std::env::var("COWD_PROMPT_CACHE_CHECK_INTERVAL")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(5);
        let max_age: u32 = std::env::var("COWD_PROMPT_CACHE_MAX_AGE")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(50);
        let memory_delta_threshold: usize = std::env::var("COWD_PROMPT_CACHE_MEMORY_DELTA")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(3);
        Self { inner: Mutex::new(CacheInner {
            cached_prompt: Vec::new(), config_path, identity_path,
            config_mtime: None, identity_mtime: None,
            memory_high_count: 0, turns_since_rebuild: 0,
            check_interval, max_age, memory_delta_threshold,
        })}
    }

    pub fn needs_rebuild(&self, current_memory_high: usize) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in needs_rebuild, recovering");
            poisoned.into_inner()
        });
        inner.turns_since_rebuild += 1;
        if inner.cached_prompt.is_empty() { return true; }
        if inner.turns_since_rebuild % inner.check_interval == 0 {
            let cfg_changed = {
                let path = inner.config_path.clone();
                check_file_changed(&path, &mut inner.config_mtime)
            };
            let id_changed = {
                let path = inner.identity_path.clone();
                check_file_changed(&path, &mut inner.identity_mtime)
            };
            if cfg_changed || id_changed { return true; }
        }
        if current_memory_high.saturating_sub(inner.memory_high_count) >= inner.memory_delta_threshold { return true; }
        if inner.turns_since_rebuild >= inner.max_age { return true; }
        false
    }

    pub fn rebuild(&self, prompt: Vec<String>, memory_high_count: usize) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in rebuild, recovering");
            poisoned.into_inner()
        });
        inner.cached_prompt = prompt;
        inner.memory_high_count = memory_high_count;
        inner.turns_since_rebuild = 0;
    }

    pub fn get(&self) -> Vec<String> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in get, recovering");
            poisoned.into_inner()
        })
        .cached_prompt
        .clone()
    }

    pub fn memory_high_count(&self) -> usize {
        self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("CachedSystemPrompt lock poisoned in memory_high_count, recovering");
            poisoned.into_inner()
        })
        .memory_high_count
    }
}

fn check_file_changed(path: &std::path::Path, cached_mtime: &mut Option<SystemTime>) -> bool {
    let current = std::fs::metadata(path).ok().and_then(|m| m.modified().ok());
    let changed = current != *cached_mtime;
    *cached_mtime = current;
    changed
}
