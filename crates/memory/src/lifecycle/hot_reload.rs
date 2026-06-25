//! Configuration hot reload system.
//!
//! Monitors configuration files and triggers reloads when changes are detected.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;

/// Configuration file entry with modification tracking.
#[derive(Debug, Clone)]
pub struct ConfigFile {
    pub path: PathBuf,
    pub last_modified: SystemTime,
    pub hash: u64,
}

/// Configuration change event.
#[derive(Debug, Clone)]
pub enum ConfigChangeEvent {
    /// A config file was modified
    Modified(PathBuf),
    /// Multiple files changed (batch update)
    Batch(Vec<PathBuf>),
    /// Config was fully reloaded
    Reloaded,
    /// Error watching a file
    WatchError(PathBuf, String),
}

/// Hot reload configuration.
#[derive(Debug, Clone)]
pub struct HotReloadConfig {
    /// Directories to watch
    pub watch_dirs: Vec<PathBuf>,
    /// Debounce duration to avoid rapid reloads
    pub debounce_ms: u64,
    /// Whether to watch subdirectories
    pub recursive: bool,
}

impl Default for HotReloadConfig {
    fn default() -> Self {
        Self {
            watch_dirs: Vec::new(),
            debounce_ms: 500,
            recursive: true,
        }
    }
}

impl HotReloadConfig {
    /// Add a directory to watch.
    pub fn watch_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.watch_dirs.push(path.into());
        self
    }

    /// Set the debounce duration in milliseconds.
    pub fn with_debounce_ms(mut self, ms: u64) -> Self {
        self.debounce_ms = ms;
        self
    }

    /// Enable recursive directory watching.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }
}

/// Configuration hot reload manager.
///
/// Monitors config files and emits change events when modifications are detected.
#[derive(Clone)]
pub struct ConfigHotReloader {
    config: HotReloadConfig,
    files: Arc<tokio::sync::RwLock<HashMap<PathBuf, ConfigFile>>>,
    event_sender: mpsc::Sender<ConfigChangeEvent>,
}

impl ConfigHotReloader {
    /// Create a new hot reloader with the given configuration.
    pub fn new(config: HotReloadConfig) -> (Self, mpsc::Receiver<ConfigChangeEvent>) {
        let (sender, receiver) = mpsc::channel(100);
        let reloader = Self {
            config,
            files: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            event_sender: sender,
        };
        (reloader, receiver)
    }

    /// Create with default configuration.
    pub fn default_reloader() -> (Self, mpsc::Receiver<ConfigChangeEvent>) {
        Self::new(HotReloadConfig::default())
    }

    /// Add a file to watch.
    pub async fn watch_file(&self, path: impl Into<PathBuf>) -> std::io::Result<()> {
        let path = path.into();
        if !path.exists() {
            return Ok(());
        }

        let metadata = fs::metadata(&path)?;
        let modified = metadata.modified()?;
        let hash = Self::compute_hash(&path)?;

        let file = ConfigFile {
            path: path.clone(),
            last_modified: modified,
            hash,
        };

        self.files.write().await.insert(path, file);
        Ok(())
    }

    /// Add a file to watch (sync version).
    pub fn watch_file_sync(&self, path: impl Into<PathBuf>) -> std::io::Result<()> {
        let path = path.into();
        if !path.exists() {
            return Ok(());
        }

        let metadata = fs::metadata(&path)?;
        let modified = metadata.modified()?;
        let hash = Self::compute_hash(&path)?;

        let file = ConfigFile {
            path: path.clone(),
            last_modified: modified,
            hash,
        };

        // Use async-aware write lock
        let files_clone = self.files.clone();
        tokio::spawn(async move {
            files_clone.write().await.insert(path, file);
        });
        Ok(())
    }

    /// Add a config directory to watch (auto-discovers config files).
    pub fn watch_config_dir(&self, dir: &Path) -> std::io::Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        if self.config.recursive {
            // Recursive: use walkdir
            for entry in walkdir::WalkDir::new(dir)
                .follow_links(true)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
            {
                let path = entry.path();
                if is_config_file(path) {
                    if let Err(e) = self.watch_file_sync(path) {
                        tracing::warn!("Failed to watch config file {}: {}", path.display(), e);
                    }
                }
            }
        } else {
            // Non-recursive: use std::fs::ReadDir
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.filter_map(|e| e.ok()) {
                    if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                        let path = entry.path();
                        if is_config_file(&path) {
                            if let Err(e) = self.watch_file_sync(&path) {
                                tracing::warn!(
                                    "Failed to watch config file {}: {}",
                                    path.display(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Check for changes and emit events.
    ///
    /// Returns the number of changed files detected.
    pub async fn check_changes(&self) -> usize {
        let mut changes = Vec::new();
        let mut files = self.files.write().await;

        for (path, file) in files.iter_mut() {
            if let Ok(metadata) = fs::metadata(path) {
                if let Ok(modified) = metadata.modified() {
                    if modified > file.last_modified {
                        if let Ok(new_hash) = Self::compute_hash(path) {
                            if new_hash != file.hash {
                                changes.push(path.clone());
                                file.last_modified = modified;
                                file.hash = new_hash;
                            }
                        }
                    }
                }
            }
        }

        drop(files); // Release the lock before sending events

        if changes.is_empty() {
            0
        } else if changes.len() == 1 {
            let _ = self
                .event_sender
                .try_send(ConfigChangeEvent::Modified(changes[0].clone()));
            1
        } else {
            let _ = self
                .event_sender
                .try_send(ConfigChangeEvent::Batch(changes.clone()));
            changes.len()
        }
    }

    /// Start the background watch loop.
    ///
    /// This method spawns a background task that periodically checks for changes.
    /// Returns a handle that can be used to stop the watcher.
    pub fn start_background_watch(self) -> Arc<HotReloadHandle> {
        let files = self.files.clone();
        let sender = self.event_sender.clone();
        let debounce = Duration::from_millis(self.config.debounce_ms);
        let dirs = self.config.watch_dirs.clone();
        let recursive = self.config.recursive;

        let handle = Arc::new(HotReloadHandle {
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            files: files.clone(),
        });

        let handle_clone = handle.clone();
        let reloader_clone = self.clone();
        tokio::spawn(async move {
            // Watch directories for new files
            for dir in &dirs {
                if recursive {
                    for entry in walkdir::WalkDir::new(dir)
                        .follow_links(true)
                        .into_iter()
                        .filter_map(|e| e.ok())
                        .filter(|e| e.file_type().is_file())
                    {
                        let path = entry.path();
                        if is_config_file(path) {
                            if let Err(e) = reloader_clone.watch_file(path).await {
                                tracing::warn!(
                                    "Failed to watch config file {}: {}",
                                    path.display(),
                                    e
                                );
                            }
                        }
                    }
                } else if let Ok(entries) = fs::read_dir(dir) {
                    for entry in entries.filter_map(|e| e.ok()) {
                        if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                            let path = entry.path();
                            if is_config_file(&path) {
                                if let Err(e) = reloader_clone.watch_file(&path).await {
                                    tracing::warn!(
                                        "Failed to watch config file {}: {}",
                                        path.display(),
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }

            let mut interval = tokio::time::interval(debounce);

            while handle_clone
                .running
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                interval.tick().await;

                // Check all watched files
                let mut changes = Vec::new();
                let mut files_guard = handle_clone.files.write().await;

                for (path, file) in files_guard.iter_mut() {
                    if let Ok(metadata) = fs::metadata(path) {
                        if let Ok(modified) = metadata.modified() {
                            if modified > file.last_modified {
                                if let Ok(new_hash) = ConfigHotReloader::compute_hash(path) {
                                    if new_hash != file.hash {
                                        changes.push(path.clone());
                                        file.last_modified = modified;
                                        file.hash = new_hash;
                                    }
                                }
                            }
                        }
                    }
                }

                if !changes.is_empty() {
                    if changes.len() == 1 {
                        let _ = sender
                            .send(ConfigChangeEvent::Modified(changes[0].clone()))
                            .await;
                    } else {
                        let _ = sender.send(ConfigChangeEvent::Batch(changes)).await;
                    }
                    let _ = sender.send(ConfigChangeEvent::Reloaded).await;
                }
            }
        });

        handle
    }

    fn compute_hash(path: &Path) -> std::io::Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let content = fs::read(path)?;
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        Ok(hasher.finish())
    }
}

/// Handle to control the background watcher.
#[derive(Debug)]
pub struct HotReloadHandle {
    running: Arc<std::sync::atomic::AtomicBool>,
    files: Arc<tokio::sync::RwLock<HashMap<PathBuf, ConfigFile>>>,
}

impl HotReloadHandle {
    /// Stop the background watcher.
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get the list of currently watched files.
    pub async fn watched_files(&self) -> Vec<PathBuf> {
        self.files.read().await.keys().cloned().collect()
    }
}

/// Check if a path is a configuration file.
fn is_config_file(path: &Path) -> bool {
    let extensions = ["yaml", "yml", "json", "toml"];
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| extensions.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Shared config hot reload state for use across the application.
#[derive(Clone)]
pub struct SharedConfigReloader {
    handle: Arc<HotReloadHandle>,
    reload_tx: mpsc::Sender<()>,
}

impl SharedConfigReloader {
    /// Create a new shared reloader with the given config directories.
    pub fn new(dirs: Vec<PathBuf>) -> (Self, mpsc::Receiver<ConfigChangeEvent>) {
        let config = HotReloadConfig::default()
            .with_debounce_ms(500)
            .with_recursive(true);

        let (reloader, mut event_rx) = ConfigHotReloader::new(config);

        for dir in &dirs {
            let _ = reloader.watch_config_dir(dir);
        }

        let handle = reloader.start_background_watch();
        let (reload_tx, mut reload_rx) = mpsc::channel::<()>(10);

        // Spawn task to forward reload events
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = reload_rx.recv() => {
                        let _ = tx.send(ConfigChangeEvent::Reloaded).await;
                    }
                    Some(event) = event_rx.recv() => {
                        let _ = tx.send(event).await;
                    }
                }
            }
        });

        (Self { handle, reload_tx }, rx)
    }

    /// Trigger a manual reload.
    pub async fn trigger_reload(&self) {
        let _ = self.reload_tx.send(()).await;
    }

    /// Stop the reloader.
    pub fn stop(&self) {
        self.handle.stop();
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_is_config_file() {
        assert!(is_config_file(Path::new("config.yaml")));
        assert!(is_config_file(Path::new("config.yml")));
        assert!(is_config_file(Path::new("config.json")));
        assert!(is_config_file(Path::new("settings.toml")));
        assert!(!is_config_file(Path::new("config.txt")));
        assert!(!is_config_file(Path::new("script.rs")));
    }

    #[tokio::test]
    async fn test_watch_single_file() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("config.yaml");
        fs::write(&config_path, "key: value\n").unwrap();

        let config = HotReloadConfig::default();
        let (reloader, mut events) = ConfigHotReloader::new(config);

        reloader.watch_file(&config_path).await.unwrap();

        // Initial check should find no changes
        assert_eq!(reloader.check_changes().await, 0);

        // Modify the file
        tokio::time::sleep(Duration::from_millis(100)).await;
        fs::write(&config_path, "key: new_value\n").unwrap();

        // Check should detect the change
        assert_eq!(reloader.check_changes().await, 1);

        // Verify event was sent
        match events.try_recv() {
            Ok(ConfigChangeEvent::Modified(path)) => {
                assert_eq!(path, config_path);
            }
            _ => panic!("Expected Modified event"),
        }
    }

    #[tokio::test]
    async fn test_watch_directory() {
        let tmp = TempDir::new().unwrap();
        let config_dir = tmp.path().join(".cowd");
        fs::create_dir(&config_dir).unwrap();

        // Create config files
        let config1 = config_dir.join("config.yaml");
        let config2 = config_dir.join("config.yml");
        fs::write(&config1, "key1: value1\n").unwrap();
        fs::write(&config2, "key2: value2\n").unwrap();

        let config = HotReloadConfig::default();
        let (reloader, _) = ConfigHotReloader::new(config);

        reloader.watch_config_dir(&config_dir).unwrap();

        // Give tokio::spawn time to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        let watched = reloader.files.read().await;
        assert_eq!(watched.len(), 2);
        assert!(watched.contains_key(&config1));
        assert!(watched.contains_key(&config2));
    }

    #[tokio::test]
    async fn test_hot_reload_handle() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("config.yaml");
        fs::write(&config_path, "initial\n").unwrap();

        let config = HotReloadConfig::default();
        let (reloader, _) = ConfigHotReloader::new(config);
        reloader.watch_file(&config_path).await.unwrap();

        let handle = reloader.start_background_watch();

        // Check watched files
        let files = handle.watched_files().await;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], config_path);

        // Stop the watcher
        handle.stop();
        assert!(!handle.running.load(std::sync::atomic::Ordering::Relaxed));
    }
}
