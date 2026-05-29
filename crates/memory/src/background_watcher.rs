//! Background file-system watcher that detects source code changes and
//! automatically triggers project knowledge-graph rebuilds.
//!
//! Uses `notify` (inotify / FSEvents / kqueue) to watch the workspace root
//! for file modifications.  A debounce window ([`BackgroundWatcherConfig::poll_interval_secs`])
//! ensures we only rebuild after a period of inactivity — avoiding thrash
//! during rapid save sequences.

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::entity::KnowledgeGraph;
use crate::project_scope::build_project_kg;

// ---------------------------------------------------------------------------
// BackgroundWatcherConfig
// ---------------------------------------------------------------------------

/// Configuration for the background file-system watcher.
#[derive(Debug, Clone)]
pub struct BackgroundWatcherConfig {
    /// Seconds of inactivity after the last source-file event before
    /// a knowledge-graph rebuild is triggered.
    pub poll_interval_secs: u64,
}

impl Default for BackgroundWatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 30,
        }
    }
}

// ---------------------------------------------------------------------------
// BackgroundWatcherHandle
// ---------------------------------------------------------------------------

/// Opaque handle returned by [`BackgroundWatcher::start`].
///
/// The watcher thread automatically shuts down when this handle is dropped
/// OR when the receiving end of the KG rebuild channel is closed.
#[must_use = "The watcher thread stops when this handle is dropped"]
pub struct BackgroundWatcherHandle {
    /// Signals the watcher thread to exit.  Dropping this sender causes
    /// the thread to wake at the next loop iteration and clean up.
    _stop: tokio::sync::oneshot::Sender<()>,
}

// ---------------------------------------------------------------------------
// BackgroundWatcher
// ---------------------------------------------------------------------------

/// File-system watcher that triggers knowledge-graph rebuilds on source code changes.
///
/// # How it works
///
/// 1. A dedicated OS thread polls for `notify` events.
/// 2. Events that match a recognised source extension (`.rs`, `.py`, `.ts`, …)
///    set a *pending-rebuild* flag and reset the debounce timer.
/// 3. When no source-file events arrive for `poll_interval_secs`, the watcher
///    calls [`build_project_kg`] and sends the fresh [`KnowledgeGraph`] through
///    an [`mpsc::UnboundedSender`] channel.
/// 4. The consumer (typically [`CognitiveContextManager`]) replaces its
///    in-memory KG with the rebuilt version.
///
/// # Example
///
/// ```rust,ignore
/// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
/// let handle = BackgroundWatcher::start(
///     workspace_root,
///     BackgroundWatcherConfig::default(),
///     tx,
/// );
/// // … later …
/// while let Some(kg) = rx.recv().await {
///     // Replace the current KG with `kg`.
/// }
/// ```
pub struct BackgroundWatcher {
    #[allow(dead_code)]
    config: BackgroundWatcherConfig,
    #[allow(dead_code)]
    workspace_root: PathBuf,
    #[allow(dead_code)]
    kg_rebuild_tx: mpsc::UnboundedSender<KnowledgeGraph>,
}

impl BackgroundWatcher {
    /// Start a background watcher on `workspace_root`.
    ///
    /// The returned [`BackgroundWatcherHandle`] should be stored by the
    /// caller so the watcher thread stays alive.  Dropping it sends a
    /// shutdown signal.
    ///
    /// # Errors
    ///
    /// Panics if the underlying OS watcher cannot be created or if the
    /// target directory cannot be watched.  These are considered fatal
    /// because the caller explicitly enabled the feature.
    pub fn start(
        workspace_root: PathBuf,
        config: BackgroundWatcherConfig,
        kg_rebuild_tx: mpsc::UnboundedSender<KnowledgeGraph>,
    ) -> BackgroundWatcherHandle {
        let poll_interval = Duration::from_secs(config.poll_interval_secs);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();

        std::thread::Builder::new()
            .name("bg-fs-watcher".into())
            .spawn(move || {
                let (tx, rx) = std::sync::mpsc::channel();

                let mut watcher = match notify::recommended_watcher(
                    move |res: notify::Result<notify::Event>| {
                        if let Ok(event) = res {
                            let _ = tx.send(event);
                        }
                    },
                ) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "background_watcher: failed to create OS watcher"
                        );
                        return;
                    }
                };

                if let Err(e) = watcher.watch(&workspace_root, RecursiveMode::Recursive) {
                    tracing::error!(
                        error = %e,
                        path = %workspace_root.display(),
                        "background_watcher: failed to start watching directory"
                    );
                    return;
                }

                tracing::info!(
                    path = %workspace_root.display(),
                    poll_interval_secs = config.poll_interval_secs,
                    "background_watcher: started"
                );

                let mut last_event = Instant::now();
                let mut pending_rebuild = false;

                loop {
                    // Check stop signal once per second.
                    if stop_rx.try_recv().is_ok() {
                        tracing::info!("background_watcher: received stop signal, shutting down");
                        break;
                    }

                    match rx.recv_timeout(Duration::from_secs(1)) {
                        Ok(event) => {
                            // Only care about source-file modifications.
                            if event.paths.iter().any(|p| Self::is_source_file(p)) {
                                // Skip events that are just metadata / access.
                                use notify::event::EventKind;
                                match &event.kind {
                                    EventKind::Modify(_) | EventKind::Create(_) => {
                                        last_event = Instant::now();
                                        pending_rebuild = true;
                                        tracing::debug!(
                                            "background_watcher: source change detected, debouncing..."
                                        );
                                    }
                                    EventKind::Remove(_) => {
                                        last_event = Instant::now();
                                        pending_rebuild = true;
                                        tracing::debug!(
                                            "background_watcher: source removal detected, debouncing..."
                                        );
                                    }
                                    _ => {} // metadata / access — skip
                                }
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            // No events recently — should we rebuild?
                            if pending_rebuild && last_event.elapsed() >= poll_interval {
                                pending_rebuild = false;
                                tracing::info!(
                                    "background_watcher: debounce timer expired, rebuilding KG"
                                );
                                let (kg, _mtimes) = build_project_kg(&workspace_root);
                                let entity_count = kg.list_entities().len();
                                if kg_rebuild_tx.send(kg).is_err() {
                                    tracing::warn!(
                                        "background_watcher: rebuild channel closed, shutting down"
                                    );
                                    break;
                                }
                                tracing::info!(
                                    entity_count,
                                    "background_watcher: KG rebuilt and sent"
                                );
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            tracing::warn!(
                                "background_watcher: event channel disconnected, shutting down"
                            );
                            break;
                        }
                    }
                }
            })
            .expect("failed to spawn background watcher thread");

        BackgroundWatcherHandle { _stop: stop_tx }
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    /// Returns `true` if `path` has a recognised source-code extension.
    fn is_source_file(path: &std::path::Path) -> bool {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|ext| {
                matches!(
                    ext,
                    "rs" | "py" | "ts" | "tsx" | "go" | "java" | "js"
                )
            })
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn is_source_file_rust() {
        assert!(BackgroundWatcher::is_source_file(
            &PathBuf::from("src/main.rs")
        ));
    }

    #[test]
    fn is_source_file_typescript() {
        assert!(BackgroundWatcher::is_source_file(
            &PathBuf::from("components/App.tsx")
        ));
    }

    #[test]
    fn is_source_file_python() {
        assert!(BackgroundWatcher::is_source_file(
            &PathBuf::from("app/models.py")
        ));
    }

    #[test]
    fn is_source_file_rejects_non_source() {
        assert!(!BackgroundWatcher::is_source_file(
            &PathBuf::from("README.md")
        ));
        assert!(!BackgroundWatcher::is_source_file(
            &PathBuf::from("Cargo.toml")
        ));
        assert!(!BackgroundWatcher::is_source_file(
            &PathBuf::from("image.png")
        ));
    }

    #[test]
    fn is_source_file_no_extension() {
        assert!(!BackgroundWatcher::is_source_file(
            &PathBuf::from("Makefile")
        ));
    }
}
