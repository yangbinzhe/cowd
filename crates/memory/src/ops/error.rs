//! Error types for the `cc-memory` crate.

use thiserror::Error;

/// The master error type for all memory-system operations.
#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("storage error: {0}")]
    Store(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("serialisation error: {0}")]
    Serialisation(#[from] serde_json::Error),

    #[error("entry not found: {0}")]
    NotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("capability `{capability}` is unavailable: {details}")]
    CapabilityUnavailable { capability: String, details: String },

    #[error("context window exceeded: used {used} / {total} tokens")]
    ContextExceeded { used: u64, total: u64 },

    #[error("write denied: source '{write_source}' cannot write to layer '{layer}'")]
    WriteDenied { layer: String, write_source: String },

    #[error("disk full: {details}. Try freeing up disk space.")]
    DiskFull { details: String },

    #[error("unexpected error: {0}")]
    Other(String),

    #[error("session storage queue is full (workers={workers}, queue_capacity={queue_capacity})")]
    StorageQueueFull {
        workers: usize,
        queue_capacity: usize,
    },

    #[error("session storage execution plane is shutting down")]
    StoragePlaneShutdown,

    #[error("session storage worker panicked; transaction outcome may be unknown")]
    StorageWorkerPanic,

    #[error("session storage worker join failed: {0}")]
    StorageWorkerJoin(String),
}

/// Detect whether a `rusqlite::Error` indicates the disk is full.
///
/// Checks both the error message (for generic `Unknown`-code failures) and the
/// structured SQLite error code for `SQLITE_FULL` / `SQLITE_CANTOPEN`.
pub fn is_disk_full_error(err: &rusqlite::Error) -> bool {
    // Some disk-full situations surface as `SqliteFailure` with an
    // `Unknown` error code — the only reliable signal is the message text.
    matches!(err, rusqlite::Error::SqliteFailure(..)
        if err.to_string().contains("disk full")
            || err.to_string().contains("database or disk is full"))
    // Other times SQLite returns the proper extended error code.
    || matches!(err.sqlite_error_code(), Some(rusqlite::ErrorCode::DiskFull | rusqlite::ErrorCode::CannotOpen))
}
