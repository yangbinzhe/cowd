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

    #[error("context window exceeded: used {used} / {total} tokens")]
    ContextExceeded { used: u64, total: u64 },

    #[error("write denied: source '{write_source}' cannot write to layer '{layer}'")]
    WriteDenied {
        layer: String,
        write_source: String,
    },

    #[error("unexpected error: {0}")]
    Other(String),
}
