//! Unified error types for the cowd runtime.
//!
//! Provides `CowdError`, `RecoveryStrategy`, and the `ErrorChain` concept
//! so that every layer of the stack can surface structured, recoverable errors.

use std::fmt;

/// Top-level error type for all cowd runtime operations.
#[derive(Debug)]
pub enum CowdError {
    /// Storage-layer failures (I/O, serialisation, corruption).
    Storage(StorageError),
    /// Configuration loading / validation failures.
    Config(String),
    /// Provider / API communication failures.
    Provider(String),
    /// Agent lifecycle failures (subprocess exit, heartbeat timeout, trust failure).
    Agent(String),
    /// Plain I/O error (wraps `std::io::Error`).
    Io(std::io::Error),
    /// Catch-all for any other error.
    Other(String),
}

/// Storage-specific errors.
#[derive(Debug)]
pub enum StorageError {
    /// File not found at the expected path.
    NotFound(String),
    /// I/O error during read / write.
    Io(std::io::Error),
    /// Data corruption detected (checksum mismatch, invalid format).
    Corruption(String),
    /// Serialisation / deserialisation failure.
    Serde(String),
    /// Write-ahead log recovery required.
    WalRecoveryNeeded(String),
}

impl fmt::Display for CowdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "storage error: {e}"),
            Self::Config(msg) => write!(f, "config error: {msg}"),
            Self::Provider(msg) => write!(f, "provider error: {msg}"),
            Self::Agent(msg) => write!(f, "agent error: {msg}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(p) => write!(f, "not found: {p}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::Corruption(d) => write!(f, "corruption: {d}"),
            Self::Serde(d) => write!(f, "serialization: {d}"),
            Self::WalRecoveryNeeded(d) => write!(f, "WAL recovery needed: {d}"),
        }
    }
}

impl std::error::Error for CowdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for CowdError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<StorageError> for CowdError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e.to_string())
    }
}

impl From<serde_yaml::Error> for StorageError {
    fn from(e: serde_yaml::Error) -> Self {
        Self::Serde(e.to_string())
    }
}

/// How the system should react to a given error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// Retry up to `max` times with `delay_ms` between attempts.
    Retry { max: u32, delay_ms: u64 },
    /// Fall back to a degraded path with a human-readable reason.
    Fallback { reason: String },
    /// Abort the current operation immediately.
    Abort,
    /// Ignore the error and continue (best-effort).
    Ignore,
}

impl CowdError {
    /// Suggest a recovery strategy for this error variant.
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::Storage(StorageError::WalRecoveryNeeded(_)) => {
                RecoveryStrategy::Fallback {
                    reason: "WAL recovery needed — falling back to JSONL storage".into(),
                }
            }
            Self::Storage(StorageError::NotFound(_)) => RecoveryStrategy::Abort,
            Self::Storage(StorageError::Corruption(_)) => RecoveryStrategy::Abort,
            Self::Storage(StorageError::Serde(_)) => RecoveryStrategy::Abort,
            Self::Storage(StorageError::Io(_)) => RecoveryStrategy::Retry {
                max: 3,
                delay_ms: 500,
            },
            Self::Provider(_) => RecoveryStrategy::Retry {
                max: 2,
                delay_ms: 1_000,
            },
            Self::Agent(_) => RecoveryStrategy::Abort,
            Self::Config(_) => RecoveryStrategy::Abort,
            Self::Io(_) => RecoveryStrategy::Retry {
                max: 1,
                delay_ms: 100,
            },
            Self::Other(_) => RecoveryStrategy::Abort,
        }
    }
}

impl CowdError {
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }
    pub fn provider(msg: impl Into<String>) -> Self {
        Self::Provider(msg.into())
    }
    pub fn agent(msg: impl Into<String>) -> Self {
        Self::Agent(msg.into())
    }
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_chain_display() {
        let err = CowdError::config("missing model field");
        assert_eq!(format!("{err}"), "config error: missing model field");
    }

    #[test]
    fn error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let cowd_err: CowdError = io_err.into();
        assert!(matches!(cowd_err, CowdError::Io(_)));
    }

    #[test]
    fn error_from_storage() {
        let storage_err = StorageError::NotFound("/tmp/session.jsonl".into());
        let cowd_err: CowdError = storage_err.into();
        assert!(matches!(cowd_err, CowdError::Storage(_)));
    }

    #[test]
    fn recovery_strategy_wal() {
        let err = CowdError::Storage(StorageError::WalRecoveryNeeded("lbug".into()));
        assert_eq!(
            err.recovery_strategy(),
            RecoveryStrategy::Fallback {
                reason: "WAL recovery needed — falling back to JSONL storage".into()
            }
        );
    }

    #[test]
    fn recovery_strategy_provider() {
        let err = CowdError::provider("rate limited");
        assert_eq!(
            err.recovery_strategy(),
            RecoveryStrategy::Retry {
                max: 2,
                delay_ms: 1000
            }
        );
    }

    #[test]
    fn recovery_strategy_agent_abort() {
        let err = CowdError::agent("worker timeout");
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Abort);
    }

    #[test]
    fn storage_error_serde_from_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("{{").unwrap_err();
        let storage_err: StorageError = json_err.into();
        assert!(matches!(storage_err, StorageError::Serde(_)));
    }

    #[test]
    fn error_source_chain() {
        use std::error::Error;
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let cowd_err = CowdError::Io(io_err);
        assert!(cowd_err.source().is_some());
    }
}
