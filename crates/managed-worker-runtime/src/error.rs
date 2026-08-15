use std::{path::PathBuf, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum ManagedWorkerError {
    #[error("invalid managed-worker specification: {0}")]
    InvalidSpec(String),
    #[error("managed-worker I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("credential has already been consumed")]
    CredentialConsumed,
    #[error("credential permissions are not owner-only: {mode:o}")]
    CredentialPermissions { mode: u32 },
    #[error("credential owner changed from uid {expected} to {actual}")]
    CredentialOwnerChanged { expected: u32, actual: u32 },
    #[error("worker generation is stale: expected {expected}, observed {observed}")]
    StaleGeneration { expected: String, observed: String },
    #[error("worker process exited before becoming ready: {0}")]
    ExitedBeforeReady(String),
    #[error("managed-worker operation exceeded its {0:?} deadline")]
    DeadlineExceeded(Duration),
    #[error("managed-worker operation was cancelled")]
    Cancelled,
    #[error("managed-worker process signal failed: {0}")]
    Signal(String),
    #[error("managed-worker HTTP/2 transport failed: {0}")]
    H2(String),
}

impl ManagedWorkerError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

pub type ManagedWorkerResult<T> = Result<T, ManagedWorkerError>;
