use thiserror::Error;

/// Errors produced by durable Session domain operations.
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session storage error: {0}")]
    Store(String),

    #[error("invalid session argument: {0}")]
    InvalidArgument(String),

    #[error("session not found: {0}")]
    NotFound(String),

    #[error("session operation failed: {0}")]
    Other(String),

    #[error("session serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

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

    #[error("session storage drain timed out with {active} active and {queued} queued operations")]
    StorageDrainTimeout { active: usize, queued: usize },

    #[error("stale session execution fence: {0}")]
    StaleExecutionFence(String),

    #[error("session idempotency conflict in {namespace} for key `{key}`")]
    IdempotencyConflict {
        namespace: &'static str,
        key: String,
    },
}

pub type Result<T> = std::result::Result<T, SessionError>;
