mod backend;
mod execution_plane;
mod history;
mod repository;
mod sqlite;
mod tiered;

pub use backend::{SessionStoreBackend, SharedSessionStoreBackend};
pub use execution_plane::{
    StorageExecutionLane, StorageExecutionLaneStats, StorageExecutionPlaneConfig,
    StorageExecutionPlaneStats,
};
pub use history::SessionHistoryReader;
pub use repository::UnifiedSessionStore;
pub use sqlite::{
    OutboxFailureClass, OutboxStatus, SessionBranchRequest, SessionBranchResult, SessionEvent,
    SessionInputAdmission, SessionLifecycleFenceRequest, SessionLifecycleTombstoneRequest,
    SessionListOptions, SessionListPage, SessionMessage, SessionMissionOutboxOperation,
    SessionMissionOutboxRecord, SessionMissionOutboxRequest, SessionRecord,
    SessionRecoveryManifest, SessionRecoverySignal, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SessionTerminalExecutionFence,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SqliteSessionStore,
};
pub use tiered::{CompressionAlgo, StorageTier, TieredSessionStore, TieredSessionStoreConfig};

pub type Result<T> = crate::error::Result<T>;
