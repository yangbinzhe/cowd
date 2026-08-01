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
pub use history::{SessionContextPage, SessionHistoryReader};
pub use repository::UnifiedSessionStore;
pub use sqlite::{
    build_context_index_cards, context_index_card_digest, context_index_source_digest,
    ActiveSessionProjection, ContextIndexCard, ContextIndexCoverage, OutboxFailureClass,
    OutboxStatus, SessionActivationManifest, SessionBranchRequest, SessionBranchResult,
    SessionEvent, SessionInputAdmission, SessionLifecycleFenceRequest,
    SessionLifecycleTombstoneRequest, SessionListOptions, SessionListPage, SessionMessage,
    SessionMessageMetadata, SessionMissionOutboxOperation, SessionMissionOutboxRecord,
    SessionMissionOutboxRequest, SessionProjectionRecoveryState, SessionRecord,
    SessionRecoveryManifest, SessionRecoverySignal, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SessionTerminalExecutionFence,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SessionUsageBucket,
    SessionUsageSummary, SqliteSessionStore, CONTEXT_INDEX_CARD_SCHEMA_VERSION,
    SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION,
};
pub use tiered::{CompressionAlgo, StorageTier, TieredSessionStore, TieredSessionStoreConfig};

pub type Result<T> = crate::error::Result<T>;
