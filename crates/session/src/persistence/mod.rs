mod discovery;
pub use discovery::{
    SessionContextDiscoveryPage, SessionDiscoveryFilter, SessionDiscoveryKind,
    SessionDiscoveryPage, SessionDiscoveryRequest, SessionDiscoveryScope, SessionDiscoverySnapshot,
};
mod backend;
#[doc(hidden)]
pub mod domain;
mod execution_plane;
mod history;
mod repository;
mod tiered;
mod types;

pub use backend::{SessionStoreBackend, SharedSessionStoreBackend};
pub use domain::terminal::{
    public_session_blocks, public_terminal_payload_v1, terminal_public_payload_digest,
};
pub use execution_plane::{
    StorageExecutionLane, StorageExecutionLaneStats, StorageExecutionPlaneConfig,
    StorageExecutionPlaneStats,
};
pub use history::{SessionContextPage, SessionHistoryReader};
pub use repository::UnifiedSessionStore;
pub use tiered::{CompressionAlgo, StorageTier, TieredSessionStore, TieredSessionStoreConfig};
pub use types::{
    build_context_index_cards, context_index_card_digest, context_index_source_digest,
    ActiveSessionProjection, ContextIndexCard, ContextIndexCoverage, OutboxFailureClass,
    OutboxStatus, SessionActivationManifest, SessionBranchRequest, SessionBranchResult,
    SessionEvent, SessionInputAdmission, SessionLifecycleFenceRequest,
    SessionLifecycleTombstoneRequest, SessionListOptions, SessionListPage, SessionMessage,
    SessionMessageMetadata, SessionPresenceProjection, SessionProjectionRecoveryState,
    SessionRecord, SessionRecoveryManifest, SessionRecoverySignal, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SessionTerminalExecutionFence,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SessionUsageBucket,
    SessionUsageSummary, CONTEXT_INDEX_CARD_SCHEMA_VERSION,
    SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION,
};
pub use types::{
    SessionPresenceMutation, SessionPresenceMutationKind, SessionPresenceMutationResult,
    SessionReceiptAuthority, SessionReceiptSource, SessionTerminalAckCommand,
    SessionTerminalAckListQuery, SessionTerminalMessageBinding, SessionTerminalReceiptContext,
    SessionTerminalReceiptQuery,
};
pub use types::{
    SessionTerminalPublicPayload, SessionTerminalPublicPayloadUnavailableReason,
    SESSION_TERMINAL_PUBLIC_PAYLOAD_DIGEST_PREFIX_V1, SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1,
    SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES,
};

pub type Result<T> = crate::error::Result<T>;
