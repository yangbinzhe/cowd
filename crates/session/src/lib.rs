#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

pub mod domain;
pub mod error;
pub mod lease;
pub mod lifecycle;
pub mod persistence;

pub use domain::{
    SessionBranchActivation, SessionBranchActivationPhase, SessionBranchActivationTransition,
    SessionCloseDisposition, SessionDomainEvent, SessionDomainEventPage, SessionDomainRef,
    SessionDomainScope, SessionLifecycleIntent, SessionLifecyclePhase, SessionLifecyclePlan,
    SessionLifecycleTransition, SESSION_DOMAIN_EVENT_TYPE,
};
pub use error::{Result as SessionResult, SessionError};
pub use lease::{SessionLease, SessionLeaseRegistry};
pub use lifecycle::{
    SessionActor, SessionAttachment, SessionLifecycleEvent, SessionLifecycleSnapshot,
    SessionLifecycleState, SessionPresenceLedger,
};
pub use persistence::{
    build_context_index_cards, context_index_card_digest, context_index_source_digest,
    ActiveSessionProjection, CompressionAlgo, ContextIndexCard, ContextIndexCoverage,
    OutboxFailureClass, OutboxStatus, SessionActivationManifest, SessionBranchRequest,
    SessionBranchResult, SessionEvent, SessionHistoryReader, SessionInputAdmission,
    SessionLifecycleFenceRequest, SessionLifecycleTombstoneRequest, SessionListOptions,
    SessionListPage, SessionMessage, SessionMessageMetadata, SessionMissionOutboxOperation,
    SessionMissionOutboxRecord, SessionMissionOutboxRequest, SessionProjectionRecoveryState,
    SessionRecord, SessionRecoveryManifest, SessionRecoverySignal, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SessionStoreBackend, SessionTerminalExecutionFence,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SharedSessionStoreBackend,
    SqliteSessionStore, StorageExecutionLane, StorageExecutionLaneStats,
    StorageExecutionPlaneConfig, StorageExecutionPlaneStats, StorageTier, TieredSessionStore,
    TieredSessionStoreConfig, UnifiedSessionStore, CONTEXT_INDEX_CARD_SCHEMA_VERSION,
    SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION,
};
