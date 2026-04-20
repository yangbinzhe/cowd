//! `memory` – unified memory framework for the claw AI assistant.
//!
//! # Architecture
//!
//! The memory system is organised into five layers (L0–L4), backed by a
//! unified `MemoryStore` trait with a SQLite implementation.  A compression
//! pipeline with three stages keeps the context window manageable, while a
//! background extractor automatically captures knowledge from the
//! conversation stream.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use memory::{MemoryOrchestrator, MemoryConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = MemoryConfig::default();
//!     let _orchestrator = MemoryOrchestrator::init(config).await.unwrap();
//! }
//! ```

// --- Public modules ---

pub mod cognitive;
pub mod compression;
pub mod config;
pub mod drift;
pub mod embedding;
pub mod entity;
pub mod error;
pub mod extractor;
pub mod handoff;
pub mod hot_reload;
pub mod layers;
pub mod orchestrator;
pub mod relevance;
pub mod seeds;
pub mod session_manager;
pub mod state_rebuilder;
pub mod store;
pub mod search;
pub mod types;
pub mod aaak_compression;
pub mod context_fence;
pub mod fresh_context;
pub mod temporal_graph;
pub mod write_guard;
pub mod closet;
pub mod miner;

// --- Convenience re-exports ---

pub use cognitive::{CognitiveContextManager, SessionRestoreStats, VectorIndexStats};
pub use config::{MemoryConfig, VectorConfig};
pub use fresh_context::{
    FreshContextManager, FreshEntry, SessionTokenBudget, SessionBudgetStatus,
};
pub use store::session::{SessionRecord, SessionSearchResult, SqliteSessionStore};
pub use embedding::{EmbeddingClient, EmbeddingCapability};
pub use error::MemoryError;
pub use orchestrator::MemoryOrchestrator;
pub use session_manager::{
    UnifiedSessionMeta, UnifiedSessionManager, SessionType,
    SharedSessionManager, create_session_manager,
};
pub use handoff::HandoffManager;
pub use hot_reload::{
    ConfigChangeEvent, ConfigFile, ConfigHotReloader, HotReloadConfig,
    HotReloadHandle, SharedConfigReloader,
};
pub use types::{
    AlertLevel,
    Blocker,
    ContextAction,
    ContextMonitor,
    Decision,
    DecisionEntry,
    DecisionStatus,
    DecisionThread,
    HandoffData,
    MatchedKeyword,
    MemoryCategory,
    MemoryEntry,
    MemoryId,
    MemoryLayer,
    MemoryMeta,
    MemorySource,
    PreparedContext,
    Priority,
    Relation,
    RelationKind,
    SearchMemoriesRequest,
    SearchMemoriesResult,
    SearchMode,
    SearchSnippet,
    Seed,
    SeedId,
    SeedTrigger,
    TaskState,
    TemporalMarker,
    TokenBudget,
    WorkItem,
    WorkItemStatus,
};
pub use search::{BM25Scorer, HybridSearcher, SearchResult as HybridSearchResult};
pub use aaak_compression::{
    AaakCompressor, AaakCompressed, AaakDictionary, Abbreviation,
    EntityType, GsdContext, GsdState, PriorityItem,
};
pub use context_fence::{
    ContextFence, FenceConfig, FenceRegistry, filter_through_fence,
    fence_from_session, build_memory_context_block, MemoryContextBlock,
    LayerBlock, EntryBlock,
};
pub use state_rebuilder::{
    RebuiltSessionState, StateRebuilder, StateItem, StateSource,
    RebuildOptions, GsdRebuiltState, GsdStateRebuilder, GsdRebuildOptions,
};
pub use temporal_graph::{
    TemporalGraph, TemporalSlice, TemporalQuery, TimeRange, GraphStats,
    temporal_relation,
};
pub use write_guard::{
    MemoryWriteGuard, WriteSource, WritePolicy, AuditLog, AuditEntry,
    AuditOperation, IntegrityChecker, Anomaly, AnomalyReport,
};
pub use compression::token_estimation::{
    TokenEstimator, HeuristicEstimator, SimpleTokenEstimator,
    estimate_tokens_text, estimate_tokens_messages,
};
