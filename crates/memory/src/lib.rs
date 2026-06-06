#![warn(deprecated)]
//! `memory` – unified memory framework for the cowd AI assistant.
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
//! use cowd_memory::{MemoryOrchestrator, MemoryConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = MemoryConfig::default();
//!     let _orchestrator = MemoryOrchestrator::init(config).await.unwrap();
//! }
//! ```

#![warn(deprecated)]

// --- Public modules ---

pub mod agent_directory;
pub mod agent_reputation;
pub mod cognitive;
pub mod compression;
pub mod config;
pub mod drift;
pub mod embedding;
pub mod entity;
pub mod entity_registry;
pub mod error;
pub mod extractor;
pub mod handoff;
pub mod hot_reload;
pub mod kernel;
pub mod layers;
pub mod maintenance;
pub mod memory_sync;
pub mod memory_pulse;
pub mod orchestrator;
pub mod resolution;
pub mod seeds;
pub mod session_store;
pub mod shared;
pub mod splitter;
pub mod state_rebuilder;
pub mod store;
pub mod runtime_event;
pub mod search;
pub mod types;
pub mod aaak_compression;
pub mod coherence;
pub mod context_fence;
pub mod context_rot;
pub mod context_sync;
pub mod fact_checker;
pub mod fresh_context;
pub mod temporal_graph;
pub mod write_guard;
pub mod closet;
pub mod background_watcher;
pub mod code_indexer;
pub mod session_resume;
pub mod project_scope;
pub mod impact_analyzer;
pub mod performance_monitor;
pub mod tool_sandbox;
pub mod tiered_store;
pub mod transaction;

// --- Convenience re-exports ---

pub use layers::shared::{L4Event, L4EventBus, L4Operation};
pub use maintenance::{
    MaintenanceCandidate, MaintenanceCandidateFilter, MaintenanceCandidateKind,
    MaintenanceCandidateStatus, MaintenanceQueue, MaintenanceScanConfig,
    scan_maintenance_candidates,
};
pub use memory_pulse::{
    MemoryPulseBatch, MemoryPulseConfig, MemoryPulseConsumer, MemoryPulseReport,
    MemoryPulseTransition,
};
pub use cognitive::{CognitiveContextManager, SessionRestoreStats, VectorIndexStats};
pub use kernel::{
    MemoryAtomView, MemoryDegradation, MemoryHealth, MemoryInformationState, MemoryKernel,
    MemoryKernelError, MemoryKernelResult, MemoryLayerView, MemoryLifecycleEvent, MemoryLink,
    MemoryLinkKind, MemoryPacketItem, MemoryPacketRole, MemoryPath, MemoryPrimitive, MemoryState,
    MemoryContextPacket, MemoryTurnContext, OmittedMemory,
};
pub use config::{MemoryConfig, TuningConfig, VectorConfig};
pub use fresh_context::{
    FreshContextManager, FreshEntry, SessionTokenBudget, SessionBudgetStatus,
};
pub use runtime_event::{
    RuntimeEvent, RuntimeEventPage, RuntimeEventScope, RuntimeRef, RUNTIME_EVENT_TYPE,
};
pub use session_store::UnifiedSessionStore;
pub use store::session::{SessionEvent, SessionMessage, SessionRecord, SessionSearchResult, SessionSnapshot};
pub use store::verbatim::{VerbatimEntry, VerbatimSink};
pub use embedding::{EmbeddingClient, EmbeddingCapability};
pub use error::MemoryError;
pub use orchestrator::MemoryOrchestrator;
pub use handoff::HandoffManager;
pub use hot_reload::{
    ConfigChangeEvent, ConfigFile, ConfigHotReloader, HotReloadConfig,
    HotReloadHandle, SharedConfigReloader,
};
pub use types::{
    AgentVisibility,
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
pub use fact_checker::{FactChecker, FactCheckResult};
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
    TimeRange,
    temporal_relation,
};
pub use write_guard::{
    MemoryWriteGuard, WriteSource, WritePolicy, AuditLog, AuditEntry,
    AuditOperation, IntegrityChecker, Anomaly, AnomalyReport,
};
pub use tool_sandbox::{ToolOutputSandbox, ToolOutputSummary};
pub use tiered_store::{
    TieredSessionStore, TieredSessionStoreConfig,
    StorageTier, CompressionAlgo,
};
pub use closet::{Closet, ClosetPointer, ClosetEntry, ClosetManager, RANK_BOOSTS, CHAR_LIMIT, PointerKind, CodeSymbolId};
pub use session_resume::SessionResume;
pub use context_rot::{ContextRotMonitor, RotMetrics, RotAlert};
pub use compression::token_estimation::{
    TokenEstimator, HeuristicEstimator, SimpleTokenEstimator,
    estimate_tokens_text, estimate_tokens_messages,
};

// --- Project scope re-exports ---
pub use project_scope::{build_project_kg, MemoryScope, ProjectManifest, ProjectScopeManager};

// --- Background watcher re-exports ---
pub use background_watcher::{BackgroundWatcher, BackgroundWatcherConfig, BackgroundWatcherHandle};

// --- Code indexer re-exports (Phase 1) ---
pub use code_indexer::{
    CodeIndexer, CodeSymbol, FileFingerprint, ImpactReport, IndexLanguage, IndexStats,
    SymbolEdge, SymbolEdgeType, SymbolKind,
};

// --- Impact analyzer re-exports (F17) ---
pub use impact_analyzer::{CallGraph, ImpactAnalyzer};

// --- Performance monitor re-exports (P9.4) ---
pub use performance_monitor::{AutoTuner, PerformanceMonitor, PerformanceReport};

// --- Agent directory re-exports (P7.3) ---
pub use agent_directory::{AgentDirectory, AgentInfo, AgentStatus, ReputationScore};

// --- Memory sync re-exports (P8.4) ---
pub use memory_sync::MemorySyncProtocol;

// --- Entity registry re-exports (P9.3) ---
pub use entity_registry::{DisambiguationKey, EntityRecord, EntityRegistry, EvolutionRecord};

// --- Transaction re-exports (F20) ---
pub use transaction::{
    FileEditEffect, ReversibleEffect, TransactionError, TransactionGuard, TransactionManager,
};

// --- Agent reputation re-exports (P9.1) ---
pub use agent_reputation::{
    AgentMetrics, DecayConfig, ReputationManager,
    apply_decay, compute_reputation,
};
