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

pub mod aaak_compression;
pub mod agent_directory;
pub mod agent_reputation;
pub mod background_watcher;
pub mod closet;
pub mod code_indexer;
pub mod cognitive;
pub mod coherence;
pub mod compression;
pub mod config;
pub mod context_fence;
pub mod context_rot;
pub mod context_sync;
pub mod drift;
pub mod embedding;
pub mod entity;
pub mod entity_registry;
pub mod error;
pub mod eval;
pub mod extractor;
pub mod fact_checker;
pub mod fresh_context;
pub mod handoff;
pub mod hot_reload;
pub mod impact_analyzer;
pub mod kernel;
pub mod layers;
pub(crate) mod legacy_jsonl;
pub mod maintenance;
pub mod memory_pulse;
pub mod memory_sync;
pub mod miner;
pub mod orchestrator;
pub mod performance_monitor;
pub mod project_scope;
pub mod resolution;
pub mod runtime_event;
pub mod search;
pub mod seeds;
pub mod session_resume;
pub mod session_store;
pub mod shared;
pub mod splitter;
pub mod state_rebuilder;
pub mod store;
pub mod temporal_graph;
pub mod tiered_store;
pub mod tool_sandbox;
pub mod transaction;
pub mod types;
pub mod write_guard;

// --- Convenience re-exports ---

pub use aaak_compression::{
    AaakCompressed, AaakCompressor, AaakDictionary, Abbreviation, EntityType, GsdContext, GsdState,
    PriorityItem,
};
pub use closet::{
    Closet, ClosetEntry, ClosetManager, ClosetPointer, CodeSymbolId, PointerKind, CHAR_LIMIT,
    RANK_BOOSTS,
};
pub use cognitive::{CognitiveContextManager, SessionRestoreStats, VectorIndexStats};
pub use compression::token_estimation::{
    estimate_tokens_messages, estimate_tokens_text, HeuristicEstimator, SimpleTokenEstimator,
    TokenEstimator,
};
pub use config::{MemoryConfig, TuningConfig, VectorConfig};
pub use context_fence::{
    build_memory_context_block, fence_from_session, filter_through_fence, ContextFence, EntryBlock,
    FenceConfig, FenceRegistry, LayerBlock, MemoryContextBlock,
};
pub use context_rot::{ContextRotMonitor, RotAlert, RotMetrics};
pub use embedding::{EmbeddingCapability, EmbeddingClient};
pub use error::MemoryError;
pub use eval::{
    evaluate_retrieval, MemoryEvalCase, MemoryEvalMiss, MemoryEvalOptions, MemoryEvalReport,
};
pub use fact_checker::{FactCheckResult, FactChecker};
pub use fresh_context::{FreshContextManager, FreshEntry, SessionBudgetStatus, SessionTokenBudget};
pub use handoff::HandoffManager;
pub use hot_reload::{
    ConfigChangeEvent, ConfigFile, ConfigHotReloader, HotReloadConfig, HotReloadHandle,
    SharedConfigReloader,
};
pub use kernel::{
    MemoryAtomView, MemoryContextPacket, MemoryDegradation, MemoryHealth, MemoryInformationState,
    MemoryKernel, MemoryKernelError, MemoryKernelResult, MemoryLayerView, MemoryLifecycleEvent,
    MemoryLink, MemoryLinkKind, MemoryPacketItem, MemoryPacketRole, MemoryPath, MemoryPrimitive,
    MemoryState, MemoryTurnContext, OmittedMemory,
};
pub use layers::shared::{L4Event, L4EventBus, L4Operation};
pub use maintenance::{
    scan_maintenance_candidates, MaintenanceCandidate, MaintenanceCandidateFilter,
    MaintenanceCandidateKind, MaintenanceCandidateStatus, MaintenanceQueue, MaintenanceScanConfig,
};
pub use memory_pulse::{
    MemoryPulseBatch, MemoryPulseConfig, MemoryPulseConsumer, MemoryPulseReport,
    MemoryPulseTransition,
};
pub use orchestrator::MemoryOrchestrator;
pub use runtime_event::{
    RuntimeEvent, RuntimeEventPage, RuntimeEventScope, RuntimeRef, RUNTIME_EVENT_TYPE,
};
pub use search::{BM25Scorer, HybridSearcher, SearchResult as HybridSearchResult};
pub use session_resume::SessionResume;
pub use session_store::UnifiedSessionStore;
pub use state_rebuilder::{
    GsdRebuildOptions, GsdRebuiltState, GsdStateRebuilder, RebuildOptions, RebuiltSessionState,
    StateItem, StateRebuilder, StateSource,
};
pub use store::session::{
    SessionEvent, SessionMessage, SessionRecord, SessionSearchResult, SessionSnapshot,
};
pub use store::verbatim::{VerbatimEntry, VerbatimSink};
pub use temporal_graph::{temporal_relation, TimeRange};
pub use tiered_store::{
    CompressionAlgo, StorageTier, TieredSessionStore, TieredSessionStoreConfig,
};
pub use tool_sandbox::{ToolOutputSandbox, ToolOutputSummary};
pub use types::{
    AgentVisibility, AlertLevel, Blocker, ContextAction, ContextMonitor, Decision, DecisionEntry,
    DecisionStatus, DecisionThread, HandoffData, MatchedKeyword, MemoryCategory, MemoryEntry,
    MemoryId, MemoryLayer, MemoryMeta, MemorySource, PreparedContext, Priority, Relation,
    RelationKind, SearchMemoriesRequest, SearchMemoriesResult, SearchMode, SearchSnippet, Seed,
    SeedId, SeedTrigger, TaskState, TemporalMarker, TokenBudget, WorkItem, WorkItemStatus,
};
pub use write_guard::{
    Anomaly, AnomalyReport, AuditEntry, AuditLog, AuditOperation, IntegrityChecker,
    MemoryWriteGuard, WritePolicy, WriteSource,
};

// --- Project scope re-exports ---
pub use project_scope::{build_project_kg, MemoryScope, ProjectManifest, ProjectScopeManager};

// --- Background watcher re-exports ---
pub use background_watcher::{BackgroundWatcher, BackgroundWatcherConfig, BackgroundWatcherHandle};

// --- Code indexer re-exports (Phase 1) ---
pub use code_indexer::{
    CodeIndexer, CodeSymbol, FileFingerprint, ImpactReport, IndexLanguage, IndexStats, SymbolEdge,
    SymbolEdgeType, SymbolKind,
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
    apply_decay, compute_reputation, AgentMetrics, DecayConfig, ReputationManager,
};
