#![warn(deprecated)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
//! `memory` – unified memory framework for the cowd AI assistant.
//!
//! # Architecture
//!
//! The memory system is organised into five layers (L0–L4), backed by a
//! unified `MemoryStore` trait with SQLite and separately composed backend
//! adapters. A compression
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

#[path = "ingestion/aaak_compression.rs"]
pub mod aaak_compression;
#[path = "lifecycle/agent_directory.rs"]
pub mod agent_directory;
#[path = "lifecycle/agent_reputation.rs"]
pub mod agent_reputation;
#[path = "lifecycle/automatic_governance.rs"]
pub mod automatic_governance;
#[path = "lifecycle/background_watcher.rs"]
pub mod background_watcher;
#[path = "ingestion/closet.rs"]
pub mod closet;
#[path = "ingestion/code_indexer.rs"]
pub mod code_indexer;
#[allow(private_interfaces)]
#[path = "kernel/cognitive.rs"]
pub mod cognitive;
#[path = "kernel/coherence.rs"]
pub mod coherence;
pub mod compression;
#[path = "ops/config.rs"]
pub mod config;
#[path = "session/context_fence.rs"]
pub mod context_fence;
#[path = "session/context_rot.rs"]
pub mod context_rot;
#[path = "session/context_sync.rs"]
pub mod context_sync;
#[path = "lifecycle/drift.rs"]
pub mod drift;
#[path = "ingestion/embedding.rs"]
pub mod embedding;
#[path = "graph/entity.rs"]
pub mod entity;
#[path = "graph/entity_registry.rs"]
pub mod entity_registry;
#[path = "ops/error.rs"]
pub mod error;
#[path = "ops/eval.rs"]
pub mod eval;
#[path = "ingestion/extractor.rs"]
pub mod extractor;
#[path = "graph/fact_checker.rs"]
pub mod fact_checker;
#[path = "session/fresh_context.rs"]
pub mod fresh_context;
#[path = "session/handoff.rs"]
pub mod handoff;
#[path = "lifecycle/hot_reload.rs"]
pub mod hot_reload;
#[path = "ingestion/impact_analyzer.rs"]
pub mod impact_analyzer;
#[path = "kernel/kernel.rs"]
pub mod kernel;
pub mod knowledge;
pub mod layers;
#[path = "ops/legacy_jsonl.rs"]
pub(crate) mod legacy_jsonl;
#[path = "lifecycle/maintenance.rs"]
pub mod maintenance;
#[path = "kernel/memory_authority.rs"]
pub mod memory_authority;
#[path = "kernel/memory_cluster.rs"]
pub mod memory_cluster;
#[allow(private_interfaces)]
#[path = "kernel/memory_pulse.rs"]
pub mod memory_pulse;
#[path = "kernel/memory_usage.rs"]
pub mod memory_usage;
#[path = "ingestion/miner.rs"]
pub mod miner;
#[path = "kernel/orchestrator.rs"]
pub mod orchestrator;
#[path = "lifecycle/performance_monitor.rs"]
pub mod performance_monitor;
#[path = "ingestion/project_scope.rs"]
pub mod project_scope;
#[path = "graph/resolution.rs"]
pub mod resolution;
pub mod search;
#[path = "ingestion/seeds.rs"]
pub mod seeds;
#[path = "session/session_resume.rs"]
pub mod session_resume;
#[path = "ops/shared.rs"]
pub mod shared;
#[path = "ingestion/splitter.rs"]
pub mod splitter;
#[path = "session/state_rebuilder.rs"]
pub mod state_rebuilder;
pub mod store;
#[path = "graph/temporal_graph.rs"]
pub mod temporal_graph;
#[path = "ops/tool_sandbox.rs"]
pub mod tool_sandbox;
#[path = "ops/transaction.rs"]
pub mod transaction;
#[path = "ops/types.rs"]
pub mod types;
#[path = "ops/write_guard.rs"]
pub mod write_guard;

// --- Convenience re-exports ---

pub use aaak_compression::{
    AaakCompressed, AaakCompressor, AaakDictionary, Abbreviation, EntityType, GsdContext, GsdState,
    PriorityItem,
};
pub use automatic_governance::{
    last_automatic_governance_report, run_automatic_governance,
    run_automatic_governance_with_resolver, AutomaticGovernanceMode, AutomaticGovernanceReport,
    SemanticGovernanceAction, SemanticGovernanceCandidate, SemanticGovernanceDecision,
    SemanticGovernanceEntry, SemanticGovernanceRequest, SemanticGovernanceResolver,
    SemanticGovernanceResponse,
};
pub use closet::{
    Closet, ClosetEntry, ClosetManager, ClosetPointer, CodeSymbolId, PointerKind, CHAR_LIMIT,
    RANK_BOOSTS,
};
pub use cognitive::{
    AutomaticGovernanceRunStatus, CognitiveContextManager, SessionRestoreStats, VectorIndexStats,
};
pub use compression::token_estimation::{
    estimate_tokens_messages, estimate_tokens_text, HeuristicEstimator, SimpleTokenEstimator,
    TokenEstimator,
};
pub use config::{GovernanceConfig, MemoryConfig, TuningConfig, VectorConfig};
pub use context_fence::{fence_from_session, filter_through_fence, ContextFence, FenceRegistry};
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
pub use kernel::reality_recall::{
    rank_and_deduplicate_candidates, rank_candidates, RecallCandidate, RecallCandidateEvidence,
    RecallCandidateScores, RecallFence, RecallOmission, RecallReport, RecallRequest, RecallSource,
    RecallSourceResult, RecallSourceStatus,
};
pub use kernel::{
    MemoryAtomView, MemoryContextPacket, MemoryContextPacketMode, MemoryDegradation, MemoryHealth,
    MemoryInformationState, MemoryKernel, MemoryKernelError, MemoryKernelResult, MemoryLayerView,
    MemoryLifecycleEvent, MemoryLink, MemoryLinkKind, MemoryPacketItem, MemoryPacketRole,
    MemoryPath, MemoryPrimitive, MemoryRuntimeSnapshot, MemoryState, MemoryTurnContext,
    OmittedMemory,
};
pub use knowledge::{
    durable_knowledge_fabric_for_config_home, ActivationGovernor, CanonExtractor,
    ClassificationResult, ConflictGovernor, ConflictStrategy, DocumentCategory, DocumentClassifier,
    DocumentContent, DocumentIngestor, DocumentMetadata, InMemoryKnowledgeStore, IngestionResult,
    KnowledgeChunk, KnowledgeConsolidationReport, KnowledgeFabric, KnowledgeFabricHealth,
    KnowledgeGraphBuilder, KnowledgeIngestionReceipt, KnowledgeIngestionService,
    KnowledgeMatrixBridgeFact, KnowledgeMatrixBridgeInput, KnowledgeMatrixBridgeRelation,
    KnowledgeNamespaceSearchResult, KnowledgeSnapshot, KnowledgeStore, KnowledgeStoreError,
    SqliteKnowledgeStore, UsageFeedbackLoop,
};
pub use maintenance::{
    scan_maintenance_candidates, MaintenanceCandidate, MaintenanceCandidateAction,
    MaintenanceCandidateFilter, MaintenanceCandidateKind, MaintenanceCandidateStatus,
    MaintenanceQueue, MaintenanceQueueBackend, MaintenanceScanConfig,
};
pub use memory_authority::{
    authority_decision, authority_level, same_memory_key, MemoryAuthorityAction,
    MemoryAuthorityDecision, MemoryAuthorityLevel,
};
pub use memory_cluster::{cluster_entries, MemoryCluster};
pub use memory_pulse::{
    MemoryPulseBatch, MemoryPulseConfig, MemoryPulseConsumer, MemoryPulseReport,
    MemoryPulseTransition,
};
pub use memory_usage::{summarize_usage, MemoryUsageSignal, MemoryUsageSummary};
pub use orchestrator::{L4PromotionCommand, MemoryOrchestrator};
pub use search::{BM25Scorer, HybridSearcher, SearchResult as HybridSearchResult};
pub use session_resume::SessionResume;
pub use state_rebuilder::{
    GsdRebuildOptions, GsdRebuiltState, GsdStateRebuilder, RebuildOptions, RebuiltSessionState,
    StateItem, StateRebuilder, StateSource,
};
pub use store::verbatim::{VerbatimEntry, VerbatimSink};
pub use store::{AuthorityLookup, MemoryScanCursor, MemoryScanPage, TaggedLookup};
pub use store::{
    FtsSearchOptions, FtsSearchResult, MemoryLayerAggregate, MemoryStore, MemoryStoreAggregate,
    MemoryStoreCapabilities,
};
pub use temporal_graph::{temporal_relation, TimeRange};
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
