//! `CognitiveContextManager` – unified entry-point (facade) for the memory framework.
//!
//! Coordinates all sub-systems (orchestrator, compression pipeline, relevance
//! scoring, dynamic loading, context monitoring, handoff, seeds, drift
//! detection) to produce the optimal [`PreparedContext`] within the current
//! token budget.
//!
//! # Progressive disclosure
//!
//! Context is assembled in priority order:
//! 1. L0 + L1 – fixed identity and working memory (always present).
//! 2. L2      – project context.
//! 3. L3      – dynamically loaded deep memories (multi-signal relevance).
//! 4. Seeds   – pre-authored fragments whose trigger condition fired.

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use parking_lot::Mutex;
use tokio::sync::mpsc;

use chrono::Utc;

use crate::{ MemoryScope, SessionResume,
    background_watcher::{BackgroundWatcher, BackgroundWatcherConfig, BackgroundWatcherHandle},
    closet::{Closet, ClosetManager},
    code_indexer::CodeSymbol,
    coherence,
    compression::{
        budget::BudgetManager,
        llm_summarizer::OpenAiSummarizer,
        monitor::ContextWindowMonitor,
        CompressionPipeline,
    },
    config::{BudgetCalculator, MemoryConfig},
    context_rot::{ContextRotMonitor, RotAlert, RotMetrics},
    drift::DriftDetector,
    embedding::EmbeddingCapability,
    entity::KnowledgeGraph,
    error::MemoryError,
    extractor::MemoryExtractor,
    fresh_context::FreshContextManager,
    handoff::HandoffManager,
    orchestrator::MemoryOrchestrator,
    project_scope::{build_project_kg, ProjectScopeManager},
    search::HybridSearcher,
    seeds::{DecisionThreadStore, SeedRegistry},
    state_rebuilder::StateRebuilder,
    store::{FtsSearchOptions, FtsSearchResult},
    store::vector::VectorIndex,
    tool_sandbox::ToolOutputSandbox,
    types::{
        Blocker, Decision, DecisionEntry, HandoffData, MatchedKeyword, MemoryEntry, MemoryId,
        MemoryLayer, MemoryCategory, MemorySource, Message, MessageRole, PreparedContext, Priority,
        SearchMemoriesRequest, SearchMemoriesResult, SearchMode, SearchSnippet, Seed, TokenBudget,
        WorkItem, WorkItemStatus,
    },
    write_guard::{AuditLog, AuditOperation, AuditEntry, IntegrityChecker, MemoryWriteGuard, WriteSource},
};

/// Result alias used throughout this module.
pub type Result<T> = std::result::Result<T, MemoryError>;

// ---------------------------------------------------------------------------
// Delegation Types
// ---------------------------------------------------------------------------

/// Result of a child agent delegation.
#[derive(Debug, Clone)]
pub struct DelegationResult {
    pub agent_role: String,
    pub task: String,
    pub result: String,
    pub parent_session_id: Option<String>,
    pub timestamp: chrono::DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Session Restoration Types
// ---------------------------------------------------------------------------

/// Statistics about a session restore operation.
#[derive(Debug, Clone, Default)]
pub struct SessionRestoreStats {
    pub memories_restored: u32,
    pub decisions_restored: u32,
    pub work_items_restored: u32,
    pub context_summary_length: usize,
}

// ---------------------------------------------------------------------------
// CachedLayer
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct CachedLayer {
    entries: Vec<MemoryEntry>,
    knowledge_graph: String,
    code_context: String,
    cached_at: Instant,
}

// ---------------------------------------------------------------------------
// CognitiveContextManager
// ---------------------------------------------------------------------------

/// Unified facade that coordinates all memory sub-systems.
///
/// Create once per session with [`CognitiveContextManager::new`] and use the
/// provided methods to prepare context, persist memories, and manage
/// cross-session handoffs.
pub struct CognitiveContextManager {
    /// Merged configuration.
    config: MemoryConfig,
    /// Five-layer memory orchestrator.
    orchestrator: MemoryOrchestrator,
    /// Three-stage compression pipeline.
    pipeline: CompressionPipeline,
    /// In-process vector index for semantic search.
    vector_index: Mutex<VectorIndex>,
    /// Hybrid (BM25+vector) searcher for re-ranking.
    hybrid_searcher: HybridSearcher,
    /// Real-time context window pressure monitor.
    monitor: ContextWindowMonitor,
    /// Cross-session handoff manager.
    handoff_mgr: HandoffManager,
    /// Pre-authored context seed registry.
    seeds: Mutex<SeedRegistry>,
    /// Persistent decision thread log.
    decisions: Mutex<DecisionThreadStore>,
    /// Persisted Closet index, loaded from SQLite on startup.
    closet: Mutex<Option<Closet>>,
    /// Staleness and contradiction detector.
    drift: DriftDetector,
    /// Write guard for anti-corruption control.
    write_guard: Option<MemoryWriteGuard>,
    /// Audit log for tracking all write operations.
    audit_log: Option<AuditLog>,
    /// Anomaly detector for write pattern irregularities.
    integrity_checker: Option<Arc<IntegrityChecker>>,
    /// Tick counter for periodic integrity checks (every 50 ticks).
    integrity_check_counter: AtomicU64,
    /// Embedding capability level (Remote/Local/FTS5Only).
    embedding_capability: EmbeddingCapability,
    /// Heuristic memory extractor, shared with background LLM extraction task.
    extractor: Arc<MemoryExtractor>,
    /// In-memory knowledge graph for entity relationships.
    kg: Arc<Mutex<KnowledgeGraph>>,
    /// Handle to the optional background file-system watcher.
    #[allow(dead_code)]
    background_watcher: Mutex<Option<BackgroundWatcherHandle>>,
    /// Sender for queuing messages to the background LLM extraction worker.
    extract_tx: mpsc::UnboundedSender<Vec<Message>>,
    /// Pending memory entries from the background LLM extraction worker,
    /// drained at the start of each turn-end cycle.
    pending_llm_entries: Arc<Mutex<Vec<MemoryEntry>>>,
    /// Handle to the background LLM extraction task.
    #[allow(dead_code)]
    extract_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Freshness-priority context manager for session budget management.
    fresh_ctx: FreshContextManager,
    /// Context rotation monitor for GSD-style health warnings.
    context_rot_monitor: Mutex<ContextRotMonitor>,
    /// Active agent ID for peer context discovery.
    current_agent: Mutex<Option<String>>,
    /// Queue of child agent delegation results, consumed by on_turn_end.
    delegation_results: Mutex<Vec<DelegationResult>>,
    /// BM25-based session resume for context recovery from prior sessions.
    session_resume: Option<SessionResume>,
    /// Optional project scope manager for KG staleness detection.
    project_scope_mgr: Option<std::sync::Arc<ProjectScopeManager>>,
    /// Path of the currently loaded project KG, used for auto-rebuild.
    project_kg_path: Mutex<Option<PathBuf>>,
    /// Tick counter for periodic KG rebuild (every 100 ticks).
    kg_rebuild_tick_counter: AtomicU64,
    /// Tick counter for cross-store consistency verification (every 50 ticks).
    cross_store_verify_counter: AtomicU64,
    /// In-memory FTS5 sandbox for indexing large tool outputs.
    tool_sandbox: Mutex<ToolOutputSandbox>,
    /// State rebuilder for session restoration from previous session data.
    state_rebuilder: Option<StateRebuilder>,
    /// Blockers preventing forward progress, collected during session.
    blockers: Mutex<Vec<String>>,
    /// Last action performed by the agent, used for handoff context.
    last_action: Mutex<Option<String>>,
    /// Cache for L0 (identity) layer entries.
    l0_cache: Mutex<Option<CachedLayer>>,
    /// Cache for L1 (core/working) layer entries.
    l1_cache: Mutex<Option<CachedLayer>>,
    /// Cache for L2 (project) layer entries.
    l2_cache: Arc<Mutex<Option<CachedLayer>>>,
}

impl CognitiveContextManager {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Initialise the manager from `config`, opening all storage backends.
    pub async fn new(config: MemoryConfig) -> Result<Self> {
        Self::new_with_workspace(config, None).await
    }

    /// Initialise the manager with an explicit workspace root for L2 project
    /// context discovery.
    pub async fn new_with_workspace(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
    ) -> Result<Self> {
        // Build the orchestrator (opens SQLite, wires all layers).
        let orchestrator =
            MemoryOrchestrator::init_with_workspace(config.clone(), workspace_root.clone()).await?;

        // Build the compression pipeline from config.
        let pipeline = CompressionPipeline::from_config(&config.compression);

        // Build the vector index with persistence support.
        // Use VectorIndex::load to restore previously persisted vectors.
        let dimension = if config.store.vector.dimension > 0 {
            config.store.vector.dimension as u32
        } else {
            // Default embedding dimension (text-embedding-3-small / OpenAI).
            1536
        };
        let persist_path = config.store.blob_dir.join("vector_index.json");
        let vector_index = Mutex::new(
            VectorIndex::load(persist_path, dimension)
                .map_err(|e| MemoryError::Store(format!("load vector index: {e}")))?,
        );

        // Build the context window monitor.
        let budget_mgr = BudgetManager::new(config.budget.clone());
        let monitor = ContextWindowMonitor::new(budget_mgr);

        // Determine embedding capability before moving config.
        let embedding_capability = EmbeddingCapability::from_config(&config.store.vector);

        // Startup info logs for optional features.
        if !embedding_capability.supports_semantic() {
            tracing::info!("vector search: disabled (FTS5 keyword-only fallback)");
        }
        if !config.compression.llm.is_configured() {
            tracing::info!("LLM summarizer: not configured (template fallback)");
        }

        // Build the memory extractor.
        let mut extractor = MemoryExtractor::new(config.extractor.clone());
        let llm_configured = config.compression.llm.is_configured();
        if llm_configured {
            let summarizer = OpenAiSummarizer::new(
                config.compression.llm.api_url.clone(),
                config.compression.llm.api_key.clone(),
                config.compression.llm.model.clone(),
            );
            extractor = extractor.with_llm(Arc::new(summarizer));
            tracing::info!("LLM-enhanced extraction enabled (Pass 5)");
        }

        // Wrap extractor in Arc for sharing with the background LLM task.
        let extractor = Arc::new(extractor);

        // ── Background LLM extraction worker ────────────────────────────────
        let (extract_tx, mut extract_rx) = mpsc::unbounded_channel::<Vec<Message>>();
        let pending_llm_entries: Arc<Mutex<Vec<MemoryEntry>>> = Arc::new(Mutex::new(Vec::new()));

        let bg_extractor = Arc::clone(&extractor);
        let bg_pending = Arc::clone(&pending_llm_entries);
        let extractor_debounce_secs = config.extractor.extractor_debounce_secs;

        let extract_handle = tokio::spawn(async move {
            let mut last_run = Instant::now();
            let debounce = Duration::from_secs(extractor_debounce_secs);
            while let Some(messages) = extract_rx.recv().await {
                if last_run.elapsed() < debounce {
                    tracing::debug!(
                        elapsed = ?last_run.elapsed(),
                        "background LLM extract: debouncing, skipping batch"
                    );
                    continue;
                }
                if bg_extractor.llm_client().is_some() {
                    match bg_extractor.llm_extract(&messages).await {
                        Ok(llm_entries) => {
                            let final_entries = bg_extractor.finalize_entries(llm_entries);
                            if !final_entries.is_empty() {
                                tracing::info!(
                                    count = final_entries.len(),
                                    "background LLM extract: {} entries ready for merge",
                                    final_entries.len()
                                );
                                bg_pending.lock().extend(final_entries);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(%e, "background LLM extract failed");
                        }
                    }
                }
                last_run = Instant::now();
            }
            tracing::debug!("background LLM extract: worker exiting");
        });

        // Load knowledge graph from persistent store.
        let kg = {
            let entities = orchestrator.store().load_entities().await.unwrap_or_default();
            let triples = orchestrator.store().load_triples().await.unwrap_or_default();
            let mut graph = KnowledgeGraph::new();
            for e in entities {
                graph.add_entity(e);
            }
            for t in triples {
                graph.add_triple_raw(t);
            }
            // Self-healing: run consistency check after KG load
            let fixes = graph.run_consistency_check();
            for fix in &fixes {
                tracing::info!("self-healing: {fix}");
            }
            if !fixes.is_empty() {
                tracing::warn!(
                    fix_count = fixes.len(),
                    "KG self-healing applied {} fixes",
                    fixes.len()
                );
            }
            tokio::task::yield_now().await;
            if graph.list_entities().is_empty() && graph.list_triples().is_empty() {
                tracing::debug!("KG loaded: empty (no persisted data)");
            } else {
                tracing::debug!(
                    "KG loaded: {} entities, {} triples",
                    graph.list_entities().len(),
                    graph.list_triples().len()
                );
            }
            Arc::new(Mutex::new(graph))
        };

        // ── Background file-system watcher setup ──────────────────────────
        // Wire up the channel BEFORE constructing Self so both the receiver
        // task and the watcher thread can be started with the right handles.
        let (kg_rebuild_tx, mut kg_rebuild_rx) =
            tokio::sync::mpsc::unbounded_channel::<KnowledgeGraph>();

        // L2 cache needs to be shared with the receiver task so it can be
        // invalidated when the project KG is rebuilt.
        let l2_cache: Arc<Mutex<Option<CachedLayer>>> = Arc::new(Mutex::new(None));

        // Spawn a lightweight tokio task that listens for rebuilt KGs and
        // replaces the in-memory graph.  The task holds a clone of the Arc.
        let kg_for_receiver = kg.clone();
        let l2_cache_for_receiver = l2_cache.clone();
        tokio::spawn(async move {
            while let Some(new_kg) = kg_rebuild_rx.recv().await {
                let mut guard = kg_for_receiver.lock();
                let old_count = guard.list_entities().len();
                *guard = new_kg;
                let new_count = guard.list_entities().len();
                tracing::info!(
                    old_count,
                    new_count,
                    "background_watcher: KG replaced in CCM"
                );
                // Invalidate L2 cache when project KG is rebuilt from file changes.
                l2_cache_for_receiver.lock().take();
                tracing::debug!("background_watcher: L2 cache invalidated");
            }
            tracing::debug!("background_watcher: receiver task exiting");
        });

        // Start the OS-level file-system watcher if the config calls for it.
        let watcher_handle: Option<BackgroundWatcherHandle> =
            if let Some(ref ws_root) = workspace_root {
                if config.extractor.poll_interval_secs > 0 {
                    let watcher_config = BackgroundWatcherConfig {
                        poll_interval_secs: config.extractor.poll_interval_secs,
                    };
                    Some(BackgroundWatcher::start(
                        ws_root.clone(),
                        watcher_config,
                        kg_rebuild_tx,
                    ))
                } else {
                    None
                }
            } else {
                None
            };

        // Restore Closet from KV store and re-inject into orchestrator.
        let closet_json = orchestrator.store().kv_get("closet").await.unwrap_or(None);
        let closet: Option<Closet> = closet_json
            .and_then(|json| {
                serde_json::from_str::<Vec<crate::closet::ClosetPointer>>(&json)
                    .ok()
                    .map(|pointers| Closet { pointers })
            });
        if let Some(ref c) = closet {
            orchestrator.restore_closet(c.clone()).await?;
        }

        // Build SessionResume from recent entries for BM25-based session recovery.
        let session_resume = {
            let recent_entries = orchestrator.store().list_all().await.unwrap_or_default();
            if recent_entries.is_empty() {
                None
            } else {
                Some(SessionResume::new(recent_entries))
            }
        };

        // Restore Seeds from KV store.
        let seeds_json = orchestrator.store().kv_get("seeds").await.unwrap_or(None);
        let saved_seeds: Vec<Seed> = seeds_json
            .and_then(|json| serde_json::from_str::<Vec<Seed>>(&json).ok())
            .unwrap_or_default();
        let seeds = {
            let mut registry = SeedRegistry::new();
            let _ = registry.bootstrap_system_seeds();
            for seed in saved_seeds {
                registry.register(seed);
            }
            Mutex::new(registry)
        };

        // Build state_rebuilder if workspace_root is available
        let ws_root = workspace_root.clone();
        let state_rebuilder = ws_root.as_ref().map(|ws| StateRebuilder::new(ws.clone()));
        let tool_sandbox = match ToolOutputSandbox::new() {
            Ok(ts) => Mutex::new(ts),
            Err(e) => {
                tracing::warn!("failed to initialize tool sandbox: {e}");
                Mutex::new(ToolOutputSandbox::new().expect("tool sandbox retry"))
            }
        };

        let integrity_checker = {
            let audit_path = config.store.blob_dir.join("audit.jsonl");
            match AuditLog::open(audit_path) {
                Ok(log) => Some(Arc::new(IntegrityChecker::new(log))),
                Err(e) => {
                    tracing::warn!("integrity checker: failed to open audit log: {e}");
                    None
                }
            }
        };

        Ok(Self {
            drift: DriftDetector::new(config.drift.clone()),
            fresh_ctx: FreshContextManager::new(config.budget.context_window),
            context_rot_monitor: Mutex::new(ContextRotMonitor::new(RotMetrics::default())),
            current_agent: Mutex::new(None),
            delegation_results: Mutex::new(Vec::new()),
            session_resume,
            project_scope_mgr: None,
            project_kg_path: Mutex::new(None),
            kg_rebuild_tick_counter: AtomicU64::new(0),
            cross_store_verify_counter: AtomicU64::new(0),
            tool_sandbox,
            state_rebuilder,
            blockers: Mutex::new(Vec::new()),
            last_action: Mutex::new(None),
            l0_cache: Mutex::new(None),
            l1_cache: Mutex::new(None),
            l2_cache,
            config,
            orchestrator,
            pipeline,
            vector_index,
            hybrid_searcher: HybridSearcher::new(0.6, 0.4),
            monitor,
            handoff_mgr: HandoffManager::new(),
            seeds,
            decisions: Mutex::new(DecisionThreadStore::new()),
            closet: Mutex::new(closet),
            write_guard: None,
            audit_log: None,
            integrity_checker,
            integrity_check_counter: AtomicU64::new(0),
            embedding_capability,
            extractor,
            kg,
            background_watcher: Mutex::new(watcher_handle),
            extract_tx,
            pending_llm_entries,
            extract_handle: Mutex::new(Some(extract_handle)),
        })
    }

    /// Initialise the manager and auto-load the project knowledge graph when
    /// workspace_root is provided.
    pub async fn new_with_project_kg(
        config: MemoryConfig,
        workspace_root: PathBuf,
    ) -> Result<Self> {
        let mgr = Self::new_with_workspace(config, Some(workspace_root.clone())).await?;
        let _ = mgr.load_project_kg(&workspace_root);
        Ok(mgr)
    }

    // -----------------------------------------------------------------------
    // Write guard configuration
    // -----------------------------------------------------------------------

    /// Set the write guard for controlling write access.
    ///
    /// Propagates the guard to the underlying [`MemoryOrchestrator`] so that
    /// [`MemoryOrchestrator::remember`] also enforces layer permissions.
    pub fn with_write_guard(mut self, guard: MemoryWriteGuard) -> Self {
        self.orchestrator = self.orchestrator.with_write_guard(Arc::new(guard.clone()));
        self.write_guard = Some(guard);
        self
    }

    /// Set the audit log for tracking write operations.
    pub fn with_audit_log(mut self, log: AuditLog) -> Self {
        self.audit_log = Some(log);
        self
    }

    /// Set the write source, creating a default guard for that source.
    pub fn with_write_source(mut self, source: WriteSource) -> Self {
        self.write_guard = Some(MemoryWriteGuard::new(source));
        self
    }

    /// Set the active agent for source_agent tagging and peer context discovery.
    pub fn set_active_agent(&self, agent_id: String) {
        self.orchestrator.set_active_agent(agent_id.clone());
        *self.current_agent.lock() = Some(agent_id);
    }

    /// Set the active session ID for auto-filling new entries and intra-turn
    /// peer perception.
    pub fn set_active_session(&self, session_id: String) {
        self.orchestrator.set_active_session(session_id);
    }

    /// Get the current active session ID (if set).
    pub fn active_session_id(&self) -> Option<String> {
        self.orchestrator.active_session_id()
    }

    /// Attach a [`ProjectScopeManager`] for KG staleness detection on turn end.
    ///
    /// When set, [`on_turn_end`] will check whether any indexed source files
    /// have changed since the last KG build and auto-rebuild if stale.
    pub fn with_project_scope(mut self, mgr: ProjectScopeManager) -> Self {
        self.project_scope_mgr = Some(std::sync::Arc::new(mgr));
        self
    }

    /// Check whether a write to `layer` is allowed under the current guard.
    pub fn check_write_access(&self, layer: crate::types::MemoryLayer) -> crate::write_guard::WritePolicy {
        match &self.write_guard {
            Some(guard) => guard.check_write(layer),
            None => crate::write_guard::WritePolicy::Allow,
        }
    }

    // -----------------------------------------------------------------------
    // Core: prepare_context
    // -----------------------------------------------------------------------

    /// Build and load the project knowledge graph (P1 KG) from source files.
    ///
    /// Scans `project_path` for code symbols, replaces the current in-memory
    /// knowledge graph with the freshly built graph. This should be called
    /// whenever the active project is switched.
    pub fn load_project_kg(&self, project_path: &PathBuf) -> Result<()> {
        let (kg, _mtimes) = build_project_kg(project_path);
        let entity_count = kg.list_entities().len();
        let mut guard = self
            .kg
            .lock()
            ;
        *guard = kg;
        // Track path for auto-rebuild on staleness
        *self.project_kg_path.lock() = Some(project_path.clone());
        tracing::info!(
            entity_count,
            path = %project_path.display(),
            "project knowledge graph loaded"
        );
        Ok(())
    }

    /// Assemble the optimal context for the upcoming model turn.
    ///
    /// Implements "progressive disclosure":
    /// 1. Load fixed layers L0 + L1.
    /// 2. Load project context L2.
    /// 3. Dynamic-load relevant deep memories L3 via multi-signal scoring.
    /// 4. Surface triggered seeds.
    /// 5. Sample context window pressure.
    /// 6. Compress if needed.
    pub async fn prepare_context(
        &self,
        query: &str,
        messages: &[Message],
        session_id: Option<&str>,
    ) -> Result<PreparedContext> {
        let _prepare_start = Instant::now();
        let mut entries: Vec<MemoryEntry> = Vec::new();

        // ═══════════════════════════════════════════════════════════════════
        // Step 0: Closet LRU prefetch — preload hot topics based on
        //         access counts tracked in closet pointers (F19).
        // ═══════════════════════════════════════════════════════════════════
        {
            let k = self.config.tuning.prefetch_hot_topics;
            if k > 0 {
                // Collect hot topics (owned strings) while holding the lock,
                // then drop the lock before async operations.
                let hot_topics: Vec<String> = {
                    let closet_guard = self.orchestrator.closet_manager().lock();
                    closet_guard
                        .get_hot_pointers(k)
                        .into_iter()
                        .map(|p| p.topic.clone())
                        .collect()
                };
                for topic in hot_topics {
                    let prefetch_set: HashSet<MemoryId> =
                        entries.iter().map(|e| e.id).collect();
                    let budget = (self.config.budget.available_tokens() / 4)
                        .min(u64::from(u32::MAX)) as u32;
                    match self
                        .orchestrator
                        .recall_relevant(&topic, None, &prefetch_set, budget)
                        .await
                    {
                        Ok(mut recalled) => {
                            for entry in &mut recalled {
                                entry.content =
                                    format!("[PREFETCH: {}] {}", topic, entry.content);
                                entry.tags.push("prefetch".into());
                                entry.source = MemorySource::Prefetch;
                                entry.priority = Priority::High;
                            }
                            entries.extend(recalled);
                        }
                        Err(e) => {
                            tracing::debug!(
                                topic = %topic,
                                error = %e,
                                "closet prefetch: recall_relevant failed for hot topic"
                            );
                        }
                    }
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // Group 1: Base layers (L0+L1) + Project layer (L2) — cache-aware
        // ═══════════════════════════════════════════════════════════════════

        // L0 + L1: check cache first; reload both together if either expired.
        {
            let l0_hit = self.l0_cache.lock().as_ref()
                .filter(|c| c.cached_at.elapsed() < Duration::from_secs(self.config.tuning.l0_cache_ttl_secs))
                .map(|c| c.entries.clone());
            let l1_hit = self.l1_cache.lock().as_ref()
                .filter(|c| c.cached_at.elapsed() < Duration::from_secs(self.config.tuning.l1_cache_ttl_secs))
                .map(|c| c.entries.clone());

            if let (Some(l0), Some(l1)) = (l0_hit, l1_hit) {
                entries.extend(l0);
                entries.extend(l1);
            } else {
                let fixed = self.orchestrator.load_fixed_layers().await?;
                let l0: Vec<_> = fixed.iter()
                    .filter(|e| matches!(e.layer, MemoryLayer::L0))
                    .cloned()
                    .collect();
                let l1: Vec<_> = fixed.iter()
                    .filter(|e| matches!(e.layer, MemoryLayer::L1))
                    .cloned()
                    .collect();
                let now = Instant::now();
                *self.l0_cache.lock() = Some(CachedLayer {
                    entries: l0.clone(),
                    knowledge_graph: String::new(),
                    code_context: String::new(),
                    cached_at: now,
                });
                *self.l1_cache.lock() = Some(CachedLayer {
                    entries: l1.clone(),
                    knowledge_graph: String::new(),
                    code_context: String::new(),
                    cached_at: now,
                });
                entries.extend(l0);
                entries.extend(l1);
            }
        }

        // L2: project context with cache
        {
            let l2_hit = self.l2_cache.lock().as_ref()
                .filter(|c| c.cached_at.elapsed() < Duration::from_secs(self.config.tuning.l2_cache_ttl_secs))
                .map(|c| c.entries.clone());
            if let Some(l2) = l2_hit {
                entries.extend(l2);
            } else {
                let l2 = self.orchestrator.load_project_context().await?;
                *self.l2_cache.lock() = Some(CachedLayer {
                    entries: l2.clone(),
                    knowledge_graph: String::new(),
                    code_context: String::new(),
                    cached_at: Instant::now(),
                });
                entries.extend(l2);
            }
        }

        // Query embedding (async, independent of cached loads)
        let query_embedding = {
            if self.embedding_capability.supports_semantic() {
                match &self.embedding_capability {
                    EmbeddingCapability::Remote { client } => {
                        match client.embed_one(query).await {
                            Ok(embed) => {
                                tracing::debug!(
                                    dim = embed.len(),
                                    "query embedding generated for hybrid search"
                                );
                                Some(embed)
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "embedding failed, falling back to FTS5 search"
                                );
                                None
                            }
                        }
                    }
                    _ => None,
                }
            } else {
                None
            }
        };

        // Track which IDs are already loaded so other layers can skip them.
        let mut already_surfaced: HashSet<MemoryId> = entries.iter().map(|e| e.id).collect();

        // ── Step 2a2: State rebuild from previous session state ──────────────
        if let Some(ref rebuilder) = self.state_rebuilder {
            let rebuilt = rebuilder.quick_rebuild().await;
            if rebuilt.overall_confidence > self.config.tuning.rebuild_confidence {
                if let Some(ref summary) = rebuilt.context_summary {
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L2,
                        category: MemoryCategory::CompressedSummary,
                        priority: Priority::Normal,
                        source: MemorySource::AutoExtracted,
                        title: "Rebuilt Context Summary".into(),
                        content: format!("[REBUILT STATE confidence={:.2}] {}", rebuilt.overall_confidence, summary.data),
                        embedding: None,
                        tags: vec!["rebuilt".into(), "state".into()],
                        relations: vec![],
                        confidence: summary.confidence,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                }
                for item in rebuilt.get_incomplete_work() {
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L1,
                        category: MemoryCategory::Reference,
                        priority: item.priority,
                        source: MemorySource::AutoExtracted,
                        title: format!("Rebuilt: {}", item.title),
                        content: format!("[REBUILT WORK ITEM] {}", item.description),
                        embedding: None,
                        tags: vec!["rebuilt".into(), "work".into()],
                        relations: vec![],
                        confidence: 0.7,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                }
                tracing::info!(
                    confidence = rebuilt.overall_confidence,
                    work_items = rebuilt.get_incomplete_work().len(),
                    "state_rebuilder: surfaced rebuilt state"
                );
            }
        }

        // ── Step 2b: P1 project knowledge graph query ───────────────────────
        {
            let kg = self
                .kg
                .lock()
                ;
            let query_tokens: Vec<String> = query
                .split_whitespace()
                .map(str::to_lowercase)
                .collect();
            let mut seen: HashSet<String> = HashSet::new();
            for token in &query_tokens {
                if seen.contains(token) {
                    continue;
                }
                if let Some(entity) = kg.get_entity_by_name(token) {
                    seen.insert(token.clone());
                    use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L2,
                        category: MemoryCategory::ProjectKnowledge,
                        priority: Priority::Normal,
                        source: MemorySource::AutoExtracted,
                        title: format!("KG entity: {}", entity.name),
                        content: format!(
                            "Project entity '{}' (type: {}, confidence: {:.2})",
                            entity.name, entity.entity_type, entity.confidence
                        ),
                        embedding: None,
                        tags: vec!["kg".into(), "project".into()],
                        relations: vec![],
                        confidence: entity.confidence as f32,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                    tracing::debug!(
                        entity = %entity.name,
                        entity_type = %entity.entity_type,
                        "P1 KG: surfaced project entity"
                    );
                }
            }
        }

        // Compute budget for L3 token-aware recall
        let budget = self.compute_budget();
        let memory_budget = budget
            .available
            .saturating_sub(self.estimate_tokens_entries(&entries))
            .min(u64::from(u32::MAX)) as u32;

        // ═══════════════════════════════════════════════════════════════════
        // Group 2: Peer context + L4 recall + L3 recall + SessionResume — all independent
        // ═══════════════════════════════════════════════════════════════════
        let current_agent = self.current_agent.lock().clone();
        let current_session = self.orchestrator.active_session_id();

        let ((peers, realtime_peers), l4_project, l4_global, l3_result, resume_result) = tokio::join!(
            // Peer context: regular + realtime
            async {
                if let Some(ref agent) = current_agent {
                    let peers = self
                        .orchestrator
                        .recall_peer_context(query, agent, 3, 5)
                        .await
                        .unwrap_or_default();
                    let realtime = if let Some(ref sid) = current_session {
                        self.orchestrator
                            .recall_peer_context_realtime(query, agent, sid, 2, 3)
                            .await
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    (peers, realtime)
                } else {
                    (Vec::new(), Vec::new())
                }
            },
            // L4 project-scoped recall
            async {
                match self.orchestrator.recall_l4_project(query, 5).await {
                    Ok(r) => r,
                    Err(e) => { tracing::warn!(error=%e, "L4 project recall failed"); Vec::new() }
                }
            },
            // L4 global-scoped recall
            async {
                match self.orchestrator.recall_l4_global(query, 5).await {
                    Ok(r) => r,
                    Err(e) => { tracing::warn!(error=%e, "L4 global recall failed"); Vec::new() }
                }
            },
            // L3 deep recall (hybrid semantic + BM25)
            async {
                self.orchestrator.recall_relevant(
                    query,
                    query_embedding.as_deref(),
                    &already_surfaced,
                    memory_budget * 2, // over-fetch for hybrid re-ranking
                )
                .await
            },
            // Session resume from prior session context
            async {
                if let Some(ref resume) = self.session_resume {
                    let store_arc = self.orchestrator.store();
                    let store: &dyn crate::store::MemoryStore = store_arc.as_ref();
                    resume.resume_recent(query, Some(store), 5).await
                } else {
                    Ok(Vec::new())
                }
            },
        );

        // Process peer context results
        if current_agent.is_some() {
            for entry in peers {
                let peer_id = entry.source_agent.as_deref().unwrap_or("unknown");
                let prefixed_content = format!("[PEER: {peer_id}] {}", entry.content);
                use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
                entries.push(MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L4,
                    category: MemoryCategory::Shared,
                    priority: Priority::Normal,
                    source: MemorySource::Import,
                    title: format!("Peer context from {peer_id}"),
                    content: prefixed_content,
                    embedding: None,
                    tags: vec!["peer".into(), "l4".into()],
                    relations: vec![],
                    confidence: 0.8,
                    access_count: 0,
                    staleness: 0.0,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: None,
                    source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                });
            }
            for entry in realtime_peers {
                let peer_id = entry.source_agent.as_deref().unwrap_or("unknown");
                let prefixed_content = format!("[REALTIME PEER: {peer_id}] {}", entry.content);
                use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
                entries.push(MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L4,
                    category: MemoryCategory::Shared,
                    priority: Priority::High,
                    source: MemorySource::Import,
                    title: format!("Realtime peer context from {peer_id}"),
                    content: prefixed_content,
                    embedding: None,
                    tags: vec!["peer".into(), "realtime".into(), "l4".into()],
                    relations: vec![],
                    confidence: 0.9,
                    access_count: 0,
                    staleness: 0.0,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: None,
                    source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                });
            }
        }

        // Process L4 recall results (filter against already-surfaced)
        for entry in l4_project.into_iter().chain(l4_global) {
            if !already_surfaced.contains(&entry.id) {
                already_surfaced.insert(entry.id);
                entries.push(entry);
            }
        }

        // ── Process L3 results: hybrid re-ranking ──
        let deep_entries = l3_result?;

        // ── Hybrid re-ranking: combine vector + BM25 scores ──
        let re_ranked = if !deep_entries.is_empty() {
            let vector_results: Vec<(String, String, f64)> = deep_entries
                .iter()
                .map(|e| (e.id.to_string(), e.content.clone(), e.confidence as f64))
                .collect();
            let all_docs: Vec<String> = deep_entries.iter().map(|e| e.content.clone()).collect();
            let doc_ids: Vec<String> = deep_entries.iter().map(|e| e.id.to_string()).collect();
            let hybrid_results = self.hybrid_searcher.search(
                query,
                vector_results,
                &all_docs,
                &doc_ids,
                memory_budget as usize,
            );
            // Re-order deep_entries by hybrid score
            let mut scored: Vec<(usize, f64)> = hybrid_results
                .iter()
                .filter_map(|r| {
                    deep_entries
                        .iter()
                        .position(|e| e.id.to_string() == r.id)
                        .map(|idx| (idx, r.hybrid_score))
                })
                .collect();
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            scored
                .into_iter()
                .take(memory_budget as usize)
                .map(|(idx, _)| deep_entries[idx].clone())
                .collect()
        } else {
            deep_entries
        };

        for e in &re_ranked {
            already_surfaced.insert(e.id);
        }
        entries.extend(re_ranked);

        // ── Process SessionResume results (after L3 to avoid &self conflict) ──
        match resume_result {
            Ok(resumed) => {
                for mut entry in resumed {
                    if !already_surfaced.contains(&entry.id) {
                        entry.content = format!("[SESSION RESUME] {}", entry.content);
                        entry.tags.push("session_resume".into());
                        already_surfaced.insert(entry.id);
                        entries.push(entry);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "session resume failed, continuing without it");
            }
        }

        // ── Session isolation filter (via ContextFence) ──
        if let Some(sid) = session_id {
            let fence = crate::context_fence::fence_from_session(sid, None, None);
            entries = crate::context_fence::filter_through_fence(&entries, &fence).into_iter().cloned().collect();
        }

        // ── Step 4: check seed triggers and inject as high-priority L1 entries ──
        let query_words: Vec<String> = query
            .split_whitespace()
            .map(str::to_lowercase)
            .collect();
        let triggered = {
            let mut reg = self
                .seeds
                .lock()
                ;
            reg.check_triggers("default", &query_words, Utc::now())
        };
        for seed in triggered {
            use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
            entries.push(MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L1,
                category: MemoryCategory::Reference,
                priority: Priority::High,
                source: MemorySource::Import,
                title: format!("Seed: {}", seed.name),
                content: seed.content,
                embedding: None,
                tags: vec!["seed".into()],
                relations: vec![],
                confidence: 1.0,
                access_count: 0,
                staleness: 0.0,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::default(),
                session_id: None,
                source_agent: None,
                visibility: crate::types::AgentVisibility::default(),
            });
            tracing::debug!(seed_id = %seed.id, "injected seed into context");
        }

        // ── Step 5: sample context window pressure ───────────────────────────
        let total_message_tokens: u64 = messages
            .iter()
            .map(|m| u64::from(m.token_estimate()))
            .sum();
        let total_entry_tokens: u64 = self.estimate_tokens_entries(&entries);
        let used_tokens = total_message_tokens + total_entry_tokens;
        let _monitor_snapshot = self.monitor.sample(used_tokens);

        // ── Step 5b: freshness-priority loading when budget is tight ─────────
        // When token usage exceeds 80% of the available budget, switch to
        // freshness-priority loading via FreshContextManager.
        let budget_usage_ratio = if budget.available > 0 {
            used_tokens as f32 / budget.available as f32
        } else {
            1.0
        };
        if budget_usage_ratio > self.config.tuning.freshness_trigger_ratio {
            tracing::info!(
                ratio = %budget_usage_ratio,
                used = %used_tokens,
                available = %budget.available,
                "freshness priority activated: budget > {:.0}%",
                self.config.tuning.freshness_trigger_ratio * 100.0
            );
            let entry_count = entries.len();
            entries = self
                .fresh_ctx
                .load_fresh_entries("cognitive-default", entries, entry_count)
                .await;
        }

        // ── Step 6: compress messages if necessary ──────────────────────────
        // Note: pipeline.run takes a mutable Vec; here we work non-destructively
        // by returning the final PreparedContext based on current state.
        // Full pipeline run is triggered in on_turn_end.

        // ── Step 6b: inject current swarm hot topics from L4 ────────────────
        let hot_topics = self.orchestrator.hot_topics(300).await;
        if !hot_topics.is_empty() {
            let topics_str = hot_topics.join(", ");
            entries.push(MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L4,
                category: MemoryCategory::Shared,
                priority: Priority::Low,
                source: MemorySource::AutoExtracted,
                title: "Swarm Hot Topics".into(),
                content: format!("Active swarm topics: {topics_str}"),
                embedding: None,
                tags: vec!["swarm".into(), "hot_topics".into()],
                relations: vec![],
                confidence: 0.8,
                access_count: 0,
                staleness: 0.0,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::default(),
                session_id: None,
                source_agent: None,
                visibility: crate::types::AgentVisibility::default(),
            });
        }

        // ── Step 7: auto-inject relevant code symbols (when applicable) ─────
        let code_context = if is_code_query(query) {
            let symbols = self
                .orchestrator
                .find_relevant_symbols(query, 5)
                .await;
            if symbols.is_empty() {
                None
            } else {
                Some(format_code_context(&symbols))
            }
        } else {
            None
        };

        // ── Step 7b: Tool output sandbox injection ──
        {
            let sandbox = self.tool_sandbox.lock();
            let count = sandbox.entry_count();
            if count > 0 {
                let snippets = sandbox.search_all(query, 3);
                for snip in snippets {
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L3,
                        category: MemoryCategory::Reference,
                        priority: Priority::Normal,
                        source: MemorySource::AutoExtracted,
                        title: format!("[SANDBOX] tool output L{}-L{}", snip.line_start, snip.line_end),
                        content: format!("[TOOL OUTPUT] {}", snip.content),
                        embedding: None,
                        tags: vec!["sandbox".into(), "tool_output".into()],
                        relations: vec![],
                        confidence: 0.7,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                }
            }
        }

        // ── Step 7c: Hot code symbol injection ──
        if let Some(hot_ctx) = self.orchestrator.get_hot_symbols_context() {
            entries.push(MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L1,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::AutoExtracted,
                title: "Hot Code Symbols".into(),
                content: hot_ctx,
                embedding: None,
                tags: vec!["hot_symbols".into(), "code".into()],
                relations: vec![],
                confidence: 0.9,
                access_count: 0, staleness: 0.0,
                created_at: Utc::now(), updated_at: Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::default(),
                session_id: None, source_agent: None,
                visibility: crate::types::AgentVisibility::default(),
            });
        }

        // ── Step 7d: Symbol-memory linking ──
        // Reserve for future: link code symbols referenced in context to memory entries.
        // This is activated when code_context is populated by the code indexer.
        if code_context.is_some() {
            tracing::debug!("symbol-memory linking: code context present, linking reserved for Phase 4 get_callers/get_callees integration");
        }

        // ── Assemble PreparedContext ─────────────────────────────────────────
        let total_tokens = self.estimate_tokens_entries(&entries);
        let depth_scale = if total_tokens > budget.available {
            budget.available as f32 / total_tokens.max(1) as f32
        } else {
            1.0
        };

        let elapsed_ms = _prepare_start.elapsed().as_millis();
        tracing::debug!(
            elapsed_ms,
            entries = entries.len(),
            total_tokens,
            "prepare_context complete"
        );

        Ok(PreparedContext {
            entries,
            total_tokens,
            budget,
            depth_scale,
            prepared_at: Utc::now(),
            code_context,
        })
    }

    /// Public entry point: prepare context with automatic code symbol injection.
    ///
    /// This wraps [`prepare_context`] and additionally injects relevant code
    /// symbols from the code indexer into [`PreparedContext::code_context`]
    /// when the query appears to be code-related.
    pub async fn build_context_with_code(
        &self,
        query: &str,
        messages: &[Message],
    ) -> Result<PreparedContext> {
        self.prepare_context(query, messages, None).await
    }

    // -----------------------------------------------------------------------
    // on_turn_end
    // -----------------------------------------------------------------------

    /// Called after each assistant turn to perform lightweight housekeeping.
    ///
    /// 1. Runs micro-compact on `messages` (in place).
    /// 2. Checks whether session-compact threshold is exceeded.
    /// 3. Runs drift detection on recently loaded entries.
    /// 4. Checks seed trigger conditions for turn-end keywords.
    /// 5. Persists vector index for durability.
    ///
    /// # Parallel-friendly decomposition
    ///
    /// This method is a sequential wrapper around three sub-operations which
    /// callers can also execute in parallel:
    ///
    /// | Step | Method                | Description                |
    /// |------|-----------------------|----------------------------|
    /// | 0    | `extract_and_remember`| Extract + persist memories |
    /// | 3-4  | `run_drift_and_seeds` | Drift detection + seeds    |
    /// | rest | (inline in on_turn_end)| Compaction, tick, KG, …  |
    ///
    /// [`run_memory_post_turn`] uses `tokio::join!` to run steps 0 and 3-4
    /// concurrently while keeping the remaining maintenance sequential.

    /// Extract memories from the current turn's messages and persist them.
    ///
    /// This covers steps 0, 0b, and 11 from the full turn-end sequence:
    ///   - Heuristic extraction from conversation messages (fast, sync)
    ///   - LLM extraction queued to background worker (non-blocking)
    ///   - Persist via `orchestrator.remember_batch`
    ///   - Index large tool outputs into the sandbox
    ///   - Batch-embed new entries into the vector index
    ///
    /// Failures are logged and swallowed so they never abort the turn.
    pub async fn extract_and_remember(&self, messages: &[Message]) -> Result<()> {
        // ── 0. Extract and persist memories ──────────────────────────────────
        let mut pending_embeddings: Vec<(MemoryId, String)> = Vec::new();
        if messages.len() >= 2 {
            tracing::debug!(
                messages_count = messages.len(),
                has_user = messages.iter().any(|m| matches!(m.role, MessageRole::User)),
                has_assistant = messages.iter().any(|m| matches!(m.role, MessageRole::Assistant)),
                has_tool = messages.iter().any(|m| matches!(m.role, MessageRole::Tool)),
                user_content_total = messages
                    .iter()
                    .filter(|m| matches!(m.role, MessageRole::User))
                    .map(|m| m.content.len())
                    .sum::<usize>(),
                "extract_and_remember: pre-extraction state"
            );

            // ── 0a. Drain background LLM results from prior turns ─────────
            {
                let mut pending = self.pending_llm_entries.lock();
                if !pending.is_empty() {
                    let drained: Vec<MemoryEntry> = pending.drain(..).collect();
                    drop(pending);
                    let drained_count = drained.len();
                    for entry in &drained {
                        pending_embeddings.push((entry.id, entry.content.clone()));
                    }
                    match self.orchestrator.remember_batch(drained).await {
                        Ok(_) => {
                            tracing::info!(
                                count = drained_count,
                                "extract_and_remember: persisted {} background LLM entries",
                                drained_count
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "extract_and_remember: background LLM entries persist failed"
                            );
                        }
                    }
                }
            }

            // ── 0b. Heuristic extraction (Passes 1-4, fast / non-blocking) ──
            let heuristic_entries = {
                let raw = self.extractor.extract_heuristic(messages);
                self.extractor.finalize_entries(raw)
            };
            if !heuristic_entries.is_empty() {
                tracing::info!(
                    entries_count = heuristic_entries.len(),
                    "extract_and_remember: heuristic extracted {} entries",
                    heuristic_entries.len()
                );
                for entry in &heuristic_entries {
                    pending_embeddings.push((entry.id, entry.content.clone()));
                }
                match self.orchestrator.remember_batch(heuristic_entries).await {
                    Ok(_) => {
                        tracing::debug!(
                            "extract_and_remember: heuristic memories persisted successfully"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "extract_and_remember: heuristic memory persistence failed"
                        );
                    }
                }

                // Queue LLM Pass 5 for background processing (non-blocking).
                if self.extractor.llm_client().is_some() {
                    let _ = self.extract_tx.send(messages.to_vec());
                    tracing::debug!("extract_and_remember: queued messages for background LLM extraction");
                }
            }

            // ── 0c. Index large tool outputs into sandbox ───────────────────
            let mut sandbox = self.tool_sandbox.lock();
            for msg in messages.iter().filter(|m| matches!(m.role, MessageRole::Tool)) {
                let call_id = msg.tool_use_id.as_deref().unwrap_or("unknown");
                let tool_name = msg.tool_name.as_deref().unwrap_or("unknown_tool");
                let threshold = self.config.tuning.sandbox_min_lines;
                if let Some(summary) = sandbox.index_tool_output(call_id, tool_name, &msg.content, threshold) {
                    tracing::info!(
                        call_id,
                        tool_name,
                        total_lines = summary.total_lines,
                        full_size = summary.full_size_bytes,
                        "tool_sandbox: indexed large tool output"
                    );
                }
            }
        } else {
            tracing::debug!(
                messages_count = messages.len(),
                "extract_and_remember: skipped (insufficient messages)"
            );
        }

        // ── 11. Batch-embed new entries ─────────────────────────────────────
        if !pending_embeddings.is_empty() {
            match &self.embedding_capability {
                EmbeddingCapability::Remote { client } => {
                    let texts: Vec<&str> = pending_embeddings
                        .iter()
                        .map(|(_, c)| c.as_str())
                        .collect();
                    match client.embed(&texts).await {
                        Ok(embeddings) => {
                            tracing::info!(
                                count = embeddings.len(),
                                "batch embedded {} entries",
                                embeddings.len()
                            );
                            let mut vi = self.vector_index.lock();
                            for ((id, _), embedding) in
                                pending_embeddings.iter().zip(embeddings.into_iter())
                            {
                                if let Err(e) = vi.upsert(*id, embedding) {
                                    tracing::warn!("batch embed upsert failed for {}: {}", id, e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("batch embedding failed: {}", e);
                        }
                    }
                }
                _ => {
                    tracing::debug!(
                        count = pending_embeddings.len(),
                        "skipping batch embed: no remote embedding client configured"
                    );
                }
            }
        }

        Ok(())
    }

    /// Run drift detection on L1 entries and check seed triggers at turn end.
    ///
    /// This covers steps 3 and 4 from the full turn-end sequence:
    ///   - Load essential layer entries, check each for staleness
    ///   - Prune stale entries via `orchestrator.forget`
    ///   - Check pre-authored seed trigger conditions against turn keywords
    ///
    /// Failures are logged and swallowed so they never abort the turn.
    pub async fn run_drift_and_seeds(&self, messages: &[Message]) -> Result<()> {
        // ── 3. Drift detection on L1 entries ────────────────────────────────
        let l1_entries = self.orchestrator.load_fixed_layers().await?;
        for entry in &l1_entries {
            match self.drift.check(entry) {
                crate::drift::DriftVerdict::Prune { reason } => {
                    tracing::debug!(
                        id = %entry.id,
                        reason = %reason,
                        "drift: pruning entry"
                    );
                    let _ = self.orchestrator.forget(&entry.id).await;
                }
                crate::drift::DriftVerdict::FlagForReview { reason } => {
                    tracing::debug!(
                        id = %entry.id,
                        reason = %reason,
                        "drift: entry flagged for review"
                    );
                }
                crate::drift::DriftVerdict::Ok => {}
            }
        }

        // ── 4. Check seed triggers at turn-end ──────────────────────────────
        let turn_keywords: Vec<String> = messages
            .iter()
            .flat_map(|m| m.content.split_whitespace().map(str::to_lowercase))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        {
            let mut reg = self.seeds.lock();
            reg.check_triggers("turn_end", &turn_keywords, Utc::now());
        }

        Ok(())
    }

    /// Run the full post-turn sequence.
    ///
    /// Convenience wrapper that calls [`extract_and_remember`],
    /// [`run_drift_and_seeds`], and [`run_memory_maintenance`] sequentially.
    ///
    /// For callers who want parallelism, combine the first two with
    /// `tokio::join!` and still call [`run_memory_maintenance`] after.
    pub async fn on_turn_end(&self, messages: &mut Vec<Message>) -> Result<()> {
        // ── Delegation observation ────────────────────────────────────────────
        {
            let drained: Vec<_> = {
                let mut delegation_queue = self.delegation_results.lock();
                delegation_queue.drain(..).collect()
            };
            for d in drained {
                let title = format!("delegation:{}:{}", d.agent_role, &d.task[..d.task.len().min(40)]);
                let content = format!("Agent: {}\nTask: {}\nResult: {}", d.agent_role, d.task, d.result);
                let tags = vec!["delegation".into(), d.agent_role.clone()];
                if let Err(e) = self.orchestrator.write(
                    MemoryLayer::L4, MemoryCategory::Shared, &title, &content,
                    Priority::Normal, MemorySource::AutoExtracted, tags, MemoryScope::default(),
                ).await {
                    tracing::warn!("delegation observation write failed: {e}");
                } else {
                    tracing::info!(agent_role = %d.agent_role, "delegation result written to L4");
                }
            }
        }

        // ── Extract ── Drift+Seeds ── Maintenance ──────────────────────
        let _ = self.extract_and_remember(messages).await;
        let _ = self.run_drift_and_seeds(messages).await;
        self.run_memory_maintenance(messages).await
    }

    /// Remaining post-turn maintenance: fact-checker, compaction, tick,
    /// KG persistence, context rotation, closet/seeds save, etc.
    ///
    /// Call this *after* `extract_and_remember` and `run_drift_and_seeds`
    /// have completed (whether sequentially or via `tokio::join!`).
    pub async fn run_memory_maintenance(&self, messages: &mut Vec<Message>) -> Result<()> {
        // ── 0c. Auto-correct contradictions via fact checker ──────────────
        {
            let mut fc = crate::orchestrator::get_fact_checker().lock();
            let report = fc.auto_correct();
            if report.corrected > 0 || report.pruned > 0 {
                tracing::info!(
                    corrected = report.corrected, pruned = report.pruned,
                    flagged = report.flagged, "auto-correction applied"
                );
            }
        }

        // ── 1. Micro compact ────────────────────────────────────────────────
        self.pipeline.micro_compact(messages);

        // ── 1b. AAAK compact ────────────────────────────────────────────────
        self.pipeline.aaak_compact(messages);

        // ── 2. Session compact if threshold exceeded ─────────────────────────
        if self.pipeline.should_session_compact(messages) {
            self.pipeline.session_compact(messages, &self.orchestrator).await?;
        }

        // ── 5. Run orchestrator maintenance tick ─────────────────────────────
        self.orchestrator.tick().await?;

        // ── 5a. Check project KG staleness and auto-rebuild if needed ───────
        if let Some(ref mgr) = self.project_scope_mgr {
            if let Some(proj_path) = self.project_kg_path.lock().as_ref() {
                let pid = crate::project_scope::hash_path(proj_path);
                if mgr.is_kg_stale(&pid).unwrap_or(false) {
                    tracing::info!("project KG is stale, auto-rebuilding...");
                    if let Err(e) = self.load_project_kg(proj_path) {
                        tracing::warn!("auto-rebuild of project KG failed: {e}");
                    }
                }
            }
        }

        // ── 5a2. Periodic KG rebuild every 100 ticks (T1) ───────────────────
        {
            let tick = self.kg_rebuild_tick_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if tick % 100 == 0 {
                if let Some(proj_path) = self.project_kg_path.lock().as_ref() {
                    tracing::info!(tick, path = %proj_path.display(), "periodic KG rebuild triggered (every 100 ticks)");
                    if let Err(e) = self.load_project_kg(proj_path) {
                        tracing::warn!("periodic KG rebuild failed: {e}");
                    } else {
                        tracing::debug!("periodic KG rebuild succeeded");
                    }
                }
            }
        }

        // ── 5a3. Cross-store consistency verification every 50 ticks (T2) ────
        {
            let tick = self.cross_store_verify_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if tick % 50 == 0 {
                let warnings = self.cross_store_verify().await;
                for w in &warnings { tracing::warn!("cross-store-verify: {w}"); }
                if !warnings.is_empty() {
                    tracing::warn!(count = warnings.len(), "cross-store consistency check found {} issues", warnings.len());
                }
            }
        }

        // ── 5a4. Integrity anomaly detection every 50 ticks (T9) ────────────
        {
            let tick = self.integrity_check_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if tick % 50 == 0 {
                if let Some(ref checker) = self.integrity_checker {
                    match checker.check_anomalies() {
                        Ok(report) => {
                            if !report.anomalies.is_empty() {
                                for anomaly in &report.anomalies {
                                    tracing::warn!("integrity anomaly detected: {:?}", anomaly);
                                }
                                tracing::warn!(count = report.anomalies.len(), "integrity check found {} anomaly(ies)", report.anomalies.len());
                            }
                        }
                        Err(e) => tracing::warn!("integrity check failed: {e}"),
                    }
                }
            }
        }

        // ── 5b. Auto-rebuild Closet periodically ────────────────────────────
        if self.orchestrator.should_rebuild_closet() {
            if let Err(e) = self.orchestrator.force_rebuild_closet().await {
                tracing::warn!("auto closet rebuild failed: {e}");
            }
        }

        // ── 6. Persist vector index ─────────────────────────────────────────
        if let Err(e) = self.vector_index.lock().persist() {
            tracing::warn!("failed to persist vector index: {}", e);
        }

        // ── 7. Persist knowledge graph (every 10 ticks) ──────────────────────
        {
            let (entities, triples): (Vec<_>, Vec<_>) = {
                let kg = self.kg.lock();
                let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
                let triples: Vec<_> = kg.list_triples().into_iter().cloned().collect();
                (entities, triples)
            };
            if !entities.is_empty() || !triples.is_empty() {
                if let Err(e) = self.orchestrator.store().save_entities(&entities).await {
                    tracing::warn!("failed to persist KG entities: {}", e);
                }
                if let Err(e) = self.orchestrator.store().save_triples(&triples).await {
                    tracing::warn!("failed to persist KG triples: {}", e);
                }
            }
        }

        // ── 8. Context rotation health check ────────────────────────────────
        {
            let total_tokens: u64 = messages.iter().map(|m| u64::from(m.token_estimate())).sum();
            let budget = self.compute_budget();
            let mut monitor = self.context_rot_monitor.lock();
            match monitor.check(total_tokens, budget.total) {
                crate::context_rot::RotAlert::Warning(msg) => tracing::warn!("{msg}"),
                crate::context_rot::RotAlert::Critical(msg) => tracing::error!("{msg}"),
                crate::context_rot::RotAlert::None => {}
            }
        }

        // ── 9. Save Closet to KV store ────────────────────────────────────────
        match ClosetManager::build_from_orchestrator(&self.orchestrator).await {
            Ok(manager) => {
                let pointers = &manager.closet().pointers;
                match serde_json::to_string(pointers) {
                    Ok(json) => {
                        if let Err(e) = self.orchestrator.store().kv_put("closet", &json).await {
                            tracing::warn!("failed to save closet: {}", e);
                        } else {
                            let mut closet_guard = self.closet.lock();
                            *closet_guard = Some(manager.closet().clone());
                        }
                    }
                    Err(e) => tracing::warn!("failed to serialize closet pointers: {}", e),
                }
            }
            Err(e) => tracing::warn!("failed to build closet: {}", e),
        }

        // ── 10. Save Seeds to KV store ──────────────────────────────────────
        {
            let reg = self.seeds.lock();
            match serde_json::to_string(reg.all_seeds()) {
                Ok(json) => {
                    if let Err(e) = self.orchestrator.store().kv_put("seeds", &json).await {
                        tracing::warn!("failed to save seeds: {}", e);
                    }
                }
                Err(e) => tracing::warn!("failed to serialize seeds: {}", e),
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // remember / recall
    // -----------------------------------------------------------------------

    /// Observe a child agent delegation result for later processing.
    ///
    /// Delegation results are queued and written to L4 in [`on_turn_end`].
    pub fn observe_delegation(
        &self,
        agent_role: &str,
        task: &str,
        result: &str,
        parent_session_id: Option<&str>,
    ) {
        let d = DelegationResult {
            agent_role: agent_role.to_string(),
            task: task.to_string(),
            result: result.to_string(),
            parent_session_id: parent_session_id.map(String::from),
            timestamp: Utc::now(),
        };
        let mut queue = self.delegation_results.lock();
        queue.push(d);
        tracing::debug!(
            agent_role = %agent_role,
            "delegation result queued"
        );
    }

    /// Write a memory entry to the appropriate layer.
    ///
    /// If a write guard is configured, the write is checked against the
    /// guard's layer permissions. Denied writes return
    /// [`MemoryError::WriteDenied`].
    pub async fn remember(&self, entry: MemoryEntry) -> Result<()> {
        // Check write guard
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self.write_guard.as_ref().map(|g| format!("{:?}", g.source())).unwrap_or_default(),
            });
        }

        // Audit log
        if policy.requires_audit() || self.audit_log.is_some() {
            if let Some(ref log) = self.audit_log {
                let _ = log.log(&AuditEntry {
                    timestamp: Utc::now(),
                    operation: AuditOperation::Create,
                    entry_id: entry.id.to_string(),
                    layer: format!("{:?}", entry.layer),
                    source: self.write_guard.as_ref().map(|g| g.source()).unwrap_or(WriteSource::System),
                    summary: truncate_summary(&entry.content, self.config.tuning.audit_truncate_len),
                    agent_id: None,
                    session_id: None,
                });
            }
        }

        self.orchestrator.remember(entry).await?;
        Ok(())
    }

    /// Recall memories by relevance to `query`, returning up to `limit` entries.
    pub async fn recall(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        let already_surfaced = HashSet::new();
        // Use a generous token budget so the limit parameter is the binding constraint.
        let token_budget = (limit as u32).saturating_mul(2000);
        let mut entries = self
            .orchestrator
            .recall_relevant(query, None, &already_surfaced, token_budget)
            .await?;
        entries.truncate(limit);
        Ok(entries)
    }

    /// List all memory entries in a specific layer.
    pub async fn list_layer_entries(
        &self,
        layer: crate::types::MemoryLayer,
    ) -> Result<Vec<crate::types::MemoryMeta>> {
        self.orchestrator.list_layer(layer).await
    }

    /// Get a single memory entry by ID.
    pub async fn get_entry(&self, id: &str) -> Result<Option<crate::types::MemoryEntry>> {
        let mem_id = match uuid::Uuid::try_parse(id) {
            Ok(id) => id,
            Err(_) => return Ok(None),
        };
        self.orchestrator.recall(&mem_id).await
    }

    /// Delete a memory entry by ID.
    ///
    /// If a write guard is configured, the delete is checked against the
    /// guard's layer permissions. Note: the layer must be inferred from the
    /// entry itself; if the entry is not found, the delete is still attempted
    /// (it will simply be a no-op).
    pub async fn delete_entry(&self, id: &str) -> Result<()> {
        let mem_id = match uuid::Uuid::try_parse(id) {
            Ok(id) => id,
            Err(_) => {
                return Err(crate::MemoryError::InvalidArgument(format!("invalid memory id: {id}")));
            }
        };

        // Try to look up the entry's layer for guard check
        if let Some(entry) = self.orchestrator.recall(&mem_id).await? {
            let policy = self.check_write_access(entry.layer);
            if !policy.is_allowed() {
                return Err(MemoryError::WriteDenied {
                    layer: format!("{:?}", entry.layer),
                    write_source: self.write_guard.as_ref().map(|g| format!("{:?}", g.source())).unwrap_or_default(),
                });
            }
            // Audit log for delete
            if policy.requires_audit() || self.audit_log.is_some() {
                if let Some(ref log) = self.audit_log {
                    let _ = log.log(&AuditEntry {
                        timestamp: Utc::now(),
                        operation: AuditOperation::Delete,
                        entry_id: id.to_string(),
                        layer: format!("{:?}", entry.layer),
                        source: self.write_guard.as_ref().map(|g| g.source()).unwrap_or(WriteSource::System),
                    summary: truncate_summary(&entry.content, self.config.tuning.audit_truncate_len),

                        agent_id: None,
                        session_id: None,
                    });
                }
            }
        }

        self.orchestrator.forget(&mem_id).await
    }

    /// Update a memory entry's content, tags, and/or priority.
    pub async fn update_entry(
        &self,
        id: &str,
        content: Option<String>,
        tags: Option<Vec<String>>,
        priority: Option<crate::types::Priority>,
    ) -> Result<()> {
        let mem_id = match uuid::Uuid::try_parse(id) {
            Ok(id) => id,
            Err(_) => {
                return Err(crate::MemoryError::InvalidArgument(format!("invalid memory id: {id}")));
            }
        };

        let mut entry = self.orchestrator.recall(&mem_id).await?
            .ok_or_else(|| crate::MemoryError::Store(format!("entry {} not found", id)))?;

        // Write guard check
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self.write_guard.as_ref().map(|g| format!("{:?}", g.source())).unwrap_or_default(),
            });
        }

        if let Some(c) = content {
            entry.content = c;
        }
        if let Some(t) = tags {
            entry.tags = t;
        }
        if let Some(p) = priority {
            entry.priority = p;
        }
        entry.updated_at = chrono::Utc::now();
        entry.staleness = 0.0;

        self.orchestrator.update(&entry).await
    }

    /// List all layers with their entry counts.
    pub async fn list_layers(&self) -> Vec<serde_json::Value> {
        use crate::types::MemoryLayer;
        let layers = [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
            MemoryLayer::L4,
        ];
        let mut result = Vec::new();
        for layer in layers {
            let metas = self.orchestrator.list_layer(layer).await.unwrap_or_default();
            result.push(serde_json::json!({
                "layer": format!("{layer:?}"),
                "entry_count": metas.len(),
            }));
        }
        result
    }

    // -----------------------------------------------------------------------
    // Vector Index Persistence
    // -----------------------------------------------------------------------

    /// Persist the vector index to disk for durability.
    ///
    /// This saves all embeddings to `blob_dir/vector_index.json`.
    /// Called automatically by [`on_turn_end`], but can be invoked manually
    /// for explicit checkpointing.
    pub fn persist_vector_index(&self) -> Result<()> {
        self.vector_index
            .lock()
            .persist()
            .map_err(|e| MemoryError::Store(format!("persist vector index: {e}")))
    }

    /// Get the number of vectors currently indexed.
    #[must_use]
    pub fn vector_index_count(&self) -> usize {
        self.vector_index.lock().count()
    }

    /// Get vector index statistics.
    #[must_use]
    pub fn vector_index_stats(&self) -> VectorIndexStats {
        VectorIndexStats {
            count: self.vector_index.lock().count(),
        }
    }

    /// Return the current embedding capability level.
    #[must_use]
    pub fn embedding_capability(&self) -> &EmbeddingCapability {
        &self.embedding_capability
    }

    /// Return the search mode label for the current embedding capability.
    #[must_use]
    pub fn search_mode_label(&self) -> &'static str {
        self.embedding_capability.search_mode_label()
    }

    /// Access the pre-built BM25 session resume index, if available.
    ///
    /// The index is built at construction time from all persisted entries.
    #[must_use]
    pub fn session_resume(&self) -> Option<&SessionResume> {
        self.session_resume.as_ref()
    }

    // -----------------------------------------------------------------------
    // FTS5 Full-text search
    // -----------------------------------------------------------------------

    /// Perform full-text search across memories using FTS5.
    ///
    /// This method provides Hermes-Agent sessions-style FTS5 indexing with:
    /// - Category and layer filtering
    /// - Highlighted snippets for context
    /// - Matched keywords extraction
    /// - Both simple and boolean query modes
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let request = SearchMemoriesRequest {
    ///     query: "Rust async programming".to_string(),
    ///     category: Some(MemoryCategory::ProjectConvention),
    ///     limit: 5,
    ///     with_snippets: true,
    ///     with_keywords: true,
    ///     ..Default::default()
    /// };
    /// let result = manager.search_memories(request).await?;
    /// for (entry, snippet) in result.entries.iter().zip(result.snippets.iter()) {
    ///     println!("Title: {}", entry.title);
    ///     if let Some(snippet) = snippet {
    ///         println!("Snippet: {}", snippet.text);
    ///     }
    /// }
    /// ```
    pub async fn search_memories(
        &self,
        request: SearchMemoriesRequest,
    ) -> Result<SearchMemoriesResult> {
        // Build FTS5 query based on search mode
        let fts_query = match request.mode {
            SearchMode::Match => prepare_fts_query(&request.query),
            SearchMode::Boolean => request.query.clone(),
            SearchMode::Prefix => request
                .query
                .split_whitespace()
                .map(|w| format!("{}*", w))
                .collect::<Vec<_>>()
                .join(" "),
        };

        // Build search options
        let options = FtsSearchOptions {
            category: request.category,
            layer: request.layer,
            with_snippets: request.with_snippets,
            with_keywords: request.with_keywords,
        };

        // Execute search through the orchestrator's store
        let fts_result: FtsSearchResult = self
            .orchestrator
            .store()
            .search_fts_advanced(&fts_query, options, request.limit)
            .await?;

        // Convert snippets to SearchSnippet format
        let snippets: Vec<Option<SearchSnippet>> = fts_result
            .snippets
            .into_iter()
            .map(|opt| {
                opt.map(|text| SearchSnippet {
                    text,
                    positions: vec![],
                })
            })
            .collect();

        // Convert keywords
        let keywords: Vec<MatchedKeyword> = fts_result
            .keywords
            .into_iter()
            .map(|(keyword, count)| MatchedKeyword {
                keyword,
                count: count as u32,
            })
            .collect();

        // Collect unique categories found in results
        use std::collections::HashSet;
        let categories_found_set: HashSet<_> = fts_result
            .entries
            .iter()
            .map(|e| e.category)
            .collect();
        let categories_found: Vec<_> = categories_found_set.into_iter().collect();

        Ok(SearchMemoriesResult {
            entries: fts_result.entries,
            snippets,
            keywords,
            total_matches: fts_result.total_matches,
            query: request.query,
            categories_found,
            search_mode: self.search_mode_label().to_string(),
        })
    }

    /// Quick FTS5 search with just a query string.
    ///
    /// Convenience method that creates a default request with the given query.
    pub async fn search(&self, query: &str) -> Result<Vec<MemoryEntry>> {
        let request = SearchMemoriesRequest {
            query: query.to_string(),
            ..Default::default()
        };
        let result = self.search_memories(request).await?;
        Ok(result.entries)
    }

    // -----------------------------------------------------------------------
    // Handoff
    // -----------------------------------------------------------------------

    /// Serialise the current session state into a [`HandoffData`] packet ready
    /// for cross-session resumption.
    pub async fn create_handoff(&self) -> Result<HandoffData> {
        let session_id = uuid::Uuid::new_v4().to_string();

        // Gather recent work items from L1 memories
        let recent = self
            .orchestrator
            .list_layer(MemoryLayer::L1)
            .await
            .unwrap_or_default();
        let work_items: Vec<WorkItem> = recent
            .iter()
            .map(|e| WorkItem {
                id: e.id.to_string(),
                title: e.title.clone(),
                description: e.title.clone(),
                status: WorkItemStatus::Pending,
                priority: e.priority,
            })
            .take(10)
            .collect();

        // Gather decisions from the decision thread store
        let decisions: Vec<Decision> = {
            let store = self.decisions.lock();
            let topics: Vec<String> = store
                .list_threads()
                .into_iter()
                .map(|s| s.to_owned())
                .collect();
            let mut result = Vec::new();
            for topic in &topics {
                if let Some(thread) = store.get_thread(topic) {
                    for entry in &thread.entries {
                        result.push(Decision {
                            id: entry.id.clone(),
                            summary: entry.summary.clone(),
                            rationale: entry.rationale.clone(),
                            status: entry.status,
                            made_at: entry.made_at,
                        });
                    }
                }
            }
            result
        };

        // Gather blockers from the tracked list
        let blockers: Vec<Blocker> = {
            let list = self.blockers.lock();
            list.iter()
                .enumerate()
                .map(|(i, desc)| Blocker {
                    id: format!("blocker-{i}"),
                    description: desc.clone(),
                    resolution_hint: None,
                })
                .collect()
        };

        // Build summary from last_action
        let last_action = self
            .last_action
            .lock()
            .clone();
        let context_notes = format!(
            "Last action: {}. Session has {} memories and {} decisions logged.",
            last_action.as_deref().unwrap_or("none"),
            recent.len(),
            decisions.len(),
        );

        let handoff = self.handoff_mgr.create_handoff(
            &session_id,
            None, // current_task — not tracked yet
            work_items,
            vec![],      // remaining items
            decisions,
            blockers,
            last_action.as_deref().unwrap_or(""),
            &context_notes,
        )?;
        self.handoff_mgr.save(&handoff)?;
        Ok(handoff)
    }

    /// Restore session state from a previously created [`HandoffData`] packet.
    pub async fn restore_handoff(&self, data: HandoffData) -> Result<()> {
        self.handoff_mgr.resume(data).await
    }

    // -----------------------------------------------------------------------
    // Session Restoration
    // -----------------------------------------------------------------------

    /// Restore memories from session history.
    ///
    /// This method reads the session history from `session_path` and extracts:
    /// - Memory entries from compressed messages
    /// - Decisions from decision messages
    /// - Work items from task-related messages
    ///
    /// Returns statistics about what was restored.
    pub async fn restore_from_session(
        &self,
        session_path: &std::path::Path,
        session_id: &str,
    ) -> Result<SessionRestoreStats> {
        use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};

        let mut stats = SessionRestoreStats::default();

        // Try to load the session file
        let contents = match std::fs::read_to_string(session_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("failed to read session file: {}", e);
                return Ok(stats);
            }
        };

        // Parse JSON or JSONL
        let messages: Vec<serde_json::Value> = if contents.trim().starts_with('{') {
            // Single JSON object
            match serde_json::from_str::<serde_json::Value>(&contents) {
                Ok(v) if v.get("messages").is_some() => {
                    v.get("messages")
                        .and_then(|m: &serde_json::Value| m.as_array())
                        .cloned()
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            }
        } else {
            // JSONL format - one JSON object per line
            contents
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter_map(|v: serde_json::Value| {
                    v.get("message")
                        .or_else(|| v.get("content"))
                        .cloned()
                })
                .collect()
        };

        // Extract memories from messages
        for msg in messages {
            // Try to extract content from message
            let text_opt: Option<String> = msg.as_str().map(String::from)
                .or_else(|| msg.get("text").and_then(|v: &serde_json::Value| v.as_str()).map(String::from))
                .or_else(|| msg.get("content").and_then(|v: &serde_json::Value| v.as_str()).map(String::from));

            if let Some(text) = text_opt {
                // Skip very short messages
                if text.len() < 50 {
                    continue;
                }

                // Extract title from first line or truncate
                let first_line = text.lines().next().unwrap_or("");
                let title = if first_line.len() > 60 {
                    format!("{}...", &first_line[..60])
                } else {
                    first_line.to_string()
                };

                // Create memory entry for this message
                let entry = MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L3, // Deep layer for restored memories
                    category: MemoryCategory::CompressedSummary,
                    priority: Priority::Normal,
                    source: MemorySource::Import,
                    title,
                    content: text.clone(),
                    embedding: None,
                    tags: vec!["restored".into(), "session".into(), session_id.into()],
                    relations: vec![],
                    confidence: 0.7, // Lower confidence for restored content
                    access_count: 0,
                    staleness: 0.0,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: Some(session_id.to_string()),
                    source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                };

                if let Err(e) = self.orchestrator.remember(entry).await {
                    tracing::warn!("failed to restore memory: {}", e);
                } else {
                    stats.memories_restored += 1;
                }
            }

            // Try to extract decisions
            if let Some(content_obj) = msg.get("content").and_then(|v: &serde_json::Value| v.as_object()) {
                if content_obj.contains_key("decision") || content_obj.contains_key("rationale") {
                    stats.decisions_restored += 1;
                }
            }
        }

        tracing::info!(
            "restored {} memories from session {}",
            stats.memories_restored,
            session_id
        );

        Ok(stats)
    }

    // -----------------------------------------------------------------------
    // Decision threads
    // -----------------------------------------------------------------------

    /// Record a decision entry into `thread_id`'s decision thread.
    ///
    /// If the thread does not yet exist it is created automatically.
    pub fn record_decision(&self, thread_id: &str, decision: DecisionEntry) -> Result<()> {
        let mut store = self
            .decisions
            .lock()
            ;

        // Ensure the thread exists.
        store.create_thread(thread_id);

        // Append the entry using the record() compatibility API.
        store.record(
            thread_id,
            decision.summary,
            decision.rationale,
            decision.alternatives,
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Build a [`TokenBudget`] from the current config, allocating by agent role.
    ///
    /// Role multipliers: Planner=0.40, Executor=0.25, Reviewer=0.15, Orchestrator=0.50.
    /// Unknown roles default to Orchestrator (0.50).
    fn compute_budget(&self) -> TokenBudget {
        let role = self
            .current_agent
            .lock()
            .clone()
            .unwrap_or_else(|| "Orchestrator".to_string());
        BudgetCalculator::new(self.config.budget.clone()).make_role_budget(&role)
    }

    /// Verify cross-store consistency: KG ↔ MemoryStore ↔ Verbatim ↔ Closet.
    ///
    /// Samples 10 random entries from each store and checks for referential
    /// integrity. Returns a list of warning strings. Kept lightweight (<10ms).
    async fn cross_store_verify(&self) -> Vec<String> {
        let mut warnings = Vec::new();

        // 1. KG entities → MemoryStore: check a random sample of KG entities
        //    have corresponding MemoryStore entries.
        {
            let entities: Vec<_> = {
                let kg = self.kg.lock();
                kg.list_entities().into_iter().cloned().collect()
            }; // kg dropped before any .await
            let sample_size = 10usize.min(entities.len());
            if sample_size > 0 {
                let store = self.orchestrator.store();
                // Use a deterministic pseudo-random subset via modulo hash on entity id
                let step = (entities.len() / sample_size).max(1);
                let mut checked = 0usize;
                for (i, entity) in entities.iter().enumerate() {
                    if i % step != 0 || checked >= sample_size {
                        continue;
                    }
                    checked += 1;
                    // Check if entity name appears in store via FTS
                    let found = store.search_fts(&entity.name, 1).await;
                    match found {
                        Ok(results) if results.is_empty() => {
                            warnings.push(format!(
                                "kg-orphan: entity '{}' ({}) not found in MemoryStore FTS",
                                entity.name, entity.id
                            ));
                        }
                        Err(e) => {
                            warnings.push(format!(
                                "kg-orphan-check: entity '{}' FTS query failed: {e}",
                                entity.name
                            ));
                        }
                        _ => {} // OK
                    }
                }
            }
        }

        // 2. Closet pointers → MemoryStore: check a random sample of drawer_ids
        //    exist in MemoryStore.
        {
            let sampled_ids: Vec<String> = {
                let closet_guard = self.closet.lock();
                if let Some(ref closet) = *closet_guard {
                    let all_ids: Vec<&str> = closet
                        .pointers
                        .iter()
                        .flat_map(|p| p.drawer_ids.iter().map(String::as_str))
                        .collect();
                    let sample_size = 10usize.min(all_ids.len());
                    if sample_size > 0 {
                        let step = (all_ids.len() / sample_size).max(1);
                        let mut result = Vec::new();
                        let mut checked = 0usize;
                        for (i, drawer_id) in all_ids.iter().enumerate() {
                            if i % step != 0 || checked >= sample_size {
                                continue;
                            }
                            checked += 1;
                            result.push(drawer_id.to_string());
                        }
                        result
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            }; // closet_guard dropped before any .await
            if !sampled_ids.is_empty() {
                let store = self.orchestrator.store();
                for drawer_id in &sampled_ids {
                    let uuid = match uuid::Uuid::parse_str(drawer_id) {
                        Ok(id) => id,
                        Err(_) => continue,
                    };
                    let found = store.get(&uuid).await;
                    match found {
                        Ok(None) => {
                            warnings.push(format!(
                                "closet-orphan: drawer_id '{drawer_id}' not found in MemoryStore"
                            ));
                        }
                        Err(e) => {
                            warnings.push(format!(
                                "closet-orphan-check: drawer_id '{drawer_id}' get failed: {e}"
                            ));
                        }
                        _ => {} // OK
                    }
                }
            }
        }

        // 3. Verbatim ↔ MemoryStore: sample MemoryStore entries and check
        //    verbatim counterparts exist (reverse check since we can't list verbatim).
        {
            let store = self.orchestrator.store();
            let all_entries = store.list_all().await;
            match all_entries {
                Ok(entries) if !entries.is_empty() => {
                    let sample_size = 10usize.min(entries.len());
                    let step = (entries.len() / sample_size).max(1);
                    let mut checked = 0usize;
                    for (i, entry) in entries.iter().enumerate() {
                        if i % step != 0 || checked >= sample_size {
                            continue;
                        }
                        checked += 1;
                        let verbatim = store.load_verbatim_by_id(&entry.id.to_string()).await;
                        match verbatim {
                            Ok(None) => {
                                warnings.push(format!(
                                    "verbatim-missing: MemoryStore entry {} has no Verbatim counterpart",
                                    entry.id
                                ));
                            }
                            Err(e) => {
                                warnings.push(format!(
                                    "verbatim-check: entry {} verbatim load failed: {e}",
                                    entry.id
                                ));
                            }
                            _ => {} // OK
                        }
                    }
                }
                Ok(_) => {} // empty store, nothing to verify
                Err(e) => {
                    warnings.push(format!("cross-store-verify: list_all failed: {e}"));
                }
            }
        }

        // 4. Coherence check: verify KG entity names appear in at least one
        //    MemoryStore entry with a minimum Jaccard similarity.
        {
            let entities: Vec<_> = {
                let kg = self.kg.lock();
                kg.list_entities().into_iter().cloned().collect()
            }; // kg dropped before any .await
            if !entities.is_empty() {
                let sample_size = 5usize.min(entities.len());
                let step = (entities.len() / sample_size).max(1);
                let store = self.orchestrator.store();
                let mut checked = 0usize;
                for (i, entity) in entities.iter().enumerate() {
                    if i % step != 0 || checked >= sample_size {
                        continue;
                    }
                    checked += 1;
                    let results = store.search_fts(&entity.name, 3).await;
                    match results {
                        Ok(entries) => {
                            let has_relevant = entries.iter().any(|e| {
                                coherence::jaccard_similarity(&entity.name, &e.content) > 0.1
                            });
                            if !has_relevant && !entries.is_empty() {
                                warnings.push(format!(
                                    "coherence-low: entity '{}' has no relevant MemoryStore entry (best checked: {})",
                                    entity.name,
                                    entries.first().map(|e| e.title.as_str()).unwrap_or("none")
                                ));
                            }
                        }
                        Err(e) => {
                            warnings.push(format!(
                                "coherence-check: entity '{}' FTS search failed: {e}",
                                entity.name
                            ));
                        }
                    }
                }
            }
        }

        warnings
    }

    /// Approximate token count for a slice of memory entries (chars / 4).
    fn estimate_tokens_entries(&self, entries: &[MemoryEntry]) -> u64 {
        entries
            .iter()
            .map(|e| (e.content.len() as u64).div_ceil(4))
            .sum()
    }

    // -----------------------------------------------------------------------
    // Agent self-aware diagnostics
    // -----------------------------------------------------------------------

    /// Return the current context window health as a [`RotAlert`].
    ///
    /// This is a **read-only** diagnostic — it does not modify any internal
    /// state, trigger debounce logic, or update counters.  Callers can use
    /// this for agent-facing health checks without side effects.
    ///
    /// The method reads the stored `context_usage_ratio` from the
    /// [`ContextRotMonitor`] metrics and maps it to the appropriate alert
    /// level:
    ///
    /// | Ratio      | Alert    |
    /// |------------|----------|
    /// | ≤ 0.65     | `None`   |
    /// | 0.65–0.75  | `Warning`|
    /// | > 0.75     | `Critical`|
    #[must_use]
    pub fn ctx_health(&self) -> RotAlert {
        let monitor = self.context_rot_monitor.lock();
        let ratio = monitor.metrics.context_usage_ratio;
        let total = self.config.budget.context_window as u64;
        let used = (ratio * total as f32) as u64;

        if ratio > 0.75 {
            RotAlert::Critical(format!(
                "⚠ CONTEXT ROT: {:.1}% usage ({} / {} tokens). Auto-record session state.",
                ratio * 100.0,
                used,
                total
            ))
        } else if ratio > 0.65 {
            RotAlert::Warning(format!(
                "⚠ Context usage at {:.1}% — inject agent-facing message.",
                ratio * 100.0
            ))
        } else {
            RotAlert::None
        }
    }
}

// ---------------------------------------------------------------------------
// FTS5 Query Helpers
// ---------------------------------------------------------------------------

/// Statistics about the vector index.
#[derive(Debug, Clone)]
pub struct VectorIndexStats {
    pub count: usize,
}

/// Prepare a query string for FTS5 MATCH by escaping special characters.
///
/// FTS5 special characters include: `"`, `'`, `(`, `)`, `*`, `:`, `^`, `-`, `+`
fn prepare_fts_query(query: &str) -> String {
    // Split into words, escape each, rejoin with implicit AND
    query
        .split_whitespace()
        .map(|word| {
            // Skip FTS5 operators
            if matches!(word.to_uppercase().as_str(), "AND" | "OR" | "NOT" | "NEAR") {
                word.to_string()
            } else {
                // Escape double quotes
                word.replace('"', "\"\"")
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Truncate content to a short summary for audit logging (privacy-preserving).
fn truncate_summary(content: &str, max_len: usize) -> String {
    if content.len() <= max_len {
        content.to_string()
    } else {
        format!("{}...", &content[..max_len])
    }
}

// ---------------------------------------------------------------------------
// Code context injection helpers
// ---------------------------------------------------------------------------

/// Heuristic to detect whether a user query is code-related.
///
/// Returns `true` if the query contains file extensions (`.rs`, `.py`, `.ts`, etc.)
/// or code-related keywords (`function`, `class`, `bug`, `fix`, `struct`, etc.).
fn is_code_query(query: &str) -> bool {
    let lower = query.to_lowercase();
    let code_extensions = [".rs", ".py", ".ts", ".tsx", ".go", ".java", ".js", ".cpp", ".h"];
    let code_keywords = [
        "function", "class", "bug", "fix", "struct", "interface", "enum",
        "fn ", "impl", "trait", "module", "import", "def ", "async", "await",
        "refactor", "compile", "compiler", "syntax", "type", "error", "warning",
        "unwra", "panic", "debug", "trace", "cargo", "npm", "node", "runtime",
    ];

    code_extensions.iter().any(|ext| lower.contains(ext))
        || code_keywords.iter().any(|kw| lower.contains(kw))
}

/// Format a list of code symbols into a context block for LLM injection.
///
/// Output format:
/// ```text
/// ## Relevant Code Symbols
/// - authenticate_user (src/auth.rs:42) — validates JWT token
///   Kind: Function
/// - MyService (src/service.rs:15) — service class
///   Kind: Class
/// ```
fn format_code_context(symbols: &[CodeSymbol]) -> String {
    let mut lines = vec!["## Relevant Code Symbols".to_string()];
    for sym in symbols {
        let desc = sym
            .doc
            .as_deref()
            .unwrap_or(&sym.signature)
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        let desc_short = if desc.len() > 80 {
            format!("{}...", &desc[..77])
        } else {
            desc
        };
        lines.push(format!(
            "- {} ({}:{}) — {}",
            sym.name, sym.file_path, sym.line, desc_short
        ));
        lines.push(format!("  Kind: {}", sym.kind.as_str()));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BudgetConfig, MemoryConfig};
    use crate::types::MemoryLayer;
    use crate::write_guard::WriteSource;

    fn test_config() -> MemoryConfig {
        MemoryConfig {
            budget: BudgetConfig { context_window: 8000, reserved_system: 2000, reserved_response: 1000, ..Default::default() },
            ..Default::default()
        }
    }

    #[test]
    fn truncate_summary_short_content_unchanged() {
        assert_eq!(truncate_summary("hello", 100), "hello");
    }

    #[test]
    fn truncate_summary_long_content_cut() {
        assert_eq!(truncate_summary(&"a".repeat(200), 10), "aaaaaaaaaa...");
    }

    #[test]
    fn truncate_summary_exact_length() {
        assert_eq!(truncate_summary("hello", 5), "hello");
    }

    #[tokio::test]
    async fn new_constructs_with_default_config() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        assert_eq!(mgr.search_mode_label(), "keyword");
        assert_eq!(mgr.vector_index_count(), 0);
    }

    #[tokio::test]
    async fn with_write_source_configures_guard() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap()
            .with_write_source(WriteSource::System);
        let policy = mgr.check_write_access(MemoryLayer::L1);
        assert!(policy.is_allowed());
    }

    #[tokio::test]
    async fn list_layers_returns_info() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let layers = mgr.list_layers().await;
        assert!(!layers.is_empty());
    }

    #[tokio::test]
    async fn embedding_capability_defaults_fts5_only() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        assert!(!mgr.embedding_capability().supports_semantic());
    }

    #[tokio::test]
    async fn vector_index_stats_empty() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        assert_eq!(mgr.vector_index_stats().count, 0);
    }

    // -----------------------------------------------------------------------
    // T7: Code context injection tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_code_query_rust_file() {
        assert!(is_code_query("fix bug in src/main.rs"));
        assert!(is_code_query("how does this function work?"));
        assert!(is_code_query("refactor the auth class"));
        assert!(is_code_query("add a new struct for user"));
        assert!(is_code_query("cargo build error"));
    }

    #[test]
    fn test_is_code_query_non_code() {
        assert!(!is_code_query("hello world"));
        assert!(!is_code_query("what is the weather today?"));
        assert!(!is_code_query("tell me a joke"));
        assert!(!is_code_query("create a summary of the meeting"));
        assert!(!is_code_query(""));
    }

    #[test]
    fn test_format_code_context() {
        let symbols = vec![
            CodeSymbol {
                id: "src/auth.rs:authenticate_user:42".into(),
                name: "authenticate_user".into(),
                kind: crate::code_indexer::SymbolKind::Function,
                file_path: "src/auth.rs".into(),
                line: 42,
                signature: "pub fn authenticate_user(token: &str) -> Result<User>".into(),
                doc: Some("validates JWT token and returns user".into()),
            },
            CodeSymbol {
                id: "src/service.rs:MyService:15".into(),
                name: "MyService".into(),
                kind: crate::code_indexer::SymbolKind::Class,
                file_path: "src/service.rs".into(),
                line: 15,
                signature: "class MyService { ... }".into(),
                doc: None,
            },
        ];

        let context = format_code_context(&symbols);
        assert!(context.contains("## Relevant Code Symbols"));
        assert!(context.contains("authenticate_user"));
        assert!(context.contains("src/auth.rs:42"));
        assert!(context.contains("validates JWT token"));
        assert!(context.contains("Kind: Function"));
        assert!(context.contains("MyService"));
        assert!(context.contains("Kind: Class"));
    }

    #[test]
    fn test_format_code_context_empty() {
        let context = format_code_context(&[]);
        assert_eq!(context, "## Relevant Code Symbols");
    }

    #[tokio::test]
    async fn test_auto_inject_on_code_query() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let query = "fix bug in src/auth.rs";
        let ctx = mgr.prepare_context(query, &[], None).await.unwrap();

        // code_context may be None (no code indexer in test config) or Some
        // This test primarily validates the pipeline doesn't crash
        assert_eq!(ctx.entries.len(), 0); // empty project has no entries
    }

    #[tokio::test]
    async fn test_no_inject_on_non_code_query() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let query = "tell me a joke";
        let ctx = mgr.prepare_context(query, &[], None).await.unwrap();

        // code_context should be None for non-code queries
        assert!(ctx.code_context.is_none());
    }

    #[tokio::test]
    async fn test_build_context_with_code_delegates() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let ctx = mgr.build_context_with_code("hello", &[]).await.unwrap();

        // build_context_with_code wraps prepare_context
        assert!(ctx.code_context.is_none()); // non-code query
    }
}
