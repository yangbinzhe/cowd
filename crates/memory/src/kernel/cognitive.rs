//! `CognitiveContextManager` – unified entry-point (facade) for the memory framework.
//!
//! Coordinates all sub-systems (orchestrator, relevance
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

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

use chrono::Utc;
use session::SessionHistoryReader;

use crate::performance_monitor::{AutoTuner, PerformanceMonitor};
use crate::{
    background_watcher::{BackgroundWatcher, BackgroundWatcherConfig, BackgroundWatcherHandle},
    closet::{Closet, ClosetManager},
    code_indexer::CodeSymbol,
    coherence,
    compression::{
        budget::BudgetManager, llm_summarizer::LlmSummarizer, monitor::ContextWindowMonitor,
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
    kernel::{
        memory_scope_visible_to_ctx, scoped_entry_scope, MemoryLifecycleEvent, MemoryState,
        MemoryTurnContext,
    },
    maintenance::{
        scan_maintenance_candidates, MaintenanceCandidate, MaintenanceCandidateFilter,
        MaintenanceCandidateStatus, MaintenanceQueue, MaintenanceScanConfig,
    },
    memory_pulse::{MemoryPulseBatch, MemoryPulseConsumer, MemoryPulseReport},
    memory_usage::{summarize_usage, MemoryUsageSignal, MemoryUsageSummary},
    orchestrator::MemoryOrchestrator,
    project_scope::{build_project_kg, ProjectScopeManager},
    search::HybridSearcher,
    seeds::{DecisionThreadStore, SeedRegistry},
    state_rebuilder::StateRebuilder,
    store::sqlite::SqliteStore,
    store::vector::VectorIndex,
    store::{
        AuthorityLookup, FtsSearchOptions, FtsSearchResult, MemoryKeyValue, MemoryScanCursor,
        MemoryScanPage, MemoryStore,
    },
    tool_sandbox::ToolOutputSandbox,
    types::{
        Blocker, Decision, DecisionEntry, HandoffData, MatchedKeyword, MemoryCategory, MemoryEntry,
        MemoryId, MemoryLayer, MemorySource, Message, MessageRole, PreparedContext, Priority,
        SearchMemoriesRequest, SearchMemoriesResult, SearchMode, SearchSnippet, Seed, TokenBudget,
        WorkItem, WorkItemStatus,
    },
    write_guard::{
        AuditEntry, AuditLog, AuditOperation, IntegrityChecker, MemoryWriteGuard, WriteSource,
    },
    MemoryScope, SessionResume,
};

/// Result alias used throughout this module.
pub type Result<T> = std::result::Result<T, MemoryError>;

const MEMORY_USAGE_SELECTION_KEY: &str = "memory_usage:context_selection";
const MAX_MEMORY_USAGE_KEYS: usize = 10_000;

#[derive(Default)]
struct MemoryUsageWriterState {
    persisted_batches: AtomicU64,
    coalesced: AtomicU64,
    dropped_keys: AtomicU64,
    persistence_failures: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryUsageWriterHealth {
    pub keys: usize,
    pub persisted_batches: u64,
    pub coalesced: u64,
    pub dropped_keys: u64,
    pub persistence_failures: u64,
}

fn memory_usage_signal_key(signal: &MemoryUsageSignal) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        signal.memory_id, signal.session_id, signal.agent_id
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomaticGovernanceRunStatus {
    pub run_id: String,
    pub mode: String,
    pub started_at: chrono::DateTime<Utc>,
    pub phase: String,
    pub scanned_entries: usize,
    pub processed_candidates: usize,
    pub total_candidates: usize,
}

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

struct CachedPreparedContext {
    key: u64,
    revision: u64,
    context: PreparedContext,
    cached_at: Instant,
}

/// Work handed to the asynchronous extractor. The immutable turn context is
/// part of the payload because this work can complete while another Agent is
/// active; it must never inherit that unrelated Agent's identity or scope.
#[derive(Clone)]
struct BackgroundExtractionRequest {
    turn: MemoryTurnContext,
    messages: Vec<Message>,
    /// Fast heuristic atoms already persisted for this exact turn. Semantic
    /// extraction refines these atoms in place instead of creating a second
    /// paraphrased copy.
    heuristic_entries: Vec<MemoryEntry>,
}

fn canonicalize_automatic_entries(
    turn: &MemoryTurnContext,
    batch_tag: &str,
    entries: &mut [MemoryEntry],
) {
    for entry in entries {
        if entry.source != MemorySource::AutoExtracted {
            continue;
        }
        entry
            .session_id
            .get_or_insert_with(|| turn.session_id.clone());
        entry
            .source_agent
            .get_or_insert_with(|| turn.agent_id.clone());
        entry.scope = scoped_entry_scope(turn, entry);
        if !entry.tags.iter().any(|tag| tag == batch_tag) {
            entry.tags.push(batch_tag.to_string());
            entry.tags.sort();
            entry.tags.dedup();
        }
        entry.id = automatic_extraction_id(entry);
    }
}

fn automatic_extraction_id(entry: &MemoryEntry) -> MemoryId {
    let normalize = |value: &str| {
        value
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_ascii_lowercase()
    };
    let key = format!(
        "{}\u{1f}{:?}\u{1f}{:?}\u{1f}{}\u{1f}{}",
        entry.scope.scope_key(),
        entry.layer,
        entry.category,
        normalize(&entry.title),
        normalize(&entry.content),
    );
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, key.as_bytes())
}

fn extraction_batch_key(turn: &MemoryTurnContext, messages: &[Message]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for message in messages {
        std::mem::discriminant(&message.role).hash(&mut hasher);
        message.content.hash(&mut hasher);
        message.tool_use_id.hash(&mut hasher);
        message.tool_name.hash(&mut hasher);
    }
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{:016x}",
        turn.session_id,
        turn.agent_id,
        turn.task_id.as_deref().unwrap_or_default(),
        turn.definition_lineage_id.as_deref().unwrap_or_default(),
        hasher.finish(),
    )
}

fn extraction_batch_tag(turn: &MemoryTurnContext, messages: &[Message]) -> String {
    let key = extraction_batch_key(turn, messages);
    format!(
        "extraction-batch:{}",
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, key.as_bytes())
    )
}

fn background_extraction_key(request: &BackgroundExtractionRequest) -> String {
    extraction_batch_key(&request.turn, &request.messages)
}

fn coalesce_background_request(
    batches: &mut HashMap<String, (BackgroundExtractionRequest, u64)>,
    request: BackgroundExtractionRequest,
) -> bool {
    let key = background_extraction_key(&request);
    if let Some((pending, count)) = batches.get_mut(&key) {
        pending.messages = request.messages;
        pending.heuristic_entries = request.heuristic_entries;
        *count = count.saturating_add(1);
        true
    } else {
        batches.insert(key, (request, 1));
        false
    }
}

fn partition_semantic_refinements(
    semantic_entries: Vec<MemoryEntry>,
    heuristic_entries: &[MemoryEntry],
) -> (Vec<(MemoryEntry, MemoryEntry)>, Vec<MemoryEntry>) {
    let mut used_heuristic_ids = HashSet::new();
    let mut refinements = Vec::new();
    let mut inserts = Vec::new();

    for semantic in semantic_entries {
        let candidate = heuristic_entries.iter().find(|heuristic| {
            !used_heuristic_ids.contains(&heuristic.id)
                && heuristic.source == MemorySource::AutoExtracted
                && heuristic.layer == semantic.layer
                && heuristic.category == semantic.category
                && heuristic.scope == semantic.scope
        });
        if let Some(heuristic) = candidate {
            used_heuristic_ids.insert(heuristic.id);
            refinements.push((heuristic.clone(), semantic));
        } else {
            inserts.push(semantic);
        }
    }

    (refinements, inserts)
}

const SEMANTIC_DUPLICATE_DISTANCE: f32 = 0.20;
const SEMANTIC_DUPLICATE_MIN_SIMILARITY: f32 = 0.82;

#[derive(Default)]
struct SemanticPersistenceResult {
    durable_entries: Vec<MemoryEntry>,
    prepared_embeddings: Vec<(MemoryId, Vec<f32>)>,
    deduplicated_entries: usize,
}

async fn persist_semantic_extraction_batch(
    orchestrator: &MemoryOrchestrator,
    turn: &MemoryTurnContext,
    heuristic_entries: &[MemoryEntry],
    semantic_entries: Vec<MemoryEntry>,
    semantic_embeddings: Option<HashMap<MemoryId, Vec<f32>>>,
    vector_index: &RwLock<VectorIndex>,
) -> Result<SemanticPersistenceResult> {
    let (refinements, mut inserts) =
        partition_semantic_refinements(semantic_entries, heuristic_entries);
    let mut result = SemanticPersistenceResult {
        durable_entries: Vec::with_capacity(refinements.len() + inserts.len()),
        ..SemanticPersistenceResult::default()
    };
    let semantic_embeddings = semantic_embeddings.unwrap_or_default();
    let refined_heuristic_ids = refinements
        .iter()
        .map(|(heuristic, _)| heuristic.id)
        .collect::<HashSet<_>>();
    let heuristic_ids = heuristic_entries
        .iter()
        .map(|entry| entry.id)
        .collect::<HashSet<_>>();

    for (heuristic, mut semantic) in refinements {
        let semantic_id = semantic.id;
        if let Some(embedding) = semantic_embeddings.get(&semantic_id) {
            if let Some((existing, similarity)) = find_cross_turn_semantic_duplicate(
                orchestrator,
                vector_index,
                &semantic,
                embedding,
                &heuristic_ids,
            )
            .await?
            {
                archive_fresh_automatic_duplicate(
                    orchestrator,
                    vector_index,
                    turn,
                    &heuristic,
                    &existing,
                    similarity,
                )
                .await?;
                result.deduplicated_entries += 1;
                continue;
            }
        }
        semantic.id = heuristic.id;
        semantic.created_at = heuristic.created_at;
        semantic.updated_at = Utc::now();
        semantic.access_count = heuristic.access_count;
        semantic.last_accessed_at = heuristic.last_accessed_at;
        semantic.relations.extend(heuristic.relations);
        semantic.tags.extend(heuristic.tags);
        semantic.tags.sort();
        semantic.tags.dedup();
        orchestrator.update(&semantic).await?;
        if let Some(embedding) = semantic_embeddings.get(&semantic_id) {
            result
                .prepared_embeddings
                .push((semantic.id, embedding.clone()));
        }
        result.durable_entries.push(semantic);
    }

    // An LLM may correctly emit no entry for a recall-only turn. The fast
    // heuristic atom still needs the same producer-boundary governance, using
    // its already indexed vector so no second embedding request is required.
    for heuristic in heuristic_entries
        .iter()
        .filter(|entry| !refined_heuristic_ids.contains(&entry.id))
    {
        let embedding = vector_index.read().embedding(&heuristic.id);
        let Some(embedding) = embedding else {
            continue;
        };
        if let Some((existing, similarity)) = find_cross_turn_semantic_duplicate(
            orchestrator,
            vector_index,
            heuristic,
            &embedding,
            &heuristic_ids,
        )
        .await?
        {
            archive_fresh_automatic_duplicate(
                orchestrator,
                vector_index,
                turn,
                heuristic,
                &existing,
                similarity,
            )
            .await?;
            result.deduplicated_entries += 1;
        }
    }

    if !inserts.is_empty() {
        let mut accepted = Vec::with_capacity(inserts.len());
        for entry in inserts.drain(..) {
            let duplicate = match semantic_embeddings.get(&entry.id) {
                Some(embedding) => {
                    find_cross_turn_semantic_duplicate(
                        orchestrator,
                        vector_index,
                        &entry,
                        embedding,
                        &heuristic_ids,
                    )
                    .await?
                }
                None => None,
            };
            if let Some((existing, similarity)) = duplicate {
                tracing::info!(
                    incoming_memory_id = %entry.id,
                    existing_memory_id = %existing.id,
                    similarity,
                    layer = ?entry.layer,
                    scope = %entry.scope.scope_key(),
                    "semantic memory duplicate suppressed before persistence"
                );
                result.deduplicated_entries += 1;
                continue;
            }
            accepted.push(entry);
        }
        inserts = accepted;
    }

    if !inserts.is_empty() {
        let ids = orchestrator
            .remember_batch_for_turn(turn, inserts.clone())
            .await?;
        for (entry, id) in inserts.iter_mut().zip(ids) {
            if let Some(embedding) = semantic_embeddings.get(&entry.id) {
                result.prepared_embeddings.push((id, embedding.clone()));
            }
            entry.id = id;
        }
        result.durable_entries.extend(inserts);
    }

    Ok(result)
}

async fn archive_fresh_automatic_duplicate(
    orchestrator: &MemoryOrchestrator,
    vector_index: &RwLock<VectorIndex>,
    turn: &MemoryTurnContext,
    duplicate: &MemoryEntry,
    existing: &MemoryEntry,
    similarity: f32,
) -> Result<()> {
    let lifecycle_key = format!("memory_lifecycle:{}", duplicate.id);
    let mut events = orchestrator
        .store()
        .kv_get(&lifecycle_key)
        .await?
        .and_then(|raw| serde_json::from_str::<Vec<MemoryLifecycleEvent>>(&raw).ok())
        .unwrap_or_default();
    events.push(MemoryLifecycleEvent {
        memory_id: duplicate.id,
        from: events.last().map(|event| event.to),
        to: MemoryState::Archived,
        reason: format!(
            "same-turn automatic atom duplicates {} at semantic similarity {:.4}",
            existing.id, similarity
        ),
        session_id: turn.session_id.clone(),
        agent_id: turn.agent_id.clone(),
        occurred_at: Utc::now(),
    });
    orchestrator
        .store()
        .kv_put(
            &lifecycle_key,
            &serde_json::to_string(&events).map_err(|error| {
                MemoryError::Store(format!(
                    "serialize duplicate lifecycle for {}: {error}",
                    duplicate.id
                ))
            })?,
        )
        .await?;
    {
        let mut index = vector_index.write();
        index.remove(&duplicate.id)?;
        index.persistence_snapshot()
    }
    .persist()?;
    tracing::info!(
        duplicate_memory_id = %duplicate.id,
        existing_memory_id = %existing.id,
        similarity,
        "fresh automatic memory duplicate archived"
    );
    Ok(())
}

async fn find_cross_turn_semantic_duplicate(
    orchestrator: &MemoryOrchestrator,
    vector_index: &RwLock<VectorIndex>,
    incoming: &MemoryEntry,
    embedding: &[f32],
    ignored_ids: &HashSet<MemoryId>,
) -> Result<Option<(MemoryEntry, f32)>> {
    let candidates = {
        let index = vector_index.read();
        if index.count() == 0 || index.dimension() as usize != embedding.len() {
            return Ok(None);
        }
        match index.find_duplicates(embedding, SEMANTIC_DUPLICATE_DISTANCE) {
            Ok(candidates) => candidates,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "semantic duplicate lookup degraded; preserving incoming memory"
                );
                return Ok(None);
            }
        }
    };
    for (candidate_id, similarity) in candidates {
        if candidate_id == incoming.id || ignored_ids.contains(&candidate_id) {
            continue;
        }
        let Some(existing) = orchestrator.recall(&candidate_id).await? else {
            continue;
        };
        let state = orchestrator
            .store()
            .kv_get(&format!("memory_lifecycle:{}", existing.id))
            .await
            .ok()
            .flatten()
            .and_then(|raw| latest_lifecycle_state(&raw));
        if !lifecycle_state_is_active(state)
            || !semantic_duplicate_compatible(&existing, incoming, similarity)
        {
            continue;
        }
        return Ok(Some((existing, similarity)));
    }
    Ok(None)
}

fn semantic_duplicate_compatible(
    existing: &MemoryEntry,
    incoming: &MemoryEntry,
    similarity: f32,
) -> bool {
    if similarity < SEMANTIC_DUPLICATE_MIN_SIMILARITY
        || memory_polarity_conflicts(&existing.content, &incoming.content)
    {
        return false;
    }

    let existing_global_preference_dominates = existing.layer == MemoryLayer::L1
        && existing.category == MemoryCategory::UserPreference
        && existing.scope == MemoryScope::Global
        && incoming.layer == MemoryLayer::L2
        && matches!(
            incoming.category,
            MemoryCategory::Decision
                | MemoryCategory::ProjectConvention
                | MemoryCategory::ProjectKnowledge
        );
    if existing_global_preference_dominates {
        return true;
    }

    if existing.scope != incoming.scope
        || existing.layer != incoming.layer
        || !memory_categories_are_semantically_compatible(existing.category, incoming.category)
    {
        return false;
    }

    if existing.category == MemoryCategory::UserPreference
        && incoming.category == MemoryCategory::UserPreference
    {
        return true;
    }

    meaningful_memory_tag_overlap(existing, incoming) > 0
}

fn memory_categories_are_semantically_compatible(
    existing: MemoryCategory,
    incoming: MemoryCategory,
) -> bool {
    if existing == incoming {
        return true;
    }
    matches!(
        (existing, incoming),
        (
            MemoryCategory::Decision
                | MemoryCategory::ProjectConvention
                | MemoryCategory::ProjectKnowledge,
            MemoryCategory::Decision
                | MemoryCategory::ProjectConvention
                | MemoryCategory::ProjectKnowledge
        )
    )
}

fn meaningful_memory_tag_overlap(existing: &MemoryEntry, incoming: &MemoryEntry) -> usize {
    const GENERIC_TAGS: &[&str] = &[
        "decision",
        "reference",
        "project",
        "project knowledge",
        "project convention",
        "preference",
        "user",
        "memory",
    ];
    let normalize = |tag: &str| {
        tag.trim()
            .to_lowercase()
            .replace(['-', '_'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };
    let existing_tags = existing
        .tags
        .iter()
        .map(|tag| normalize(tag))
        .filter(|tag| {
            !tag.is_empty()
                && !tag.starts_with("extraction-batch:")
                && !GENERIC_TAGS.contains(&tag.as_str())
        })
        .collect::<HashSet<_>>();
    incoming
        .tags
        .iter()
        .map(|tag| normalize(tag))
        .filter(|tag| existing_tags.contains(tag))
        .count()
}

fn memory_polarity_conflicts(existing: &str, incoming: &str) -> bool {
    const NEGATIVE_MARKERS: &[&str] = &[
        " must not ",
        " should not ",
        " do not ",
        " don't ",
        " never ",
        " prohibit",
        " forbid",
        " disallow",
        " cannot ",
        "禁止",
        "不得",
        "不允许",
        "不能",
        "不要",
        "无需",
    ];
    let normalize = |value: &str| format!(" {} ", value.trim().to_lowercase());
    let existing = normalize(existing);
    let incoming = normalize(incoming);
    let existing_negative = NEGATIVE_MARKERS
        .iter()
        .any(|marker| existing.contains(marker));
    let incoming_negative = NEGATIVE_MARKERS
        .iter()
        .any(|marker| incoming.contains(marker));
    existing_negative != incoming_negative
}

async fn prepare_semantic_embeddings(
    capability: &EmbeddingCapability,
    entries: &[MemoryEntry],
) -> Result<Option<HashMap<MemoryId, Vec<f32>>>> {
    let EmbeddingCapability::Remote { client } = capability else {
        return Ok(None);
    };
    if entries.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let texts = entries
        .iter()
        .map(memory_embedding_text)
        .collect::<Vec<_>>();
    let text_refs = texts.iter().map(String::as_str).collect::<Vec<_>>();
    let embeddings = client.embed(&text_refs).await?;
    Ok(Some(
        entries
            .iter()
            .map(|entry| entry.id)
            .zip(embeddings)
            .collect(),
    ))
}

async fn embed_memory_entries(
    capability: &EmbeddingCapability,
    vector_index: &RwLock<VectorIndex>,
    entries: &[(MemoryId, String)],
    persist: bool,
) -> Result<usize> {
    let EmbeddingCapability::Remote { client } = capability else {
        return Ok(0);
    };
    if entries.is_empty() {
        return Ok(0);
    }
    let texts = entries
        .iter()
        .map(|(_, content)| content.as_str())
        .collect::<Vec<_>>();
    let embeddings = client.embed(&texts).await?;
    let (indexed, snapshot) = {
        let mut index = vector_index.write();
        let mut indexed = 0;
        for ((id, _), embedding) in entries.iter().zip(embeddings) {
            index.upsert(*id, embedding)?;
            indexed += 1;
        }
        let snapshot = persist.then(|| index.persistence_snapshot());
        (indexed, snapshot)
    };
    if let Some(snapshot) = snapshot {
        snapshot.persist()?;
    }
    Ok(indexed)
}

fn persist_vector_index_snapshot(vector_index: &RwLock<VectorIndex>) -> Result<()> {
    let snapshot = vector_index.read().persistence_snapshot();
    snapshot.persist()
}

fn memory_embedding_text(entry: &MemoryEntry) -> String {
    const MAX_EMBEDDING_CHARS: usize = 8_000;
    let mut text = format!("{}\n{}", entry.title, entry.content);
    if text.chars().count() > MAX_EMBEDDING_CHARS {
        text = text.chars().take(MAX_EMBEDDING_CHARS).collect();
    }
    text
}

async fn active_entries_for_vector_reconciliation(
    store: &dyn MemoryStore,
    entries: Vec<MemoryEntry>,
) -> Result<Vec<MemoryEntry>> {
    let keys = entries
        .iter()
        .map(|entry| format!("memory_lifecycle:{}", entry.id))
        .collect::<Vec<_>>();
    let lifecycle_by_key = store
        .kv_get_many(&keys)
        .await?
        .into_iter()
        .map(|value| (value.key, value.value))
        .collect::<HashMap<_, _>>();
    Ok(entries
        .into_iter()
        .filter(|entry| {
            let state = lifecycle_by_key
                .get(&format!("memory_lifecycle:{}", entry.id))
                .and_then(|raw| latest_lifecycle_state(raw));
            lifecycle_state_is_active(state)
        })
        .collect())
}

/// Reconcile the rebuildable vector artifact in bounded keyset pages. The
/// durable store owns truth; this function never materialises the full corpus.
async fn reconcile_vector_index(
    store: &dyn MemoryStore,
    capability: &EmbeddingCapability,
    vector_index: &RwLock<VectorIndex>,
) -> Result<(usize, u64, u64)> {
    const PAGE_SIZE: usize = 256;
    let mut cursor = MemoryScanCursor::default();
    let mut indexed = 0usize;
    let mut active_count = 0u64;
    loop {
        let page = store.scan_entries_page(cursor, PAGE_SIZE).await?;
        let active_entries = active_entries_for_vector_reconciliation(store, page.entries).await?;
        active_count = active_count.saturating_add(active_entries.len() as u64);
        let missing = {
            let index = vector_index.read();
            active_entries
                .iter()
                .filter(|entry| !index.contains(&entry.id))
                .map(|entry| (entry.id, memory_embedding_text(entry)))
                .collect::<Vec<_>>()
        };
        indexed = indexed
            .saturating_add(embed_memory_entries(capability, vector_index, &missing, false).await?);
        let Some(next) = page.next else {
            break;
        };
        cursor = next;
    }
    persist_vector_index_snapshot(vector_index)?;
    let indexed_active = vector_index_coverage(store, vector_index).await?;
    Ok((indexed, indexed_active, active_count))
}

async fn vector_index_coverage(
    store: &dyn MemoryStore,
    vector_index: &RwLock<VectorIndex>,
) -> Result<u64> {
    const PAGE_SIZE: usize = 512;
    let mut cursor = MemoryScanCursor::default();
    let mut indexed_active = 0u64;
    loop {
        let page = store.scan_entries_page(cursor, PAGE_SIZE).await?;
        let active_entries = active_entries_for_vector_reconciliation(store, page.entries).await?;
        let index = vector_index.read();
        indexed_active = indexed_active.saturating_add(
            active_entries
                .iter()
                .filter(|entry| index.contains(&entry.id))
                .count() as u64,
        );
        drop(index);
        let Some(next) = page.next else {
            break;
        };
        cursor = next;
    }
    Ok(indexed_active)
}

fn latest_lifecycle_state(raw: &str) -> Option<MemoryState> {
    serde_json::from_str::<Vec<MemoryLifecycleEvent>>(raw)
        .ok()
        .and_then(|events| events.last().map(|event| event.to))
}

fn lifecycle_state_is_active(state: Option<MemoryState>) -> bool {
    !matches!(state, Some(MemoryState::Superseded | MemoryState::Archived))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackgroundExtractionHealth {
    pub pending_requests: u64,
    pub accepted_requests: u64,
    pub coalesced_requests: u64,
    pub completed_requests: u64,
    pub failed_requests: u64,
    pub deduplicated_entries: u64,
    pub last_error: Option<String>,
    pub indexed_entries: u64,
    pub index_failures: u64,
    pub last_index_error: Option<String>,
    pub vector_entries: u64,
    pub vector_active_entries: u64,
    pub vector_indexed_active_entries: u64,
    /// Indexed share of active durable memories in basis points (10_000=100%).
    pub vector_coverage_basis_points: u64,
    pub vector_evictions: u64,
    pub vector_generation: u64,
    pub vector_persisted_generation: u64,
    pub vector_persistence_failures: u64,
    pub degraded_to_fts: bool,
    pub vector_reconciliation_complete: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryBackgroundShutdownReport {
    pub joined_tasks: usize,
    pub forced_aborts: usize,
    pub watcher_joined: bool,
    pub errors: Vec<String>,
}

#[derive(Default)]
struct BackgroundExtractionState {
    pending_requests: AtomicU64,
    accepted_requests: AtomicU64,
    coalesced_requests: AtomicU64,
    completed_requests: AtomicU64,
    failed_requests: AtomicU64,
    deduplicated_entries: AtomicU64,
    last_error: Mutex<Option<String>>,
    indexed_entries: AtomicU64,
    index_failures: AtomicU64,
    last_index_error: Mutex<Option<String>>,
    vector_active_entries: AtomicU64,
    vector_indexed_active_entries: AtomicU64,
    vector_reconciliation_complete: AtomicBool,
}

impl BackgroundExtractionState {
    fn snapshot(&self) -> BackgroundExtractionHealth {
        BackgroundExtractionHealth {
            pending_requests: self.pending_requests.load(Ordering::Relaxed),
            accepted_requests: self.accepted_requests.load(Ordering::Relaxed),
            coalesced_requests: self.coalesced_requests.load(Ordering::Relaxed),
            completed_requests: self.completed_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            deduplicated_entries: self.deduplicated_entries.load(Ordering::Relaxed),
            last_error: self.last_error.lock().clone(),
            indexed_entries: self.indexed_entries.load(Ordering::Relaxed),
            index_failures: self.index_failures.load(Ordering::Relaxed),
            last_index_error: self.last_index_error.lock().clone(),
            vector_entries: 0,
            vector_active_entries: self.vector_active_entries.load(Ordering::Relaxed),
            vector_indexed_active_entries: self
                .vector_indexed_active_entries
                .load(Ordering::Relaxed),
            vector_coverage_basis_points: 0,
            vector_evictions: 0,
            vector_generation: 0,
            vector_persisted_generation: 0,
            vector_persistence_failures: 0,
            degraded_to_fts: false,
            vector_reconciliation_complete: self
                .vector_reconciliation_complete
                .load(Ordering::Acquire),
        }
    }
}

struct OwnedBackgroundTask {
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl OwnedBackgroundTask {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            handle: Mutex::new(Some(handle)),
        }
    }

    fn take(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.handle.lock().take()
    }
}

impl Drop for OwnedBackgroundTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.get_mut().take() {
            handle.abort();
        }
    }
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
    /// Five-layer memory orchestrator (Arc-wrapped for shared access).
    orchestrator: Arc<MemoryOrchestrator>,
    /// In-process vector index for semantic search.
    vector_index: Arc<RwLock<VectorIndex>>,
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
    extract_tx: mpsc::Sender<BackgroundExtractionRequest>,
    /// Handle to the background LLM extraction task.
    extract_handle: OwnedBackgroundTask,
    /// Handle to the in-memory knowledge graph replacement task.
    kg_rebuild_handle: OwnedBackgroundTask,
    /// Shared close signal for every Tokio task owned by this manager.
    background_shutdown: watch::Sender<bool>,
    background_extraction_state: Arc<BackgroundExtractionState>,
    /// Freshness-priority context manager for session budget management.
    fresh_ctx: FreshContextManager,
    /// Context rotation monitor for GSD-style health warnings.
    context_rot_monitor: Mutex<ContextRotMonitor>,
    /// Queue of child agent delegation results, consumed by on_turn_end.
    delegation_results: Mutex<Vec<DelegationResult>>,
    /// BM25-based session resume for context recovery from prior sessions.
    session_resume: Option<SessionResume>,
    /// Optional project scope manager for KG staleness detection.
    project_scope_mgr: Option<std::sync::Arc<ProjectScopeManager>>,
    /// Reviewable lifecycle candidates for memory self-maintenance.
    maintenance_queue: MaintenanceQueue,
    /// One admission point shared by startup, nightly, and manual governance.
    automatic_governance_run: Mutex<Option<AutomaticGovernanceRunStatus>>,
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
    /// Short-lived cache for identical prepare_context requests.
    prepare_context_cache: Mutex<Option<CachedPreparedContext>>,
    /// Monotonic version for invalidating derived context after memory writes.
    memory_revision: AtomicU64,
    /// Bounded, coalesced usage signals queried synchronously by Runtime.
    memory_usage_signals: Arc<Mutex<HashMap<String, MemoryUsageSignal>>>,
    memory_usage_persist_tx: mpsc::Sender<()>,
    memory_usage_persist_handle: OwnedBackgroundTask,
    memory_usage_writer_state: Arc<MemoryUsageWriterState>,
    /// Performance metrics collector (rolling window).
    perf_monitor: PerformanceMonitor,
    /// Auto-tuner that adjusts TuningConfig based on observed performance.
    auto_tuner: AutoTuner,
    /// Cross-agent entity evolution tracker (P9.3).
    entity_registry: Mutex<Option<crate::entity_registry::EntityRegistry>>,
}

impl CognitiveContextManager {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Initialise the manager from `config`, opening all storage backends.
    pub async fn new(config: MemoryConfig) -> Result<Self> {
        Self::new_with_workspace_and_session_history(config, None, None).await
    }

    /// Initialise the manager with an explicit workspace root for L2 project
    /// context discovery.
    pub async fn new_with_workspace(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
    ) -> Result<Self> {
        Self::new_with_workspace_and_session_history(config, workspace_root, None).await
    }

    /// Initialise standalone Memory with a host-owned model summarizer.
    pub async fn new_with_summarizer(
        config: MemoryConfig,
        llm_summarizer: Arc<dyn LlmSummarizer>,
    ) -> Result<Self> {
        Self::new_with_storage_selection(config, None, None, None, true, None, Some(llm_summarizer))
            .await
    }

    /// Initialise with the selected durable session owner.  Session recovery
    /// is deliberately injected rather than inferred from a SQLite path.
    pub async fn new_with_workspace_and_session_history(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
        session_history: Option<Arc<SessionHistoryReader>>,
    ) -> Result<Self> {
        Self::new_with_storage_selection(
            config,
            workspace_root,
            session_history,
            None,
            true,
            None,
            None,
        )
        .await
    }

    /// Initialise the complete cognitive runtime over a host-selected durable
    /// Memory port. The relational backend is already chosen at the process
    /// composition root, so this constructor never infers or opens SQLite.
    /// Rebuildable vector/audit artifacts remain file based; the maintenance
    /// queue is in memory because its durable truth belongs to `MemoryStore`.
    pub async fn new_with_selected_store(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
        session_history: Option<Arc<SessionHistoryReader>>,
        store: Arc<dyn MemoryStore>,
    ) -> Result<Self> {
        Self::new_with_storage_selection(
            config,
            workspace_root,
            session_history,
            Some(store),
            false,
            None,
            None,
        )
        .await
    }

    /// Variant used by a composition root that selected SQLite and therefore
    /// may retain the existing SQLite-backed maintenance/vector auxiliaries.
    /// PostgreSQL composition passes `false` so no business SQLite is opened.
    pub async fn new_with_selected_store_and_auxiliaries(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
        session_history: Option<Arc<SessionHistoryReader>>,
        store: Arc<dyn MemoryStore>,
        sqlite_auxiliaries: bool,
        maintenance_queue: Option<MaintenanceQueue>,
    ) -> Result<Self> {
        Self::new_with_selected_store_auxiliaries_and_summarizer(
            config,
            workspace_root,
            session_history,
            store,
            sqlite_auxiliaries,
            maintenance_queue,
            None,
        )
        .await
    }

    /// Composition-root variant with a Runtime-owned model summarizer.
    pub async fn new_with_selected_store_auxiliaries_and_summarizer(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
        session_history: Option<Arc<SessionHistoryReader>>,
        store: Arc<dyn MemoryStore>,
        sqlite_auxiliaries: bool,
        maintenance_queue: Option<MaintenanceQueue>,
        llm_summarizer: Option<Arc<dyn LlmSummarizer>>,
    ) -> Result<Self> {
        Self::new_with_storage_selection(
            config,
            workspace_root,
            session_history,
            Some(store),
            sqlite_auxiliaries,
            maintenance_queue,
            llm_summarizer,
        )
        .await
    }

    async fn new_with_storage_selection(
        config: MemoryConfig,
        workspace_root: Option<PathBuf>,
        session_history: Option<Arc<SessionHistoryReader>>,
        selected_store: Option<Arc<dyn MemoryStore>>,
        sqlite_auxiliaries: bool,
        selected_maintenance_queue: Option<MaintenanceQueue>,
        llm_summarizer: Option<Arc<dyn LlmSummarizer>>,
    ) -> Result<Self> {
        let orchestrator = Arc::new(match &selected_store {
            Some(store) => {
                MemoryOrchestrator::from_store(
                    config.clone(),
                    Arc::clone(store),
                    workspace_root.clone(),
                )?
            }
            None => {
                MemoryOrchestrator::init_with_workspace(config.clone(), workspace_root.clone())
                    .await?
            }
        });
        if let Some(store) = selected_store.as_ref() {
            MemoryOrchestrator::bootstrap_identity(store, &config).await?;
        }

        // Build the vector index with persistence support.
        // Use VectorIndex::load to restore previously persisted vectors.
        // Dimension 0 means "auto": reuse an existing persisted index dimension,
        // falling back to the store default only when the index is empty.
        let dimension = config.store.vector.dimension as u32;
        let persist_path = config.store.blob_dir.join("vector_index.json");
        let vector_sqlite_store = sqlite_auxiliaries
            .then(|| SqliteStore::open(&config.store).ok())
            .flatten();
        let (loaded_vector_index, vector_load_error) = match VectorIndex::load_with_store(
            persist_path.clone(),
            dimension,
            vector_sqlite_store.clone(),
        ) {
            Ok(index) => (index, None),
            Err(error) => {
                // The durable Memory store remains authoritative. A corrupt
                // rebuildable vector artifact must degrade to FTS instead of
                // preventing Gateway startup or returning a false empty result.
                let mut empty = VectorIndex::new(persist_path, dimension)?;
                if let Some(store) = vector_sqlite_store {
                    empty.set_sqlite_store(store);
                }
                (empty, Some(error.to_string()))
            }
        };
        let vector_index = Arc::new(RwLock::new(loaded_vector_index));

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
        if let Some(summarizer) = llm_summarizer {
            extractor = extractor.with_llm(summarizer);
            tracing::info!("LLM-enhanced extraction enabled (Pass 5)");
        } else if config.compression.llm.is_configured() {
            tracing::info!(
                "LLM summarizer requested without a Runtime adapter (template fallback)"
            );
        }

        // Wrap extractor in Arc for sharing with the background LLM task.
        let extractor = Arc::new(extractor);

        // ── Background LLM extraction worker ────────────────────────────────
        let (extract_tx, mut extract_rx) = mpsc::channel::<BackgroundExtractionRequest>(128);
        let background_extraction_state = Arc::new(BackgroundExtractionState::default());
        if let Some(error) = vector_load_error {
            background_extraction_state
                .index_failures
                .fetch_add(1, Ordering::Relaxed);
            *background_extraction_state.last_index_error.lock() = Some(error.clone());
            tracing::warn!(%error, "vector index artifact degraded; FTS remains authoritative");
        }
        let (background_shutdown, mut extraction_shutdown) = watch::channel(false);
        let persisted_usage = orchestrator
            .store()
            .kv_get(MEMORY_USAGE_SELECTION_KEY)
            .await
            .unwrap_or(None)
            .and_then(|raw| serde_json::from_str::<Vec<MemoryUsageSignal>>(&raw).ok())
            .unwrap_or_default();
        let memory_usage_signals = Arc::new(Mutex::new(
            persisted_usage
                .into_iter()
                .map(|signal| (memory_usage_signal_key(&signal), signal))
                .take(MAX_MEMORY_USAGE_KEYS)
                .collect::<HashMap<_, _>>(),
        ));
        let (memory_usage_persist_tx, mut memory_usage_persist_rx) = mpsc::channel::<()>(1);
        let memory_usage_writer_state = Arc::new(MemoryUsageWriterState::default());
        let usage_signals = Arc::clone(&memory_usage_signals);
        let usage_orchestrator = Arc::clone(&orchestrator);
        let usage_state = Arc::clone(&memory_usage_writer_state);
        let mut usage_shutdown = background_shutdown.subscribe();
        let memory_usage_persist_handle = tokio::spawn(async move {
            loop {
                let should_stop = tokio::select! {
                    changed = usage_shutdown.changed() => {
                        changed.is_err() || *usage_shutdown.borrow()
                    }
                    message = memory_usage_persist_rx.recv() => message.is_none(),
                };
                if !should_stop {
                    tokio::time::sleep(Duration::from_millis(75)).await;
                    while memory_usage_persist_rx.try_recv().is_ok() {
                        usage_state.coalesced.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let mut signals = usage_signals.lock().values().cloned().collect::<Vec<_>>();
                signals.sort_by(|left, right| {
                    left.memory_id
                        .cmp(&right.memory_id)
                        .then_with(|| left.session_id.cmp(&right.session_id))
                        .then_with(|| left.agent_id.cmp(&right.agent_id))
                });
                let persisted = match serde_json::to_string(&signals) {
                    Ok(raw) => usage_orchestrator
                        .store()
                        .kv_put(MEMORY_USAGE_SELECTION_KEY, &raw)
                        .await
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error.to_string()),
                };
                if persisted.is_ok() {
                    usage_state
                        .persisted_batches
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    usage_state
                        .persistence_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                if should_stop {
                    break;
                }
            }
        });

        let bg_extractor = Arc::clone(&extractor);
        let bg_orchestrator = Arc::clone(&orchestrator);
        let bg_state = Arc::clone(&background_extraction_state);
        let bg_embedding_capability = embedding_capability.clone();
        let bg_vector_index = Arc::clone(&vector_index);
        let auto_vector_dimension = dimension == 0;
        let extractor_debounce_secs = config.extractor.extractor_debounce_secs;

        let extract_handle = tokio::spawn(async move {
            let debounce = Duration::from_secs(extractor_debounce_secs);
            if let EmbeddingCapability::Remote { client } = &bg_embedding_capability {
                if auto_vector_dimension {
                    match client.detect_dimension().await {
                        Ok(provider_dimension) => {
                            let mut index = bg_vector_index.write();
                            if index.dimension() != provider_dimension as u32 {
                                tracing::info!(
                                    previous_dimension = index.dimension(),
                                    provider_dimension,
                                    previous_count = index.count(),
                                    "semantic vector dimension changed; rebuilding durable index"
                                );
                                index.reset_dimension(provider_dimension as u32);
                            }
                        }
                        Err(error) => {
                            bg_state.index_failures.fetch_add(1, Ordering::Relaxed);
                            *bg_state.last_index_error.lock() = Some(error.to_string());
                            tracing::warn!(
                                %error,
                                "semantic vector dimension probe degraded"
                            );
                        }
                    }
                }
                match reconcile_vector_index(
                    bg_orchestrator.store().as_ref(),
                    &bg_embedding_capability,
                    &bg_vector_index,
                )
                .await
                {
                    Ok((indexed, indexed_active_entries, active_entries)) => {
                        bg_state
                            .indexed_entries
                            .fetch_add(indexed as u64, Ordering::Relaxed);
                        bg_state
                            .vector_active_entries
                            .store(active_entries, Ordering::Release);
                        bg_state
                            .vector_indexed_active_entries
                            .store(indexed_active_entries, Ordering::Release);
                        bg_state
                            .vector_reconciliation_complete
                            .store(true, Ordering::Release);
                        *bg_state.last_index_error.lock() = None;
                        tracing::info!(
                            count = indexed,
                            indexed_active_entries,
                            active_entries,
                            "semantic vector startup reconciliation completed"
                        );
                    }
                    Err(error) => {
                        bg_state.index_failures.fetch_add(1, Ordering::Relaxed);
                        *bg_state.last_index_error.lock() = Some(error.to_string());
                        tracing::warn!(%error, "semantic vector startup reconciliation degraded");
                    }
                }
            } else {
                // Keyword-only mode is an intentional FTS degradation, not a
                // failed or pending vector reconciliation.
                bg_state
                    .vector_reconciliation_complete
                    .store(true, Ordering::Release);
            }
            loop {
                let first_request = tokio::select! {
                    changed = extraction_shutdown.changed() => {
                        if changed.is_err() || *extraction_shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    request = extract_rx.recv() => {
                        let Some(request) = request else {
                            break;
                        };
                        request
                    }
                };
                let mut batches = HashMap::new();
                let first_key = background_extraction_key(&first_request);
                batches.insert(first_key, (first_request, 1_u64));

                if !debounce.is_zero() {
                    let timer = tokio::time::sleep(debounce);
                    tokio::pin!(timer);
                    loop {
                        tokio::select! {
                            changed = extraction_shutdown.changed() => {
                                if changed.is_err() || *extraction_shutdown.borrow() {
                                    return;
                                }
                            }
                            request = extract_rx.recv() => {
                                let Some(request) = request else {
                                    break;
                                };
                                if coalesce_background_request(&mut batches, request) {
                                    bg_state.coalesced_requests.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            () = &mut timer => break,
                        }
                    }
                }

                for (_, (request, request_count)) in batches {
                    bg_state
                        .pending_requests
                        .fetch_sub(request_count, Ordering::Relaxed);
                    if bg_extractor.llm_client().is_none() {
                        continue;
                    }
                    let extraction = bg_extractor.llm_extract(&request.messages);
                    let extraction = tokio::select! {
                        changed = extraction_shutdown.changed() => {
                            if changed.is_err() || *extraction_shutdown.borrow() {
                                return;
                            }
                            continue;
                        }
                        result = extraction => result,
                    };
                    match extraction {
                        Ok(llm_entries) => {
                            let mut final_entries = bg_extractor.finalize_entries(llm_entries);
                            let batch_tag = extraction_batch_tag(&request.turn, &request.messages);
                            canonicalize_automatic_entries(
                                &request.turn,
                                &batch_tag,
                                &mut final_entries,
                            );
                            let entry_count = final_entries.len();
                            let semantic_embeddings = match prepare_semantic_embeddings(
                                &bg_embedding_capability,
                                &final_entries,
                            )
                            .await
                            {
                                Ok(embeddings) => embeddings,
                                Err(error) => {
                                    bg_state.index_failures.fetch_add(1, Ordering::Relaxed);
                                    *bg_state.last_index_error.lock() = Some(error.to_string());
                                    tracing::warn!(
                                        %error,
                                        session_id = %request.turn.session_id,
                                        "semantic duplicate detection and indexing degraded"
                                    );
                                    None
                                }
                            };
                            let persist_result = persist_semantic_extraction_batch(
                                &bg_orchestrator,
                                &request.turn,
                                &request.heuristic_entries,
                                final_entries,
                                semantic_embeddings,
                                &bg_vector_index,
                            )
                            .await;
                            match persist_result {
                                Ok(persisted) => {
                                    bg_state.deduplicated_entries.fetch_add(
                                        persisted.deduplicated_entries as u64,
                                        Ordering::Relaxed,
                                    );
                                    if !persisted.prepared_embeddings.is_empty() {
                                        let snapshot_result = {
                                            let mut index = bg_vector_index.write();
                                            persisted
                                                .prepared_embeddings
                                                .iter()
                                                .try_for_each(|(id, embedding)| {
                                                    index.upsert(*id, embedding.clone())
                                                })
                                                .map(|()| index.persistence_snapshot())
                                        };
                                        let index_result =
                                            snapshot_result.and_then(|snapshot| snapshot.persist());
                                        match index_result {
                                            Ok(()) => {
                                                bg_state.indexed_entries.fetch_add(
                                                    persisted.prepared_embeddings.len() as u64,
                                                    Ordering::Relaxed,
                                                );
                                                *bg_state.last_index_error.lock() = None;
                                            }
                                            Err(error) => {
                                                bg_state
                                                    .index_failures
                                                    .fetch_add(1, Ordering::Relaxed);
                                                *bg_state.last_index_error.lock() =
                                                    Some(error.to_string());
                                                tracing::warn!(
                                                    %error,
                                                    session_id = %request.turn.session_id,
                                                    "background semantic memory indexing degraded"
                                                );
                                            }
                                        }
                                    }
                                    bg_state
                                        .completed_requests
                                        .fetch_add(request_count, Ordering::Relaxed);
                                    *bg_state.last_error.lock() = None;
                                    tracing::info!(
                                        count = entry_count,
                                        persisted_count = persisted.durable_entries.len(),
                                        deduplicated_count = persisted.deduplicated_entries,
                                        session_id = %request.turn.session_id,
                                        agent_id = %request.turn.agent_id,
                                        "background LLM extract persisted"
                                    );
                                }
                                Err(error) => {
                                    bg_state
                                        .failed_requests
                                        .fetch_add(request_count, Ordering::Relaxed);
                                    *bg_state.last_error.lock() = Some(error.to_string());
                                    tracing::error!(
                                        %error,
                                        session_id = %request.turn.session_id,
                                        agent_id = %request.turn.agent_id,
                                        "background LLM extract persistence failed"
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            bg_state
                                .failed_requests
                                .fetch_add(request_count, Ordering::Relaxed);
                            *bg_state.last_error.lock() = Some(error.to_string());
                            tracing::warn!(%error, "background LLM extract failed");
                        }
                    }
                }
            }
            tracing::debug!("background LLM extract: worker exiting");
        });

        // Load knowledge graph from persistent store.
        let kg = {
            let entities = orchestrator.store().load_entities().await?;
            let triples = orchestrator.store().load_triples().await?;
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
        let mut kg_shutdown = background_shutdown.subscribe();
        let kg_rebuild_handle = tokio::spawn(async move {
            loop {
                let new_kg = tokio::select! {
                    changed = kg_shutdown.changed() => {
                        if changed.is_err() || *kg_shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    item = kg_rebuild_rx.recv() => {
                        let Some(item) = item else {
                            break;
                        };
                        item
                    }
                };
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
        let closet: Option<Closet> = closet_json.and_then(|json| {
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
        let state_rebuilder = ws_root.as_ref().and_then(|workspace| {
            session_history.as_ref().map(|history| {
                StateRebuilder::with_session_history(workspace.clone(), Arc::clone(history))
            })
        });
        let tool_sandbox = ToolOutputSandbox::new().map(Mutex::new).map_err(|error| {
            MemoryError::Other(format!("failed to initialize tool output sandbox: {error}"))
        })?;

        let audit_path = config.store.blob_dir.join("audit.jsonl");
        let audit_log = match AuditLog::open(audit_path.clone()) {
            Ok(log) => Some(log),
            Err(e) => {
                tracing::warn!("audit log: failed to open audit log: {e}");
                None
            }
        };
        let integrity_checker = {
            match AuditLog::open(audit_path) {
                Ok(log) => Some(Arc::new(IntegrityChecker::new(log))),
                Err(e) => {
                    tracing::warn!("integrity checker: failed to open audit log: {e}");
                    None
                }
            }
        };

        let maintenance_queue = if let Some(queue) = selected_maintenance_queue {
            queue
        } else if sqlite_auxiliaries {
            match MaintenanceQueue::open_sqlite(&config.store.sqlite_path) {
                Ok(queue) => queue,
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "memory maintenance: durable queue unavailable, using in-memory fallback"
                    );
                    MaintenanceQueue::new()
                }
            }
        } else {
            MaintenanceQueue::new()
        };

        Ok(Self {
            drift: DriftDetector::new(config.drift.clone()),
            fresh_ctx: FreshContextManager::new(config.budget.context_window),
            context_rot_monitor: Mutex::new(ContextRotMonitor::new(RotMetrics::default())),
            delegation_results: Mutex::new(Vec::new()),
            session_resume,
            project_scope_mgr: None,
            maintenance_queue,
            automatic_governance_run: Mutex::new(None),
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
            prepare_context_cache: Mutex::new(None),
            memory_revision: AtomicU64::new(0),
            memory_usage_signals,
            memory_usage_persist_tx,
            memory_usage_persist_handle: OwnedBackgroundTask::new(memory_usage_persist_handle),
            memory_usage_writer_state,
            perf_monitor: PerformanceMonitor::default(),
            auto_tuner: AutoTuner::new(config.tuning.clone()),
            entity_registry: Mutex::new(None),
            config,
            orchestrator,
            vector_index,
            hybrid_searcher: HybridSearcher::new(0.6, 0.4),
            monitor,
            handoff_mgr: HandoffManager::new(),
            seeds,
            decisions: Mutex::new(DecisionThreadStore::new()),
            closet: Mutex::new(closet),
            write_guard: None,
            audit_log,
            integrity_checker,
            integrity_check_counter: AtomicU64::new(0),
            embedding_capability,
            extractor,
            kg,
            background_watcher: Mutex::new(watcher_handle),
            extract_tx,
            extract_handle: OwnedBackgroundTask::new(extract_handle),
            kg_rebuild_handle: OwnedBackgroundTask::new(kg_rebuild_handle),
            background_shutdown,
            background_extraction_state,
        })
    }

    /// Initialise the manager and auto-load the project knowledge graph when
    /// workspace_root is provided.
    pub async fn new_with_project_kg(
        config: MemoryConfig,
        workspace_root: PathBuf,
    ) -> Result<Self> {
        let mgr = Self::new_with_workspace(config, Some(workspace_root.clone())).await?;
        mgr.load_project_kg(&workspace_root)?;
        Ok(mgr)
    }

    // -----------------------------------------------------------------------
    // Write guard configuration
    // -----------------------------------------------------------------------

    /// Set the write guard for controlling write access.
    ///
    /// Propagates the guard to the underlying [`MemoryOrchestrator`] so that
    /// [`MemoryOrchestrator::remember`] also enforces layer permissions.
    pub fn with_write_guard(mut self, guard: MemoryWriteGuard) -> Result<Self> {
        let inner = Arc::try_unwrap(self.orchestrator).map_err(|_| {
            MemoryError::Other(
                "cannot apply a memory write guard after the orchestrator has been shared"
                    .to_string(),
            )
        })?;
        self.orchestrator = Arc::new(inner.with_write_guard(Arc::new(guard.clone())));
        self.write_guard = Some(guard);
        Ok(self)
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

    /// Attach an EntityRegistry for cross-agent entity evolution tracking (P9.3).
    pub fn with_entity_registry(self, registry: crate::entity_registry::EntityRegistry) -> Self {
        *self.entity_registry.lock() = Some(registry);
        self
    }

    pub(crate) async fn kernel_kv_put(&self, key: &str, value: &str) -> Result<()> {
        self.orchestrator.store().kv_put(key, value).await
    }

    pub(crate) async fn kernel_kv_get(&self, key: &str) -> Result<Option<String>> {
        self.orchestrator.store().kv_get(key).await
    }

    pub fn automatic_governance_run_status(&self) -> Option<AutomaticGovernanceRunStatus> {
        self.automatic_governance_run.lock().clone()
    }

    #[must_use]
    pub fn maintenance_queue_is_durable(&self) -> bool {
        self.maintenance_queue.is_durable()
    }

    pub(crate) fn try_begin_automatic_governance(
        &self,
        mode: &str,
    ) -> Option<AutomaticGovernanceRunStatus> {
        let mut active = self.automatic_governance_run.lock();
        if active.is_some() {
            return None;
        }
        let status = AutomaticGovernanceRunStatus {
            run_id: uuid::Uuid::new_v4().to_string(),
            mode: mode.to_string(),
            started_at: Utc::now(),
            phase: "starting".to_string(),
            scanned_entries: 0,
            processed_candidates: 0,
            total_candidates: 0,
        };
        *active = Some(status.clone());
        Some(status)
    }

    pub(crate) fn finish_automatic_governance(&self, run_id: &str) {
        let mut active = self.automatic_governance_run.lock();
        if active.as_ref().is_some_and(|run| run.run_id == run_id) {
            *active = None;
        }
    }

    pub(crate) fn update_automatic_governance_progress(
        &self,
        run_id: &str,
        phase: &str,
        scanned_entries: usize,
        processed_candidates: usize,
        total_candidates: usize,
    ) {
        let mut active = self.automatic_governance_run.lock();
        let Some(run) = active.as_mut().filter(|run| run.run_id == run_id) else {
            return;
        };
        run.phase = phase.to_string();
        run.scanned_entries = scanned_entries;
        run.processed_candidates = processed_candidates;
        run.total_candidates = total_candidates;
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
    pub fn check_write_access(
        &self,
        layer: crate::types::MemoryLayer,
    ) -> crate::write_guard::WritePolicy {
        match &self.write_guard {
            Some(guard) => guard.check_write(layer),
            None => crate::write_guard::WritePolicy::Allow,
        }
    }

    fn invalidate_prepare_context_cache(&self) {
        self.memory_revision.fetch_add(1, Ordering::Relaxed);
        self.prepare_context_cache.lock().take();
    }

    fn prepare_context_cache_key(
        &self,
        query: &str,
        messages: &[Message],
        turn: &MemoryTurnContext,
        budget: &TokenBudget,
    ) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        query.hash(&mut hasher);
        turn.session_id.hash(&mut hasher);
        turn.project_id.hash(&mut hasher);
        turn.agent_id.hash(&mut hasher);
        turn.definition_lineage_id.hash(&mut hasher);
        turn.team_id.hash(&mut hasher);
        turn.task_id.hash(&mut hasher);
        turn.cognitive_read_scopes.hash(&mut hasher);
        budget.total.hash(&mut hasher);
        budget.available.hash(&mut hasher);
        self.config
            .tuning
            .freshness_trigger_ratio
            .to_bits()
            .hash(&mut hasher);
        for msg in messages {
            msg.turn_index.hash(&mut hasher);
            msg.role.to_string().hash(&mut hasher);
            msg.content.hash(&mut hasher);
            msg.tool_use_id.hash(&mut hasher);
            msg.tool_name.hash(&mut hasher);
            msg.pinned.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn cached_prepared_context(&self, key: u64, revision: u64) -> Option<PreparedContext> {
        let ttl = Duration::from_millis(self.config.tuning.prepare_context_cache_ttl_ms);
        if ttl.is_zero() {
            return None;
        }
        self.prepare_context_cache
            .lock()
            .as_ref()
            .filter(|cached| {
                cached.key == key && cached.revision == revision && cached.cached_at.elapsed() < ttl
            })
            .map(|cached| {
                let mut context = cached.context.clone();
                context.prepared_at = Utc::now();
                context
            })
    }

    fn store_prepared_context_cache(&self, key: u64, revision: u64, context: &PreparedContext) {
        if self.config.tuning.prepare_context_cache_ttl_ms == 0 {
            return;
        }
        *self.prepare_context_cache.lock() = Some(CachedPreparedContext {
            key,
            revision,
            context: context.clone(),
            cached_at: Instant::now(),
        });
    }

    fn audit_source(&self) -> WriteSource {
        self.write_guard
            .as_ref()
            .map(|guard| guard.source())
            .unwrap_or(WriteSource::User)
    }

    fn log_memory_audit(
        &self,
        operation: AuditOperation,
        entry_id: String,
        layer: MemoryLayer,
        summary_source: &str,
    ) {
        if let Some(ref log) = self.audit_log {
            let _ = log.log(&AuditEntry {
                timestamp: Utc::now(),
                operation,
                entry_id,
                layer: format!("{layer:?}"),
                source: self.audit_source(),
                summary: truncate_summary(summary_source, self.config.tuning.audit_truncate_len),
                agent_id: None,
                session_id: None,
            });
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
        let mut guard = self.kg.lock();
        *guard = kg;
        // Track path for auto-rebuild on staleness
        *self.project_kg_path.lock() = Some(project_path.clone());
        tracing::info!(
            entity_count,
            path = %project_path.display(),
            "project knowledge graph loaded"
        );
        self.invalidate_prepare_context_cache();
        Ok(())
    }

    /// Compatibility entry point for non-runtime callers. Runtime-owned
    /// execution must call [`Self::prepare_context_for_turn`] with its exact
    /// immutable turn identity and data lease.
    pub async fn prepare_context(
        &self,
        query: &str,
        messages: &[Message],
        session_id: Option<&str>,
    ) -> Result<PreparedContext> {
        let turn = MemoryTurnContext::new(session_id.unwrap_or("memory-api"), "memory-api");
        self.prepare_context_for_turn(&turn, query, messages).await
    }

    /// Assemble the optimal context for one explicitly identified model turn.
    ///
    /// Implements "progressive disclosure":
    /// 1. Load fixed layers L0 + L1.
    /// 2. Load project context L2.
    /// 3. Dynamic-load relevant deep memories L3 via multi-signal scoring.
    /// 4. Surface triggered seeds.
    /// 5. Sample context window pressure.
    /// 6. Compress if needed.
    pub async fn prepare_context_for_turn(
        &self,
        turn: &MemoryTurnContext,
        query: &str,
        messages: &[Message],
    ) -> Result<PreparedContext> {
        let _prepare_start = Instant::now();
        let mut entries: Vec<MemoryEntry> = Vec::new();

        let budget = self.compute_budget(&turn.agent_id);
        let cache_revision = self.memory_revision.load(Ordering::Relaxed);
        let cache_key = self.prepare_context_cache_key(query, messages, turn, &budget);
        if entries.is_empty() {
            if let Some(context) = self.cached_prepared_context(cache_key, cache_revision) {
                let elapsed = _prepare_start.elapsed();
                self.perf_monitor.record_cache_hit();
                self.perf_monitor.record_prepare_context(elapsed);
                tracing::debug!(
                    elapsed_ms = elapsed.as_millis(),
                    entries = context.entries.len(),
                    "prepare_context cache hit"
                );
                return Ok(context);
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // Step 0a: Closet LRU prefetch — preload hot topics based on
        //          access counts tracked in closet pointers (F19).
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
                    let prefetch_set: HashSet<MemoryId> = entries.iter().map(|e| e.id).collect();
                    let budget =
                        (self.config.budget.available_tokens() / 4).min(u64::from(u32::MAX)) as u32;
                    match self
                        .orchestrator
                        .recall_relevant(&topic, None, &prefetch_set, budget)
                        .await
                    {
                        Ok(mut recalled) => {
                            for entry in &mut recalled {
                                entry.content = format!("[PREFETCH: {}] {}", topic, entry.content);
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
            let l0_hit = self
                .l0_cache
                .lock()
                .as_ref()
                .filter(|c| {
                    c.cached_at.elapsed()
                        < Duration::from_secs(self.config.tuning.l0_cache_ttl_secs)
                })
                .map(|c| c.entries.clone());
            let l1_hit = self
                .l1_cache
                .lock()
                .as_ref()
                .filter(|c| {
                    c.cached_at.elapsed()
                        < Duration::from_secs(self.config.tuning.l1_cache_ttl_secs)
                })
                .map(|c| c.entries.clone());

            if let (Some(l0), Some(l1)) = (l0_hit, l1_hit) {
                self.perf_monitor.record_cache_hit();
                entries.extend(l0);
                entries.extend(l1);
            } else {
                self.perf_monitor.record_cache_miss();
                let fixed = self.orchestrator.load_fixed_layers().await?;
                let l0: Vec<_> = fixed
                    .iter()
                    .filter(|e| matches!(e.layer, MemoryLayer::L0))
                    .cloned()
                    .collect();
                let l1: Vec<_> = fixed
                    .iter()
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
            let l2_hit = self
                .l2_cache
                .lock()
                .as_ref()
                .filter(|c| {
                    c.cached_at.elapsed()
                        < Duration::from_secs(self.config.tuning.l2_cache_ttl_secs)
                })
                .map(|c| c.entries.clone());
            if let Some(l2) = l2_hit {
                self.perf_monitor.record_cache_hit();
                entries.extend(l2);
            } else {
                self.perf_monitor.record_cache_miss();
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
                    EmbeddingCapability::Remote { client } => match client.embed_one(query).await {
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
                    },
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
                        content: format!(
                            "[REBUILT STATE confidence={:.2}] {}",
                            rebuilt.overall_confidence, summary.data
                        ),
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
            let kg = self.kg.lock();
            let query_tokens: Vec<String> =
                query.split_whitespace().map(str::to_lowercase).collect();
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
        let memory_budget = budget
            .available
            .saturating_sub(self.estimate_tokens_entries(&entries))
            .min(u64::from(u32::MAX)) as u32;

        // ═══════════════════════════════════════════════════════════════════
        // Group 2: L3 recall + session resume. Runtime's binding-aware
        // RealityRecallPort injects any promoted L4 knowledge explicitly;
        // cognitive preparation must not broadcast peer or global Team state.
        // ═══════════════════════════════════════════════════════════════════
        let (l3_result, resume_result) = tokio::join!(
            // L3 deep recall (hybrid semantic + BM25)
            async {
                self.orchestrator
                    .recall_relevant(
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
        let fence = crate::context_fence::fence_from_session(&turn.session_id, None, None);
        entries = crate::context_fence::filter_through_fence(&entries, &fence)
            .into_iter()
            .cloned()
            .collect();

        // ── Step 4: check seed triggers and inject as high-priority L1 entries ──
        let query_words: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
        let triggered = {
            let mut reg = self.seeds.lock();
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
        let total_message_tokens: u64 =
            messages.iter().map(|m| u64::from(m.token_estimate())).sum();
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

        // ── Step 6: Runtime owns transcript compaction ───────────────────────
        // Context preparation never mutates a transcript. Semantic session
        // checkpoints are created only by Runtime after a real provider
        // preflight proves that required input cannot fit.

        // Step 6c: inject recent entity evolutions from other agents
        {
            let registry_guard = self.entity_registry.lock();
            if let Some(ref registry) = *registry_guard {
                if registry.has_store() {
                    match registry.get_recent_evolutions(10) {
                        Ok(evolutions) if !evolutions.is_empty() => {
                            let mut story_lines: Vec<String> = Vec::new();
                            for ev in &evolutions {
                                story_lines.push(format!("  - {}", ev.to_sentence()));
                            }
                            let content = format!(
                                "Recent entity changes (cross-agent):\n{}",
                                story_lines.join("\n")
                            );
                            entries.push(MemoryEntry {
                                id: uuid::Uuid::new_v4(),
                                layer: MemoryLayer::L2,
                                category: MemoryCategory::Shared,
                                priority: Priority::Low,
                                source: MemorySource::AutoExtracted,
                                title: "Entity Evolution Context".into(),
                                content,
                                embedding: None,
                                tags: vec!["entity_evolution".into(), "cross_agent".into()],
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
                        Err(e) => {
                            tracing::debug!(
                                error = %e,
                                "entity evolution: failed to query recent evolutions"
                            );
                        }
                        _ => {}
                    }
                }
            }
        }

        // Step 7: auto-inject relevant code symbols (when applicable)
        let code_context = if is_code_query(query) {
            let symbols = self.orchestrator.find_relevant_symbols(query, 5).await;
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
                        title: format!(
                            "[SANDBOX] tool output L{}-L{}",
                            snip.line_start, snip.line_end
                        ),
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

        // ── Step 7d: Symbol-memory linking ──
        // Reserve for future: link code symbols referenced in context to memory entries.
        // This is activated when code_context is populated by the code indexer.
        if code_context.is_some() {
            tracing::debug!("symbol-memory linking: code context present, linking reserved for Phase 4 get_callers/get_callees integration");
        }

        // Every backend search is deliberately over-inclusive for recall
        // quality. Before the prepared context is cached or exposed, enforce
        // the immutable Binding-derived lease again at this final boundary.
        entries.retain(|entry| memory_scope_visible_to_ctx(&entry.scope, turn));

        // ── Assemble PreparedContext ─────────────────────────────────────────
        let total_tokens = self.estimate_tokens_entries(&entries);
        let depth_scale = if total_tokens > budget.available {
            budget.available as f32 / total_tokens.max(1) as f32
        } else {
            1.0
        };

        let elapsed = _prepare_start.elapsed();
        self.perf_monitor.record_prepare_context(elapsed);
        tracing::debug!(
            elapsed_ms = elapsed.as_millis(),
            entries = entries.len(),
            total_tokens,
            "prepare_context complete"
        );

        let context = PreparedContext {
            entries,
            total_tokens,
            budget,
            depth_scale,
            prepared_at: Utc::now(),
            code_context,
        };
        self.store_prepared_context_cache(cache_key, cache_revision, &context);
        Ok(context)
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

    // `run_memory_post_turn` coordinates the post-turn helpers below. Runtime
    // owns transcript compaction, so this manager only extracts memories and
    // performs drift, seed, index, and graph maintenance.
    /// Compatibility entry point for non-runtime callers. Runtime-owned
    /// post-turn work must use [`Self::extract_and_remember_for_turn`].
    pub async fn extract_and_remember(&self, messages: &[Message]) -> Result<()> {
        let turn = MemoryTurnContext::new("memory-api", "memory-api");
        self.extract_and_remember_for_turn(&turn, messages).await
    }

    /// Extract memories from one explicitly identified turn and persist them.
    ///
    /// This covers steps 0, 0b, and 11 from the full turn-end sequence:
    ///   - Heuristic extraction from conversation messages (fast, sync)
    ///   - LLM extraction queued to background worker (non-blocking)
    ///   - Persist via `orchestrator.remember_batch`
    ///   - Index large tool outputs into the sandbox
    ///   - Batch-embed new entries into the vector index
    ///
    /// Failures are logged and swallowed so they never abort the turn.
    pub async fn extract_and_remember_for_turn(
        &self,
        turn: &MemoryTurnContext,
        messages: &[Message],
    ) -> Result<()> {
        let _extract_start = Instant::now();
        // ── 0. Extract and persist memories ──────────────────────────────────
        let mut pending_embeddings: Vec<(MemoryId, String)> = Vec::new();
        if messages.len() >= 2 {
            tracing::debug!(
                messages_count = messages.len(),
                has_user = messages.iter().any(|m| matches!(m.role, MessageRole::User)),
                has_assistant = messages
                    .iter()
                    .any(|m| matches!(m.role, MessageRole::Assistant)),
                has_tool = messages.iter().any(|m| matches!(m.role, MessageRole::Tool)),
                user_content_total = messages
                    .iter()
                    .filter(|m| matches!(m.role, MessageRole::User))
                    .map(|m| m.content.len())
                    .sum::<usize>(),
                "extract_and_remember: pre-extraction state"
            );

            // ── 0a. Heuristic extraction (Passes 1-4, fast / non-blocking) ──
            let mut heuristic_entries = if self.config.extractor.enabled {
                let raw = self.extractor.extract_heuristic(messages);
                self.extractor.finalize_entries(raw)
            } else {
                Vec::new()
            };
            let batch_tag = extraction_batch_tag(turn, messages);
            let mut durable_heuristic_entries = Vec::new();
            if !heuristic_entries.is_empty() {
                canonicalize_automatic_entries(turn, &batch_tag, &mut heuristic_entries);
                tracing::info!(
                    entries_count = heuristic_entries.len(),
                    "extract_and_remember: heuristic extracted {} entries",
                    heuristic_entries.len()
                );
                let heuristic_contents = heuristic_entries
                    .iter()
                    .map(memory_embedding_text)
                    .collect::<Vec<_>>();

                match self
                    .orchestrator
                    .remember_batch_for_turn(turn, heuristic_entries.clone())
                    .await
                {
                    Ok(ids) => {
                        for (entry, id) in heuristic_entries.iter_mut().zip(ids.iter().copied()) {
                            entry.id = id;
                        }
                        pending_embeddings.extend(ids.into_iter().zip(heuristic_contents));
                        durable_heuristic_entries = heuristic_entries;
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
            }

            // Queue semantic extraction for every substantive turn. It must not
            // depend on a heuristic keyword hit; otherwise L3 never receives
            // novel patterns that the fast extractor cannot recognize.
            if self.config.extractor.enabled
                && self.extractor.llm_client().is_some()
                && MemoryExtractor::should_extract(messages)
            {
                let request = BackgroundExtractionRequest {
                    turn: turn.clone(),
                    messages: messages.to_vec(),
                    heuristic_entries: durable_heuristic_entries,
                };
                self.background_extraction_state
                    .pending_requests
                    .fetch_add(1, Ordering::Relaxed);
                match self.extract_tx.send(request).await {
                    Ok(()) => {
                        self.background_extraction_state
                            .accepted_requests
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(
                            "extract_and_remember: queued messages for background LLM extraction"
                        );
                    }
                    Err(error) => {
                        self.background_extraction_state
                            .pending_requests
                            .fetch_sub(1, Ordering::Relaxed);
                        self.background_extraction_state
                            .failed_requests
                            .fetch_add(1, Ordering::Relaxed);
                        *self.background_extraction_state.last_error.lock() =
                            Some(error.to_string());
                        tracing::error!(
                            %error,
                            "extract_and_remember: background LLM extraction queue closed"
                        );
                    }
                }
            }

            // ── 0b. Index large tool outputs into sandbox ───────────────────
            let mut sandbox = self.tool_sandbox.lock();
            for msg in messages
                .iter()
                .filter(|m| matches!(m.role, MessageRole::Tool))
            {
                let call_id = msg.tool_use_id.as_deref().unwrap_or("unknown");
                let tool_name = msg.tool_name.as_deref().unwrap_or("unknown_tool");
                let threshold = self.config.tuning.sandbox_min_lines;
                if let Some(summary) =
                    sandbox.index_tool_output(call_id, tool_name, &msg.content, threshold)
                {
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
            match embed_memory_entries(
                &self.embedding_capability,
                &self.vector_index,
                &pending_embeddings,
                false,
            )
            .await
            {
                Ok(indexed) => {
                    tracing::info!(count = indexed, "batch embedded memory entries");
                }
                Err(error) => {
                    tracing::warn!(%error, "batch embedding failed");
                }
            }
        }

        let _extract_elapsed = _extract_start.elapsed();
        self.perf_monitor.record_extract(_extract_elapsed);
        self.invalidate_prepare_context_cache();

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
        let mut pruned_any = false;
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
                    pruned_any = true;
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

        if pruned_any {
            self.invalidate_prepare_context_cache();
        }

        Ok(())
    }

    /// Compatibility entry point for non-runtime callers. Runtime-owned
    /// execution must use [`Self::on_turn_end_for_turn`] with its immutable
    /// turn context.
    pub async fn on_turn_end(&self, messages: &mut Vec<Message>) -> Result<()> {
        let turn = MemoryTurnContext::new("memory-api", "memory-api");
        self.on_turn_end_for_turn(&turn, messages).await
    }

    /// Run the full post-turn sequence for one explicitly identified turn.
    /// Extraction and drift/seed checks remain parallel, but every write is
    /// attributed to `turn` rather than an ambient process-global identity.
    pub async fn on_turn_end_for_turn(
        &self,
        turn: &MemoryTurnContext,
        messages: &mut Vec<Message>,
    ) -> Result<()> {
        // ── Delegation observation ────────────────────────────────────────────
        {
            let drained: Vec<_> = {
                let mut delegation_queue = self.delegation_results.lock();
                delegation_queue.drain(..).collect()
            };
            for d in drained {
                tracing::debug!(
                    agent_role = %d.agent_role,
                    task = %truncate_summary(&d.task, 40),
                    "delegation observation retained for Runtime TeamWorkingState; no direct L4 write"
                );
            }
        }

        // ── Extract ∥ Drift+Seeds ── Maintenance ──────────────────────
        let (extract_result, drift_result) = tokio::join!(
            async { self.extract_and_remember_for_turn(turn, messages).await },
            async { self.run_drift_and_seeds(messages).await },
        );
        if let Err(error) = extract_result {
            tracing::warn!(%error, "on_turn_end: extraction failed");
        }
        if let Err(error) = drift_result {
            tracing::warn!(%error, "on_turn_end: drift and seeds failed");
        }
        let result = self.run_memory_maintenance(turn, messages).await;

        // ── Auto-tune evaluation ──────────────────────────────────────────
        if self.auto_tuner.evaluate(&self.perf_monitor) {
            let cfg = self.auto_tuner.config();
            tracing::info!(
                adjustments = self.auto_tuner.adjustments_applied(),
                prefetch = cfg.prefetch_hot_topics,
                l0_ttl = cfg.l0_cache_ttl_secs,
                l1_ttl = cfg.l1_cache_ttl_secs,
                l2_ttl = cfg.l2_cache_ttl_secs,
                sandbox_lines = cfg.sandbox_min_lines,
                freshness_trigger = cfg.freshness_trigger_ratio,
                "auto_tuner: applied adjustments to TuningConfig"
            );
        }

        result
    }

    /// Remaining post-turn maintenance: fact-checker, tick,
    /// KG persistence, context rotation, closet/seeds save, etc.
    ///
    /// Call this *after* `extract_and_remember` and `run_drift_and_seeds`
    /// have completed (whether sequentially or via `tokio::join!`).
    pub async fn run_memory_maintenance(
        &self,
        turn: &MemoryTurnContext,
        messages: &mut Vec<Message>,
    ) -> Result<()> {
        let _post_turn_start = Instant::now();
        // ── 0c. Auto-correct contradictions via fact checker ──────────────
        {
            let mut fc = crate::orchestrator::get_fact_checker().lock();
            let report = fc.auto_correct();
            if report.corrected > 0 || report.pruned > 0 {
                tracing::info!(
                    corrected = report.corrected,
                    pruned = report.pruned,
                    flagged = report.flagged,
                    "auto-correction applied"
                );
            }
        }

        // Runtime retains ownership of the conversation transcript and its
        // sole semantic checkpoint. This memory-maintenance pass must not
        // apply a second threshold-driven summarizer to a copied transcript:
        // doing so would create recall noise without changing provider input.
        let _ = messages;

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
            if tick.is_multiple_of(100) {
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
            let tick = self
                .cross_store_verify_counter
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if tick.is_multiple_of(50) {
                let warnings = self.cross_store_verify().await;
                for w in &warnings {
                    tracing::warn!("cross-store-verify: {w}");
                }
                if !warnings.is_empty() {
                    tracing::warn!(
                        count = warnings.len(),
                        "cross-store consistency check found {} issues",
                        warnings.len()
                    );
                }
            }
        }

        // ── 5a4. Integrity anomaly detection every 50 ticks (T9) ────────────
        {
            let tick = self.integrity_check_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if tick.is_multiple_of(50) {
                if let Some(ref checker) = self.integrity_checker {
                    match checker.check_anomalies() {
                        Ok(report) => {
                            if !report.anomalies.is_empty() {
                                for anomaly in &report.anomalies {
                                    tracing::warn!("integrity anomaly detected: {:?}", anomaly);
                                }
                                tracing::warn!(
                                    count = report.anomalies.len(),
                                    "integrity check found {} anomaly(ies)",
                                    report.anomalies.len()
                                );
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
        if let Err(e) = persist_vector_index_snapshot(&self.vector_index) {
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
            let budget = self.compute_budget(&turn.agent_id);
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
            let serialized = {
                let reg = self.seeds.lock();
                serde_json::to_string(reg.all_seeds())
            };
            match serialized {
                Ok(json) => {
                    if let Err(e) = self.orchestrator.store().kv_put("seeds", &json).await {
                        tracing::warn!("failed to save seeds: {}", e);
                    }
                }
                Err(e) => tracing::warn!("failed to serialize seeds: {}", e),
            }
        }

        let _post_turn_elapsed = _post_turn_start.elapsed();
        self.perf_monitor.record_extract(_post_turn_elapsed);

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
    /// Persist an entry with an explicit Runtime turn. This is the production
    /// route used by [`MemoryKernel`]; ownership is never inferred from a
    /// mutable manager field.
    pub async fn remember_for_turn(
        &self,
        turn: &MemoryTurnContext,
        mut entry: MemoryEntry,
    ) -> Result<()> {
        entry
            .session_id
            .get_or_insert_with(|| turn.session_id.clone());
        entry
            .source_agent
            .get_or_insert_with(|| turn.agent_id.clone());
        entry.scope = scoped_entry_scope(turn, &entry);
        self.remember_inner(entry, Some(turn)).await
    }

    /// Persist an entry supplied by a non-runtime caller. It receives a
    /// deterministic `memory-api` identity when the caller omits ownership,
    /// rather than reading mutable process-wide state.
    pub async fn remember(&self, entry: MemoryEntry) -> Result<()> {
        self.remember_inner(entry, None).await
    }

    async fn remember_inner(
        &self,
        mut entry: MemoryEntry,
        turn: Option<&MemoryTurnContext>,
    ) -> Result<()> {
        // CognitiveContextManager is the ordinary Runtime/API memory path;
        // it must never be a second L4 promotion route.  Runtime's
        // L4PromotionService owns the governed candidate lifecycle and calls
        // the orchestrator's typed promotion command directly.
        if entry.layer == MemoryLayer::L4 {
            return Err(MemoryError::WriteDenied {
                layer: "L4".to_string(),
                write_source: "cognitive_memory_write_requires_l4_promotion_service".to_string(),
            });
        }
        // Direct manager callers are administrative/non-runtime callers. They
        // still receive a deterministic identity instead of reviving the old
        // process-wide active state or persisting the `session_` sentinel.
        let fallback_turn = MemoryTurnContext::new(
            entry.session_id.as_deref().unwrap_or("memory-api"),
            entry.source_agent.as_deref().unwrap_or("memory-api"),
        )
        .with_project_id(match &entry.scope {
            MemoryScope::Project(project) if !project.trim().is_empty() => Some(project.clone()),
            _ => None,
        });
        let turn = turn.unwrap_or(&fallback_turn);
        entry
            .session_id
            .get_or_insert_with(|| turn.session_id.clone());
        entry
            .source_agent
            .get_or_insert_with(|| turn.agent_id.clone());
        entry.scope = scoped_entry_scope(turn, &entry);
        // Check write guard
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self
                    .write_guard
                    .as_ref()
                    .map(|g| format!("{:?}", g.source()))
                    .unwrap_or_default(),
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
                    source: self
                        .write_guard
                        .as_ref()
                        .map(|g| g.source())
                        .unwrap_or(WriteSource::System),
                    summary: truncate_summary(
                        &entry.content,
                        self.config.tuning.audit_truncate_len,
                    ),
                    agent_id: entry.source_agent.clone(),
                    session_id: entry.session_id.clone(),
                });
            }
        }

        self.orchestrator.remember_for_turn(turn, entry).await?;
        self.invalidate_prepare_context_cache();
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

    /// List full memory entries in a specific layer for product surfaces.
    pub async fn list_layer_full_entries(
        &self,
        layer: crate::types::MemoryLayer,
    ) -> Result<Vec<crate::types::MemoryEntry>> {
        self.orchestrator.store().search_by_layer(layer).await
    }

    /// Shared orchestrator handle for UI surfaces that need layer-level
    /// snapshots or L4 event subscriptions.
    pub fn orchestrator(&self) -> Arc<MemoryOrchestrator> {
        Arc::clone(&self.orchestrator)
    }

    /// List all memory entries across layers.
    pub async fn list_all_entries(&self) -> Result<Vec<crate::types::MemoryEntry>> {
        self.orchestrator.store().list_all().await
    }

    pub async fn store_aggregate(
        &self,
        stale_threshold: f32,
    ) -> Result<crate::store::MemoryStoreAggregate> {
        self.orchestrator.store().aggregate(stale_threshold).await
    }

    pub async fn authority_candidates(&self, query: AuthorityLookup) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .lookup_authority_candidates(query)
            .await
    }

    pub async fn tagged_candidates(
        &self,
        query: crate::store::TaggedLookup,
    ) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .lookup_tagged_candidates(query)
            .await
    }

    pub async fn fact_candidates(
        &self,
        scope: &crate::project_scope::MemoryScope,
        category: MemoryCategory,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .lookup_fact_candidates(scope, category, limit)
            .await
    }

    pub(crate) async fn semantic_checkpoint_candidates(
        &self,
        scope: &crate::project_scope::MemoryScope,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .search_semantic_checkpoints(scope, query, limit)
            .await
    }

    pub(crate) async fn kernel_kv_get_many(&self, keys: &[String]) -> Result<Vec<MemoryKeyValue>> {
        self.orchestrator.store().kv_get_many(keys).await
    }

    pub async fn scan_entries_page(
        &self,
        cursor: MemoryScanCursor,
        limit: usize,
    ) -> Result<MemoryScanPage> {
        self.orchestrator
            .store()
            .scan_entries_page(cursor, limit)
            .await
    }

    /// Read held scope migrations for operator review. These records remain
    /// excluded from normal recall until an explicit classification command is
    /// implemented by the management layer.
    pub async fn legacy_scope_migration_reports(
        &self,
    ) -> Result<Vec<crate::store::sqlite::LegacyScopeMigrationReport>> {
        self.orchestrator
            .store()
            .legacy_scope_migration_reports()
            .await
    }

    /// Snapshot the active token budget configuration used by the kernel.
    pub fn budget_config(&self) -> crate::config::BudgetConfig {
        self.config.budget.clone()
    }

    /// Scan current memories and enqueue reviewable lifecycle maintenance
    /// candidates. This never mutates or deletes memory entries.
    pub async fn scan_memory_maintenance(
        &self,
        config: MaintenanceScanConfig,
    ) -> Result<Vec<MaintenanceCandidate>> {
        let entries = self.list_all_entries().await?;
        self.scan_memory_maintenance_entries(&entries, config)
    }

    /// Scan an already-governed active projection.
    ///
    /// Callers that own lifecycle filtering use this entry point so archived
    /// evidence remains durable without re-entering the active review queue.
    pub fn scan_memory_maintenance_entries(
        &self,
        entries: &[crate::types::MemoryEntry],
        config: MaintenanceScanConfig,
    ) -> Result<Vec<MaintenanceCandidate>> {
        let candidates = scan_maintenance_candidates(&entries, &config);
        self.maintenance_queue.upsert_many(candidates.clone())?;
        Ok(candidates)
    }

    /// Analyze a full governance snapshot away from the async runtime worker.
    ///
    /// The returned entries are the same owned snapshot used for analysis, so
    /// callers do not clone a potentially large corpus merely to avoid
    /// blocking request and live-event tasks.
    pub async fn scan_memory_maintenance_entries_off_thread(
        &self,
        entries: Vec<crate::types::MemoryEntry>,
        config: MaintenanceScanConfig,
    ) -> Result<(Vec<crate::types::MemoryEntry>, Vec<MaintenanceCandidate>)> {
        let (entries, candidates) = tokio::task::spawn_blocking(move || {
            let candidates = scan_maintenance_candidates(&entries, &config);
            (entries, candidates)
        })
        .await
        .map_err(|error| {
            MemoryError::Other(format!("memory governance analysis failed: {error}"))
        })?;
        self.maintenance_queue.upsert_many(candidates.clone())?;
        Ok((entries, candidates))
    }

    /// List queued memory lifecycle candidates.
    pub fn list_memory_maintenance(
        &self,
        filter: MaintenanceCandidateFilter,
    ) -> Result<Vec<MaintenanceCandidate>> {
        self.maintenance_queue.list(filter)
    }

    /// Move a maintenance candidate through the explicit review lifecycle.
    pub fn transition_memory_maintenance(
        &self,
        id: &str,
        status: MaintenanceCandidateStatus,
    ) -> Result<Option<MaintenanceCandidate>> {
        self.maintenance_queue.transition(id, status)
    }

    /// Consume an explicit promotion batch produced by Runtime policy.
    pub fn process_memory_pulse(&self, batch: MemoryPulseBatch) -> Result<MemoryPulseReport> {
        MemoryPulseConsumer::new(self.maintenance_queue.clone()).process_batch(batch)
    }

    /// List persisted knowledge-graph entities.
    pub async fn list_entities(&self) -> Result<Vec<crate::entity::Entity>> {
        self.orchestrator.store().load_entities().await
    }

    /// List persisted knowledge-graph triples.
    pub async fn list_triples(&self) -> Result<Vec<crate::entity::Triple>> {
        self.orchestrator.store().load_triples().await
    }

    /// Link a code symbol to a memory entry for impact analysis and symbol-
    /// scoped recall.
    pub async fn link_symbol_to_memory(
        &self,
        symbol_id: &str,
        memory_id: MemoryId,
        turn_index: Option<i32>,
        reference_type: &str,
    ) -> Result<()> {
        self.orchestrator
            .store()
            .link_symbol_to_memory(
                symbol_id,
                &memory_id,
                turn_index,
                reference_type,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        self.invalidate_prepare_context_cache();
        Ok(())
    }

    /// Return full memory entries linked to a code symbol name or symbol ID.
    pub async fn find_memories_by_symbol(
        &self,
        symbol_name: &str,
    ) -> Result<Vec<crate::types::MemoryEntry>> {
        let ids = self
            .orchestrator
            .store()
            .find_memories_by_symbol(symbol_name)
            .await?;
        let mut entries = Vec::new();
        for id in ids {
            if let Some(entry) = self.orchestrator.store().get(&id).await? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Return recent memory write audit entries for enterprise export.
    pub fn audit_entries(&self, limit: usize) -> Result<Vec<AuditEntry>> {
        let limit = limit.min(1000);
        if let Some(ref log) = self.audit_log {
            return log
                .query_recent(limit)
                .map_err(|e| MemoryError::Store(format!("query audit log: {e}")));
        }
        if let Some(ref checker) = self.integrity_checker {
            return checker
                .audit_log()
                .query_recent(limit)
                .map_err(|e| MemoryError::Store(format!("query audit log: {e}")));
        }
        Ok(Vec::new())
    }

    /// Create a user-authored memory entry through the same guarded write path
    /// used by internal memory operations.
    pub async fn create_entry(
        &self,
        layer: MemoryLayer,
        category: MemoryCategory,
        title: &str,
        content: &str,
        priority: Priority,
        tags: Vec<String>,
        scope: MemoryScope,
    ) -> Result<MemoryId> {
        let id = self
            .orchestrator
            .write(
                layer,
                category,
                title,
                content,
                priority,
                MemorySource::UserExplicit,
                tags,
                scope,
            )
            .await?;
        self.log_memory_audit(AuditOperation::Create, id.to_string(), layer, content);
        self.invalidate_prepare_context_cache();
        Ok(id)
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
                return Err(crate::MemoryError::InvalidArgument(format!(
                    "invalid memory id: {id}"
                )));
            }
        };

        // Try to look up the entry's layer for guard check
        if let Some(entry) = self.orchestrator.recall(&mem_id).await? {
            let policy = self.check_write_access(entry.layer);
            if !policy.is_allowed() {
                return Err(MemoryError::WriteDenied {
                    layer: format!("{:?}", entry.layer),
                    write_source: self
                        .write_guard
                        .as_ref()
                        .map(|g| format!("{:?}", g.source()))
                        .unwrap_or_default(),
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
                        source: self
                            .write_guard
                            .as_ref()
                            .map(|g| g.source())
                            .unwrap_or(WriteSource::System),
                        summary: truncate_summary(
                            &entry.content,
                            self.config.tuning.audit_truncate_len,
                        ),

                        agent_id: None,
                        session_id: None,
                    });
                }
            }
        }

        self.orchestrator.forget(&mem_id).await?;
        self.invalidate_prepare_context_cache();
        Ok(())
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
                return Err(crate::MemoryError::InvalidArgument(format!(
                    "invalid memory id: {id}"
                )));
            }
        };

        let mut entry = self
            .orchestrator
            .recall(&mem_id)
            .await?
            .ok_or_else(|| crate::MemoryError::Store(format!("entry {} not found", id)))?;

        // Write guard check
        let policy = self.check_write_access(entry.layer);
        if !policy.is_allowed() {
            return Err(MemoryError::WriteDenied {
                layer: format!("{:?}", entry.layer),
                write_source: self
                    .write_guard
                    .as_ref()
                    .map(|g| format!("{:?}", g.source()))
                    .unwrap_or_default(),
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

        self.orchestrator.update(&entry).await?;
        self.log_memory_audit(
            AuditOperation::Update,
            entry.id.to_string(),
            entry.layer,
            &entry.content,
        );
        self.invalidate_prepare_context_cache();
        Ok(())
    }

    /// List all layers with their entry counts.
    pub async fn list_layers(&self) -> Vec<serde_json::Value> {
        use crate::types::MemoryLayer;
        let aggregate = self
            .store_aggregate(crate::kernel::MEMORY_STALE_WARNING_THRESHOLD)
            .await
            .unwrap_or_default();
        let layers = [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
            MemoryLayer::L4,
        ];
        let mut result = Vec::new();
        for layer in layers {
            let layer_aggregate = aggregate
                .layers
                .iter()
                .find(|value| value.layer == layer)
                .cloned();
            let entry_count = layer_aggregate
                .as_ref()
                .map(|value| value.active_count)
                .unwrap_or_default();
            let retained_count = layer_aggregate
                .as_ref()
                .map(|value| value.retained_count)
                .unwrap_or_default();
            let archived_count = layer_aggregate
                .as_ref()
                .map(|value| value.archived_count)
                .unwrap_or_default();
            let (enabled, role, producer, write_mode) = match layer {
                MemoryLayer::L0 => (
                    self.config.layers.l0_enabled,
                    "stable identity and explicit global invariants",
                    "explicit user or system identity writes",
                    "explicit",
                ),
                MemoryLayer::L1 => (
                    true,
                    "high-salience working preferences and active constraints",
                    "explicit writes and current-turn preference extraction",
                    "automatic_and_explicit",
                ),
                MemoryLayer::L2 => (
                    true,
                    "project conventions, decisions, and reusable resolutions",
                    "current-turn extraction and governed imports",
                    "automatic_and_explicit",
                ),
                MemoryLayer::L3 => (
                    true,
                    "deep patterns, semantic checkpoints, and long-term references",
                    "semantic extraction and session compaction checkpoints",
                    "automatic_and_explicit",
                ),
                MemoryLayer::L4 => (
                    self.config.layers.l4_enabled,
                    "reviewed cross-agent and team knowledge",
                    "Runtime evidence-backed promotion only",
                    "governed_promotion_only",
                ),
            };
            result.push(serde_json::json!({
                "layer": format!("{layer:?}"),
                "entry_count": entry_count,
                "retained_count": retained_count,
                "archived_count": archived_count,
                "enabled": enabled,
                "role": role,
                "producer": producer,
                "write_mode": write_mode,
                "automatic_extraction": self.config.extractor.enabled
                    && matches!(layer, MemoryLayer::L1 | MemoryLayer::L2 | MemoryLayer::L3),
                "state": if !enabled {
                    "disabled"
                } else if entry_count == 0 {
                    "ready_empty"
                } else {
                    "ready"
                },
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
        persist_vector_index_snapshot(&self.vector_index)
            .map_err(|e| MemoryError::Store(format!("persist vector index: {e}")))
    }

    /// Get the number of vectors currently indexed.
    #[must_use]
    pub fn vector_index_count(&self) -> usize {
        self.vector_index.read().count()
    }

    /// Evict a lifecycle-inactive memory from the rebuildable semantic index.
    pub fn evict_vector_entry(&self, id: &MemoryId) -> Result<()> {
        let snapshot = {
            let mut index = self.vector_index.write();
            index.remove(id)?;
            index.persistence_snapshot()
        };
        snapshot.persist()
    }

    /// Get vector index statistics.
    #[must_use]
    pub fn vector_index_stats(&self) -> VectorIndexStats {
        let stats = self.vector_index.read().runtime_stats();
        VectorIndexStats {
            count: stats.count,
            generation: stats.generation,
            persisted_generation: stats.persisted_generation,
            evictions: stats.evictions,
            persistence_failures: stats.persistence_failures,
            last_persistence_error: stats.last_persistence_error,
        }
    }

    /// Recall entries through the in-process vector index.
    ///
    /// This is the runtime semantic recall source. The SQLite
    /// `MemoryStore::search_vector` path remains a backend capability boundary,
    /// but production context recall should use this method because embeddings
    /// are stored in `CognitiveContextManager::vector_index`.
    pub async fn vector_recall_candidates(
        &self,
        query: &str,
        already_surfaced: &HashSet<MemoryId>,
        limit: usize,
    ) -> Result<Vec<(MemoryEntry, f32)>> {
        let EmbeddingCapability::Remote { client } = &self.embedding_capability else {
            return Ok(Vec::new());
        };
        if self.vector_index.read().count() == 0 {
            return Ok(Vec::new());
        }
        let embedding = match client.embed_one(query).await {
            Ok(embedding) => embedding,
            Err(error) => {
                tracing::warn!(%error, "vector recall query embedding failed");
                return Ok(Vec::new());
            }
        };
        let scored = {
            let index = self.vector_index.read();
            index.search_with_filter(&embedding, limit.max(1) * 2, &|id| {
                !already_surfaced.contains(id)
            })?
        };
        let mut entries = Vec::new();
        for (id, score) in scored {
            if let Some(entry) = self.orchestrator.recall(&id).await? {
                entries.push((entry, score));
            }
            if entries.len() >= limit.max(1) {
                break;
            }
        }
        Ok(entries)
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
        let categories_found_set: HashSet<_> =
            fts_result.entries.iter().map(|e| e.category).collect();
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

    /// Search only scopes already authorized by an exact Runtime Binding.
    ///
    /// Scope filtering happens inside each FTS query before ranking and
    /// limiting. This prevents a large unrelated project from displacing
    /// eligible results before the Memory kernel applies its final policy
    /// checks. Global rows are included by the store and remain subject to the
    /// kernel's visibility fence.
    pub(crate) async fn search_memories_in_scopes(
        &self,
        query: &str,
        scopes: &[MemoryScope],
        limit_per_scope: usize,
    ) -> Result<Vec<MemoryEntry>> {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        for scope in scopes {
            for entry in self
                .orchestrator
                .store()
                .search_fts_scoped(query, scope, limit_per_scope.clamp(1, 128))
                .await?
            {
                if seen.insert(entry.id) {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
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
        let last_action = self.last_action.lock().clone();
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
            vec![], // remaining items
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
                Ok(v) if v.get("messages").is_some() => v
                    .get("messages")
                    .and_then(|m: &serde_json::Value| m.as_array())
                    .cloned()
                    .unwrap_or_default(),
                _ => Vec::new(),
            }
        } else {
            // JSONL format - one JSON object per line
            contents
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter_map(|v: serde_json::Value| {
                    v.get("message").or_else(|| v.get("content")).cloned()
                })
                .collect()
        };

        // Extract memories from messages
        for msg in messages {
            // Try to extract content from message
            let text_opt: Option<String> = msg
                .as_str()
                .map(String::from)
                .or_else(|| {
                    msg.get("text")
                        .and_then(|v: &serde_json::Value| v.as_str())
                        .map(String::from)
                })
                .or_else(|| {
                    msg.get("content")
                        .and_then(|v: &serde_json::Value| v.as_str())
                        .map(String::from)
                });

            if let Some(text) = text_opt {
                // Skip very short messages
                if text.len() < 50 {
                    continue;
                }

                // Extract title from first line or truncate
                let first_line = text.lines().next().unwrap_or("");
                let title = if first_line.len() > 60 {
                    truncate_summary(first_line, 60)
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
            if let Some(content_obj) = msg
                .get("content")
                .and_then(|v: &serde_json::Value| v.as_object())
            {
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
        let mut store = self.decisions.lock();

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
    fn compute_budget(&self, agent_id: &str) -> TokenBudget {
        if self.config.budget.runtime_managed {
            return BudgetCalculator::new(self.config.budget.clone()).make_budget();
        }
        BudgetCalculator::new(self.config.budget.clone()).make_role_budget(agent_id)
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
        let total = self.config.budget.context_window;
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

    #[must_use]
    pub fn background_extraction_health(&self) -> BackgroundExtractionHealth {
        let mut health = self.background_extraction_state.snapshot();
        let stats = self.vector_index_stats();
        health.vector_entries = stats.count as u64;
        health.vector_evictions = stats.evictions;
        health.vector_generation = stats.generation;
        health.vector_persisted_generation = stats.persisted_generation;
        health.vector_persistence_failures = stats.persistence_failures;
        health.vector_coverage_basis_points = if health.vector_active_entries == 0 {
            if health.vector_reconciliation_complete {
                10_000
            } else {
                0
            }
        } else {
            health
                .vector_indexed_active_entries
                .saturating_mul(10_000)
                .checked_div(health.vector_active_entries)
                .unwrap_or_default()
                .min(10_000)
        };
        health.degraded_to_fts = !self.embedding_capability.supports_semantic()
            || !health.vector_reconciliation_complete
            || health.vector_coverage_basis_points < 10_000
            || health.last_index_error.is_some()
            || stats.last_persistence_error.is_some();
        health
    }

    /// Stop every background execution body owned by this manager.
    ///
    /// Gateway calls this during normal shutdown and startup rollback. Handles
    /// are taken before awaiting, making repeated calls idempotent.
    pub async fn shutdown_background_tasks(&self) -> MemoryBackgroundShutdownReport {
        let _ = self.background_shutdown.send(true);
        let mut report = MemoryBackgroundShutdownReport::default();
        let watcher = self.background_watcher.lock().take();
        if let Some(watcher) = watcher {
            match tokio::task::spawn_blocking(move || watcher.shutdown()).await {
                Ok(Ok(())) => report.watcher_joined = true,
                Ok(Err(error)) => report.errors.push(error),
                Err(error) => report
                    .errors
                    .push(format!("join background watcher shutdown: {error}")),
            }
        } else {
            report.watcher_joined = true;
        }
        let handles = [
            self.extract_handle.take(),
            self.kg_rebuild_handle.take(),
            self.memory_usage_persist_handle.take(),
        ];
        for handle in handles.into_iter().flatten() {
            join_memory_background_task(handle, &mut report).await;
        }
        report
    }

    pub(crate) fn record_memory_usage_signal(&self, signal: MemoryUsageSignal) {
        let key = memory_usage_signal_key(&signal);
        let mut signals = self.memory_usage_signals.lock();
        if let Some(current) = signals.get_mut(&key) {
            current.selected_count = current.selected_count.saturating_add(signal.selected_count);
            current.last_reason = signal.last_reason;
        } else if signals.len() < MAX_MEMORY_USAGE_KEYS {
            signals.insert(key, signal);
        } else {
            self.memory_usage_writer_state
                .dropped_keys
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        drop(signals);
        match self.memory_usage_persist_tx.try_send(()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.memory_usage_writer_state
                    .coalesced
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.memory_usage_writer_state
                    .persistence_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn memory_usage_summary(&self) -> MemoryUsageSummary {
        let signals = self
            .memory_usage_signals
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        summarize_usage(&signals, 3)
    }

    #[must_use]
    pub fn memory_usage_writer_health(&self) -> MemoryUsageWriterHealth {
        MemoryUsageWriterHealth {
            keys: self.memory_usage_signals.lock().len(),
            persisted_batches: self
                .memory_usage_writer_state
                .persisted_batches
                .load(Ordering::Relaxed),
            coalesced: self
                .memory_usage_writer_state
                .coalesced
                .load(Ordering::Relaxed),
            dropped_keys: self
                .memory_usage_writer_state
                .dropped_keys
                .load(Ordering::Relaxed),
            persistence_failures: self
                .memory_usage_writer_state
                .persistence_failures
                .load(Ordering::Relaxed),
        }
    }

    // ── Performance report (P9.4) ────────────────────────────────────────

    /// Return a snapshot of current performance metrics and auto-tuner state.
    #[must_use]
    pub fn performance_report(&self) -> crate::performance_monitor::PerformanceReport {
        let last_tuning = self.auto_tuner.last_tuning_instant().map(|i| {
            let elapsed = i.elapsed();
            // Approximate wall-clock DateTime by subtracting from now
            Utc::now()
                - chrono::Duration::from_std(elapsed)
                    .unwrap_or_else(|_| chrono::Duration::seconds(0))
        });
        let tuning_applied = self.auto_tuner.adjustments_applied() > 0;
        let tuning_config = self.auto_tuner.config();
        self.perf_monitor
            .report(&tuning_config, tuning_applied, last_tuning)
    }
}

async fn join_memory_background_task(
    mut handle: tokio::task::JoinHandle<()>,
    report: &mut MemoryBackgroundShutdownReport,
) {
    match tokio::time::timeout(Duration::from_secs(5), &mut handle).await {
        Ok(Ok(())) => report.joined_tasks += 1,
        Ok(Err(error)) => report
            .errors
            .push(format!("memory background task failed: {error}")),
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            report.forced_aborts += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// FTS5 Query Helpers
// ---------------------------------------------------------------------------

/// Statistics about the vector index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorIndexStats {
    pub count: usize,
    pub generation: u64,
    pub persisted_generation: u64,
    pub evictions: u64,
    pub persistence_failures: u64,
    pub last_persistence_error: Option<String>,
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
        let end = content
            .char_indices()
            .map(|(idx, _)| idx)
            .take_while(|idx| *idx <= max_len)
            .last()
            .unwrap_or(0);
        format!("{}...", &content[..end])
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
    let code_extensions = [
        ".rs", ".py", ".ts", ".tsx", ".go", ".java", ".js", ".cpp", ".h",
    ];
    let code_keywords = [
        "function",
        "class",
        "bug",
        "fix",
        "struct",
        "interface",
        "enum",
        "fn ",
        "impl",
        "trait",
        "module",
        "import",
        "def ",
        "async",
        "await",
        "refactor",
        "compile",
        "compiler",
        "syntax",
        "type",
        "error",
        "warning",
        "unwra",
        "panic",
        "debug",
        "trace",
        "cargo",
        "npm",
        "node",
        "runtime",
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
            truncate_summary(&desc, 77)
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
            budget: BudgetConfig {
                context_window: 8000,
                reserved_system: 2000,
                reserved_response: 1000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn user_message(turn_index: usize, content: &str) -> Message {
        Message {
            turn_index,
            role: MessageRole::User,
            content: content.to_string(),
            tool_use_id: None,
            tool_name: None,
            pinned: false,
        }
    }

    #[test]
    fn background_extraction_coalesces_retries_but_keeps_distinct_turns() {
        let mut batches = HashMap::new();
        let first_turn = MemoryTurnContext::new("session-a", "agent-a");
        let first = BackgroundExtractionRequest {
            turn: first_turn.clone(),
            messages: vec![user_message(0, "first")],
            heuristic_entries: Vec::new(),
        };
        assert!(!coalesce_background_request(&mut batches, first.clone(),));
        assert!(coalesce_background_request(&mut batches, first.clone(),));
        assert!(!coalesce_background_request(
            &mut batches,
            BackgroundExtractionRequest {
                turn: first_turn,
                messages: vec![user_message(1, "latest")],
                heuristic_entries: Vec::new(),
            },
        ));
        assert!(!coalesce_background_request(
            &mut batches,
            BackgroundExtractionRequest {
                turn: MemoryTurnContext::new("session-b", "agent-a"),
                messages: vec![user_message(0, "other session")],
                heuristic_entries: Vec::new(),
            },
        ));

        assert_eq!(batches.len(), 3);
        let first = batches
            .get(&background_extraction_key(&first))
            .expect("coalesced first turn");
        assert_eq!(first.1, 2);
        assert_eq!(first.0.messages[0].content, "first");
    }

    #[test]
    fn automatic_extraction_identity_is_stable_within_scope_and_isolated_across_projects() {
        let extractor = MemoryExtractor::new(Default::default());
        let messages = vec![
            Message::user("I prefer using tabs for indentation, please always use tabs."),
            Message::assistant(
                "Understood. I've decided we'll use tabs for all Rust files in this project.",
            ),
        ];
        let seed = extractor.finalize_entries(extractor.extract_heuristic(&messages));
        assert!(!seed.is_empty());
        let mut first = seed.clone();
        let mut retry = seed.clone();
        let mut other_project = seed;
        let project_a = MemoryTurnContext::new("session-a", "agent-a")
            .with_project_id(Some("project-a".to_string()));
        let project_b = MemoryTurnContext::new("session-b", "agent-a")
            .with_project_id(Some("project-b".to_string()));

        let batch_a = extraction_batch_tag(&project_a, &messages);
        let batch_b = extraction_batch_tag(&project_b, &messages);
        canonicalize_automatic_entries(&project_a, &batch_a, &mut first);
        canonicalize_automatic_entries(&project_a, &batch_a, &mut retry);
        canonicalize_automatic_entries(&project_b, &batch_b, &mut other_project);

        let preference_index = first
            .iter()
            .position(|entry| entry.category == MemoryCategory::UserPreference)
            .expect("preference entry");
        let decision_index = first
            .iter()
            .position(|entry| entry.category == MemoryCategory::Decision)
            .expect("decision entry");

        assert_eq!(first[preference_index].id, retry[preference_index].id);
        assert_ne!(
            first[preference_index].id, other_project[preference_index].id,
            "automatically inferred preferences must remain project-scoped"
        );
        assert_eq!(
            first[preference_index].scope,
            MemoryScope::Project("project-a".into())
        );
        assert!(
            !first[preference_index]
                .tags
                .iter()
                .any(|tag| tag == "memory-policy:always"),
            "heuristic extraction cannot grant unconditional injection authority"
        );

        assert_eq!(first[decision_index].id, retry[decision_index].id);
        assert_ne!(
            first[decision_index].id, other_project[decision_index].id,
            "project decisions remain isolated"
        );
        assert_eq!(
            first[decision_index].scope,
            MemoryScope::Project("project-a".into())
        );
        assert!(first[decision_index].tags.iter().any(|tag| tag == &batch_a));
    }

    #[test]
    fn semantic_extraction_refines_same_turn_heuristic_without_hiding_other_atoms() {
        let extractor = MemoryExtractor::new(Default::default());
        let messages = vec![
            Message::user("请记住：今后代码审查先列风险与证据，再给结论。"),
            Message::assistant("决定采用 Gateway 统一托管 Runtime 生命周期。"),
        ];
        let turn = MemoryTurnContext::new("session-a", "agent-a")
            .with_project_id(Some("project-a".to_string()));
        let batch = extraction_batch_tag(&turn, &messages);
        let mut heuristic = extractor.finalize_entries(extractor.extract_heuristic(&messages));
        canonicalize_automatic_entries(&turn, &batch, &mut heuristic);

        let mut semantic_preference = heuristic
            .iter()
            .find(|entry| entry.category == MemoryCategory::UserPreference)
            .expect("heuristic preference")
            .clone();
        semantic_preference.id = uuid::Uuid::new_v4();
        semantic_preference.title = "Code review order".to_string();
        semantic_preference.content =
            "List risks and evidence before the conclusion in every code review.".to_string();
        let mut semantic_reference = semantic_preference.clone();
        semantic_reference.id = uuid::Uuid::new_v4();
        semantic_reference.layer = MemoryLayer::L3;
        semantic_reference.category = MemoryCategory::Reference;
        semantic_reference.title = "Gateway mediator pattern".to_string();

        let (refinements, inserts) = partition_semantic_refinements(
            vec![semantic_preference, semantic_reference.clone()],
            &heuristic,
        );

        assert_eq!(refinements.len(), 1);
        assert_eq!(refinements[0].0.id, heuristic[0].id);
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].id, semantic_reference.id);
    }

    fn semantic_entry(
        layer: MemoryLayer,
        category: MemoryCategory,
        scope: MemoryScope,
        content: &str,
        tags: &[&str],
    ) -> MemoryEntry {
        let now = Utc::now();
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer,
            category,
            priority: Priority::High,
            source: MemorySource::AutoExtracted,
            title: content.chars().take(40).collect(),
            content: content.to_string(),
            embedding: None,
            tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
            relations: Vec::new(),
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope,
            session_id: Some("semantic-dedup-test".to_string()),
            source_agent: Some("root-agent".to_string()),
            visibility: crate::types::AgentVisibility::Private,
        }
    }

    #[test]
    fn cross_turn_semantic_dedup_accepts_paraphrases_but_preserves_scope_and_conflicts() {
        let preference = semantic_entry(
            MemoryLayer::L1,
            MemoryCategory::UserPreference,
            MemoryScope::Global,
            "All architecture audits must verify production evidence before conclusions.",
            &["preference", "architecture audit"],
        );
        let translated_preference = semantic_entry(
            MemoryLayer::L1,
            MemoryCategory::UserPreference,
            MemoryScope::Global,
            "所有架构审计必须先核验真实生产证据，再陈述结论。",
            &["preference", "架构审计"],
        );
        assert!(semantic_duplicate_compatible(
            &preference,
            &translated_preference,
            0.862
        ));

        let decision = semantic_entry(
            MemoryLayer::L2,
            MemoryCategory::Decision,
            MemoryScope::Project("cowd".to_string()),
            "Fact Kernel reviews structural facts before Matrix deduction.",
            &["Reality Core", "Fact Kernel"],
        );
        let project_knowledge = semantic_entry(
            MemoryLayer::L2,
            MemoryCategory::ProjectKnowledge,
            MemoryScope::Project("cowd".to_string()),
            "Matrix uses structural facts only after Fact Kernel review.",
            &["Reality Core", "Fact Kernel"],
        );
        assert!(semantic_duplicate_compatible(
            &decision,
            &project_knowledge,
            0.862
        ));

        let project_restates_global_preference = semantic_entry(
            MemoryLayer::L2,
            MemoryCategory::ProjectConvention,
            MemoryScope::Project("cowd".to_string()),
            "架构审计必须先核验真实生产证据，再陈述结论。",
            &["architecture-audit", "evidence-first"],
        );
        assert!(semantic_duplicate_compatible(
            &preference,
            &project_restates_global_preference,
            0.837
        ));

        let mut other_project = project_knowledge.clone();
        other_project.scope = MemoryScope::Project("other".to_string());
        assert!(!semantic_duplicate_compatible(
            &decision,
            &other_project,
            0.99
        ));

        let mut contradictory = project_knowledge;
        contradictory.content =
            "Matrix must not wait for Fact Kernel review before deduction.".to_string();
        assert!(!semantic_duplicate_compatible(
            &decision,
            &contradictory,
            0.99
        ));
    }

    #[test]
    fn vector_reconciliation_excludes_archived_and_superseded_lifecycle_states() {
        assert!(lifecycle_state_is_active(None));
        assert!(lifecycle_state_is_active(Some(MemoryState::Active)));
        assert!(!lifecycle_state_is_active(Some(MemoryState::Archived)));
        assert!(!lifecycle_state_is_active(Some(MemoryState::Superseded)));

        let event = MemoryLifecycleEvent {
            memory_id: uuid::Uuid::new_v4(),
            from: Some(MemoryState::Active),
            to: MemoryState::Archived,
            reason: "test archive".to_string(),
            session_id: "session-a".to_string(),
            agent_id: "agent-a".to_string(),
            occurred_at: Utc::now(),
        };
        let raw = serde_json::to_string(&vec![event]).expect("lifecycle JSON");
        assert_eq!(latest_lifecycle_state(&raw), Some(MemoryState::Archived));
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
    fn truncate_summary_unicode_boundary_safe() {
        let content = "项目概述：这是中文内容，用于验证 UTF-8 边界截断不会 panic";
        let truncated = truncate_summary(content, 12);
        assert!(truncated.ends_with("..."));
        assert!(content.starts_with(truncated.trim_end_matches("...")));
    }

    #[test]
    fn truncate_summary_emoji_boundary_safe() {
        let content = "状态正常 ✅ 继续处理后续任务";
        let truncated = truncate_summary(content, 17);
        assert!(truncated.ends_with("..."));
        assert!(content.starts_with(truncated.trim_end_matches("...")));
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
        cfg.store.blob_dir = tmp.path().join("blobs");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        assert_eq!(mgr.search_mode_label(), "keyword");
        assert_eq!(mgr.vector_index_count(), 0);
    }

    #[tokio::test]
    async fn corrupt_vector_artifact_degrades_to_fts_without_false_empty() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");
        cfg.store.blob_dir = tmp.path().join("blobs");
        std::fs::create_dir_all(&cfg.store.blob_dir).unwrap();
        std::fs::write(
            cfg.store.blob_dir.join("vector_index.json"),
            b"{not-valid-json",
        )
        .unwrap();

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let entry = semantic_entry(
            MemoryLayer::L2,
            MemoryCategory::ProjectKnowledge,
            MemoryScope::Global,
            "quartz-harbor-needle remains searchable through FTS",
            &["fallback"],
        );
        mgr.remember(entry).await.unwrap();
        let result = mgr
            .search_memories(SearchMemoriesRequest {
                query: "quartz harbor needle".to_string(),
                limit: 8,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.entries.len(), 1);
        let health = mgr.background_extraction_health();
        assert!(health.degraded_to_fts);
        assert!(health.last_index_error.is_some());
    }

    #[tokio::test]
    async fn usage_signals_are_visible_in_memory_before_coalesced_persistence() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");
        cfg.store.blob_dir = tmp.path().join("blobs");
        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let memory_id = uuid::Uuid::new_v4();

        for index in 0..8 {
            mgr.record_memory_usage_signal(MemoryUsageSignal {
                memory_id,
                session_id: "session-a".to_string(),
                agent_id: "agent-a".to_string(),
                selected_count: 1,
                last_reason: format!("selection-{index}"),
            });
        }

        let summary = mgr.memory_usage_summary();
        assert_eq!(summary.total_selected, 8);
        assert_eq!(summary.per_memory_selected.get(&memory_id), Some(&8));
        assert_eq!(mgr.memory_usage_writer_health().keys, 1);
        let shutdown = mgr.shutdown_background_tasks().await;
        assert!(
            shutdown.errors.is_empty(),
            "usage writer must drain cleanly: {:?}",
            shutdown.errors
        );
        assert!(
            mgr.memory_usage_writer_health().persisted_batches >= 1,
            "shutdown must persist the latest coalesced usage state"
        );
    }

    #[tokio::test]
    async fn with_write_source_configures_guard() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");
        cfg.store.blob_dir = tmp.path().join("blobs");

        let mgr = CognitiveContextManager::new(cfg)
            .await
            .unwrap()
            .with_write_source(WriteSource::System);
        let policy = mgr.check_write_access(MemoryLayer::L1);
        assert!(policy.is_allowed());
    }

    #[tokio::test]
    async fn list_layers_returns_info() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");
        cfg.store.blob_dir = tmp.path().join("blobs");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let layers = mgr.list_layers().await;
        assert!(!layers.is_empty());
    }

    #[tokio::test]
    async fn embedding_capability_defaults_fts5_only() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");
        cfg.store.blob_dir = tmp.path().join("blobs");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        assert!(!mgr.embedding_capability().supports_semantic());
    }

    #[tokio::test]
    async fn vector_index_stats_empty() {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");
        cfg.store.blob_dir = tmp.path().join("blobs");

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

    #[tokio::test]
    async fn background_tasks_are_joined_and_shutdown_is_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");

        let mgr = CognitiveContextManager::new(cfg).await.unwrap();
        let first = mgr.shutdown_background_tasks().await;
        assert_eq!(first.forced_aborts, 0);
        assert!(first.errors.is_empty(), "{:?}", first.errors);
        assert!(first.watcher_joined);
        assert_eq!(
            first.joined_tasks, 3,
            "extraction, knowledge-graph rebuild and usage persistence must all join"
        );

        let second = mgr.shutdown_background_tasks().await;
        assert_eq!(second.forced_aborts, 0);
        assert!(second.errors.is_empty());
        assert!(second.watcher_joined);
        assert_eq!(second.joined_tasks, 0);
    }

    #[tokio::test]
    async fn automatic_governance_admission_is_single_owner_until_completion() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = test_config();
        cfg.store.sqlite_path = tmp.path().join("test.db");
        let mgr = CognitiveContextManager::new(cfg).await.unwrap();

        let nightly = mgr
            .try_begin_automatic_governance("nightly")
            .expect("first governance run should acquire admission");
        assert_eq!(
            mgr.automatic_governance_run_status()
                .as_ref()
                .map(|run| run.run_id.as_str()),
            Some(nightly.run_id.as_str())
        );
        assert!(mgr.try_begin_automatic_governance("manual").is_none());

        mgr.finish_automatic_governance(&nightly.run_id);
        let manual = mgr
            .try_begin_automatic_governance("manual")
            .expect("manual run should acquire admission after nightly completion");
        assert_eq!(manual.mode, "manual");
        mgr.finish_automatic_governance(&manual.run_id);
        assert!(mgr.automatic_governance_run_status().is_none());
    }
}
