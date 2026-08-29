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

#[path = "cognitive/maintenance.rs"]
mod maintenance_ops;
#[path = "cognitive/recall.rs"]
mod recall;
#[path = "cognitive/write.rs"]
mod write;

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
            Some(store) => MemoryOrchestrator::from_store(
                config.clone(),
                Arc::clone(store),
                workspace_root.clone(),
            )?,
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
#[path = "cognitive/tests.rs"]
mod tests;
