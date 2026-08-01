//! MemoryKernel boundary and living-memory invariants.
//!
//! This module is intentionally a facade over the existing
//! [`CognitiveContextManager`]. It establishes the v0.8.12 control boundary
//! without rewriting the mature memory subsystems underneath it.

pub mod reality_recall;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use chrono::Utc;
use fact_kernel::{FactKernelService, FactReviewReceipt, InMemoryFactStore};
use harness_contract::reality::RecallSourceKind;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    cognitive::{BackgroundExtractionHealth, CognitiveContextManager},
    compression::session::{SessionCheckpointFact, SessionSemanticCheckpoint},
    error::MemoryError,
    memory_authority::{
        authority_decision, same_memory_key, MemoryAuthorityAction, MemoryAuthorityDecision,
    },
    memory_cluster::{cluster_entries, MemoryCluster},
    memory_usage::{MemoryUsageSignal, MemoryUsageSummary},
    project_scope::MemoryScope,
    store::AuthorityLookup,
    types::{
        AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Message,
        PreparedContext, Priority, TokenBudget,
    },
};

use reality_recall::{RecallCandidate, RecallOmission, RecallReport, RecallSourceResult};

/// Result alias for the MemoryKernel boundary.
pub type MemoryKernelResult<T> = std::result::Result<T, MemoryKernelError>;

pub(crate) const MEMORY_STALE_WARNING_THRESHOLD: f32 = 0.85;
const MEMORY_STALE_RANK_PENALTY: i32 = 45;
const MEMORY_AGING_RANK_PENALTY: i32 = 18;

/// Errors at the kernel boundary.
///
/// Foreground prepare/post-turn paths should usually degrade instead of
/// returning this error. The type exists for construction and explicit health
/// calls where callers need to distinguish memory-system failures from normal
/// empty recall.
#[derive(Debug, thiserror::Error)]
pub enum MemoryKernelError {
    #[error("memory backend failed: {0}")]
    Backend(#[from] MemoryError),
}

/// The five primitive concepts every memory surface must map to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryPrimitive {
    Atom,
    Evidence,
    Link,
    State,
    Recall,
}

impl MemoryPrimitive {
    #[must_use]
    pub fn all() -> [Self; 5] {
        [
            Self::Atom,
            Self::Evidence,
            Self::Link,
            Self::State,
            Self::Recall,
        ]
    }
}

/// Runtime information state, orthogonal to L0-L4 governance layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryInformationState {
    Trace,
    Pattern,
    Orientation,
}

/// Explicit lifecycle state for an interpreted memory atom.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryState {
    Candidate,
    Observed,
    Active,
    Validated,
    Conflicted,
    Superseded,
    Stale,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticCheckpointMemoryReceipt {
    pub memory_ids: Vec<MemoryId>,
    pub fact_review: FactReviewReceipt,
}

/// Read-side atom projection used by kernel/UI/tests.
///
/// This is a view over existing [`MemoryEntry`] data. It is not a new store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryAtomView {
    pub id: MemoryId,
    pub layer: MemoryLayer,
    pub information_state: MemoryInformationState,
    pub state: MemoryState,
    pub evidence_pointer: Option<String>,
    pub explicit_authority: bool,
    pub confidence: f32,
    pub salience: f32,
    pub title: String,
}

impl MemoryAtomView {
    #[must_use]
    pub fn from_entry(entry: &MemoryEntry, information_state: MemoryInformationState) -> Self {
        let explicit_authority = matches!(entry.layer, MemoryLayer::L0)
            || matches!(entry.source, crate::types::MemorySource::UserExplicit);
        let state = if entry.staleness >= MEMORY_STALE_WARNING_THRESHOLD {
            MemoryState::Stale
        } else if entry.confidence < 0.35 {
            MemoryState::Conflicted
        } else {
            MemoryState::Active
        };
        let evidence_pointer = match entry.layer {
            MemoryLayer::L0 | MemoryLayer::L1 => None,
            MemoryLayer::L2 | MemoryLayer::L3 | MemoryLayer::L4 => {
                Some(format!("memory:{}", entry.id))
            }
        };

        Self {
            id: entry.id,
            layer: entry.layer,
            information_state,
            state,
            evidence_pointer,
            explicit_authority,
            confidence: entry.confidence,
            salience: entry.priority as i32 as f32 - entry.staleness,
            title: entry.title.clone(),
        }
    }

    /// Orientation can guide the model only when it is explainable.
    #[must_use]
    pub fn is_explainable_orientation(&self) -> bool {
        self.information_state == MemoryInformationState::Orientation
            && matches!(self.state, MemoryState::Active | MemoryState::Validated)
            && (self.evidence_pointer.is_some() || self.explicit_authority)
    }
}

/// Read-only projection for one governance layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryLayerView {
    pub layer: MemoryLayer,
    pub atoms: Vec<MemoryAtomView>,
    pub read_only: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MemoryLinkKind {
    Related,
    Supersedes,
    Summarizes,
    DependsOn,
    ProducedBy,
    BelongsTo,
    Mentions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryLink {
    pub from: MemoryId,
    pub to: MemoryId,
    pub kind: MemoryLinkKind,
    pub weight: f32,
    pub evidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryPath {
    pub entries: Vec<MemoryAtomView>,
    pub links: Vec<MemoryLink>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MemoryPacketRole {
    Orientation,
    Supporting,
    Warning,
    Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryPacketItem {
    pub atom: MemoryAtomView,
    pub role: MemoryPacketRole,
    pub reason: String,
    #[serde(default)]
    pub content_preview: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OmittedMemory {
    pub id: MemoryId,
    pub title: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryContextPacket {
    pub selected: Vec<MemoryPacketItem>,
    pub omitted: Vec<OmittedMemory>,
    pub token_estimate: u64,
    pub truncated: bool,
    #[serde(default)]
    pub recall_report: RecallReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryContextPacketMode {
    RecordUsage,
    Preview,
}

impl MemoryContextPacketMode {
    fn records_usage(self) -> bool {
        matches!(self, Self::RecordUsage)
    }
}

impl MemoryLayerView {
    #[must_use]
    pub fn new(layer: MemoryLayer, atoms: Vec<MemoryAtomView>) -> Self {
        Self {
            layer,
            atoms,
            read_only: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryLifecycleEvent {
    pub memory_id: MemoryId,
    pub from: Option<MemoryState>,
    pub to: MemoryState,
    pub reason: String,
    pub session_id: String,
    pub agent_id: String,
    pub occurred_at: chrono::DateTime<Utc>,
}

/// Kernel-scoped session/agent/task binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryTurnContext {
    pub session_id: String,
    pub project_id: Option<String>,
    pub agent_id: String,
    /// Definition lineage is distinct from `agent_id`, which is always an
    /// isolated runtime instance identity.
    pub definition_lineage_id: Option<String>,
    pub team_id: Option<String>,
    pub task_id: Option<String>,
    /// Explicit cognitive read lease supplied by the Runtime Binding. This is
    /// checked again after every retrieval source returns candidates, so a
    /// broad backend query cannot leak a memory into an Agent context.
    #[serde(default = "default_cognitive_read_scopes")]
    pub cognitive_read_scopes: Vec<harness_contract::agent::CognitiveReadScope>,
}

impl MemoryTurnContext {
    #[must_use]
    pub fn new(session_id: impl Into<String>, agent_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            project_id: None,
            agent_id: agent_id.into(),
            definition_lineage_id: None,
            team_id: None,
            task_id: None,
            cognitive_read_scopes: default_cognitive_read_scopes(),
        }
    }

    #[must_use]
    pub fn with_project_id(mut self, project_id: Option<String>) -> Self {
        self.project_id = project_id;
        self
    }

    #[must_use]
    pub fn with_definition_lineage_id(mut self, definition_lineage_id: Option<String>) -> Self {
        self.definition_lineage_id = definition_lineage_id;
        self
    }

    #[must_use]
    pub fn with_task_id(mut self, task_id: Option<String>) -> Self {
        self.task_id = task_id;
        self
    }

    #[must_use]
    pub fn with_team_id(mut self, team_id: Option<String>) -> Self {
        self.team_id = team_id;
        self
    }

    #[must_use]
    pub fn with_cognitive_read_scopes(
        mut self,
        scopes: Vec<harness_contract::agent::CognitiveReadScope>,
    ) -> Self {
        self.cognitive_read_scopes = scopes;
        self
    }
}

fn default_cognitive_read_scopes() -> Vec<harness_contract::agent::CognitiveReadScope> {
    use harness_contract::agent::CognitiveReadScope;

    vec![
        CognitiveReadScope::Session,
        CognitiveReadScope::Team,
        CognitiveReadScope::WorkspaceKnowledge,
        CognitiveReadScope::Project,
        CognitiveReadScope::DefinitionLineage,
    ]
}

/// A degraded memory subsystem or fallback path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryDegradation {
    StoreUnavailable,
    FtsUnavailable,
    VectorUnavailable,
    LinkTraversalLimited,
    DistillationBacklog,
    ImportFailed,
    MalformedAtomSkipped,
    PrepareFailed(String),
    PostTurnFailed(String),
}

/// Product-visible memory health snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryHealth {
    pub orientation_pressure: f32,
    pub conflict_pressure: f32,
    pub stale_pressure: f32,
    pub evidence_coverage: f32,
    pub link_coverage: f32,
    pub background_lag_ms: Option<u64>,
    pub background_extraction: BackgroundExtractionHealth,
    pub degraded: Vec<MemoryDegradation>,
}

impl Default for MemoryHealth {
    fn default() -> Self {
        Self {
            orientation_pressure: 0.0,
            conflict_pressure: 0.0,
            stale_pressure: 0.0,
            evidence_coverage: 1.0,
            link_coverage: 1.0,
            background_lag_ms: None,
            background_extraction: BackgroundExtractionHealth::default(),
            degraded: Vec::new(),
        }
    }
}

impl MemoryHealth {
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        !self.degraded.is_empty()
    }
}

/// The only runtime boundary that should prepare or mutate memory for a turn.
#[derive(Clone)]
pub struct MemoryKernel {
    manager: Arc<CognitiveContextManager>,
}

impl MemoryKernel {
    #[must_use]
    pub fn new(manager: Arc<CognitiveContextManager>) -> Self {
        Self { manager }
    }

    #[must_use]
    pub fn manager(&self) -> &Arc<CognitiveContextManager> {
        &self.manager
    }

    /// Prepare memory for a turn. Backend failure degrades to an empty context
    /// so foreground agent/session execution is not aborted by memory.
    pub async fn prepare(
        &self,
        ctx: &MemoryTurnContext,
        query: &str,
        messages: &[Message],
    ) -> MemoryKernelResult<PreparedContext> {
        match self
            .manager
            .prepare_context_for_turn(ctx, query, messages)
            .await
        {
            Ok(mut prepared) => {
                prepared.entries = self
                    .filter_active_entries(prepared.entries)
                    .await
                    .into_iter()
                    .filter(|entry| memory_entry_visible_to_ctx(entry, ctx))
                    .collect();
                prepared.total_tokens = prepared
                    .entries
                    .iter()
                    .map(|entry| entry.content.len() as u64 / 4)
                    .sum();
                Ok(prepared)
            }
            Err(error) => {
                tracing::warn!(
                    session_id = %ctx.session_id,
                    agent_id = %ctx.agent_id,
                    %error,
                    "memory kernel prepare degraded"
                );
                Ok(Self::empty_degraded_context())
            }
        }
    }

    /// Post-turn memory maintenance. Failures are degraded and non-fatal.
    pub async fn post_turn(
        &self,
        ctx: &MemoryTurnContext,
        messages: &mut Vec<Message>,
    ) -> MemoryKernelResult<()> {
        if let Err(error) = self.manager.on_turn_end_for_turn(ctx, messages).await {
            tracing::warn!(
                session_id = %ctx.session_id,
                agent_id = %ctx.agent_id,
                %error,
                "memory kernel post_turn degraded"
            );
        }
        Ok(())
    }

    /// Write one memory atom through the kernel governance boundary.
    ///
    /// The caller supplies the semantic entry. The kernel supplies runtime
    /// ownership metadata so agent/session writes remain auditable and scoped.
    pub async fn remember(
        &self,
        ctx: &MemoryTurnContext,
        mut entry: MemoryEntry,
    ) -> MemoryKernelResult<()> {
        entry
            .session_id
            .get_or_insert_with(|| ctx.session_id.clone());
        entry
            .source_agent
            .get_or_insert_with(|| ctx.agent_id.clone());
        entry.scope = scoped_entry_scope(ctx, &entry);

        if entry.layer == MemoryLayer::L0
            && !matches!(entry.source, crate::types::MemorySource::UserExplicit)
            && ctx.agent_id != "system"
        {
            tracing::warn!(
                session_id = %ctx.session_id,
                agent_id = %ctx.agent_id,
                "memory kernel denied non-authoritative L0 write"
            );
            return Ok(());
        }

        let memory_id = entry.id;
        let authority_match = self.authority_match(&entry).await?;
        let authority_action = authority_match
            .as_ref()
            .map(|(_, decision)| decision.action)
            .unwrap_or(MemoryAuthorityAction::SupersedeExisting);
        if matches!(authority_action, MemoryAuthorityAction::Duplicate) {
            if let Some((existing_id, decision)) = authority_match {
                self.record_lifecycle_event(
                    ctx,
                    existing_id,
                    self.latest_state(existing_id).await.unwrap_or(None),
                    MemoryState::Observed,
                    format!("duplicate write skipped: {}", decision.reason),
                )
                .await?;
            }
            tracing::debug!(
                session_id = %ctx.session_id,
                agent_id = %ctx.agent_id,
                memory_id = %memory_id,
                "memory kernel skipped duplicate write"
            );
            return Ok(());
        }
        self.manager.remember_for_turn(ctx, entry).await?;
        if let Some((existing_id, decision)) = authority_match {
            match decision.action {
                MemoryAuthorityAction::SupersedeExisting => {
                    self.record_lifecycle_event(
                        ctx,
                        existing_id,
                        self.latest_state(existing_id).await?,
                        MemoryState::Superseded,
                        format!("superseded by {memory_id}: {}", decision.reason),
                    )
                    .await?;
                }
                MemoryAuthorityAction::MarkConflict => {
                    self.record_lifecycle_event(
                        ctx,
                        existing_id,
                        self.latest_state(existing_id).await?,
                        MemoryState::Conflicted,
                        format!("conflicts with {memory_id}: {}", decision.reason),
                    )
                    .await?;
                }
                MemoryAuthorityAction::KeepExisting | MemoryAuthorityAction::Duplicate => {}
            }
        }
        self.record_lifecycle_event(
            ctx,
            memory_id,
            None,
            match authority_action {
                MemoryAuthorityAction::MarkConflict => MemoryState::Conflicted,
                MemoryAuthorityAction::Duplicate => MemoryState::Observed,
                _ => MemoryState::Active,
            },
            "remembered through memory kernel",
        )
        .await?;
        Ok(())
    }

    /// Persist a runtime semantic compaction checkpoint through the memory
    /// governance boundary.
    ///
    /// The checkpoint is produced by the session compactor but written here so
    /// every fact receives active session/agent scope and lifecycle governance.
    pub async fn checkpoint_compaction(
        &self,
        ctx: &MemoryTurnContext,
        checkpoint: SessionSemanticCheckpoint,
    ) -> MemoryKernelResult<SemanticCheckpointMemoryReceipt> {
        let candidate_to_fact = checkpoint
            .facts
            .iter()
            .enumerate()
            .map(|(index, fact)| {
                (
                    checkpoint.fact_candidate_id_key(index),
                    (index, fact.clone()),
                )
            })
            .collect::<HashMap<_, _>>();
        let scope = ctx
            .task_id
            .as_ref()
            .map(|task_id| MemoryScope::Task(task_id.clone()))
            .unwrap_or_else(|| MemoryScope::Session(ctx.session_id.clone()));
        let mut seen_categories = HashSet::new();
        let mut existing_candidates = Vec::new();
        for fact in &checkpoint.facts {
            if seen_categories.insert(fact.category) {
                existing_candidates.extend(
                    self.manager
                        .fact_candidates(&scope, fact.category, 1024)
                        .await?,
                );
            }
        }
        let mut fact_service = FactKernelService::with_store(InMemoryFactStore::new());
        for entry in self.filter_active_entries(existing_candidates).await {
            fact_service.upsert_fact(entry.to_fact_record());
        }
        let fact_review = fact_service.review_candidates(checkpoint.to_fact_extraction_batch());

        let mut ids = Vec::with_capacity(fact_review.promoted.len());
        for decision in &fact_review.promoted {
            let Some((fact_index, fact)) =
                candidate_to_fact.get(decision.candidate.candidate_id.as_str())
            else {
                continue;
            };
            let id = self
                .remember_checkpoint_fact(ctx, &checkpoint.checkpoint_id, *fact_index, fact.clone())
                .await?;
            ids.push(id);
        }

        Ok(SemanticCheckpointMemoryReceipt {
            memory_ids: ids,
            fact_review,
        })
    }

    async fn remember_checkpoint_fact(
        &self,
        ctx: &MemoryTurnContext,
        checkpoint_id: &str,
        fact_index: usize,
        fact: SessionCheckpointFact,
    ) -> MemoryKernelResult<MemoryId> {
        let now = Utc::now();
        let evidence_block = if fact.evidence_refs.is_empty() {
            String::new()
        } else {
            let refs = fact
                .evidence_refs
                .iter()
                .map(|evidence| {
                    let source = evidence.source.as_deref().unwrap_or("source");
                    format!(
                        "- {}:{} ({source}; {})",
                        evidence.ref_type,
                        evidence.id,
                        evidence.boundary.as_str()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            format!("\n\nEvidence refs:\n{refs}")
        };
        let scope = ctx
            .task_id
            .as_ref()
            .map(|task_id| MemoryScope::Task(task_id.clone()))
            .unwrap_or_else(|| MemoryScope::Session(ctx.session_id.clone()));
        let mut tags = fact.tags;
        tags.push(format!("fact-kind:{:?}", fact.kind).to_lowercase());
        if let Some(project_id) = &ctx.project_id {
            tags.push(format!("project:{project_id}"));
        }
        if let Some(task_id) = &ctx.task_id {
            tags.push(format!("task:{task_id}"));
        }
        let entry = MemoryEntry {
            // A checkpoint event is durable before this projection. Use a
            // deterministic ID so replay after a crash either creates the
            // missing memory once or reuses the existing fact without
            // duplicating the knowledge base.
            id: checkpoint_memory_id(checkpoint_id, fact_index),
            layer: fact.layer,
            category: fact.category,
            priority: Priority::Normal,
            source: MemorySource::Compression,
            title: fact.title,
            content: format!("{}{}", fact.content, evidence_block),
            embedding: None,
            tags,
            relations: vec![],
            confidence: fact.confidence,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope,
            session_id: Some(ctx.session_id.clone()),
            source_agent: Some(ctx.agent_id.clone()),
            visibility: AgentVisibility::default(),
        };
        let id = entry.id;
        self.remember(ctx, entry).await?;
        Ok(id)
    }

    pub async fn archive(
        &self,
        ctx: &MemoryTurnContext,
        memory_id: MemoryId,
        reason: impl Into<String>,
    ) -> MemoryKernelResult<()> {
        self.transition_state(ctx, memory_id, MemoryState::Archived, reason)
            .await
    }

    /// Append a lifecycle transition. Evidence is not mutated.
    pub async fn transition_state(
        &self,
        ctx: &MemoryTurnContext,
        memory_id: MemoryId,
        to: MemoryState,
        reason: impl Into<String>,
    ) -> MemoryKernelResult<()> {
        let from = self.latest_state(memory_id).await?;
        self.record_lifecycle_event(ctx, memory_id, from, to, reason)
            .await
    }

    pub async fn lifecycle_events(
        &self,
        memory_id: MemoryId,
    ) -> MemoryKernelResult<Vec<MemoryLifecycleEvent>> {
        Ok(self
            .load_lifecycle_events(memory_id)
            .await
            .unwrap_or_default())
    }

    pub async fn filter_active_entries(&self, entries: Vec<MemoryEntry>) -> Vec<MemoryEntry> {
        let states = self
            .latest_states(entries.iter().map(|entry| entry.id))
            .await
            .unwrap_or_default();
        entries
            .into_iter()
            .filter(|entry| {
                let state = states.get(&entry.id).copied().flatten();
                !matches!(state, Some(MemoryState::Superseded | MemoryState::Archived))
            })
            .collect()
    }

    async fn latest_states(
        &self,
        memory_ids: impl IntoIterator<Item = MemoryId>,
    ) -> MemoryKernelResult<HashMap<MemoryId, Option<MemoryState>>> {
        let keyed = memory_ids
            .into_iter()
            .map(|memory_id| (lifecycle_key(memory_id), memory_id))
            .collect::<HashMap<_, _>>();
        let keys = keyed.keys().cloned().collect::<Vec<_>>();
        let values = self.manager.kernel_kv_get_many(&keys).await?;
        let mut states = keyed
            .values()
            .copied()
            .map(|memory_id| (memory_id, None))
            .collect::<HashMap<_, _>>();
        for value in values {
            let Some(memory_id) = keyed.get(&value.key).copied() else {
                continue;
            };
            let events = serde_json::from_str::<Vec<MemoryLifecycleEvent>>(&value.value).map_err(
                |error| {
                    MemoryKernelError::Backend(MemoryError::Store(format!(
                        "decode lifecycle events for {memory_id}: {error}"
                    )))
                },
            )?;
            states.insert(memory_id, events.last().map(|event| event.to));
        }
        Ok(states)
    }

    pub async fn context_packet(
        &self,
        ctx: &MemoryTurnContext,
        query: &str,
        messages: &[Message],
        max_items: usize,
        max_tokens: u64,
    ) -> MemoryKernelResult<MemoryContextPacket> {
        self.context_packet_with_mode(
            ctx,
            query,
            messages,
            max_items,
            max_tokens,
            MemoryContextPacketMode::RecordUsage,
        )
        .await
    }

    pub async fn context_packet_preview(
        &self,
        ctx: &MemoryTurnContext,
        query: &str,
        messages: &[Message],
        max_items: usize,
        max_tokens: u64,
    ) -> MemoryKernelResult<MemoryContextPacket> {
        self.context_packet_with_mode(
            ctx,
            query,
            messages,
            max_items,
            max_tokens,
            MemoryContextPacketMode::Preview,
        )
        .await
    }

    /// Actively search Memory inside an exact Runtime Binding.
    ///
    /// Unlike [`Self::context_packet_preview`], this path does not begin with
    /// progressive passive layer loading. It queries the durable FTS/vector
    /// indexes first, then applies the same lifecycle, scope, relevance,
    /// deduplication, and token-budget gates used by normal context assembly.
    /// This distinction is required for Session/Task memories that are valid
    /// on demand but intentionally absent from every turn's passive packet.
    pub async fn retrieve_packet_preview(
        &self,
        ctx: &MemoryTurnContext,
        query: &str,
        max_items: usize,
        max_tokens: u64,
    ) -> MemoryKernelResult<MemoryContextPacket> {
        let candidate_limit = max_items.saturating_mul(8).clamp(16, 128);
        let search_scopes = memory_binding_search_scopes(ctx);
        let mut candidates = self
            .manager
            .search_memories_in_scopes(query, &search_scopes, candidate_limit)
            .await?;
        let mut vector_scores = HashMap::new();
        let seen = candidates
            .iter()
            .map(|entry| entry.id)
            .collect::<HashSet<_>>();
        for (entry, score) in self
            .manager
            .vector_recall_candidates(query, &seen, candidate_limit)
            .await
            .unwrap_or_default()
        {
            vector_scores.insert(entry.id, score);
            candidates.push(entry);
        }
        let candidates = self.filter_active_entries(candidates).await;
        let (candidates, mut omitted) = deduplicate_memory_entries_for_recall(candidates);
        let mut selected_sources = HashMap::new();
        let mut relevance_omissions = Vec::new();
        let candidates = candidates
            .into_iter()
            // Scope failures are intentionally silent: callers must not learn
            // even the title/id of a Memory outside their Binding.
            .filter(|entry| memory_entry_visible_to_ctx(entry, ctx))
            .filter(|entry| {
                let relevance =
                    memory_entry_turn_relevance(entry, query, vector_scores.get(&entry.id).copied());
                if relevance.accepted {
                    selected_sources.insert(entry.id, RecallSourceKind::Memory);
                    true
                } else {
                    relevance_omissions.push(OmittedMemory {
                        id: entry.id,
                        title: entry.title.clone(),
                        reason: format!(
                            "active retrieval relevance below scope threshold ({:.2} < {:.2}, scope={})",
                            relevance.score, relevance.threshold, entry.scope
                        ),
                    });
                    false
                }
            })
            .collect();
        omitted.append(&mut relevance_omissions);
        let usage_summary = self.usage_summary().await.unwrap_or_default();
        let mut packet = self
            .context_packet_from_entries_with_budget(
                candidates,
                max_items,
                max_tokens,
                &self.manager.budget_config(),
                Some(&usage_summary),
            )
            .await?;
        packet.omitted.extend(omitted);
        packet.recall_report = recall_report_from_packet(
            &packet,
            selected_sources,
            vector_scores,
            vec![RecallSourceResult {
                source: RecallSourceKind::Memory,
                status: "active_binding_search".to_string(),
                selected_count: packet.selected.len(),
                omitted_count: packet.omitted.len(),
                degraded_reason: None,
            }],
        );
        Ok(packet)
    }

    /// Read one exact Memory entry through the same lifecycle and Runtime
    /// Binding gates used by active retrieval.
    ///
    /// An unknown or unauthorized id returns `None` so callers cannot use ids
    /// to probe another Session, Project, Agent, Team, or knowledge scope.
    pub async fn retrieve_visible_entry(
        &self,
        ctx: &MemoryTurnContext,
        memory_id: MemoryId,
    ) -> MemoryKernelResult<Option<MemoryEntry>> {
        let Some(entry) = self.manager.get_entry(&memory_id.to_string()).await? else {
            return Ok(None);
        };
        let mut active = self.filter_active_entries(vec![entry]).await;
        Ok(active
            .pop()
            .filter(|entry| memory_entry_visible_to_ctx(entry, ctx)))
    }

    async fn context_packet_with_mode(
        &self,
        ctx: &MemoryTurnContext,
        query: &str,
        messages: &[Message],
        max_items: usize,
        max_tokens: u64,
        mode: MemoryContextPacketMode,
    ) -> MemoryKernelResult<MemoryContextPacket> {
        let mut prepared = self.prepare(ctx, query, messages).await?;
        let mut source_results = Vec::new();
        let mut selected_sources: HashMap<MemoryId, RecallSourceKind> = HashMap::new();
        let mut vector_scores: HashMap<MemoryId, f32> = HashMap::new();
        let mut checkpoint_omissions = Vec::new();
        prepared.entries.retain(|entry| {
            if !entry.tags.iter().any(|tag| tag == "semantic-checkpoint") {
                return true;
            }
            let score = checkpoint_recall_score(entry, ctx, query);
            if score >= 0.35 {
                true
            } else {
                checkpoint_omissions.push(OmittedMemory {
                    id: entry.id,
                    title: entry.title.clone(),
                    reason: format!("semantic checkpoint relevance too low ({score:.2})"),
                });
                false
            }
        });
        let checkpoint_limit = (max_items / 4).clamp(1, 6);
        let (checkpoint_entries, mut scoped_checkpoint_omissions) = self
            .scoped_checkpoint_entries(ctx, query, checkpoint_limit)
            .await;
        checkpoint_omissions.append(&mut scoped_checkpoint_omissions);
        if !checkpoint_entries.is_empty() {
            let mut seen: HashSet<MemoryId> =
                prepared.entries.iter().map(|entry| entry.id).collect();
            let mut merged = Vec::with_capacity(prepared.entries.len() + checkpoint_entries.len());
            for entry in checkpoint_entries {
                if seen.insert(entry.id) {
                    merged.push(entry);
                }
            }
            merged.extend(prepared.entries);
            prepared.entries = merged;
        }
        let mut seen_for_vector: HashSet<MemoryId> =
            prepared.entries.iter().map(|entry| entry.id).collect();
        match self
            .manager
            .vector_recall_candidates(query, &seen_for_vector, max_items.max(1))
            .await
        {
            Ok(vector_entries) => {
                let mut added = 0usize;
                let score_by_id = vector_entries
                    .iter()
                    .map(|(entry, score)| (entry.id, *score))
                    .collect::<HashMap<_, _>>();
                let active_vector_entries = self
                    .filter_active_entries(
                        vector_entries.into_iter().map(|(entry, _)| entry).collect(),
                    )
                    .await;
                for entry in active_vector_entries {
                    let score = score_by_id.get(&entry.id).copied().unwrap_or_default();
                    if seen_for_vector.insert(entry.id) {
                        vector_scores.insert(entry.id, score);
                        selected_sources.insert(entry.id, RecallSourceKind::Memory);
                        prepared.entries.push(entry);
                        added += 1;
                    }
                }
                source_results.push(RecallSourceResult {
                    source: RecallSourceKind::Memory,
                    status: "enabled_and_wired".to_string(),
                    selected_count: added,
                    omitted_count: 0,
                    degraded_reason: None,
                });
            }
            Err(error) => source_results.push(RecallSourceResult {
                source: RecallSourceKind::Memory,
                status: "degraded".to_string(),
                selected_count: 0,
                omitted_count: 0,
                degraded_reason: Some(format!("vector recall unavailable: {error}")),
            }),
        }
        let (deduplicated_entries, mut duplicate_omissions) =
            deduplicate_memory_entries_for_recall(prepared.entries);
        let mut relevance_omissions = Vec::new();
        prepared.entries = deduplicated_entries
            .into_iter()
            .filter(|entry| memory_entry_visible_to_ctx(entry, ctx))
            .filter(|entry| {
                let vector_score = vector_scores.get(&entry.id).copied();
                let relevance = memory_entry_turn_relevance(entry, query, vector_score);
                if relevance.accepted {
                    true
                } else {
                    relevance_omissions.push(OmittedMemory {
                        id: entry.id,
                        title: entry.title.clone(),
                        reason: format!(
                            "turn relevance below scope threshold ({:.2} < {:.2}, scope={})",
                            relevance.score, relevance.threshold, entry.scope
                        ),
                    });
                    false
                }
            })
            .collect();
        checkpoint_omissions.append(&mut duplicate_omissions);
        checkpoint_omissions.append(&mut relevance_omissions);
        let usage_summary = self.usage_summary().await.unwrap_or_default();
        let mut packet = self
            .context_packet_from_entries_with_budget(
                prepared.entries,
                max_items,
                max_tokens,
                &self.manager.budget_config(),
                Some(&usage_summary),
            )
            .await?;
        packet.omitted.extend(checkpoint_omissions);
        packet.recall_report =
            recall_report_from_packet(&packet, selected_sources, vector_scores, source_results);
        if mode.records_usage() {
            self.record_context_usage(ctx, &packet).await?;
        }
        Ok(packet)
    }

    async fn scoped_checkpoint_entries(
        &self,
        ctx: &MemoryTurnContext,
        query: &str,
        limit: usize,
    ) -> (Vec<MemoryEntry>, Vec<OmittedMemory>) {
        let scope = ctx
            .task_id
            .as_ref()
            .map(|task_id| MemoryScope::Task(task_id.clone()))
            .unwrap_or_else(|| MemoryScope::Session(ctx.session_id.clone()));
        let Ok(entries) = self
            .manager
            .semantic_checkpoint_candidates(&scope, query, limit.saturating_mul(4).max(16))
            .await
        else {
            return (Vec::new(), Vec::new());
        };
        let mut omitted = Vec::new();
        let mut scored = entries
            .into_iter()
            .filter(|entry| {
                entry.tags.iter().any(|tag| tag == "semantic-checkpoint")
                    && memory_entry_visible_to_ctx(entry, ctx)
            })
            .map(|entry| (checkpoint_recall_score(&entry, ctx, query), entry))
            .collect::<Vec<_>>();
        scored.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.updated_at.cmp(&left.updated_at))
        });
        let mut selected = Vec::new();
        for (score, entry) in scored {
            if score < 0.35 {
                omitted.push(OmittedMemory {
                    id: entry.id,
                    title: entry.title,
                    reason: format!("semantic checkpoint relevance too low ({score:.2})"),
                });
                continue;
            }
            if selected.len() < limit.max(1) {
                selected.push(entry);
            } else {
                omitted.push(OmittedMemory {
                    id: entry.id,
                    title: entry.title,
                    reason: "semantic checkpoint recall limit exhausted".to_string(),
                });
            }
        }
        (selected, omitted)
    }

    pub async fn context_packet_from_entries(
        &self,
        entries: Vec<MemoryEntry>,
        max_items: usize,
        max_tokens: u64,
    ) -> MemoryKernelResult<MemoryContextPacket> {
        self.context_packet_from_entries_with_budget(
            entries,
            max_items,
            max_tokens,
            &self.manager.budget_config(),
            None,
        )
        .await
    }

    async fn context_packet_from_entries_with_budget(
        &self,
        mut entries: Vec<MemoryEntry>,
        max_items: usize,
        max_tokens: u64,
        budget: &crate::config::BudgetConfig,
        usage: Option<&MemoryUsageSummary>,
    ) -> MemoryKernelResult<MemoryContextPacket> {
        entries.sort_by(|left, right| {
            memory_entry_selection_rank_with_usage(right, usage)
                .cmp(&memory_entry_selection_rank_with_usage(left, usage))
        });
        let mut selected = Vec::new();
        let mut omitted = Vec::new();
        let mut token_estimate = 0_u64;
        let mut layer_tokens: HashMap<MemoryLayer, u64> = HashMap::new();
        let mut truncated = false;

        for entry in entries {
            let atom = self
                .atom_with_lifecycle_state(&entry, MemoryInformationState::Orientation)
                .await;
            let item_tokens = (entry.content.len() as u64 / 4).max(1);
            if let Some(layer_cap) = layer_budget_cap(budget, entry.layer) {
                let used = layer_tokens.get(&entry.layer).copied().unwrap_or_default();
                if used.saturating_add(item_tokens) > layer_cap {
                    truncated = true;
                    omitted.push(OmittedMemory {
                        id: entry.id,
                        title: entry.title,
                        reason: format!("layer {:?} budget exhausted", entry.layer),
                    });
                    continue;
                }
            }
            if selected.len() >= max_items
                || token_estimate.saturating_add(item_tokens) > max_tokens
            {
                truncated = true;
                omitted.push(OmittedMemory {
                    id: entry.id,
                    title: entry.title,
                    reason: "packet budget exhausted".to_string(),
                });
                continue;
            }

            let (role, mut reason) = packet_role_and_reason(&atom);
            if let Some(selected_count) =
                usage.and_then(|summary| summary.per_memory_selected.get(&entry.id).copied())
            {
                if selected_count > 0 {
                    reason.push_str(&format!("; usage_feedback:selected_count={selected_count}"));
                }
            }
            layer_tokens
                .entry(entry.layer)
                .and_modify(|used| *used = used.saturating_add(item_tokens))
                .or_insert(item_tokens);
            selected.push(MemoryPacketItem {
                atom,
                role,
                reason,
                content_preview: truncate_memory_content_preview(&entry.content),
            });
            token_estimate = token_estimate.saturating_add(item_tokens);
        }

        Ok(MemoryContextPacket {
            selected,
            omitted,
            token_estimate,
            truncated,
            recall_report: RecallReport::default(),
        })
    }

    pub async fn links(&self) -> MemoryKernelResult<Vec<MemoryLink>> {
        let entries = self.manager.list_all_entries().await?;
        Ok(build_links(&entries))
    }

    pub async fn clusters(&self, limit: usize) -> MemoryKernelResult<Vec<MemoryCluster>> {
        let entries = self
            .filter_active_entries(self.manager.list_all_entries().await?)
            .await;
        let mut clusters = cluster_entries(&entries, 960);
        clusters.truncate(limit.max(1));
        Ok(clusters)
    }

    pub async fn usage_summary(&self) -> MemoryKernelResult<MemoryUsageSummary> {
        Ok(self.manager.memory_usage_summary())
    }

    pub async fn runtime_snapshot(&self) -> MemoryKernelResult<MemoryRuntimeSnapshot> {
        let entries = self.manager.list_all_entries().await?;
        let active_entries = self.filter_active_entries(entries.clone()).await;
        let health = health_from_entries(&entries, None);
        let clusters = cluster_entries(&active_entries, 960);
        let usage = self.usage_summary().await.unwrap_or_default();
        Ok(MemoryRuntimeSnapshot {
            total_entries: entries.len(),
            active_entries: active_entries.len(),
            cluster_count: clusters.len(),
            hot_memory_count: usage.hot_memory_ids.len(),
            conflict_pressure: health.conflict_pressure,
            stale_pressure: health.stale_pressure,
            authority_ready: true,
            clusters: clusters.into_iter().take(8).collect(),
            usage,
        })
    }

    pub async fn path_recall(
        &self,
        start: MemoryId,
        max_hops: usize,
        max_nodes: usize,
    ) -> MemoryKernelResult<MemoryPath> {
        let entries = self.manager.list_all_entries().await?;
        let links = build_links(&entries);
        let entry_by_id: std::collections::HashMap<MemoryId, &MemoryEntry> =
            entries.iter().map(|entry| (entry.id, entry)).collect();
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::from([(start, 0usize)]);
        let mut path_entries = Vec::new();
        let mut path_links = Vec::new();
        let mut truncated = false;

        while let Some((current, depth)) = queue.pop_front() {
            if !visited.insert(current) {
                continue;
            }
            if let Some(entry) = entry_by_id.get(&current) {
                let mut atom = MemoryAtomView::from_entry(entry, MemoryInformationState::Trace);
                if let Ok(Some(state)) = self.latest_state(entry.id).await {
                    atom.state = state;
                }
                path_entries.push(atom);
            }
            if path_entries.len() >= max_nodes {
                truncated = queue.front().is_some()
                    || links
                        .iter()
                        .any(|link| link.from == current || link.to == current);
                break;
            }
            if depth >= max_hops {
                continue;
            }

            for link in links
                .iter()
                .filter(|link| link.from == current || link.to == current)
            {
                let next = if link.from == current {
                    link.to
                } else {
                    link.from
                };
                if !visited.contains(&next) {
                    path_links.push(link.clone());
                    queue.push_back((next, depth + 1));
                }
                if path_entries.len() + queue.len() >= max_nodes {
                    truncated = true;
                    break;
                }
            }
            if truncated {
                break;
            }
        }

        Ok(MemoryPath {
            entries: path_entries,
            links: path_links,
            truncated,
        })
    }

    pub async fn health(&self, _ctx: &MemoryTurnContext) -> MemoryKernelResult<MemoryHealth> {
        let started = Instant::now();
        let background_extraction = self.manager.background_extraction_health();
        let aggregate = match self
            .manager
            .store_aggregate(MEMORY_STALE_WARNING_THRESHOLD)
            .await
        {
            Ok(aggregate) => aggregate,
            Err(error) => {
                return Ok(MemoryHealth {
                    degraded: vec![MemoryDegradation::PrepareFailed(error.to_string())],
                    background_lag_ms: Some(started.elapsed().as_millis() as u64),
                    background_extraction,
                    ..MemoryHealth::default()
                });
            }
        };
        let mut health =
            health_from_aggregate(&aggregate, Some(started.elapsed().as_millis() as u64));
        if let Some(error) = background_extraction.last_error.as_ref() {
            health
                .degraded
                .push(MemoryDegradation::PostTurnFailed(error.clone()));
        } else if background_extraction.pending_requests > 0 {
            health.degraded.push(MemoryDegradation::DistillationBacklog);
        }
        if background_extraction.last_index_error.is_some() {
            health.degraded.push(MemoryDegradation::VectorUnavailable);
        }
        health.background_extraction = background_extraction;
        Ok(health)
    }

    /// Build a read-only projection for one governance layer.
    pub async fn layer_view(
        &self,
        layer: MemoryLayer,
        information_state: MemoryInformationState,
    ) -> MemoryKernelResult<MemoryLayerView> {
        let entries = self.manager.list_all_entries().await?;
        let atoms = self
            .atoms_with_lifecycle_state(
                entries.iter().filter(|entry| entry.layer == layer),
                information_state,
            )
            .await;
        Ok(MemoryLayerView::new(layer, atoms))
    }

    /// Build read-only projections for all governance layers.
    pub async fn layer_views(
        &self,
        information_state: MemoryInformationState,
    ) -> MemoryKernelResult<Vec<MemoryLayerView>> {
        let entries = self.manager.list_all_entries().await?;
        let layers = [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
            MemoryLayer::L4,
        ];
        let mut views = Vec::with_capacity(layers.len());
        for layer in layers {
            let atoms = self
                .atoms_with_lifecycle_state(
                    entries.iter().filter(|entry| entry.layer == layer),
                    information_state,
                )
                .await;
            views.push(MemoryLayerView::new(layer, atoms));
        }
        Ok(views)
    }

    async fn atoms_with_lifecycle_state<'a>(
        &self,
        entries: impl Iterator<Item = &'a MemoryEntry>,
        information_state: MemoryInformationState,
    ) -> Vec<MemoryAtomView> {
        let entries = entries.collect::<Vec<_>>();
        let states = self
            .latest_states(entries.iter().map(|entry| entry.id))
            .await
            .unwrap_or_default();
        entries
            .into_iter()
            .map(|entry| {
                let mut atom = MemoryAtomView::from_entry(entry, information_state);
                if let Some(Some(state)) = states.get(&entry.id) {
                    atom.state = *state;
                }
                atom
            })
            .collect()
    }

    async fn atom_with_lifecycle_state(
        &self,
        entry: &MemoryEntry,
        information_state: MemoryInformationState,
    ) -> MemoryAtomView {
        let mut atom = MemoryAtomView::from_entry(entry, information_state);
        if let Ok(Some(state)) = self.latest_state(entry.id).await {
            atom.state = state;
        }
        atom
    }

    fn empty_degraded_context() -> PreparedContext {
        PreparedContext {
            entries: Vec::new(),
            total_tokens: 0,
            budget: TokenBudget {
                total: 0,
                reserved_system: 0,
                reserved_response: 0,
                allocated_memory: 0,
                allocated_conversation: 0,
                available: 0,
            },
            depth_scale: 0.0,
            prepared_at: Utc::now(),
            code_context: None,
        }
    }

    async fn record_lifecycle_event(
        &self,
        ctx: &MemoryTurnContext,
        memory_id: MemoryId,
        from: Option<MemoryState>,
        to: MemoryState,
        reason: impl Into<String>,
    ) -> MemoryKernelResult<()> {
        let mut events = self.load_lifecycle_events(memory_id).await?;
        events.push(MemoryLifecycleEvent {
            memory_id,
            from,
            to,
            reason: reason.into(),
            session_id: ctx.session_id.clone(),
            agent_id: ctx.agent_id.clone(),
            occurred_at: Utc::now(),
        });

        let raw = serde_json::to_string(&events).map_err(crate::MemoryError::Serialisation)?;
        self.manager
            .kernel_kv_put(&lifecycle_key(memory_id), &raw)
            .await?;
        Ok(())
    }

    async fn latest_state(&self, memory_id: MemoryId) -> MemoryKernelResult<Option<MemoryState>> {
        Ok(self
            .load_lifecycle_events(memory_id)
            .await?
            .last()
            .map(|event| event.to))
    }

    async fn load_lifecycle_events(
        &self,
        memory_id: MemoryId,
    ) -> MemoryKernelResult<Vec<MemoryLifecycleEvent>> {
        let raw = self
            .manager
            .kernel_kv_get(&lifecycle_key(memory_id))
            .await?;
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };

        serde_json::from_str(&raw).map_err(|error| {
            MemoryKernelError::Backend(MemoryError::Store(format!(
                "decode lifecycle events for {memory_id}: {error}"
            )))
        })
    }

    async fn authority_match(
        &self,
        incoming: &MemoryEntry,
    ) -> MemoryKernelResult<Option<(MemoryId, MemoryAuthorityDecision)>> {
        let incoming_key = same_memory_key(incoming);
        let entries = self
            .manager
            .authority_candidates(AuthorityLookup {
                fingerprint: incoming_key.clone(),
                scope: incoming.scope.clone(),
                limit: 64,
            })
            .await?;
        let states = self
            .latest_states(entries.iter().map(|entry| entry.id))
            .await
            .unwrap_or_default();
        let mut best: Option<(MemoryId, MemoryAuthorityDecision)> = None;
        for existing in entries {
            if existing.id == incoming.id || same_memory_key(&existing) != incoming_key {
                continue;
            }
            if matches!(
                states.get(&existing.id).copied().flatten(),
                Some(MemoryState::Superseded | MemoryState::Archived)
            ) {
                continue;
            }
            let decision = authority_decision(&existing, incoming);
            if matches!(
                decision.action,
                MemoryAuthorityAction::SupersedeExisting
                    | MemoryAuthorityAction::MarkConflict
                    | MemoryAuthorityAction::Duplicate
            ) {
                best = Some((existing.id, decision));
                break;
            }
        }
        Ok(best)
    }

    async fn record_context_usage(
        &self,
        ctx: &MemoryTurnContext,
        packet: &MemoryContextPacket,
    ) -> MemoryKernelResult<()> {
        if packet.selected.is_empty() {
            return Ok(());
        }
        for item in &packet.selected {
            self.manager.record_memory_usage_signal(MemoryUsageSignal {
                memory_id: item.atom.id,
                session_id: ctx.session_id.clone(),
                agent_id: ctx.agent_id.clone(),
                selected_count: 1,
                last_reason: item.reason.clone(),
            });
        }
        Ok(())
    }
}

fn checkpoint_memory_id(checkpoint_id: &str, fact_index: usize) -> MemoryId {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("cowd:semantic-checkpoint:{checkpoint_id}:fact:{fact_index}").as_bytes(),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryRuntimeSnapshot {
    pub total_entries: usize,
    pub active_entries: usize,
    pub cluster_count: usize,
    pub hot_memory_count: usize,
    pub conflict_pressure: f32,
    pub stale_pressure: f32,
    pub authority_ready: bool,
    pub clusters: Vec<MemoryCluster>,
    pub usage: MemoryUsageSummary,
}

fn lifecycle_key(memory_id: MemoryId) -> String {
    format!("memory_lifecycle:{memory_id}")
}

fn build_links(entries: &[MemoryEntry]) -> Vec<MemoryLink> {
    let mut links = Vec::new();
    let mut by_session: std::collections::HashMap<&str, Vec<MemoryId>> =
        std::collections::HashMap::new();
    let mut by_agent: std::collections::HashMap<&str, Vec<MemoryId>> =
        std::collections::HashMap::new();
    let mut by_tag: std::collections::HashMap<&str, Vec<MemoryId>> =
        std::collections::HashMap::new();

    for entry in entries {
        for relation in &entry.relations {
            links.push(MemoryLink {
                from: entry.id,
                to: relation.target_id,
                kind: relation_kind_to_link_kind(relation.kind),
                weight: relation.strength.clamp(0.0, 1.0),
                evidence: format!("relation:{:?}", relation.kind),
            });
        }
        if let Some(session_id) = entry.session_id.as_deref() {
            by_session.entry(session_id).or_default().push(entry.id);
        }
        if let Some(agent_id) = entry.source_agent.as_deref() {
            by_agent.entry(agent_id).or_default().push(entry.id);
        }
        for tag in &entry.tags {
            by_tag.entry(tag.as_str()).or_default().push(entry.id);
        }
    }

    add_group_links(&mut links, by_session, MemoryLinkKind::BelongsTo, "session");
    add_group_links(&mut links, by_agent, MemoryLinkKind::ProducedBy, "agent");
    add_group_links(&mut links, by_tag, MemoryLinkKind::Mentions, "tag");
    links
}

fn add_group_links(
    links: &mut Vec<MemoryLink>,
    groups: std::collections::HashMap<&str, Vec<MemoryId>>,
    kind: MemoryLinkKind,
    evidence_prefix: &str,
) {
    for (key, ids) in groups {
        for pair in ids.windows(2) {
            links.push(MemoryLink {
                from: pair[0],
                to: pair[1],
                kind,
                weight: 0.5,
                evidence: format!("{evidence_prefix}:{key}"),
            });
        }
    }
}

fn relation_kind_to_link_kind(kind: crate::types::RelationKind) -> MemoryLinkKind {
    match kind {
        crate::types::RelationKind::DependsOn => MemoryLinkKind::DependsOn,
        crate::types::RelationKind::Supersedes => MemoryLinkKind::Supersedes,
        crate::types::RelationKind::Summarizes => MemoryLinkKind::Summarizes,
        _ => MemoryLinkKind::Related,
    }
}

fn packet_role_and_reason(atom: &MemoryAtomView) -> (MemoryPacketRole, String) {
    match atom.state {
        MemoryState::Conflicted => (
            MemoryPacketRole::Conflict,
            "memory has low confidence and needs review".to_string(),
        ),
        MemoryState::Stale => (
            MemoryPacketRole::Warning,
            "memory is stale and should be treated cautiously".to_string(),
        ),
        MemoryState::Candidate => (
            MemoryPacketRole::Supporting,
            "candidate memory can support reasoning but is not authoritative".to_string(),
        ),
        MemoryState::Observed => (
            MemoryPacketRole::Supporting,
            "observed memory can support reasoning but is not yet validated".to_string(),
        ),
        MemoryState::Active | MemoryState::Validated if atom.is_explainable_orientation() => (
            MemoryPacketRole::Orientation,
            "active explainable orientation memory".to_string(),
        ),
        MemoryState::Active | MemoryState::Validated => (
            MemoryPacketRole::Supporting,
            "active memory lacks explicit orientation evidence".to_string(),
        ),
        MemoryState::Superseded | MemoryState::Archived => (
            MemoryPacketRole::Warning,
            "inactive memory should not be used as current orientation".to_string(),
        ),
    }
}

pub(crate) fn scoped_entry_scope(ctx: &MemoryTurnContext, entry: &MemoryEntry) -> MemoryScope {
    match &entry.scope {
        // `MemoryScope::default()` is the historical Session("") sentinel.
        // Never persist it: it is neither visible to its source session nor a
        // meaningful authorization boundary.
        MemoryScope::Session(session_id) if session_id.trim().is_empty() => {
            default_scope_for_entry(ctx, entry)
        }
        // Global is idempotent for content whose durability semantics or
        // explicit authority actually permits workspace-wide visibility.
        // An inferred private L2/L3 atom cannot promote itself to Global merely
        // by carrying that enum value.
        MemoryScope::Global
            if entry.layer == MemoryLayer::L0
                || !matches!(entry.visibility, AgentVisibility::Private)
                || matches!(
                    entry.source,
                    MemorySource::UserExplicit | MemorySource::Import
                )
                || entry.tags.iter().any(|tag| {
                    matches!(tag.as_str(), "memory-policy:always" | "always-active")
                }) =>
        {
            MemoryScope::Global
        }
        MemoryScope::Global => MemoryScope::AgentInstance(ctx.agent_id.clone()),
        MemoryScope::Project(project) if project == "default" => ctx
            .project_id
            .as_ref()
            .map(|project_id| MemoryScope::Project(project_id.clone()))
            .unwrap_or_else(|| MemoryScope::Session(ctx.session_id.clone())),
        MemoryScope::Task(task) if task == "default" => ctx
            .task_id
            .as_ref()
            .map(|task_id| MemoryScope::Task(task_id.clone()))
            .unwrap_or_else(|| MemoryScope::Session(ctx.session_id.clone())),
        other => other.clone(),
    }
}

fn default_scope_for_entry(ctx: &MemoryTurnContext, entry: &MemoryEntry) -> MemoryScope {
    if entry.layer == MemoryLayer::L0
        || entry
            .tags
            .iter()
            .any(|tag| matches!(tag.as_str(), "memory-policy:always" | "always-active"))
    {
        return MemoryScope::Global;
    }
    if entry.category == MemoryCategory::Shared {
        if let Some(team_id) = ctx.team_id.as_deref().filter(|id| !id.trim().is_empty()) {
            return MemoryScope::TeamRun(team_id.to_string());
        }
    }
    let is_session_checkpoint = entry.category == MemoryCategory::CompressedSummary
        || entry.tags.iter().any(|tag| tag == "semantic-checkpoint");
    if matches!(entry.layer, MemoryLayer::L2 | MemoryLayer::L3) && !is_session_checkpoint {
        if let Some(project_id) = ctx.project_id.as_deref().filter(|id| !id.trim().is_empty()) {
            return MemoryScope::Project(project_id.to_string());
        }
    }
    default_scope_for_ctx(ctx)
}

pub(crate) fn default_scope_for_ctx(ctx: &MemoryTurnContext) -> MemoryScope {
    if let Some(task_id) = ctx.task_id.as_deref().filter(|id| !id.trim().is_empty()) {
        return MemoryScope::Task(task_id.to_string());
    }
    if let Some(project_id) = ctx.project_id.as_deref().filter(|id| !id.trim().is_empty()) {
        return MemoryScope::Project(project_id.to_string());
    }
    MemoryScope::Session(ctx.session_id.clone())
}

fn memory_entry_visible_to_ctx(entry: &MemoryEntry, ctx: &MemoryTurnContext) -> bool {
    // Historic extraction treated every UserPreference as globally readable.
    // A private inferred topic preference is not workspace-wide authority even
    // when an older row already carries `scope=global`.
    if matches!(entry.scope, MemoryScope::Global)
        && entry.layer != MemoryLayer::L0
        && matches!(entry.visibility, AgentVisibility::Private)
        && matches!(
            entry.source,
            MemorySource::AutoExtracted | MemorySource::Compression | MemorySource::Prefetch
        )
        && !entry
            .tags
            .iter()
            .any(|tag| matches!(tag.as_str(), "memory-policy:always" | "always-active"))
    {
        return false;
    }
    memory_scope_visible_to_ctx(&entry.scope, ctx)
}

fn memory_binding_search_scopes(ctx: &MemoryTurnContext) -> Vec<MemoryScope> {
    use harness_contract::agent::CognitiveReadScope;

    let mut scopes = vec![MemoryScope::AgentInstance(ctx.agent_id.clone())];
    if ctx
        .cognitive_read_scopes
        .contains(&CognitiveReadScope::Session)
    {
        scopes.push(MemoryScope::Session(ctx.session_id.clone()));
        if let Some(task_id) = ctx.task_id.as_deref().filter(|id| !id.trim().is_empty()) {
            scopes.push(MemoryScope::Task(task_id.to_string()));
        }
    }
    if ctx
        .cognitive_read_scopes
        .contains(&CognitiveReadScope::Project)
    {
        if let Some(project_id) = ctx.project_id.as_deref().filter(|id| !id.trim().is_empty()) {
            scopes.push(MemoryScope::Project(project_id.to_string()));
        }
    }
    if ctx
        .cognitive_read_scopes
        .contains(&CognitiveReadScope::DefinitionLineage)
    {
        if let Some(definition_id) = ctx
            .definition_lineage_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
        {
            scopes.push(MemoryScope::AgentDefinitionLineage(
                definition_id.to_string(),
            ));
        }
    }
    if ctx
        .cognitive_read_scopes
        .contains(&CognitiveReadScope::Team)
    {
        if let Some(team_id) = ctx.team_id.as_deref().filter(|id| !id.trim().is_empty()) {
            scopes.push(MemoryScope::TeamRun(team_id.to_string()));
        }
    }
    if ctx
        .cognitive_read_scopes
        .contains(&CognitiveReadScope::WorkspaceKnowledge)
    {
        scopes.push(MemoryScope::Global);
    }
    scopes.sort_by_key(MemoryScope::scope_key);
    scopes.dedup();
    scopes
}

pub(crate) fn memory_scope_visible_to_ctx(scope: &MemoryScope, ctx: &MemoryTurnContext) -> bool {
    use harness_contract::agent::CognitiveReadScope;

    match scope {
        MemoryScope::Global => ctx
            .cognitive_read_scopes
            .contains(&CognitiveReadScope::WorkspaceKnowledge),
        MemoryScope::Session(session_id) => {
            session_id == &ctx.session_id
                && ctx
                    .cognitive_read_scopes
                    .contains(&CognitiveReadScope::Session)
        }
        MemoryScope::Project(project_id) => {
            ctx.project_id.as_ref() == Some(project_id)
                && ctx
                    .cognitive_read_scopes
                    .contains(&CognitiveReadScope::Project)
        }
        MemoryScope::Task(task_id) => {
            ctx.task_id.as_ref() == Some(task_id)
                && ctx
                    .cognitive_read_scopes
                    .contains(&CognitiveReadScope::Session)
        }
        MemoryScope::AgentDefinitionLineage(definition_id) => {
            ctx.definition_lineage_id.as_ref() == Some(definition_id)
                && ctx
                    .cognitive_read_scopes
                    .contains(&CognitiveReadScope::DefinitionLineage)
        }
        MemoryScope::AgentInstance(instance_id) => instance_id == &ctx.agent_id,
        MemoryScope::TeamRun(team_id) => {
            ctx.team_id.as_ref() == Some(team_id)
                && ctx
                    .cognitive_read_scopes
                    .contains(&CognitiveReadScope::Team)
        }
        MemoryScope::LegacyUnresolvedAgent(_) => false,
    }
}

#[derive(Debug, Clone, Copy)]
struct TurnRelevanceDecision {
    accepted: bool,
    score: f32,
    threshold: f32,
}

fn memory_entry_turn_relevance(
    entry: &MemoryEntry,
    query: &str,
    vector_score: Option<f32>,
) -> TurnRelevanceDecision {
    if entry.layer == MemoryLayer::L0
        || entry
            .tags
            .iter()
            .any(|tag| matches!(tag.as_str(), "memory-policy:always" | "always-active"))
    {
        return TurnRelevanceDecision {
            accepted: true,
            score: 1.0,
            threshold: 0.0,
        };
    }

    let lexical_score = query_overlap_score(query, &entry.title, &entry.content);
    let semantic_score = vector_score.unwrap_or_default().clamp(0.0, 1.0);
    let score = lexical_score.max(semantic_score);
    let threshold = match entry.scope {
        MemoryScope::Session(_) | MemoryScope::Task(_) => 0.05,
        MemoryScope::AgentInstance(_)
        | MemoryScope::AgentDefinitionLineage(_)
        | MemoryScope::TeamRun(_) => 0.07,
        MemoryScope::Project(_) => 0.08,
        MemoryScope::Global | MemoryScope::LegacyUnresolvedAgent(_) => 0.12,
    };
    TurnRelevanceDecision {
        accepted: score >= threshold,
        score,
        threshold,
    }
}

fn checkpoint_recall_score(entry: &MemoryEntry, ctx: &MemoryTurnContext, query: &str) -> f32 {
    let scope_score = match &entry.scope {
        MemoryScope::Task(task_id) if ctx.task_id.as_ref() == Some(task_id) => 0.34,
        MemoryScope::Session(session_id) if session_id == &ctx.session_id => 0.30,
        MemoryScope::Project(project_id) if ctx.project_id.as_ref() == Some(project_id) => 0.20,
        MemoryScope::TeamRun(team_id) if ctx.team_id.as_ref() == Some(team_id) => 0.22,
        MemoryScope::AgentDefinitionLineage(definition_id)
            if ctx.definition_lineage_id.as_ref() == Some(definition_id) =>
        {
            0.18
        }
        MemoryScope::AgentInstance(instance_id) if instance_id == &ctx.agent_id => 0.16,
        MemoryScope::Global => 0.08,
        _ => 0.0,
    };
    let overlap_score = query_overlap_score(query, &entry.title, &entry.content);
    let kind_score = if entry
        .tags
        .iter()
        .any(|tag| tag == "fact-kind:constraint" || tag == "fact-kind:summary")
    {
        0.08
    } else if entry
        .tags
        .iter()
        .any(|tag| tag == "fact-kind:decision" || tag == "fact-kind:pendingwork")
    {
        0.06
    } else {
        0.03
    };
    if overlap_score <= 0.0 && !query.trim().is_empty() {
        return (scope_score + kind_score * 0.2 + entry.confidence.clamp(0.0, 1.0) * 0.02)
            .min(0.30);
    }
    (scope_score + overlap_score + kind_score + entry.confidence.clamp(0.0, 1.0) * 0.08).min(1.0)
}

fn recall_report_from_packet(
    packet: &MemoryContextPacket,
    source_by_id: HashMap<MemoryId, RecallSourceKind>,
    vector_scores: HashMap<MemoryId, f32>,
    mut sources: Vec<RecallSourceResult>,
) -> RecallReport {
    let selected = packet
        .selected
        .iter()
        .map(|item| {
            let source = source_by_id
                .get(&item.atom.id)
                .copied()
                .unwrap_or(RecallSourceKind::Memory);
            let relevance = memory_atom_relevance(&item.atom);
            let entry = memory_entry_from_packet_item(item);
            let mut candidate = RecallCandidate::from_entry(entry, source, relevance);
            if let Some(score) = vector_scores.get(&item.atom.id).copied() {
                candidate = candidate.with_vector_similarity(score);
            }
            candidate
        })
        .collect::<Vec<_>>();
    let omitted = packet
        .omitted
        .iter()
        .map(|item| RecallOmission {
            id: item.id,
            title: item.title.clone(),
            source: RecallSourceKind::Memory,
            reason: item.reason.clone(),
        })
        .collect::<Vec<_>>();
    let memory_selected = selected.len();
    let memory_omitted = omitted.len();
    if let Some(memory_source) = sources
        .iter_mut()
        .find(|source| source.source == RecallSourceKind::Memory)
    {
        memory_source.selected_count = memory_source.selected_count.max(memory_selected);
        memory_source.omitted_count = memory_source.omitted_count.max(memory_omitted);
    } else {
        sources.push(RecallSourceResult {
            source: RecallSourceKind::Memory,
            status: "enabled_and_wired".to_string(),
            selected_count: memory_selected,
            omitted_count: memory_omitted,
            degraded_reason: None,
        });
    }
    RecallReport::from_selected_omitted(selected, omitted, sources, packet.truncated)
}

fn memory_entry_from_packet_item(item: &MemoryPacketItem) -> MemoryEntry {
    MemoryEntry {
        id: item.atom.id,
        layer: item.atom.layer,
        category: crate::types::MemoryCategory::Reference,
        priority: if item.atom.salience >= 3.0 {
            Priority::High
        } else {
            Priority::Normal
        },
        source: MemorySource::AutoExtracted,
        title: item.atom.title.clone(),
        content: if item.content_preview.trim().is_empty() {
            item.reason.clone()
        } else {
            item.content_preview.clone()
        },
        embedding: None,
        tags: vec![format!("packet-role:{:?}", item.role)],
        relations: Vec::new(),
        confidence: item.atom.confidence,
        access_count: 0,
        staleness: (1.0 - item.atom.salience / 5.0).clamp(0.0, 1.0),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Global,
        session_id: None,
        source_agent: None,
        visibility: AgentVisibility::Shared,
    }
}

fn truncate_memory_content_preview(content: &str) -> String {
    const MAX_CHARS: usize = 480;
    let mut chars = content.chars();
    let preview = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn memory_atom_relevance(atom: &MemoryAtomView) -> f32 {
    let state_score = match atom.state {
        MemoryState::Validated => 1.0,
        MemoryState::Active => 0.85,
        MemoryState::Observed => 0.75,
        MemoryState::Candidate => 0.60,
        MemoryState::Conflicted => 0.35,
        MemoryState::Stale | MemoryState::Superseded | MemoryState::Archived => 0.20,
    };
    (state_score * 0.6 + atom.confidence.clamp(0.0, 1.0) * 0.4).clamp(0.0, 1.0)
}

fn query_overlap_score(query: &str, title: &str, content: &str) -> f32 {
    let query = query.trim().to_lowercase();
    let haystack = format!("{} {}", title.to_lowercase(), content.to_lowercase());
    if query.chars().count() > 4 && haystack.contains(query.as_str()) {
        return 0.46;
    }
    let query_terms = semantic_relevance_terms(&query);
    if query_terms.is_empty() {
        return 0.0;
    }
    let haystack_terms = semantic_relevance_terms(&haystack);
    let matched = query_terms.intersection(&haystack_terms).count();
    if matched == 0 {
        return 0.0;
    }
    let coverage = matched as f32 / query_terms.len() as f32;
    let specificity =
        matched as f32 / haystack_terms.len().max(1).min(query_terms.len() * 4) as f32;
    (coverage * 0.36 + specificity.min(1.0) * 0.10).min(0.46)
}

fn semantic_relevance_terms(text: &str) -> HashSet<String> {
    let mut terms = HashSet::new();
    let mut ascii_word = String::new();
    let mut cjk_run = Vec::new();

    let flush_ascii = |word: &mut String, terms: &mut HashSet<String>| {
        if word.chars().count() >= 2 && !is_generic_relevance_term(word) {
            terms.insert(std::mem::take(word));
        } else {
            word.clear();
        }
    };
    let flush_cjk = |run: &mut Vec<char>, terms: &mut HashSet<String>| {
        if run.len() == 1 {
            terms.insert(run[0].to_string());
        } else {
            for pair in run.windows(2) {
                terms.insert(pair.iter().collect());
            }
        }
        run.clear();
    };

    for character in text.chars().flat_map(char::to_lowercase) {
        if is_cjk(character) {
            flush_ascii(&mut ascii_word, &mut terms);
            cjk_run.push(character);
        } else {
            flush_cjk(&mut cjk_run, &mut terms);
            if character.is_alphanumeric() || matches!(character, '_' | '-') {
                ascii_word.push(character);
            } else {
                flush_ascii(&mut ascii_word, &mut terms);
            }
        }
    }
    flush_ascii(&mut ascii_word, &mut terms);
    flush_cjk(&mut cjk_run, &mut terms);
    terms
}

fn is_cjk(character: char) -> bool {
    matches!(
        character,
        '\u{3400}'..='\u{4dbf}'
            | '\u{4e00}'..='\u{9fff}'
            | '\u{f900}'..='\u{faff}'
    )
}

fn is_generic_relevance_term(term: &str) -> bool {
    matches!(
        term,
        "the"
            | "and"
            | "for"
            | "with"
            | "from"
            | "this"
            | "that"
            | "have"
            | "what"
            | "when"
            | "where"
            | "how"
            | "please"
            | "分析"
            | "问题"
            | "现在"
            | "当前"
            | "进行"
            | "这个"
            | "那个"
    )
}

fn memory_entry_selection_rank(entry: &MemoryEntry) -> i32 {
    let explicit = matches!(entry.source, MemorySource::UserExplicit);
    let checkpoint = entry.tags.iter().any(|tag| tag == "semantic-checkpoint");
    match (explicit, entry.layer, checkpoint) {
        (true, MemoryLayer::L0 | MemoryLayer::L1, _) => 100,
        (true, _, _) => 90,
        (_, MemoryLayer::L0, _) => 80,
        (_, MemoryLayer::L1, _) => 70,
        (_, _, true) => 60,
        (_, MemoryLayer::L2, _) => 50,
        (_, MemoryLayer::L3, _) => 40,
        (_, MemoryLayer::L4, _) => 30,
    }
}

fn memory_entry_selection_rank_with_usage(
    entry: &MemoryEntry,
    usage: Option<&MemoryUsageSummary>,
) -> i32 {
    let base = memory_entry_selection_rank(entry);
    let selected_count = usage
        .and_then(|summary| summary.per_memory_selected.get(&entry.id).copied())
        .unwrap_or_default();
    let boost = match selected_count {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=5 => 4,
        _ => 6,
    };
    let stale_penalty = if entry.staleness >= MEMORY_STALE_WARNING_THRESHOLD {
        MEMORY_STALE_RANK_PENALTY
    } else if entry.staleness >= 0.65 {
        MEMORY_AGING_RANK_PENALTY
    } else {
        0
    };
    base + boost - stale_penalty
}

fn deduplicate_memory_entries_for_recall(
    entries: Vec<MemoryEntry>,
) -> (Vec<MemoryEntry>, Vec<OmittedMemory>) {
    let mut by_key: HashMap<String, usize> = HashMap::new();
    let mut deduplicated: Vec<MemoryEntry> = Vec::with_capacity(entries.len());
    let mut omitted = Vec::new();

    for entry in entries {
        let key = memory_entry_dedup_key(&entry);
        let Some(existing_index) = by_key.get(&key).copied() else {
            by_key.insert(key, deduplicated.len());
            deduplicated.push(entry);
            continue;
        };

        if memory_entry_is_better_recall_representative(&entry, &deduplicated[existing_index]) {
            let previous = std::mem::replace(&mut deduplicated[existing_index], entry);
            omitted.push(OmittedMemory {
                id: previous.id,
                title: previous.title,
                reason: format!(
                    "duplicate recall candidate merged into {}",
                    deduplicated[existing_index].id
                ),
            });
        } else {
            omitted.push(OmittedMemory {
                id: entry.id,
                title: entry.title,
                reason: format!(
                    "duplicate recall candidate merged into {}",
                    deduplicated[existing_index].id
                ),
            });
        }
    }

    (deduplicated, omitted)
}

fn memory_entry_is_better_recall_representative(
    candidate: &MemoryEntry,
    current: &MemoryEntry,
) -> bool {
    let candidate_rank = memory_entry_selection_rank(candidate);
    let current_rank = memory_entry_selection_rank(current);
    candidate_rank
        .cmp(&current_rank)
        .then_with(|| {
            current
                .staleness
                .partial_cmp(&candidate.staleness)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            candidate
                .confidence
                .partial_cmp(&current.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| candidate.updated_at.cmp(&current.updated_at))
        .is_gt()
}

fn memory_entry_dedup_key(entry: &MemoryEntry) -> String {
    let title = normalize_memory_dedup_text(&entry.title);
    let content = normalize_memory_dedup_text(&entry.content);
    if title.len().saturating_add(content.len()) < 12 {
        return entry.id.to_string();
    }
    format!("{title}\n{content}")
}

fn normalize_memory_dedup_text(text: &str) -> String {
    text.to_lowercase()
        .replace('…', "...")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn layer_budget_cap(budget: &crate::config::BudgetConfig, layer: MemoryLayer) -> Option<u64> {
    if !budget.runtime_managed {
        return None;
    }
    let cap = match layer {
        MemoryLayer::L0 => budget.l0_reserved,
        MemoryLayer::L1 => budget.l1_working,
        MemoryLayer::L2 => budget.l2_project,
        MemoryLayer::L3 => budget.l3_deep.saturating_add(budget.l3_checkpoint),
        MemoryLayer::L4 => budget.l4_shared,
    };
    (cap > 0).then_some(cap)
}

fn health_from_entries(entries: &[MemoryEntry], background_lag_ms: Option<u64>) -> MemoryHealth {
    if entries.is_empty() {
        return MemoryHealth {
            background_lag_ms,
            ..MemoryHealth::default()
        };
    }

    let total = entries.len() as f32;
    let orientation_like = entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.layer,
                MemoryLayer::L0 | MemoryLayer::L1 | MemoryLayer::L2
            )
        })
        .count() as f32;
    let conflicted = entries
        .iter()
        .filter(|entry| entry.confidence < 0.35)
        .count() as f32;
    let stale = entries
        .iter()
        .filter(|entry| entry.staleness >= MEMORY_STALE_WARNING_THRESHOLD)
        .count() as f32;
    let evidence_backed = entries
        .iter()
        .filter(|entry| {
            matches!(entry.layer, MemoryLayer::L0 | MemoryLayer::L1)
                || matches!(
                    entry.source,
                    crate::types::MemorySource::UserExplicit | crate::types::MemorySource::Import
                )
                || matches!(
                    entry.layer,
                    MemoryLayer::L2 | MemoryLayer::L3 | MemoryLayer::L4
                )
        })
        .count() as f32;
    let linked = entries
        .iter()
        .filter(|entry| !entry.relations.is_empty() || !entry.tags.is_empty())
        .count() as f32;

    MemoryHealth {
        orientation_pressure: (orientation_like / total).clamp(0.0, 1.0),
        conflict_pressure: (conflicted / total).clamp(0.0, 1.0),
        stale_pressure: (stale / total).clamp(0.0, 1.0),
        evidence_coverage: (evidence_backed / total).clamp(0.0, 1.0),
        link_coverage: (linked / total).clamp(0.0, 1.0),
        background_lag_ms,
        background_extraction: BackgroundExtractionHealth::default(),
        degraded: Vec::new(),
    }
}

fn health_from_aggregate(
    aggregate: &crate::store::MemoryStoreAggregate,
    background_lag_ms: Option<u64>,
) -> MemoryHealth {
    if aggregate.total_entries == 0 {
        return MemoryHealth {
            background_lag_ms,
            ..MemoryHealth::default()
        };
    }
    let total = aggregate.total_entries as f32;
    MemoryHealth {
        orientation_pressure: (aggregate.orientation_like as f32 / total).clamp(0.0, 1.0),
        conflict_pressure: (aggregate.conflicted as f32 / total).clamp(0.0, 1.0),
        stale_pressure: (aggregate.stale as f32 / total).clamp(0.0, 1.0),
        evidence_coverage: (aggregate.evidence_backed as f32 / total).clamp(0.0, 1.0),
        link_coverage: (aggregate.linked as f32 / total).clamp(0.0, 1.0),
        background_lag_ms,
        background_extraction: BackgroundExtractionHealth::default(),
        degraded: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;
    use crate::compression::session::{
        CheckpointFactKind, CheckpointTokenStats, CompactionSourceRange, SessionCheckpointFact,
        SessionResumeCursor, SessionSemanticCheckpoint,
    };
    use crate::types::{AgentVisibility, MemoryCategory};
    use harness_contract::reality::EvidenceRef;

    fn memory_entry(
        title: &str,
        content: &str,
        staleness: f32,
        confidence: f32,
        updated_offset_secs: i64,
    ) -> MemoryEntry {
        let now = Utc::now();
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::High,
            source: MemorySource::AutoExtracted,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: vec!["knowledge".to_string()],
            relations: Vec::new(),
            confidence,
            access_count: 0,
            staleness,
            created_at: now - Duration::days(1),
            updated_at: now + Duration::seconds(updated_offset_secs),
            last_accessed_at: None,
            scope: MemoryScope::Project("cowd".to_string()),
            session_id: Some("s1".to_string()),
            source_agent: Some("kernel-test".to_string()),
            visibility: AgentVisibility::Shared,
        }
    }

    #[test]
    fn stale_memory_becomes_warning_at_threshold() {
        let entry = memory_entry("Old decision", "Use the previous API", 0.86, 1.0, 0);

        let atom = MemoryAtomView::from_entry(&entry, MemoryInformationState::Orientation);
        let (role, reason) = packet_role_and_reason(&atom);

        assert_eq!(atom.state, MemoryState::Stale);
        assert_eq!(role, MemoryPacketRole::Warning);
        assert!(reason.contains("stale"));
        assert!(
            memory_entry_selection_rank_with_usage(&entry, None)
                < memory_entry_selection_rank(&entry)
        );
    }

    #[test]
    fn deduplicate_memory_entries_for_recall_keeps_fresher_candidate() {
        let stale = memory_entry(
            "Runtime rule",
            "Do not expand context endlessly",
            0.91,
            0.95,
            -30,
        );
        let fresh = memory_entry(
            "Runtime rule",
            "Do not expand context endlessly",
            0.05,
            0.90,
            30,
        );
        let fresh_id = fresh.id;

        let (deduplicated, omitted) = deduplicate_memory_entries_for_recall(vec![stale, fresh]);

        assert_eq!(deduplicated.len(), 1);
        assert_eq!(deduplicated[0].id, fresh_id);
        assert_eq!(omitted.len(), 1);
        assert!(omitted[0].reason.contains("duplicate recall candidate"));
    }

    #[tokio::test]
    async fn exact_memory_retrieval_respects_runtime_binding_scope() {
        let temp = tempfile::tempdir().expect("temporary memory root");
        let manager = Arc::new(
            CognitiveContextManager::new(crate::config::MemoryConfig {
                store: crate::config::StoreConfig {
                    sqlite_path: temp.path().join("memory.sqlite"),
                    blob_dir: temp.path().join("blobs"),
                    enable_vector_index: false,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("manager"),
        );
        let kernel = MemoryKernel::new(Arc::clone(&manager));
        let visible = memory_entry("Visible", "current project fact", 0.0, 0.95, 0);
        let visible_id = visible.id;
        let hidden = MemoryEntry {
            scope: MemoryScope::Project("other-project".to_string()),
            ..memory_entry("Hidden", "other project fact", 0.0, 0.95, 0)
        };
        let hidden_id = hidden.id;
        manager.remember(visible).await.expect("visible memory");
        manager.remember(hidden).await.expect("hidden memory");
        let context = MemoryTurnContext::new("session-a", "agent-a")
            .with_project_id(Some("cowd".to_string()))
            .with_cognitive_read_scopes(vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Project,
            ]);

        assert!(kernel
            .retrieve_visible_entry(&context, visible_id)
            .await
            .expect("visible lookup")
            .is_some());
        assert!(kernel
            .retrieve_visible_entry(&context, hidden_id)
            .await
            .expect("hidden lookup")
            .is_none());
    }

    #[test]
    fn turn_relevance_rejects_unrelated_high_priority_memory() {
        let mut entry = memory_entry(
            "Medicine inventory policy",
            "Track prescription inventory by batch and expiry date.",
            0.0,
            0.98,
            0,
        );
        entry.layer = MemoryLayer::L1;
        entry.priority = Priority::Critical;
        entry.scope = MemoryScope::Session("session-a".to_string());

        let decision = memory_entry_turn_relevance(&entry, "分析 Rust 会话上下文压缩机制", None);

        assert!(!decision.accepted);
        assert!(decision.score < decision.threshold);
    }

    #[test]
    fn turn_relevance_understands_chinese_terms() {
        let mut entry = memory_entry(
            "会话记忆治理",
            "上下文压缩后应保留会话记忆线索并支持主动召回。",
            0.0,
            0.9,
            0,
        );
        entry.scope = MemoryScope::Project("cowd".to_string());

        let decision =
            memory_entry_turn_relevance(&entry, "分析上下文记忆召回为什么会混入无关内容", None);

        assert!(decision.accepted, "{decision:?}");
    }

    #[test]
    fn l0_and_explicit_always_policy_survive_topic_filter() {
        let mut identity = memory_entry("Identity", "Always answer as Cowd.", 0.0, 1.0, 0);
        identity.layer = MemoryLayer::L0;
        identity.scope = MemoryScope::Global;
        assert!(
            memory_entry_turn_relevance(&identity, "unrelated manufacturing question", None)
                .accepted
        );

        let mut policy = memory_entry("Language", "Always use Chinese.", 0.0, 1.0, 0);
        policy.tags.push("memory-policy:always".to_string());
        assert!(
            memory_entry_turn_relevance(&policy, "unrelated manufacturing question", None).accepted
        );
    }

    #[test]
    fn inferred_private_legacy_global_memory_is_not_visible() {
        let mut entry = memory_entry("Old preference", "Topic-specific preference", 0.0, 0.9, 0);
        entry.layer = MemoryLayer::L1;
        entry.category = MemoryCategory::UserPreference;
        entry.scope = MemoryScope::Global;
        entry.visibility = AgentVisibility::Private;
        entry.source = MemorySource::AutoExtracted;
        let context = MemoryTurnContext::new("session-a", "agent-a");

        assert!(!memory_entry_visible_to_ctx(&entry, &context));
    }

    #[test]
    fn inferred_l1_preference_defaults_to_turn_scope_not_global() {
        let mut entry = memory_entry("Preference", "Use a local formatting rule.", 0.0, 0.9, 0);
        entry.layer = MemoryLayer::L1;
        entry.category = MemoryCategory::UserPreference;
        entry.scope = MemoryScope::default();
        entry.visibility = AgentVisibility::Private;
        entry.source = MemorySource::AutoExtracted;
        let context =
            MemoryTurnContext::new("session-a", "agent-a").with_task_id(Some("task-a".to_string()));

        assert_eq!(
            scoped_entry_scope(&context, &entry),
            MemoryScope::Task("task-a".to_string())
        );
    }

    #[test]
    fn default_scope_never_persists_the_empty_session_sentinel() {
        let context = MemoryTurnContext::new("session-a", "primary");
        let empty_scope_entry = MemoryEntry {
            scope: MemoryScope::default(),
            ..memory_entry("Scoped", "must stay with its session", 0.0, 1.0, 0)
        };
        assert_eq!(
            scoped_entry_scope(&context, &empty_scope_entry),
            MemoryScope::Session("session-a".to_string())
        );
    }

    #[test]
    fn automatic_memory_scope_matches_its_durability_semantics() {
        let context = MemoryTurnContext::new("session-a", "primary")
            .with_project_id(Some("project-a".to_string()))
            .with_task_id(Some("task-a".to_string()))
            .with_team_id(Some("team-a".to_string()));

        let preference = MemoryEntry {
            layer: MemoryLayer::L1,
            category: MemoryCategory::UserPreference,
            scope: MemoryScope::default(),
            ..memory_entry("Preference", "Always report evidence first", 0.0, 1.0, 0)
        };
        assert_eq!(
            scoped_entry_scope(&context, &preference),
            MemoryScope::Task("task-a".to_string())
        );
        // An already-governed shared preference may still carry an explicit
        // Global scope; inference alone no longer grants that authority.
        let resolved_preference = MemoryEntry {
            scope: MemoryScope::Global,
            ..preference.clone()
        };
        assert_eq!(
            scoped_entry_scope(&context, &resolved_preference),
            MemoryScope::Global
        );

        let project_decision = MemoryEntry {
            layer: MemoryLayer::L2,
            category: MemoryCategory::Decision,
            scope: MemoryScope::default(),
            ..memory_entry("Decision", "Gateway owns lifecycle", 0.0, 1.0, 0)
        };
        assert_eq!(
            scoped_entry_scope(&context, &project_decision),
            MemoryScope::Project("project-a".to_string())
        );

        let checkpoint = MemoryEntry {
            layer: MemoryLayer::L3,
            category: MemoryCategory::CompressedSummary,
            scope: MemoryScope::default(),
            tags: vec!["semantic-checkpoint".to_string()],
            ..memory_entry("Checkpoint", "Current session state", 0.0, 1.0, 0)
        };
        assert_eq!(
            scoped_entry_scope(&context, &checkpoint),
            MemoryScope::Task("task-a".to_string())
        );

        let shared = MemoryEntry {
            layer: MemoryLayer::L3,
            category: MemoryCategory::Shared,
            scope: MemoryScope::default(),
            ..memory_entry("Shared", "Team working convention", 0.0, 1.0, 0)
        };
        assert_eq!(
            scoped_entry_scope(&context, &shared),
            MemoryScope::TeamRun("team-a".to_string())
        );
    }

    #[test]
    fn cognitive_read_lease_filters_team_project_and_global_memory() {
        use harness_contract::agent::CognitiveReadScope;

        let context = MemoryTurnContext::new("session-a", "agent-a")
            .with_project_id(Some("project-a".to_string()))
            .with_team_id(Some("team-a".to_string()))
            .with_cognitive_read_scopes(vec![CognitiveReadScope::Session]);
        assert!(memory_scope_visible_to_ctx(
            &MemoryScope::Session("session-a".to_string()),
            &context
        ));
        assert!(!memory_scope_visible_to_ctx(
            &MemoryScope::Project("project-a".to_string()),
            &context
        ));
        assert!(!memory_scope_visible_to_ctx(&MemoryScope::Global, &context));

        let expanded = context.with_cognitive_read_scopes(vec![
            CognitiveReadScope::Session,
            CognitiveReadScope::Project,
            CognitiveReadScope::Team,
            CognitiveReadScope::WorkspaceKnowledge,
        ]);
        assert!(memory_scope_visible_to_ctx(
            &MemoryScope::Project("project-a".to_string()),
            &expanded
        ));
        assert!(memory_scope_visible_to_ctx(
            &MemoryScope::TeamRun("team-a".to_string()),
            &expanded
        ));
        assert!(memory_scope_visible_to_ctx(&MemoryScope::Global, &expanded));
    }

    #[test]
    fn definition_instance_and_team_scopes_never_cross_a_binding_lease() {
        use harness_contract::agent::CognitiveReadScope;

        let context = MemoryTurnContext::new("session-a", "instance-a")
            .with_definition_lineage_id(Some("builtin/cowd/researcher".to_string()))
            .with_team_id(Some("team-a".to_string()))
            .with_cognitive_read_scopes(vec![
                CognitiveReadScope::Session,
                CognitiveReadScope::DefinitionLineage,
                CognitiveReadScope::Team,
            ]);
        assert!(memory_scope_visible_to_ctx(
            &MemoryScope::AgentDefinitionLineage("builtin/cowd/researcher".to_string()),
            &context
        ));
        assert!(!memory_scope_visible_to_ctx(
            &MemoryScope::AgentDefinitionLineage("builtin/cowd/reviewer".to_string()),
            &context
        ));
        assert!(memory_scope_visible_to_ctx(
            &MemoryScope::AgentInstance("instance-a".to_string()),
            &context
        ));
        assert!(!memory_scope_visible_to_ctx(
            &MemoryScope::AgentInstance("instance-b".to_string()),
            &context
        ));
        assert!(memory_scope_visible_to_ctx(
            &MemoryScope::TeamRun("team-a".to_string()),
            &context
        ));
        assert!(!memory_scope_visible_to_ctx(
            &MemoryScope::TeamRun("team-b".to_string()),
            &context
        ));
        assert!(!memory_scope_visible_to_ctx(
            &MemoryScope::LegacyUnresolvedAgent("researcher".to_string()),
            &context
        ));
    }

    #[tokio::test]
    async fn checkpoint_fact_projection_is_idempotent_across_replay() {
        let temp = tempfile::tempdir().expect("temporary memory root");
        let manager = Arc::new(
            CognitiveContextManager::new(crate::config::MemoryConfig {
                store: crate::config::StoreConfig {
                    sqlite_path: temp.path().join("memory.sqlite"),
                    blob_dir: temp.path().join("blobs"),
                    enable_vector_index: false,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("memory manager"),
        );
        let kernel = MemoryKernel::new(Arc::clone(&manager));
        let context = MemoryTurnContext::new("session-idempotent", "primary");
        let checkpoint = SessionSemanticCheckpoint {
            schema_version: crate::compression::session::SESSION_SEMANTIC_CHECKPOINT_SCHEMA_VERSION,
            checkpoint_id: "checkpoint-idempotent".to_string(),
            execution_identity: harness_contract::execution::ExecutionIdentity::for_session_turn(
                "primary",
                "workspace-idempotent",
                "session-idempotent",
                "turn-idempotent",
            )
            .unwrap(),
            session_id: "session-idempotent".to_string(),
            agent_id: "primary".to_string(),
            project_id: None,
            task_id: None,
            team_id: None,
            summary: "Keep the durable decision.".to_string(),
            user_rules: Vec::new(),
            goal: Some("validate replay".to_string()),
            constraints: Vec::new(),
            decisions: vec!["use one checkpoint".to_string()],
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            file_changes: Vec::new(),
            resume_cursor: SessionResumeCursor {
                message_index: 1,
                event_sequence: Some(1),
                checkpoint_id: "checkpoint-idempotent".to_string(),
            },
            token_stats: CheckpointTokenStats {
                before: 200,
                after: 40,
                message_count: 2,
            },
            source_range: CompactionSourceRange {
                session_id: "session-idempotent".to_string(),
                message_start: 0,
                message_end_exclusive: 2,
                event_start: Some(0),
                event_end_exclusive: Some(2),
                raw_refs: vec![EvidenceRef::durable("checkpoint-replay-source")],
            },
            facts: vec![SessionCheckpointFact {
                kind: CheckpointFactKind::Decision,
                title: "Checkpoint decision".to_string(),
                content: "Use a deterministic checkpoint fact identifier.".to_string(),
                category: MemoryCategory::Decision,
                layer: MemoryLayer::L2,
                tags: vec!["semantic-checkpoint".to_string()],
                confidence: 0.9,
                evidence_refs: vec![EvidenceRef::durable("checkpoint-replay-source")],
            }],
        };

        let first = kernel
            .checkpoint_compaction(&context, checkpoint.clone())
            .await
            .expect("first projection");
        let entries_after_first = manager.list_all_entries().await.expect("entries");
        assert!(entries_after_first
            .iter()
            .any(|entry| entry.id == checkpoint_memory_id(&checkpoint.checkpoint_id, 0)));

        let second = kernel
            .checkpoint_compaction(&context, checkpoint.clone())
            .await
            .expect("replayed projection");
        let entries_after_replay = manager.list_all_entries().await.expect("entries");

        assert_eq!(entries_after_replay.len(), entries_after_first.len());
        assert!(first
            .memory_ids
            .iter()
            .all(|id| entries_after_replay.iter().any(|entry| entry.id == *id)));
        assert!(second.memory_ids.is_empty() || second.memory_ids == first.memory_ids);
    }
}
