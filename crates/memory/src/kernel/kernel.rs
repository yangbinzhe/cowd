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

use crate::{
    cognitive::CognitiveContextManager,
    compression::session::{SessionCheckpointFact, SessionSemanticCheckpoint},
    error::MemoryError,
    memory_authority::{
        authority_decision, same_memory_key, MemoryAuthorityAction, MemoryAuthorityDecision,
    },
    memory_cluster::{cluster_entries, MemoryCluster},
    memory_usage::{summarize_usage, MemoryUsageSignal, MemoryUsageSummary},
    project_scope::MemoryScope,
    types::{
        AgentVisibility, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Message,
        PreparedContext, Priority, TokenBudget,
    },
};

use reality_recall::{RecallCandidate, RecallOmission, RecallReport, RecallSourceResult};

/// Result alias for the MemoryKernel boundary.
pub type MemoryKernelResult<T> = std::result::Result<T, MemoryKernelError>;

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
        let state = if entry.staleness >= 1.0 {
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
    pub team_id: Option<String>,
    pub task_id: Option<String>,
}

impl MemoryTurnContext {
    #[must_use]
    pub fn new(session_id: impl Into<String>, agent_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            project_id: None,
            agent_id: agent_id.into(),
            team_id: None,
            task_id: None,
        }
    }

    #[must_use]
    pub fn with_project_id(mut self, project_id: Option<String>) -> Self {
        self.project_id = project_id;
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
        self.manager.set_active_session(ctx.session_id.clone());
        self.manager.set_active_agent(ctx.agent_id.clone());

        match self
            .manager
            .prepare_context(query, messages, Some(&ctx.session_id))
            .await
        {
            Ok(mut prepared) => {
                prepared.entries = self.filter_active_entries(prepared.entries).await;
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
        self.manager.set_active_session(ctx.session_id.clone());
        self.manager.set_active_agent(ctx.agent_id.clone());

        if let Err(error) = self.manager.on_turn_end(messages).await {
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
        self.manager.set_active_session(ctx.session_id.clone());
        self.manager.set_active_agent(ctx.agent_id.clone());

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
        if let Err(error) = self.manager.remember(entry).await {
            tracing::warn!(
                session_id = %ctx.session_id,
                agent_id = %ctx.agent_id,
                %error,
                "memory kernel remember degraded"
            );
        } else {
            if let Some((existing_id, decision)) = authority_match {
                match decision.action {
                    MemoryAuthorityAction::SupersedeExisting => {
                        self.record_lifecycle_event(
                            ctx,
                            existing_id,
                            self.latest_state(existing_id).await.unwrap_or(None),
                            MemoryState::Superseded,
                            format!("superseded by {memory_id}: {}", decision.reason),
                        )
                        .await;
                    }
                    MemoryAuthorityAction::MarkConflict => {
                        self.record_lifecycle_event(
                            ctx,
                            existing_id,
                            self.latest_state(existing_id).await.unwrap_or(None),
                            MemoryState::Conflicted,
                            format!("conflicts with {memory_id}: {}", decision.reason),
                        )
                        .await;
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
            .await;
        }
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
            .map(|(index, fact)| (checkpoint.fact_candidate_id_key(index), fact.clone()))
            .collect::<HashMap<_, _>>();
        let mut fact_service = FactKernelService::with_store(InMemoryFactStore::new());
        for entry in self.manager.list_all_entries().await? {
            fact_service.upsert_fact(entry.to_fact_record());
        }
        let fact_review = fact_service.review_candidates(checkpoint.to_fact_extraction_batch());

        let mut ids = Vec::with_capacity(fact_review.promoted.len());
        for decision in &fact_review.promoted {
            let Some(fact) = candidate_to_fact.get(decision.candidate.candidate_id.as_str()) else {
                continue;
            };
            let id = self.remember_checkpoint_fact(ctx, fact.clone()).await?;
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
                    let label = evidence.0.label.as_deref().unwrap_or("source");
                    format!("- {}:{} ({label})", evidence.0.ref_type, evidence.0.id)
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
            id: uuid::Uuid::new_v4(),
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
        let from = self.latest_state(memory_id).await.unwrap_or(None);
        self.record_lifecycle_event(ctx, memory_id, from, to, reason)
            .await;
        Ok(())
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
        let mut active = Vec::with_capacity(entries.len());
        for entry in entries {
            let state = self.latest_state(entry.id).await.ok().flatten();
            if !matches!(state, Some(MemoryState::Superseded | MemoryState::Archived)) {
                active.push(entry);
            }
        }
        active
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
                for (entry, score) in vector_entries {
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
        let aaak_index = crate::aaak_index::AaakIndex::from_entries(
            &prepared.entries,
            (max_tokens / 8).clamp(128, 2_048),
        );
        source_results.push(RecallSourceResult {
            source: RecallSourceKind::CompactNavigation,
            status: if aaak_index.slots.is_empty() {
                "degraded"
            } else {
                "enabled_and_wired"
            }
            .to_string(),
            selected_count: aaak_index.slots.len(),
            omitted_count: 0,
            degraded_reason: aaak_index
                .slots
                .is_empty()
                .then(|| "AAAK compact index has no slots for this packet".to_string()),
        });
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
        let Ok(entries) = self.manager.list_all_entries().await else {
            return (Vec::new(), Vec::new());
        };
        let mut omitted = Vec::new();
        let mut scored = entries
            .into_iter()
            .filter(|entry| {
                entry.tags.iter().any(|tag| tag == "semantic-checkpoint")
                    && memory_scope_visible_to_ctx(&entry.scope, ctx)
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
        let raw = self
            .manager
            .kernel_kv_get(MEMORY_USAGE_KEY)
            .await?
            .unwrap_or_else(|| "[]".to_string());
        let signals: Vec<MemoryUsageSignal> = serde_json::from_str(&raw).unwrap_or_default();
        Ok(summarize_usage(&signals, 3))
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
        let entries = match self.manager.list_all_entries().await {
            Ok(entries) => entries,
            Err(error) => {
                return Ok(MemoryHealth {
                    degraded: vec![MemoryDegradation::PrepareFailed(error.to_string())],
                    background_lag_ms: Some(started.elapsed().as_millis() as u64),
                    ..MemoryHealth::default()
                });
            }
        };
        Ok(health_from_entries(
            &entries,
            Some(started.elapsed().as_millis() as u64),
        ))
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
        let mut atoms = Vec::new();
        for entry in entries {
            atoms.push(
                self.atom_with_lifecycle_state(entry, information_state)
                    .await,
            );
        }
        atoms
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
    ) {
        let mut events = self
            .load_lifecycle_events(memory_id)
            .await
            .unwrap_or_default();
        events.push(MemoryLifecycleEvent {
            memory_id,
            from,
            to,
            reason: reason.into(),
            session_id: ctx.session_id.clone(),
            agent_id: ctx.agent_id.clone(),
            occurred_at: Utc::now(),
        });

        match serde_json::to_string(&events) {
            Ok(raw) => {
                if let Err(error) = self
                    .manager
                    .kernel_kv_put(&lifecycle_key(memory_id), &raw)
                    .await
                {
                    tracing::warn!(%memory_id, %error, "memory lifecycle persist degraded");
                }
            }
            Err(error) => {
                tracing::warn!(%memory_id, %error, "memory lifecycle serialize failed");
            }
        }
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
        let entries = self.manager.list_all_entries().await?;
        let mut best: Option<(MemoryId, MemoryAuthorityDecision)> = None;
        for existing in entries {
            if existing.id == incoming.id || same_memory_key(&existing) != incoming_key {
                continue;
            }
            if matches!(
                self.latest_state(existing.id).await.ok().flatten(),
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
        let raw = self
            .manager
            .kernel_kv_get(MEMORY_USAGE_KEY)
            .await?
            .unwrap_or_else(|| "[]".to_string());
        let mut signals: Vec<MemoryUsageSignal> = serde_json::from_str(&raw).unwrap_or_default();
        for item in &packet.selected {
            signals.push(MemoryUsageSignal {
                memory_id: item.atom.id,
                session_id: ctx.session_id.clone(),
                agent_id: ctx.agent_id.clone(),
                selected_count: 1,
                last_reason: item.reason.clone(),
            });
            if matches!(item.atom.state, MemoryState::Active | MemoryState::Observed) {
                self.record_lifecycle_event(
                    ctx,
                    item.atom.id,
                    Some(item.atom.state),
                    MemoryState::Validated,
                    "validated by context selection",
                )
                .await;
            }
        }
        if signals.len() > 2_000 {
            let start = signals.len() - 2_000;
            signals.drain(0..start);
        }
        if let Ok(raw) = serde_json::to_string(&signals) {
            if let Err(error) = self.manager.kernel_kv_put(MEMORY_USAGE_KEY, &raw).await {
                tracing::warn!(%error, "memory usage persist degraded");
            }
        }
        Ok(())
    }
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

const MEMORY_USAGE_KEY: &str = "memory_usage:context_selection";

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

fn scoped_entry_scope(ctx: &MemoryTurnContext, entry: &MemoryEntry) -> MemoryScope {
    match &entry.scope {
        MemoryScope::Global => {
            if matches!(entry.visibility, AgentVisibility::Private) {
                return MemoryScope::Agent(ctx.agent_id.clone());
            }
            if let Some(team_id) = &ctx.team_id {
                return MemoryScope::Project(team_id.clone());
            }
            if let Some(project_id) = &ctx.project_id {
                return MemoryScope::Project(project_id.clone());
            }
            MemoryScope::Session(ctx.session_id.clone())
        }
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

fn memory_scope_visible_to_ctx(scope: &MemoryScope, ctx: &MemoryTurnContext) -> bool {
    match scope {
        MemoryScope::Global => true,
        MemoryScope::Session(session_id) => session_id == &ctx.session_id,
        MemoryScope::Project(project_id) => {
            ctx.project_id.as_ref() == Some(project_id) || ctx.team_id.as_ref() == Some(project_id)
        }
        MemoryScope::Task(task_id) => ctx.task_id.as_ref() == Some(task_id),
        MemoryScope::Agent(agent_id) => agent_id == &ctx.agent_id,
    }
}

fn checkpoint_recall_score(entry: &MemoryEntry, ctx: &MemoryTurnContext, query: &str) -> f32 {
    let scope_score = match &entry.scope {
        MemoryScope::Task(task_id) if ctx.task_id.as_ref() == Some(task_id) => 0.34,
        MemoryScope::Session(session_id) if session_id == &ctx.session_id => 0.30,
        MemoryScope::Project(project_id)
            if ctx.project_id.as_ref() == Some(project_id)
                || ctx.team_id.as_ref() == Some(project_id) =>
        {
            0.20
        }
        MemoryScope::Agent(agent_id) if agent_id == &ctx.agent_id => 0.18,
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
    let query = query.to_lowercase();
    let haystack = format!("{} {}", title.to_lowercase(), content.to_lowercase());
    if query.trim().len() > 8 && haystack.contains(query.trim()) {
        return 0.46;
    }
    let tokens = query
        .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
        .filter(|token| token.len() >= 3)
        .collect::<HashSet<_>>();
    if tokens.is_empty() {
        return 0.0;
    }
    let matched = tokens
        .iter()
        .filter(|token| haystack.contains(**token))
        .count();
    ((matched as f32 / tokens.len() as f32) * 0.42).min(0.42)
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
    base + boost
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
        .filter(|entry| entry.staleness >= 1.0)
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
        degraded: Vec::new(),
    }
}
