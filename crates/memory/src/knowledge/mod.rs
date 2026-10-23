//! Universal knowledge fabric for large corpus governance.
//!
//! This module owns unstructured corpus metadata, canon packs, activation
//! policy, and usage signals. Runtime consumes its activation result; gateway
//! exposes projections; matrix stores the structured facts derived from it.

mod query;
pub use query::{
    KnowledgeCandidateCursor, KnowledgeCandidatePage, KnowledgeCandidateScope,
    KnowledgeSourceRevision, KNOWLEDGE_CANDIDATE_PAGE_SIZE,
};

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use harness_contract::core::KernelRef;
use harness_contract::knowledge::{
    estimate_tokens, KnowledgeActivationPlan, KnowledgeActivationPolicy, KnowledgeCanonPack,
    KnowledgeCanonRule, KnowledgeComplianceWarning, KnowledgeConflictRecord, KnowledgeCorpus,
    KnowledgeGovernanceLevel, KnowledgeNamespace, KnowledgeObjectState, KnowledgePack,
    KnowledgePackKind, KnowledgeTurnReport, KnowledgeUsageSignal,
};
use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentContent {
    pub title: String,
    pub body: String,
    pub source: Option<String>,
    pub author: Option<String>,
    pub created_at: Option<String>,
    pub modified_at: Option<String>,
    pub language: Option<String>,
}

impl DocumentContent {
    #[must_use]
    pub fn new(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            body: body.into(),
            source: None,
            author: None,
            created_at: None,
            modified_at: None,
            language: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentCategory {
    Technical,
    UserGuide,
    ApiReference,
    Architecture,
    MeetingNotes,
    Task,
    Configuration,
    CodeReview,
    KnowledgeBase,
    Other,
}

impl DocumentCategory {
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Technical => "Technical Documentation",
            Self::UserGuide => "User Guide",
            Self::ApiReference => "API Reference",
            Self::Architecture => "Architecture Document",
            Self::MeetingNotes => "Meeting Notes",
            Self::Task => "Task/Issue",
            Self::Configuration => "Configuration",
            Self::CodeReview => "Code Review",
            Self::KnowledgeBase => "Knowledge Base",
            Self::Other => "Other",
        }
    }

    #[must_use]
    pub const fn layer_priority(self) -> u8 {
        match self {
            Self::Configuration => 4,
            Self::UserGuide | Self::ApiReference => 3,
            Self::Technical | Self::Architecture | Self::KnowledgeBase => 2,
            Self::MeetingNotes | Self::Task | Self::CodeReview => 1,
            Self::Other => 0,
        }
    }

    #[must_use]
    pub const fn pack_kind(self) -> KnowledgePackKind {
        match self {
            Self::Technical | Self::ApiReference | Self::Architecture | Self::Configuration => {
                KnowledgePackKind::TechnicalStandard
            }
            Self::UserGuide | Self::KnowledgeBase => KnowledgePackKind::ReferenceLibrary,
            Self::MeetingNotes | Self::Task | Self::CodeReview | Self::Other => {
                KnowledgePackKind::ReferenceLibrary
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    pub title: String,
    pub category: DocumentCategory,
    pub confidence: f32,
    pub keywords: Vec<String>,
    pub tags: Vec<String>,
    pub source: Option<String>,
    pub author: Option<String>,
    pub created_at: Option<String>,
    pub modified_at: Option<String>,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationResult {
    pub metadata: DocumentMetadata,
    pub reasoning: Vec<String>,
    pub suggested_layer: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeIngestionReceipt {
    pub corpus: KnowledgeCorpus,
    pub pack: KnowledgePack,
    pub canon: KnowledgeCanonPack,
    pub conflicts: Vec<KnowledgeConflictRecord>,
    pub chunks: Vec<KnowledgeChunk>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeSnapshot {
    pub corpus: Vec<KnowledgeCorpus>,
    pub packs: Vec<KnowledgePack>,
    pub canon: Vec<KnowledgeCanonPack>,
    pub conflicts: Vec<KnowledgeConflictRecord>,
    pub chunks: Vec<KnowledgeChunk>,
    pub usage: Vec<KnowledgeUsageSignal>,
}

impl KnowledgeSnapshot {
    #[must_use]
    pub fn health(&self) -> KnowledgeFabricHealth {
        KnowledgeFabricHealth {
            corpus_count: self.corpus.len(),
            pack_count: self.packs.len(),
            canon_count: self.canon.len(),
            conflict_count: self.conflicts.len(),
            unresolved_conflict_count: self
                .conflicts
                .iter()
                .filter(|conflict| {
                    conflict.decision.is_none()
                        && conflict.pack_id.as_deref().is_none_or(|pack_id| {
                            self.packs.iter().any(|pack| {
                                pack.pack_id == pack_id && knowledge_state_is_actionable(pack.state)
                            })
                        })
                })
                .count(),
            usage_signal_count: self.usage.len(),
            active_pack_count: self
                .packs
                .iter()
                .filter(|pack| pack.state == KnowledgeObjectState::Active)
                .count(),
            quarantined_pack_count: self
                .packs
                .iter()
                .filter(|pack| pack.state == KnowledgeObjectState::Quarantined)
                .count(),
        }
    }
}

#[derive(Debug, Error)]
pub enum KnowledgeStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage error: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("knowledge source read set changed")]
    StaleRead,
    #[error("knowledge read exceeds the bounded frame")]
    ReadLimit,
}

pub trait KnowledgeStore: Send + Sync {
    fn save_receipt(
        &self,
        receipt: &KnowledgeIngestionReceipt,
    ) -> Result<KnowledgeIngestionReceipt, KnowledgeStoreError>;
    fn record_usage(&self, signal: &KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError>;
    fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError>;
    fn maintenance_inputs(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError>;
    fn projection(&self) -> Result<serde_json::Value, KnowledgeStoreError>;
    /// Scope is applied before bounded page admission. All continuations must
    /// retain the original source revision and exact namespace/project identity.
    fn read_candidate_page(
        &self,
        scope: &KnowledgeCandidateScope,
        cursor: Option<&KnowledgeCandidateCursor>,
        limit: usize,
        budget_bytes: usize,
    ) -> Result<KnowledgeCandidatePage, KnowledgeStoreError>;
    /// Compare the original domain read set and apply every maintenance write
    /// in one transaction. A conflicting read is never silently retried.
    fn commit_maintenance(
        &self,
        expected: &KnowledgeSnapshot,
        packs: &[KnowledgePack],
        conflicts: &[KnowledgeConflictRecord],
    ) -> Result<(), KnowledgeStoreError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeChunk {
    pub chunk_id: String,
    pub corpus_id: String,
    pub ordinal: usize,
    pub title: String,
    pub text: String,
    pub evidence_ref: EvidenceRef,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeFabricHealth {
    pub corpus_count: usize,
    pub pack_count: usize,
    pub canon_count: usize,
    pub conflict_count: usize,
    pub unresolved_conflict_count: usize,
    pub usage_signal_count: usize,
    pub active_pack_count: usize,
    pub quarantined_pack_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeConsolidationReport {
    pub canonical_pack_ids: Vec<String>,
    pub superseded_pack_ids: Vec<String>,
    pub unresolved_conflict_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeNamespaceSearchResult {
    pub namespace: KnowledgeNamespace,
    pub query: String,
    pub packs: Vec<KnowledgePack>,
    pub canon: Vec<KnowledgeCanonPack>,
    pub blocked_namespaces: Vec<String>,
}

#[derive(Debug, Default)]
struct KnowledgeFabricState {
    corpus: BTreeMap<String, KnowledgeCorpus>,
    packs: BTreeMap<String, KnowledgePack>,
    canon: BTreeMap<String, KnowledgeCanonPack>,
    conflicts: BTreeMap<String, KnowledgeConflictRecord>,
    chunks: BTreeMap<String, KnowledgeChunk>,
    usage: Vec<KnowledgeUsageSignal>,
    ingestion_receipts: BTreeMap<String, (String, KnowledgeIngestionReceipt)>,
    candidate_revisions: BTreeMap<String, u64>,
    candidate_store_id: String,
}

/// A read is complete or refused; these limits never truncate a policy input.
pub const KNOWLEDGE_READ_MAX_ROWS: usize = 4096;
pub const KNOWLEDGE_READ_MAX_BYTES: usize = 8 * 1024 * 1024;

impl KnowledgeFabricState {
    fn from_snapshot(snapshot: KnowledgeSnapshot) -> Self {
        Self {
            corpus: snapshot
                .corpus
                .into_iter()
                .map(|v| (v.corpus_id.clone(), v))
                .collect(),
            packs: snapshot
                .packs
                .into_iter()
                .map(|v| (v.pack_id.clone(), v))
                .collect(),
            canon: snapshot
                .canon
                .into_iter()
                .map(|v| (v.canon_id.clone(), v))
                .collect(),
            conflicts: snapshot
                .conflicts
                .into_iter()
                .map(|v| (v.conflict_id.clone(), v))
                .collect(),
            chunks: snapshot
                .chunks
                .into_iter()
                .map(|v| (v.chunk_id.clone(), v))
                .collect(),
            usage: snapshot.usage,
            ingestion_receipts: BTreeMap::new(),
            candidate_revisions: BTreeMap::new(),
            candidate_store_id: String::new(),
        }
    }
}

struct Budget(usize);
impl std::io::Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("knowledge read budget"))?;
        if self.0 > KNOWLEDGE_READ_MAX_BYTES {
            return Err(std::io::Error::other("knowledge read budget"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn bounded_state_snapshot(
    state: &KnowledgeFabricState,
    include_history: bool,
) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
    // Count serialized bytes into a bounded sink before cloning any payload.
    let rows = state.corpus.len()
        + state.packs.len()
        + state.canon.len()
        + state.conflicts.len()
        + if include_history {
            state.chunks.len() + state.usage.len()
        } else {
            0
        };
    if rows > KNOWLEDGE_READ_MAX_ROWS {
        return Err(KnowledgeStoreError::ReadLimit);
    }
    let mut budget = Budget(0);
    macro_rules! count {
        ($items:expr) => {
            for item in $items {
                serde_json::to_writer(&mut budget, item)
                    .map_err(|_| KnowledgeStoreError::ReadLimit)?;
            }
        };
    }
    count!(state.corpus.values());
    count!(state.packs.values());
    count!(state.canon.values());
    count!(state.conflicts.values());
    if include_history {
        count!(state.chunks.values());
        count!(&state.usage);
    }
    Ok(KnowledgeSnapshot {
        corpus: state.corpus.values().cloned().collect(),
        packs: state.packs.values().cloned().collect(),
        canon: state.canon.values().cloned().collect(),
        conflicts: state.conflicts.values().cloned().collect(),
        chunks: if include_history {
            state.chunks.values().cloned().collect()
        } else {
            Vec::new()
        },
        usage: if include_history {
            state.usage.clone()
        } else {
            Vec::new()
        },
    })
}

/// Stable request identity and full typed input commitment. Generated commit
/// times are excluded explicitly; semantic fields and payloads remain bound.
/// The immutable receipt is written in the same original store transaction.
pub fn knowledge_ingestion_identity(
    receipt: &KnowledgeIngestionReceipt,
) -> Result<(String, String), KnowledgeStoreError> {
    fn digest(domain: &str, value: &impl Serialize) -> Result<String, KnowledgeStoreError> {
        let mut hash = Sha256::new();
        hash.update(domain.as_bytes());
        hash.update([0]);
        hash.update(serde_json::to_vec(value)?);
        Ok(format!("{:x}", hash.finalize()))
    }
    let key = digest(
        "cowd.knowledge.ingestion.identity.v1",
        &(&receipt.pack.pack_id, &receipt.corpus.corpus_id),
    )?;
    let mut input = receipt.clone();
    let epoch = DateTime::<Utc>::UNIX_EPOCH;
    input.corpus.created_at = epoch;
    input.corpus.updated_at = epoch;
    input.pack.created_at = epoch;
    input.pack.updated_at = epoch;
    input.canon.updated_at = epoch;
    for conflict in &mut input.conflicts {
        conflict.detected_at = epoch;
    }
    let commitment = digest("cowd.knowledge.ingestion.input.v1", &input)?;
    Ok((key, commitment))
}

/// Only the original maintenance inputs participate; usage telemetry cannot
/// invalidate an unrelated pack maintenance transaction.
pub fn same_knowledge_maintenance_inputs(
    a: &KnowledgeSnapshot,
    b: &KnowledgeSnapshot,
) -> Result<bool, KnowledgeStoreError> {
    Ok(serde_json::to_value((&a.corpus, &a.packs, &a.conflicts))?
        == serde_json::to_value((&b.corpus, &b.packs, &b.conflicts))?)
}

#[derive(Debug, Clone)]
pub struct KnowledgeTurnProjection {
    receipt: KnowledgeIngestionReceipt,
}

#[derive(Clone)]
pub struct KnowledgeFabric {
    store: Arc<dyn KnowledgeStore>,
}

impl Default for KnowledgeFabric {
    fn default() -> Self {
        Self::with_store(Arc::new(InMemoryKnowledgeStore::new()))
    }
}

impl KnowledgeFabric {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The original store is the sole authority. Construction neither loads a
    /// mirror nor turns a failed read into an empty, apparently healthy fabric.
    #[must_use]
    pub fn with_store(store: Arc<dyn KnowledgeStore>) -> Self {
        Self { store }
    }

    pub fn ingest_document(
        &self,
        namespace: KnowledgeNamespace,
        activation_policy: KnowledgeActivationPolicy,
        governance_level: KnowledgeGovernanceLevel,
        content: DocumentContent,
    ) -> Result<KnowledgeIngestionReceipt, KnowledgeStoreError> {
        let input_bytes = content
            .body
            .len()
            .saturating_add(content.title.len())
            .saturating_add(content.source.as_ref().map_or(0, String::len));
        if input_bytes > KNOWLEDGE_READ_MAX_BYTES {
            return Err(KnowledgeStoreError::ReadLimit);
        }
        let receipt = KnowledgeIngestionService::new().prepare_document(
            namespace,
            activation_policy,
            governance_level,
            content,
            false,
        );
        self.store.save_receipt(&receipt)
    }

    #[must_use]
    pub fn activate(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        project_id: Option<&str>,
    ) -> Result<
        (
            KnowledgeActivationPlan,
            Vec<KnowledgeCanonPack>,
            Vec<KnowledgeComplianceWarning>,
        ),
        KnowledgeStoreError,
    > {
        ActivationGovernor::new().activate(self, session_id, intent, profile, project_id)
    }

    #[must_use]
    pub fn search_namespace(
        &self,
        namespace: &KnowledgeNamespace,
        query: &str,
    ) -> Result<KnowledgeNamespaceSearchResult, KnowledgeStoreError> {
        let (packs, canon) =
            self.read_complete_candidates(KnowledgeCandidateScope::Namespace(namespace.clone()))?;
        let state = KnowledgeFabricState {
            packs: packs
                .into_iter()
                .map(|pack| (pack.pack_id.clone(), pack))
                .collect(),
            canon: canon
                .into_iter()
                .map(|canon| (canon.canon_id.clone(), canon))
                .collect(),
            ..KnowledgeFabricState::default()
        };
        let query_lc = query.to_ascii_lowercase();
        let mut packs = Vec::new();
        let mut canon = Vec::new();
        let mut blocked_namespaces = Vec::new();
        for pack in state.packs.values() {
            if &pack.namespace != namespace {
                blocked_namespaces.push(format!(
                    "{} outside requested namespace",
                    pack.namespace.key()
                ));
                continue;
            }
            let haystack = format!(
                "{} {}",
                pack.name.to_ascii_lowercase(),
                pack.namespace.key()
            );
            if query_lc
                .split_whitespace()
                .any(|term| term.len() >= 3 && haystack.contains(term))
                || query_lc.trim().is_empty()
            {
                packs.push(pack.clone());
                if let Some(canon_pack) = pack
                    .canon_pack_ref
                    .as_ref()
                    .and_then(|id| state.canon.get(id))
                {
                    canon.push(canon_pack.clone());
                }
            }
        }
        Ok(KnowledgeNamespaceSearchResult {
            namespace: namespace.clone(),
            query: query.to_string(),
            packs,
            canon,
            blocked_namespaces,
        })
    }

    fn read_complete_candidates(
        &self,
        scope: KnowledgeCandidateScope,
    ) -> Result<(Vec<KnowledgePack>, Vec<KnowledgeCanonPack>), KnowledgeStoreError> {
        let mut cursor = None;
        let mut packs = Vec::new();
        let mut canon = BTreeMap::new();
        let mut remaining = KNOWLEDGE_READ_MAX_BYTES;
        let mut rows = 0usize;
        loop {
            let page = self.store.read_candidate_page(
                &scope,
                cursor.as_ref(),
                KNOWLEDGE_CANDIDATE_PAGE_SIZE,
                remaining,
            )?;
            if let Some(previous) = &cursor {
                previous.validate(&scope, &page.revision)?;
            }
            rows = rows
                .checked_add(page.packs.len() + page.canon.len())
                .ok_or(KnowledgeStoreError::ReadLimit)?;
            if rows > KNOWLEDGE_READ_MAX_ROWS {
                return Err(KnowledgeStoreError::ReadLimit);
            }
            remaining = remaining
                .checked_sub(page.materialized_bytes)
                .ok_or(KnowledgeStoreError::ReadLimit)?;
            if page.next.is_some() && page.packs.is_empty() {
                return Err(KnowledgeStoreError::Backend(
                    "Knowledge candidate page made no progress".into(),
                ));
            }
            for pack in &page.packs {
                if let Some(reference) = &pack.canon_pack_ref {
                    if !page
                        .canon
                        .iter()
                        .any(|item| &item.canon_id == reference && item.pack_id == pack.pack_id)
                    {
                        return Err(KnowledgeStoreError::Backend(
                            "Knowledge candidate canonical identity mismatch".into(),
                        ));
                    }
                }
            }
            for item in page.canon {
                canon.insert(item.canon_id.clone(), item);
            }
            packs.extend(page.packs);
            match page.next {
                None => return Ok((packs, canon.into_values().collect())),
                Some(next) => {
                    next.validate(&scope, &page.revision)?;
                    if cursor
                        .as_ref()
                        .is_some_and(|previous| next.after_pack_id <= previous.after_pack_id)
                    {
                        return Err(KnowledgeStoreError::StaleRead);
                    }
                    cursor = Some(next);
                }
            }
        }
    }

    pub fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
        self.store.snapshot()
    }

    fn activate_inner(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        project_id: Option<&str>,
    ) -> Result<
        (
            KnowledgeActivationPlan,
            Vec<KnowledgeCanonPack>,
            Vec<KnowledgeComplianceWarning>,
        ),
        KnowledgeStoreError,
    > {
        self.activate_with_turn_projection(session_id, intent, profile, project_id, &[])
    }

    /// Evaluate persisted knowledge and ephemeral turn views through the same
    /// original activation policy. This never adopts or writes a turn view.
    #[must_use]
    pub fn activate_with_turn_projection(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        project_id: Option<&str>,
        projected: &[KnowledgeTurnProjection],
    ) -> Result<
        (
            KnowledgeActivationPlan,
            Vec<KnowledgeCanonPack>,
            Vec<KnowledgeComplianceWarning>,
        ),
        KnowledgeStoreError,
    > {
        let (packs, canon) =
            self.read_complete_candidates(KnowledgeCandidateScope::Activation {
                project_id: project_id.map(str::to_owned),
            })?;
        let state = KnowledgeFabricState {
            packs: packs
                .into_iter()
                .map(|pack| (pack.pack_id.clone(), pack))
                .collect(),
            canon: canon
                .into_iter()
                .map(|canon| (canon.canon_id.clone(), canon))
                .collect(),
            ..KnowledgeFabricState::default()
        };
        let turn = projected
            .iter()
            .map(|view| {
                let item = &view.receipt;
                (item.pack.pack_id.as_str(), (&item.pack, Some(&item.canon)))
            })
            .collect::<BTreeMap<_, _>>();
        let candidates = state
            .packs
            .values()
            .map(|pack| {
                (
                    pack,
                    pack.canon_pack_ref
                        .as_ref()
                        .and_then(|id| state.canon.get(id)),
                )
            })
            .chain(turn.values().copied());
        let intent_lc = intent.to_ascii_lowercase();
        let mut selected_namespaces = Vec::new();
        let mut blocked_namespaces = Vec::new();
        let mut active_pack_ids = Vec::new();
        let mut canon_refs = Vec::new();
        let mut evidence_refs = Vec::new();
        let mut reasons = Vec::new();
        let mut canon_packs = Vec::new();
        let mut warnings = Vec::new();

        for (pack, canon) in candidates {
            if !matches!(
                pack.state,
                KnowledgeObjectState::Active | KnowledgeObjectState::Canonized
            ) {
                blocked_namespaces.push(format!(
                    "{} blocked because state={:?}",
                    pack.namespace.key(),
                    pack.state
                ));
                continue;
            }
            let fits = activation_fits(pack, &intent_lc, project_id);
            if !fits {
                blocked_namespaces.push(format!("{} not relevant to intent", pack.namespace.key()));
                continue;
            }
            selected_namespaces.push(pack.namespace.clone());
            active_pack_ids.push(pack.pack_id.clone());
            evidence_refs.extend(pack.evidence_refs.clone());
            reasons.push(format!(
                "pack {} selected by {:?}",
                pack.pack_id, pack.activation_policy
            ));
            if let Some(canon_id) = pack.canon_pack_ref.as_ref() {
                canon_refs.push(canon_id.clone());
                if let Some(canon) = canon {
                    for rule in &canon.rules {
                        if matches!(
                            rule.governance_level,
                            KnowledgeGovernanceLevel::Required | KnowledgeGovernanceLevel::Blocking
                        ) {
                            warnings.push(KnowledgeComplianceWarning {
                                warning_id: format!(
                                    "knowledge-warning-{}",
                                    stable_hash(&format!("{}:{}", pack.pack_id, rule.rule_id))
                                ),
                                pack_id: pack.pack_id.clone(),
                                rule_id: Some(rule.rule_id.clone()),
                                level: rule.governance_level,
                                summary: format!("active knowledge rule: {}", rule.summary),
                                evidence_refs: rule.evidence_refs.clone(),
                            });
                        }
                    }
                    canon_packs.push(canon.clone());
                }
            }
        }

        let token_estimate = canon_packs
            .iter()
            .map(|canon| canon.token_estimate)
            .sum::<u64>();
        let plan = KnowledgeActivationPlan {
            plan_id: format!(
                "knowledge-plan-{}",
                stable_hash(&format!("{session_id}:{intent}:{profile}"))
            ),
            session_id: session_id.to_string(),
            intent: intent.to_string(),
            profile: profile.to_string(),
            selected_namespaces,
            blocked_namespaces,
            active_pack_ids,
            canon_refs,
            evidence_refs,
            reasons,
            token_estimate,
            generated_at: Utc::now(),
        };
        Ok((plan, canon_packs, warnings))
    }

    pub fn record_usage(&self, signal: KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError> {
        UsageFeedbackLoop::new().record(self.store.as_ref(), &signal)
    }

    /// Quarantine every active knowledge pack derived from one source.
    ///
    /// Memory lifecycle is authoritative for memory-derived documents. When a
    /// memory is archived or superseded, its materialized knowledge pack must
    /// stop participating in activation and Matrix projection while its
    /// corpus/canon evidence remains available for audit.
    pub fn quarantine_source(&self, source_ref: &str) -> Result<Vec<String>, KnowledgeStoreError> {
        let expected = self.store.maintenance_inputs()?;
        let mut state = KnowledgeFabricState::from_snapshot(expected.clone());
        let quarantined = {
            let corpus_ids = state
                .corpus
                .values()
                .filter(|corpus| corpus.source_ref.id == source_ref)
                .map(|corpus| corpus.corpus_id.clone())
                .collect::<BTreeSet<_>>();
            if corpus_ids.is_empty() {
                return Ok(Vec::new());
            }
            let now = Utc::now();
            state
                .packs
                .values_mut()
                .filter(|pack| {
                    pack.source_corpus_refs
                        .iter()
                        .any(|corpus_id| corpus_ids.contains(corpus_id))
                        && pack.state != KnowledgeObjectState::Quarantined
                })
                .map(|pack| {
                    pack.state = KnowledgeObjectState::Quarantined;
                    pack.health_score_bp = 0;
                    pack.updated_at = now;
                    pack.clone()
                })
                .collect::<Vec<_>>()
        };
        let resolved = retire_conflicts_in_state(&mut state);
        self.store
            .commit_maintenance(&expected, &quarantined, &resolved)?;
        Ok(quarantined.into_iter().map(|pack| pack.pack_id).collect())
    }

    /// Close conflict reviews whose source pack can no longer participate.
    ///
    /// Quarantine, supersession, deprecation, and archival are already
    /// authoritative lifecycle decisions. Keeping their conflicts in the
    /// human queue creates permanent duplicate work without changing runtime
    /// behavior, so the decision is persisted while all source evidence stays
    /// intact.
    pub fn retire_inactive_conflicts(&self) -> Result<Vec<String>, KnowledgeStoreError> {
        let expected = self.store.maintenance_inputs()?;
        let mut state = KnowledgeFabricState::from_snapshot(expected.clone());
        let resolved = retire_conflicts_in_state(&mut state);
        self.store.commit_maintenance(&expected, &[], &resolved)?;
        Ok(resolved
            .into_iter()
            .map(|conflict| conflict.conflict_id)
            .collect())
    }

    /// Consolidate exact-content duplicates inside one namespace.
    ///
    /// This operation never deletes corpus, canon, or evidence. One active pack
    /// retains the union of every durable reference while redundant active
    /// packs become `Superseded`. Semantic or contradictory content is not
    /// guessed here and remains visible through the conflict review queue.
    pub fn consolidate_exact_duplicates(
        &self,
    ) -> Result<KnowledgeConsolidationReport, KnowledgeStoreError> {
        let expected = self.store.maintenance_inputs()?;
        let mut state = KnowledgeFabricState::from_snapshot(expected.clone());
        let (mut report, changed_packs) = {
            let mut corpus_groups = BTreeMap::<String, BTreeSet<String>>::new();
            for corpus in state.corpus.values() {
                corpus_groups
                    .entry(format!(
                        "{}:{}",
                        normalize_namespace_key(&corpus.namespace),
                        corpus.source_hash
                    ))
                    .or_default()
                    .insert(corpus.corpus_id.clone());
            }

            let mut report = KnowledgeConsolidationReport {
                unresolved_conflict_ids: state
                    .conflicts
                    .values()
                    .filter(|conflict| conflict_is_actionable(&state, conflict))
                    .map(|conflict| conflict.conflict_id.clone())
                    .collect(),
                ..KnowledgeConsolidationReport::default()
            };
            let mut changed = BTreeMap::<String, KnowledgePack>::new();
            for corpus_ids in corpus_groups.values().filter(|ids| ids.len() > 1) {
                let mut pack_ids = state
                    .packs
                    .values()
                    .filter(|pack| {
                        matches!(
                            pack.state,
                            KnowledgeObjectState::Active | KnowledgeObjectState::Canonized
                        ) && pack
                            .source_corpus_refs
                            .iter()
                            .any(|corpus_id| corpus_ids.contains(corpus_id))
                    })
                    .map(|pack| pack.pack_id.clone())
                    .collect::<Vec<_>>();
                pack_ids.sort();
                pack_ids.dedup();
                if pack_ids.len() < 2 {
                    continue;
                }
                let Some(canonical_id) = pack_ids
                    .iter()
                    .filter_map(|id| state.packs.get(id))
                    .max_by_key(|pack| knowledge_pack_rank(pack))
                    .map(|pack| pack.pack_id.clone())
                else {
                    continue;
                };

                let source_refs = pack_ids
                    .iter()
                    .filter_map(|id| state.packs.get(id))
                    .flat_map(|pack| pack.source_corpus_refs.iter().cloned())
                    .collect::<BTreeSet<_>>();
                let evidence_refs = dedup_serialized_refs(
                    pack_ids
                        .iter()
                        .filter_map(|id| state.packs.get(id))
                        .flat_map(|pack| pack.evidence_refs.iter().cloned()),
                )?;
                let matrix_refs = dedup_serialized_refs(
                    pack_ids
                        .iter()
                        .filter_map(|id| state.packs.get(id))
                        .flat_map(|pack| pack.matrix_refs.iter().cloned()),
                )?;
                let memory_refs = dedup_serialized_refs(
                    pack_ids
                        .iter()
                        .filter_map(|id| state.packs.get(id))
                        .flat_map(|pack| pack.memory_refs.iter().cloned()),
                )?;
                let now = Utc::now();
                if let Some(canonical) = state.packs.get_mut(&canonical_id) {
                    canonical.source_corpus_refs = source_refs.into_iter().collect();
                    canonical.evidence_refs = evidence_refs;
                    canonical.matrix_refs = matrix_refs;
                    canonical.memory_refs = memory_refs;
                    canonical.version = canonical
                        .version
                        .parse::<u64>()
                        .unwrap_or(1)
                        .saturating_add(1)
                        .to_string();
                    canonical.updated_at = now;
                    changed.insert(canonical_id.clone(), canonical.clone());
                }
                report.canonical_pack_ids.push(canonical_id.clone());
                for pack_id in pack_ids.into_iter().filter(|id| id != &canonical_id) {
                    if let Some(pack) = state.packs.get_mut(&pack_id) {
                        pack.state = KnowledgeObjectState::Superseded;
                        pack.updated_at = now;
                        changed.insert(pack_id.clone(), pack.clone());
                        report.superseded_pack_ids.push(pack_id);
                    }
                }
            }
            (report, changed.into_values().collect::<Vec<_>>())
        };

        let resolved = retire_conflicts_in_state(&mut state);
        self.store
            .commit_maintenance(&expected, &changed_packs, &resolved)?;
        report.canonical_pack_ids.sort();
        report.canonical_pack_ids.dedup();
        report.superseded_pack_ids.sort();
        report.superseded_pack_ids.dedup();
        Ok(report)
    }

    #[must_use]
    pub fn turn_report(
        &self,
        plan: &KnowledgeActivationPlan,
        warnings: Vec<KnowledgeComplianceWarning>,
    ) -> KnowledgeTurnReport {
        KnowledgeTurnReport {
            activation_plan_id: Some(plan.plan_id.clone()),
            active_pack_ids: plan.active_pack_ids.clone(),
            blocked_namespaces: plan.blocked_namespaces.clone(),
            compliance_warnings: warnings,
            evidence_refs: plan.evidence_refs.clone(),
            usage_signals: Vec::new(),
        }
    }

    #[must_use]
    pub fn projection(&self) -> Result<serde_json::Value, KnowledgeStoreError> {
        self.store.projection()
    }
}

/// Render the original projection from bounded metadata and a native usage
/// aggregate. No chunk or usage history is exported to answer online health.
pub fn knowledge_projection_from_metadata(
    snapshot: KnowledgeSnapshot,
    usage_signal_count: usize,
) -> Result<serde_json::Value, KnowledgeStoreError> {
    let state = KnowledgeFabricState::from_snapshot(snapshot);
    let mut health = health_from_state(&state);
    health.usage_signal_count = usage_signal_count;
    Ok(serde_json::json!({
        "kind": "memory.knowledge_fabric",
        "health": health,
        "namespace_tree": knowledge_namespace_tree(&state),
        "activation_policy_distribution": knowledge_activation_policy_distribution(&state),
        "governance_distribution": knowledge_governance_distribution(&state),
        "conflict_projection": knowledge_conflict_projection(&state),
        "maintenance_candidates": knowledge_maintenance_candidates(&state),
        "recall_quality": knowledge_recall_quality_projection(&state),
        "corpus": state.corpus.values().collect::<Vec<_>>(),
        "packs": state.packs.values().collect::<Vec<_>>(),
        "canon": state.canon.values().collect::<Vec<_>>(),
        "conflicts": state.conflicts.values().collect::<Vec<_>>(),
        "usage_signal_count": usage_signal_count,
    }))
}
#[derive(Debug, Clone, Default)]
pub struct KnowledgeIngestionService {
    classifier: DocumentClassifier,
}

impl KnowledgeIngestionService {
    #[must_use]
    pub fn new() -> Self {
        Self {
            classifier: DocumentClassifier::new(),
        }
    }

    #[must_use]
    pub fn classify(&self, content: &DocumentContent) -> ClassificationResult {
        self.classifier.classify(content)
    }

    /// Pure conversion of an already selected Memory item into this turn's
    /// view. The returned carrier is not a durable adoption receipt.
    #[must_use]
    pub fn project_for_turn(
        &self,
        namespace: KnowledgeNamespace,
        activation_policy: KnowledgeActivationPolicy,
        governance_level: KnowledgeGovernanceLevel,
        content: DocumentContent,
    ) -> KnowledgeTurnProjection {
        KnowledgeTurnProjection {
            receipt: self.prepare_document(
                namespace,
                activation_policy,
                governance_level,
                content,
                true,
            ),
        }
    }

    fn prepare_document(
        &self,
        namespace: KnowledgeNamespace,
        activation_policy: KnowledgeActivationPolicy,
        governance_level: KnowledgeGovernanceLevel,
        content: DocumentContent,
        for_turn: bool,
    ) -> KnowledgeIngestionReceipt {
        let classification = self.classify(&content);
        let now = Utc::now();
        let corpus_id = format!(
            "corpus-{}",
            stable_hash(&format!(
                "{}:{}:{}",
                namespace.key(),
                content.source.clone().unwrap_or_default(),
                content.body
            ))
        );
        let source_ref = KernelRef::new(
            "knowledge_source",
            content
                .source
                .clone()
                .unwrap_or_else(|| format!("inline:{}", content.title)),
        )
        .with_label(content.title.clone());
        let chunks = chunk_document(&corpus_id, &content);
        let corpus = KnowledgeCorpus {
            corpus_id: corpus_id.clone(),
            name: content.title.clone(),
            namespace: namespace.clone(),
            source_ref: source_ref.clone(),
            source_hash: format!("hash-{}", stable_hash(&content.body)),
            state: KnowledgeObjectState::Indexed,
            chunk_count: chunks.len(),
            created_at: now,
            updated_at: now,
        };
        let pack_id = if for_turn {
            // A turn view cannot replace a persisted pack with the same title.
            format!(
                "turn-pack-{}",
                stable_hash(&format!(
                    "{}:{}:{corpus_id}",
                    namespace.key(),
                    content.title
                ))
            )
        } else {
            format!(
                "pack-{}",
                stable_hash(&format!("{}:{}", namespace.key(), content.title))
            )
        };
        let canon = CanonExtractor::new().extract(
            &pack_id,
            &classification,
            &content,
            &chunks,
            governance_level,
        );
        let conflicts = ConflictGovernor::new().detect(&pack_id, &canon);
        let state = if conflicts.is_empty() {
            KnowledgeObjectState::Active
        } else {
            KnowledgeObjectState::Conflicted
        };
        let pack = KnowledgePack {
            pack_id: pack_id.clone(),
            name: classification.metadata.title.clone(),
            kind: classification.metadata.category.pack_kind(),
            namespace,
            activation_policy,
            governance_level,
            source_corpus_refs: vec![corpus_id.clone()],
            canon_pack_ref: Some(canon.canon_id.clone()),
            graph_ref: Some(format!("knowledge-graph:{pack_id}")),
            // Knowledge 保持为受治理的记忆域；结构化业务事实仅通过显式
            // source/growth 投影进入 Matrix，避免激活知识时发生隐式重复写。
            matrix_refs: Vec::new(),
            memory_refs: vec![KernelRef::new(
                "memory",
                format!("knowledge-pack:{pack_id}"),
            )],
            evidence_refs: chunks
                .iter()
                .map(|chunk| chunk.evidence_ref.clone())
                .collect(),
            version: "1".to_string(),
            state,
            health_score_bp: if conflicts.is_empty() { 9_000 } else { 6_000 },
            owner: "memory.knowledge_fabric".to_string(),
            created_at: now,
            updated_at: now,
        };
        KnowledgeIngestionReceipt {
            corpus,
            pack,
            canon,
            conflicts,
            chunks,
            warnings: classification.reasoning,
        }
    }

    #[must_use]
    pub fn ingest_collection(
        &self,
        fabric: &KnowledgeFabric,
        namespace: KnowledgeNamespace,
        activation_policy: KnowledgeActivationPolicy,
        governance_level: KnowledgeGovernanceLevel,
        documents: Vec<DocumentContent>,
    ) -> Vec<Result<KnowledgeIngestionReceipt, KnowledgeStoreError>> {
        documents
            .into_iter()
            .map(|document| {
                fabric.ingest_document(
                    namespace.clone(),
                    activation_policy,
                    governance_level,
                    document,
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct CanonExtractor;

impl CanonExtractor {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn extract(
        &self,
        pack_id: &str,
        classification: &ClassificationResult,
        content: &DocumentContent,
        chunks: &[KnowledgeChunk],
        governance_level: KnowledgeGovernanceLevel,
    ) -> KnowledgeCanonPack {
        build_canon_pack(pack_id, classification, content, chunks, governance_level)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConflictGovernor;

impl ConflictGovernor {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn detect(
        &self,
        pack_id: &str,
        canon: &KnowledgeCanonPack,
    ) -> Vec<KnowledgeConflictRecord> {
        detect_conflicts(pack_id, canon)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ActivationGovernor;

impl ActivationGovernor {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn activate(
        &self,
        fabric: &KnowledgeFabric,
        session_id: &str,
        intent: &str,
        profile: &str,
        project_id: Option<&str>,
    ) -> Result<
        (
            KnowledgeActivationPlan,
            Vec<KnowledgeCanonPack>,
            Vec<KnowledgeComplianceWarning>,
        ),
        KnowledgeStoreError,
    > {
        fabric.activate_inner(session_id, intent, profile, project_id)
    }
}

#[derive(Debug, Clone, Default)]
pub struct UsageFeedbackLoop;

impl UsageFeedbackLoop {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn record(
        &self,
        store: &dyn KnowledgeStore,
        signal: &KnowledgeUsageSignal,
    ) -> Result<(), KnowledgeStoreError> {
        store.record_usage(signal)
    }
}

impl std::fmt::Debug for KnowledgeFabric {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug must not perform I/O or hide a failed read as an empty health.
        formatter
            .debug_struct("KnowledgeFabric")
            .finish_non_exhaustive()
    }
}

fn retire_conflicts_in_state(state: &mut KnowledgeFabricState) -> Vec<KnowledgeConflictRecord> {
    let inactive_pack_states = state
        .packs
        .values()
        .filter(|pack| !knowledge_state_is_actionable(pack.state))
        .map(|pack| (pack.pack_id.clone(), pack.state))
        .collect::<BTreeMap<_, _>>();
    state
        .conflicts
        .values_mut()
        .filter_map(|conflict| {
            let pack_id = conflict.pack_id.as_deref()?;
            let pack_state = inactive_pack_states.get(pack_id).copied()?;
            if conflict.decision.is_some() {
                return None;
            }
            conflict.decision = Some("source_pack_inactive".to_string());
            conflict.state = pack_state;
            Some(conflict.clone())
        })
        .collect::<Vec<_>>()
}

#[derive(Debug, Clone)]
pub struct InMemoryKnowledgeStore {
    state: Arc<RwLock<KnowledgeFabricState>>,
}

impl Default for InMemoryKnowledgeStore {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(KnowledgeFabricState {
                candidate_store_id: uuid::Uuid::new_v4().to_string(),
                ..KnowledgeFabricState::default()
            })),
        }
    }
}

impl InMemoryKnowledgeStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

fn bump_candidate_revisions(
    state: &mut KnowledgeFabricState,
    namespaces: impl Iterator<Item = KnowledgeNamespace>,
) -> Result<(), KnowledgeStoreError> {
    let keys = namespaces
        .flat_map(|namespace| query::candidate_revision_keys(&namespace))
        .collect::<BTreeSet<_>>();
    if keys
        .iter()
        .any(|key| state.candidate_revisions.get(key).copied().unwrap_or(0) == u64::MAX)
    {
        return Err(KnowledgeStoreError::Backend(
            "Knowledge candidate revision exhausted".into(),
        ));
    }
    for key in keys {
        *state.candidate_revisions.entry(key).or_default() += 1;
    }
    Ok(())
}

impl KnowledgeStore for InMemoryKnowledgeStore {
    fn save_receipt(
        &self,
        receipt: &KnowledgeIngestionReceipt,
    ) -> Result<KnowledgeIngestionReceipt, KnowledgeStoreError> {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (key, digest) = knowledge_ingestion_identity(receipt)?;
        if let Some((original_digest, original)) = state.ingestion_receipts.get(&key) {
            if original_digest != &digest {
                return Err(KnowledgeStoreError::Backend(
                    "knowledge ingestion identity conflict".into(),
                ));
            }
            return Ok(original.clone());
        }
        bump_candidate_revisions(&mut state, std::iter::once(receipt.pack.namespace.clone()))?;
        state
            .corpus
            .insert(receipt.corpus.corpus_id.clone(), receipt.corpus.clone());
        state
            .packs
            .insert(receipt.pack.pack_id.clone(), receipt.pack.clone());
        state
            .canon
            .insert(receipt.canon.canon_id.clone(), receipt.canon.clone());
        for chunk in &receipt.chunks {
            state.chunks.insert(chunk.chunk_id.clone(), chunk.clone());
        }
        for conflict in &receipt.conflicts {
            state
                .conflicts
                .insert(conflict.conflict_id.clone(), conflict.clone());
        }
        state
            .ingestion_receipts
            .insert(key, (digest, receipt.clone()));
        Ok(receipt.clone())
    }

    fn record_usage(&self, signal: &KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError> {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = state
            .usage
            .iter()
            .find(|item| item.signal_id == signal.signal_id)
        {
            if serde_json::to_value(existing)? != serde_json::to_value(signal)? {
                return Err(KnowledgeStoreError::Backend(
                    "knowledge usage identity conflict".into(),
                ));
            }
            return Ok(());
        }
        state.usage.push(signal.clone());
        Ok(())
    }

    fn maintenance_inputs(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        bounded_state_snapshot(&state, false)
    }
    fn projection(&self) -> Result<serde_json::Value, KnowledgeStoreError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        knowledge_projection_from_metadata(
            bounded_state_snapshot(&state, false)?,
            state.usage.len(),
        )
    }

    fn read_candidate_page(
        &self,
        scope: &KnowledgeCandidateScope,
        cursor: Option<&KnowledgeCandidateCursor>,
        limit: usize,
        budget_bytes: usize,
    ) -> Result<KnowledgeCandidatePage, KnowledgeStoreError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revision = KnowledgeSourceRevision {
            store_id: state.candidate_store_id.clone(),
            counters: scope
                .revision_keys()
                .into_iter()
                .map(|key| {
                    let value = state.candidate_revisions.get(&key).copied().unwrap_or(0);
                    (key, value)
                })
                .collect(),
        };
        if let Some(cursor) = cursor {
            cursor.validate(scope, &revision)?;
        }
        let limit = limit.clamp(1, KNOWLEDGE_CANDIDATE_PAGE_SIZE);
        let budget_bytes = budget_bytes.min(KNOWLEDGE_READ_MAX_BYTES);
        let selected = state
            .packs
            .values()
            .filter(|pack| {
                scope.includes(&pack.namespace)
                    && cursor.is_none_or(|cursor| pack.pack_id > cursor.after_pack_id)
            })
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = selected.len() > limit;
        let mut budget = Budget(0);
        let mut packs = Vec::new();
        let mut canon = Vec::new();
        for pack in selected.into_iter().take(limit) {
            serde_json::to_writer(&mut budget, pack).map_err(|_| KnowledgeStoreError::ReadLimit)?;
            if budget.0 > budget_bytes {
                return Err(KnowledgeStoreError::ReadLimit);
            }
            if let Some(item) = pack
                .canon_pack_ref
                .as_ref()
                .and_then(|id| state.canon.get(id))
            {
                if item.pack_id != pack.pack_id {
                    return Err(KnowledgeStoreError::Backend(
                        "Knowledge candidate canonical identity mismatch".into(),
                    ));
                }
                serde_json::to_writer(&mut budget, item)
                    .map_err(|_| KnowledgeStoreError::ReadLimit)?;
                if budget.0 > budget_bytes {
                    return Err(KnowledgeStoreError::ReadLimit);
                }
                canon.push(item);
            }
            packs.push(pack);
        }
        let next = if has_more {
            Some(KnowledgeCandidateCursor {
                scope: scope.clone(),
                revision: revision.clone(),
                after_pack_id: packs
                    .last()
                    .ok_or(KnowledgeStoreError::StaleRead)?
                    .pack_id
                    .clone(),
            })
        } else {
            None
        };
        Ok(KnowledgeCandidatePage {
            packs: packs.into_iter().cloned().collect(),
            canon: canon.into_iter().cloned().collect(),
            revision,
            materialized_bytes: budget.0,
            next,
        })
    }

    fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        bounded_state_snapshot(&state, true)
    }

    fn commit_maintenance(
        &self,
        expected: &KnowledgeSnapshot,
        packs: &[KnowledgePack],
        conflicts: &[KnowledgeConflictRecord],
    ) -> Result<(), KnowledgeStoreError> {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = bounded_state_snapshot(&state, false)?;
        if !same_knowledge_maintenance_inputs(expected, &current)? {
            return Err(KnowledgeStoreError::StaleRead);
        }
        let namespaces = packs
            .iter()
            .flat_map(|pack| {
                state
                    .packs
                    .get(&pack.pack_id)
                    .map(|old| old.namespace.clone())
                    .into_iter()
                    .chain(std::iter::once(pack.namespace.clone()))
            })
            .collect::<Vec<_>>();
        bump_candidate_revisions(&mut state, namespaces.into_iter())?;
        for pack in packs {
            state.packs.insert(pack.pack_id.clone(), pack.clone());
        }
        for conflict in conflicts {
            state
                .conflicts
                .insert(conflict.conflict_id.clone(), conflict.clone());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct DocumentClassifier;

impl DocumentClassifier {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn classify(&self, content: &DocumentContent) -> ClassificationResult {
        let text = format!("{} {}", content.title, content.body);
        let text_lc = text.to_ascii_lowercase();
        let category = if contains_any(&text_lc, &["architecture", "design", "架构", "设计"]) {
            DocumentCategory::Architecture
        } else if contains_any(&text_lc, &["api", "endpoint", "接口"]) {
            DocumentCategory::ApiReference
        } else if contains_any(&text_lc, &["config", "yaml", "json", "配置"]) {
            DocumentCategory::Configuration
        } else if contains_any(&text_lc, &["guide", "tutorial", "指南", "教程"]) {
            DocumentCategory::UserGuide
        } else if contains_any(&text_lc, &["review", "审查", "审核"]) {
            DocumentCategory::CodeReview
        } else if contains_any(&text_lc, &["task", "issue", "todo", "任务"]) {
            DocumentCategory::Task
        } else if contains_any(&text_lc, &["knowledge", "faq", "知识库"]) {
            DocumentCategory::KnowledgeBase
        } else if contains_any(&text_lc, &["implementation", "algorithm", "性能", "技术"]) {
            DocumentCategory::Technical
        } else {
            DocumentCategory::Other
        };
        let keywords = extract_keywords(&text);
        let tags = build_tags(category, &keywords);
        ClassificationResult {
            metadata: DocumentMetadata {
                title: content.title.clone(),
                category,
                confidence: if category == DocumentCategory::Other {
                    0.35
                } else {
                    0.82
                },
                keywords,
                tags,
                source: content.source.clone(),
                author: content.author.clone(),
                created_at: content.created_at.clone(),
                modified_at: content.modified_at.clone(),
                language: content
                    .language
                    .clone()
                    .unwrap_or_else(|| "zh-CN".to_string()),
            },
            reasoning: vec![format!("classified as {:?}", category)],
            suggested_layer: category.layer_priority(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictStrategy {
    NewestWins,
    OldestWins,
    Merge,
    HighestConfidence,
    KeepBoth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionResult {
    pub success: bool,
    pub metadata: DocumentMetadata,
    pub layer: u8,
    pub error: Option<String>,
    pub warnings: Vec<String>,
    pub knowledge_receipt: Option<KnowledgeIngestionReceipt>,
}

#[derive(Debug, Clone)]
pub struct DocumentIngestor {
    fabric: KnowledgeFabric,
    namespace: KnowledgeNamespace,
    activation_policy: KnowledgeActivationPolicy,
    governance_level: KnowledgeGovernanceLevel,
    conflict_strategy: ConflictStrategy,
}

impl Default for DocumentIngestor {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentIngestor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            fabric: KnowledgeFabric::new(),
            namespace: KnowledgeNamespace::SharedLibrary("default".to_string()),
            activation_policy: KnowledgeActivationPolicy::OnDemand,
            governance_level: KnowledgeGovernanceLevel::Advisory,
            conflict_strategy: ConflictStrategy::NewestWins,
        }
    }

    #[must_use]
    pub fn with_fabric(mut self, fabric: KnowledgeFabric) -> Self {
        self.fabric = fabric;
        self
    }

    #[must_use]
    pub fn with_namespace(mut self, namespace: KnowledgeNamespace) -> Self {
        self.namespace = namespace;
        self
    }

    #[must_use]
    pub fn with_activation_policy(mut self, activation_policy: KnowledgeActivationPolicy) -> Self {
        self.activation_policy = activation_policy;
        self
    }

    #[must_use]
    pub fn with_governance_level(mut self, governance_level: KnowledgeGovernanceLevel) -> Self {
        self.governance_level = governance_level;
        self
    }

    #[must_use]
    pub fn with_conflict_strategy(mut self, conflict_strategy: ConflictStrategy) -> Self {
        self.conflict_strategy = conflict_strategy;
        self
    }

    #[must_use]
    pub fn ingest(&self, content: &DocumentContent) -> IngestionResult {
        let classification = DocumentClassifier::new().classify(content);
        let mut warnings = classification.reasoning.clone();
        warnings.push(format!("conflict strategy: {:?}", self.conflict_strategy));
        let receipt = self.fabric.ingest_document(
            self.namespace.clone(),
            self.activation_policy,
            self.governance_level,
            content.clone(),
        );
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(error) => {
                return IngestionResult {
                    success: false,
                    metadata: classification.metadata,
                    layer: classification.suggested_layer,
                    error: Some(error.to_string()),
                    warnings,
                    knowledge_receipt: None,
                }
            }
        };
        IngestionResult {
            success: receipt.pack.state != KnowledgeObjectState::Quarantined,
            metadata: classification.metadata,
            layer: classification.suggested_layer,
            error: None,
            warnings,
            knowledge_receipt: Some(receipt),
        }
    }
}

fn chunk_document(corpus_id: &str, content: &DocumentContent) -> Vec<KnowledgeChunk> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut ordinal = 0usize;
    for line in content.body.lines() {
        if current.len() + line.len() > 1_800 && !current.trim().is_empty() {
            chunks.push(make_chunk(
                corpus_id,
                &content.title,
                ordinal,
                &current,
                &content.source,
            ));
            ordinal += 1;
            current.clear();
        }
        current.push_str(line);
        current.push('\n');
    }
    if !current.trim().is_empty() {
        chunks.push(make_chunk(
            corpus_id,
            &content.title,
            ordinal,
            &current,
            &content.source,
        ));
    }
    if chunks.is_empty() {
        chunks.push(make_chunk(
            corpus_id,
            &content.title,
            0,
            &content.body,
            &content.source,
        ));
    }
    chunks
}

fn make_chunk(
    corpus_id: &str,
    title: &str,
    ordinal: usize,
    text: &str,
    source: &Option<String>,
) -> KnowledgeChunk {
    let chunk_id = format!("chunk-{corpus_id}-{ordinal}");
    KnowledgeChunk {
        chunk_id: chunk_id.clone(),
        corpus_id: corpus_id.to_string(),
        ordinal,
        title: title.to_string(),
        text: text.trim().to_string(),
        evidence_ref: EvidenceRef::observed("knowledge_chunk", chunk_id)
            .with_source(source.clone().unwrap_or_else(|| title.to_string())),
        token_estimate: estimate_tokens(text),
    }
}

fn build_canon_pack(
    pack_id: &str,
    classification: &ClassificationResult,
    content: &DocumentContent,
    chunks: &[KnowledgeChunk],
    governance_level: KnowledgeGovernanceLevel,
) -> KnowledgeCanonPack {
    let lines = content
        .body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(12)
        .collect::<Vec<_>>();
    let mut rules = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        if is_rule_like(line) {
            rules.push(KnowledgeCanonRule {
                rule_id: format!("rule-{pack_id}-{idx}"),
                summary: line.trim_start_matches(['-', '*']).trim().to_string(),
                governance_level,
                applies_to: classification.metadata.tags.clone(),
                evidence_refs: chunks
                    .first()
                    .map(|chunk| vec![chunk.evidence_ref.clone()])
                    .unwrap_or_default(),
            });
        }
    }
    if rules.is_empty() {
        rules.push(KnowledgeCanonRule {
            rule_id: format!("rule-{pack_id}-summary"),
            summary: format!("Use {} as relevant background knowledge.", content.title),
            governance_level: KnowledgeGovernanceLevel::Advisory,
            applies_to: classification.metadata.tags.clone(),
            evidence_refs: chunks
                .first()
                .map(|chunk| vec![chunk.evidence_ref.clone()])
                .unwrap_or_default(),
        });
    }
    let summary = lines.join(" ");
    KnowledgeCanonPack {
        canon_id: format!("canon-{pack_id}"),
        pack_id: pack_id.to_string(),
        summary: if summary.is_empty() {
            content.title.clone()
        } else {
            summary.chars().take(1200).collect()
        },
        rules,
        glossary: classification.metadata.keywords.clone(),
        procedures: extract_procedures(&content.body),
        evidence_refs: chunks
            .iter()
            .map(|chunk| chunk.evidence_ref.clone())
            .collect(),
        token_estimate: estimate_tokens(&content.title)
            + chunks
                .iter()
                .map(|c| c.token_estimate.min(256))
                .sum::<u64>(),
        updated_at: Utc::now(),
    }
}

fn detect_conflicts(pack_id: &str, canon: &KnowledgeCanonPack) -> Vec<KnowledgeConflictRecord> {
    let mut conflicts = Vec::new();
    for left in &canon.rules {
        for right in &canon.rules {
            if left.rule_id >= right.rule_id {
                continue;
            }
            let Some((left_polarity, left_core)) = rule_polarity_and_core(&left.summary) else {
                continue;
            };
            let Some((right_polarity, right_core)) = rule_polarity_and_core(&right.summary) else {
                continue;
            };
            let contradictory =
                left_polarity != right_polarity && semantic_cores_match(&left_core, &right_core);
            if contradictory {
                conflicts.push(KnowledgeConflictRecord {
                    conflict_id: format!(
                        "knowledge-conflict-{}",
                        stable_hash(&format!("{}:{}", left.rule_id, right.rule_id))
                    ),
                    pack_id: Some(pack_id.to_string()),
                    conflict_type: "direct_contradiction".to_string(),
                    summary: format!("{} conflicts with {}", left.summary, right.summary),
                    left_ref: KernelRef::new("canon_rule", left.rule_id.clone()),
                    right_ref: KernelRef::new("canon_rule", right.rule_id.clone()),
                    decision: None,
                    state: KnowledgeObjectState::Conflicted,
                    evidence_refs: left
                        .evidence_refs
                        .iter()
                        .chain(right.evidence_refs.iter())
                        .cloned()
                        .collect(),
                    detected_at: Utc::now(),
                });
            }
        }
    }
    conflicts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RulePolarity {
    Required,
    Forbidden,
}

fn rule_polarity_and_core(summary: &str) -> Option<(RulePolarity, String)> {
    let normalized = summary.to_lowercase();
    let negative_markers = [
        "must not",
        "should not",
        "is forbidden",
        "is prohibited",
        "forbidden",
        "prohibited",
        "不得",
        "禁止",
        "不允许",
        "不可",
        "严禁",
    ];
    let positive_markers = [
        "must",
        "should",
        "is required",
        "required",
        "必须",
        "应当",
        "需要",
        "务必",
    ];
    let negative = negative_markers
        .iter()
        .any(|marker| normalized.contains(marker));
    let mut positive_source = normalized.clone();
    for marker in &negative_markers {
        positive_source = positive_source.replace(marker, "");
    }
    let positive = positive_markers
        .iter()
        .any(|marker| positive_source.contains(marker));
    let polarity = match (positive, negative) {
        (true, false) => RulePolarity::Required,
        (false, true) => RulePolarity::Forbidden,
        // Mixed clauses need semantic review; keyword heuristics must not
        // manufacture a contradiction from two compatible constraints.
        _ => return None,
    };
    let mut core = normalized;
    for marker in negative_markers.iter().chain(positive_markers.iter()) {
        core = core.replace(marker, "");
    }
    core.retain(|character| character.is_alphanumeric());
    (core.chars().count() >= 4).then_some((polarity, core))
}

fn semantic_cores_match(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let left_len = left.chars().count();
    let right_len = right.chars().count();
    let (shorter, longer, shorter_len, longer_len) = if left_len <= right_len {
        (left, right, left_len, right_len)
    } else {
        (right, left, right_len, left_len)
    };
    if longer.contains(shorter) && shorter_len.saturating_mul(100) >= longer_len.saturating_mul(80)
    {
        return true;
    }
    let left_pairs = character_pairs(left);
    let right_pairs = character_pairs(right);
    if left_pairs.is_empty() || right_pairs.is_empty() {
        return false;
    }
    let intersection = left_pairs.intersection(&right_pairs).count();
    let union = left_pairs.union(&right_pairs).count();
    intersection.saturating_mul(100) >= union.saturating_mul(80)
}

fn character_pairs(value: &str) -> BTreeSet<(char, char)> {
    value
        .chars()
        .collect::<Vec<_>>()
        .windows(2)
        .map(|pair| (pair[0], pair[1]))
        .collect()
}

fn activation_fits(pack: &KnowledgePack, intent_lc: &str, project_id: Option<&str>) -> bool {
    // Activation preference never grants access to another project's rules.
    if let KnowledgeNamespace::Project(project) = &pack.namespace {
        if project_id != Some(project.as_str()) {
            return false;
        }
    }
    match &pack.activation_policy {
        KnowledgeActivationPolicy::ExplicitOnly => false,
        KnowledgeActivationPolicy::DefaultForProjectGroup => {
            matches!(&pack.namespace,
                KnowledgeNamespace::Project(id) | KnowledgeNamespace::ProjectGroup(id)
                if project_id == Some(id.as_str()))
        }
        KnowledgeActivationPolicy::DefaultForDomain
        | KnowledgeActivationPolicy::DefaultForIntent
        | KnowledgeActivationPolicy::DefaultForRole
        | KnowledgeActivationPolicy::DefaultForUser
        | KnowledgeActivationPolicy::BlockingPolicy
        | KnowledgeActivationPolicy::OnDemand => {
            let haystack = format!(
                "{} {}",
                pack.name.to_ascii_lowercase(),
                pack.namespace.key()
            );
            intent_lc
                .split_whitespace()
                .any(|term| term.len() >= 3 && haystack.contains(term))
                || matches!(
                    pack.activation_policy,
                    KnowledgeActivationPolicy::DefaultForDomain
                        | KnowledgeActivationPolicy::BlockingPolicy
                )
        }
    }
}

fn health_from_state(state: &KnowledgeFabricState) -> KnowledgeFabricHealth {
    KnowledgeFabricHealth {
        corpus_count: state.corpus.len(),
        pack_count: state.packs.len(),
        canon_count: state.canon.len(),
        conflict_count: state.conflicts.len(),
        unresolved_conflict_count: state
            .conflicts
            .values()
            .filter(|conflict| conflict_is_actionable(state, conflict))
            .count(),
        usage_signal_count: state.usage.len(),
        active_pack_count: state
            .packs
            .values()
            .filter(|pack| pack.state == KnowledgeObjectState::Active)
            .count(),
        quarantined_pack_count: state
            .packs
            .values()
            .filter(|pack| pack.state == KnowledgeObjectState::Quarantined)
            .count(),
    }
}

fn knowledge_namespace_tree(state: &KnowledgeFabricState) -> Vec<serde_json::Value> {
    let mut namespaces: BTreeMap<String, (usize, usize, usize)> = BTreeMap::new();
    for corpus in state.corpus.values() {
        let entry = namespaces
            .entry(normalize_namespace_key(&corpus.namespace))
            .or_insert((0, 0, 0));
        entry.0 += 1;
    }
    for pack in state.packs.values() {
        let entry = namespaces
            .entry(normalize_namespace_key(&pack.namespace))
            .or_insert((0, 0, 0));
        entry.1 += 1;
        if matches!(
            pack.state,
            KnowledgeObjectState::Active | KnowledgeObjectState::Canonized
        ) {
            entry.2 += 1;
        }
    }
    namespaces
        .into_iter()
        .map(
            |(namespace, (corpus_count, pack_count, active_pack_count))| {
                let (level, id) = namespace
                    .split_once(':')
                    .map_or((namespace.as_str(), ""), |(level, id)| (level, id));
                serde_json::json!({
                    "namespace": namespace,
                    "level": level,
                    "id": id,
                    "corpus_count": corpus_count,
                    "pack_count": pack_count,
                    "active_pack_count": active_pack_count,
                })
            },
        )
        .collect()
}

fn knowledge_activation_policy_distribution(
    state: &KnowledgeFabricState,
) -> Vec<serde_json::Value> {
    count_by_key(state.packs.values().map(|pack| {
        format!("{:?}", pack.activation_policy)
            .to_ascii_lowercase()
            .replace("blockingpolicy", "blocking")
    }))
}

fn knowledge_governance_distribution(state: &KnowledgeFabricState) -> Vec<serde_json::Value> {
    count_by_key(
        state
            .packs
            .values()
            .map(|pack| format!("{:?}", pack.governance_level).to_ascii_lowercase()),
    )
}

fn knowledge_conflict_projection(state: &KnowledgeFabricState) -> serde_json::Value {
    let unresolved = state
        .conflicts
        .values()
        .filter(|conflict| conflict_is_actionable(state, conflict))
        .count();
    let conflicts = state
        .conflicts
        .values()
        .map(|conflict| {
            serde_json::json!({
                "id": conflict.conflict_id,
                "pack_id": conflict.pack_id,
                "type": conflict.conflict_type,
                "summary": conflict.summary,
                "decision": conflict.decision,
                "state": conflict.state,
                "detected_at": conflict.detected_at,
                "resolution_policy": "authority_then_freshness_then_confidence_else_hold",
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "total": conflicts.len(),
        "unresolved": unresolved,
        "resolution_policy": [
            "system",
            "user_explicit",
            "project_policy",
            "domain_policy",
            "imported_reference",
            "derived",
            "freshness",
            "confidence",
            "hold_with_warning",
        ],
        "conflicts": conflicts,
    })
}

fn knowledge_maintenance_candidates(state: &KnowledgeFabricState) -> Vec<serde_json::Value> {
    let mut candidates = Vec::new();
    for conflict in state
        .conflicts
        .values()
        .filter(|item| conflict_is_actionable(state, item))
    {
        candidates.push(serde_json::json!({
            "id": format!("knowledge-maintenance:conflict:{}", conflict.conflict_id),
            "kind": "unresolved_conflict",
            "status": "pending",
            "severity": "high",
            "pack_id": conflict.pack_id,
            "reason": conflict.summary,
            "action": "review_conflict_resolution",
        }));
    }
    let active_corpus_ids = state
        .packs
        .values()
        .filter(|pack| {
            matches!(
                pack.state,
                KnowledgeObjectState::Active | KnowledgeObjectState::Canonized
            )
        })
        .flat_map(|pack| pack.source_corpus_refs.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut source_hashes: BTreeMap<String, Vec<&KnowledgeCorpus>> = BTreeMap::new();
    for corpus in state.corpus.values() {
        if !active_corpus_ids.contains(&corpus.corpus_id) {
            continue;
        }
        source_hashes
            .entry(format!(
                "{}:{}",
                normalize_namespace_key(&corpus.namespace),
                corpus.source_hash
            ))
            .or_default()
            .push(corpus);
    }
    for (source_hash, corpus) in source_hashes
        .into_iter()
        .filter(|(_, corpus)| corpus.len() > 1)
    {
        candidates.push(serde_json::json!({
            "id": format!("knowledge-maintenance:duplicate:{source_hash}"),
            "kind": "duplicate_merge_candidate",
            "status": "pending",
            "severity": "medium",
            "reason": "multiple corpus records share the same source hash",
            "corpus_ids": corpus.iter().map(|item| item.corpus_id.as_str()).collect::<Vec<_>>(),
            "action": "review_duplicate_merge",
        }));
    }
    for pack in state
        .packs
        .values()
        .filter(|pack| knowledge_state_is_actionable(pack.state))
    {
        if pack.health_score_bp < 7_000 {
            candidates.push(serde_json::json!({
                "id": format!("knowledge-maintenance:health:{}", pack.pack_id),
                "kind": "stale_rule_review_candidate",
                "status": "pending",
                "severity": "medium",
                "pack_id": pack.pack_id,
                "namespace": normalize_namespace_key(&pack.namespace),
                "reason": format!("pack health score {} below 7000bp", pack.health_score_bp),
                "action": "review_pack_health",
            }));
        }
    }
    candidates
}

fn knowledge_pack_rank(pack: &KnowledgePack) -> (u8, u8, u16, DateTime<Utc>, &str) {
    (
        match pack.governance_level {
            KnowledgeGovernanceLevel::Advisory => 1,
            KnowledgeGovernanceLevel::Required => 2,
            KnowledgeGovernanceLevel::Blocking => 3,
        },
        match pack.activation_policy {
            KnowledgeActivationPolicy::ExplicitOnly => 0,
            KnowledgeActivationPolicy::OnDemand => 1,
            KnowledgeActivationPolicy::DefaultForIntent
            | KnowledgeActivationPolicy::DefaultForRole
            | KnowledgeActivationPolicy::DefaultForUser => 2,
            KnowledgeActivationPolicy::DefaultForProjectGroup
            | KnowledgeActivationPolicy::DefaultForDomain => 3,
            KnowledgeActivationPolicy::BlockingPolicy => 4,
        },
        pack.health_score_bp,
        pack.updated_at,
        pack.pack_id.as_str(),
    )
}

fn dedup_serialized_refs<T>(
    refs: impl IntoIterator<Item = T>,
) -> Result<Vec<T>, KnowledgeStoreError>
where
    T: Serialize,
{
    let mut unique = BTreeMap::new();
    for reference in refs {
        unique
            .entry(serde_json::to_string(&reference)?)
            .or_insert(reference);
    }
    Ok(unique.into_values().collect())
}

fn knowledge_recall_quality_projection(state: &KnowledgeFabricState) -> serde_json::Value {
    let namespace_rows = knowledge_namespace_tree(state);
    let suppressed_by_namespace = state
        .packs
        .values()
        .filter(|pack| {
            !matches!(
                pack.state,
                KnowledgeObjectState::Active | KnowledgeObjectState::Canonized
            )
        })
        .map(|pack| {
            serde_json::json!({
                "namespace": normalize_namespace_key(&pack.namespace),
                "pack_id": pack.pack_id,
                "reason": format!("state={:?}", pack.state),
            })
        })
        .collect::<Vec<_>>();
    let unrelated_selected_count = 0usize;
    let omitted_high_value_count = state
        .packs
        .values()
        .filter(|pack| {
            pack.governance_level != KnowledgeGovernanceLevel::Advisory
                && !matches!(
                    pack.state,
                    KnowledgeObjectState::Active | KnowledgeObjectState::Canonized
                )
        })
        .count();
    let precision_estimate = if state.packs.is_empty() {
        1.0
    } else {
        let active = state
            .packs
            .values()
            .filter(|pack| {
                matches!(
                    pack.state,
                    KnowledgeObjectState::Active | KnowledgeObjectState::Canonized
                )
            })
            .count();
        active as f64 / state.packs.len() as f64
    };
    serde_json::json!({
        "selected_by_namespace": namespace_rows,
        "suppressed_by_namespace": suppressed_by_namespace,
        "cross_project_contamination_warnings": [],
        "unrelated_selected_count": unrelated_selected_count,
        "omitted_high_value_count": omitted_high_value_count,
        "precision_estimate": precision_estimate,
        "conflict_warnings": state.conflicts.values().filter(|item| conflict_is_actionable(state, item)).map(|item| item.summary.as_str()).collect::<Vec<_>>(),
        "policy": "project scoped knowledge stays out of unrelated projects; global/domain knowledge enters body only when required, blocking, or relevant, otherwise it is kept as pointer/governance evidence",
    })
}

fn normalize_namespace_key(namespace: &KnowledgeNamespace) -> String {
    match namespace {
        KnowledgeNamespace::SharedLibrary(id) if id == "global" => "global".to_string(),
        other => other.key(),
    }
}

fn knowledge_state_is_actionable(state: KnowledgeObjectState) -> bool {
    matches!(
        state,
        KnowledgeObjectState::Active
            | KnowledgeObjectState::Canonized
            | KnowledgeObjectState::Candidate
            | KnowledgeObjectState::Conflicted
    )
}

fn conflict_is_actionable(
    state: &KnowledgeFabricState,
    conflict: &KnowledgeConflictRecord,
) -> bool {
    conflict.decision.is_none()
        && conflict.pack_id.as_deref().is_none_or(|pack_id| {
            state
                .packs
                .get(pack_id)
                .is_some_and(|pack| knowledge_state_is_actionable(pack.state))
        })
}

fn count_by_key<I>(keys: I) -> Vec<serde_json::Value>
where
    I: IntoIterator<Item = String>,
{
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for key in keys {
        *counts.entry(key).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(key, count)| serde_json::json!({ "key": key, "count": count }))
        .collect()
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn extract_keywords(text: &str) -> Vec<String> {
    let stop_words = [
        "the", "and", "that", "with", "this", "from", "have", "shall", "must", "should", "需要",
        "必须", "一个", "我们", "以及", "进行",
    ];
    let mut freq = BTreeMap::<String, usize>::new();
    for word in text
        .split(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
        .map(str::trim)
        .filter(|word| word.chars().count() >= 3)
    {
        let word = word.to_ascii_lowercase();
        if !stop_words.contains(&word.as_str()) {
            *freq.entry(word).or_default() += 1;
        }
    }
    let mut words = freq
        .into_iter()
        .map(|(word, count)| (count, word))
        .collect::<Vec<_>>();
    words.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    words.into_iter().take(16).map(|(_, word)| word).collect()
}

fn build_tags(category: DocumentCategory, keywords: &[String]) -> Vec<String> {
    let mut tags = BTreeSet::new();
    tags.insert(
        category
            .display_name()
            .to_ascii_lowercase()
            .replace(' ', "_"),
    );
    for keyword in keywords.iter().take(8) {
        tags.insert(keyword.clone());
    }
    tags.into_iter().collect()
}

fn extract_procedures(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("1.")
                || line.starts_with("2.")
                || line.starts_with("3.")
                || line.contains("步骤")
                || line.to_ascii_lowercase().contains("step")
        })
        .take(16)
        .map(str::to_string)
        .collect()
}

fn is_rule_like(line: &str) -> bool {
    let lc = line.to_ascii_lowercase();
    lc.contains("must")
        || lc.contains("shall")
        || lc.contains("should")
        || lc.contains("required")
        || lc.contains("prohibit")
        || line.contains("必须")
        || line.contains("应该")
        || line.contains("不得")
        || line.contains("禁止")
        || line.contains("需要")
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_projection_preserves_durable_rules_and_does_not_survive_the_turn() {
        let fabric = KnowledgeFabric::new();
        let durable = fabric
            .ingest_document(
                KnowledgeNamespace::Project("project-a".into()),
                KnowledgeActivationPolicy::DefaultForDomain,
                KnowledgeGovernanceLevel::Required,
                DocumentContent::new(
                    "Safety policy",
                    "The operator must retain the original audit evidence.",
                ),
            )
            .unwrap();
        let before = serde_json::to_value(fabric.snapshot().unwrap()).unwrap();
        let turn = KnowledgeIngestionService::new().project_for_turn(
            KnowledgeNamespace::Project("project-a".into()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Advisory,
            DocumentContent::new("Safety policy", "A temporary suggestion for this turn."),
        );
        assert_ne!(durable.pack.pack_id, turn.receipt.pack.pack_id);
        let (plan, canons, warnings) = fabric
            .activate_with_turn_projection(
                "session",
                "safety policy",
                "profile",
                Some("project-a"),
                &[turn.clone()],
            )
            .unwrap();
        assert_eq!(plan.active_pack_ids.len(), 2);
        assert!(plan.active_pack_ids.contains(&durable.pack.pack_id));
        assert!(canons.iter().any(|canon| canon
            .rules
            .iter()
            .any(|rule| rule.summary.contains("original audit evidence"))));
        assert!(warnings
            .iter()
            .any(|warning| warning.level == KnowledgeGovernanceLevel::Required));
        assert_eq!(
            serde_json::to_value(fabric.snapshot().unwrap()).unwrap(),
            before
        );
        let (next, _, _) = fabric
            .activate("next", "safety policy", "profile", Some("project-a"))
            .unwrap();
        assert_eq!(next.active_pack_ids, vec![durable.pack.pack_id]);
        let (foreign, _, _) = fabric
            .activate_with_turn_projection(
                "foreign",
                "safety policy",
                "profile",
                Some("project-b"),
                &[turn],
            )
            .unwrap();
        assert!(foreign.active_pack_ids.is_empty());
    }

    #[test]
    fn project_namespace_fences_every_activation_policy_before_relevance() {
        for policy in [
            KnowledgeActivationPolicy::ExplicitOnly,
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeActivationPolicy::DefaultForIntent,
            KnowledgeActivationPolicy::DefaultForRole,
            KnowledgeActivationPolicy::DefaultForUser,
            KnowledgeActivationPolicy::BlockingPolicy,
            KnowledgeActivationPolicy::OnDemand,
        ] {
            let fabric = KnowledgeFabric::new();
            fabric
                .ingest_document(
                    KnowledgeNamespace::Project("cowd".into()),
                    policy,
                    KnowledgeGovernanceLevel::Required,
                    DocumentContent::new(
                        "Safety policy",
                        "The operator must retain audit evidence.",
                    ),
                )
                .unwrap();
            for project in [None, Some("cow"), Some("cowd-other")] {
                let (plan, _, _) = fabric
                    .activate("foreign", "safety policy cowd", "profile", project)
                    .unwrap();
                assert!(plan.active_pack_ids.is_empty(), "{policy:?} {project:?}");
            }
            let (own, _, _) = fabric
                .activate("own", "safety policy", "profile", Some("cowd"))
                .unwrap();
            assert_eq!(
                own.active_pack_ids.is_empty(),
                policy == KnowledgeActivationPolicy::ExplicitOnly,
                "{policy:?}"
            );
        }
    }

    #[test]
    fn ingestion_creates_corpus_pack_canon_without_global_memory() {
        let fabric = KnowledgeFabric::new();
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::SharedLibrary("ops".to_string()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new(
                "Architecture Supply Chain Policy",
                "Architecture: default process\n必须保留证据链\nStep 1. inspect demand\nStep 2. review supplier risk",
            ),
        ).unwrap();

        assert!(receipt.corpus.corpus_id.starts_with("corpus-"));
        assert_eq!(
            receipt.pack.namespace,
            KnowledgeNamespace::SharedLibrary("ops".to_string())
        );
        assert_eq!(
            receipt.pack.activation_policy,
            KnowledgeActivationPolicy::DefaultForDomain
        );
        assert!(!receipt.canon.rules.is_empty());
        assert!(!receipt.chunks.is_empty());
    }

    #[test]
    fn archived_memory_source_quarantines_durable_knowledge() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let mut document = DocumentContent::new(
            "Derived project decision",
            "The project must use the governed runtime path.",
        );
        document.source = Some("memory:archived-source".to_string());
        let receipt = fabric
            .ingest_document(
                KnowledgeNamespace::Project("cowd".to_string()),
                KnowledgeActivationPolicy::OnDemand,
                KnowledgeGovernanceLevel::Advisory,
                document,
            )
            .unwrap();
        let quarantined = fabric.quarantine_source("memory:archived-source").unwrap();
        assert_eq!(quarantined, vec![receipt.pack.pack_id.clone()]);
        let (activation, _, _) = fabric
            .activate("session", "governed runtime", "main_turn", Some("cowd"))
            .unwrap();
        assert!(activation.active_pack_ids.is_empty());

        let reloaded = KnowledgeFabric::with_store(store);
        let pack = reloaded
            .snapshot()
            .unwrap()
            .packs
            .into_iter()
            .find(|pack| pack.pack_id == receipt.pack.pack_id)
            .unwrap();
        assert_eq!(pack.state, KnowledgeObjectState::Quarantined);
    }

    #[test]
    fn activation_selects_default_pack_and_reports_required_rules() {
        let fabric = KnowledgeFabric::new();
        let receipt = fabric
            .ingest_document(
                KnowledgeNamespace::Domain("architecture".to_string()),
                KnowledgeActivationPolicy::DefaultForDomain,
                KnowledgeGovernanceLevel::Required,
                DocumentContent::new(
                    "Architecture Rules",
                    "must retain evidence before final answer",
                ),
            )
            .unwrap();
        let (plan, _canon, warnings) = fabric
            .activate("s1", "architecture review", "DeepInvestigation", None)
            .unwrap();

        assert!(plan.active_pack_ids.contains(&receipt.pack.pack_id));
        assert!(!warnings.is_empty());
        let report = fabric.turn_report(&plan, warnings);
        assert_eq!(
            report.activation_plan_id.as_deref(),
            Some(plan.plan_id.as_str())
        );
    }

    #[test]
    fn namespace_blocks_irrelevant_project_knowledge_without_polluting_global_context() {
        let fabric = KnowledgeFabric::new();
        let project_receipt = fabric
            .ingest_document(
                KnowledgeNamespace::Project("cowd".to_string()),
                KnowledgeActivationPolicy::DefaultForProjectGroup,
                KnowledgeGovernanceLevel::Required,
                DocumentContent::new("Cowd Runtime Rules", "must record context turn report"),
            )
            .unwrap();
        let unrelated_receipt = fabric
            .ingest_document(
                KnowledgeNamespace::Project("erp".to_string()),
                KnowledgeActivationPolicy::DefaultForProjectGroup,
                KnowledgeGovernanceLevel::Required,
                DocumentContent::new("ERP Billing Rules", "must reconcile invoice ledger"),
            )
            .unwrap();

        let (plan, _, _) = fabric
            .activate(
                "s1",
                "context turn report",
                "DeepInvestigation",
                Some("cowd"),
            )
            .unwrap();

        assert!(plan.active_pack_ids.contains(&project_receipt.pack.pack_id));
        assert!(!plan
            .active_pack_ids
            .contains(&unrelated_receipt.pack.pack_id));
        assert!(
            plan.blocked_namespaces
                .iter()
                .all(|item| !item.contains("project:erp")),
            "foreign project metadata is not fetched for a scoped activation"
        );
    }

    #[test]
    fn universal_knowledge_fabric_records_conflicts_without_implicit_matrix_writes() {
        let fabric = KnowledgeFabric::new();
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::SharedLibrary("operations".to_string()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Blocking,
            DocumentContent::new(
                "Operations Contradiction Pack",
                "must keep supplier recovery evidence\nmust not keep supplier recovery evidence",
            ),
        ).unwrap();

        assert!(!receipt.conflicts.is_empty());
        assert!(receipt.pack.matrix_refs.is_empty());
        let projection = fabric.projection().unwrap();
        assert_eq!(
            projection["health"]["unresolved_conflict_count"]
                .as_u64()
                .unwrap_or_default(),
            receipt.conflicts.len() as u64
        );
        assert!(projection["namespace_tree"]
            .as_array()
            .is_some_and(|rows| rows
                .iter()
                .any(|row| row["namespace"] == "shared:operations")));
        assert_eq!(
            projection["conflict_projection"]["unresolved"]
                .as_u64()
                .unwrap_or_default(),
            receipt.conflicts.len() as u64
        );
        assert!(projection["maintenance_candidates"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["kind"] == "unresolved_conflict")));
        assert!(projection["recall_quality"]["conflict_warnings"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty()));
    }

    #[test]
    fn compatible_governance_rules_do_not_manufacture_a_conflict() {
        let fabric = KnowledgeFabric::new();
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::Project("cowd".to_string()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new(
                "Fact governance",
                "禁止未经验证的对话直接晋升为事实\n所有事实断言必须经过证据验证流程后才能被采纳，不可绕过治理机制",
            ),
        ).unwrap();

        assert!(receipt.conflicts.is_empty());
        assert_eq!(receipt.pack.state, KnowledgeObjectState::Active);
    }

    #[test]
    fn quarantining_a_source_retires_its_conflict_without_losing_evidence() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let mut document = DocumentContent::new(
            "Retired conflict",
            "must retain speculative output\nmust not retain speculative output",
        );
        document.source = Some("memory:retired-conflict".to_string());
        let receipt = fabric
            .ingest_document(
                KnowledgeNamespace::Project("cowd".to_string()),
                KnowledgeActivationPolicy::OnDemand,
                KnowledgeGovernanceLevel::Advisory,
                document,
            )
            .unwrap();
        assert!(!receipt.conflicts.is_empty());

        fabric.quarantine_source("memory:retired-conflict").unwrap();
        let projection = fabric.projection().unwrap();
        assert_eq!(projection["health"]["unresolved_conflict_count"], 0);
        assert!(projection["maintenance_candidates"]
            .as_array()
            .is_some_and(Vec::is_empty));

        let reloaded = KnowledgeFabric::with_store(store);
        let snapshot = reloaded.snapshot().unwrap();
        assert_eq!(snapshot.health().unresolved_conflict_count, 0);
        assert!(snapshot
            .conflicts
            .iter()
            .all(|conflict| conflict.decision.as_deref() == Some("source_pack_inactive")));
        assert!(!snapshot.corpus.is_empty());
        assert!(!snapshot.canon.is_empty());
        assert!(!snapshot.chunks.is_empty());
    }

    #[test]
    fn exact_duplicate_knowledge_is_consolidated_without_losing_evidence() {
        let fabric = KnowledgeFabric::new();
        let mut first = DocumentContent::new(
            "Operations handbook",
            "Every release must retain a verified evidence chain.",
        );
        first.source = Some("file:///shared/operations-v1.md".to_string());
        let mut second = DocumentContent::new(
            "Operations handbook copy",
            "Every release must retain a verified evidence chain.",
        );
        second.source = Some("file:///imports/operations-copy.md".to_string());
        let first_receipt = fabric
            .ingest_document(
                KnowledgeNamespace::SharedLibrary("operations".to_string()),
                KnowledgeActivationPolicy::DefaultForDomain,
                KnowledgeGovernanceLevel::Required,
                first,
            )
            .unwrap();
        let second_receipt = fabric
            .ingest_document(
                KnowledgeNamespace::SharedLibrary("operations".to_string()),
                KnowledgeActivationPolicy::OnDemand,
                KnowledgeGovernanceLevel::Advisory,
                second,
            )
            .unwrap();

        let report = fabric.consolidate_exact_duplicates().unwrap();
        assert_eq!(report.superseded_pack_ids.len(), 1);
        assert!(report
            .canonical_pack_ids
            .contains(&first_receipt.pack.pack_id));
        let snapshot = fabric.snapshot().unwrap();
        let canonical = snapshot
            .packs
            .iter()
            .find(|pack| pack.pack_id == first_receipt.pack.pack_id)
            .unwrap();
        assert_eq!(canonical.source_corpus_refs.len(), 2);
        assert!(canonical.evidence_refs.len() >= 2);
        assert_eq!(
            snapshot
                .packs
                .iter()
                .find(|pack| pack.pack_id == second_receipt.pack.pack_id)
                .unwrap()
                .state,
            KnowledgeObjectState::Superseded
        );

        let repeated = fabric.consolidate_exact_duplicates().unwrap();
        assert!(repeated.superseded_pack_ids.is_empty());
    }

    #[test]
    fn derived_knowledge_store_preserves_corpus_pack_canon_conflict_and_usage() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let receipt = fabric
            .ingest_document(
                KnowledgeNamespace::SharedLibrary("quality".to_string()),
                KnowledgeActivationPolicy::DefaultForDomain,
                KnowledgeGovernanceLevel::Blocking,
                DocumentContent::new(
                    "Quality Blocking Rules",
                    "must keep inspection evidence\nmust not keep inspection evidence",
                ),
            )
            .unwrap();
        fabric
            .record_usage(KnowledgeUsageSignal {
                signal_id: "usage-1".to_string(),
                session_id: "s1".to_string(),
                pack_id: receipt.pack.pack_id.clone(),
                action: "selected".to_string(),
                summary: "selected during test".to_string(),
                score_delta_bp: 5,
                occurred_at: Utc::now(),
            })
            .unwrap();

        let reloaded = KnowledgeFabric::with_store(store);
        let snapshot = reloaded.snapshot().unwrap();
        assert_eq!(snapshot.corpus.len(), 1);
        assert_eq!(snapshot.packs.len(), 1);
        assert_eq!(snapshot.canon.len(), 1);
        assert_eq!(snapshot.conflicts.len(), receipt.conflicts.len());
        assert_eq!(snapshot.usage.len(), 1);
    }

    #[test]
    fn namespace_search_and_governors_are_real_runtime_services() {
        let fabric = KnowledgeFabric::new();
        let receipts = KnowledgeIngestionService::new().ingest_collection(
            &fabric,
            KnowledgeNamespace::Domain("manufacturing".to_string()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Required,
            vec![DocumentContent::new(
                "Manufacturing Process",
                "must validate demand before sourcing\nStep 1. validate demand",
            )],
        );
        let search = fabric
            .search_namespace(
                &KnowledgeNamespace::Domain("manufacturing".to_string()),
                "manufacturing demand",
            )
            .unwrap();
        assert!(receipts[0].as_ref().unwrap().pack.matrix_refs.is_empty());
        assert_eq!(search.packs.len(), 1);
        assert!(!search.canon.is_empty());
        assert!(search.canon[0]
            .rules
            .iter()
            .any(|rule| rule.governance_level == KnowledgeGovernanceLevel::Required));
    }
    struct FaultingKnowledgeStore {
        inner: InMemoryKnowledgeStore,
        mode: std::sync::atomic::AtomicU8,
    }

    impl FaultingKnowledgeStore {
        fn mode(&self) -> u8 {
            self.mode.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn set(&self, mode: u8) {
            self.mode.store(mode, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl KnowledgeStore for FaultingKnowledgeStore {
        fn save_receipt(
            &self,
            receipt: &KnowledgeIngestionReceipt,
        ) -> Result<KnowledgeIngestionReceipt, KnowledgeStoreError> {
            if self.mode() == 1 {
                return Err(KnowledgeStoreError::Backend(
                    "write refused before commit".into(),
                ));
            }
            let saved = self.inner.save_receipt(receipt)?;
            if self.mode() == 2 {
                return Err(KnowledgeStoreError::Backend(
                    "response lost after commit".into(),
                ));
            }
            Ok(saved)
        }
        fn record_usage(&self, signal: &KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError> {
            if self.mode() == 1 {
                return Err(KnowledgeStoreError::Backend("usage write refused".into()));
            }
            self.inner.record_usage(signal)
        }
        fn read_candidate_page(
            &self,
            scope: &KnowledgeCandidateScope,
            cursor: Option<&KnowledgeCandidateCursor>,
            limit: usize,
            budget_bytes: usize,
        ) -> Result<KnowledgeCandidatePage, KnowledgeStoreError> {
            if self.mode() == 3 {
                return Err(KnowledgeStoreError::Backend("read refused".into()));
            }
            self.inner
                .read_candidate_page(scope, cursor, limit, budget_bytes)
        }
        fn maintenance_inputs(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
            if self.mode() == 3 {
                return Err(KnowledgeStoreError::Backend("read refused".into()));
            }
            self.inner.maintenance_inputs()
        }
        fn projection(&self) -> Result<serde_json::Value, KnowledgeStoreError> {
            if self.mode() == 3 {
                return Err(KnowledgeStoreError::Backend("read refused".into()));
            }
            self.inner.projection()
        }
        fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
            if self.mode() == 3 {
                return Err(KnowledgeStoreError::Backend("read refused".into()));
            }
            self.inner.snapshot()
        }
        fn commit_maintenance(
            &self,
            expected: &KnowledgeSnapshot,
            packs: &[KnowledgePack],
            conflicts: &[KnowledgeConflictRecord],
        ) -> Result<(), KnowledgeStoreError> {
            self.inner.commit_maintenance(expected, packs, conflicts)
        }
    }

    fn faulting_fabric() -> (Arc<FaultingKnowledgeStore>, KnowledgeFabric) {
        let store = Arc::new(FaultingKnowledgeStore {
            inner: InMemoryKnowledgeStore::new(),
            mode: std::sync::atomic::AtomicU8::new(0),
        });
        (store.clone(), KnowledgeFabric::with_store(store))
    }

    fn ingest_fault_test(
        fabric: &KnowledgeFabric,
    ) -> Result<KnowledgeIngestionReceipt, KnowledgeStoreError> {
        let mut document = DocumentContent::new("durable rule", "must retain approval evidence");
        document.source = Some("memory:durable-rule".into());
        fabric.ingest_document(
            KnowledgeNamespace::Project("cowd".into()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            document,
        )
    }

    #[test]
    fn knowledge_refused_ingestion_does_not_publish_a_receipt_or_phantom_pack() {
        let (store, fabric) = faulting_fabric();
        store.set(1);
        assert!(ingest_fault_test(&fabric).is_err());
        assert!(fabric.snapshot().unwrap().packs.is_empty());
        let result = DocumentIngestor::new()
            .with_fabric(fabric)
            .ingest(&DocumentContent::new("write denied", "body"));
        assert!(!result.success);
        assert!(result.error.is_some());
        assert!(result.knowledge_receipt.is_none());
        assert!(store.inner.snapshot().unwrap().packs.is_empty());
    }

    #[test]
    fn knowledge_lost_commit_response_stays_unknown_and_reopen_retry_is_idempotent() {
        let (store, fabric) = faulting_fabric();
        store.set(2);
        assert!(
            ingest_fault_test(&fabric).is_err(),
            "lost response is not a successful receipt"
        );
        let original = store.inner.snapshot().unwrap();
        assert_eq!(original.packs.len(), 1);
        store.set(0);
        drop(fabric);
        let reopened = KnowledgeFabric::with_store(store);
        let receipt = ingest_fault_test(&reopened).unwrap();
        assert_eq!(
            receipt.pack, original.packs[0],
            "retry returns original committed timestamps and identity"
        );
        reopened.quarantine_source("memory:durable-rule").unwrap();
        let repeated = ingest_fault_test(&reopened).unwrap();
        assert_eq!(
            repeated.pack, receipt.pack,
            "immutable ingestion receipt is historical, not current lifecycle"
        );
        let current = reopened.snapshot().unwrap();
        assert_eq!(current.packs.len(), 1);
        assert_eq!(
            current.packs[0].state,
            KnowledgeObjectState::Quarantined,
            "retry cannot resurrect a quarantined source"
        );
    }

    #[test]
    fn knowledge_read_failure_reaches_activation_search_projection_and_maintenance() {
        let (store, fabric) = faulting_fabric();
        ingest_fault_test(&fabric).unwrap();
        store.set(3);
        assert!(fabric
            .activate("s", "approval", "main", Some("cowd"))
            .is_err());
        assert!(fabric
            .search_namespace(&KnowledgeNamespace::Project("cowd".into()), "")
            .is_err());
        assert!(fabric.projection().is_err());
        assert!(fabric.quarantine_source("memory:durable-rule").is_err());
        assert!(fabric.consolidate_exact_duplicates().is_err());
        assert_eq!(
            store.inner.snapshot().unwrap().packs[0].state,
            KnowledgeObjectState::Active
        );
    }

    #[test]
    fn knowledge_usage_failure_unknown_duplicate_idempotent_changed_identity_refused() {
        let (store, fabric) = faulting_fabric();
        let signal = KnowledgeUsageSignal {
            signal_id: "one".into(),
            session_id: "session".into(),
            pack_id: "pack".into(),
            action: "selected".into(),
            summary: "real observed selection".into(),
            score_delta_bp: 1,
            occurred_at: Utc::now(),
        };
        store.set(1);
        assert!(fabric.record_usage(signal.clone()).is_err());
        assert!(fabric.snapshot().unwrap().usage.is_empty());
        store.set(0);
        fabric.record_usage(signal.clone()).unwrap();
        fabric.record_usage(signal.clone()).unwrap();
        let mut wrong = signal.clone();
        wrong.session_id = "foreign".into();
        assert!(fabric.record_usage(wrong).is_err());
        let current = fabric.snapshot().unwrap();
        assert_eq!(current.usage.len(), 1);
        assert_eq!(current.usage[0].session_id, "session");
    }

    #[test]
    fn knowledge_maintenance_cas_rejects_foreign_change_without_partial_updates() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let receipt = ingest_fault_test(&fabric).unwrap();
        let old = fabric.snapshot().unwrap();
        let mut changed = receipt.pack.clone();
        changed.governance_level = KnowledgeGovernanceLevel::Blocking;
        store
            .commit_maintenance(
                &store.maintenance_inputs().unwrap(),
                &[changed.clone()],
                &[],
            )
            .unwrap();
        let mut stale = receipt.pack.clone();
        stale.state = KnowledgeObjectState::Quarantined;
        assert!(matches!(
            store.commit_maintenance(&old, &[stale], &[]),
            Err(KnowledgeStoreError::StaleRead)
        ));
        let current = fabric.snapshot().unwrap();
        assert_eq!(current.packs[0], changed);
    }

    #[test]
    fn knowledge_ingestion_same_identity_changed_policy_refused_namespace_remains_distinct() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let receipt = ingest_fault_test(&fabric).unwrap();
        let mut wrong = receipt.clone();
        wrong.pack.governance_level = KnowledgeGovernanceLevel::Blocking;
        assert!(store.save_receipt(&wrong).is_err());
        let other = fabric
            .ingest_document(
                KnowledgeNamespace::Project("foreign".into()),
                KnowledgeActivationPolicy::OnDemand,
                KnowledgeGovernanceLevel::Advisory,
                DocumentContent::new("durable rule", "must retain approval evidence"),
            )
            .unwrap();
        assert_ne!(receipt.corpus.corpus_id, other.corpus.corpus_id);
        assert_ne!(receipt.pack.pack_id, other.pack.pack_id);
        assert_eq!(fabric.snapshot().unwrap().packs.len(), 2);
    }

    #[test]
    fn knowledge_oversized_read_is_an_error_not_a_truncated_success() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let receipt = ingest_fault_test(&fabric).unwrap();
        let mut pack = receipt.pack;
        pack.name = "x".repeat(KNOWLEDGE_READ_MAX_BYTES + 1);
        store
            .commit_maintenance(&store.maintenance_inputs().unwrap(), &[pack], &[])
            .unwrap();
        assert!(matches!(
            fabric.snapshot(),
            Err(KnowledgeStoreError::ReadLimit)
        ));
        assert!(matches!(
            fabric.activate("s", "approval", "main", Some("cowd")),
            Err(KnowledgeStoreError::ReadLimit)
        ));
    }
    #[test]
    fn knowledge_online_reads_do_not_materialize_or_budget_usage_history() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store);
        let receipt = ingest_fault_test(&fabric).unwrap();
        for i in 0..=KNOWLEDGE_READ_MAX_ROWS {
            fabric
                .record_usage(KnowledgeUsageSignal {
                    signal_id: format!("signal-{i}"),
                    session_id: "s".into(),
                    pack_id: receipt.pack.pack_id.clone(),
                    action: "observed".into(),
                    summary: "selection".into(),
                    score_delta_bp: 0,
                    occurred_at: Utc::now(),
                })
                .unwrap();
        }
        assert!(
            matches!(fabric.snapshot(), Err(KnowledgeStoreError::ReadLimit)),
            "full export is a separate bounded query"
        );
        let plan = fabric
            .activate("s", "approval", "main", Some("cowd"))
            .unwrap()
            .0;
        assert!(plan.active_pack_ids.contains(&receipt.pack.pack_id));
        let projection = fabric.projection().unwrap();
        assert_eq!(
            projection["usage_signal_count"],
            KNOWLEDGE_READ_MAX_ROWS + 1
        );
        assert_eq!(
            projection["health"]["usage_signal_count"],
            KNOWLEDGE_READ_MAX_ROWS + 1
        );
        assert_eq!(
            fabric.quarantine_source("memory:durable-rule").unwrap(),
            vec![receipt.pack.pack_id]
        );
    }

    #[test]
    fn knowledge_namespace_filter_precedes_materialization_budget() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let receipt = ingest_fault_test(&fabric).unwrap();
        let other = fabric
            .ingest_document(
                KnowledgeNamespace::Project("foreign".into()),
                KnowledgeActivationPolicy::OnDemand,
                KnowledgeGovernanceLevel::Advisory,
                DocumentContent::new("foreign", "foreign content"),
            )
            .unwrap();
        let mut oversized = other.pack;
        oversized.name = "x".repeat(KNOWLEDGE_READ_MAX_BYTES + 1);
        store
            .commit_maintenance(&store.maintenance_inputs().unwrap(), &[oversized], &[])
            .unwrap();
        let own = fabric
            .search_namespace(&KnowledgeNamespace::Project("cowd".into()), "")
            .unwrap();
        assert_eq!(own.packs.len(), 1);
        assert_eq!(own.packs[0].pack_id, receipt.pack.pack_id);
        assert!(matches!(
            fabric.search_namespace(&KnowledgeNamespace::Project("foreign".into()), ""),
            Err(KnowledgeStoreError::ReadLimit)
        ));
    }
    #[test]
    fn knowledge_collection_reports_each_commit_instead_of_hiding_a_partial_batch() {
        let fabric = KnowledgeFabric::new();
        let receipts = KnowledgeIngestionService::new().ingest_collection(
            &fabric,
            KnowledgeNamespace::Project("cowd".into()),
            KnowledgeActivationPolicy::OnDemand,
            KnowledgeGovernanceLevel::Advisory,
            vec![
                DocumentContent::new("first", "first durable body"),
                DocumentContent::new("too large", "x".repeat(KNOWLEDGE_READ_MAX_BYTES + 1)),
                DocumentContent::new("last", "last durable body"),
            ],
        );
        assert_eq!(receipts.len(), 3);
        assert!(receipts[0].is_ok());
        assert!(matches!(&receipts[1], Err(KnowledgeStoreError::ReadLimit)));
        assert!(receipts[2].is_ok());
        let state = fabric.snapshot().unwrap();
        assert_eq!(state.packs.len(), 2);
        assert!(state.packs.iter().any(|pack| pack.name == "first"));
        assert!(state.packs.iter().any(|pack| pack.name == "last"));
    }
    fn ingest_query_rule(
        fabric: &KnowledgeFabric,
        namespace: KnowledgeNamespace,
        title: &str,
        level: KnowledgeGovernanceLevel,
    ) -> KnowledgeIngestionReceipt {
        fabric
            .ingest_document(
                namespace,
                KnowledgeActivationPolicy::DefaultForDomain,
                level,
                DocumentContent::new(title, "must validate source before write"),
            )
            .unwrap()
    }

    #[test]
    fn knowledge_small_project_ignores_oversize_foreign_project_and_keeps_shared_blocking_rules() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let own = ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A policy",
            KnowledgeGovernanceLevel::Required,
        );
        let foreign = ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("B".into()),
            "B policy",
            KnowledgeGovernanceLevel::Advisory,
        );
        let shared = ingest_query_rule(
            &fabric,
            KnowledgeNamespace::SharedLibrary("global".into()),
            "shared safety",
            KnowledgeGovernanceLevel::Blocking,
        );
        let mut oversized = foreign.pack.clone();
        oversized.name = "x".repeat(KNOWLEDGE_READ_MAX_BYTES + 1);
        store
            .commit_maintenance(&store.maintenance_inputs().unwrap(), &[oversized], &[])
            .unwrap();
        let (plan, canon, warnings) = fabric
            .activate("s", "validate source", "main", Some("A"))
            .unwrap();
        assert!(plan.active_pack_ids.contains(&own.pack.pack_id));
        assert!(plan.active_pack_ids.contains(&shared.pack.pack_id));
        assert!(!plan.active_pack_ids.contains(&foreign.pack.pack_id));
        assert!(canon.iter().any(|item| item.pack_id == shared.pack.pack_id));
        assert!(warnings
            .iter()
            .any(|item| item.pack_id == shared.pack.pack_id
                && item.level == KnowledgeGovernanceLevel::Blocking));
        assert!(
            matches!(
                fabric.activate("s", "validate source", "main", Some("B")),
                Err(KnowledgeStoreError::ReadLimit)
            ),
            "an authorized oversized required set is refused, never silently truncated"
        );
    }

    #[test]
    fn knowledge_foreign_project_row_growth_does_not_block_current_scope() {
        let fabric = KnowledgeFabric::new();
        let own = ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A policy",
            KnowledgeGovernanceLevel::Required,
        );
        for i in 0..=(KNOWLEDGE_READ_MAX_ROWS / 2) {
            ingest_query_rule(
                &fabric,
                KnowledgeNamespace::Project("B".into()),
                &format!("B policy {i}"),
                KnowledgeGovernanceLevel::Required,
            );
        }
        let plan = fabric
            .activate("s", "validate source", "main", Some("A"))
            .unwrap()
            .0;
        assert_eq!(plan.active_pack_ids, vec![own.pack.pack_id]);
        assert!(matches!(
            fabric.activate("s", "validate source", "main", Some("B")),
            Err(KnowledgeStoreError::ReadLimit)
        ));
    }

    #[test]
    fn knowledge_continuation_binds_original_scope_and_revisions_without_foreign_starvation() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A first",
            KnowledgeGovernanceLevel::Required,
        );
        ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A second",
            KnowledgeGovernanceLevel::Required,
        );
        ingest_query_rule(
            &fabric,
            KnowledgeNamespace::SharedLibrary("global".into()),
            "shared first",
            KnowledgeGovernanceLevel::Blocking,
        );
        let scope = KnowledgeCandidateScope::Activation {
            project_id: Some("A".into()),
        };
        let first = store
            .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
            .unwrap();
        let cursor = first
            .next
            .as_ref()
            .expect("a genuine continuation must be exercised");
        ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("B".into()),
            "B late",
            KnowledgeGovernanceLevel::Required,
        );
        // An idempotent original receipt is not a new source mutation either.
        ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A first",
            KnowledgeGovernanceLevel::Required,
        );
        let second = store
            .read_candidate_page(&scope, Some(cursor), 1, KNOWLEDGE_READ_MAX_BYTES)
            .unwrap();
        assert_eq!(second.revision, first.revision);
        assert_eq!(second.packs.len(), 1);
        assert_ne!(first.packs[0].pack_id, second.packs[0].pack_id);
        let foreign = KnowledgeCandidateScope::Activation {
            project_id: Some("B".into()),
        };
        assert!(matches!(
            store.read_candidate_page(&foreign, Some(cursor), 1, KNOWLEDGE_READ_MAX_BYTES),
            Err(KnowledgeStoreError::StaleRead)
        ));
        ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A late",
            KnowledgeGovernanceLevel::Required,
        );
        assert!(matches!(
            store.read_candidate_page(&scope, Some(cursor), 1, KNOWLEDGE_READ_MAX_BYTES),
            Err(KnowledgeStoreError::StaleRead)
        ));
        let refreshed = store
            .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
            .unwrap();
        ingest_query_rule(
            &fabric,
            KnowledgeNamespace::SharedLibrary("global".into()),
            "shared late",
            KnowledgeGovernanceLevel::Blocking,
        );
        assert!(matches!(
            store.read_candidate_page(&scope, refreshed.next.as_ref(), 1, KNOWLEDGE_READ_MAX_BYTES),
            Err(KnowledgeStoreError::StaleRead)
        ));
    }

    #[test]
    fn knowledge_candidate_cannot_borrow_foreign_canonical_identity() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let own = ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("A".into()),
            "A policy",
            KnowledgeGovernanceLevel::Required,
        );
        let foreign = ingest_query_rule(
            &fabric,
            KnowledgeNamespace::Project("B".into()),
            "B policy",
            KnowledgeGovernanceLevel::Blocking,
        );
        let mut wrong = own.pack;
        wrong.canon_pack_ref = Some(foreign.canon.canon_id);
        store
            .commit_maintenance(&store.maintenance_inputs().unwrap(), &[wrong], &[])
            .unwrap();
        assert!(fabric
            .activate("s", "validate source", "main", Some("A"))
            .is_err());
    }
    #[test]
    fn knowledge_continuation_cannot_borrow_another_store_with_equal_counters() {
        let first = Arc::new(InMemoryKnowledgeStore::new());
        let second = Arc::new(InMemoryKnowledgeStore::new());
        for store in [&first, &second] {
            let fabric = KnowledgeFabric::with_store(store.clone());
            ingest_query_rule(
                &fabric,
                KnowledgeNamespace::Project("A".into()),
                "A first",
                KnowledgeGovernanceLevel::Required,
            );
            ingest_query_rule(
                &fabric,
                KnowledgeNamespace::Project("A".into()),
                "A second",
                KnowledgeGovernanceLevel::Required,
            );
        }
        let scope = KnowledgeCandidateScope::Activation {
            project_id: Some("A".into()),
        };
        let a = first
            .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
            .unwrap();
        let b = second
            .read_candidate_page(&scope, None, 1, KNOWLEDGE_READ_MAX_BYTES)
            .unwrap();
        assert_eq!(a.revision.counters, b.revision.counters);
        assert_ne!(a.revision.store_id, b.revision.store_id);
        assert!(a.next.is_some());
        assert!(b.next.is_some());
        assert!(matches!(
            second.read_candidate_page(&scope, a.next.as_ref(), 1, KNOWLEDGE_READ_MAX_BYTES),
            Err(KnowledgeStoreError::StaleRead)
        ));
        assert!(second
            .read_candidate_page(&scope, b.next.as_ref(), 1, KNOWLEDGE_READ_MAX_BYTES)
            .is_ok());
    }
}
