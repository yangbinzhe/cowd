//! Universal knowledge fabric for large corpus governance.
//!
//! This module owns unstructured corpus metadata, canon packs, activation
//! policy, and usage signals. Runtime consumes its activation result; gateway
//! exposes projections; matrix stores the structured facts derived from it.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, RwLock};

use chrono::Utc;
use harness_contract::core::KernelRef;
use harness_contract::knowledge::{
    estimate_tokens, KnowledgeActivationPlan, KnowledgeActivationPolicy, KnowledgeCanonPack,
    KnowledgeCanonRule, KnowledgeComplianceWarning, KnowledgeConflictRecord, KnowledgeCorpus,
    KnowledgeGovernanceLevel, KnowledgeNamespace, KnowledgeObjectState, KnowledgePack,
    KnowledgePackKind, KnowledgeTurnReport, KnowledgeUsageSignal,
};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
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
                .filter(|conflict| conflict.decision.is_none())
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
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("backend error: {0}")]
    Backend(String),
}

pub trait KnowledgeStore: Send + Sync {
    fn save_receipt(&self, receipt: &KnowledgeIngestionReceipt) -> Result<(), KnowledgeStoreError>;
    fn record_usage(&self, signal: &KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError>;
    fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeChunk {
    pub chunk_id: String,
    pub corpus_id: String,
    pub ordinal: usize,
    pub title: String,
    pub text: String,
    pub evidence_ref: KernelRef,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeNamespaceSearchResult {
    pub namespace: KnowledgeNamespace,
    pub query: String,
    pub packs: Vec<KnowledgePack>,
    pub canon: Vec<KnowledgeCanonPack>,
    pub blocked_namespaces: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeMatrixBridgeFact {
    pub fact_id: String,
    pub fact_type: String,
    pub summary: String,
    pub source_ref: String,
    pub confidence: f32,
    pub evidence_refs: Vec<KernelRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeMatrixBridgeRelation {
    pub relation_id: String,
    pub relation_type: String,
    pub from_ref: String,
    pub to_ref: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeMatrixBridgeInput {
    pub source_pack_id: String,
    pub source_name: String,
    pub pack_id: String,
    pub facts: Vec<KnowledgeMatrixBridgeFact>,
    pub relations: Vec<KnowledgeMatrixBridgeRelation>,
}

#[derive(Debug, Default)]
struct KnowledgeFabricState {
    corpus: BTreeMap<String, KnowledgeCorpus>,
    packs: BTreeMap<String, KnowledgePack>,
    canon: BTreeMap<String, KnowledgeCanonPack>,
    conflicts: BTreeMap<String, KnowledgeConflictRecord>,
    chunks: BTreeMap<String, KnowledgeChunk>,
    usage: Vec<KnowledgeUsageSignal>,
}

#[derive(Clone, Default)]
pub struct KnowledgeFabric {
    state: Arc<RwLock<KnowledgeFabricState>>,
    store: Option<Arc<dyn KnowledgeStore>>,
}

impl KnowledgeFabric {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_store(store: Arc<dyn KnowledgeStore>) -> Self {
        let fabric = Self {
            state: Arc::new(RwLock::new(KnowledgeFabricState::default())),
            store: Some(store),
        };
        fabric.reload_from_store();
        fabric
    }

    pub fn reload_from_store(&self) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let Ok(snapshot) = store.snapshot() else {
            return;
        };
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = KnowledgeFabricState {
            corpus: snapshot
                .corpus
                .into_iter()
                .map(|item| (item.corpus_id.clone(), item))
                .collect(),
            packs: snapshot
                .packs
                .into_iter()
                .map(|item| (item.pack_id.clone(), item))
                .collect(),
            canon: snapshot
                .canon
                .into_iter()
                .map(|item| (item.canon_id.clone(), item))
                .collect(),
            conflicts: snapshot
                .conflicts
                .into_iter()
                .map(|item| (item.conflict_id.clone(), item))
                .collect(),
            chunks: snapshot
                .chunks
                .into_iter()
                .map(|item| (item.chunk_id.clone(), item))
                .collect(),
            usage: snapshot.usage,
        };
    }

    pub fn ingest_document(
        &self,
        namespace: KnowledgeNamespace,
        activation_policy: KnowledgeActivationPolicy,
        governance_level: KnowledgeGovernanceLevel,
        content: DocumentContent,
    ) -> KnowledgeIngestionReceipt {
        let classification = KnowledgeIngestionService::new().classify(&content);
        let now = Utc::now();
        let corpus_id = format!(
            "corpus-{}",
            stable_hash(&format!(
                "{}:{}",
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
        let pack_id = format!(
            "pack-{}",
            stable_hash(&format!("{}:{}", namespace.key(), content.title))
        );
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
            matrix_refs: KnowledgeGraphBuilder::new()
                .build_bridge(&pack_id, &classification.metadata.title, &canon)
                .facts
                .iter()
                .map(|fact| KernelRef::new("matrix_fact", fact.fact_id.clone()))
                .collect(),
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
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.corpus.insert(corpus_id, corpus.clone());
        state.packs.insert(pack_id, pack.clone());
        state.canon.insert(canon.canon_id.clone(), canon.clone());
        for chunk in &chunks {
            state.chunks.insert(chunk.chunk_id.clone(), chunk.clone());
        }
        for conflict in &conflicts {
            state
                .conflicts
                .insert(conflict.conflict_id.clone(), conflict.clone());
        }
        let mut receipt = KnowledgeIngestionReceipt {
            corpus,
            pack,
            canon,
            conflicts,
            chunks,
            warnings: classification.reasoning,
        };
        if let Some(store) = self.store.as_ref() {
            if let Err(err) = store.save_receipt(&receipt) {
                receipt
                    .warnings
                    .push(format!("knowledge store persist failed: {err}"));
            }
        }
        receipt
    }

    #[must_use]
    pub fn activate(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        project_id: Option<&str>,
    ) -> (
        KnowledgeActivationPlan,
        Vec<KnowledgeCanonPack>,
        Vec<KnowledgeComplianceWarning>,
    ) {
        ActivationGovernor::new().activate(self, session_id, intent, profile, project_id)
    }

    #[must_use]
    pub fn search_namespace(
        &self,
        namespace: &KnowledgeNamespace,
        query: &str,
    ) -> KnowledgeNamespaceSearchResult {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        KnowledgeNamespaceSearchResult {
            namespace: namespace.clone(),
            query: query.to_string(),
            packs,
            canon,
            blocked_namespaces,
        }
    }

    #[must_use]
    pub fn matrix_bridge_for_pack(&self, pack_id: &str) -> Option<KnowledgeMatrixBridgeInput> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pack = state.packs.get(pack_id)?;
        let canon = pack
            .canon_pack_ref
            .as_ref()
            .and_then(|canon_id| state.canon.get(canon_id))?;
        Some(KnowledgeGraphBuilder::new().build_bridge(pack_id, &pack.name, canon))
    }

    #[must_use]
    pub fn snapshot(&self) -> KnowledgeSnapshot {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        KnowledgeSnapshot {
            corpus: state.corpus.values().cloned().collect(),
            packs: state.packs.values().cloned().collect(),
            canon: state.canon.values().cloned().collect(),
            conflicts: state.conflicts.values().cloned().collect(),
            chunks: state.chunks.values().cloned().collect(),
            usage: state.usage.clone(),
        }
    }

    fn activate_inner(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        project_id: Option<&str>,
    ) -> (
        KnowledgeActivationPlan,
        Vec<KnowledgeCanonPack>,
        Vec<KnowledgeComplianceWarning>,
    ) {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let intent_lc = intent.to_ascii_lowercase();
        let mut selected_namespaces = Vec::new();
        let mut blocked_namespaces = Vec::new();
        let mut active_pack_ids = Vec::new();
        let mut canon_refs = Vec::new();
        let mut evidence_refs = Vec::new();
        let mut reasons = Vec::new();
        let mut canon_packs = Vec::new();
        let mut warnings = Vec::new();

        for pack in state.packs.values() {
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
                if let Some(canon) = state.canon.get(canon_id) {
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
        (plan, canon_packs, warnings)
    }

    pub fn record_usage(&self, signal: KnowledgeUsageSignal) {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .usage
            .push(signal.clone());
        UsageFeedbackLoop::new().record(self.store.as_deref(), &signal);
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
    pub fn projection(&self) -> serde_json::Value {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        serde_json::json!({
            "kind": "memory.knowledge_fabric",
            "health": health_from_state(&state),
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
            "usage_signal_count": state.usage.len(),
        })
    }
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

    #[must_use]
    pub fn ingest_collection(
        &self,
        fabric: &KnowledgeFabric,
        namespace: KnowledgeNamespace,
        activation_policy: KnowledgeActivationPolicy,
        governance_level: KnowledgeGovernanceLevel,
        documents: Vec<DocumentContent>,
    ) -> Vec<KnowledgeIngestionReceipt> {
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
pub struct KnowledgeGraphBuilder;

impl KnowledgeGraphBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn build_bridge(
        &self,
        pack_id: &str,
        pack_name: &str,
        canon: &KnowledgeCanonPack,
    ) -> KnowledgeMatrixBridgeInput {
        let source_pack_id = format!("knowledge-source-{pack_id}");
        let mut facts = Vec::new();
        let mut relations = Vec::new();
        for rule in &canon.rules {
            let fact_type = match rule.governance_level {
                KnowledgeGovernanceLevel::Advisory => "knowledge_canon_rule",
                KnowledgeGovernanceLevel::Required => "knowledge_constraint",
                KnowledgeGovernanceLevel::Blocking => "knowledge_constraint",
            };
            let fact_id = format!("knowledge-rule:{pack_id}:{}", rule.rule_id);
            facts.push(KnowledgeMatrixBridgeFact {
                fact_id: fact_id.clone(),
                fact_type: fact_type.to_string(),
                summary: rule.summary.clone(),
                source_ref: source_pack_id.clone(),
                confidence: match rule.governance_level {
                    KnowledgeGovernanceLevel::Advisory => 0.74,
                    KnowledgeGovernanceLevel::Required => 0.88,
                    KnowledgeGovernanceLevel::Blocking => 0.95,
                },
                evidence_refs: rule.evidence_refs.clone(),
            });
            relations.push(KnowledgeMatrixBridgeRelation {
                relation_id: format!("knowledge-pack-rule:{pack_id}:{}", rule.rule_id),
                relation_type: "pack_contains_rule".to_string(),
                from_ref: pack_id.to_string(),
                to_ref: fact_id,
                confidence: 0.9,
            });
        }
        for (idx, procedure) in canon.procedures.iter().enumerate() {
            facts.push(KnowledgeMatrixBridgeFact {
                fact_id: format!("knowledge-procedure:{pack_id}:{idx}"),
                fact_type: "knowledge_process_step".to_string(),
                summary: procedure.clone(),
                source_ref: source_pack_id.clone(),
                confidence: 0.8,
                evidence_refs: canon.evidence_refs.clone(),
            });
        }
        KnowledgeMatrixBridgeInput {
            source_pack_id,
            source_name: format!("Knowledge Fabric: {pack_name}"),
            pack_id: pack_id.to_string(),
            facts,
            relations,
        }
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
    ) -> (
        KnowledgeActivationPlan,
        Vec<KnowledgeCanonPack>,
        Vec<KnowledgeComplianceWarning>,
    ) {
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

    pub fn record(&self, store: Option<&dyn KnowledgeStore>, signal: &KnowledgeUsageSignal) {
        if let Some(store) = store {
            let _ = store.record_usage(signal);
        }
    }
}

impl std::fmt::Debug for KnowledgeFabric {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KnowledgeFabric")
            .field("has_store", &self.store.is_some())
            .field("health", &self.snapshot().health())
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryKnowledgeStore {
    state: Arc<RwLock<KnowledgeFabricState>>,
}

impl InMemoryKnowledgeStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl KnowledgeStore for InMemoryKnowledgeStore {
    fn save_receipt(&self, receipt: &KnowledgeIngestionReceipt) -> Result<(), KnowledgeStoreError> {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        Ok(())
    }

    fn record_usage(&self, signal: &KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .usage
            .push(signal.clone());
        Ok(())
    }

    fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(KnowledgeSnapshot {
            corpus: state.corpus.values().cloned().collect(),
            packs: state.packs.values().cloned().collect(),
            canon: state.canon.values().cloned().collect(),
            conflicts: state.conflicts.values().cloned().collect(),
            chunks: state.chunks.values().cloned().collect(),
            usage: state.usage.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SqliteKnowledgeStore {
    db_path: Arc<std::path::PathBuf>,
}

impl SqliteKnowledgeStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KnowledgeStoreError> {
        let store = Self {
            db_path: Arc::new(path.as_ref().to_path_buf()),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    fn connection(&self) -> Result<Connection, KnowledgeStoreError> {
        Ok(Connection::open(self.db_path.as_path())?)
    }

    fn ensure_schema(&self) -> Result<(), KnowledgeStoreError> {
        let conn = self.connection()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS knowledge_corpus (
                corpus_id TEXT PRIMARY KEY,
                namespace_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS knowledge_pack (
                pack_id TEXT PRIMARY KEY,
                namespace_key TEXT NOT NULL,
                state TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS knowledge_canon (
                canon_id TEXT PRIMARY KEY,
                pack_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS knowledge_conflict (
                conflict_id TEXT PRIMARY KEY,
                pack_id TEXT,
                state TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                detected_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS knowledge_chunk (
                chunk_id TEXT PRIMARY KEY,
                corpus_id TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS knowledge_usage (
                signal_id TEXT PRIMARY KEY,
                pack_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            );
            "#,
        )?;
        Ok(())
    }
}

pub fn durable_knowledge_fabric_for_config_home(
    config_home: impl AsRef<Path>,
) -> Result<KnowledgeFabric, KnowledgeStoreError> {
    let registry = storage::StorageRegistry::default_for_config_home(config_home);
    let db_path = registry
        .endpoint(&storage::StorageDomainId::Knowledge)?
        .as_handle()
        .path;
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let store = Arc::new(SqliteKnowledgeStore::open(db_path)?);
    Ok(KnowledgeFabric::with_store(store))
}

impl KnowledgeStore for SqliteKnowledgeStore {
    fn save_receipt(&self, receipt: &KnowledgeIngestionReceipt) -> Result<(), KnowledgeStoreError> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO knowledge_corpus (corpus_id, namespace_key, payload_json, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![
                receipt.corpus.corpus_id,
                receipt.corpus.namespace.key(),
                serde_json::to_string(&receipt.corpus)?,
                receipt.corpus.updated_at.to_rfc3339(),
            ],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO knowledge_pack (pack_id, namespace_key, state, payload_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                receipt.pack.pack_id,
                receipt.pack.namespace.key(),
                format!("{:?}", receipt.pack.state),
                serde_json::to_string(&receipt.pack)?,
                receipt.pack.updated_at.to_rfc3339(),
            ],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO knowledge_canon (canon_id, pack_id, payload_json, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![
                receipt.canon.canon_id,
                receipt.canon.pack_id,
                serde_json::to_string(&receipt.canon)?,
                receipt.canon.updated_at.to_rfc3339(),
            ],
        )?;
        for conflict in &receipt.conflicts {
            conn.execute(
                "INSERT OR REPLACE INTO knowledge_conflict (conflict_id, pack_id, state, payload_json, detected_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    conflict.conflict_id,
                    conflict.pack_id,
                    format!("{:?}", conflict.state),
                    serde_json::to_string(conflict)?,
                    conflict.detected_at.to_rfc3339(),
                ],
            )?;
        }
        for chunk in &receipt.chunks {
            conn.execute(
                "INSERT OR REPLACE INTO knowledge_chunk (chunk_id, corpus_id, payload_json) VALUES (?1, ?2, ?3)",
                params![chunk.chunk_id, chunk.corpus_id, serde_json::to_string(chunk)?],
            )?;
        }
        Ok(())
    }

    fn record_usage(&self, signal: &KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO knowledge_usage (signal_id, pack_id, session_id, payload_json, occurred_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                signal.signal_id,
                signal.pack_id,
                signal.session_id,
                serde_json::to_string(signal)?,
                signal.occurred_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
        fn load_json<T: for<'de> Deserialize<'de>>(
            conn: &Connection,
            table: &str,
        ) -> Result<Vec<T>, KnowledgeStoreError> {
            let mut stmt = conn.prepare(&format!("SELECT payload_json FROM {table}"))?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut values = Vec::new();
            for row in rows {
                values.push(serde_json::from_str(&row?)?);
            }
            Ok(values)
        }
        let conn = self.connection()?;
        Ok(KnowledgeSnapshot {
            corpus: load_json(&conn, "knowledge_corpus")?,
            packs: load_json(&conn, "knowledge_pack")?,
            canon: load_json(&conn, "knowledge_canon")?,
            conflicts: load_json(&conn, "knowledge_conflict")?,
            chunks: load_json(&conn, "knowledge_chunk")?,
            usage: load_json(&conn, "knowledge_usage")?,
        })
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
        evidence_ref: KernelRef::new("knowledge_chunk", chunk_id)
            .with_label(source.clone().unwrap_or_else(|| title.to_string())),
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
            let left_lc = left.summary.to_ascii_lowercase();
            let right_lc = right.summary.to_ascii_lowercase();
            let contradictory = (left_lc.contains("must") && right_lc.contains("must not"))
                || (left_lc.contains("必须") && right_lc.contains("不得"))
                || (left_lc.contains("禁止") && right_lc.contains("必须"));
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

fn activation_fits(pack: &KnowledgePack, intent_lc: &str, project_id: Option<&str>) -> bool {
    match &pack.activation_policy {
        KnowledgeActivationPolicy::ExplicitOnly => false,
        KnowledgeActivationPolicy::DefaultForProjectGroup => {
            project_id.is_some_and(|project| pack.namespace.key().contains(project))
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
            .filter(|conflict| conflict.decision.is_none())
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
        .filter(|conflict| conflict.decision.is_none())
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
        .filter(|item| item.decision.is_none())
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
    let mut source_hashes: BTreeMap<String, Vec<&KnowledgeCorpus>> = BTreeMap::new();
    for corpus in state.corpus.values() {
        source_hashes
            .entry(corpus.source_hash.clone())
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
    for pack in state.packs.values() {
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
        if pack.state == KnowledgeObjectState::Quarantined {
            candidates.push(serde_json::json!({
                "id": format!("knowledge-maintenance:quarantine:{}", pack.pack_id),
                "kind": "quarantined_item_review_candidate",
                "status": "pending",
                "severity": "high",
                "pack_id": pack.pack_id,
                "namespace": normalize_namespace_key(&pack.namespace),
                "reason": "pack is quarantined and cannot enter runtime context",
                "action": "review_quarantine",
            }));
        }
    }
    candidates
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
        "conflict_warnings": state.conflicts.values().filter(|item| item.decision.is_none()).map(|item| item.summary.as_str()).collect::<Vec<_>>(),
        "policy": "project scoped knowledge stays out of unrelated projects; global/domain knowledge enters body only when required, blocking, or relevant, otherwise it is kept as pointer/governance evidence",
    })
}

fn normalize_namespace_key(namespace: &KnowledgeNamespace) -> String {
    match namespace {
        KnowledgeNamespace::SharedLibrary(id) if id == "global" => "global".to_string(),
        other => other.key(),
    }
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
        );

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
    fn activation_selects_default_pack_and_reports_required_rules() {
        let fabric = KnowledgeFabric::new();
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::Domain("architecture".to_string()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new(
                "Architecture Rules",
                "must retain evidence before final answer",
            ),
        );
        let (plan, _canon, warnings) =
            fabric.activate("s1", "architecture review", "DeepInvestigation", None);

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
        let project_receipt = fabric.ingest_document(
            KnowledgeNamespace::Project("cowd".to_string()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new("Cowd Runtime Rules", "must record context turn report"),
        );
        let unrelated_receipt = fabric.ingest_document(
            KnowledgeNamespace::Project("erp".to_string()),
            KnowledgeActivationPolicy::DefaultForProjectGroup,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new("ERP Billing Rules", "must reconcile invoice ledger"),
        );

        let (plan, _, _) = fabric.activate(
            "s1",
            "context turn report",
            "DeepInvestigation",
            Some("cowd"),
        );

        assert!(plan.active_pack_ids.contains(&project_receipt.pack.pack_id));
        assert!(!plan
            .active_pack_ids
            .contains(&unrelated_receipt.pack.pack_id));
        assert!(plan
            .blocked_namespaces
            .iter()
            .any(|item| item.contains("project:erp")));
    }

    #[test]
    fn universal_knowledge_fabric_records_conflicts_and_matrix_refs() {
        let fabric = KnowledgeFabric::new();
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::SharedLibrary("operations".to_string()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Blocking,
            DocumentContent::new(
                "Operations Contradiction Pack",
                "must keep supplier recovery evidence\nmust not keep supplier recovery evidence",
            ),
        );

        assert!(!receipt.conflicts.is_empty());
        assert!(!receipt.pack.matrix_refs.is_empty());
        let projection = fabric.projection();
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
    fn sqlite_knowledge_store_persists_corpus_pack_canon_conflict_and_usage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(SqliteKnowledgeStore::open(dir.path().join("knowledge.db")).unwrap());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::SharedLibrary("quality".to_string()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Blocking,
            DocumentContent::new(
                "Quality Blocking Rules",
                "must keep inspection evidence\nmust not keep inspection evidence",
            ),
        );
        fabric.record_usage(KnowledgeUsageSignal {
            signal_id: "usage-1".to_string(),
            session_id: "s1".to_string(),
            pack_id: receipt.pack.pack_id.clone(),
            action: "selected".to_string(),
            summary: "selected during test".to_string(),
            score_delta_bp: 5,
            occurred_at: Utc::now(),
        });

        let reloaded = KnowledgeFabric::with_store(store);
        let snapshot = reloaded.snapshot();
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
        let search = fabric.search_namespace(
            &KnowledgeNamespace::Domain("manufacturing".to_string()),
            "manufacturing demand",
        );
        assert_eq!(search.packs.len(), 1);
        let bridge = fabric
            .matrix_bridge_for_pack(&receipts[0].pack.pack_id)
            .expect("matrix bridge");
        assert!(!bridge.facts.is_empty());
        assert!(bridge
            .facts
            .iter()
            .any(|fact| fact.fact_type == "knowledge_constraint"));
    }
}
