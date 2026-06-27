//! Universal knowledge fabric for large corpus governance.
//!
//! This module owns unstructured corpus metadata, canon packs, activation
//! policy, and usage signals. Runtime consumes its activation result; gateway
//! exposes projections; matrix stores the structured facts derived from it.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use chrono::Utc;
use harness_contract::core::KernelRef;
use harness_contract::knowledge::{
    estimate_tokens, KnowledgeActivationPlan, KnowledgeActivationPolicy, KnowledgeCanonPack,
    KnowledgeCanonRule, KnowledgeComplianceWarning, KnowledgeConflictRecord, KnowledgeCorpus,
    KnowledgeGovernanceLevel, KnowledgeNamespace, KnowledgeObjectState, KnowledgePack,
    KnowledgePackKind, KnowledgeTurnReport, KnowledgeUsageSignal,
};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Default)]
struct KnowledgeFabricState {
    corpus: BTreeMap<String, KnowledgeCorpus>,
    packs: BTreeMap<String, KnowledgePack>,
    canon: BTreeMap<String, KnowledgeCanonPack>,
    conflicts: BTreeMap<String, KnowledgeConflictRecord>,
    chunks: BTreeMap<String, KnowledgeChunk>,
    usage: Vec<KnowledgeUsageSignal>,
}

#[derive(Debug, Clone, Default)]
pub struct KnowledgeFabric {
    state: Arc<RwLock<KnowledgeFabricState>>,
}

impl KnowledgeFabric {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ingest_document(
        &self,
        namespace: KnowledgeNamespace,
        activation_policy: KnowledgeActivationPolicy,
        governance_level: KnowledgeGovernanceLevel,
        content: DocumentContent,
    ) -> KnowledgeIngestionReceipt {
        let classification = DocumentClassifier::new().classify(&content);
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
        let canon = build_canon_pack(
            &pack_id,
            &classification,
            &content,
            &chunks,
            governance_level,
        );
        let conflicts = detect_conflicts(&pack_id, &canon);
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
            matrix_refs: knowledge_matrix_refs(&pack_id, &canon),
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
            .push(signal);
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
            "corpus": state.corpus.values().collect::<Vec<_>>(),
            "packs": state.packs.values().collect::<Vec<_>>(),
            "canon": state.canon.values().collect::<Vec<_>>(),
            "conflicts": state.conflicts.values().collect::<Vec<_>>(),
            "usage_signal_count": state.usage.len(),
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

fn knowledge_matrix_refs(pack_id: &str, canon: &KnowledgeCanonPack) -> Vec<KernelRef> {
    canon
        .rules
        .iter()
        .map(|rule| {
            KernelRef::new(
                "matrix_fact",
                format!("knowledge-rule:{pack_id}:{}", rule.rule_id),
            )
        })
        .collect()
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
    }
}
