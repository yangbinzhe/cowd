//! Universal knowledge fabric contracts.
//!
//! These are pure DTOs shared by memory, runtime, gateway, and eval. They do
//! not own storage, provider calls, or gateway policy decisions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::KernelRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum KnowledgeNamespace {
    Corpus(String),
    SharedLibrary(String),
    Project(String),
    ProjectGroup(String),
    Domain(String),
    User(String),
}

impl KnowledgeNamespace {
    #[must_use]
    pub fn key(&self) -> String {
        match self {
            Self::Corpus(id) => format!("corpus:{id}"),
            Self::SharedLibrary(id) => format!("shared:{id}"),
            Self::Project(id) => format!("project:{id}"),
            Self::ProjectGroup(id) => format!("project_group:{id}"),
            Self::Domain(id) => format!("domain:{id}"),
            Self::User(id) => format!("user:{id}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgePackKind {
    DomainDefault,
    EnterprisePolicy,
    ProjectFoundation,
    TechnicalStandard,
    ProcedureManual,
    GlossaryOntology,
    CaseBase,
    ReferenceLibrary,
    TrainingMaterial,
    PersonalPrinciple,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeObjectState {
    RawIngested,
    Indexed,
    Classified,
    Canonized,
    Active,
    Candidate,
    Conflicted,
    Superseded,
    Deprecated,
    Quarantined,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeActivationPolicy {
    ExplicitOnly,
    OnDemand,
    DefaultForDomain,
    DefaultForProjectGroup,
    DefaultForIntent,
    DefaultForRole,
    DefaultForUser,
    BlockingPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeGovernanceLevel {
    Advisory,
    Required,
    Blocking,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeCorpus {
    pub corpus_id: String,
    pub name: String,
    pub namespace: KnowledgeNamespace,
    pub source_ref: KernelRef,
    pub source_hash: String,
    pub state: KnowledgeObjectState,
    pub chunk_count: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeCanonRule {
    pub rule_id: String,
    pub summary: String,
    pub governance_level: KnowledgeGovernanceLevel,
    pub applies_to: Vec<String>,
    pub evidence_refs: Vec<KernelRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeCanonPack {
    pub canon_id: String,
    pub pack_id: String,
    pub summary: String,
    pub rules: Vec<KnowledgeCanonRule>,
    pub glossary: Vec<String>,
    pub procedures: Vec<String>,
    pub evidence_refs: Vec<KernelRef>,
    pub token_estimate: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgePack {
    pub pack_id: String,
    pub name: String,
    pub kind: KnowledgePackKind,
    pub namespace: KnowledgeNamespace,
    pub activation_policy: KnowledgeActivationPolicy,
    pub governance_level: KnowledgeGovernanceLevel,
    pub source_corpus_refs: Vec<String>,
    pub canon_pack_ref: Option<String>,
    pub graph_ref: Option<String>,
    pub matrix_refs: Vec<KernelRef>,
    pub memory_refs: Vec<KernelRef>,
    pub evidence_refs: Vec<KernelRef>,
    pub version: String,
    pub state: KnowledgeObjectState,
    pub health_score_bp: u16,
    pub owner: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeConflictRecord {
    pub conflict_id: String,
    pub pack_id: Option<String>,
    pub conflict_type: String,
    pub summary: String,
    pub left_ref: KernelRef,
    pub right_ref: KernelRef,
    pub decision: Option<String>,
    pub state: KnowledgeObjectState,
    pub evidence_refs: Vec<KernelRef>,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeActivationPlan {
    pub plan_id: String,
    pub session_id: String,
    pub intent: String,
    pub profile: String,
    pub selected_namespaces: Vec<KnowledgeNamespace>,
    pub blocked_namespaces: Vec<String>,
    pub active_pack_ids: Vec<String>,
    pub canon_refs: Vec<String>,
    pub evidence_refs: Vec<KernelRef>,
    pub reasons: Vec<String>,
    pub token_estimate: u64,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeComplianceWarning {
    pub warning_id: String,
    pub pack_id: String,
    pub rule_id: Option<String>,
    pub level: KnowledgeGovernanceLevel,
    pub summary: String,
    pub evidence_refs: Vec<KernelRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeUsageSignal {
    pub signal_id: String,
    pub session_id: String,
    pub pack_id: String,
    pub action: String,
    pub summary: String,
    pub score_delta_bp: i16,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KnowledgeTurnReport {
    pub activation_plan_id: Option<String>,
    pub active_pack_ids: Vec<String>,
    pub blocked_namespaces: Vec<String>,
    pub compliance_warnings: Vec<KnowledgeComplianceWarning>,
    pub evidence_refs: Vec<KernelRef>,
    pub usage_signals: Vec<KnowledgeUsageSignal>,
}

#[must_use]
pub fn estimate_tokens(content: &str) -> u64 {
    (content.chars().count() as u64).div_ceil(4).max(1)
}
