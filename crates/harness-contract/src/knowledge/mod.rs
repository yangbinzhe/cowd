//! Universal knowledge fabric contracts.
//!
//! These are pure DTOs shared by memory, runtime, gateway, and eval. They do
//! not own storage, provider calls, or gateway policy decisions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    core::{KernelRef, TaskRisk},
    execution::ExecutionIdentity,
    reality::{EvidenceRef, RealityBoundary},
};

/// Visibility and write boundary for runtime-produced knowledge.
///
/// The scope is explicit and independent from the physical Memory backend.
/// Runtime may convert it to a storage-specific scope only after governance
/// has accepted the candidate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum KnowledgeCandidateScope {
    AgentPrivate(String),
    Team(String),
    Workspace(String),
    Global,
}

impl KnowledgeCandidateScope {
    #[must_use]
    pub fn key(&self) -> String {
        match self {
            Self::AgentPrivate(id) => format!("agent:{id}"),
            Self::Team(id) => format!("team:{id}"),
            Self::Workspace(id) => format!("workspace:{id}"),
            Self::Global => "global".to_string(),
        }
    }

    #[must_use]
    pub const fn requires_approval(&self) -> bool {
        !matches!(self, Self::AgentPrivate(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeAuthority {
    AgentObservation,
    TeamSynthesis,
    WorkspaceVerified,
    HumanApproved,
    SystemPolicy,
}

impl KnowledgeAuthority {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::AgentObservation => 1,
            Self::TeamSynthesis => 2,
            Self::WorkspaceVerified => 3,
            Self::HumanApproved => 4,
            Self::SystemPolicy => 5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeNovelty {
    New,
    Reinforces,
    Duplicate,
    Conflicts,
    Supersedes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeCandidateState {
    Proposed,
    Validated,
    AwaitingApproval,
    Approved,
    Blocked,
    Promoted,
    Rejected,
    Superseded,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KnowledgeLineage {
    #[serde(default)]
    pub parent_candidate_ids: Vec<String>,
    #[serde(default)]
    pub source_refs: Vec<EvidenceRef>,
}

/// Canonical runtime-to-knowledge handoff.
///
/// Agent and Team execution may only emit this immutable candidate. The
/// governed L4 promotion service remains the only writer allowed to turn it
/// into shared durable Memory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeCandidate {
    pub candidate_id: String,
    pub execution_identity: ExecutionIdentity,
    pub scope: KnowledgeCandidateScope,
    pub title: String,
    pub claim: String,
    pub evidence_refs: Vec<EvidenceRef>,
    pub authority: KnowledgeAuthority,
    pub lineage: KnowledgeLineage,
    pub novelty: KnowledgeNovelty,
    pub risk: TaskRisk,
    #[serde(default)]
    pub tags: Vec<String>,
    pub producer: String,
    pub producer_version: String,
    pub created_at_ms: u64,
}

impl KnowledgeCandidate {
    pub fn validate(&self) -> Result<(), String> {
        self.execution_identity
            .validate()
            .map_err(|error| error.to_string())?;
        for (field, value) in [
            ("candidate_id", self.candidate_id.as_str()),
            ("title", self.title.as_str()),
            ("claim", self.claim.as_str()),
            ("producer", self.producer.as_str()),
            ("producer_version", self.producer_version.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("knowledge candidate requires {field}"));
            }
        }
        if self.evidence_refs.is_empty() {
            return Err("knowledge candidate requires evidence".to_string());
        }
        if self.evidence_refs.iter().any(|evidence| {
            evidence.ref_type.trim().is_empty()
                || evidence.id.trim().is_empty()
                || !evidence.boundary.can_be_authoritative()
        }) {
            return Err(
                "knowledge candidate evidence must be authoritative and addressable".to_string(),
            );
        }
        if self.evidence_refs.iter().any(|evidence| {
            evidence.boundary == RealityBoundary::Inferred
                && evidence.confidence_bp.unwrap_or_default() < 7_000
        }) {
            return Err(
                "inferred knowledge candidate evidence requires confidence_bp >= 7000".to_string(),
            );
        }
        match &self.scope {
            KnowledgeCandidateScope::AgentPrivate(agent_id)
                if self.execution_identity.agent_run_id() != Some(agent_id.as_str())
                    || agent_id.trim().is_empty() =>
            {
                return Err(
                    "agent-private candidate requires an agent execution identity".to_string(),
                );
            }
            KnowledgeCandidateScope::Team(team_id)
                if self.execution_identity.team_run_id() != Some(team_id.as_str()) =>
            {
                return Err("team candidate scope does not match execution identity".to_string());
            }
            KnowledgeCandidateScope::Workspace(workspace_id)
                if self.execution_identity.workspace_id() != workspace_id =>
            {
                return Err(
                    "workspace candidate scope does not match execution identity".to_string(),
                );
            }
            _ => {}
        }
        Ok(())
    }
}

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
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeCanonPack {
    pub canon_id: String,
    pub pack_id: String,
    pub summary: String,
    pub rules: Vec<KnowledgeCanonRule>,
    pub glossary: Vec<String>,
    pub procedures: Vec<String>,
    pub evidence_refs: Vec<EvidenceRef>,
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
    pub evidence_refs: Vec<EvidenceRef>,
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
    pub evidence_refs: Vec<EvidenceRef>,
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
    pub evidence_refs: Vec<EvidenceRef>,
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
    pub evidence_refs: Vec<EvidenceRef>,
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
    pub evidence_refs: Vec<EvidenceRef>,
    pub usage_signals: Vec<KnowledgeUsageSignal>,
}

#[must_use]
pub fn estimate_tokens(content: &str) -> u64 {
    (content.chars().count() as u64).div_ceil(4).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> ExecutionIdentity {
        let graph = ExecutionIdentity::for_task_graph(
            "principal",
            "workspace",
            "mission",
            "task",
            "session",
            "turn",
            "graph",
        )
        .unwrap();
        ExecutionIdentity::for_agent_node(&graph, "agent-run", "node").unwrap()
    }

    fn candidate(evidence: EvidenceRef) -> KnowledgeCandidate {
        KnowledgeCandidate {
            candidate_id: "candidate-1".to_string(),
            execution_identity: identity(),
            scope: KnowledgeCandidateScope::AgentPrivate("agent-1".to_string()),
            title: "Verified finding".to_string(),
            claim: "The observed build completed.".to_string(),
            evidence_refs: vec![evidence],
            authority: KnowledgeAuthority::AgentObservation,
            lineage: KnowledgeLineage::default(),
            novelty: KnowledgeNovelty::New,
            risk: TaskRisk::Low,
            tags: vec!["build".to_string()],
            producer: "runtime.agent".to_string(),
            producer_version: "1".to_string(),
            created_at_ms: 1,
        }
    }

    #[test]
    fn observed_candidate_is_valid() {
        assert!(candidate(EvidenceRef::new("tool", "receipt-1"))
            .validate()
            .is_ok());
    }

    #[test]
    fn simulated_candidate_cannot_be_authoritative_knowledge() {
        let error = candidate(
            EvidenceRef::new("simulation", "scenario-1").with_boundary(RealityBoundary::Simulated),
        )
        .validate()
        .unwrap_err();
        assert!(error.contains("authoritative"));
    }

    #[test]
    fn inferred_candidate_requires_explicit_confidence() {
        let error = candidate(
            EvidenceRef::new("analysis", "inference-1")
                .with_boundary(RealityBoundary::Inferred)
                .with_confidence_bp(6_999),
        )
        .validate()
        .unwrap_err();
        assert!(error.contains("confidence_bp"));
    }
}
