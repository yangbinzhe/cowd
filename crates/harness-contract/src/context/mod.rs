//! Context epoch and prompt assembly for Cowd AI work kernel.

pub use crate::core::EvidenceRef;
use crate::core::{AiKernelError, AiKernelResult, KernelRef};
use crate::tool::ToolExposureProjection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceDurability {
    Pending,
    Durable,
    Unavailable,
}

/// Backend-neutral reference to bytes managed by the Runtime artifact plane.
///
/// The selector is the only public locator. Physical paths, database keys, and
/// compact/blob tier choices are adapter-private and must never cross a
/// Runtime or Gateway contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub selector: String,
    pub sha256: String,
    pub bytes: u64,
    pub media_type: String,
    pub durability: EvidenceDurability,
    pub visibility_scope: String,
}

impl ArtifactRef {
    #[must_use]
    pub fn durable(
        selector: impl Into<String>,
        sha256: impl Into<String>,
        bytes: u64,
        media_type: impl Into<String>,
        visibility_scope: impl Into<String>,
    ) -> Self {
        Self {
            selector: selector.into(),
            sha256: sha256.into(),
            bytes,
            media_type: media_type.into(),
            durability: EvidenceDurability::Durable,
            visibility_scope: visibility_scope.into(),
        }
    }

    #[must_use]
    pub const fn is_durable(&self) -> bool {
        matches!(self.durability, EvidenceDurability::Durable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactWriteDescriptor {
    pub media_type: String,
    pub visibility_scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_name: Option<String>,
}

/// Tool output before Runtime publishes its evidence receipt.
///
/// Existing bounded tools can return inline text. Tools that may produce large
/// output publish directly through the Runtime artifact sink and return only a
/// bounded summary plus the durable selector. This keeps large bytes out of
/// the Tool execution result, effect receipt, Session history, and model
/// request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolOutputDraft {
    BoundedInline {
        content: String,
    },
    StagedArtifact {
        summary: String,
        artifact_ref: ArtifactRef,
    },
}

impl ToolOutputDraft {
    #[must_use]
    pub fn bounded_inline(content: impl Into<String>) -> Self {
        Self::BoundedInline {
            content: content.into(),
        }
    }

    #[must_use]
    pub fn staged_artifact(summary: impl Into<String>, artifact_ref: ArtifactRef) -> Self {
        Self::StagedArtifact {
            summary: summary.into(),
            artifact_ref,
        }
    }

    #[must_use]
    pub fn model_text(&self) -> &str {
        match self {
            Self::BoundedInline { content } => content,
            Self::StagedArtifact { summary, .. } => summary,
        }
    }

    #[must_use]
    pub const fn artifact_ref(&self) -> Option<&ArtifactRef> {
        match self {
            Self::BoundedInline { .. } => None,
            Self::StagedArtifact { artifact_ref, .. } => Some(artifact_ref),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutputEnvelope {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_ref: Option<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_ref: Option<EvidenceAccessRef>,
    pub receipt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceAccessRef {
    pub evidence_ref: EvidenceRef,
    pub sha256: String,
    pub bytes: u64,
    pub media_type: String,
    pub durability: EvidenceDurability,
    pub retrieval_selector: String,
    pub visibility_scope: String,
}

impl EvidenceAccessRef {
    #[must_use]
    pub fn durable(
        evidence_ref: EvidenceRef,
        sha256: impl Into<String>,
        bytes: u64,
        media_type: impl Into<String>,
        retrieval_selector: impl Into<String>,
        visibility_scope: impl Into<String>,
    ) -> Self {
        Self {
            evidence_ref,
            sha256: sha256.into(),
            bytes,
            media_type: media_type.into(),
            durability: EvidenceDurability::Durable,
            retrieval_selector: retrieval_selector.into(),
            visibility_scope: visibility_scope.into(),
        }
    }

    #[must_use]
    pub const fn is_durable(&self) -> bool {
        matches!(self.durability, EvidenceDurability::Durable)
    }

    /// A typed, non-retrievable reference for a relationship or execution
    /// marker. It deliberately carries no selector, bytes, or hash and can
    /// never be mistaken for durable raw evidence across a Session boundary.
    #[must_use]
    pub fn unavailable(
        evidence_ref: EvidenceRef,
        media_type: impl Into<String>,
        visibility_scope: impl Into<String>,
    ) -> Self {
        Self {
            evidence_ref,
            sha256: String::new(),
            bytes: 0,
            media_type: media_type.into(),
            durability: EvidenceDurability::Unavailable,
            retrieval_selector: String::new(),
            visibility_scope: visibility_scope.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudgetLeaseRef {
    pub lease_id: String,
    pub owner_id: String,
    pub scope: String,
    pub max_tokens: u64,
    pub consumed_tokens: u64,
    pub revision: u64,
}

impl ContextBudgetLeaseRef {
    #[must_use]
    pub fn new(
        lease_id: impl Into<String>,
        owner_id: impl Into<String>,
        scope: impl Into<String>,
        max_tokens: u64,
        revision: u64,
    ) -> Self {
        Self {
            lease_id: lease_id.into(),
            owner_id: owner_id.into(),
            scope: scope.into(),
            max_tokens,
            consumed_tokens: 0,
            revision,
        }
    }

    #[must_use]
    pub fn with_consumed_tokens(mut self, consumed_tokens: u64) -> Self {
        self.consumed_tokens = consumed_tokens.min(self.max_tokens);
        self
    }

    #[must_use]
    pub const fn remaining_tokens(&self) -> u64 {
        self.max_tokens.saturating_sub(self.consumed_tokens)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceContentKind {
    Text,
    Json,
    Diff,
    Error,
    Media,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceAuditProjection {
    pub evidence_ref: EvidenceRef,
    pub content_kind: EvidenceContentKind,
    pub raw_tokens: u64,
    pub receipt_tokens: u64,
    pub omitted_tokens: u64,
    pub raw_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<EvidenceAccessRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextMode {
    MainTurn,
    Goal,
    Agent,
    Review,
    Resume,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIdentity {
    pub session_id: String,
    pub task_id: Option<String>,
    pub agent_id: String,
    pub mode: ContextMode,
}

impl ContextIdentity {
    #[must_use]
    pub fn main(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            task_id: None,
            agent_id: "primary".to_string(),
            mode: ContextMode::MainTurn,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSourceKind {
    StableHead,
    RuntimeHeader,
    UserRequest,
    Conversation,
    Memory,
    Knowledge,
    Fact,
    Matrix,
    Task,
    ToolTrace,
    Workspace,
    AgentPeer,
    Handoff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextAuthority {
    System,
    User,
    Project,
    Session,
    Agent,
    Tool,
    Derived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextRole {
    Instruction,
    Identity,
    Orientation,
    Evidence,
    Warning,
    TaskState,
    RecentTurn,
    ToolSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContextSourceLifecycle {
    Static,
    #[default]
    Runtime,
    Ephemeral,
    Session,
    Durable,
    External,
    SuppressedForCurrentTurn,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSourceRef {
    pub source_id: String,
    pub source: ContextSourceKind,
    pub authority: ContextAuthority,
    pub lifecycle: ContextSourceLifecycle,
    pub version: Option<String>,
    pub reason: Option<String>,
    pub refs: Vec<KernelRef>,
    pub conflict_with: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    pub id: String,
    pub source: ContextSourceKind,
    pub authority: ContextAuthority,
    pub role: ContextRole,
    pub content: String,
    pub token_estimate: u64,
    pub score: f32,
    pub refs: Vec<KernelRef>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_version: Option<String>,
    #[serde(default)]
    pub source_lifecycle: ContextSourceLifecycle,
    #[serde(default)]
    pub source_reason: Option<String>,
    #[serde(default)]
    pub conflict_with: Vec<String>,
}

impl ContextItem {
    #[must_use]
    pub fn new(
        source: ContextSourceKind,
        authority: ContextAuthority,
        role: ContextRole,
        content: impl Into<String>,
    ) -> Self {
        let content = content.into();
        Self {
            id: format!("ctx-item-{}", uuid::Uuid::new_v4()),
            source,
            authority,
            role,
            token_estimate: estimate_tokens(&content),
            content,
            score: 1.0,
            refs: Vec::new(),
            source_id: None,
            source_version: None,
            source_lifecycle: ContextSourceLifecycle::Runtime,
            source_reason: None,
            conflict_with: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_score(mut self, score: f32) -> Self {
        self.score = score.clamp(0.0, 1.0);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudget {
    pub max_tokens: u64,
    pub stable_reserved: u64,
    pub runtime_reserved: u64,
}

impl ContextBudget {
    #[must_use]
    pub const fn new(max_tokens: u64) -> Self {
        Self {
            max_tokens,
            stable_reserved: 0,
            runtime_reserved: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextOmission {
    pub item_id: String,
    pub source: ContextSourceKind,
    pub reason: String,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEpoch {
    pub epoch_id: String,
    pub identity: ContextIdentity,
    pub budget: ContextBudget,
    pub selected: Vec<ContextItem>,
    pub omitted: Vec<ContextOmission>,
    #[serde(default)]
    pub source_registry: Vec<ContextSourceRef>,
    pub token_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextAlignmentReport {
    pub epoch_id: String,
    pub envelope_id: String,
    pub epoch_selected_count: usize,
    pub envelope_selected_count: usize,
    pub epoch_omitted_count: usize,
    pub envelope_omitted_count: usize,
    pub selected_delta: isize,
    pub omitted_delta: isize,
    pub aligned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptAssemblyPlan {
    pub epoch_id: String,
    pub sections: Vec<PromptSection>,
    pub token_total: u64,
    pub omissions: Vec<ContextOmission>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSection {
    pub source: ContextSourceKind,
    pub role: ContextRole,
    pub content: String,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextArtifactKind {
    UserRequest,
    ToolRawOutput,
    ToolSummary,
    AgentSummary,
    Decision,
    MemoryCandidate,
    CompactionSummary,
    VerificationEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextRetentionPolicy {
    Ephemeral,
    RetainForTurn,
    RetainForSession,
    Durable,
    MemoryCandidate,
    DropAfterCompaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextArtifact {
    pub id: String,
    pub kind: ContextArtifactKind,
    pub retention: ContextRetentionPolicy,
    pub summary: String,
    pub content: Option<String>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub token_estimate: u64,
}

impl ContextArtifact {
    #[must_use]
    pub fn new(
        kind: ContextArtifactKind,
        retention: ContextRetentionPolicy,
        summary: impl Into<String>,
    ) -> Self {
        let summary = summary.into();
        Self {
            id: format!("ctx-artifact-{}", uuid::Uuid::new_v4()),
            kind,
            retention,
            token_estimate: estimate_tokens(&summary),
            summary,
            content: None,
            evidence_refs: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_content(mut self, content: impl Into<String>) -> Self {
        let content = content.into();
        self.token_estimate = self
            .token_estimate
            .saturating_add(estimate_tokens(&content));
        self.content = Some(content);
        self
    }

    #[must_use]
    pub fn with_evidence_ref(mut self, evidence_ref: EvidenceRef) -> Self {
        self.evidence_refs.push(evidence_ref);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolObservation {
    pub tool_name: String,
    pub invocation_id: String,
    pub raw_ref: EvidenceRef,
    pub model_summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_envelope: Option<ToolOutputEnvelope>,
    pub artifacts: Vec<ContextArtifact>,
    pub token_estimate: u64,
}

impl ToolObservation {
    #[must_use]
    pub fn new(
        tool_name: impl Into<String>,
        invocation_id: impl Into<String>,
        raw_ref: EvidenceRef,
        model_summary: impl Into<String>,
    ) -> Self {
        let model_summary = model_summary.into();
        Self {
            tool_name: tool_name.into(),
            invocation_id: invocation_id.into(),
            raw_ref,
            token_estimate: estimate_tokens(&model_summary),
            model_summary,
            output_envelope: None,
            artifacts: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_output_envelope(mut self, output_envelope: ToolOutputEnvelope) -> Self {
        self.output_envelope = Some(output_envelope);
        self
    }

    #[must_use]
    pub fn with_artifact(mut self, artifact: ContextArtifact) -> Self {
        self.token_estimate = self.token_estimate.saturating_add(artifact.token_estimate);
        self.artifacts.push(artifact);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReturnContextEnvelope {
    pub parent_session_id: String,
    pub child_agent_id: String,
    pub result_summary: String,
    pub observations: Vec<ToolObservation>,
    pub artifacts: Vec<ContextArtifact>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub decisions: Vec<String>,
    pub conflicts: Vec<String>,
    pub memory_candidates: Vec<String>,
    pub next_actions: Vec<String>,
    pub failed: bool,
    pub token_estimate: u64,
}

impl AgentReturnContextEnvelope {
    #[must_use]
    pub fn new(
        parent_session_id: impl Into<String>,
        child_agent_id: impl Into<String>,
        result_summary: impl Into<String>,
    ) -> Self {
        let result_summary = result_summary.into();
        Self {
            parent_session_id: parent_session_id.into(),
            child_agent_id: child_agent_id.into(),
            token_estimate: estimate_tokens(&result_summary),
            result_summary,
            observations: Vec::new(),
            artifacts: Vec::new(),
            evidence_refs: Vec::new(),
            decisions: Vec::new(),
            conflicts: Vec::new(),
            memory_candidates: Vec::new(),
            next_actions: Vec::new(),
            failed: false,
        }
    }

    #[must_use]
    pub fn with_observation(mut self, observation: ToolObservation) -> Self {
        self.token_estimate = self
            .token_estimate
            .saturating_add(observation.token_estimate);
        self.evidence_refs.push(observation.raw_ref.clone());
        self.observations.push(observation);
        self
    }

    #[must_use]
    pub fn with_artifact(mut self, artifact: ContextArtifact) -> Self {
        self.token_estimate = self.token_estimate.saturating_add(artifact.token_estimate);
        self.artifacts.push(artifact);
        self
    }

    #[must_use]
    pub fn with_evidence_ref(mut self, evidence_ref: EvidenceRef) -> Self {
        self.evidence_refs.push(evidence_ref);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPressureState {
    pub profile: String,
    pub max_tokens: u64,
    pub used_tokens: u64,
    pub reserved_tokens: u64,
    pub remaining_tokens: u64,
    pub pressure_percent: u8,
    pub compaction_recommended: bool,
}

impl ContextPressureState {
    #[must_use]
    pub fn new(profile: impl Into<String>, max_tokens: u64, used_tokens: u64) -> Self {
        let remaining_tokens = max_tokens.saturating_sub(used_tokens);
        let pressure_percent = if max_tokens == 0 {
            100
        } else {
            ((used_tokens.saturating_mul(100)) / max_tokens).min(100) as u8
        };
        Self {
            profile: profile.into(),
            max_tokens,
            used_tokens,
            reserved_tokens: 0,
            remaining_tokens,
            pressure_percent,
            compaction_recommended: pressure_percent >= 80,
        }
    }

    #[must_use]
    pub fn with_reserved_tokens(mut self, reserved_tokens: u64) -> Self {
        self.reserved_tokens = reserved_tokens;
        self.remaining_tokens = self
            .max_tokens
            .saturating_sub(self.used_tokens.saturating_add(reserved_tokens));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextGovernanceDecision {
    pub id: String,
    pub pressure: ContextPressureState,
    pub compact: bool,
    pub retain_artifact_ids: Vec<String>,
    pub drop_artifact_ids: Vec<String>,
    pub estimated_tokens_to_reclaim: u64,
    pub reason: String,
}

impl ContextGovernanceDecision {
    #[must_use]
    pub fn new(pressure: ContextPressureState, reason: impl Into<String>) -> Self {
        let compact = pressure.compaction_recommended;
        Self {
            id: format!("ctx-governance-{}", uuid::Uuid::new_v4()),
            pressure,
            compact,
            retain_artifact_ids: Vec::new(),
            drop_artifact_ids: Vec::new(),
            estimated_tokens_to_reclaim: 0,
            reason: reason.into(),
        }
    }

    #[must_use]
    pub fn retain_artifact(mut self, artifact_id: impl Into<String>) -> Self {
        self.retain_artifact_ids.push(artifact_id.into());
        self
    }

    #[must_use]
    pub fn drop_artifact(mut self, artifact_id: impl Into<String>, token_estimate: u64) -> Self {
        self.drop_artifact_ids.push(artifact_id.into());
        self.estimated_tokens_to_reclaim = self
            .estimated_tokens_to_reclaim
            .saturating_add(token_estimate);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionReceipt {
    pub id: String,
    pub decision_id: String,
    pub retained_artifact_ids: Vec<String>,
    pub dropped_artifact_ids: Vec<String>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub input_token_estimate: u64,
    pub output_token_estimate: u64,
}

impl CompactionReceipt {
    #[must_use]
    pub fn new(
        decision_id: impl Into<String>,
        input_token_estimate: u64,
        output_token_estimate: u64,
    ) -> Self {
        Self {
            id: format!("ctx-compaction-{}", uuid::Uuid::new_v4()),
            decision_id: decision_id.into(),
            retained_artifact_ids: Vec::new(),
            dropped_artifact_ids: Vec::new(),
            evidence_refs: Vec::new(),
            input_token_estimate,
            output_token_estimate,
        }
    }

    #[must_use]
    pub fn with_evidence_ref(mut self, evidence_ref: EvidenceRef) -> Self {
        self.evidence_refs.push(evidence_ref);
        self
    }
}

/// Turn-local evidence for the cost and usefulness of dynamic Tool exposure.
///
/// Precision is the share of activated descriptors subsequently invoked.
/// Recall requires task-specific expected Tool ground truth, so Runtime leaves
/// it absent and the paired evaluator fills it from its frozen rubric. Denied
/// or unhealthy descriptors are reported separately and never counted as
/// missed executable capability.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExposureMetrics {
    pub provider_requests: u64,
    pub catalog_lookups: u64,
    pub catalog_lookup_micros: u64,
    pub tool_search_calls: u64,
    pub tool_search_additional_rounds: u64,
    pub activation_candidates: u64,
    pub activations: u64,
    pub activated_invocations: u64,
    pub descriptor_misses: u64,
    pub permission_rejections: u64,
    pub unavailable_descriptors: u64,
    pub schema_tokens_max: u64,
    pub schema_compilations: u64,
    pub schema_cache_hits: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_precision_bp: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_recall_bp: Option<u16>,
}

/// Proof that the Provider wire preserves the stable system prefix while
/// request-local controls remain outside it. Provider-native cache counters
/// come from the actual usage response; no local completion cache is implied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StablePrefixMetrics {
    pub provider_requests: u64,
    pub stable_prefix_fingerprint: String,
    pub stable_prefix_bytes: u64,
    pub runtime_system_bytes_max: u64,
    pub wire_identity_failures: u64,
    pub request_compiler_compilations: u64,
    pub request_compiler_cache_hits: u64,
    pub native_cache_creation_input_tokens: u64,
    pub native_cache_read_input_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextTurnReport {
    pub turn_id: String,
    pub profile: String,
    pub pressure: ContextPressureState,
    pub input_token_estimate: u64,
    pub output_token_estimate: u64,
    pub evidence_refs: Vec<EvidenceRef>,
    pub observations: Vec<ToolObservation>,
    pub governance_decision: Option<ContextGovernanceDecision>,
    pub compaction_receipt: Option<CompactionReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger: Option<ContextLedgerProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_exposure: Option<ToolExposureProjection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_projections: Vec<EvidenceAuditProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knowledge: Option<crate::knowledge::KnowledgeTurnReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_exposure_metrics: Option<ToolExposureMetrics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_prefix_metrics: Option<StablePrefixMetrics>,
}

impl ContextTurnReport {
    #[must_use]
    pub fn new(turn_id: impl Into<String>, pressure: ContextPressureState) -> Self {
        Self {
            turn_id: turn_id.into(),
            profile: pressure.profile.clone(),
            input_token_estimate: pressure.used_tokens,
            output_token_estimate: 0,
            pressure,
            evidence_refs: Vec::new(),
            observations: Vec::new(),
            governance_decision: None,
            compaction_receipt: None,
            ledger: None,
            tool_exposure: None,
            audit_projections: Vec::new(),
            knowledge: None,
            tool_exposure_metrics: None,
            stable_prefix_metrics: None,
        }
    }

    #[must_use]
    pub fn with_ledger(mut self, ledger: ContextLedgerProjection) -> Self {
        self.ledger = Some(ledger);
        self
    }

    #[must_use]
    pub fn with_tool_exposure(mut self, exposure: ToolExposureProjection) -> Self {
        self.tool_exposure = Some(exposure);
        self
    }

    #[must_use]
    pub fn with_audit_projection(mut self, projection: EvidenceAuditProjection) -> Self {
        if !self
            .evidence_refs
            .iter()
            .any(|reference| reference == &projection.evidence_ref)
        {
            self.evidence_refs.push(projection.evidence_ref.clone());
        }
        self.audit_projections.push(projection);
        self
    }

    #[must_use]
    pub fn with_output_token_estimate(mut self, output_token_estimate: u64) -> Self {
        self.output_token_estimate = output_token_estimate;
        self
    }

    #[must_use]
    pub fn with_evidence_ref(mut self, evidence_ref: EvidenceRef) -> Self {
        self.evidence_refs.push(evidence_ref);
        self
    }

    #[must_use]
    pub fn with_observation(mut self, observation: ToolObservation) -> Self {
        self.evidence_refs.push(observation.raw_ref.clone());
        self.observations.push(observation);
        self
    }

    #[must_use]
    pub fn with_governance_decision(mut self, decision: ContextGovernanceDecision) -> Self {
        self.governance_decision = Some(decision);
        self
    }

    #[must_use]
    pub fn with_compaction_receipt(mut self, receipt: CompactionReceipt) -> Self {
        self.compaction_receipt = Some(receipt);
        self
    }

    #[must_use]
    pub fn with_knowledge(mut self, report: crate::knowledge::KnowledgeTurnReport) -> Self {
        self.knowledge = Some(report);
        self
    }

    #[must_use]
    pub fn with_tool_exposure_metrics(mut self, metrics: ToolExposureMetrics) -> Self {
        self.tool_exposure_metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_stable_prefix_metrics(mut self, metrics: StablePrefixMetrics) -> Self {
        self.stable_prefix_metrics = Some(metrics);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextComponentUsage {
    pub kind: String,
    pub tokens: u64,
    pub occurrences: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextLedgerProjection {
    pub max_tokens: u64,
    pub consumed_tokens: u64,
    pub remaining_tokens: u64,
    pub tool_result_limit: u64,
    pub tool_result_consumed: u64,
    pub components: Vec<ContextComponentUsage>,
    pub request_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calibrated_input_tokens: Option<u64>,
}

impl EvidenceRef {
    #[must_use]
    pub fn new(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self(KernelRef::new(ref_type, id))
    }

    #[must_use]
    pub fn durable(id: impl Into<String>) -> Self {
        Self::new("durable_evidence", id)
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.0.id
    }
}

#[derive(Debug, Clone)]
pub struct ContextEpochBuilder {
    identity: ContextIdentity,
    budget: ContextBudget,
    items: Vec<ContextItem>,
}

impl ContextEpochBuilder {
    #[must_use]
    pub fn new(identity: ContextIdentity, budget: ContextBudget) -> Self {
        Self {
            identity,
            budget,
            items: Vec::new(),
        }
    }

    #[must_use]
    pub fn add_item(mut self, item: ContextItem) -> Self {
        self.items.push(item);
        self
    }

    pub fn build(mut self) -> AiKernelResult<ContextEpoch> {
        if self.budget.max_tokens == 0 {
            return Err(AiKernelError::InvalidInput(
                "context budget must be greater than zero".to_string(),
            ));
        }
        self.items.sort_by(compare_context_items);
        let mut selected = Vec::new();
        let mut omitted = Vec::new();
        let mut token_total = 0u64;
        let mut source_registry = Vec::new();
        for item in self.items {
            let item = normalize_context_item_source(item);
            if token_total.saturating_add(item.token_estimate) <= self.budget.max_tokens {
                token_total = token_total.saturating_add(item.token_estimate);
                source_registry.push(context_source_ref_from_item(&item));
                selected.push(item);
            } else {
                source_registry.push(ContextSourceRef {
                    source_id: item.source_id.clone().unwrap_or_else(|| item.id.clone()),
                    source: item.source,
                    authority: item.authority,
                    lifecycle: ContextSourceLifecycle::SuppressedForCurrentTurn,
                    version: item.source_version.clone(),
                    reason: Some("context budget exceeded".to_string()),
                    refs: item.refs.clone(),
                    conflict_with: item.conflict_with.clone(),
                });
                omitted.push(ContextOmission {
                    item_id: item.id,
                    source: item.source,
                    reason: "context budget exceeded".to_string(),
                    token_estimate: item.token_estimate,
                });
            }
        }
        Ok(ContextEpoch {
            epoch_id: format!("ctx-epoch-{}", uuid::Uuid::new_v4()),
            identity: self.identity,
            budget: self.budget,
            selected,
            omitted,
            source_registry,
            token_total,
        })
    }
}

impl ContextEpoch {
    #[must_use]
    pub fn prompt_assembly_plan(&self) -> PromptAssemblyPlan {
        let sections = self
            .selected
            .iter()
            .map(|item| PromptSection {
                source: item.source,
                role: item.role,
                content: item.content.clone(),
                token_estimate: item.token_estimate,
            })
            .collect();
        PromptAssemblyPlan {
            epoch_id: self.epoch_id.clone(),
            sections,
            token_total: self.token_total,
            omissions: self.omitted.clone(),
        }
    }

    #[must_use]
    pub fn alignment_report(
        &self,
        envelope_id: impl Into<String>,
        envelope_selected_count: usize,
        envelope_omitted_count: usize,
    ) -> ContextAlignmentReport {
        let selected_delta = self.selected.len() as isize - envelope_selected_count as isize;
        let omitted_delta = self.omitted.len() as isize - envelope_omitted_count as isize;
        ContextAlignmentReport {
            epoch_id: self.epoch_id.clone(),
            envelope_id: envelope_id.into(),
            epoch_selected_count: self.selected.len(),
            envelope_selected_count,
            epoch_omitted_count: self.omitted.len(),
            envelope_omitted_count,
            selected_delta,
            omitted_delta,
            aligned: selected_delta == 0 && omitted_delta == 0,
        }
    }
}

fn normalize_context_item_source(mut item: ContextItem) -> ContextItem {
    if item.source_id.as_deref().unwrap_or_default().is_empty() {
        item.source_id = Some(item.id.clone());
    }
    if item.source_lifecycle == ContextSourceLifecycle::Runtime {
        item.source_lifecycle = default_source_lifecycle(item.source);
    }
    if item
        .source_reason
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        item.source_reason = Some(format!(
            "selected_for_{:?}_role_from_{:?}",
            item.role, item.source
        ));
    }
    item
}

fn context_source_ref_from_item(item: &ContextItem) -> ContextSourceRef {
    ContextSourceRef {
        source_id: item.source_id.clone().unwrap_or_else(|| item.id.clone()),
        source: item.source,
        authority: item.authority,
        lifecycle: item.source_lifecycle,
        version: item.source_version.clone(),
        reason: item.source_reason.clone(),
        refs: item.refs.clone(),
        conflict_with: item.conflict_with.clone(),
    }
}

fn default_source_lifecycle(source: ContextSourceKind) -> ContextSourceLifecycle {
    match source {
        ContextSourceKind::StableHead | ContextSourceKind::RuntimeHeader => {
            ContextSourceLifecycle::Static
        }
        ContextSourceKind::Memory
        | ContextSourceKind::Knowledge
        | ContextSourceKind::Fact
        | ContextSourceKind::Matrix => ContextSourceLifecycle::Durable,
        ContextSourceKind::Workspace => ContextSourceLifecycle::External,
        ContextSourceKind::UserRequest
        | ContextSourceKind::Conversation
        | ContextSourceKind::Task
        | ContextSourceKind::ToolTrace
        | ContextSourceKind::AgentPeer
        | ContextSourceKind::Handoff => ContextSourceLifecycle::Runtime,
    }
}

fn compare_context_items(left: &ContextItem, right: &ContextItem) -> std::cmp::Ordering {
    source_priority(left.source)
        .cmp(&source_priority(right.source))
        .then_with(|| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.token_estimate.cmp(&right.token_estimate))
}

fn source_priority(source: ContextSourceKind) -> u8 {
    match source {
        ContextSourceKind::StableHead => 0,
        ContextSourceKind::RuntimeHeader => 1,
        ContextSourceKind::UserRequest => 2,
        ContextSourceKind::Task => 3,
        ContextSourceKind::Workspace => 4,
        ContextSourceKind::Knowledge => 5,
        ContextSourceKind::Memory => 6,
        ContextSourceKind::Fact => 7,
        ContextSourceKind::Matrix => 8,
        ContextSourceKind::ToolTrace => 9,
        ContextSourceKind::Conversation => 10,
        ContextSourceKind::AgentPeer => 11,
        ContextSourceKind::Handoff => 12,
    }
}

fn estimate_tokens(content: &str) -> u64 {
    let chars = content.chars().count() as u64;
    chars.div_ceil(4).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(source: ContextSourceKind, content: &str, score: f32) -> ContextItem {
        ContextItem::new(
            source,
            ContextAuthority::Derived,
            ContextRole::Evidence,
            content,
        )
        .with_score(score)
    }

    #[test]
    fn epoch_keeps_stable_and_user_context_before_lower_priority_items() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(20))
            .add_item(item(
                ContextSourceKind::Memory,
                "remember this long memory",
                1.0,
            ))
            .add_item(item(ContextSourceKind::StableHead, "system", 0.1))
            .add_item(item(ContextSourceKind::UserRequest, "user asks", 0.5))
            .build()
            .unwrap();

        assert_eq!(epoch.selected[0].source, ContextSourceKind::StableHead);
        assert_eq!(epoch.selected[1].source, ContextSourceKind::UserRequest);
    }

    #[test]
    fn epoch_records_omissions_when_budget_is_exceeded() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(5))
            .add_item(item(ContextSourceKind::StableHead, "system", 1.0))
            .add_item(item(
                ContextSourceKind::Workspace,
                "this content is definitely too long for the tiny budget",
                1.0,
            ))
            .build()
            .unwrap();

        assert_eq!(epoch.selected.len(), 1);
        assert_eq!(epoch.omitted.len(), 1);
        assert_eq!(epoch.omitted[0].reason, "context budget exceeded");
    }

    #[test]
    fn prompt_assembly_plan_preserves_selected_items_and_omissions() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(5))
            .add_item(item(ContextSourceKind::StableHead, "system", 1.0))
            .add_item(item(
                ContextSourceKind::Memory,
                "too much memory content",
                1.0,
            ))
            .build()
            .unwrap();
        let plan = epoch.prompt_assembly_plan();

        assert_eq!(plan.epoch_id, epoch.epoch_id);
        assert_eq!(plan.sections.len(), epoch.selected.len());
        assert_eq!(plan.omissions.len(), epoch.omitted.len());
    }

    #[test]
    fn epoch_builds_source_registry_for_selected_and_omitted_items() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(5))
            .add_item(item(ContextSourceKind::Memory, "short", 1.0))
            .add_item(item(
                ContextSourceKind::Knowledge,
                "this knowledge item is too long for the budget",
                1.0,
            ))
            .build()
            .unwrap();

        assert_eq!(epoch.source_registry.len(), 2);
        assert!(epoch
            .source_registry
            .iter()
            .any(|source| source.lifecycle == ContextSourceLifecycle::Durable));
        assert!(epoch
            .source_registry
            .iter()
            .any(|source| source.lifecycle == ContextSourceLifecycle::SuppressedForCurrentTurn));
    }

    #[test]
    fn alignment_report_compares_epoch_with_envelope_counts() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(5))
            .add_item(item(ContextSourceKind::StableHead, "system", 1.0))
            .add_item(item(
                ContextSourceKind::Memory,
                "too much memory content",
                1.0,
            ))
            .build()
            .unwrap();

        let aligned =
            epoch.alignment_report("envelope-1", epoch.selected.len(), epoch.omitted.len());
        let drifted = epoch.alignment_report("envelope-2", 10, 0);

        assert!(aligned.aligned);
        assert!(!drifted.aligned);
        assert_eq!(drifted.envelope_id, "envelope-2");
    }

    #[test]
    fn tool_observation_has_durable_raw_ref_and_model_summary() {
        let raw_ref = EvidenceRef::durable("tool-raw-1");
        let observation = ToolObservation::new(
            "exec_command",
            "invocation-1",
            raw_ref.clone(),
            "cargo test completed successfully",
        );

        assert_eq!(observation.raw_ref, raw_ref);
        assert_eq!(observation.raw_ref.0.ref_type, "durable_evidence");
        assert_eq!(
            observation.model_summary,
            "cargo test completed successfully"
        );
        assert!(observation.token_estimate > 0);
    }

    #[test]
    fn turn_report_records_pressure_profile_tokens_and_evidence_refs() {
        let raw_ref = EvidenceRef::durable("tool-raw-2");
        let pressure =
            ContextPressureState::new("default", 10_000, 8_250).with_reserved_tokens(500);
        let observation = ToolObservation::new(
            "rg",
            "invocation-2",
            raw_ref.clone(),
            "found matching context governance files",
        );
        let report = ContextTurnReport::new("turn-1", pressure.clone())
            .with_output_token_estimate(320)
            .with_observation(observation)
            .with_evidence_ref(EvidenceRef::durable("report-evidence-1"));

        assert_eq!(report.profile, "default");
        assert_eq!(report.pressure, pressure);
        assert_eq!(report.input_token_estimate, 8_250);
        assert_eq!(report.output_token_estimate, 320);
        assert_eq!(report.evidence_refs[0], raw_ref);
        assert_eq!(report.evidence_refs[1].id(), "report-evidence-1");
        assert!(report.pressure.compaction_recommended);
    }
}
