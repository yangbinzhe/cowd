//! Intelligent runtime context envelope.
//!
//! This module introduces a typed context boundary without changing provider
//! behavior yet. The first invariant is prompt-cache friendliness: stable
//! system instructions stay ahead of runtime and dynamic packets.

use chrono::{DateTime, Utc};
use harness_contract::knowledge::KnowledgeTurnReport;
use serde::{Deserialize, Serialize};

use model_protocol::prompt_cache::stable_hash_bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextMode {
    MainTurn,
    SoloGoal,
    YoloGoal,
    SubAgent,
    Collaboration,
    Review,
    Resume,
    Cron,
    SurfaceQuickReply,
    SurfaceTaskIntake,
    DeepInvestigation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextProfile {
    MainTurn,
    SoloGoal,
    YoloGoal,
    SubAgent,
    Collaboration,
    Review,
    Resume,
    Cron,
    SurfaceQuickReply,
    SurfaceTaskIntake,
    DeepInvestigation,
}

impl From<ContextMode> for ContextProfile {
    fn from(mode: ContextMode) -> Self {
        match mode {
            ContextMode::MainTurn => Self::MainTurn,
            ContextMode::SoloGoal => Self::SoloGoal,
            ContextMode::YoloGoal => Self::YoloGoal,
            ContextMode::SubAgent => Self::SubAgent,
            ContextMode::Collaboration => Self::Collaboration,
            ContextMode::Review => Self::Review,
            ContextMode::Resume => Self::Resume,
            ContextMode::Cron => Self::Cron,
            ContextMode::SurfaceQuickReply => Self::SurfaceQuickReply,
            ContextMode::SurfaceTaskIntake => Self::SurfaceTaskIntake,
            ContextMode::DeepInvestigation => Self::DeepInvestigation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIdentity {
    pub session_id: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: String,
    pub parent_agent_id: Option<String>,
    pub team_id: Option<String>,
    pub mode: ContextMode,
}

impl ContextIdentity {
    pub fn main(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            project_id: None,
            task_id: None,
            agent_id: "primary".to_string(),
            parent_agent_id: None,
            team_id: None,
            mode: ContextMode::MainTurn,
        }
    }

    pub fn sub_agent(
        session_id: impl Into<String>,
        agent_id: impl Into<String>,
        parent_agent_id: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            project_id: None,
            task_id: None,
            agent_id: agent_id.into(),
            parent_agent_id: Some(parent_agent_id.into()),
            team_id: None,
            mode: ContextMode::SubAgent,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextSourceKind {
    StableHead,
    RuntimeHeader,
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
pub enum ContextVisibility {
    Private,
    Shared,
    Team,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    pub source_kind: ContextSourceKind,
    pub authority: ContextAuthority,
    pub lifecycle: ContextSourceLifecycle,
    pub version: Option<String>,
    pub reason: Option<String>,
    pub evidence: Vec<String>,
    pub conflict_with: Vec<String>,
}

impl ContextSourceRef {
    fn from_item(item: &ContextItem) -> Self {
        Self {
            source_id: item.source_id.clone().unwrap_or_else(|| item.id.clone()),
            source_kind: item.source,
            authority: item.authority,
            lifecycle: item.source_lifecycle,
            version: item.source_version.clone(),
            reason: item.source_reason.clone(),
            evidence: item.evidence.clone(),
            conflict_with: item.conflict_with.clone(),
        }
    }

    fn from_omission(omission: &ContextOmission) -> Self {
        let source_id = format!(
            "omitted:{:?}:{}",
            omission.source,
            stable_hash_bytes(omission.reason.as_bytes())
        );
        Self {
            source_id,
            source_kind: omission.source,
            authority: ContextAuthority::Derived,
            lifecycle: ContextSourceLifecycle::SuppressedForCurrentTurn,
            version: None,
            reason: Some(omission.reason.clone()),
            evidence: Vec::new(),
            conflict_with: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    pub id: String,
    pub source: ContextSourceKind,
    pub authority: ContextAuthority,
    pub visibility: ContextVisibility,
    pub role: ContextRole,
    pub content: String,
    pub token_estimate: u64,
    pub score: f32,
    pub evidence: Vec<String>,
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
    pub fn new(
        id: impl Into<String>,
        source: ContextSourceKind,
        role: ContextRole,
        content: impl Into<String>,
    ) -> Self {
        let content = content.into();
        Self {
            id: id.into(),
            source,
            authority: ContextAuthority::Derived,
            visibility: ContextVisibility::Private,
            role,
            token_estimate: estimate_tokens(&content),
            content,
            score: 1.0,
            evidence: Vec::new(),
            source_id: None,
            source_version: None,
            source_lifecycle: ContextSourceLifecycle::Runtime,
            source_reason: None,
            conflict_with: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextOmission {
    pub source: ContextSourceKind,
    pub reason: String,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextLease {
    pub source: ContextSourceKind,
    pub min_tokens: u64,
    pub target_tokens: u64,
    pub max_tokens: u64,
    pub priority: u8,
    pub degradation: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentContextLease {
    pub parent_session_id: String,
    pub parent_agent_id: String,
    pub child_agent_id: String,
    pub task_contract: String,
    pub allowed_sources: Vec<ContextSourceKind>,
    pub max_tokens: u64,
    pub required_return: Vec<AgentReturnRequirement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentReturnRequirement {
    ResultSummary,
    Evidence,
    Decisions,
    Conflicts,
    MemoryCandidates,
    NextActions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReturnContextProjection {
    pub parent_session_id: String,
    pub child_agent_id: String,
    pub result_summary: String,
    pub evidence: Vec<String>,
    pub decisions: Vec<String>,
    pub conflicts: Vec<String>,
    pub memory_candidates: Vec<String>,
    pub next_actions: Vec<String>,
    pub failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTracePacket {
    pub tool_name: String,
    pub invocation_id: String,
    pub status: ToolTraceStatus,
    pub summary: String,
    pub changed_files: Vec<String>,
    pub evidence_ids: Vec<String>,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolTraceStatus {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePacket {
    pub root: String,
    pub touched_files: Vec<String>,
    pub hot_symbols: Vec<String>,
    pub project_notes: Vec<String>,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeContextPacket {
    pub session_id: String,
    pub handoff_summary: Option<String>,
    pub active_task: Option<String>,
    pub recent_decisions: Vec<String>,
    pub blockers: Vec<String>,
    pub source: ResumeContextSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumeContextSource {
    SessionDb,
    Handoff,
    TaskRegistry,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudgetReport {
    pub total_tokens: u64,
    pub used_tokens: u64,
    pub leases: Vec<ContextLease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextDiagnostics {
    pub stable_head_hash: String,
    pub runtime_header_hash: String,
    pub dynamic_tail_hash: String,
    pub degraded_sources: Vec<ContextSourceKind>,
    pub pressure_bp: u16,
    #[serde(default)]
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextPressureLevel {
    Nominal,
    Elevated,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextDegradationPath {
    None,
    SourceFallback,
    TrimDynamicTail,
    SummarizeEvidence,
    HandoffBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextLeanProbe {
    pub envelope_id: String,
    pub profile: ContextProfile,
    pub stable_head_hash: String,
    pub runtime_header_hash: String,
    pub dynamic_tail_hash: String,
    pub selected_count: usize,
    pub omitted_count: usize,
    pub budget_total_tokens: u64,
    pub budget_used_tokens: u64,
    pub pressure_bp: u16,
    pub pressure_level: ContextPressureLevel,
    pub degradation_path: ContextDegradationPath,
    pub degraded_sources: Vec<ContextSourceKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextPolicyAction {
    None,
    TrimToolTrace,
    SummarizeEvidence,
    PreferOrientationPacket,
    WriteHandoff,
    RecommendSessionBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPolicyDecision {
    pub profile: ContextProfile,
    pub pressure_level: ContextPressureLevel,
    pub degradation_path: ContextDegradationPath,
    pub action: ContextPolicyAction,
    pub reason: String,
    pub stable_head_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPolicyProposal {
    pub proposal_id: String,
    pub session_id: String,
    pub envelope_id: String,
    pub action: ContextPolicyAction,
    pub confidence: f32,
    pub expected_saving_tokens: u64,
    pub affected_sources: Vec<ContextSourceKind>,
    pub safe_to_auto_apply: bool,
    pub reason: String,
    pub stable_head_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StableHeadComparison {
    pub previous_hash: String,
    pub next_hash: String,
    pub reusable: bool,
    pub runtime_header_changed: bool,
    pub dynamic_tail_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextCacheStabilityReport {
    pub previous_envelope_id: String,
    pub next_envelope_id: String,
    pub stable_head_reusable: bool,
    pub runtime_header_changed: bool,
    pub dynamic_tail_changed: bool,
    pub prompt_cache_friendly: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextModeCoverageEntry {
    pub profile: ContextProfile,
    pub mode: ContextMode,
    pub envelope_id: String,
    pub stable_head_hash: String,
    pub runtime_header_hash: String,
    pub dynamic_tail_hash: String,
    pub stable_head_reusable: bool,
    pub selected_count: usize,
    pub omitted_count: usize,
    pub pressure_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextModeCoverageReport {
    pub required_profiles: Vec<ContextProfile>,
    pub covered_profiles: Vec<ContextProfile>,
    pub stable_head_hash: String,
    pub all_profiles_covered: bool,
    pub all_stable_heads_reusable: bool,
    pub entries: Vec<ContextModeCoverageEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextSegmentKind {
    StableHead,
    RuntimeHeader,
    DynamicTail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSegmentSnapshot {
    pub kind: ContextSegmentKind,
    pub hash: String,
    pub token_estimate: u64,
    pub item_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSnapshot {
    pub envelope_id: String,
    pub session_id: String,
    pub agent_id: String,
    pub profile: ContextProfile,
    pub stable_head_hash: String,
    pub runtime_header_hash: String,
    pub dynamic_tail_hash: String,
    pub total_tokens: u64,
    pub used_tokens: u64,
    pub segments: Vec<ContextSegmentSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSegmentChange {
    pub kind: ContextSegmentKind,
    pub previous_hash: String,
    pub next_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSnapshotDiff {
    pub previous_envelope_id: String,
    pub next_envelope_id: String,
    pub stable_head_reusable: bool,
    pub runtime_header_changed: bool,
    pub dynamic_tail_changed: bool,
    pub changed_segments: Vec<ContextSegmentChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudgetAllocation {
    pub source: ContextSourceKind,
    pub min_tokens: u64,
    pub target_tokens: u64,
    pub max_tokens: u64,
    pub used_tokens: u64,
    pub selected_count: usize,
    pub omitted_count: usize,
    pub exhausted: bool,
    pub priority: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextBudgetExplanation {
    pub total_tokens: u64,
    pub used_tokens: u64,
    pub pressure_bp: u16,
    pub allocations: Vec<ContextBudgetAllocation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentContextView {
    pub child_agent_id: String,
    pub parent_agent_id: String,
    pub envelope: ContextEnvelope,
    pub inherited_item_ids: Vec<String>,
    pub isolated_omissions: Vec<ContextOmission>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextEpochReport {
    pub epoch_id: String,
    pub envelope_id: String,
    pub session_id: String,
    pub profile: ContextProfile,
    pub selected_count: usize,
    pub omitted_count: usize,
    pub source_count: usize,
    pub active_sources: Vec<ContextSourceRef>,
    pub suppressed_sources: Vec<ContextSourceRef>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeContextMemoryDecision {
    pub item_id: String,
    pub source_kind: ContextSourceKind,
    pub role: Option<ContextRole>,
    pub selected: bool,
    pub reason: String,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeContextKnowledgeDecision {
    pub activated_pack_ids: Vec<String>,
    pub suppressed_namespaces: Vec<String>,
    pub compliance_warnings: Vec<String>,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeContextFactDecision {
    pub trigger: String,
    pub mode: String,
    pub degraded: bool,
    pub reason: String,
    pub candidate_count: usize,
    pub review_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCompressionCheckpointRef {
    pub checkpoint_id: String,
    pub source: String,
    pub summary: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeContextGovernanceReport {
    pub report_id: String,
    pub envelope_id: String,
    pub context_epoch: String,
    pub session_id: String,
    pub profile: ContextProfile,
    pub selected_memory: Vec<RuntimeContextMemoryDecision>,
    pub omitted_memory: Vec<RuntimeContextMemoryDecision>,
    pub knowledge: RuntimeContextKnowledgeDecision,
    pub fact_extraction: Option<RuntimeContextFactDecision>,
    pub compression_checkpoint: Option<RuntimeCompressionCheckpointRef>,
    pub contamination_notes: Vec<String>,
    pub conflict_notes: Vec<String>,
    pub source_registry: Vec<ContextSourceRef>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssembledContext {
    pub stable_head: Vec<String>,
    pub runtime_header: Vec<String>,
    pub dynamic_tail: Vec<String>,
}

impl AssembledContext {
    pub fn system_prompt(&self) -> Vec<String> {
        let mut prompt = Vec::with_capacity(
            self.stable_head.len() + self.runtime_header.len() + self.dynamic_tail.len(),
        );
        prompt.extend(self.stable_head.clone());
        prompt.extend(self.runtime_header.clone());
        prompt.extend(self.dynamic_tail.clone());
        prompt
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEnvelope {
    pub id: String,
    #[serde(default)]
    pub epoch_id: String,
    pub identity: ContextIdentity,
    pub profile: ContextProfile,
    pub intent: String,
    pub selected: Vec<ContextItem>,
    pub omitted: Vec<ContextOmission>,
    #[serde(default)]
    pub source_registry: Vec<ContextSourceRef>,
    #[serde(default)]
    pub epoch_report: Option<ContextEpochReport>,
    pub budget: ContextBudgetReport,
    pub diagnostics: ContextDiagnostics,
    pub assembled: AssembledContext,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ContextEnvelopeRequest {
    pub identity: ContextIdentity,
    pub profile: ContextProfile,
    pub intent: String,
    pub stable_head: Vec<String>,
    pub runtime_header: Vec<String>,
    pub dynamic_items: Vec<ContextItem>,
    pub omitted: Vec<ContextOmission>,
    pub total_budget_tokens: u64,
}

pub struct ContextRuntimeKernel;

impl ContextRuntimeKernel {
    pub fn mode_for_profile(profile: ContextProfile) -> ContextMode {
        match profile {
            ContextProfile::MainTurn => ContextMode::MainTurn,
            ContextProfile::SoloGoal => ContextMode::SoloGoal,
            ContextProfile::YoloGoal => ContextMode::YoloGoal,
            ContextProfile::SubAgent => ContextMode::SubAgent,
            ContextProfile::Collaboration => ContextMode::Collaboration,
            ContextProfile::Review => ContextMode::Review,
            ContextProfile::Resume => ContextMode::Resume,
            ContextProfile::Cron => ContextMode::Cron,
            ContextProfile::SurfaceQuickReply => ContextMode::SurfaceQuickReply,
            ContextProfile::SurfaceTaskIntake => ContextMode::SurfaceTaskIntake,
            ContextProfile::DeepInvestigation => ContextMode::DeepInvestigation,
        }
    }

    pub fn required_profiles() -> Vec<ContextProfile> {
        vec![
            ContextProfile::MainTurn,
            ContextProfile::SoloGoal,
            ContextProfile::YoloGoal,
            ContextProfile::SubAgent,
            ContextProfile::Collaboration,
            ContextProfile::Review,
            ContextProfile::Resume,
            ContextProfile::Cron,
            ContextProfile::SurfaceQuickReply,
            ContextProfile::SurfaceTaskIntake,
            ContextProfile::DeepInvestigation,
        ]
    }

    pub fn runtime_header(identity: &ContextIdentity, profile: ContextProfile) -> Vec<String> {
        let project = identity.project_id.as_deref().unwrap_or("none");
        let task = identity.task_id.as_deref().unwrap_or("none");
        let parent = identity.parent_agent_id.as_deref().unwrap_or("none");
        let team = identity.team_id.as_deref().unwrap_or("none");
        vec![format!(
            "session:{} agent:{} parent:{} project:{} task:{} team:{} mode:{:?} profile:{:?}",
            identity.session_id,
            identity.agent_id,
            parent,
            project,
            task,
            team,
            identity.mode,
            profile
        )]
    }

    #[must_use]
    pub fn governance_report_id(session_id: &str, intent: &str) -> String {
        format!(
            "ctx-governance-{}",
            stable_hash_bytes(format!("{session_id}:{intent}").as_bytes())
        )
    }

    pub fn build_envelope(request: ContextEnvelopeRequest) -> ContextEnvelope {
        let profile = request.profile;
        let leases = Self::default_leases(profile, request.total_budget_tokens);
        let (dynamic_items, lease_omissions) = Self::apply_leases(request.dynamic_items, &leases);
        let dynamic_items = dynamic_items
            .into_iter()
            .map(normalize_context_item_source)
            .collect::<Vec<_>>();
        let mut omitted = request.omitted;
        omitted.extend(lease_omissions);
        let dynamic_tail = dynamic_items
            .iter()
            .map(Self::format_context_item)
            .collect::<Vec<_>>();
        let used_tokens = request
            .stable_head
            .iter()
            .chain(request.runtime_header.iter())
            .map(|text| estimate_tokens(text))
            .sum::<u64>()
            + dynamic_items
                .iter()
                .map(|item| item.token_estimate)
                .sum::<u64>();
        let pressure_bp = if request.total_budget_tokens == 0 {
            0
        } else {
            ((used_tokens.saturating_mul(10_000)) / request.total_budget_tokens).min(10_000) as u16
        };
        let assembled = AssembledContext {
            stable_head: request.stable_head,
            runtime_header: request.runtime_header,
            dynamic_tail,
        };
        let diagnostics = ContextDiagnostics {
            stable_head_hash: hash_segments(&assembled.stable_head),
            runtime_header_hash: hash_segments(&assembled.runtime_header),
            dynamic_tail_hash: hash_segments(&assembled.dynamic_tail),
            degraded_sources: Vec::new(),
            pressure_bp,
            recommendations: context_recommendations(
                pressure_bp,
                dynamic_items.len(),
                omitted.len(),
            ),
        };
        let id = envelope_id(&request.identity, &request.intent, &diagnostics);
        let epoch_id = context_epoch_id(&id);
        let active_sources = dynamic_items
            .iter()
            .map(ContextSourceRef::from_item)
            .collect::<Vec<_>>();
        let suppressed_sources = omitted
            .iter()
            .map(ContextSourceRef::from_omission)
            .collect::<Vec<_>>();
        let mut source_registry = active_sources.clone();
        source_registry.extend(suppressed_sources.clone());
        let created_at = Utc::now();
        let epoch_report = ContextEpochReport {
            epoch_id: epoch_id.clone(),
            envelope_id: id.clone(),
            session_id: request.identity.session_id.clone(),
            profile,
            selected_count: dynamic_items.len(),
            omitted_count: omitted.len(),
            source_count: source_registry.len(),
            active_sources,
            suppressed_sources,
            created_at,
        };

        ContextEnvelope {
            id,
            epoch_id,
            identity: request.identity,
            profile,
            intent: request.intent,
            selected: dynamic_items,
            omitted,
            source_registry,
            epoch_report: Some(epoch_report),
            budget: ContextBudgetReport {
                total_tokens: request.total_budget_tokens,
                used_tokens,
                leases,
            },
            diagnostics,
            assembled,
            created_at,
        }
    }

    pub fn lean_probe(envelope: &ContextEnvelope) -> ContextLeanProbe {
        ContextLeanProbe {
            envelope_id: envelope.id.clone(),
            profile: envelope.profile,
            stable_head_hash: envelope.diagnostics.stable_head_hash.clone(),
            runtime_header_hash: envelope.diagnostics.runtime_header_hash.clone(),
            dynamic_tail_hash: envelope.diagnostics.dynamic_tail_hash.clone(),
            selected_count: envelope.selected.len(),
            omitted_count: envelope.omitted.len(),
            budget_total_tokens: envelope.budget.total_tokens,
            budget_used_tokens: envelope.budget.used_tokens,
            pressure_bp: envelope.diagnostics.pressure_bp,
            pressure_level: pressure_level_for_bp(envelope.diagnostics.pressure_bp),
            degradation_path: degradation_path_for(
                envelope.diagnostics.pressure_bp,
                envelope.omitted.len(),
                &envelope.diagnostics.degraded_sources,
            ),
            degraded_sources: envelope.diagnostics.degraded_sources.clone(),
        }
    }

    pub fn governance_report(
        envelope: &ContextEnvelope,
        knowledge: Option<&KnowledgeTurnReport>,
        fact_extraction: Option<RuntimeContextFactDecision>,
        compression_checkpoint: Option<RuntimeCompressionCheckpointRef>,
    ) -> RuntimeContextGovernanceReport {
        let selected_memory = envelope
            .selected
            .iter()
            .filter(|item| item.source == ContextSourceKind::Memory)
            .map(|item| RuntimeContextMemoryDecision {
                item_id: item.id.clone(),
                source_kind: item.source,
                role: Some(item.role),
                selected: true,
                reason: item
                    .source_reason
                    .clone()
                    .unwrap_or_else(|| "selected_for_current_turn".to_string()),
                token_estimate: item.token_estimate,
            })
            .collect::<Vec<_>>();
        let omitted_memory = envelope
            .omitted
            .iter()
            .filter(|item| item.source == ContextSourceKind::Memory)
            .map(|item| RuntimeContextMemoryDecision {
                item_id: format!("omitted:{}", stable_hash_bytes(item.reason.as_bytes())),
                source_kind: item.source,
                role: None,
                selected: false,
                reason: item.reason.clone(),
                token_estimate: item.token_estimate,
            })
            .collect::<Vec<_>>();
        let knowledge = knowledge
            .map(|report| RuntimeContextKnowledgeDecision {
                activated_pack_ids: report.active_pack_ids.clone(),
                suppressed_namespaces: report.blocked_namespaces.clone(),
                compliance_warnings: report
                    .compliance_warnings
                    .iter()
                    .map(|warning| {
                        format!(
                            "{:?}:{}:{}",
                            warning.level, warning.pack_id, warning.summary
                        )
                    })
                    .collect(),
                evidence_refs: report
                    .evidence_refs
                    .iter()
                    .map(|reference| format!("{}/{}", reference.ref_type, reference.id))
                    .collect(),
            })
            .unwrap_or_default();
        let contamination_notes = envelope
            .omitted
            .iter()
            .filter(|item| item.reason.contains("suppressed_for_current_turn"))
            .map(|item| item.reason.clone())
            .collect::<Vec<_>>();
        let conflict_notes = envelope
            .selected
            .iter()
            .flat_map(|item| item.conflict_with.clone())
            .chain(envelope.omitted.iter().filter_map(|item| {
                item.reason
                    .contains("conflict")
                    .then(|| item.reason.clone())
            }))
            .collect::<Vec<_>>();
        RuntimeContextGovernanceReport {
            report_id: Self::governance_report_id(&envelope.identity.session_id, &envelope.intent),
            envelope_id: envelope.id.clone(),
            context_epoch: envelope.epoch_id.clone(),
            session_id: envelope.identity.session_id.clone(),
            profile: envelope.profile,
            selected_memory,
            omitted_memory,
            knowledge,
            fact_extraction,
            compression_checkpoint,
            contamination_notes,
            conflict_notes,
            source_registry: envelope.source_registry.clone(),
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn format_context_item(item: &ContextItem) -> String {
        format!(
            "### context {:?} | {:?} | score {:.2}\n{}",
            item.source, item.role, item.score, item.content
        )
    }

    pub fn policy_decision(probe: &ContextLeanProbe) -> ContextPolicyDecision {
        let (action, reason) = context_policy_action(probe);
        ContextPolicyDecision {
            profile: probe.profile,
            pressure_level: probe.pressure_level,
            degradation_path: probe.degradation_path,
            action,
            reason,
            stable_head_hash: probe.stable_head_hash.clone(),
        }
    }

    pub fn policy_proposal(envelope: &ContextEnvelope) -> ContextPolicyProposal {
        let probe = Self::lean_probe(envelope);
        let decision = Self::policy_decision(&probe);
        ContextPolicyProposal {
            proposal_id: format!(
                "ctx-proposal-{}-{}",
                envelope.id,
                context_policy_action_name(decision.action)
            ),
            session_id: envelope.identity.session_id.clone(),
            envelope_id: envelope.id.clone(),
            action: decision.action,
            confidence: confidence_for_policy(&probe, decision.action),
            expected_saving_tokens: expected_saving_tokens(&probe, decision.action),
            affected_sources: affected_sources_for_action(decision.action, &probe),
            safe_to_auto_apply: safe_to_auto_apply(decision.action),
            reason: decision.reason,
            stable_head_hash: probe.stable_head_hash,
        }
    }

    pub fn compare_stable_head(
        previous: &ContextEnvelope,
        next: &ContextEnvelope,
    ) -> StableHeadComparison {
        StableHeadComparison {
            previous_hash: previous.diagnostics.stable_head_hash.clone(),
            next_hash: next.diagnostics.stable_head_hash.clone(),
            reusable: previous.diagnostics.stable_head_hash == next.diagnostics.stable_head_hash,
            runtime_header_changed: previous.diagnostics.runtime_header_hash
                != next.diagnostics.runtime_header_hash,
            dynamic_tail_changed: previous.diagnostics.dynamic_tail_hash
                != next.diagnostics.dynamic_tail_hash,
        }
    }

    pub fn cache_stability_report(
        previous: &ContextEnvelope,
        next: &ContextEnvelope,
    ) -> ContextCacheStabilityReport {
        let comparison = Self::compare_stable_head(previous, next);
        let prompt_cache_friendly = comparison.reusable;
        let reason = if !comparison.reusable {
            "stable head changed; provider prompt cache cannot safely reuse the prefix"
        } else if comparison.runtime_header_changed {
            "stable head is reusable; runtime header changed because identity or mode changed"
        } else if comparison.dynamic_tail_changed {
            "stable head and runtime header are reusable; only dynamic tail changed"
        } else {
            "stable head, runtime header, and dynamic tail are unchanged"
        }
        .to_string();
        ContextCacheStabilityReport {
            previous_envelope_id: previous.id.clone(),
            next_envelope_id: next.id.clone(),
            stable_head_reusable: comparison.reusable,
            runtime_header_changed: comparison.runtime_header_changed,
            dynamic_tail_changed: comparison.dynamic_tail_changed,
            prompt_cache_friendly,
            reason,
        }
    }

    pub fn snapshot(envelope: &ContextEnvelope) -> ContextSnapshot {
        let stable_tokens = envelope
            .assembled
            .stable_head
            .iter()
            .map(|text| estimate_tokens(text))
            .sum();
        let runtime_tokens = envelope
            .assembled
            .runtime_header
            .iter()
            .map(|text| estimate_tokens(text))
            .sum();
        let dynamic_tokens = envelope
            .selected
            .iter()
            .map(|item| item.token_estimate)
            .sum();
        ContextSnapshot {
            envelope_id: envelope.id.clone(),
            session_id: envelope.identity.session_id.clone(),
            agent_id: envelope.identity.agent_id.clone(),
            profile: envelope.profile,
            stable_head_hash: envelope.diagnostics.stable_head_hash.clone(),
            runtime_header_hash: envelope.diagnostics.runtime_header_hash.clone(),
            dynamic_tail_hash: envelope.diagnostics.dynamic_tail_hash.clone(),
            total_tokens: envelope.budget.total_tokens,
            used_tokens: envelope.budget.used_tokens,
            segments: vec![
                ContextSegmentSnapshot {
                    kind: ContextSegmentKind::StableHead,
                    hash: envelope.diagnostics.stable_head_hash.clone(),
                    token_estimate: stable_tokens,
                    item_count: envelope.assembled.stable_head.len(),
                },
                ContextSegmentSnapshot {
                    kind: ContextSegmentKind::RuntimeHeader,
                    hash: envelope.diagnostics.runtime_header_hash.clone(),
                    token_estimate: runtime_tokens,
                    item_count: envelope.assembled.runtime_header.len(),
                },
                ContextSegmentSnapshot {
                    kind: ContextSegmentKind::DynamicTail,
                    hash: envelope.diagnostics.dynamic_tail_hash.clone(),
                    token_estimate: dynamic_tokens,
                    item_count: envelope.selected.len(),
                },
            ],
        }
    }

    pub fn snapshot_diff(
        previous: &ContextEnvelope,
        next: &ContextEnvelope,
    ) -> ContextSnapshotDiff {
        let previous_snapshot = Self::snapshot(previous);
        let next_snapshot = Self::snapshot(next);
        let mut changed_segments = Vec::new();
        for previous_segment in &previous_snapshot.segments {
            if let Some(next_segment) = next_snapshot
                .segments
                .iter()
                .find(|segment| segment.kind == previous_segment.kind)
            {
                if previous_segment.hash != next_segment.hash {
                    changed_segments.push(ContextSegmentChange {
                        kind: previous_segment.kind,
                        previous_hash: previous_segment.hash.clone(),
                        next_hash: next_segment.hash.clone(),
                    });
                }
            }
        }
        ContextSnapshotDiff {
            previous_envelope_id: previous.id.clone(),
            next_envelope_id: next.id.clone(),
            stable_head_reusable: previous_snapshot.stable_head_hash
                == next_snapshot.stable_head_hash,
            runtime_header_changed: previous_snapshot.runtime_header_hash
                != next_snapshot.runtime_header_hash,
            dynamic_tail_changed: previous_snapshot.dynamic_tail_hash
                != next_snapshot.dynamic_tail_hash,
            changed_segments,
        }
    }

    pub fn budget_explanation(envelope: &ContextEnvelope) -> ContextBudgetExplanation {
        let mut allocations = Vec::new();
        for lease in &envelope.budget.leases {
            let used_tokens = envelope
                .selected
                .iter()
                .filter(|item| item.source == lease.source)
                .map(|item| item.token_estimate)
                .sum::<u64>();
            let selected_count = envelope
                .selected
                .iter()
                .filter(|item| item.source == lease.source)
                .count();
            let omitted_count = envelope
                .omitted
                .iter()
                .filter(|item| item.source == lease.source)
                .count();
            allocations.push(ContextBudgetAllocation {
                source: lease.source,
                min_tokens: lease.min_tokens,
                target_tokens: lease.target_tokens,
                max_tokens: lease.max_tokens,
                used_tokens,
                selected_count,
                omitted_count,
                exhausted: omitted_count > 0 || used_tokens >= lease.max_tokens,
                priority: lease.priority,
            });
        }
        ContextBudgetExplanation {
            total_tokens: envelope.budget.total_tokens,
            used_tokens: envelope.budget.used_tokens,
            pressure_bp: envelope.diagnostics.pressure_bp,
            allocations,
        }
    }

    pub fn agent_context_view(
        parent: &ContextEnvelope,
        lease: AgentContextLease,
    ) -> AgentContextView {
        let mut inherited = Vec::new();
        let mut isolated_omissions = Vec::new();
        for item in &parent.selected {
            if !lease.allowed_sources.contains(&item.source) {
                continue;
            }
            if item.visibility == ContextVisibility::Private
                && item.authority == ContextAuthority::Agent
                && item.id != lease.child_agent_id
            {
                isolated_omissions.push(ContextOmission {
                    source: item.source,
                    reason: format!(
                        "private agent context isolated from {}",
                        lease.child_agent_id
                    ),
                    token_estimate: item.token_estimate,
                });
                continue;
            }
            if item.visibility != ContextVisibility::Private
                || matches!(
                    item.source,
                    ContextSourceKind::Task | ContextSourceKind::Workspace
                )
            {
                inherited.push(item.clone());
            }
        }

        let mut contract_item = ContextItem::new(
            format!("agent-contract:{}", lease.child_agent_id),
            ContextSourceKind::Task,
            ContextRole::TaskState,
            lease.task_contract.clone(),
        );
        contract_item.authority = ContextAuthority::System;
        contract_item.visibility = ContextVisibility::Private;
        let mut dynamic_items = vec![contract_item];
        dynamic_items.extend(inherited.clone());

        let mut identity = Self::child_identity_from_lease(&lease);
        identity.project_id = parent.identity.project_id.clone();
        identity.task_id = parent.identity.task_id.clone();
        identity.team_id = parent.identity.team_id.clone();
        let runtime_header = Self::runtime_header(&identity, ContextProfile::SubAgent);
        let envelope = Self::build_envelope(ContextEnvelopeRequest {
            identity,
            profile: ContextProfile::SubAgent,
            intent: parent.intent.clone(),
            stable_head: parent.assembled.stable_head.clone(),
            runtime_header,
            dynamic_items,
            omitted: isolated_omissions.clone(),
            total_budget_tokens: lease.max_tokens.max(1),
        });
        AgentContextView {
            child_agent_id: lease.child_agent_id,
            parent_agent_id: lease.parent_agent_id,
            inherited_item_ids: inherited.into_iter().map(|item| item.id).collect(),
            envelope,
            isolated_omissions,
        }
    }

    pub fn mode_coverage_report(
        session_id: impl Into<String>,
        intent: impl Into<String>,
        stable_head: Vec<String>,
        dynamic_items: Vec<ContextItem>,
        total_budget_tokens: u64,
    ) -> ContextModeCoverageReport {
        let session_id = session_id.into();
        let intent = intent.into();
        let required_profiles = Self::required_profiles();
        let mut entries = Vec::with_capacity(required_profiles.len());
        let mut reference_stable_hash = None::<String>;

        for profile in &required_profiles {
            let mut identity = ContextIdentity::main(session_id.clone());
            identity.mode = Self::mode_for_profile(*profile);
            if matches!(profile, ContextProfile::SubAgent) {
                identity.agent_id = "sub-agent".to_string();
                identity.parent_agent_id = Some("primary".to_string());
            }
            let envelope = Self::build_envelope(ContextEnvelopeRequest {
                profile: *profile,
                runtime_header: Self::runtime_header(&identity, *profile),
                identity,
                intent: intent.clone(),
                stable_head: stable_head.clone(),
                dynamic_items: dynamic_items.clone(),
                omitted: Vec::new(),
                total_budget_tokens,
            });
            let stable_head_hash = envelope.diagnostics.stable_head_hash.clone();
            let reference_hash =
                reference_stable_hash.get_or_insert_with(|| stable_head_hash.clone());
            let envelope_id = envelope.id.clone();
            let runtime_header_hash = envelope.diagnostics.runtime_header_hash.clone();
            let dynamic_tail_hash = envelope.diagnostics.dynamic_tail_hash.clone();
            let selected_count = envelope.selected.len();
            let omitted_count = envelope.omitted.len();
            let pressure_bp = envelope.diagnostics.pressure_bp;
            entries.push(ContextModeCoverageEntry {
                profile: *profile,
                mode: Self::mode_for_profile(*profile),
                envelope_id,
                stable_head_hash,
                runtime_header_hash,
                dynamic_tail_hash,
                stable_head_reusable: *reference_hash == envelope.diagnostics.stable_head_hash,
                selected_count,
                omitted_count,
                pressure_bp,
            });
        }

        let covered_profiles = entries
            .iter()
            .map(|entry| entry.profile)
            .collect::<Vec<_>>();
        let all_profiles_covered = required_profiles
            .iter()
            .all(|profile| covered_profiles.contains(profile));
        let all_stable_heads_reusable = entries.iter().all(|entry| entry.stable_head_reusable);

        ContextModeCoverageReport {
            required_profiles,
            covered_profiles,
            stable_head_hash: reference_stable_hash.unwrap_or_default(),
            all_profiles_covered,
            all_stable_heads_reusable,
            entries,
        }
    }

    pub fn pressure_level(pressure_bp: u16) -> ContextPressureLevel {
        pressure_level_for_bp(pressure_bp)
    }

    pub fn degradation_path(envelope: &ContextEnvelope) -> ContextDegradationPath {
        degradation_path_for(
            envelope.diagnostics.pressure_bp,
            envelope.omitted.len(),
            &envelope.diagnostics.degraded_sources,
        )
    }

    pub fn apply_leases(
        items: Vec<ContextItem>,
        leases: &[ContextLease],
    ) -> (Vec<ContextItem>, Vec<ContextOmission>) {
        let mut ranked = items
            .iter()
            .enumerate()
            .collect::<Vec<(usize, &ContextItem)>>();
        ranked.sort_by(|(_, a), (_, b)| {
            source_priority(b.source, leases)
                .cmp(&source_priority(a.source, leases))
                .then_with(|| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| a.id.cmp(&b.id))
        });

        let mut used_by_source = std::collections::BTreeMap::<String, u64>::new();
        let mut selected_indexes = Vec::new();
        let mut omitted = Vec::new();

        for (index, item) in ranked {
            let lease = leases.iter().find(|lease| lease.source == item.source);
            let max_tokens = lease.map(|lease| lease.max_tokens).unwrap_or(u64::MAX);
            let key = format!("{:?}", item.source);
            let used = used_by_source.get(&key).copied().unwrap_or(0);
            if used.saturating_add(item.token_estimate) > max_tokens {
                omitted.push(ContextOmission {
                    source: item.source,
                    reason: "context lease exhausted".to_string(),
                    token_estimate: item.token_estimate,
                });
                continue;
            }
            used_by_source.insert(key, used.saturating_add(item.token_estimate));
            selected_indexes.push(index);
        }

        selected_indexes.sort_unstable();
        let selected = selected_indexes
            .into_iter()
            .filter_map(|index| items.get(index).cloned())
            .collect();
        (selected, omitted)
    }

    pub fn default_leases(profile: ContextProfile, total_budget_tokens: u64) -> Vec<ContextLease> {
        let budget = total_budget_tokens.max(1);
        let pct = |basis_points: u64| budget.saturating_mul(basis_points) / 10_000;
        match profile {
            ContextProfile::SubAgent => vec![
                context_lease(
                    ContextSourceKind::Task,
                    pct(1_000),
                    pct(2_000),
                    pct(3_000),
                    95,
                ),
                context_lease(
                    ContextSourceKind::Knowledge,
                    pct(500),
                    pct(1_200),
                    pct(2_000),
                    82,
                ),
                context_lease(ContextSourceKind::Fact, 0, pct(700), pct(1_200), 81),
                context_lease(ContextSourceKind::Matrix, 0, pct(700), pct(1_200), 79),
                context_lease(
                    ContextSourceKind::Memory,
                    pct(1_000),
                    pct(2_000),
                    pct(3_000),
                    80,
                ),
                context_lease(ContextSourceKind::ToolTrace, 0, pct(1_000), pct(1_500), 70),
                context_lease(ContextSourceKind::Workspace, 0, pct(800), pct(1_200), 65),
                context_lease(ContextSourceKind::AgentPeer, 0, pct(500), pct(800), 40),
            ],
            ContextProfile::YoloGoal | ContextProfile::SoloGoal => vec![
                context_lease(
                    ContextSourceKind::Task,
                    pct(1_500),
                    pct(2_500),
                    pct(3_500),
                    100,
                ),
                context_lease(
                    ContextSourceKind::ToolTrace,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    85,
                ),
                context_lease(
                    ContextSourceKind::Knowledge,
                    pct(500),
                    pct(1_200),
                    pct(2_000),
                    82,
                ),
                context_lease(ContextSourceKind::Fact, 0, pct(800), pct(1_400), 81),
                context_lease(ContextSourceKind::Matrix, 0, pct(800), pct(1_400), 80),
                context_lease(
                    ContextSourceKind::Memory,
                    pct(1_000),
                    pct(2_000),
                    pct(3_000),
                    80,
                ),
                context_lease(
                    ContextSourceKind::Workspace,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    75,
                ),
                context_lease(ContextSourceKind::AgentPeer, 0, pct(1_000), pct(1_500), 55),
            ],
            ContextProfile::Review => vec![
                context_lease(
                    ContextSourceKind::ToolTrace,
                    pct(1_000),
                    pct(2_500),
                    pct(3_500),
                    100,
                ),
                context_lease(
                    ContextSourceKind::Workspace,
                    pct(1_000),
                    pct(2_500),
                    pct(3_500),
                    95,
                ),
                context_lease(
                    ContextSourceKind::Task,
                    pct(500),
                    pct(1_500),
                    pct(2_000),
                    85,
                ),
                context_lease(
                    ContextSourceKind::Knowledge,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    88,
                ),
                context_lease(ContextSourceKind::Fact, 0, pct(1_000), pct(1_500), 86),
                context_lease(ContextSourceKind::Matrix, 0, pct(1_000), pct(1_500), 84),
                context_lease(
                    ContextSourceKind::Memory,
                    pct(500),
                    pct(1_500),
                    pct(2_000),
                    70,
                ),
                context_lease(ContextSourceKind::AgentPeer, 0, pct(1_000), pct(1_500), 65),
            ],
            ContextProfile::Resume => vec![
                context_lease(
                    ContextSourceKind::Handoff,
                    pct(1_000),
                    pct(2_000),
                    pct(3_000),
                    100,
                ),
                context_lease(
                    ContextSourceKind::Conversation,
                    pct(1_000),
                    pct(2_000),
                    pct(3_000),
                    95,
                ),
                context_lease(
                    ContextSourceKind::Task,
                    pct(500),
                    pct(1_500),
                    pct(2_000),
                    90,
                ),
                context_lease(
                    ContextSourceKind::Knowledge,
                    pct(500),
                    pct(1_200),
                    pct(2_000),
                    84,
                ),
                context_lease(ContextSourceKind::Fact, 0, pct(800), pct(1_200), 82),
                context_lease(ContextSourceKind::Matrix, 0, pct(800), pct(1_200), 80),
                context_lease(
                    ContextSourceKind::Memory,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    80,
                ),
                context_lease(ContextSourceKind::Workspace, 0, pct(800), pct(1_200), 60),
            ],
            ContextProfile::Collaboration => vec![
                context_lease(
                    ContextSourceKind::AgentPeer,
                    pct(1_000),
                    pct(2_500),
                    pct(3_500),
                    100,
                ),
                context_lease(
                    ContextSourceKind::Task,
                    pct(500),
                    pct(1_500),
                    pct(2_000),
                    90,
                ),
                context_lease(
                    ContextSourceKind::Knowledge,
                    pct(500),
                    pct(1_200),
                    pct(2_000),
                    84,
                ),
                context_lease(ContextSourceKind::Fact, 0, pct(800), pct(1_200), 82),
                context_lease(ContextSourceKind::Matrix, 0, pct(800), pct(1_200), 80),
                context_lease(
                    ContextSourceKind::Memory,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    80,
                ),
                context_lease(ContextSourceKind::ToolTrace, 0, pct(1_000), pct(1_500), 70),
                context_lease(ContextSourceKind::Workspace, 0, pct(1_000), pct(1_500), 65),
            ],
            ContextProfile::SurfaceQuickReply => vec![
                context_lease(ContextSourceKind::Conversation, 0, pct(800), pct(1_200), 95),
                context_lease(ContextSourceKind::Memory, 0, pct(700), pct(1_000), 85),
                context_lease(ContextSourceKind::Knowledge, 0, pct(400), pct(700), 78),
                context_lease(ContextSourceKind::Fact, 0, pct(300), pct(500), 76),
                context_lease(ContextSourceKind::Matrix, 0, pct(300), pct(500), 74),
                context_lease(ContextSourceKind::Task, 0, pct(500), pct(800), 75),
                context_lease(ContextSourceKind::ToolTrace, 0, pct(400), pct(600), 60),
                context_lease(ContextSourceKind::Workspace, 0, pct(300), pct(500), 40),
                context_lease(ContextSourceKind::AgentPeer, 0, pct(200), pct(300), 30),
            ],
            ContextProfile::SurfaceTaskIntake => vec![
                context_lease(
                    ContextSourceKind::Conversation,
                    pct(500),
                    pct(1_500),
                    pct(2_000),
                    95,
                ),
                context_lease(
                    ContextSourceKind::Task,
                    pct(500),
                    pct(1_500),
                    pct(2_000),
                    90,
                ),
                context_lease(ContextSourceKind::Memory, 0, pct(1_000), pct(1_500), 80),
                context_lease(ContextSourceKind::Knowledge, 0, pct(1_000), pct(1_500), 82),
                context_lease(ContextSourceKind::Fact, 0, pct(700), pct(1_000), 81),
                context_lease(ContextSourceKind::Matrix, 0, pct(700), pct(1_000), 79),
                context_lease(ContextSourceKind::ToolTrace, 0, pct(700), pct(1_000), 60),
                context_lease(ContextSourceKind::Workspace, 0, pct(700), pct(1_000), 55),
            ],
            ContextProfile::DeepInvestigation => vec![
                context_lease(
                    ContextSourceKind::ToolTrace,
                    pct(1_000),
                    pct(2_500),
                    pct(3_500),
                    100,
                ),
                context_lease(
                    ContextSourceKind::Workspace,
                    pct(1_000),
                    pct(2_500),
                    pct(3_500),
                    95,
                ),
                context_lease(
                    ContextSourceKind::Task,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    90,
                ),
                context_lease(
                    ContextSourceKind::Knowledge,
                    pct(500),
                    pct(2_000),
                    pct(3_000),
                    92,
                ),
                context_lease(
                    ContextSourceKind::Fact,
                    pct(300),
                    pct(1_500),
                    pct(2_500),
                    90,
                ),
                context_lease(
                    ContextSourceKind::Matrix,
                    pct(300),
                    pct(1_500),
                    pct(2_500),
                    88,
                ),
                context_lease(
                    ContextSourceKind::Memory,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    80,
                ),
                context_lease(ContextSourceKind::AgentPeer, 0, pct(1_000), pct(1_500), 65),
            ],
            ContextProfile::Cron | ContextProfile::MainTurn => vec![
                context_lease(
                    ContextSourceKind::Conversation,
                    pct(1_000),
                    pct(2_500),
                    pct(3_500),
                    95,
                ),
                context_lease(
                    ContextSourceKind::Knowledge,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    88,
                ),
                context_lease(ContextSourceKind::Fact, 0, pct(1_000), pct(1_500), 86),
                context_lease(ContextSourceKind::Matrix, 0, pct(1_000), pct(1_500), 84),
                context_lease(
                    ContextSourceKind::Memory,
                    pct(1_000),
                    pct(2_000),
                    pct(3_000),
                    85,
                ),
                context_lease(
                    ContextSourceKind::Task,
                    pct(500),
                    pct(1_500),
                    pct(2_500),
                    80,
                ),
                context_lease(ContextSourceKind::ToolTrace, 0, pct(1_000), pct(1_500), 70),
                context_lease(ContextSourceKind::Workspace, 0, pct(1_000), pct(1_500), 65),
                context_lease(ContextSourceKind::AgentPeer, 0, pct(800), pct(1_200), 50),
                context_lease(ContextSourceKind::Handoff, 0, pct(800), pct(1_200), 50),
            ],
        }
    }

    pub fn child_identity_from_lease(lease: &AgentContextLease) -> ContextIdentity {
        ContextIdentity::sub_agent(
            lease.parent_session_id.clone(),
            lease.child_agent_id.clone(),
            lease.parent_agent_id.clone(),
        )
    }

    pub fn agent_return_item(packet: &AgentReturnContextProjection) -> ContextItem {
        let mut content = format!(
            "Agent {} returned: {}",
            packet.child_agent_id, packet.result_summary
        );
        if !packet.decisions.is_empty() {
            content.push_str("\nDecisions:\n");
            for decision in &packet.decisions {
                content.push_str("- ");
                content.push_str(decision);
                content.push('\n');
            }
        }
        if !packet.conflicts.is_empty() {
            content.push_str("\nConflicts:\n");
            for conflict in &packet.conflicts {
                content.push_str("- ");
                content.push_str(conflict);
                content.push('\n');
            }
        }
        let mut item = ContextItem::new(
            format!("agent-return:{}", packet.child_agent_id),
            ContextSourceKind::AgentPeer,
            if packet.failed {
                ContextRole::Warning
            } else {
                ContextRole::Evidence
            },
            content,
        );
        item.authority = ContextAuthority::Agent;
        item.visibility = ContextVisibility::Shared;
        item.evidence = packet
            .evidence
            .iter()
            .map(|evidence| format!("agent://{}/evidence/{}", packet.child_agent_id, evidence))
            .collect();
        item
    }

    pub fn tool_trace_item(packet: &ToolTracePacket) -> ContextItem {
        let mut item = ContextItem::new(
            format!("tool-trace:{}", packet.invocation_id),
            ContextSourceKind::ToolTrace,
            ContextRole::ToolSummary,
            format!(
                "{} {:?}: {}",
                packet.tool_name, packet.status, packet.summary
            ),
        );
        item.authority = ContextAuthority::Tool;
        item.evidence = packet
            .evidence_ids
            .iter()
            .map(|id| format!("tool://{}/evidence/{id}", packet.invocation_id))
            .collect();
        item.evidence.extend(
            packet
                .changed_files
                .iter()
                .map(|file| format!("workspace://changed-file/{file}")),
        );
        item.token_estimate = packet.token_estimate;
        item
    }

    pub fn workspace_item(packet: &WorkspacePacket) -> ContextItem {
        let mut parts = vec![format!("Workspace root: {}", packet.root)];
        if !packet.touched_files.is_empty() {
            parts.push(format!(
                "Touched files: {}",
                packet.touched_files.join("; ")
            ));
        }
        if !packet.hot_symbols.is_empty() {
            parts.push(format!("Hot symbols: {}", packet.hot_symbols.join("; ")));
        }
        if !packet.project_notes.is_empty() {
            parts.push(format!(
                "Project notes: {}",
                packet.project_notes.join("; ")
            ));
        }
        let mut item = ContextItem::new(
            format!("workspace:{}", stable_hash_bytes(packet.root.as_bytes())),
            ContextSourceKind::Workspace,
            ContextRole::Evidence,
            parts.join("\n"),
        );
        item.authority = ContextAuthority::Project;
        item.visibility = ContextVisibility::Shared;
        item.evidence = std::iter::once(format!("workspace://root/{}", packet.root))
            .chain(
                packet
                    .touched_files
                    .iter()
                    .map(|file| format!("workspace://changed-file/{file}")),
            )
            .chain(
                packet
                    .hot_symbols
                    .iter()
                    .map(|symbol| format!("workspace://symbol/{symbol}")),
            )
            .collect();
        item.token_estimate = packet.token_estimate;
        item
    }

    pub fn resume_item(packet: &ResumeContextPacket) -> ContextItem {
        let mut parts = Vec::new();
        if let Some(summary) = &packet.handoff_summary {
            parts.push(format!("Handoff: {summary}"));
        }
        if let Some(task) = &packet.active_task {
            parts.push(format!("Active task: {task}"));
        }
        if !packet.recent_decisions.is_empty() {
            parts.push(format!("Decisions: {}", packet.recent_decisions.join("; ")));
        }
        if !packet.blockers.is_empty() {
            parts.push(format!("Blockers: {}", packet.blockers.join("; ")));
        }
        let source = match packet.source {
            ResumeContextSource::SessionDb => ContextSourceKind::Conversation,
            ResumeContextSource::Handoff | ResumeContextSource::Mixed => ContextSourceKind::Handoff,
            ResumeContextSource::TaskRegistry => ContextSourceKind::Task,
        };
        let mut item = ContextItem::new(
            format!("resume:{}", packet.session_id),
            source,
            ContextRole::TaskState,
            parts.join("\n"),
        );
        item.authority = ContextAuthority::Session;
        item.evidence = vec![format!(
            "session://{}/resume/{:?}",
            packet.session_id, packet.source
        )];
        item
    }
}

fn context_lease(
    source: ContextSourceKind,
    min_tokens: u64,
    target_tokens: u64,
    max_tokens: u64,
    priority: u8,
) -> ContextLease {
    ContextLease {
        source,
        min_tokens,
        target_tokens,
        max_tokens,
        priority,
        degradation: vec!["omit lower score context items".to_string()],
    }
}

fn source_priority(source: ContextSourceKind, leases: &[ContextLease]) -> u8 {
    leases
        .iter()
        .find(|lease| lease.source == source)
        .map(|lease| lease.priority)
        .unwrap_or(0)
}

fn estimate_tokens(text: &str) -> u64 {
    (text.len() as u64).div_ceil(4).max(1)
}

fn context_recommendations(
    pressure_bp: u16,
    selected_count: usize,
    omitted_count: usize,
) -> Vec<String> {
    let mut recommendations = Vec::new();
    if pressure_bp >= 9_000 {
        recommendations.push(
            "Start a handoff or session boundary before adding more large context".to_string(),
        );
        recommendations
            .push("Prefer summarized tool traces and memory packets over raw evidence".to_string());
    } else if pressure_bp >= 7_000 {
        recommendations
            .push("Review omitted context and compact low-value recent turns".to_string());
    }
    if omitted_count > 0 {
        recommendations.push(format!(
            "{omitted_count} context items were omitted; inspect omissions before relying on recall completeness"
        ));
    }
    if selected_count == 0 {
        recommendations.push(
            "No dynamic context selected; verify memory/session/task sources are available"
                .to_string(),
        );
    }
    recommendations
}

fn pressure_level_for_bp(pressure_bp: u16) -> ContextPressureLevel {
    if pressure_bp >= 9_000 {
        ContextPressureLevel::Critical
    } else if pressure_bp >= 8_500 {
        ContextPressureLevel::High
    } else if pressure_bp >= 7_000 {
        ContextPressureLevel::Elevated
    } else {
        ContextPressureLevel::Nominal
    }
}

fn degradation_path_for(
    pressure_bp: u16,
    omitted_count: usize,
    degraded_sources: &[ContextSourceKind],
) -> ContextDegradationPath {
    if pressure_bp >= 9_000 {
        ContextDegradationPath::HandoffBoundary
    } else if pressure_bp >= 8_500 {
        ContextDegradationPath::SummarizeEvidence
    } else if pressure_bp >= 7_000 || omitted_count > 0 {
        ContextDegradationPath::TrimDynamicTail
    } else if !degraded_sources.is_empty() {
        ContextDegradationPath::SourceFallback
    } else {
        ContextDegradationPath::None
    }
}

fn context_policy_action(probe: &ContextLeanProbe) -> (ContextPolicyAction, String) {
    if matches!(
        probe.degradation_path,
        ContextDegradationPath::SourceFallback
    ) {
        return (
            ContextPolicyAction::PreferOrientationPacket,
            "source fallback detected; prefer compact orientation before broad recall".to_string(),
        );
    }

    match (probe.pressure_level, probe.profile) {
        (ContextPressureLevel::Critical, ContextProfile::Review) => (
            ContextPolicyAction::SummarizeEvidence,
            "critical review pressure; keep evidence references and summarize bulky detail"
                .to_string(),
        ),
        (ContextPressureLevel::Critical, ContextProfile::YoloGoal | ContextProfile::SoloGoal) => (
            ContextPolicyAction::WriteHandoff,
            "critical goal pressure; preserve active task state with a handoff boundary"
                .to_string(),
        ),
        (ContextPressureLevel::Critical, _) => (
            ContextPolicyAction::RecommendSessionBoundary,
            "critical context pressure; recommend explicit session boundary".to_string(),
        ),
        (ContextPressureLevel::High, ContextProfile::Review) => (
            ContextPolicyAction::SummarizeEvidence,
            "high review pressure; summarize evidence bodies while retaining refs".to_string(),
        ),
        (ContextPressureLevel::High, ContextProfile::YoloGoal | ContextProfile::SoloGoal) => (
            ContextPolicyAction::TrimToolTrace,
            "high goal pressure; trim tool trace before task and memory context".to_string(),
        ),
        (ContextPressureLevel::High, _) => (
            ContextPolicyAction::TrimToolTrace,
            "high context pressure; trim low-value tool trace first".to_string(),
        ),
        (ContextPressureLevel::Elevated, _) if probe.omitted_count > 0 => (
            ContextPolicyAction::PreferOrientationPacket,
            "elevated pressure with omissions; prefer compact orientation packet".to_string(),
        ),
        _ => (
            ContextPolicyAction::None,
            "context pressure nominal; no policy action required".to_string(),
        ),
    }
}

fn context_policy_action_name(action: ContextPolicyAction) -> &'static str {
    match action {
        ContextPolicyAction::None => "none",
        ContextPolicyAction::TrimToolTrace => "trim_tool_trace",
        ContextPolicyAction::SummarizeEvidence => "summarize_evidence",
        ContextPolicyAction::PreferOrientationPacket => "prefer_orientation_packet",
        ContextPolicyAction::WriteHandoff => "write_handoff",
        ContextPolicyAction::RecommendSessionBoundary => "recommend_session_boundary",
    }
}

fn safe_to_auto_apply(action: ContextPolicyAction) -> bool {
    matches!(
        action,
        ContextPolicyAction::TrimToolTrace
            | ContextPolicyAction::PreferOrientationPacket
            | ContextPolicyAction::RecommendSessionBoundary
            | ContextPolicyAction::None
    )
}

fn expected_saving_tokens(probe: &ContextLeanProbe, action: ContextPolicyAction) -> u64 {
    let pressure_over_target = probe
        .budget_used_tokens
        .saturating_sub(probe.budget_total_tokens.saturating_mul(7) / 10);
    match action {
        ContextPolicyAction::None => 0,
        ContextPolicyAction::TrimToolTrace => {
            pressure_over_target.max(probe.budget_total_tokens / 20)
        }
        ContextPolicyAction::PreferOrientationPacket => {
            pressure_over_target.max(probe.budget_total_tokens / 25)
        }
        ContextPolicyAction::RecommendSessionBoundary => pressure_over_target,
        ContextPolicyAction::SummarizeEvidence => {
            pressure_over_target.max(probe.budget_total_tokens / 10)
        }
        ContextPolicyAction::WriteHandoff => {
            pressure_over_target.max(probe.budget_total_tokens / 8)
        }
    }
}

fn confidence_for_policy(probe: &ContextLeanProbe, action: ContextPolicyAction) -> f32 {
    if action == ContextPolicyAction::None {
        return 1.0;
    }
    let pressure = f32::from(probe.pressure_bp) / 10_000.0;
    let omission_bonus = (probe.omitted_count as f32 * 0.03).min(0.15);
    (0.45 + pressure * 0.4 + omission_bonus).clamp(0.1, 0.95)
}

fn affected_sources_for_action(
    action: ContextPolicyAction,
    probe: &ContextLeanProbe,
) -> Vec<ContextSourceKind> {
    if !probe.degraded_sources.is_empty() {
        return probe.degraded_sources.clone();
    }
    match action {
        ContextPolicyAction::TrimToolTrace => vec![ContextSourceKind::ToolTrace],
        ContextPolicyAction::SummarizeEvidence => {
            vec![ContextSourceKind::Workspace, ContextSourceKind::ToolTrace]
        }
        ContextPolicyAction::PreferOrientationPacket => vec![ContextSourceKind::Memory],
        ContextPolicyAction::WriteHandoff => {
            vec![ContextSourceKind::Handoff, ContextSourceKind::Conversation]
        }
        ContextPolicyAction::RecommendSessionBoundary | ContextPolicyAction::None => Vec::new(),
    }
}

fn hash_segments(segments: &[String]) -> String {
    let mut bytes = Vec::new();
    for segment in segments {
        bytes.extend_from_slice(segment.as_bytes());
        bytes.push(0);
    }
    format!("{:016x}", stable_hash_bytes(&bytes))
}

fn envelope_id(
    identity: &ContextIdentity,
    intent: &str,
    diagnostics: &ContextDiagnostics,
) -> String {
    let raw = format!(
        "{}:{}:{:?}:{}:{}:{}",
        identity.session_id,
        identity.agent_id,
        identity.mode,
        intent,
        diagnostics.runtime_header_hash,
        diagnostics.dynamic_tail_hash
    );
    format!("{:016x}", stable_hash_bytes(raw.as_bytes()))
}

fn context_epoch_id(envelope_id: &str) -> String {
    format!("ctx-epoch-{envelope_id}")
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
            "selected_for_{:?}_profile_from_{:?}",
            item.role, item.source
        ));
    }
    item
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
        ContextSourceKind::Conversation
        | ContextSourceKind::Task
        | ContextSourceKind::ToolTrace
        | ContextSourceKind::AgentPeer
        | ContextSourceKind::Handoff => ContextSourceLifecycle::Runtime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fact_extraction::{
        RuleFactExtractor, RuntimeFactExtractionInput, RuntimeFactExtractionTrigger,
        RuntimeFactExtractor,
    };

    fn request_with_dynamic(content: &str) -> ContextEnvelopeRequest {
        let identity = ContextIdentity::main("session-1");
        ContextEnvelopeRequest {
            profile: ContextProfile::from(identity.mode),
            identity,
            intent: "ship context runtime".to_string(),
            stable_head: vec!["system: stable instructions".to_string()],
            runtime_header: vec!["runtime: main session".to_string()],
            dynamic_items: vec![ContextItem::new(
                "memory-1",
                ContextSourceKind::Memory,
                ContextRole::Orientation,
                content,
            )],
            omitted: Vec::new(),
            total_budget_tokens: 10_000,
        }
    }

    fn item_with_tokens(
        id: &str,
        source: ContextSourceKind,
        role: ContextRole,
        token_estimate: u64,
    ) -> ContextItem {
        let mut item = ContextItem::new(id, source, role, id);
        item.token_estimate = token_estimate;
        item
    }

    fn probe_for_policy(
        profile: ContextProfile,
        pressure_level: ContextPressureLevel,
        degradation_path: ContextDegradationPath,
        omitted_count: usize,
    ) -> ContextLeanProbe {
        ContextLeanProbe {
            envelope_id: "env-policy".to_string(),
            profile,
            stable_head_hash: "stable-head".to_string(),
            runtime_header_hash: "runtime-header".to_string(),
            dynamic_tail_hash: "dynamic-tail".to_string(),
            selected_count: 3,
            omitted_count,
            budget_total_tokens: 10_000,
            budget_used_tokens: match pressure_level {
                ContextPressureLevel::Nominal => 4_000,
                ContextPressureLevel::Elevated => 7_500,
                ContextPressureLevel::High => 8_700,
                ContextPressureLevel::Critical => 9_500,
            },
            pressure_bp: match pressure_level {
                ContextPressureLevel::Nominal => 4_000,
                ContextPressureLevel::Elevated => 7_500,
                ContextPressureLevel::High => 8_700,
                ContextPressureLevel::Critical => 9_500,
            },
            pressure_level,
            degradation_path,
            degraded_sources: Vec::new(),
        }
    }

    #[test]
    fn envelope_preserves_prompt_segment_order() {
        let envelope = ContextRuntimeKernel::build_envelope(request_with_dynamic("dynamic memory"));
        let prompt = envelope.assembled.system_prompt();

        assert_eq!(prompt[0], "system: stable instructions");
        assert_eq!(prompt[1], "runtime: main session");
        assert!(prompt[2].contains("dynamic memory"));
    }

    #[test]
    fn governance_report_explains_memory_knowledge_and_fact_decisions() {
        let mut request = request_with_dynamic("FACT: deployment requires review");
        request.omitted.push(ContextOmission {
            source: ContextSourceKind::Memory,
            reason: "suppressed_for_current_turn: current user rule overrides stale memory"
                .to_string(),
            token_estimate: 12,
        });
        let envelope = ContextRuntimeKernel::build_envelope(request);
        let knowledge = KnowledgeTurnReport {
            activation_plan_id: Some("plan-1".to_string()),
            active_pack_ids: vec!["pack-a".to_string()],
            blocked_namespaces: vec!["global/noisy".to_string()],
            compliance_warnings: Vec::new(),
            evidence_refs: vec![harness_contract::core::KernelRef::new(
                "knowledge_pack",
                "pack-a",
            )],
            usage_signals: Vec::new(),
        };
        let report = ContextRuntimeKernel::governance_report(
            &envelope,
            Some(&knowledge),
            Some(RuntimeContextFactDecision {
                trigger: "TurnEnd".to_string(),
                mode: "rule_only".to_string(),
                degraded: false,
                reason: "user rule update".to_string(),
                candidate_count: 1,
                review_required: true,
            }),
            Some(RuntimeCompressionCheckpointRef {
                checkpoint_id: "checkpoint-a".to_string(),
                source: "summary_compression".to_string(),
                summary: "checkpoint summary".to_string(),
                evidence_refs: vec!["evidence-a".to_string()],
            }),
        );

        assert_eq!(report.envelope_id, envelope.id);
        assert_eq!(report.context_epoch, envelope.epoch_id);
        assert_eq!(report.selected_memory.len(), 1);
        assert_eq!(report.omitted_memory.len(), 1);
        assert_eq!(report.knowledge.activated_pack_ids, vec!["pack-a"]);
        assert_eq!(
            report
                .fact_extraction
                .as_ref()
                .expect("fact decision")
                .candidate_count,
            1
        );
        assert!(report
            .contamination_notes
            .iter()
            .any(|note| note.contains("suppressed_for_current_turn")));
        assert_eq!(
            report
                .compression_checkpoint
                .as_ref()
                .expect("checkpoint")
                .checkpoint_id,
            "checkpoint-a"
        );
    }

    #[test]
    fn reality_runtime_decision_unifies_recall_knowledge_fact_and_checkpoint() {
        let mut request = request_with_dynamic("FACT: deployment requires review");
        request.omitted.push(ContextOmission {
            source: ContextSourceKind::Memory,
            reason: "suppressed_for_current_turn: cross-project stale deployment memory"
                .to_string(),
            token_estimate: 256,
        });
        let envelope = ContextRuntimeKernel::build_envelope(request);
        let knowledge = KnowledgeTurnReport {
            activation_plan_id: Some("plan-1".to_string()),
            active_pack_ids: vec!["pack-a".to_string()],
            blocked_namespaces: vec!["global/noisy".to_string()],
            compliance_warnings: Vec::new(),
            evidence_refs: vec![harness_contract::core::KernelRef::new(
                "knowledge_pack",
                "pack-a",
            )],
            usage_signals: Vec::new(),
        };
        let governance = ContextRuntimeKernel::governance_report(
            &envelope,
            Some(&knowledge),
            Some(RuntimeContextFactDecision {
                trigger: "TurnEnd".to_string(),
                mode: "rule_only".to_string(),
                degraded: false,
                reason: "turn produced fact candidate".to_string(),
                candidate_count: 1,
                review_required: true,
            }),
            Some(RuntimeCompressionCheckpointRef {
                checkpoint_id: "checkpoint-a".to_string(),
                source: "summary_compression".to_string(),
                summary: "checkpoint summary".to_string(),
                evidence_refs: vec!["evidence-a".to_string()],
            }),
        );
        let input = RuntimeFactExtractionInput::new(
            RuntimeFactExtractionTrigger::TurnEnd,
            "FACT: deployment requires review",
        )
        .with_session_id(Some(governance.session_id.clone()))
        .with_evidence_refs(vec!["evidence-a".to_string()]);
        let batch = RuleFactExtractor.extract(&input);
        let mut fact_service = fact_kernel::FactKernelService::new();
        let review = fact_service.review_candidates(batch.clone());

        let decision = crate::RealityRuntimeDecision::from_governance(
            &governance,
            Some(&batch),
            Some(&review),
        );

        assert_eq!(decision.kind, "runtime.reality_runtime_decision");
        assert_eq!(decision.recall_quality.selected_count, 1);
        assert_eq!(decision.recall_quality.suppressed_count, 1);
        assert!(decision.recall_quality.cross_project_contamination);
        assert_eq!(decision.knowledge.activated_pack_ids, vec!["pack-a"]);
        assert_eq!(decision.fact_plan.candidate_count, 1);
        assert_eq!(decision.fact_plan.promoted_count, 1);
        assert!(decision.context_budget_plan.checkpoint_required);
        assert!(decision
            .resume_pointers
            .iter()
            .any(|pointer| pointer == "checkpoint:checkpoint-a"));
    }

    #[test]
    fn stable_head_hash_survives_dynamic_tail_changes() {
        let a = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory alpha"));
        let b = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory beta"));

        assert_eq!(
            a.diagnostics.stable_head_hash,
            b.diagnostics.stable_head_hash
        );
        assert_eq!(
            a.diagnostics.runtime_header_hash,
            b.diagnostics.runtime_header_hash
        );
        assert_ne!(
            a.diagnostics.dynamic_tail_hash,
            b.diagnostics.dynamic_tail_hash
        );
    }

    #[test]
    fn stable_header_hash_unchanged_for_dialogue_only_change() {
        let previous = ContextRuntimeKernel::build_envelope(request_with_dynamic("turn one"));
        let next = ContextRuntimeKernel::build_envelope(request_with_dynamic("turn two"));
        let diff = ContextRuntimeKernel::snapshot_diff(&previous, &next);

        assert!(diff.stable_head_reusable);
        assert!(!diff.runtime_header_changed);
        assert!(diff.dynamic_tail_changed);
        assert_eq!(diff.changed_segments.len(), 1);
        assert_eq!(
            diff.changed_segments[0].kind,
            ContextSegmentKind::DynamicTail
        );
    }

    #[test]
    fn context_budget_drops_lowest_priority_dynamic_segment() {
        let mut high = item_with_tokens(
            "task-high",
            ContextSourceKind::Task,
            ContextRole::TaskState,
            300,
        );
        high.score = 0.9;
        let mut low = item_with_tokens(
            "memory-low",
            ContextSourceKind::Memory,
            ContextRole::Orientation,
            400,
        );
        low.score = 0.1;
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main("session-budget"),
            profile: ContextProfile::YoloGoal,
            intent: "budget".to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![low, high],
            omitted: Vec::new(),
            total_budget_tokens: 1_000,
        });
        let budget = ContextRuntimeKernel::budget_explanation(&envelope);

        assert!(envelope.selected.iter().any(|item| item.id == "task-high"));
        assert!(envelope
            .omitted
            .iter()
            .any(|item| item.source == ContextSourceKind::Memory));
        assert!(budget
            .allocations
            .iter()
            .any(|item| item.source == ContextSourceKind::Memory && item.omitted_count == 1));
    }

    #[test]
    fn agent_context_view_isolated_but_shares_project_facts() {
        let mut project_fact = ContextItem::new(
            "workspace-fact",
            ContextSourceKind::Workspace,
            ContextRole::Evidence,
            "workspace project fact",
        );
        project_fact.visibility = ContextVisibility::Shared;
        project_fact.authority = ContextAuthority::Project;
        let mut private_peer = ContextItem::new(
            "peer-private",
            ContextSourceKind::AgentPeer,
            ContextRole::Evidence,
            "private peer note",
        );
        private_peer.visibility = ContextVisibility::Private;
        private_peer.authority = ContextAuthority::Agent;
        let parent = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main("session-agent"),
            profile: ContextProfile::Collaboration,
            intent: "delegate".to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![project_fact, private_peer],
            omitted: Vec::new(),
            total_budget_tokens: 10_000,
        });

        let view = ContextRuntimeKernel::agent_context_view(
            &parent,
            AgentContextLease {
                parent_session_id: "session-agent".to_string(),
                parent_agent_id: "primary".to_string(),
                child_agent_id: "reviewer".to_string(),
                task_contract: "review safely".to_string(),
                allowed_sources: vec![ContextSourceKind::Workspace, ContextSourceKind::AgentPeer],
                max_tokens: 4_000,
                required_return: vec![AgentReturnRequirement::Evidence],
            },
        );

        assert_eq!(view.envelope.identity.agent_id, "reviewer");
        assert!(view
            .inherited_item_ids
            .contains(&"workspace-fact".to_string()));
        assert!(!view
            .inherited_item_ids
            .contains(&"peer-private".to_string()));
        assert!(view
            .isolated_omissions
            .iter()
            .any(|item| item.reason.contains("private agent context")));
        assert_eq!(
            view.envelope.diagnostics.stable_head_hash,
            parent.diagnostics.stable_head_hash
        );
    }

    #[test]
    fn context_snapshot_diff_reports_changed_segments() {
        let first = ContextRuntimeKernel::build_envelope(request_with_dynamic("alpha"));
        let mut changed_request = request_with_dynamic("alpha");
        changed_request.runtime_header = vec!["runtime: changed".to_string()];
        changed_request.dynamic_items = vec![ContextItem::new(
            "memory-2",
            ContextSourceKind::Memory,
            ContextRole::Orientation,
            "beta",
        )];
        let second = ContextRuntimeKernel::build_envelope(changed_request);
        let snapshot = ContextRuntimeKernel::snapshot(&second);
        let diff = ContextRuntimeKernel::snapshot_diff(&first, &second);

        assert_eq!(snapshot.segments.len(), 3);
        assert!(diff.stable_head_reusable);
        assert!(diff.runtime_header_changed);
        assert!(diff.dynamic_tail_changed);
        assert_eq!(diff.changed_segments.len(), 2);
    }

    #[test]
    fn stable_head_comparison_tracks_cache_reuse_and_tail_change() {
        let a = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory alpha"));
        let b = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory beta"));
        let comparison = ContextRuntimeKernel::compare_stable_head(&a, &b);

        assert!(comparison.reusable);
        assert!(!comparison.runtime_header_changed);
        assert!(comparison.dynamic_tail_changed);
        assert_eq!(comparison.previous_hash, a.diagnostics.stable_head_hash);

        let mut changed_request = request_with_dynamic("memory beta");
        changed_request.stable_head = vec!["system: changed stable instructions".to_string()];
        let changed = ContextRuntimeKernel::build_envelope(changed_request);
        let changed_comparison = ContextRuntimeKernel::compare_stable_head(&a, &changed);
        assert!(!changed_comparison.reusable);
    }

    #[test]
    fn cache_stability_report_marks_dynamic_only_changes_cache_friendly() {
        let a = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory alpha"));
        let b = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory beta"));

        let report = ContextRuntimeKernel::cache_stability_report(&a, &b);

        assert!(report.stable_head_reusable);
        assert!(!report.runtime_header_changed);
        assert!(report.dynamic_tail_changed);
        assert!(report.prompt_cache_friendly);
        assert!(report.reason.contains("dynamic tail"));
    }

    #[test]
    fn mode_coverage_report_proves_all_profiles_share_stable_head() {
        let report = ContextRuntimeKernel::mode_coverage_report(
            "session-coverage",
            "continue safely",
            vec!["system stable contract".to_string()],
            vec![
                ContextItem::new(
                    "task-1",
                    ContextSourceKind::Task,
                    ContextRole::TaskState,
                    "active task",
                ),
                ContextItem::new(
                    "memory-1",
                    ContextSourceKind::Memory,
                    ContextRole::Orientation,
                    "project memory",
                ),
            ],
            10_000,
        );

        assert_eq!(
            report.covered_profiles.len(),
            ContextRuntimeKernel::required_profiles().len()
        );
        assert!(report.all_profiles_covered);
        assert!(report.all_stable_heads_reusable);
        assert!(report
            .entries
            .iter()
            .any(|entry| entry.profile == ContextProfile::YoloGoal
                && entry.mode == ContextMode::YoloGoal));
        assert!(report
            .entries
            .iter()
            .any(|entry| entry.profile == ContextProfile::SubAgent
                && entry.mode == ContextMode::SubAgent));
    }

    #[test]
    fn lean_probe_reports_critical_pressure_degradation_path() {
        let identity = ContextIdentity::main("session-hot");
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile: ContextProfile::MainTurn,
            identity,
            intent: "stress context pressure".to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![
                item_with_tokens(
                    "conversation-hot",
                    ContextSourceKind::Conversation,
                    ContextRole::RecentTurn,
                    3_000,
                ),
                item_with_tokens(
                    "memory-hot",
                    ContextSourceKind::Memory,
                    ContextRole::Orientation,
                    2_800,
                ),
                item_with_tokens(
                    "task-hot",
                    ContextSourceKind::Task,
                    ContextRole::TaskState,
                    2_000,
                ),
                item_with_tokens(
                    "tool-hot",
                    ContextSourceKind::ToolTrace,
                    ContextRole::ToolSummary,
                    1_000,
                ),
                item_with_tokens(
                    "workspace-hot",
                    ContextSourceKind::Workspace,
                    ContextRole::Evidence,
                    1_000,
                ),
            ],
            omitted: Vec::new(),
            total_budget_tokens: 10_000,
        });

        let probe = ContextRuntimeKernel::lean_probe(&envelope);

        assert_eq!(probe.selected_count, 5);
        assert_eq!(probe.omitted_count, 0);
        assert!(probe.pressure_bp >= 9_000);
        assert_eq!(probe.pressure_level, ContextPressureLevel::Critical);
        assert_eq!(
            probe.degradation_path,
            ContextDegradationPath::HandoffBoundary
        );
        assert_eq!(
            probe.stable_head_hash,
            envelope.diagnostics.stable_head_hash
        );
    }

    #[test]
    fn context_policy_uses_probe_for_safe_degradation() {
        let probe = probe_for_policy(
            ContextProfile::MainTurn,
            ContextPressureLevel::Critical,
            ContextDegradationPath::HandoffBoundary,
            2,
        );

        let decision = ContextRuntimeKernel::policy_decision(&probe);

        assert_eq!(
            decision.action,
            ContextPolicyAction::RecommendSessionBoundary
        );
        assert_eq!(decision.stable_head_hash, probe.stable_head_hash);
        assert_eq!(decision.pressure_level, ContextPressureLevel::Critical);
    }

    #[test]
    fn yolo_policy_preserves_active_task_under_pressure() {
        let probe = probe_for_policy(
            ContextProfile::YoloGoal,
            ContextPressureLevel::High,
            ContextDegradationPath::SummarizeEvidence,
            1,
        );

        let decision = ContextRuntimeKernel::policy_decision(&probe);

        assert_eq!(decision.action, ContextPolicyAction::TrimToolTrace);
        assert!(decision.reason.contains("task and memory"));
        assert_eq!(decision.stable_head_hash, "stable-head");
    }

    #[test]
    fn review_policy_prioritizes_evidence_refs() {
        let probe = probe_for_policy(
            ContextProfile::Review,
            ContextPressureLevel::Critical,
            ContextDegradationPath::HandoffBoundary,
            1,
        );

        let decision = ContextRuntimeKernel::policy_decision(&probe);

        assert_eq!(decision.action, ContextPolicyAction::SummarizeEvidence);
        assert!(decision.reason.contains("evidence references"));
    }

    #[test]
    fn lean_probe_distinguishes_source_fallback_from_pressure() {
        let mut envelope =
            ContextRuntimeKernel::build_envelope(request_with_dynamic("tiny memory"));
        envelope.diagnostics.degraded_sources = vec![ContextSourceKind::Memory];
        let probe = ContextRuntimeKernel::lean_probe(&envelope);

        assert_eq!(probe.pressure_level, ContextPressureLevel::Nominal);
        assert_eq!(
            probe.degradation_path,
            ContextDegradationPath::SourceFallback
        );
        assert_eq!(probe.degraded_sources, vec![ContextSourceKind::Memory]);
    }

    #[test]
    fn lean_probe_reports_lease_omission_as_tail_trim_path() {
        let mut oversized = item_with_tokens(
            "memory-oversized",
            ContextSourceKind::Memory,
            ContextRole::Orientation,
            400,
        );
        oversized.score = 1.0;

        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main("session-lease"),
            profile: ContextProfile::MainTurn,
            intent: "force lease omission".to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![oversized],
            omitted: Vec::new(),
            total_budget_tokens: 1_000,
        });
        let probe = ContextRuntimeKernel::lean_probe(&envelope);

        assert_eq!(probe.selected_count, 0);
        assert_eq!(probe.omitted_count, 1);
        assert_eq!(probe.pressure_level, ContextPressureLevel::Nominal);
        assert_eq!(
            probe.degradation_path,
            ContextDegradationPath::TrimDynamicTail
        );
    }

    #[test]
    fn sub_agent_identity_tracks_parent() {
        let identity = ContextIdentity::sub_agent("session-1", "reviewer", "primary");

        assert_eq!(identity.mode, ContextMode::SubAgent);
        assert_eq!(identity.agent_id, "reviewer");
        assert_eq!(identity.parent_agent_id.as_deref(), Some("primary"));
    }

    #[test]
    fn envelope_serializes_for_ui_diagnostics() {
        let envelope = ContextRuntimeKernel::build_envelope(request_with_dynamic("serialize me"));
        let json = serde_json::to_string(&envelope).expect("envelope should serialize");

        assert!(json.contains("stable_head_hash"));
        assert!(json.contains("dynamic_tail_hash"));
        assert!(json.contains("recommendations"));
        assert!(json.contains("serialize me"));
    }

    #[test]
    fn dynamic_context_items_use_compact_markdown_not_xml_tags() {
        let envelope =
            ContextRuntimeKernel::build_envelope(request_with_dynamic("compact context body"));
        let rendered = envelope
            .assembled
            .dynamic_tail
            .first()
            .expect("dynamic context should be rendered");

        assert!(rendered.starts_with("### context Memory | Orientation"));
        assert!(rendered.contains("score "));
        assert!(rendered.contains("compact context body"));
        assert!(!rendered.contains("<context_item"));
        assert!(!rendered.contains("</context_item>"));
    }

    #[test]
    fn context_epoch_report_tracks_active_and_suppressed_sources() {
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile: ContextProfile::MainTurn,
            identity: ContextIdentity::main("session-epoch"),
            intent: "inspect memory pollution".to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![ContextItem::new(
                "memory://active",
                ContextSourceKind::Memory,
                ContextRole::Evidence,
                "active memory fact",
            )],
            omitted: vec![ContextOmission {
                source: ContextSourceKind::Knowledge,
                reason: "suppressed_for_current_turn: unrelated domain".to_string(),
                token_estimate: 64,
            }],
            total_budget_tokens: 8_000,
        });

        assert!(envelope.epoch_id.starts_with("ctx-epoch-"));
        assert_eq!(envelope.source_registry.len(), 2);
        assert_eq!(
            envelope.selected[0].source_lifecycle,
            ContextSourceLifecycle::Durable
        );
        let report = envelope.epoch_report.as_ref().unwrap();
        assert_eq!(report.active_sources.len(), 1);
        assert_eq!(report.suppressed_sources.len(), 1);
        assert_eq!(
            report.suppressed_sources[0].lifecycle,
            ContextSourceLifecycle::SuppressedForCurrentTurn
        );
        assert_eq!(report.active_sources[0].source_id, "memory://active");
    }

    #[test]
    fn envelope_reports_pressure_recommendations() {
        let mut request = request_with_dynamic(&"x".repeat(3_600));
        request.total_budget_tokens = 1_000;
        request.omitted.push(ContextOmission {
            source: ContextSourceKind::Memory,
            reason: "lease exhausted".to_string(),
            token_estimate: 128,
        });

        let envelope = ContextRuntimeKernel::build_envelope(request);

        assert!(envelope
            .omitted
            .iter()
            .any(|item| item.reason == "context lease exhausted"));
        assert!(envelope
            .diagnostics
            .recommendations
            .iter()
            .any(|item| item.contains("omitted")));
    }

    #[test]
    fn high_pressure_recommendations_suggest_handoff() {
        let recommendations = context_recommendations(9_500, 3, 0);

        assert!(recommendations
            .iter()
            .any(|recommendation| recommendation.contains("handoff")));
    }

    #[test]
    fn lease_budget_omits_items_deterministically() {
        let mut high = ContextItem::new(
            "b-high",
            ContextSourceKind::Memory,
            ContextRole::Orientation,
            "keep",
        );
        high.score = 0.9;
        high.token_estimate = 4;
        let mut low = ContextItem::new(
            "a-low",
            ContextSourceKind::Memory,
            ContextRole::Orientation,
            "omit",
        );
        low.score = 0.1;
        low.token_estimate = 4;
        let lease = ContextLease {
            source: ContextSourceKind::Memory,
            min_tokens: 0,
            target_tokens: 4,
            max_tokens: 4,
            priority: 8,
            degradation: vec!["omit lower score".to_string()],
        };

        let (selected, omitted) = ContextRuntimeKernel::apply_leases(vec![low, high], &[lease]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "b-high");
        assert_eq!(omitted.len(), 1);
        assert_eq!(omitted[0].reason, "context lease exhausted");
    }

    #[test]
    fn default_profile_leases_prioritize_yolo_task_context() {
        let leases = ContextRuntimeKernel::default_leases(ContextProfile::YoloGoal, 10_000);
        let task = leases
            .iter()
            .find(|lease| lease.source == ContextSourceKind::Task)
            .expect("task lease");
        let peer = leases
            .iter()
            .find(|lease| lease.source == ContextSourceKind::AgentPeer)
            .expect("peer lease");

        assert!(task.priority > peer.priority);
        assert!(task.max_tokens > peer.max_tokens);
    }

    #[test]
    fn build_envelope_applies_profile_leases_and_preserves_selected_order() {
        let identity = ContextIdentity {
            session_id: "session-yolo".to_string(),
            project_id: None,
            task_id: Some("task-1".to_string()),
            agent_id: "primary".to_string(),
            parent_agent_id: None,
            team_id: None,
            mode: ContextMode::YoloGoal,
        };
        let mut task = ContextItem::new(
            "task-context",
            ContextSourceKind::Task,
            ContextRole::TaskState,
            "current phase and acceptance criteria",
        );
        task.token_estimate = 20;
        task.score = 0.5;
        let mut peer = ContextItem::new(
            "peer-context",
            ContextSourceKind::AgentPeer,
            ContextRole::Evidence,
            "large peer packet",
        );
        peer.token_estimate = 2_000;
        peer.score = 1.0;

        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity,
            profile: ContextProfile::YoloGoal,
            intent: "continue".to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![task, peer],
            omitted: Vec::new(),
            total_budget_tokens: 1_000,
        });

        assert_eq!(envelope.selected.len(), 1);
        assert_eq!(envelope.selected[0].id, "task-context");
        assert_eq!(envelope.omitted.len(), 1);
        assert_eq!(envelope.budget.leases[0].source, ContextSourceKind::Task);
    }

    #[test]
    fn agent_context_lease_creates_child_identity_and_return_item() {
        let lease = AgentContextLease {
            parent_session_id: "session-1".to_string(),
            parent_agent_id: "primary".to_string(),
            child_agent_id: "reviewer".to_string(),
            task_contract: "review diff".to_string(),
            allowed_sources: vec![ContextSourceKind::Memory, ContextSourceKind::ToolTrace],
            max_tokens: 2_000,
            required_return: vec![AgentReturnRequirement::ResultSummary],
        };
        let identity = ContextRuntimeKernel::child_identity_from_lease(&lease);
        assert_eq!(identity.mode, ContextMode::SubAgent);
        assert_eq!(identity.parent_agent_id.as_deref(), Some("primary"));

        let packet = AgentReturnContextProjection {
            parent_session_id: "session-1".to_string(),
            child_agent_id: "reviewer".to_string(),
            result_summary: "diff is safe".to_string(),
            evidence: vec!["test:passed".to_string()],
            decisions: vec!["ship".to_string()],
            conflicts: Vec::new(),
            memory_candidates: Vec::new(),
            next_actions: Vec::new(),
            failed: false,
        };
        let item = ContextRuntimeKernel::agent_return_item(&packet);
        assert_eq!(item.source, ContextSourceKind::AgentPeer);
        assert_eq!(item.authority, ContextAuthority::Agent);
        assert!(item.content.contains("diff is safe"));
        assert_eq!(
            item.evidence,
            vec!["agent://reviewer/evidence/test:passed".to_string()]
        );
    }

    #[test]
    fn tool_trace_and_resume_packets_become_context_items() {
        let trace = ToolTracePacket {
            tool_name: "bash".to_string(),
            invocation_id: "tool-1".to_string(),
            status: ToolTraceStatus::Failed,
            summary: "cargo test failed in parser".to_string(),
            changed_files: vec!["src/parser.rs".to_string()],
            evidence_ids: vec!["event-9".to_string()],
            token_estimate: 12,
        };
        let trace_item = ContextRuntimeKernel::tool_trace_item(&trace);
        assert_eq!(trace_item.source, ContextSourceKind::ToolTrace);
        assert_eq!(trace_item.token_estimate, 12);
        assert!(trace_item.content.contains("parser"));
        assert!(trace_item
            .evidence
            .contains(&"tool://tool-1/evidence/event-9".to_string()));
        assert!(trace_item
            .evidence
            .contains(&"workspace://changed-file/src/parser.rs".to_string()));

        let resume = ResumeContextPacket {
            session_id: "session-1".to_string(),
            handoff_summary: Some("continue context runtime".to_string()),
            active_task: Some("phase 6".to_string()),
            recent_decisions: vec!["db-first".to_string()],
            blockers: Vec::new(),
            source: ResumeContextSource::Mixed,
        };
        let resume_item = ContextRuntimeKernel::resume_item(&resume);
        assert_eq!(resume_item.source, ContextSourceKind::Handoff);
        assert!(resume_item.content.contains("phase 6"));
        assert!(resume_item
            .evidence
            .contains(&"session://session-1/resume/Mixed".to_string()));

        let mut task_resume = resume.clone();
        task_resume.source = ResumeContextSource::TaskRegistry;
        let task_item = ContextRuntimeKernel::resume_item(&task_resume);
        assert_eq!(task_item.source, ContextSourceKind::Task);
        assert!(task_item.content.contains("phase 6"));
    }

    #[test]
    fn context_policy_large_tail_remains_bounded() {
        let identity = ContextIdentity::main("large-tail-session");
        let dynamic_items = (0..200)
            .map(|idx| {
                item_with_tokens(
                    &format!("tool-trace-{idx:03}"),
                    ContextSourceKind::ToolTrace,
                    ContextRole::ToolSummary,
                    50,
                )
            })
            .collect::<Vec<_>>();

        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile: ContextProfile::MainTurn,
            identity,
            intent: "inspect large tool tail".to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items,
            omitted: Vec::new(),
            total_budget_tokens: 1_000,
        });
        let probe = ContextRuntimeKernel::lean_probe(&envelope);

        assert!(envelope.selected.len() <= 3);
        assert!(envelope.omitted.len() >= 197);
        assert_eq!(
            probe.degradation_path,
            ContextDegradationPath::TrimDynamicTail
        );
    }

    #[test]
    fn context_policy_proposes_token_saving_action() {
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main("policy-session"),
            profile: ContextProfile::MainTurn,
            intent: "continue under pressure".to_string(),
            stable_head: vec!["stable instructions ".repeat(200)],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: Vec::new(),
            omitted: Vec::new(),
            total_budget_tokens: 100,
        });

        let proposal = ContextRuntimeKernel::policy_proposal(&envelope);
        assert_ne!(proposal.action, ContextPolicyAction::None);
        assert!(proposal.expected_saving_tokens > 0);
        assert!(proposal.safe_to_auto_apply);
        assert_eq!(proposal.session_id, "policy-session");
    }

    #[test]
    fn context_policy_preserves_stable_head() {
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main("stable-session"),
            profile: ContextProfile::MainTurn,
            intent: "protect stable head".to_string(),
            stable_head: vec!["stable instructions".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: Vec::new(),
            omitted: Vec::new(),
            total_budget_tokens: 1_000,
        });
        let before = envelope.assembled.stable_head.clone();
        let proposal = ContextRuntimeKernel::policy_proposal(&envelope);

        assert_eq!(envelope.assembled.stable_head, before);
        assert_eq!(
            proposal.stable_head_hash,
            envelope.diagnostics.stable_head_hash
        );
    }

    #[test]
    fn context_policy_review_evidence_requires_review() {
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main("review-session"),
            profile: ContextProfile::Review,
            intent: "review evidence".to_string(),
            stable_head: vec!["stable review instructions ".repeat(200)],
            runtime_header: Vec::new(),
            dynamic_items: Vec::new(),
            omitted: Vec::new(),
            total_budget_tokens: 100,
        });

        let proposal = ContextRuntimeKernel::policy_proposal(&envelope);
        assert_eq!(proposal.action, ContextPolicyAction::SummarizeEvidence);
        assert!(!proposal.safe_to_auto_apply);
        assert_eq!(
            proposal.affected_sources,
            vec![ContextSourceKind::Workspace, ContextSourceKind::ToolTrace]
        );
    }

    #[test]
    fn workspace_packet_becomes_project_context_item() {
        let packet = WorkspacePacket {
            root: "/workspace/cowd".to_string(),
            touched_files: vec!["crates/runtime/src/context_runtime.rs".to_string()],
            hot_symbols: vec!["ContextRuntimeKernel".to_string()],
            project_notes: vec!["develop branch".to_string()],
            token_estimate: 42,
        };

        let item = ContextRuntimeKernel::workspace_item(&packet);

        assert_eq!(item.source, ContextSourceKind::Workspace);
        assert_eq!(item.authority, ContextAuthority::Project);
        assert_eq!(item.visibility, ContextVisibility::Shared);
        assert_eq!(item.token_estimate, 42);
        assert!(item.content.contains("ContextRuntimeKernel"));
        assert!(item.evidence.contains(
            &"workspace://changed-file/crates/runtime/src/context_runtime.rs".to_string()
        ));
        assert!(item
            .evidence
            .contains(&"workspace://symbol/ContextRuntimeKernel".to_string()));
    }
}
