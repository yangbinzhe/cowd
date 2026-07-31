//! Stable, transport-neutral execution projection contracts.
//!
//! Runtime builds these values from canonical stores. Surfaces only consume
//! them through Gateway and never infer lifecycle from prose event streams.

use serde::{Deserialize, Serialize};

use super::command::ExecutionCommandKind;
use crate::context::ContextComponentUsage;
use crate::core::ExecutionPattern;
use crate::execution_graph::ExecutionGraphProjection;
use crate::reality::{EvidenceCompleteness, EvidenceRef};
use crate::strategy::{
    ExecutionCandidateEstimate, ExecutionCandidateKind, StrategyDecisionSource,
    StrategyResourceSnapshot,
};

pub const EXECUTION_PROJECTION_SCHEMA_VERSION: u32 = 2;
pub const EXECUTION_PROJECTION_REDUCER_VERSION: u32 = 1;
pub const STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION: u32 = 1;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionDetailScope {
    #[default]
    Summary,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProjectionQueryContext {
    pub principal: String,
    pub workspace_id: String,
    #[serde(default)]
    pub session_scopes: Vec<String>,
    #[serde(default)]
    pub mission_scopes: Vec<String>,
    #[serde(default)]
    pub visibility_grants: Vec<String>,
    #[serde(default)]
    pub detail_scope: ProjectionDetailScope,
    pub authorization_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProjectionEntity {
    pub id: String,
    pub kind: String,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<ProjectionEntityPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ProjectionEntityPayload {
    Admission(AdmissionProjection),
    Outcome(OutcomeProjection),
    Evidence(EvidenceProjection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionProjectionStatus {
    Accepted,
    Queued,
    WaitingResource,
    WaitingScope,
    WaitingApproval,
    Materialized,
    Running,
    Terminal,
    Blocked,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AdmissionProjection {
    pub request_id: String,
    pub status: AdmissionProjectionStatus,
    pub requested_service_class: String,
    pub resolved_service_class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at_ms: Option<u64>,
    pub queue_age_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker: Option<String>,
    #[serde(default)]
    pub resource_demands: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_scope: Option<String>,
    pub accepted_at_ms: u64,
    pub policy_revision: u64,
    #[serde(default)]
    pub refs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeQualityProjection {
    Unknown,
    Estimated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OutcomeProjection {
    pub execution_id: String,
    pub session_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_graph_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    pub config_revision: String,
    pub strategy_revision: String,
    pub terminal_class: String,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
    pub tool_calls: u64,
    pub duplicate_tool_calls: u64,
    pub retries: u64,
    pub quality: OutcomeQualityProjection,
    pub evidence_completeness: EvidenceCompleteness,
    pub freshness_ms: u64,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct EvidenceProjection {
    pub evidence_ref: EvidenceRef,
    pub support: String,
    pub completeness: EvidenceCompleteness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projector_lag_commits: Option<u64>,
}

/// Public, capability-cropped responsibility assigned to one strategy lane.
///
/// Runtime deliberately omits workspace paths, prompts, hidden content and
/// internal reasoning. `capability_cropped_refs` contains only opaque public
/// evidence references admitted by the projection reducer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StrategyEvidenceScopeProjection {
    pub role_id: String,
    pub focus_id: String,
    pub responsibility_summary: String,
    #[serde(default)]
    pub capability_cropped_refs: Vec<String>,
    pub scope_hash: String,
    pub overlap_budget_bp: u16,
    pub novelty_target_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StrategyTransitionProjection {
    pub revision: u64,
    pub kind: String,
    pub status: String,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StrategyProofStatus {
    NotProven,
    Calibrated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StrategyActualStatus {
    Unknown,
    Observed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StrategyActualProjection {
    pub duration_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub tool_calls: u64,
    pub duplicate_tool_calls: u64,
    pub max_tool_concurrency_observed: u64,
    pub parallel_tool_batches: u64,
    #[serde(default)]
    pub write_attempt_refs: Vec<String>,
    pub evidence_overlap_bp: u16,
    pub evidence_overlap_observed: bool,
    pub working_state_verified: bool,
    pub merge_cost_ms: u64,
    pub parent_merge_count: u8,
    #[serde(default)]
    pub evaluation_token_limit: u64,
    #[serde(default)]
    pub evaluation_tokens_consumed: u64,
    #[serde(default)]
    pub evaluation_budget_observed: bool,
    #[serde(default)]
    pub evaluation_budget_breached: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality_score_bp: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_speedup_ratio_bp: Option<u16>,
    pub terminal_reason: String,
}

/// Backward-compatible typed strategy projection.
///
/// The legacy-named fields intentionally preserve the [`ProjectionEntity`]
/// JSON shape. An old reader ignores the additive schema and typed fields; a
/// new reader accepts an old generic entity and reports it as a legacy/unknown
/// decision because all typed fields use serde defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StrategyDecisionProjection {
    #[serde(default = "strategy_decision_projection_schema_version")]
    pub schema_version: u32,
    pub id: String,
    pub kind: String,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_candidate: Option<ExecutionCandidateKind>,
    #[serde(
        default,
        rename = "selected_pattern",
        alias = "pattern",
        skip_serializing_if = "Option::is_none"
    )]
    pub pattern: Option<ExecutionPattern>,
    #[serde(default)]
    pub candidate_estimates: Vec<ExecutionCandidateEstimate>,
    #[serde(default, rename = "benefit_reason", alias = "benefit_reasons")]
    pub benefit_reasons: Vec<String>,
    #[serde(default, rename = "cost_reason", alias = "cost_reasons")]
    pub cost_reasons: Vec<String>,
    #[serde(default)]
    pub evidence_scopes: Vec<StrategyEvidenceScopeProjection>,
    #[serde(default, rename = "downgrade", alias = "downgrades")]
    pub downgrades: Vec<StrategyTransitionProjection>,
    #[serde(default, rename = "early_stop", alias = "early_stops")]
    pub early_stops: Vec<StrategyTransitionProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated: Option<ExecutionCandidateEstimate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual: Option<StrategyActualProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_snapshot: Option<StrategyResourceSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<StrategyDecisionSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof_status: Option<StrategyProofStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_status: Option<StrategyActualStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_execution_id: Option<String>,
}

const fn strategy_decision_projection_schema_version() -> u32 {
    STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProjectionCommandAvailability {
    pub command: ExecutionCommandKind,
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Summary of one direct or transitive child graph included in a root
/// execution projection. Its nodes remain in that graph's own projection;
/// this entity only exposes explicit, queryable lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ChildExecutionProjection {
    pub execution_id: String,
    pub parent_execution_id: String,
    pub parent_node_id: String,
    pub revision: u64,
    pub cursor: u64,
    pub status: String,
    pub objective: String,
}

/// Typed context-window facts for the currently executing turn.  This is
/// deliberately separate from cumulative session token statistics: it is the
/// bounded prompt ledger used for the next provider request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct ContextUsageProjection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_percent_bp: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_sequence: Option<u64>,
    #[serde(default)]
    pub components: Vec<ContextComponentUsage>,
}

/// Stable, ID-deduplicated counters for a running execution.  A missing live
/// projection is intentionally distinct from zero-valued counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct RunMetricsProjection {
    #[serde(default)]
    pub tool_calls: u64,
    #[serde(default)]
    pub memory_recalls: u64,
    #[serde(default)]
    pub memory_evidence: u64,
    #[serde(default)]
    pub approvals: u64,
    #[serde(default)]
    pub context_items: u64,
    #[serde(default)]
    pub files_touched: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

/// Runtime-owned latency attribution for one execution.
///
/// Provider wall time is measured by the provider stream owner. Harness time
/// is the remaining execution wall time, so parallel child executions keep
/// their own attribution instead of being added into a misleading root total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct ExecutionLatencyProjection {
    #[serde(default)]
    pub total_elapsed_ms: u64,
    #[serde(default)]
    pub harness_elapsed_ms: u64,
    #[serde(default)]
    pub provider_wall_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_token_latency_ms: Option<u64>,
    #[serde(default)]
    pub provider_active_stream_ms: u64,
}

/// Runtime-owned, current-turn facts.  It is an additive field on the
/// existing execution projection so older surface clients remain compatible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLiveStatus {
    Queued,
    PreparingContext,
    CallingModel,
    Thinking,
    CallingTool,
    WaitingApproval,
    Finalizing,
    Complete,
    Cancelled,
    Error,
}

impl ExecutionLiveStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Cancelled | Self::Error)
    }
}

/// One Runtime-owned assistant output segment.
///
/// Byte ranges and revisions are local to `part_id`; consumers concatenate
/// parts by `causal_sequence` and never infer a global assistant stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionLiveOutputPart {
    pub model_step_id: String,
    pub item_id: String,
    pub part_id: String,
    pub causal_sequence: u64,
    #[serde(default)]
    pub completed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    #[serde(default)]
    pub preview_start_bytes: u64,
    #[serde(default)]
    pub bytes: u64,
}

/// Runtime-owned, current-turn facts.  It is an additive field on the
/// existing execution projection so older surface clients remain compatible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionLiveState {
    /// Monotonic within one in-process execution carrier.  The durable graph
    /// revision remains on [`ExecutionProjection::revision`]; consumers use
    /// this field to discard stale live snapshots without conflating cursors.
    pub revision: u64,
    pub status: ExecutionLiveStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub last_progress_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_usage: Option<ContextUsageProjection>,
    #[serde(default)]
    pub metrics: RunMetricsProjection,
    #[serde(default)]
    pub latency: ExecutionLatencyProjection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_preview: Option<String>,
    /// Byte offset of `output_preview` within the current assistant part.
    /// Older producers default to zero, meaning the preview is a complete
    /// prefix/snapshot.
    #[serde(default)]
    pub output_preview_start_bytes: u64,
    /// Total UTF-8 bytes emitted for the current assistant part.
    #[serde(default)]
    pub output_bytes: u64,
    /// Authoritative per-item output streams ordered by causal sequence.
    #[serde(default)]
    pub output_parts: Vec<ExecutionLiveOutputPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Lightweight same-connection update for Runtime-owned live facts.
///
/// Durable graph/entity changes continue to use [`ExecutionProjection`].
/// Streaming text and rapidly changing metrics use this envelope so a token
/// delta never forces storage scans or serialization of the full graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionLiveUpdate {
    pub schema_version: u32,
    pub execution_id: String,
    pub live: ExecutionLiveState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionExecutionEntryProjection {
    /// Stable Session ingress/lifecycle execution identity.
    pub execution_id: String,
    /// Queryable Runtime graph identity, once graph materialization completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub status: ExecutionLiveStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_ref: Option<String>,
}

/// A discovery-only session-to-execution relation. Detailed execution facts
/// remain in [`ExecutionProjection`] and are loaded on demand by graph ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionExecutionIndexProjection {
    pub session_id: String,
    /// Durable turn executions ordered from oldest to newest. This collection
    /// is intentionally lightweight so Surfaces can render a turn index
    /// without materializing every execution graph.
    #[serde(default)]
    pub executions: Vec<SessionExecutionEntryProjection>,
    #[serde(default)]
    pub active_execution_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_execution_id: Option<String>,
    /// Runtime graph compiled for `latest_execution_id`.
    ///
    /// The execution ID is the stable Session ingress/lifecycle identity,
    /// while this ID addresses the queryable execution graph projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_graph_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_status: Option<ExecutionLiveStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_live_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_ref: Option<String>,
}

/// Typed discovery response for all sessions that currently have a recoverable
/// active execution.  It avoids exposing an untyped JSON array at the
/// cross-surface bootstrap boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct SessionExecutionIndicesProjection {
    #[serde(default)]
    pub items: Vec<SessionExecutionIndexProjection>,
}

/// A stable, deterministic relation between one durable Session turn and its
/// Runtime execution.  This is a binding/capability record, not a second copy
/// of execution evidence: callers follow `execution_id` for details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TurnEvidenceProjection {
    pub session_id: String,
    pub turn_id: String,
    pub input_message_id: String,
    pub execution_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assistant_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_report_id: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub freshness: EvidenceFreshness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceFreshness {
    Live,
    Durable,
    Unavailable,
}

/// Session-scoped evidence header data.  Per-message actions are enabled only
/// when a matching [`TurnEvidenceProjection`] is present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionEvidenceProjection {
    pub session_id: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub turns: Vec<TurnEvidenceProjection>,
    pub freshness: EvidenceFreshness,
}

pub const SESSION_HISTORY_INDEX_SCHEMA_VERSION: u32 = 1;

/// Body-free message metadata used to navigate a long Session before loading
/// exact transcript rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionHistoryMessageMetadataProjection {
    pub message_id: String,
    pub sequence: u64,
    pub role: String,
    pub blocks_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub created_at_ms: u64,
    pub content_bytes: u64,
}

/// Rebuildable navigation card. The card is never authoritative transcript
/// content; `source_start_sequence..=source_end_sequence` and `source_digest`
/// locate and verify the immutable source rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionHistoryCardProjection {
    pub card_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_card_id: Option<String>,
    pub source_start_sequence: u64,
    pub source_end_sequence: u64,
    pub source_message_count: u64,
    pub source_digest: String,
    pub summary: String,
    pub scope: String,
    pub authority: String,
    pub generation: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionHistoryRecoveryState {
    Ready,
    ManifestRebuilt,
    IndexPending,
    CheckpointMissing,
    CheckpointMalformed,
}

/// Bounded, transport-neutral read model for Session activation and history
/// navigation. Surfaces render this first and fetch transcript bodies only
/// through the exact message/page APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionHistoryIndexProjection {
    pub schema_version: u32,
    pub session_id: String,
    pub projection_generation: u64,
    pub durable_cursor: u64,
    pub event_cursor: u64,
    pub history_revision: u64,
    pub total_messages: u64,
    pub total_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_checkpoint_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_checkpoint_event_id: Option<String>,
    pub index_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed_through_sequence: Option<u64>,
    pub index_card_count: u64,
    pub index_complete: bool,
    pub recovery_state: SessionHistoryRecoveryState,
    #[serde(default)]
    pub recent_metadata: Vec<SessionHistoryMessageMetadataProjection>,
    #[serde(default)]
    pub cards: Vec<SessionHistoryCardProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionProjection {
    pub schema_version: u32,
    pub execution_id: String,
    pub revision: u64,
    pub cursor: u64,
    pub detail_scope: ProjectionDetailScope,
    pub authorization_revision: u64,
    pub redaction_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<StrategyDecisionProjection>,
    pub graph: ExecutionGraphProjection,
    #[serde(default)]
    pub child_executions: Vec<ChildExecutionProjection>,
    #[serde(default)]
    pub goals: Vec<ProjectionEntity>,
    #[serde(default)]
    pub agents: Vec<ProjectionEntity>,
    #[serde(default)]
    pub teams: Vec<ProjectionEntity>,
    #[serde(default)]
    pub relations: Vec<ProjectionEntity>,
    #[serde(default)]
    pub approvals: Vec<ProjectionEntity>,
    #[serde(default)]
    pub admissions: Vec<ProjectionEntity>,
    #[serde(default)]
    pub outcomes: Vec<ProjectionEntity>,
    #[serde(default)]
    pub interventions: Vec<ProjectionEntity>,
    #[serde(default)]
    pub usage: Vec<ProjectionEntity>,
    #[serde(default)]
    pub context: Vec<ProjectionEntity>,
    #[serde(default)]
    pub evidence: Vec<ProjectionEntity>,
    #[serde(default)]
    pub health: Vec<ProjectionEntity>,
    #[serde(default)]
    pub recovery: Vec<ProjectionEntity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<ExecutionLiveState>,
    #[serde(default)]
    pub available_commands: Vec<ProjectionCommandAvailability>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    fn execution_v2_payload() -> serde_json::Value {
        serde_json::json!({
            "schema_version": EXECUTION_PROJECTION_SCHEMA_VERSION,
            "execution_id": "execution-v2",
            "revision": 4,
            "cursor": 9,
            "detail_scope": "summary",
            "authorization_revision": 1,
            "redaction_revision": "sha256:test",
            "graph": {
                "graph_id": "execution-v2",
                "revision": 4,
                "objective": "v2 payload remains readable",
                "nodes": [],
                "edges": [],
                "commit_cursor": 9,
                "terminal_result_ref": null,
            },
        })
    }

    #[derive(Deserialize)]
    struct LegacyExecutionProjection {
        execution_id: String,
        revision: u64,
        cursor: u64,
    }

    #[test]
    fn live_execution_field_is_additive_within_the_v2_contract() {
        let canonical = execution_v2_payload();
        let projection: ExecutionProjection =
            serde_json::from_value(canonical).expect("v2 payload must deserialize");
        assert!(projection.live.is_none());
        let mut incomplete = execution_v2_payload();
        incomplete
            .as_object_mut()
            .expect("execution object")
            .remove("detail_scope");
        assert!(
            serde_json::from_value::<ExecutionProjection>(incomplete).is_err(),
            "v2 authority fields are required and old snapshots must fail closed"
        );

        let mut live_projection = projection.clone();
        live_projection.live = Some(ExecutionLiveState {
            revision: 3,
            status: ExecutionLiveStatus::CallingModel,
            status_detail: Some("requesting model".to_string()),
            turn_id: Some("turn-live".to_string()),
            started_at_ms: 10,
            updated_at_ms: 11,
            last_progress_at_ms: 11,
            context_usage: None,
            metrics: RunMetricsProjection::default(),
            latency: ExecutionLatencyProjection::default(),
            output_preview: None,
            output_preview_start_bytes: 0,
            output_bytes: 0,
            output_parts: Vec::new(),
            terminal_ref: None,
            error: None,
        });
        let encoded = serde_json::to_value(live_projection).expect("live projection serializes");
        let legacy_reader: LegacyExecutionProjection =
            serde_json::from_value(encoded).expect("legacy reader ignores additive live field");
        assert_eq!(legacy_reader.execution_id, "execution-v2");
        assert_eq!(legacy_reader.revision, 4);
        assert_eq!(legacy_reader.cursor, 9);
    }

    #[test]
    fn typed_strategy_projection_is_bidirectionally_compatible_with_legacy_entity_json() {
        let legacy = serde_json::json!({
            "id": "legacy-strategy-event",
            "kind": "strategy",
            "revision": 7,
            "status": "running",
            "summary": "runtime.strategy.selected",
            "evidence_refs": [],
            "detail": {"legacy": true}
        });
        let decoded: StrategyDecisionProjection =
            serde_json::from_value(legacy).expect("new reader accepts old generic strategy");
        assert_eq!(
            decoded.schema_version,
            STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION
        );
        assert!(decoded.decision_id.is_none());
        assert!(decoded.selected_candidate.is_none());
        assert!(decoded.actual_status.is_none());

        let typed: StrategyDecisionProjection = serde_json::from_value(serde_json::json!({
            "id": "decision-1",
            "kind": "strategy_decision",
            "revision": 2,
            "status": "completed",
            "summary": "runtime.strategy.outcome",
            "evidence_refs": ["evidence:checked"],
            "decision_id": "decision-1",
            "execution_id": "execution-1",
            "session_id": "session-1",
            "turn_id": "turn-1",
            "selected_candidate": "team",
            "pattern": "collaborate",
            "candidate_estimates": [],
            "benefit_reasons": [],
            "cost_reasons": [],
            "evidence_scopes": [],
            "downgrades": [],
            "early_stops": [],
            "proof_status": "not_proven",
            "actual_status": "unknown"
        }))
        .expect("typed projection fixture");
        let encoded = serde_json::to_value(typed).expect("typed strategy serializes");
        let legacy_reader: ProjectionEntity =
            serde_json::from_value(encoded).expect("legacy generic reader ignores typed fields");
        assert_eq!(legacy_reader.id, "decision-1");
        assert_eq!(legacy_reader.kind, "strategy_decision");
        assert_eq!(legacy_reader.revision, 2);
    }
}
