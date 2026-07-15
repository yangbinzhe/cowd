//! Stable, transport-neutral execution projection contracts.
//!
//! Runtime builds these values from canonical stores. Surfaces only consume
//! them through Gateway and never infer lifecycle from prose event streams.

use serde::{Deserialize, Serialize};

use crate::context::ContextComponentUsage;
use crate::execution_graph::ExecutionGraphProjection;

pub const EXECUTION_PROJECTION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionDetailScope {
    #[default]
    Summary,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCommandAvailability {
    pub command: ExecutionCommandKind,
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Summary of one direct or transitive child graph included in a root
/// execution projection. Its nodes remain in that graph's own projection;
/// this entity only exposes explicit, queryable lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
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

/// Runtime-owned, current-turn facts.  It is an additive field on the
/// existing execution projection so older surface clients remain compatible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Runtime-owned, current-turn facts.  It is an additive field on the
/// existing execution projection so older surface clients remain compatible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A discovery-only session-to-execution relation.  Detailed facts always
/// remain in [`ExecutionProjection`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExecutionIndexProjection {
    pub session_id: String,
    #[serde(default)]
    pub active_execution_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_execution_id: Option<String>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SessionExecutionIndicesProjection {
    #[serde(default)]
    pub items: Vec<SessionExecutionIndexProjection>,
}

/// A stable, deterministic relation between one durable Session turn and its
/// Runtime execution.  This is a binding/capability record, not a second copy
/// of execution evidence: callers follow `execution_id` for details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceFreshness {
    Live,
    Durable,
    Unavailable,
}

/// Session-scoped evidence header data.  Per-message actions are enabled only
/// when a matching [`TurnEvidenceProjection`] is present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEvidenceProjection {
    pub session_id: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub turns: Vec<TurnEvidenceProjection>,
    pub freshness: EvidenceFreshness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionProjection {
    pub schema_version: u32,
    pub execution_id: String,
    pub revision: u64,
    pub cursor: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<ProjectionEntity>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEventKind {
    UpsertNode,
    UpsertChildExecution,
    RemoveNode,
    UpsertAgent,
    UpsertTeam,
    UpsertSessionRelation,
    GoalChanged,
    ContextChanged,
    InterventionRecorded,
    ApprovalChanged,
    UsageChanged,
    HealthChanged,
    CommandsChanged,
    TerminalCommitted,
    CursorAdvanced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionEvent {
    pub commit_cursor: u64,
    pub transaction_index: u32,
    pub event_id: String,
    pub kind: ProjectionEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity: Option<ProjectionEntity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionDelta {
    pub schema_version: u32,
    pub execution_id: String,
    pub base_cursor: u64,
    pub target_cursor: u64,
    #[serde(default)]
    pub events: Vec<ProjectionEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCommandKind {
    Pause,
    Resume,
    Cancel,
    Replan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCommandRequest {
    pub command_id: String,
    pub expected_revision: u64,
    pub command: ExecutionCommandKind,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCommandReceipt {
    pub command_id: String,
    pub accepted_revision: u64,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    fn legacy_execution_payload() -> serde_json::Value {
        serde_json::json!({
            "schema_version": EXECUTION_PROJECTION_SCHEMA_VERSION,
            "execution_id": "execution-legacy",
            "revision": 4,
            "cursor": 9,
            "graph": {
                "graph_id": "execution-legacy",
                "revision": 4,
                "objective": "legacy payload remains readable",
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
    fn live_execution_field_is_additive_in_both_contract_directions() {
        let legacy = legacy_execution_payload();
        let projection: ExecutionProjection =
            serde_json::from_value(legacy).expect("old payload must deserialize");
        assert!(projection.live.is_none());

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
            output_preview: None,
            terminal_ref: None,
            error: None,
        });
        let encoded = serde_json::to_value(live_projection).expect("live projection serializes");
        let legacy_reader: LegacyExecutionProjection =
            serde_json::from_value(encoded).expect("legacy reader ignores additive live field");
        assert_eq!(legacy_reader.execution_id, "execution-legacy");
        assert_eq!(legacy_reader.revision, 4);
        assert_eq!(legacy_reader.cursor, 9);
    }
}
