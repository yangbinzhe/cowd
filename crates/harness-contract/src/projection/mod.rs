//! Stable, transport-neutral execution projection contracts.
//!
//! Runtime builds these values from canonical stores. Surfaces only consume
//! them through Gateway and never infer lifecycle from prose event streams.

use serde::{Deserialize, Serialize};

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
