use serde::{Deserialize, Serialize};

pub const EXECUTION_ACTIVITY_SCHEMA_VERSION: u32 = 3;

const fn required_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionActivityKind {
    Execution,
    Goal,
    Team,
    Agent,
    Skill,
    Model,
    ToolBatch,
    Tool,
    Approval,
    Verify,
    Artifact,
    Outcome,
    Replan,
    Recovery,
    Runtime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityVisibility {
    Narrative,
    Operational,
    Audit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityRelationKind {
    Contains,
    DelegatedTo,
    Invoked,
    DependsOn,
    ApprovedBy,
    Produced,
    Consumed,
    ContributesTo,
    ReplannedTo,
    RecoveredFrom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionScopeProjection {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub execution_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_node_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionActivityProjection {
    pub schema_version: u32,
    pub activity_id: String,
    pub scope: ExecutionScopeProjection,
    pub kind: ExecutionActivityKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Stable, bounded label for human-facing surfaces. This is separate from
    /// process and result summaries so clients do not infer names from event
    /// protocol identifiers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_label: Option<String>,
    /// Business phase or lifecycle phase, when Runtime can state it without
    /// exposing a private prompt or provider transcript.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default)]
    pub visibility: Vec<ActivityVisibility>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_activity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initiator_activity_id: Option<String>,
    #[serde(default)]
    pub causal_parent_ids: Vec<String>,
    #[serde(default)]
    pub dependency_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_activation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_contract_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    pub status: String,
    /// Safe explanation for a waiting, warning, blocked, failed or cancelled
    /// status. Surfaces must not derive this from raw evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    /// Whether failure of this activity contributes to root execution
    /// failure. Unknown legacy/event activities are required by default.
    #[serde(default = "required_by_default")]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    pub sequence: u64,
    pub commit_cursor: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_summary: Option<String>,
    /// Bounded semantic output. This remains distinct from the label and from
    /// a public in-progress reasoning summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_summary: Option<String>,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub definition_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_capability: Option<String>,
}

/// Immutable execution ownership attached to every Runtime event that may
/// participate in the public activity projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RuntimeActivityBinding {
    pub root_execution_id: String,
    pub activity_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_activity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initiator_activity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_activation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_contract_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_group_id: Option<String>,
    pub revision: u64,
    pub fence: u64,
}

impl RuntimeActivityBinding {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.root_execution_id.trim().is_empty() {
            return Err("root_execution_id must not be empty");
        }
        if self.activity_id.trim().is_empty() {
            return Err("activity_id must not be empty");
        }
        if self.revision == 0 {
            return Err("revision must be positive");
        }
        if self.fence == 0 {
            return Err("fence must be positive");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionActivityRelation {
    pub relation_id: String,
    pub kind: ActivityRelationKind,
    pub from_activity_id: String,
    pub to_activity_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_ref: Option<String>,
}
