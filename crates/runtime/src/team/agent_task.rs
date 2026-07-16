//! Declarative team task contracts.
//!
//! These values describe a collaboration plan and its reported outcome. They
//! deliberately do not store, dispatch, or own an Agent lifecycle: graph
//! execution and `AgentRuntime` are the only execution owners.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskStatus {
    Pending,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTask {
    pub task_id: String,
    pub team_id: String,
    pub session_id: String,
    pub role_id: String,
    pub agent_id: Option<String>,
    pub objective: String,
    pub expected_output: String,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub status: AgentTaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<AgentTaskOutcome>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum AgentTaskQualityStatus {
    #[default]
    Accepted,
    NeedsReview,
    Degraded,
    Failed,
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskOutcome {
    pub result_summary: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub conflicts: Vec<String>,
    #[serde(default)]
    pub suggested_next_actions: Vec<String>,
    #[serde(default)]
    pub quality_status: AgentTaskQualityStatus,
    #[serde(default)]
    pub completed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskCompletionReceipt {
    pub task_id: String,
    pub team_id: String,
    pub session_id: String,
    pub role_id: String,
    pub agent_id: Option<String>,
    pub status: AgentTaskStatus,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<AgentTaskOutcome>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}
