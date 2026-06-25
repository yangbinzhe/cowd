use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Planner,
    Executor,
    Researcher,
    MemoryCurator,
    Reviewer,
    Merger,
    Human,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentNodeStatus {
    Pending,
    Ready,
    Running,
    Reviewing,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskNode {
    pub id: String,
    pub role: AgentRole,
    pub title: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub status: AgentNodeStatus,
    #[serde(default)]
    pub assigned_agent: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMessage {
    pub id: String,
    pub from: String,
    pub to: Option<String>,
    pub node_id: Option<String>,
    pub content: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEvidence {
    pub id: String,
    pub node_id: String,
    pub kind: String,
    pub reference: String,
    pub summary: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReview {
    pub id: String,
    pub node_id: String,
    pub reviewer: String,
    pub verdict: ReviewVerdict,
    pub comment: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewVerdict {
    Accept,
    Challenge,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMergeDecision {
    pub id: String,
    pub node_ids: Vec<String>,
    pub decision: String,
    #[serde(default)]
    pub conflicts: Vec<String>,
    pub created_at_ms: u64,
}
