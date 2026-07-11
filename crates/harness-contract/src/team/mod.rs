//! Stable contracts for a graph-owned multi-agent team.
//!
//! These types describe collaboration intent, bindings, and projections. They
//! do not start agents, schedule work, or mutate an execution graph.

use serde::{Deserialize, Serialize};

use crate::context::EvidenceAccessRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamTemplateId {
    SingleExecutor,
    ExecuteReview,
    FanoutResearchSynthesis,
    DebateConsensus,
    ImplementationReviewFix,
    IncidentResponse,
    LongRunningProject,
}

impl TeamTemplateId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SingleExecutor => "single_executor",
            Self::ExecuteReview => "execute_review",
            Self::FanoutResearchSynthesis => "fanout_research_synthesis",
            Self::DebateConsensus => "debate_consensus",
            Self::ImplementationReviewFix => "implementation_review_fix",
            Self::IncidentResponse => "incident_response",
            Self::LongRunningProject => "long_running_project",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamTemplateAvailability {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleSpec {
    pub role_id: String,
    pub responsibility: String,
    pub required_capabilities: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub acceptance: Vec<String>,
    pub evidence_duties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTaskSpec {
    pub task_id: String,
    pub role_id: String,
    pub objective: String,
    pub acceptance: Vec<String>,
    pub depends_on_task_ids: Vec<String>,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub allowed_tools: Vec<String>,
    pub permission_lease: String,
    pub model_lease: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTaskBinding {
    pub team_id: String,
    pub task_id: String,
    pub agent_id: String,
    pub run_id: String,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTaskTrace {
    pub task_id: String,
    pub role_id: String,
    pub agent_id: String,
    pub run_id: String,
    pub node_id: String,
    pub status: String,
    pub result_ref: Option<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamBoardEntry {
    pub task_id: String,
    pub agent_id: String,
    pub summary: String,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub conflicts: Vec<String>,
    pub unresolved: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamScorecard {
    pub completion_rate: f32,
    pub conflict_count: usize,
    pub unresolved_count: usize,
    pub evidence_count: usize,
    pub coordination_overhead_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamLiftVerdict {
    pub accepted: bool,
    pub max_parallel_agents: usize,
    pub reasons: Vec<String>,
    pub resized_from: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamReviewPacket {
    pub team_id: String,
    pub graph_id: String,
    pub traces: Vec<TeamTaskTrace>,
    pub board: Vec<TeamBoardEntry>,
    pub scorecard: TeamScorecard,
    pub unresolved: Vec<String>,
}

/// The terminal Team result is a reference projection, never a second graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRunResult {
    pub team_id: String,
    pub graph_id: String,
    pub graph_revision: u64,
    pub result_ref: String,
    pub evidence_refs: Vec<EvidenceAccessRef>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_ids_have_stable_wire_names() {
        assert_eq!(
            TeamTemplateId::FanoutResearchSynthesis.as_str(),
            "fanout_research_synthesis"
        );
        assert_eq!(
            serde_json::to_string(&TeamTemplateId::ExecuteReview).unwrap(),
            "\"execute_review\""
        );
    }
}
