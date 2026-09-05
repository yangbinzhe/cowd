use std::collections::BTreeMap;

use harness_contract::agent_action::{AgentAction, AgentActionEnvelope};
use harness_contract::goal::ObjectiveTerminalKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgenticProgramStatus {
    Open,
    CompletionRequested,
    Verified,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticProgramProjection {
    pub program_id: String,
    pub objective_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub root_execution_id: Option<String>,
    pub required_team_count: u8,
    pub objective_summary: String,
    pub model_lease: String,
    pub permission_ceiling: harness_contract::policy::PermissionMode,
    pub resource_scopes: Vec<String>,
    pub revision: u64,
    pub status: AgenticProgramStatus,
    pub teams: BTreeMap<String, AgenticTeamProjection>,
    pub agents: BTreeMap<String, AgentMemberProjection>,
    pub tasks: BTreeMap<String, AgenticTaskProjection>,
    pub topics: BTreeMap<String, Vec<AgenticTopicEntryProjection>>,
    pub artifacts: BTreeMap<String, AgenticArtifactProjection>,
    pub final_artifact_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_request: Option<AgenticCompletionRequestProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective_verdict: Option<AgenticObjectiveVerdictProjection>,
    pub unresolved: Vec<String>,
}

impl AgenticProgramProjection {
    #[must_use]
    pub fn empty(program_id: impl Into<String>, objective_id: impl Into<String>) -> Self {
        Self {
            program_id: program_id.into(),
            objective_id: objective_id.into(),
            session_id: String::new(),
            turn_id: String::new(),
            root_execution_id: None,
            required_team_count: 0,
            objective_summary: String::new(),
            model_lease: "default".to_string(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            resource_scopes: Vec::new(),
            revision: 0,
            status: AgenticProgramStatus::Open,
            teams: BTreeMap::new(),
            agents: BTreeMap::new(),
            tasks: BTreeMap::new(),
            topics: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            final_artifact_ref: None,
            completion_request: None,
            objective_verdict: None,
            unresolved: Vec::new(),
        }
    }

    pub(crate) fn apply(
        &mut self,
        envelope: &AgentActionEnvelope,
        entity_ref: Option<&str>,
        revision: u64,
        applied_at_ms: u64,
    ) {
        match &envelope.action {
            AgentAction::StateInspect(_) => {}
            AgentAction::TeamCreate(input) => {
                super::roster::apply_team_create(self, envelope, input, entity_ref)
            }
            AgentAction::AgentInvite(input) => {
                super::roster::apply_agent_invite(self, envelope, input, entity_ref)
            }
            AgentAction::TaskPublish(input) => {
                super::work_market::apply_task_publish(self, envelope, input, entity_ref)
            }
            AgentAction::TaskClaim(input) => {
                super::work_market::apply_task_claim(self, envelope, input, applied_at_ms)
            }
            AgentAction::TaskRelease(input) => super::work_market::apply_task_release(self, input),
            AgentAction::TaskSupersede(input) => {
                super::work_market::apply_task_supersede(self, envelope, input)
            }
            AgentAction::TaskAttemptFail(input) => {
                super::work_market::apply_task_attempt_fail(self, input)
            }
            AgentAction::TaskSubmit(input) => super::work_market::apply_task_submit(self, input),
            AgentAction::TaskReview(input) => {
                super::work_market::apply_task_review(self, envelope, input)
            }
            AgentAction::MessagePublish(input) => {
                super::topic::apply_message_publish(self, envelope, input, entity_ref, revision)
            }
            AgentAction::ArtifactCommit(input) => {
                super::topic::apply_artifact_commit(self, envelope, input, entity_ref)
            }
            AgentAction::ObjectiveCompleteRequest(input) => {
                super::supervision::apply_completion_request(self, envelope, input, revision)
            }
        }
        self.revision = revision;
    }

    pub(crate) fn apply_objective_verdict(
        &mut self,
        verdict: AgenticObjectiveVerdictProjection,
        revision: u64,
    ) {
        self.status = match verdict.kind {
            ObjectiveTerminalKind::Satisfied => AgenticProgramStatus::Verified,
            ObjectiveTerminalKind::PartiallySatisfied
            | ObjectiveTerminalKind::Blocked
            | ObjectiveTerminalKind::Failed
            | ObjectiveTerminalKind::Cancelled => AgenticProgramStatus::Blocked,
        };
        self.objective_verdict = Some(verdict);
        self.revision = revision;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticCompletionRequestProjection {
    pub action_id: String,
    pub requested_by: String,
    pub program_revision: u64,
    pub final_artifact_ref: String,
    pub evidence_refs: Vec<String>,
    pub unresolved: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticObjectiveVerdictProjection {
    pub goal_id: String,
    pub goal_revision: u64,
    pub terminal_fence: String,
    pub authority_revision: u64,
    pub kind: ObjectiveTerminalKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticTeamProjection {
    pub team_id: String,
    pub name: String,
    pub mission: String,
    pub objective: Option<String>,
    pub topic_ref: String,
    pub created_by: String,
    pub member_ids: Vec<String>,
    pub task_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMemberProjection {
    pub agent_id: String,
    pub team_id: String,
    pub role: String,
    pub mission: String,
    pub required_capabilities: Vec<String>,
    pub invited_by: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgenticTaskStatus {
    Published,
    Claimed,
    Submitted,
    Accepted,
    Rework,
    Blocked,
    /// The Task did not complete. It was durably retired in favor of the
    /// concrete successor Tasks recorded on the projection.
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticTaskProjection {
    pub task_id: String,
    pub team_id: String,
    pub title: String,
    pub objective: String,
    pub acceptance: String,
    pub required_capabilities: Vec<String>,
    pub depends_on: Vec<String>,
    pub status: AgenticTaskStatus,
    pub claimant: Option<String>,
    pub claim_generation: u64,
    /// Physical graph that owns the current claim. This fencing token stops
    /// a late response from an expired execution submitting into a new claim.
    pub claim_execution_id: Option<String>,
    pub claimed_at_ms: Option<u64>,
    pub lease_expires_at_ms: Option<u64>,
    pub artifact_refs: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub unresolved: Vec<String>,
    pub review_reason: Option<String>,
    pub reviewed_by: Option<String>,
    pub failed_attempts: u8,
    pub review_generation: u64,
    pub failed_review_attempts: u8,
    pub last_failure: Option<String>,
    #[serde(default)]
    pub replacement_task_refs: Vec<String>,
    #[serde(default)]
    pub supersede_evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticTopicEntryProjection {
    pub entry_id: String,
    pub revision: u64,
    pub actor_id: String,
    pub summary: Option<String>,
    pub content_ref: Option<String>,
    pub refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticArtifactProjection {
    pub artifact_ref: String,
    pub content_ref: String,
    pub kind: String,
    pub title: String,
    pub relates_to: Vec<String>,
    pub committed_by: String,
}
