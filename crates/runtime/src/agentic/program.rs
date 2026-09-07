use std::collections::BTreeMap;

use harness_contract::agent_action::{AgentAction, AgentActionEnvelope};
use harness_contract::goal::ObjectiveTerminalKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgenticProgramStatus {
    Open,
    Waiting,
    CompletionRequested,
    Draining,
    Verified,
    Partial,
    Blocked,
    Failed,
    Cancelled,
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
    /// Program-owned membership truth.  An Agent identity can participate in
    /// several Teams, while each physical run remains bound to one immutable
    /// membership scope.
    #[serde(default)]
    pub memberships: BTreeMap<String, AgenticMembershipProjection>,
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
            memberships: BTreeMap::new(),
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
            AgentAction::TaskRelease(input) => {
                super::work_market::apply_task_release(self, envelope, input)
            }
            AgentAction::TaskSupersede(input) => {
                super::work_market::apply_task_supersede(self, envelope, input)
            }
            AgentAction::TaskWithdraw(input) => {
                super::work_market::apply_task_withdraw(self, envelope, input)
            }
            AgentAction::TaskAttemptDispatch(input) => {
                super::work_market::apply_task_attempt_dispatch(self, input)
            }
            AgentAction::TaskAttemptFail(input) => {
                super::work_market::apply_task_attempt_fail(self, input)
            }
            AgentAction::TaskSubmit(input) => {
                super::work_market::apply_task_submit(self, envelope, input)
            }
            AgentAction::TaskReview(input) => {
                super::work_market::apply_task_review(self, envelope, input)
            }
            AgentAction::MessagePublish(input) => {
                super::topic::apply_message_publish(self, envelope, input, entity_ref, revision)
            }
            AgentAction::ArtifactCommit(input) => {
                super::topic::apply_artifact_commit(self, envelope, input, entity_ref)
            }
            // Goal semantic changes are applied by Runtime's Goal owner in
            // the same ingress path. Program journals the action for causal
            // visibility but does not become a competing Objective writer.
            AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_) => {}
            AgentAction::MembershipUpdate(input) => {
                super::roster::apply_membership_update(self, envelope, input)
            }
            AgentAction::TeamUpdate(input) => super::roster::apply_team_update(self, input),
            AgentAction::ObjectiveCompleteRequest(input) => {
                super::supervision::apply_completion_request(self, envelope, input, revision)
            }
        }
        super::roster::reconcile_lifecycle(self);
        self.revision = revision;
    }

    pub(crate) fn apply_objective_verdict(
        &mut self,
        verdict: AgenticObjectiveVerdictProjection,
        revision: u64,
    ) {
        self.status = match verdict.kind {
            ObjectiveTerminalKind::Satisfied => AgenticProgramStatus::Verified,
            ObjectiveTerminalKind::PartiallySatisfied => AgenticProgramStatus::Partial,
            ObjectiveTerminalKind::Blocked => AgenticProgramStatus::Blocked,
            ObjectiveTerminalKind::Failed => AgenticProgramStatus::Failed,
            ObjectiveTerminalKind::Cancelled => AgenticProgramStatus::Cancelled,
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
    pub result_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_artifact_ref: Option<String>,
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
    #[serde(default)]
    pub lifecycle: AgenticTeamLifecycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgenticTeamLifecycle {
    #[default]
    Active,
    Draining,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMemberProjection {
    pub agent_id: String,
    #[serde(default)]
    pub display_name: String,
    pub role: String,
    pub mission: String,
    pub required_capabilities: Vec<String>,
    pub invited_by: String,
    #[serde(default)]
    pub membership_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile_ref: Option<String>,
    #[serde(default)]
    pub expertise_hints: Vec<String>,
    #[serde(default)]
    pub execution_requirements: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgenticMembershipLifecycle {
    #[default]
    Active,
    Draining,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticMembershipProjection {
    pub membership_id: String,
    pub agent_id: String,
    pub team_id: String,
    #[serde(default)]
    pub lifecycle: AgenticMembershipLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_ref: Option<String>,
    pub created_by: String,
}

impl AgenticProgramProjection {
    #[must_use]
    pub fn membership_id(agent_id: &str, team_id: &str) -> String {
        format!("membership:{agent_id}:{team_id}")
    }

    #[must_use]
    pub fn membership_for(
        &self,
        agent_id: &str,
        team_id: &str,
    ) -> Option<&AgenticMembershipProjection> {
        self.memberships
            .get(&Self::membership_id(agent_id, team_id))
    }

    #[must_use]
    pub fn agent_is_active_in(&self, agent_id: &str, team_id: &str) -> bool {
        self.membership_for(agent_id, team_id)
            .is_some_and(|membership| membership.lifecycle == AgenticMembershipLifecycle::Active)
            && self
                .teams
                .get(team_id)
                .is_some_and(|team| team.lifecycle == AgenticTeamLifecycle::Active)
    }

    #[must_use]
    pub fn active_team_ids_for<'a>(&'a self, agent_id: &str) -> Vec<&'a str> {
        self.memberships
            .values()
            .filter(|membership| {
                membership.agent_id == agent_id
                    && membership.lifecycle == AgenticMembershipLifecycle::Active
                    && self
                        .teams
                        .get(&membership.team_id)
                        .is_some_and(|team| team.lifecycle == AgenticTeamLifecycle::Active)
            })
            .map(|membership| membership.team_id.as_str())
            .collect()
    }

    /// Choose the immutable Team scope for one new physical run. Execute
    /// runs are bound to the Task owner Team. Review runs prefer another
    /// active Team but can use the owner Team when that is the only available
    /// authorized scope; reviewer identity remains independently checked.
    #[must_use]
    pub fn dispatch_team_id_for<'a>(
        &'a self,
        agent_id: &str,
        task: &AgenticTaskProjection,
        review: bool,
    ) -> Option<&'a str> {
        let mut teams = self.active_team_ids_for(agent_id);
        teams.sort_unstable();
        if !review {
            return teams.into_iter().find(|team_id| *team_id == task.team_id);
        }
        teams
            .iter()
            .copied()
            .find(|team_id| *team_id != task.team_id)
            .or_else(|| teams.into_iter().next())
    }
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
    /// Runtime has fenced the current physical claim and durably requested
    /// cancellation.  No new Task write can be accepted until the attempt
    /// exits and its effects are accounted for.
    CancelRequested,
    /// A Task was intentionally withdrawn; its dependents remain waiting
    /// unless a separately accepted replacement satisfies the relation.
    Withdrawn,
    /// The Task did not complete. It was durably retired in favor of the
    /// concrete successor Tasks recorded on the projection.
    Superseded,
}

impl AgenticTaskStatus {
    #[must_use]
    pub const fn is_retired(self) -> bool {
        matches!(self, Self::Withdrawn | Self::Superseded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticTaskProjection {
    pub task_id: String,
    pub team_id: String,
    pub title: String,
    pub objective: String,
    pub acceptance: String,
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub obligation_refs: Vec<String>,
    #[serde(default)]
    pub purpose: harness_contract::agent_action::TaskPurpose,
    #[serde(default)]
    pub execution_requirements: Vec<String>,
    #[serde(default)]
    pub expertise_hints: Vec<String>,
    pub depends_on: Vec<String>,
    pub status: AgenticTaskStatus,
    pub claimant: Option<String>,
    pub claim_generation: u64,
    /// Physical graph that owns the current claim. This fencing token stops
    /// a late response from an expired execution submitting into a new claim.
    pub claim_execution_id: Option<String>,
    pub claimed_at_ms: Option<u64>,
    pub lease_expires_at_ms: Option<u64>,
    /// Physical effects admitted by Runtime. Unlike `claim_execution_id`,
    /// this also covers pre-claim execution and independent review work.
    #[serde(default)]
    pub active_attempts: BTreeMap<String, AgenticTaskAttemptProjection>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_requested_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Durable content reference explaining an explicit withdrawal.  A
    /// supersede keeps its short semantic reason in `superseded_reason`.
    pub cancel_reason_ref: Option<String>,
    #[serde(default)]
    pub cancel_evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_retirement: Option<AgenticTaskRetirement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticTaskAttemptProjection {
    pub execution_id: String,
    pub agent_id: String,
    #[serde(default)]
    pub membership_id: String,
    pub mode: harness_contract::agent_action::AgentAttemptMode,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgenticTaskRetirement {
    Withdrawn,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticTopicEntryProjection {
    pub entry_id: String,
    pub revision: u64,
    pub actor_id: String,
    pub summary: Option<String>,
    pub content_ref: Option<String>,
    pub refs: Vec<String>,
    #[serde(default)]
    pub recipients: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<harness_contract::agent_action::TaskIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgenticArtifactProjection {
    pub artifact_ref: String,
    pub content_ref: String,
    pub kind: String,
    pub title: String,
    pub relates_to: Vec<String>,
    pub committed_by: String,
    /// The physical Task claim that authorized this artifact. It distinguishes
    /// a fresh rework attempt from an earlier submission by the same logical
    /// Agent, so stale evidence cannot satisfy a new delivery boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_execution_id: Option<String>,
    /// Monotonic generation of the Task claim that authorized this artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_generation: Option<u64>,
}
