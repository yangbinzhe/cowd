use harness_contract::agent_action::{AgentActionEnvelope, AgentInviteInput, TeamCreateInput};

use super::program::{AgentMemberProjection, AgenticProgramProjection, AgenticTeamProjection};

pub(crate) fn apply_team_create(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TeamCreateInput,
    entity_ref: Option<&str>,
) {
    let Some(team_id) = entity_ref else {
        return;
    };
    projection.teams.insert(
        team_id.to_string(),
        AgenticTeamProjection {
            team_id: team_id.to_string(),
            name: input.name.trim().to_string(),
            mission: input.mission.trim().to_string(),
            objective: input.objective.clone(),
            topic_ref: format!("topic:{team_id}"),
            created_by: envelope.actor.actor_id.clone(),
            member_ids: Vec::new(),
            task_ids: Vec::new(),
        },
    );
}

pub(crate) fn apply_agent_invite(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &AgentInviteInput,
    entity_ref: Option<&str>,
) {
    let Some(agent_id) = entity_ref else {
        return;
    };
    projection.agents.insert(
        agent_id.to_string(),
        AgentMemberProjection {
            agent_id: agent_id.to_string(),
            team_id: input.team_ref.clone(),
            role: input.role.trim().to_string(),
            mission: input.mission.trim().to_string(),
            required_capabilities: input.required_capabilities.clone(),
            invited_by: envelope.actor.actor_id.clone(),
        },
    );
    if let Some(team) = projection.teams.get_mut(&input.team_ref) {
        team.member_ids.push(agent_id.to_string());
    }
}
