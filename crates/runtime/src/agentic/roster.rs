use harness_contract::agent_action::{
    AgentActionEnvelope, AgentInviteInput, MembershipOperation, MembershipUpdateInput,
    TeamCreateInput, TeamUpdateInput,
};

use super::program::{
    AgentMemberProjection, AgenticMembershipLifecycle, AgenticMembershipProjection,
    AgenticProgramProjection, AgenticTaskStatus, AgenticTeamLifecycle, AgenticTeamProjection,
};

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
            lifecycle: AgenticTeamLifecycle::Active,
        },
    );
}

pub(crate) fn apply_membership_update(
    projection: &mut AgenticProgramProjection,
    _envelope: &AgentActionEnvelope,
    input: &MembershipUpdateInput,
) {
    if !projection.agents.contains_key(&input.agent_ref)
        || !projection.teams.contains_key(&input.team_ref)
    {
        return;
    }
    let membership_id = AgenticProgramProjection::membership_id(&input.agent_ref, &input.team_ref);
    match input.operation {
        MembershipOperation::Join => {
            let membership = projection
                .memberships
                .entry(membership_id.clone())
                .or_insert_with(|| AgenticMembershipProjection {
                    membership_id: membership_id.clone(),
                    agent_id: input.agent_ref.clone(),
                    team_id: input.team_ref.clone(),
                    lifecycle: AgenticMembershipLifecycle::Active,
                    delegation_ref: None,
                    reason_ref: input.reason_ref.clone(),
                    created_by: _envelope.actor.actor_id.clone(),
                });
            membership.lifecycle = AgenticMembershipLifecycle::Active;
            membership.reason_ref.clone_from(&input.reason_ref);
            if let Some(team) = projection.teams.get_mut(&input.team_ref) {
                if !team.member_ids.contains(&input.agent_ref) {
                    team.member_ids.push(input.agent_ref.clone());
                }
            }
            if let Some(member) = projection.agents.get_mut(&input.agent_ref) {
                if !member.membership_ids.contains(&membership_id) {
                    member.membership_ids.push(membership_id);
                }
            }
        }
        MembershipOperation::Leave => {
            if let Some(membership) = projection.memberships.get_mut(&membership_id) {
                membership.lifecycle = AgenticMembershipLifecycle::Draining;
                membership.reason_ref.clone_from(&input.reason_ref);
            }
        }
    }
}

pub(crate) fn apply_team_update(
    projection: &mut AgenticProgramProjection,
    input: &TeamUpdateInput,
) {
    if let Some(team) = projection.teams.get_mut(&input.team_ref) {
        if let Some(mission) = input.mission_ref.as_ref() {
            team.mission.clone_from(mission);
        }
        if input.request_retire {
            team.lifecycle = AgenticTeamLifecycle::Draining;
            for membership in projection.memberships.values_mut() {
                if membership.team_id == input.team_ref
                    && membership.lifecycle == AgenticMembershipLifecycle::Active
                {
                    membership.lifecycle = AgenticMembershipLifecycle::Draining;
                    membership.reason_ref.clone_from(&input.reason_ref);
                }
            }
        }
    }
}

pub(crate) fn apply_agent_invite(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &AgentInviteInput,
    entity_ref: Option<&str>,
) {
    let Some(new_agent_id) = entity_ref else {
        return;
    };
    let agent_id = input.existing_agent_ref.as_deref().unwrap_or(new_agent_id);
    projection
        .agents
        .entry(agent_id.to_string())
        .or_insert_with(|| AgentMemberProjection {
            agent_id: agent_id.to_string(),
            display_name: input.role.trim().to_string(),
            role: input.role.trim().to_string(),
            mission: input.mission.trim().to_string(),
            required_capabilities: input.required_capabilities.clone(),
            invited_by: envelope.actor.actor_id.clone(),
            membership_ids: Vec::new(),
            definition_ref: input.definition_ref.clone(),
            model_profile_ref: input.model_profile_ref.clone(),
            expertise_hints: input.expertise_hints.clone(),
            execution_requirements: input.execution_requirements.clone(),
        });
    let membership_id = AgenticProgramProjection::membership_id(agent_id, &input.team_ref);
    projection.memberships.insert(
        membership_id.clone(),
        AgenticMembershipProjection {
            membership_id: membership_id.clone(),
            agent_id: agent_id.to_string(),
            team_id: input.team_ref.clone(),
            lifecycle: AgenticMembershipLifecycle::Active,
            delegation_ref: None,
            reason_ref: None,
            created_by: envelope.actor.actor_id.clone(),
        },
    );
    if let Some(member) = projection.agents.get_mut(agent_id) {
        if !member.membership_ids.contains(&membership_id) {
            member.membership_ids.push(membership_id);
        }
    }
    if let Some(team) = projection.teams.get_mut(&input.team_ref) {
        if !team.member_ids.contains(&agent_id.to_string()) {
            team.member_ids.push(agent_id.to_string());
        }
    }
}

/// Advance draining scopes only after their live task claims are gone.  This
/// keeps historical membership visible while preventing retired Teams from
/// accepting new work or silently erasing the ownership of in-flight work.
pub(crate) fn reconcile_lifecycle(projection: &mut AgenticProgramProjection) {
    let draining_memberships = projection
        .memberships
        .values()
        .filter(|membership| membership.lifecycle == AgenticMembershipLifecycle::Draining)
        .map(|membership| {
            (
                membership.membership_id.clone(),
                membership.agent_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    for (membership_id, agent_id) in draining_memberships {
        let owns_live_claim = projection.tasks.values().any(|task| {
            task.active_attempts
                .values()
                .any(|attempt| attempt.membership_id == membership_id)
                || (task.claimant.as_deref() == Some(agent_id.as_str())
                    && matches!(
                        task.status,
                        AgenticTaskStatus::Claimed | AgenticTaskStatus::CancelRequested
                    ))
        });
        if !owns_live_claim {
            if let Some(membership) = projection.memberships.get_mut(&membership_id) {
                membership.lifecycle = AgenticMembershipLifecycle::Retired;
            }
        }
    }
    let draining_teams = projection
        .teams
        .values()
        .filter(|team| team.lifecycle == AgenticTeamLifecycle::Draining)
        .map(|team| team.team_id.clone())
        .collect::<Vec<_>>();
    for team_id in draining_teams {
        let has_unfinished_work = projection.tasks.values().any(|task| {
            task.team_id == team_id
                && !matches!(
                    task.status,
                    AgenticTaskStatus::Accepted
                        | AgenticTaskStatus::Withdrawn
                        | AgenticTaskStatus::Superseded
                )
        });
        if !has_unfinished_work {
            if let Some(team) = projection.teams.get_mut(&team_id) {
                team.lifecycle = AgenticTeamLifecycle::Retired;
            }
        }
    }
}
