//! Organizing authority comes from the immutable physical execution grant.
use crate::agentic::program::AgenticProgramProjection;
use harness_contract::agent::AgentTaskPacket;
use harness_contract::agent_action::AgentActionEnvelope;
use harness_contract::execution_graph::ExecutionNodeKind;

pub(super) fn can_create_team(
    graphs: Option<&crate::ExecutionGraphStateStore>,
    program: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
) -> bool {
    can_invoke_bound_action(graphs, program, envelope, "team_create")
}

pub(super) fn can_invoke_bound_action(
    graphs: Option<&crate::ExecutionGraphStateStore>,
    program: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    tool_id: &str,
) -> bool {
    let actor = &envelope.actor;
    let (Some(graphs), Some(execution), Some(agent), Some(team)) = (
        graphs,
        actor.execution_id.as_deref(),
        actor.agent_id.as_deref(),
        actor.team_id.as_deref(),
    ) else {
        return false;
    };
    if actor.actor_id != agent || !program.agent_is_active_in(agent, team) {
        return false;
    }
    let Ok(graph) = graphs.load(execution) else {
        return false;
    };
    graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .any(|node| {
            if graph
                .node_statuses
                .get(&node.id)
                .is_none_or(|status| status.is_terminal())
            {
                return false;
            }
            let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref) else {
                return false;
            };
            let (Some(scope), Some(binding)) =
                (packet.agentic_binding.as_ref(), packet.binding.as_ref())
            else {
                return false;
            };
            packet.graph_id() == execution
                && packet.session_id() == actor.session_id
                && graph.lineage.as_ref().is_some_and(|lineage| {
                    lineage.turn_id == actor.turn_id && lineage.session_id == actor.session_id
                })
                && scope.program_id == actor.program_id
                && scope.agent_id == agent
                && scope.team_id == team
                && program
                    .membership_for(agent, team)
                    .is_some_and(|member| member.membership_id == scope.membership_id)
                && packet.allowed_tools.iter().any(|tool| tool == tool_id)
                && binding
                    .tool_contract_refs
                    .iter()
                    .any(|tool| tool == tool_id)
                && crate::agent::binding::recompute_binding_digest(binding)
                    .is_ok_and(|digest| digest == binding.binding_digest)
        })
}

pub(super) fn can_organize_team(
    program: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    team_id: &str,
) -> bool {
    let actor = &envelope.actor;
    let Some(agent) = actor.agent_id.as_deref() else {
        return false;
    };
    if !program.agent_is_active_in(agent, team_id)
        || !actor
            .team_id
            .as_deref()
            .is_some_and(|bound_team| program.agent_is_active_in(agent, bound_team))
    {
        return false;
    }
    actor.team_id.as_deref() == Some(team_id)
        || program
            .membership_for(agent, team_id)
            .is_some_and(|membership| {
                membership.delegation_ref.is_some()
                    && membership.delegation_ref == actor.execution_id
            })
}
