use std::collections::BTreeMap;

use harness_contract::agent::AgentTaskPacket;
use harness_contract::context::ContextBudgetLeaseRef;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
};
use harness_contract::team::{
    TeamLiftVerdict, TeamRoleSpec, TeamTaskBinding, TeamTaskSpec, TeamTemplateId,
};

use crate::execution_core::graph::executors::{
    AgentTaskExecutor, SynthesizeNodeExecutor, VerifyNodeExecutor,
};
use crate::{TeamRoleDependency, TeamTemplateRegistry};

#[derive(Debug, Clone)]
pub struct TeamBuildRequest {
    pub team_id: String,
    pub session_id: String,
    pub objective: String,
    pub template_id: TeamTemplateId,
    pub roles: Vec<TeamRoleSpec>,
    pub role_dependencies: Vec<TeamRoleDependency>,
    /// Reusable capability profiles selected from the scoped catalog. Runtime
    /// lifecycle identities are still graph-local: a catalog profile is never
    /// reused as a mutable AgentRuntime record across concurrent role runs.
    pub selected_agent_profiles: BTreeMap<String, String>,
    pub verdict: TeamLiftVerdict,
    pub permission_lease: String,
    pub model_lease: String,
    pub backend_constraint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TeamBuild {
    pub graph: ExecutionGraph,
    pub tasks: Vec<TeamTaskSpec>,
    pub bindings: Vec<TeamTaskBinding>,
}

#[derive(Debug, Default)]
pub struct TeamBuilder;

impl TeamBuilder {
    pub fn build(&self, request: TeamBuildRequest) -> Result<TeamBuild, String> {
        if request.team_id.trim().is_empty()
            || request.session_id.trim().is_empty()
            || request.objective.trim().is_empty()
        {
            return Err("team build requires team_id, session_id, and objective".into());
        }
        if !request.verdict.accepted {
            return Err("team build requires an accepted collaboration lift verdict".into());
        }
        if matches!(
            request.template_id,
            TeamTemplateId::DebateConsensus
                | TeamTemplateId::ImplementationReviewFix
                | TeamTemplateId::IncidentResponse
        ) {
            return Err(
                "protocol templates must be compiled by TeamRuntime through execution_core::protocols"
                    .into(),
            );
        }
        TeamTemplateRegistry::validate(
            request.template_id,
            &request.roles,
            &request.role_dependencies,
        )
        .map_err(|error| error.to_string())?;
        let mut graph = ExecutionGraph::new(request.objective.clone());
        graph.id = format!("team-graph:{}", request.team_id);
        let mut tasks = Vec::new();
        let mut bindings = Vec::new();
        let mut agent_nodes = Vec::<String>::new();
        let mut task_id_by_role = BTreeMap::new();
        let mut node_id_by_role = BTreeMap::new();
        let mut role_dependencies = request.role_dependencies.clone();
        let parallel_limit = request.verdict.max_parallel_agents.max(1);
        // The Runner remains the only scheduler. When the lift gate shrinks
        // fanout below the requested role count, encode deterministic waves in
        // the graph itself rather than retaining a Team-side semaphore.
        for (index, role) in request.roles.iter().enumerate().skip(parallel_limit) {
            let from_role_id = request.roles[index - parallel_limit].role_id.clone();
            if !role_dependencies.iter().any(|dependency| {
                dependency.from_role_id == from_role_id && dependency.to_role_id == role.role_id
            }) {
                role_dependencies.push(TeamRoleDependency {
                    from_role_id,
                    to_role_id: role.role_id.clone(),
                });
            }
        }

        for (index, role) in request.roles.iter().enumerate() {
            let task_id = format!("{}:task:{}", request.team_id, role.role_id);
            let node_id = format!("{}:agent:{}", graph.id, role.role_id);
            let agent_id = format!("{}:agent:{}", request.team_id, role.role_id);
            let selected_profile = request.selected_agent_profiles.get(&role.role_id).cloned();
            let run_id = format!("{}:run:{}", request.team_id, role.role_id);
            let task = TeamTaskSpec {
                task_id: task_id.clone(),
                role_id: role.role_id.clone(),
                objective: format!(
                    "{}\n\nRole {}: {}",
                    request.objective, role.role_id, role.responsibility
                ),
                acceptance: role.acceptance.clone(),
                depends_on_task_ids: Vec::new(),
                context_refs: Vec::new(),
                evidence_refs: Vec::new(),
                allowed_tools: role.allowed_tools.clone(),
                permission_lease: request.permission_lease.clone(),
                model_lease: request.model_lease.clone(),
            };
            let packet = AgentTaskPacket {
                run_id: run_id.clone(),
                agent_id: agent_id.clone(),
                task_id: task_id.clone(),
                session_id: request.session_id.clone(),
                mission_id: None,
                team_id: Some(request.team_id.clone()),
                graph_id: graph.id.clone(),
                node_id: node_id.clone(),
                attempt: 1,
                expected_graph_revision: 0,
                objective: task.objective.clone(),
                acceptance: task.acceptance.clone(),
                constraints: std::iter::once(format!(
                    "team_template:{}",
                    request.template_id.as_str()
                ))
                .chain(
                    selected_profile
                        .into_iter()
                        .map(|profile| format!("agent_profile:{profile}")),
                )
                .chain(request.backend_constraint.clone())
                .collect(),
                context_refs: task.context_refs.clone(),
                evidence_refs: task.evidence_refs.clone(),
                allowed_tools: task.allowed_tools.clone(),
                allowed_skills: Vec::new(),
                permission_lease: task.permission_lease.clone(),
                model_lease: task.model_lease.clone(),
                budget_lease: ContextBudgetLeaseRef::new(
                    format!("team-budget:{}:{}", request.team_id, index),
                    agent_id.clone(),
                    "team_agent",
                    0,
                    1,
                ),
                idempotency_key: format!("team-agent:{}:{}", request.team_id, role.role_id),
            };
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::AgentTask,
                AgentTaskExecutor::KIND,
                serde_json::to_string(&packet).map_err(|error| error.to_string())?,
            );
            node.id = node_id.clone();
            node.idempotency_key = packet.idempotency_key.clone();
            node.acceptance.criteria = task.acceptance.clone();
            graph.nodes.push(node);
            task_id_by_role.insert(role.role_id.clone(), task_id.clone());
            node_id_by_role.insert(role.role_id.clone(), node_id.clone());
            tasks.push(task);
            bindings.push(TeamTaskBinding {
                team_id: request.team_id.clone(),
                task_id,
                agent_id,
                run_id,
                graph_id: graph.id.clone(),
                node_id: node_id.clone(),
                attempt: 1,
                expected_graph_revision: 0,
            });
            agent_nodes.push(node_id);
        }
        if graph.nodes.is_empty() {
            return Err("accepted team has no roles to compile".into());
        }

        for dependency in &role_dependencies {
            let from = node_id_by_role
                .get(&dependency.from_role_id)
                .ok_or_else(|| {
                    format!(
                        "team dependency source role `{}` is not part of the compiled team",
                        dependency.from_role_id
                    )
                })?
                .clone();
            let to = node_id_by_role
                .get(&dependency.to_role_id)
                .ok_or_else(|| {
                    format!(
                        "team dependency target role `{}` is not part of the compiled team",
                        dependency.to_role_id
                    )
                })?
                .clone();
            graph.edges.push(ExecutionEdge {
                from,
                to,
                kind: ExecutionEdgeKind::DependsOn,
            });
            let task = tasks
                .iter_mut()
                .find(|task| task.role_id == dependency.to_role_id)
                .ok_or_else(|| {
                    format!(
                        "team task for dependency target role `{}` is missing",
                        dependency.to_role_id
                    )
                })?;
            task.depends_on_task_ids.push(
                task_id_by_role
                    .get(&dependency.from_role_id)
                    .ok_or_else(|| {
                        format!(
                            "team task for dependency source role `{}` is missing",
                            dependency.from_role_id
                        )
                    })?
                    .clone(),
            );
        }

        let mut verify = ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            VerifyNodeExecutor::KIND,
            format!("team:{}", request.team_id),
        );
        verify.id = format!("{}:verify", graph.id);
        let mut synthesize = ExecutionNodeSpec::new(
            ExecutionNodeKind::Synthesize,
            SynthesizeNodeExecutor::KIND,
            format!("team:{}", request.team_id),
        );
        synthesize.id = format!("{}:synthesize", graph.id);
        for from in agent_nodes {
            graph.edges.push(ExecutionEdge {
                from,
                to: verify.id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            });
        }
        graph.edges.push(ExecutionEdge {
            from: verify.id.clone(),
            to: synthesize.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes.extend([verify, synthesize]);
        harness_contract::execution_graph::validate_execution_graph(&graph)
            .map_err(|error| error.to_string())?;
        Ok(TeamBuild {
            graph,
            tasks,
            bindings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(id: &str) -> TeamRoleSpec {
        TeamRoleSpec {
            role_id: id.into(),
            responsibility: format!("{id} responsibility"),
            required_capabilities: vec!["analysis".into()],
            allowed_tools: vec!["read_file".into()],
            acceptance: vec!["evidence".into()],
            evidence_duties: vec!["evidence".into()],
        }
    }

    #[test]
    fn compiles_task_first_parallel_agent_nodes_then_single_terminal_chain() {
        let build = TeamBuilder
            .build(TeamBuildRequest {
                team_id: "team-1".into(),
                session_id: "session-1".into(),
                objective: "compare independent options".into(),
                template_id: TeamTemplateId::FanoutResearchSynthesis,
                roles: vec![role("research-a"), role("research-b"), role("research-c")],
                role_dependencies: Vec::new(),
                selected_agent_profiles: BTreeMap::new(),
                verdict: TeamLiftVerdict {
                    accepted: true,
                    max_parallel_agents: 2,
                    reasons: vec!["independent".into()],
                    resized_from: 3,
                },
                permission_lease: "read_only".into(),
                model_lease: "fast".into(),
                backend_constraint: None,
            })
            .expect("team graph");
        assert_eq!(build.tasks.len(), 3);
        assert_eq!(build.bindings.len(), 3);
        assert_eq!(
            build
                .graph
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
                .count(),
            3
        );
        assert_eq!(
            build
                .graph
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Synthesize)
                .count(),
            1
        );
        assert!(build.graph.edges.iter().any(|edge| {
            edge.from == "team-graph:team-1:agent:research-a"
                && edge.to == "team-graph:team-1:agent:research-c"
                && edge.kind == ExecutionEdgeKind::DependsOn
        }));
        assert!(build.graph.edges.iter().any(|edge| {
            edge.from == "team-graph:team-1:verify"
                && edge.to == "team-graph:team-1:synthesize"
                && edge.kind == ExecutionEdgeKind::DependsOn
        }));
        assert!(build
            .bindings
            .iter()
            .all(|binding| binding.agent_id.starts_with("team-1:agent:")));
        assert_eq!(
            build
                .graph
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Verify)
                .count(),
            1
        );
    }
}
