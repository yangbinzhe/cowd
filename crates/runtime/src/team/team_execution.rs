//! Pure team planning into the canonical execution graph.
//!
//! Team templates describe work. They do not dispatch agents or commit state;
//! the execution graph runner is the sole execution writer.

use std::collections::{BTreeMap, BTreeSet};

use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdge, ExecutionEdgeKind, ExecutionGraph,
    ExecutionGraphQualityReport, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};

use crate::{
    global_agent_task_mailbox, global_team_runtime_service, AgentTask, AgentTaskStatus,
    CollaborationTemplateId, TeamRuntimeSnapshot,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationTemplateRuntimeSpec {
    pub template_id: CollaborationTemplateId,
    pub roles: Vec<TeamExecutionRoleSpec>,
    pub dependency_edges: Vec<TeamExecutionDependency>,
    pub max_parallelism: usize,
    pub review_gates: Vec<String>,
    pub expected_outputs: Vec<String>,
    pub failure_policy: String,
    pub final_synthesis_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamExecutionRoleSpec {
    pub role_id: String,
    pub responsibility: String,
    pub allowed_tools: Vec<String>,
    pub evidence_duties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamExecutionDependency {
    pub from_role_id: String,
    pub to_role_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamExecutionPlan {
    pub team_id: String,
    pub session_id: String,
    pub objective: String,
    pub spec: CollaborationTemplateRuntimeSpec,
    pub tasks: Vec<AgentTask>,
    pub execution_graph: ExecutionGraph,
    pub execution_graph_quality: ExecutionGraphQualityReport,
    pub ready_node_ids: Vec<String>,
    pub blocked_node_ids: Vec<String>,
}

#[derive(Debug, Default)]
pub struct TeamExecutionLoop;

impl TeamExecutionLoop {
    /// Build a deterministic execution graph projection for a persisted team.
    /// This function performs no dispatch, lifecycle, evidence, or status write.
    pub fn plan(team_id: &str) -> Result<TeamExecutionPlan, String> {
        let team = global_team_runtime_service()
            .get(team_id)
            .ok_or_else(|| format!("team runtime not found: {team_id}"))?;
        let spec = runtime_spec_for(&team);
        let existing_by_role = global_agent_task_mailbox()
            .list_for_team(team_id)
            .into_iter()
            .map(|task| (task.role_id.clone(), task))
            .collect::<BTreeMap<_, _>>();
        let tasks = team
            .agents
            .iter()
            .map(|agent| {
                existing_by_role
                    .get(&agent.role_id)
                    .cloned()
                    .unwrap_or_else(|| AgentTask {
                        task_id: make_task_id(&team.team_id, &agent.role_id),
                        team_id: team.team_id.clone(),
                        session_id: team.session_id.clone(),
                        role_id: agent.role_id.clone(),
                        agent_id: agent.agent_id.clone(),
                        objective: format!("{}: {}", agent.role_id, team.objective),
                        expected_output: format!(
                            "Produce role output for {} and evidence for {}",
                            agent.role_id,
                            team.template_id.as_str()
                        ),
                        context_refs: vec![format!("team:{}", team.team_id)],
                        evidence_refs: agent.evidence_duties.clone(),
                        status: AgentTaskStatus::Pending,
                        outcome: None,
                        created_at_ms: 0,
                        updated_at_ms: 0,
                    })
            })
            .collect::<Vec<_>>();
        let execution_graph = build_execution_graph(&team, &spec, &tasks)?;
        let execution_graph_quality = execution_graph_quality(&execution_graph);
        let ready_node_ids = ready_node_ids(&execution_graph);
        let blocked_node_ids =
            graph_nodes_with_status(&execution_graph, ExecutionNodeStatus::Blocked);
        Ok(TeamExecutionPlan {
            team_id: team.team_id,
            session_id: team.session_id,
            objective: team.objective,
            spec,
            tasks,
            execution_graph,
            execution_graph_quality,
            ready_node_ids,
            blocked_node_ids,
        })
    }
}

fn build_execution_graph(
    team: &TeamRuntimeSnapshot,
    spec: &CollaborationTemplateRuntimeSpec,
    tasks: &[AgentTask],
) -> Result<ExecutionGraph, String> {
    let mut graph = ExecutionGraph::new(team.objective.clone());
    graph.id = format!("team-execution-graph-{}", stable_id(&team.team_id));
    let mut role_node_ids = BTreeMap::new();

    for task in tasks {
        let node_id = execution_node_id(&team.team_id, &task.role_id);
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "runtime.agent",
            format!("agent_task:{}", task.task_id),
        );
        node.id = node_id.clone();
        node.idempotency_key = format!("{}:{}", graph.id, task.task_id);
        node.resource_scopes = vec![
            format!("session:{}", team.session_id),
            format!("team:{}", team.team_id),
        ];
        graph
            .node_statuses
            .insert(node_id.clone(), execution_status_for_task(task.status));
        graph.nodes.push(node);
        role_node_ids.insert(task.role_id.clone(), node_id);
    }

    for dependency in &spec.dependency_edges {
        let from = role_node_ids.get(&dependency.from_role_id).ok_or_else(|| {
            format!(
                "dependency references missing role: {}",
                dependency.from_role_id
            )
        })?;
        let to = role_node_ids.get(&dependency.to_role_id).ok_or_else(|| {
            format!(
                "dependency references missing role: {}",
                dependency.to_role_id
            )
        })?;
        graph.edges.push(ExecutionEdge {
            from: from.clone(),
            to: to.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
    }

    let verify_id = format!("execution-node-{}-verify", stable_id(&team.team_id));
    let mut verify = ExecutionNodeSpec::new(
        ExecutionNodeKind::Verify,
        "runtime.verify",
        format!("team:{}:review", team.team_id),
    );
    verify.id = verify_id.clone();
    verify.idempotency_key = format!("{}:{verify_id}", graph.id);
    verify.acceptance.criteria = spec.review_gates.clone();
    graph
        .node_statuses
        .insert(verify_id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(verify);
    for role_node_id in role_node_ids.values() {
        graph.edges.push(ExecutionEdge {
            from: role_node_id.clone(),
            to: verify_id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
    }

    let synthesis_id = format!("execution-node-{}-synthesis", stable_id(&team.team_id));
    let mut synthesis = ExecutionNodeSpec::new(
        ExecutionNodeKind::Synthesize,
        "runtime.synthesize",
        format!("team:{}:synthesis", team.team_id),
    );
    synthesis.id = synthesis_id.clone();
    synthesis.idempotency_key = format!("{}:{synthesis_id}", graph.id);
    synthesis.acceptance.criteria = vec![spec.final_synthesis_policy.clone()];
    graph
        .node_statuses
        .insert(synthesis_id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(synthesis);
    graph.edges.push(ExecutionEdge {
        from: verify_id,
        to: synthesis_id,
        kind: ExecutionEdgeKind::DependsOn,
    });

    validate_execution_graph(&graph).map_err(|error| error.to_string())?;
    mark_blocked_dependents(&mut graph);
    Ok(graph)
}

fn execution_status_for_task(status: AgentTaskStatus) -> ExecutionNodeStatus {
    match status {
        AgentTaskStatus::Pending => ExecutionNodeStatus::Planned,
        AgentTaskStatus::Claimed | AgentTaskStatus::Running => ExecutionNodeStatus::Running,
        AgentTaskStatus::Completed => ExecutionNodeStatus::Completed,
        AgentTaskStatus::Failed => ExecutionNodeStatus::Failed,
        AgentTaskStatus::Cancelled => ExecutionNodeStatus::Cancelled,
    }
}

fn graph_nodes_with_status(graph: &ExecutionGraph, status: ExecutionNodeStatus) -> Vec<String> {
    graph
        .node_statuses
        .iter()
        .filter_map(|(id, current)| (*current == status).then_some(id.clone()))
        .collect()
}

fn ready_node_ids(graph: &ExecutionGraph) -> Vec<String> {
    graph
        .nodes
        .iter()
        .filter_map(|node| {
            let current = graph
                .node_statuses
                .get(&node.id)
                .copied()
                .unwrap_or(ExecutionNodeStatus::Planned);
            if current != ExecutionNodeStatus::Planned {
                return None;
            }
            graph
                .edges
                .iter()
                .filter(|edge| edge.kind == ExecutionEdgeKind::DependsOn && edge.to == node.id)
                .map(|edge| {
                    graph
                        .node_statuses
                        .get(&edge.from)
                        .copied()
                        .unwrap_or(ExecutionNodeStatus::Planned)
                })
                .all(|status| status == ExecutionNodeStatus::Completed)
                .then_some(node.id.clone())
        })
        .collect()
}

fn mark_blocked_dependents(graph: &mut ExecutionGraph) {
    let mut blocked = graph
        .node_statuses
        .iter()
        .filter_map(|(id, status)| {
            matches!(
                status,
                ExecutionNodeStatus::Failed
                    | ExecutionNodeStatus::Cancelled
                    | ExecutionNodeStatus::Blocked
            )
            .then_some(id.clone())
        })
        .collect::<BTreeSet<_>>();
    loop {
        let before = blocked.len();
        for edge in graph
            .edges
            .iter()
            .filter(|edge| edge.kind == ExecutionEdgeKind::DependsOn)
        {
            if blocked.contains(&edge.from) {
                blocked.insert(edge.to.clone());
            }
        }
        if blocked.len() == before {
            break;
        }
    }
    for id in blocked {
        if !matches!(
            graph.node_statuses.get(&id),
            Some(ExecutionNodeStatus::Failed | ExecutionNodeStatus::Cancelled)
        ) {
            graph.node_statuses.insert(id, ExecutionNodeStatus::Blocked);
        }
    }
}

fn execution_graph_quality(graph: &ExecutionGraph) -> ExecutionGraphQualityReport {
    let validation = validate_execution_graph(graph);
    ExecutionGraphQualityReport {
        node_count: graph.nodes.len(),
        edge_count: graph.edges.len(),
        ready_count: ready_node_ids(graph).len(),
        blocked_count: graph_nodes_with_status(graph, ExecutionNodeStatus::Blocked).len(),
        failed_count: graph_nodes_with_status(graph, ExecutionNodeStatus::Failed).len(),
        has_verify_node: graph
            .nodes
            .iter()
            .any(|node| node.kind == ExecutionNodeKind::Verify),
        has_synthesize_node: graph
            .nodes
            .iter()
            .any(|node| node.kind == ExecutionNodeKind::Synthesize),
        is_dag: validation.is_ok(),
        warnings: validation
            .err()
            .map(|error| vec![error.to_string()])
            .unwrap_or_default(),
    }
}

fn make_task_id(team_id: &str, role_id: &str) -> String {
    format!("agent-task-{}-{}", stable_id(team_id), stable_id(role_id))
}

fn execution_node_id(team_id: &str, role_id: &str) -> String {
    format!(
        "execution-node-{}-{}",
        stable_id(team_id),
        stable_id(role_id)
    )
}

fn stable_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn runtime_spec_for(team: &TeamRuntimeSnapshot) -> CollaborationTemplateRuntimeSpec {
    let roles = team
        .agents
        .iter()
        .map(|agent| TeamExecutionRoleSpec {
            role_id: agent.role_id.clone(),
            responsibility: agent.responsibility.clone(),
            allowed_tools: agent.allowed_tools.clone(),
            evidence_duties: agent.evidence_duties.clone(),
        })
        .collect::<Vec<_>>();
    let dependency_edges = match team.template_id {
        CollaborationTemplateId::SingleExecutor => Vec::new(),
        CollaborationTemplateId::FanoutResearchSynthesis => roles
            .iter()
            .filter(|role| role.role_id != "synthesizer")
            .map(|role| TeamExecutionDependency {
                from_role_id: role.role_id.clone(),
                to_role_id: "synthesizer".to_string(),
                reason: "research output feeds synthesis".to_string(),
            })
            .collect(),
        CollaborationTemplateId::DebateConsensus => vec![
            TeamExecutionDependency {
                from_role_id: "proposer".to_string(),
                to_role_id: "arbiter".to_string(),
                reason: "proposer argument feeds arbitration".to_string(),
            },
            TeamExecutionDependency {
                from_role_id: "skeptic".to_string(),
                to_role_id: "arbiter".to_string(),
                reason: "skeptic critique feeds arbitration".to_string(),
            },
        ],
        _ => roles
            .windows(2)
            .map(|pair| TeamExecutionDependency {
                from_role_id: pair[0].role_id.clone(),
                to_role_id: pair[1].role_id.clone(),
                reason: "template role sequence".to_string(),
            })
            .collect(),
    };
    CollaborationTemplateRuntimeSpec {
        template_id: team.template_id,
        roles,
        dependency_edges,
        max_parallelism: team.plan.max_parallelism,
        review_gates: team.plan.human_approval_points.clone(),
        expected_outputs: team
            .agents
            .iter()
            .map(|agent| format!("{} output", agent.role_id))
            .collect(),
        failure_policy: "failed role blocks final synthesis unless steward overrides".to_string(),
        final_synthesis_policy: team.plan.merge_contract.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CollaborationTemplateMatcher, StartMissionSessionRequest, StartTeamRuntimeRequest,
    };
    use harness_contract::strategy::{decide_strategy, StrategyInput};

    #[test]
    fn team_plan_is_a_valid_canonical_graph_without_dispatch_side_effects() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("team-plan-session-{suffix}");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "team planning".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("session");
        let prompt = "implement feature then review and fix";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
        let team = global_team_runtime_service()
            .start(StartTeamRuntimeRequest {
                session_id,
                objective: prompt.to_string(),
                collaboration_decision: decision,
            })
            .expect("team");

        let before = global_agent_task_mailbox().list_for_team(&team.team_id);
        let plan = TeamExecutionLoop::plan(&team.team_id).expect("plan");
        let after = global_agent_task_mailbox().list_for_team(&team.team_id);

        assert!(before.is_empty() && after.is_empty());
        assert_eq!(plan.tasks.len(), plan.spec.roles.len());
        assert!(plan.execution_graph_quality.is_dag);
        assert!(plan.execution_graph_quality.has_verify_node);
        assert!(plan.execution_graph_quality.has_synthesize_node);
        assert!(!plan.ready_node_ids.is_empty());
    }
}
