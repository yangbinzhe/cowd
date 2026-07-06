//! Runtime-owned multi-agent team execution loop.

use std::collections::{BTreeMap, BTreeSet};

use harness_contract::{
    core::KernelRef,
    workgraph::{WorkEdgeKind, WorkGraph, WorkGraphQualityReport, WorkNode, WorkNodeKind},
};
use serde::{Deserialize, Serialize};

use crate::{
    global_agent_event_bus, global_agent_lifecycle_service, global_agent_task_mailbox,
    global_conflict_arbiter, global_mission_evidence_bus, global_team_runtime_service,
    AgentExecutionCommandKind, AgentProgressEvent, AgentTask, AgentTaskStatus,
    CollaborationTemplateId, ConflictResolutionRequest, ConflictSeverity, ConflictSourceKind,
    MissionEvidenceRef, TeamRuntimeSnapshot,
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
    pub workgraph: WorkGraph,
    pub workgraph_quality: WorkGraphQualityReport,
    pub ready_node_ids: Vec<String>,
    pub blocked_node_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamExecutionReport {
    pub kind: String,
    pub team_id: String,
    pub planned_task_count: usize,
    pub assigned_task_count: usize,
    pub delivered_agent_inputs: usize,
    pub events: Vec<AgentProgressEvent>,
    pub evidence: Vec<MissionEvidenceRef>,
    pub errors: Vec<String>,
    pub workgraph_id: String,
    pub workgraph_quality: WorkGraphQualityReport,
    pub ready_node_ids: Vec<String>,
    pub running_node_ids: Vec<String>,
    pub completed_node_ids: Vec<String>,
    pub blocked_node_ids: Vec<String>,
}

#[derive(Debug, Default)]
pub struct TeamExecutionLoop;

impl TeamExecutionLoop {
    pub fn plan(team_id: &str) -> Result<TeamExecutionPlan, String> {
        let team = global_team_runtime_service()
            .get(team_id)
            .ok_or_else(|| format!("team runtime not found: {team_id}"))?;
        let spec = runtime_spec_for(&team);
        let existing = global_agent_task_mailbox().list_for_team(team_id);
        let existing_by_role = existing
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
                        created_at_ms: 0,
                        updated_at_ms: 0,
                    })
            })
            .collect::<Vec<_>>();
        let workgraph = build_workgraph(&team, &spec, &tasks)?;
        let workgraph_quality = workgraph.quality_report();
        let ready_node_ids = workgraph
            .ready_nodes()
            .into_iter()
            .map(|node| node.id.clone())
            .collect::<Vec<_>>();
        let blocked_node_ids = graph_nodes_with_status(
            &workgraph,
            harness_contract::workgraph::WorkNodeStatus::Blocked,
        );
        Ok(TeamExecutionPlan {
            team_id: team.team_id,
            session_id: team.session_id,
            objective: team.objective,
            spec,
            tasks,
            workgraph,
            workgraph_quality,
            ready_node_ids,
            blocked_node_ids,
        })
    }

    pub fn tick_ready(team_id: &str) -> Result<TeamExecutionReport, String> {
        let plan = Self::plan(team_id)?;
        let mut assigned_task_count = 0usize;
        let mut delivered_agent_inputs = 0usize;
        let mut events = Vec::new();
        let mut evidence = Vec::new();
        let mut errors = Vec::new();
        let current_tasks = global_agent_task_mailbox().list_for_team(team_id);
        let existing_task_ids = current_tasks
            .iter()
            .map(|task| task.task_id.as_str())
            .collect::<BTreeSet<_>>();
        let task_by_node = task_by_workgraph_node(&plan);
        let ready_task_node_ids = plan
            .workgraph
            .ready_nodes()
            .into_iter()
            .filter(|node| node.kind == WorkNodeKind::AgentTask)
            .filter_map(|node| {
                task_by_node
                    .get(&node.id)
                    .filter(|task| !existing_task_ids.contains(task.task_id.as_str()))
                    .map(|_| node.id.clone())
            })
            .take(plan.spec.max_parallelism.max(1))
            .collect::<Vec<_>>();

        for node_id in &ready_task_node_ids {
            let Some(task) = task_by_node.get(node_id) else {
                continue;
            };
            assigned_task_count = assigned_task_count.saturating_add(1);
            global_agent_task_mailbox().assign((*task).clone());
            let assigned = (*task).clone();

            let progress = global_agent_event_bus().push(AgentProgressEvent {
                event_id: String::new(),
                team_id: plan.team_id.clone(),
                session_id: plan.session_id.clone(),
                agent_id: assigned.agent_id.clone(),
                role_id: assigned.role_id.clone(),
                task_id: Some(assigned.task_id.clone()),
                event_type: "agent.task.ready".to_string(),
                message: format!(
                    "workgraph node {node_id} ready for role {}",
                    assigned.role_id
                ),
                evidence_refs: assigned.evidence_refs.clone(),
                created_at_ms: 0,
            });
            events.push(progress);

            if let Some(agent_id) = &assigned.agent_id {
                let payload = serde_json::json!({
                    "task_id": assigned.task_id,
                    "objective": assigned.objective,
                    "expected_output": assigned.expected_output,
                    "context_refs": assigned.context_refs,
                    "evidence_refs": assigned.evidence_refs,
                });
                match global_agent_lifecycle_service().command(
                    agent_id,
                    AgentExecutionCommandKind::Input,
                    Some(payload),
                ) {
                    Ok(_) => {
                        delivered_agent_inputs = delivered_agent_inputs.saturating_add(1);
                        let _ = global_agent_task_mailbox().set_status(
                            &assigned.task_id,
                            AgentTaskStatus::Running,
                            "task delivered to agent",
                        );
                    }
                    Err(error) => {
                        errors.push(format!("{}: {error}", assigned.task_id));
                    }
                }
            }

            let evidence_ref = global_mission_evidence_bus().record(MissionEvidenceRef {
                evidence_id: String::new(),
                mission_id: Some("mission-control".to_string()),
                session_id: plan.session_id.clone(),
                team_id: Some(plan.team_id.clone()),
                agent_id: assigned.agent_id.clone(),
                kind: "agent_task".to_string(),
                summary: format!(
                    "{} task prepared from workgraph node {node_id}",
                    assigned.role_id
                ),
                source_ref: Some(assigned.task_id.clone()),
                created_at_ms: 0,
            });
            evidence.push(evidence_ref);
        }

        let final_plan = Self::plan(team_id)?;
        if !final_plan.blocked_node_ids.is_empty() {
            let _ = global_conflict_arbiter().resolve(ConflictResolutionRequest {
                source: ConflictSourceKind::WorkGraph,
                severity: ConflictSeverity::High,
                summary: format!(
                    "team workgraph {} has blocked nodes: {}",
                    final_plan.workgraph.id,
                    final_plan.blocked_node_ids.join(",")
                ),
                evidence_refs: vec![format!("workgraph:{}", final_plan.workgraph.id)],
                affected_scope: vec![
                    format!("session:{}", final_plan.session_id),
                    format!("team:{}", final_plan.team_id),
                ],
            });
        }
        Ok(TeamExecutionReport {
            kind: "runtime.team_execution_report".to_string(),
            team_id: plan.team_id,
            planned_task_count: plan.tasks.len(),
            assigned_task_count,
            delivered_agent_inputs,
            events,
            evidence,
            errors,
            workgraph_id: final_plan.workgraph.id.clone(),
            workgraph_quality: final_plan.workgraph_quality,
            ready_node_ids: final_plan.ready_node_ids,
            running_node_ids: graph_nodes_with_status(
                &final_plan.workgraph,
                harness_contract::workgraph::WorkNodeStatus::Running,
            ),
            completed_node_ids: graph_nodes_with_status(
                &final_plan.workgraph,
                harness_contract::workgraph::WorkNodeStatus::Completed,
            ),
            blocked_node_ids: final_plan.blocked_node_ids,
        })
    }
}

fn build_workgraph(
    team: &TeamRuntimeSnapshot,
    spec: &CollaborationTemplateRuntimeSpec,
    tasks: &[AgentTask],
) -> Result<WorkGraph, String> {
    let mut graph = WorkGraph::new(team.objective.clone());
    graph.id = format!("team-workgraph-{}", stable_id(&team.team_id));

    let mut role_node_ids = BTreeMap::new();
    for task in tasks {
        let node_id = workgraph_node_id(&team.team_id, &task.role_id);
        let mut node = WorkNode::new(
            WorkNodeKind::AgentTask,
            task.role_id.clone(),
            task.objective.clone(),
        );
        node.id = node_id.clone();
        node.agent_id = task.agent_id.clone();
        node.status = worknode_status_for_task(task.status);
        node.refs = vec![
            KernelRef {
                ref_type: "team".to_string(),
                id: team.team_id.clone(),
                label: Some("Team runtime".to_string()),
            },
            KernelRef {
                ref_type: "agent_task".to_string(),
                id: task.task_id.clone(),
                label: Some(task.role_id.clone()),
            },
        ];
        graph.add_node(node).map_err(|error| error.to_string())?;
        role_node_ids.insert(task.role_id.clone(), node_id);
    }

    for dependency in &spec.dependency_edges {
        let Some(from) = role_node_ids.get(&dependency.from_role_id) else {
            return Err(format!(
                "dependency references missing role: {}",
                dependency.from_role_id
            ));
        };
        let Some(to) = role_node_ids.get(&dependency.to_role_id) else {
            return Err(format!(
                "dependency references missing role: {}",
                dependency.to_role_id
            ));
        };
        graph
            .add_edge(from, to, WorkEdgeKind::DependsOn)
            .map_err(|error| error.to_string())?;
    }

    let review_id = format!("work-node-{}-review", stable_id(&team.team_id));
    let mut review = WorkNode::new(
        WorkNodeKind::Review,
        "review",
        format!("Review team output for {}", team.objective),
    );
    review.id = review_id.clone();
    review.refs = vec![KernelRef {
        ref_type: "team".to_string(),
        id: team.team_id.clone(),
        label: Some("Team runtime".to_string()),
    }];
    graph.add_node(review).map_err(|error| error.to_string())?;

    for role_node_id in role_node_ids.values() {
        graph
            .add_edge(role_node_id, &review_id, WorkEdgeKind::DependsOn)
            .map_err(|error| error.to_string())?;
    }

    let synthesis_id = format!("work-node-{}-synthesis", stable_id(&team.team_id));
    let mut synthesis = WorkNode::new(
        WorkNodeKind::Synthesis,
        "synthesis",
        spec.final_synthesis_policy.clone(),
    );
    synthesis.id = synthesis_id.clone();
    synthesis.refs = vec![KernelRef {
        ref_type: "team".to_string(),
        id: team.team_id.clone(),
        label: Some("Team runtime".to_string()),
    }];
    graph
        .add_node(synthesis)
        .map_err(|error| error.to_string())?;
    graph
        .add_edge(&review_id, &synthesis_id, WorkEdgeKind::DependsOn)
        .map_err(|error| error.to_string())?;

    for task in tasks.iter().filter(|task| {
        matches!(
            task.status,
            AgentTaskStatus::Failed | AgentTaskStatus::Cancelled
        )
    }) {
        let node_id = workgraph_node_id(&team.team_id, &task.role_id);
        graph
            .mark_failed(&node_id)
            .map_err(|error| error.to_string())?;
    }

    Ok(graph)
}

fn task_by_workgraph_node(plan: &TeamExecutionPlan) -> BTreeMap<String, &AgentTask> {
    plan.tasks
        .iter()
        .map(|task| (workgraph_node_id(&plan.team_id, &task.role_id), task))
        .collect()
}

fn worknode_status_for_task(
    status: AgentTaskStatus,
) -> harness_contract::workgraph::WorkNodeStatus {
    match status {
        AgentTaskStatus::Pending => harness_contract::workgraph::WorkNodeStatus::Planned,
        AgentTaskStatus::Claimed | AgentTaskStatus::Running => {
            harness_contract::workgraph::WorkNodeStatus::Running
        }
        AgentTaskStatus::Completed => harness_contract::workgraph::WorkNodeStatus::Completed,
        AgentTaskStatus::Failed | AgentTaskStatus::Cancelled => {
            harness_contract::workgraph::WorkNodeStatus::Failed
        }
    }
}

fn graph_nodes_with_status(
    graph: &WorkGraph,
    status: harness_contract::workgraph::WorkNodeStatus,
) -> Vec<String> {
    graph
        .nodes
        .iter()
        .filter(|node| node.status == status)
        .map(|node| node.id.clone())
        .collect()
}

fn make_task_id(team_id: &str, role_id: &str) -> String {
    format!("agent-task-{}-{}", stable_id(team_id), stable_id(role_id))
}

fn workgraph_node_id(team_id: &str, role_id: &str) -> String {
    format!("work-node-{}-{}", stable_id(team_id), stable_id(role_id))
}

fn stable_id(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    normalized.trim_matches('-').to_string()
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
    fn team_execution_uses_workgraph_ready_batch_for_sequential_roles() {
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("team-exec-session-{suffix}");
        crate::global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "team execution".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("session");
        let prompt = "implement feature then review and fix";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
        let team = global_team_runtime_service()
            .start(StartTeamRuntimeRequest {
                session_id: session_id.clone(),
                objective: prompt.to_string(),
                collaboration_decision: decision,
            })
            .expect("team");

        let plan = TeamExecutionLoop::plan(&team.team_id).expect("plan");
        assert_eq!(plan.team_id, team.team_id);
        assert!(!plan.tasks.is_empty());
        assert_eq!(plan.tasks.len(), plan.spec.roles.len());
        assert!(plan.workgraph_quality.is_dag);
        assert!(plan.workgraph_quality.has_review_node);
        assert!(plan.workgraph_quality.has_synthesis_node);

        let report = TeamExecutionLoop::tick_ready(&team.team_id).expect("tick");
        assert_eq!(report.kind, "runtime.team_execution_report");
        assert_eq!(report.planned_task_count, plan.tasks.len());
        assert_eq!(report.assigned_task_count, 1);
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.evidence.len(), 1);
        let assigned = global_agent_task_mailbox().list_for_team(&team.team_id);
        assert_eq!(assigned.len(), 1);

        global_agent_task_mailbox()
            .set_status(
                &assigned[0].task_id,
                AgentTaskStatus::Completed,
                "first role completed",
            )
            .expect("complete first role");
        let second_report = TeamExecutionLoop::tick_ready(&team.team_id).expect("second tick");
        assert_eq!(second_report.assigned_task_count, 1);
        assert_eq!(
            global_agent_task_mailbox()
                .list_for_team(&team.team_id)
                .len(),
            2
        );
        assert!(!global_agent_event_bus()
            .list_for_team(&team.team_id)
            .is_empty());
        assert!(!global_mission_evidence_bus()
            .list_for_team(&team.team_id)
            .is_empty());
    }

    #[test]
    fn team_execution_respects_template_max_parallelism_for_ready_nodes() {
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("team-fanout-session-{suffix}");
        crate::global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "team fanout execution".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("session");
        let prompt = "research compare analyze multiple implementations and synthesize findings";
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
        assert_eq!(
            decision.template_id,
            CollaborationTemplateId::FanoutResearchSynthesis
        );
        let team = global_team_runtime_service()
            .start(StartTeamRuntimeRequest {
                session_id,
                objective: prompt.to_string(),
                collaboration_decision: decision,
            })
            .expect("team");

        let plan = TeamExecutionLoop::plan(&team.team_id).expect("plan");
        assert!(plan.ready_node_ids.len() >= 2);
        assert_eq!(plan.spec.max_parallelism, 3);

        let report = TeamExecutionLoop::tick_ready(&team.team_id).expect("tick");
        assert_eq!(report.assigned_task_count, 3);
        assert_eq!(
            global_agent_task_mailbox()
                .list_for_team(&team.team_id)
                .len(),
            3
        );
        assert!(report.workgraph_quality.is_dag);
    }

    #[test]
    fn blocked_workgraph_nodes_emit_conflict_receipt() {
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("team-conflict-session-{suffix}");
        crate::global_mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "team conflict execution".to_string(),
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

        let first = TeamExecutionLoop::tick_ready(&team.team_id).expect("first tick");
        assert_eq!(first.assigned_task_count, 1);
        let assigned = global_agent_task_mailbox().list_for_team(&team.team_id);
        global_agent_task_mailbox()
            .set_status(
                &assigned[0].task_id,
                AgentTaskStatus::Failed,
                "first role failed",
            )
            .expect("fail first role");
        let before = global_conflict_arbiter().receipts().len();
        let report = TeamExecutionLoop::tick_ready(&team.team_id).expect("blocked tick");
        assert!(!report.blocked_node_ids.is_empty());
        let receipts = global_conflict_arbiter().receipts();
        assert!(receipts.len() > before);
        assert!(receipts
            .iter()
            .any(|receipt| receipt.source == ConflictSourceKind::WorkGraph
                && receipt.summary.contains("blocked nodes")));
    }
}
