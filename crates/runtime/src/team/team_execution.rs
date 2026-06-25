//! Runtime-owned multi-agent team execution loop.

use serde::{Deserialize, Serialize};

use crate::{
    global_agent_event_bus, global_agent_lifecycle_service, global_agent_task_mailbox,
    global_mission_evidence_bus, global_team_runtime_service, AgentExecutionCommandKind,
    AgentProgressEvent, AgentTask, AgentTaskStatus, CollaborationTemplateId, MissionEvidenceRef,
    TeamRuntimeSnapshot,
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
        let tasks = if existing.is_empty() {
            team.agents
                .iter()
                .map(|agent| AgentTask {
                    task_id: format!("agent-task-{}", uuid::Uuid::new_v4()),
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
                .collect::<Vec<_>>()
        } else {
            existing
        };
        Ok(TeamExecutionPlan {
            team_id: team.team_id,
            session_id: team.session_id,
            objective: team.objective,
            spec,
            tasks,
        })
    }

    pub fn tick_ready(team_id: &str) -> Result<TeamExecutionReport, String> {
        let plan = Self::plan(team_id)?;
        let mut assigned_task_count = 0usize;
        let mut delivered_agent_inputs = 0usize;
        let mut events = Vec::new();
        let mut evidence = Vec::new();
        let mut errors = Vec::new();

        for task in &plan.tasks {
            let current_tasks = global_agent_task_mailbox().list_for_team(team_id);
            let already_exists = current_tasks.iter().any(|current| {
                current.role_id == task.role_id && current.objective == task.objective
            });
            let assigned = if already_exists {
                current_tasks
                    .into_iter()
                    .find(|current| {
                        current.role_id == task.role_id && current.objective == task.objective
                    })
                    .unwrap_or_else(|| task.clone())
            } else {
                assigned_task_count = assigned_task_count.saturating_add(1);
                global_agent_task_mailbox().assign(task.clone());
                task.clone()
            };

            let progress = global_agent_event_bus().push(AgentProgressEvent {
                event_id: String::new(),
                team_id: plan.team_id.clone(),
                session_id: plan.session_id.clone(),
                agent_id: assigned.agent_id.clone(),
                role_id: assigned.role_id.clone(),
                task_id: Some(assigned.task_id.clone()),
                event_type: "agent.task.ready".to_string(),
                message: format!("task ready for role {}", assigned.role_id),
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
                summary: format!("{} task prepared", assigned.role_id),
                source_ref: Some(assigned.task_id.clone()),
                created_at_ms: 0,
            });
            evidence.push(evidence_ref);
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
        })
    }
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
                from_role_id: "pro".to_string(),
                to_role_id: "judge".to_string(),
                reason: "pro argument feeds judging".to_string(),
            },
            TeamExecutionDependency {
                from_role_id: "con".to_string(),
                to_role_id: "judge".to_string(),
                reason: "con argument feeds judging".to_string(),
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
    fn team_execution_assigns_agent_tasks_events_and_evidence() {
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

        let report = TeamExecutionLoop::tick_ready(&team.team_id).expect("tick");
        assert_eq!(report.kind, "runtime.team_execution_report");
        assert_eq!(report.planned_task_count, plan.tasks.len());
        assert_eq!(report.assigned_task_count, plan.tasks.len());
        assert_eq!(report.events.len(), plan.tasks.len());
        assert_eq!(report.evidence.len(), plan.tasks.len());
        assert!(!global_agent_task_mailbox()
            .list_for_team(&team.team_id)
            .is_empty());
        assert!(!global_agent_event_bus()
            .list_for_team(&team.team_id)
            .is_empty());
        assert!(!global_mission_evidence_bus()
            .list_for_team(&team.team_id)
            .is_empty());
    }
}
