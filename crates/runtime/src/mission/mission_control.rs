//! Pure Mission Control projection reducer.
//!
//! Gateway owns the cached materialized view and supplies canonical Session
//! nodes. Runtime contributes only projections from Runtime-owned aggregates.

use std::collections::BTreeMap;

use harness_contract::mission::{
    MissionControlActionReadiness, MissionControlAgentNode, MissionControlApprovalNode,
    MissionControlEventDigest, MissionControlEventLine, MissionControlGraphEdge,
    MissionControlGraphNode, MissionControlGraphProjection, MissionControlMissionSummary,
    MissionControlProjection, MissionControlReadiness, MissionControlSessionNode,
    MissionControlSummary, MissionControlTaskNode, MissionControlTeamNode,
    MissionWorkspaceProjection, MISSION_CONTROL_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};

use crate::{RuntimeEventScope, RuntimeServices};

#[derive(Debug, Default)]
pub struct MissionControlRuntime;

impl MissionControlRuntime {
    #[must_use]
    pub fn projection(
        services: &RuntimeServices,
        sessions: Vec<MissionControlSessionNode>,
        active_session_id: Option<String>,
        selected_mission_id: Option<String>,
    ) -> MissionControlProjection {
        build_projection(
            services,
            sessions,
            active_session_id,
            selected_mission_id,
            true,
        )
    }

    #[must_use]
    pub fn summary_projection(
        services: &RuntimeServices,
        sessions: Vec<MissionControlSessionNode>,
        active_session_id: Option<String>,
        selected_mission_id: Option<String>,
    ) -> MissionControlProjection {
        build_projection(
            services,
            sessions,
            active_session_id,
            selected_mission_id,
            false,
        )
    }
}

fn build_projection(
    services: &RuntimeServices,
    sessions: Vec<MissionControlSessionNode>,
    active_session_id: Option<String>,
    selected_mission_id: Option<String>,
    include_execution_details: bool,
) -> MissionControlProjection {
    let mut mission_aggregates = services.mission_runtime().aggregates();
    mission_aggregates.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| left.mission_id.cmp(&right.mission_id))
    });
    let selected_mission_id = selected_mission_id
        .filter(|mission_id| {
            mission_aggregates
                .iter()
                .any(|mission| mission.mission_id == *mission_id)
        })
        .unwrap_or_else(|| services.mission_runtime().default_mission_id().to_string());
    let selected_aggregate = mission_aggregates
        .iter()
        .find(|mission| mission.mission_id == selected_mission_id)
        .cloned();
    let mission = services.mission_runtime().projection_for(
        &selected_mission_id,
        services.session_relations(),
        services.agent_runtime(),
        services.team_runtime(),
        services.approval_queue(),
        services.conflict_resolver(),
        services.mission_evidence(),
        services.mission_schedules().projection(),
    );
    let team_projection = services.team_runtime().projection_json();
    let agent_projection = serde_json::json!({
        "kind": "runtime.agents",
        "agents": services.agent_runtime().list(),
    });
    let workspace_approval_projection = services.approval_queue().projection();
    let all_tasks = services.task_aggregate_service().list().unwrap_or_default();
    let graph_bindings = all_tasks
        .iter()
        .flat_map(|task| {
            task.graph_refs.iter().map(|graph| {
                (
                    graph.graph_id.clone(),
                    (task.mission_id.clone(), task.task_id.clone()),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let tasks = task_nodes(services, &all_tasks)
        .into_iter()
        .filter(|task| task.mission_id == selected_mission_id)
        .collect::<Vec<_>>();
    let teams = team_nodes(&team_projection, &graph_bindings)
        .into_iter()
        .filter(|team| team.mission_id.as_deref() == Some(selected_mission_id.as_str()))
        .collect::<Vec<_>>();
    let agents = agent_nodes(&agent_projection)
        .into_iter()
        .filter(|agent| agent.mission_id.as_deref() == Some(selected_mission_id.as_str()))
        .collect::<Vec<_>>();
    let selected_session_ids = mission_session_ids(services, &all_tasks, &selected_mission_id);
    let selected_task_ids = tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let selected_sessions = sessions
        .iter()
        .filter(|session| selected_session_ids.contains(session.session_id.as_str()))
        .cloned()
        .map(|mut session| {
            let task_ids = all_tasks
                .iter()
                .filter(|task| {
                    task.mission_id == selected_mission_id
                        && task_sessions(services, task).contains(&session.session_id)
                })
                .map(|task| task.task_id.clone())
                .collect::<Vec<_>>();
            session.contributing_task_count = task_ids.len();
            session.contributing_task_ids = task_ids;
            session
        })
        .collect::<Vec<_>>();
    let selected_agent_ids = agents
        .iter()
        .map(|agent| agent.agent_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let approvals = approval_nodes(&mission.approval_projection);
    let organization_decisions = services
        .task_runtime_port()
        .organization_decisions(None, 100)
        .unwrap_or_default()
        .into_iter()
        .filter(|decision| {
            decision.target_mission_id == selected_mission_id
                || decision
                    .canonical_root_task_id()
                    .is_some_and(|task_id| selected_task_ids.contains(task_id))
                || decision
                    .affected_task_ids
                    .iter()
                    .any(|task_id| selected_task_ids.contains(task_id))
        })
        .collect::<Vec<_>>();
    let selected_team_ids = teams
        .iter()
        .map(|team| team.team_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let selected_graph_ids = all_tasks
        .iter()
        .filter(|task| task.mission_id == selected_mission_id)
        .flat_map(|task| {
            task.graph_refs
                .iter()
                .map(|reference| reference.graph_id.clone())
        })
        .collect::<std::collections::BTreeSet<_>>();
    let unambiguous_session_ids = selected_session_ids
        .iter()
        .filter(|session_id| {
            let mission_ids = all_tasks
                .iter()
                .filter(|task| task_sessions(services, task).contains(*session_id))
                .map(|task| task.mission_id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            mission_ids.len() == 1 && mission_ids.contains(&selected_mission_id)
        })
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let event_digest = event_digest_for_mission(
        50,
        services,
        &selected_mission_id,
        &selected_task_ids,
        &selected_graph_ids,
        &selected_team_ids,
        &selected_agent_ids,
        &unambiguous_session_ids,
    );
    let execution_graphs = if include_execution_details {
        mission_execution_graph_summary(services, &selected_graph_ids, &graph_bindings)
    } else {
        serde_json::json!({
            "kind": "runtime.mission_execution_graphs",
            "count": selected_graph_ids.len(),
            "execution_graphs": [],
            "relation_source": "task_lineage",
            "detail": "summary",
        })
    };
    let relations = mission.relation_projection.clone();
    let conflicts = mission.conflict_projection.clone();
    let evidence = mission.evidence_projection.clone();
    let capabilities = mission.capability_projection.clone();
    let mission_health = mission.health_projection.clone();
    let summary = summary(
        &selected_sessions,
        active_session_id.clone(),
        tasks.len(),
        teams.len(),
        agents.len(),
        approvals
            .iter()
            .filter(|approval| approval.status == "pending")
            .count(),
        event_digest.recovery_required.len(),
        organization_decisions
            .iter()
            .filter(|decision| {
                matches!(
                    decision.status,
                    harness_contract::mission::MissionOrganizationStatus::Pending
                        | harness_contract::mission::MissionOrganizationStatus::Claimed
                        | harness_contract::mission::MissionOrganizationStatus::Failed
                )
            })
            .count(),
    );
    let workspace = MissionWorkspaceProjection {
        workspace_id: services.workspace_key().to_string(),
        title: "Mission Control".to_string(),
        active_session_id,
        session_count: sessions.len(),
        running_agent_count: agent_nodes(&agent_projection)
            .iter()
            .filter(|agent| agent.status.as_deref() == Some("running"))
            .count(),
        pending_approval_count: approval_nodes(&workspace_approval_projection)
            .iter()
            .filter(|approval| approval.status == "pending")
            .count(),
        recovery_required_count: workspace_recovery_required_count(services),
    };
    let control_readiness = control_readiness(
        &summary,
        &selected_sessions,
        &teams,
        &agents,
        &approvals,
        &conflicts,
    );
    let missions = mission_aggregates
        .iter()
        .map(|aggregate| {
            mission_summary(
                services,
                aggregate,
                &all_tasks,
                &team_projection,
                &agent_projection,
            )
        })
        .collect();
    let mission_graph = mission_graph(
        services,
        &selected_mission_id,
        selected_aggregate.as_ref(),
        &selected_sessions,
        &tasks,
        &teams,
        &agents,
        &approvals,
        &graph_bindings,
        &mission.schedule_projection,
        &conflicts,
        &event_digest,
        include_execution_details,
    );
    let pending_organization_count = summary.pending_organization_count;

    MissionControlProjection {
        schema_version: MISSION_CONTROL_SCHEMA_VERSION,
        kind: "mission_control.projection".to_string(),
        workspace,
        summary,
        control_readiness,
        selected_mission_id,
        missions,
        mission: serde_json::to_value(mission).unwrap_or_default(),
        sessions: selected_sessions,
        tasks,
        teams,
        agents,
        approvals,
        organization_decisions,
        mission_graph,
        relations,
        execution_graphs,
        conflicts,
        evidence,
        capabilities,
        event_digest,
        health: serde_json::json!({
            "ok": mission_health["ok"].as_bool().unwrap_or(true),
            "status": mission_health["status"].as_str().unwrap_or("ready"),
            "mission": mission_health,
            "session_owner": "gateway.session_service",
            "projection_owner": "gateway.mission_materialized_projector",
            "mission_organizer": {
                "pending": pending_organization_count,
                "source": "runtime.task.organization_decisions",
            },
        }),
    }
}

fn mission_execution_graph_summary(
    services: &RuntimeServices,
    selected_graph_ids: &std::collections::BTreeSet<String>,
    graph_bindings: &BTreeMap<String, (String, String)>,
) -> serde_json::Value {
    use harness_contract::execution_graph::ExecutionNodeStatus;

    let graphs = selected_graph_ids
        .iter()
        .filter_map(|graph_id| {
            let projection = services.graph_state_store().projection(graph_id).ok()?;
            let mut ready = 0usize;
            let mut blocked = 0usize;
            let mut running = 0usize;
            let mut failed = 0usize;
            for node in &projection.nodes {
                match node.status {
                    ExecutionNodeStatus::Planned | ExecutionNodeStatus::Ready => ready += 1,
                    ExecutionNodeStatus::Running
                    | ExecutionNodeStatus::WaitingInput
                    | ExecutionNodeStatus::WaitingApproval
                    | ExecutionNodeStatus::WaitingExternal
                    | ExecutionNodeStatus::Paused => running += 1,
                    ExecutionNodeStatus::Blocked => blocked += 1,
                    ExecutionNodeStatus::Failed => failed += 1,
                    ExecutionNodeStatus::Completed | ExecutionNodeStatus::Cancelled => {}
                }
            }
            let status = if blocked > 0 {
                "partial"
            } else if failed > 0 {
                "failed"
            } else if running > 0 {
                "running"
            } else if ready > 0 {
                "planned"
            } else {
                "completed"
            };
            let binding = graph_bindings.get(graph_id);
            Some(serde_json::json!({
                "graph_id": graph_id,
                "task_id": binding.as_ref().map(|(_, task_id)| task_id),
                "mission_id": binding.as_ref().map(|(mission_id, _)| mission_id),
                "objective": projection.objective,
                "status": status,
                "nodes_count": projection.nodes.len(),
                "edges_count": projection.edges.len(),
                "ready_count": ready,
                "blocked_count": blocked,
                "running_count": running,
                "failed_count": failed,
            }))
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "kind": "runtime.mission_execution_graphs",
        "count": graphs.len(),
        "execution_graphs": graphs,
        "relation_source": "task_lineage",
    })
}

fn control_readiness(
    summary: &MissionControlSummary,
    sessions: &[MissionControlSessionNode],
    teams: &[MissionControlTeamNode],
    agents: &[MissionControlAgentNode],
    approvals: &[MissionControlApprovalNode],
    conflicts: &serde_json::Value,
) -> MissionControlReadiness {
    let pending_approvals = approvals
        .iter()
        .filter(|approval| approval.status == "pending")
        .count();
    let runnable_teams = teams
        .iter()
        .filter(|team| {
            team.status
                .as_deref()
                .map(|status| matches!(status, "running" | "ready" | "active" | "planned"))
                .unwrap_or(true)
        })
        .count();
    let routable_agents = agents
        .iter()
        .filter(|agent| {
            agent
                .status
                .as_deref()
                .is_some_and(|status| matches!(status, "running" | "ready" | "working"))
        })
        .count();
    let dispatchable_sessions = sessions
        .iter()
        .filter(|session| {
            session.active
                && !matches!(
                    session.status.as_str(),
                    "archived" | "deleted" | "archiving" | "deleting"
                )
        })
        .count();
    let high_conflict_count = high_or_critical_conflict_count(conflicts);
    let critical_conflict_count = critical_conflict_count(conflicts);
    let mut actions = vec![
        readiness(
            "team.create",
            dispatchable_sessions > 0,
            if dispatchable_sessions > 0 {
                "canonical Session is available for a Team"
            } else {
                "no active canonical Session is available"
            },
            false,
            Some("runtime.team_instantiation"),
            dispatchable_sessions,
        ),
        readiness(
            "agent.input",
            routable_agents > 0,
            if routable_agents > 0 {
                "running Agents accept governed input"
            } else {
                "no running Agent accepts input"
            },
            false,
            Some("runtime.agent_command"),
            routable_agents,
        ),
        readiness(
            "approval.decide",
            pending_approvals > 0,
            if pending_approvals > 0 {
                "pending approval requests require an authenticated human"
            } else {
                "no pending approval request"
            },
            true,
            Some("runtime.approval_queue"),
            pending_approvals,
        ),
        readiness(
            "recovery.plan",
            summary.recovery_required_count > 0,
            if summary.recovery_required_count > 0 {
                "runtime events contain recoverable work"
            } else {
                "no recovery candidate currently visible"
            },
            false,
            Some("runtime.recovery_planner"),
            summary.recovery_required_count,
        ),
        readiness(
            "conflict.resolve",
            high_conflict_count > 0,
            if high_conflict_count > 0 {
                "high or critical conflicts require control action"
            } else {
                "no high or critical conflict"
            },
            critical_conflict_count > 0,
            Some("runtime.conflict_arbiter"),
            high_conflict_count,
        ),
        readiness(
            "team.observe",
            runnable_teams > 0,
            if runnable_teams > 0 {
                "Team and Graph progress is materialized"
            } else {
                "no Team execution is currently visible"
            },
            false,
            Some("runtime.execution_graph"),
            runnable_teams,
        ),
    ];
    actions.sort_by(|left, right| left.action.cmp(&right.action));
    MissionControlReadiness {
        kind: "mission_control.control_readiness".to_string(),
        ready_count: actions.iter().filter(|action| action.available).count(),
        blocked_count: actions.iter().filter(|action| !action.available).count(),
        actions,
    }
}

fn readiness(
    action: &str,
    available: bool,
    reason: &str,
    requires_approval: bool,
    policy_marker: Option<&str>,
    target_count: usize,
) -> MissionControlActionReadiness {
    MissionControlActionReadiness {
        action: action.to_string(),
        available,
        reason: reason.to_string(),
        requires_approval,
        policy_marker: policy_marker.map(str::to_string),
        target_count,
    }
}

fn summary(
    sessions: &[MissionControlSessionNode],
    active_session_id: Option<String>,
    task_count: usize,
    team_count: usize,
    agent_count: usize,
    pending_approval_count: usize,
    recovery_required_count: usize,
    pending_organization_count: usize,
) -> MissionControlSummary {
    let count = |status: &str| {
        sessions
            .iter()
            .filter(|session| session.status == status || session.hydration == status)
            .count()
    };
    MissionControlSummary {
        session_count: sessions.len(),
        active_session_id,
        background_session_count: count("metadata_loaded"),
        paused_session_count: count("suspended"),
        closed_session_count: sessions
            .iter()
            .filter(|session| matches!(session.status.as_str(), "archived" | "deleted"))
            .count(),
        task_count,
        team_count,
        agent_count,
        pending_approval_count,
        recovery_required_count,
        pending_organization_count,
    }
}

fn task_nodes(
    services: &RuntimeServices,
    tasks: &[harness_contract::task::TaskAggregate],
) -> Vec<MissionControlTaskNode> {
    let mut tasks = tasks
        .iter()
        .cloned()
        .map(|task| {
            let turn_count = services
                .task_aggregate_service()
                .bindings_for_task(&task.task_id)
                .map(|bindings| bindings.len())
                .unwrap_or_default();
            MissionControlTaskNode {
                task_id: task.task_id,
                mission_id: task.mission_id,
                kind: task_kind_name(task.kind).to_string(),
                root_task_id: task.root_task_id,
                parent_task_id: task.parent_task_id,
                origin_session_id: task.origin_session_id,
                objective: task.objective,
                status: task.status.as_str().to_string(),
                revision: task.revision,
                current_phase_id: task.current_phase_id,
                phase_count: task.phases.len(),
                graph_count: task.graph_refs.len(),
                turn_count,
                assignment_source: task.mission_assigned_by,
                failure_count: task.failure_count,
                blocker_reason: task.blocker_reason,
                created_at_ms: task.created_at_ms,
                updated_at_ms: task.updated_at_ms,
            }
        })
        .collect::<Vec<_>>();
    tasks.sort_by_key(|task| (std::cmp::Reverse(task.updated_at_ms), task.task_id.clone()));
    tasks
}

fn task_kind_name(kind: harness_contract::task::TaskKind) -> &'static str {
    match kind {
        harness_contract::task::TaskKind::Root => "root",
        harness_contract::task::TaskKind::Delegated => "delegated",
    }
}

fn task_sessions(
    services: &RuntimeServices,
    task: &harness_contract::task::TaskAggregate,
) -> std::collections::BTreeSet<String> {
    let mut session_ids = std::collections::BTreeSet::from([task.origin_session_id.clone()]);
    session_ids.extend(
        services
            .task_aggregate_service()
            .bindings_for_task(&task.task_id)
            .unwrap_or_default()
            .into_iter()
            .map(|binding| binding.session_id),
    );
    session_ids
}

fn mission_session_ids(
    services: &RuntimeServices,
    tasks: &[harness_contract::task::TaskAggregate],
    mission_id: &str,
) -> std::collections::BTreeSet<String> {
    tasks
        .iter()
        .filter(|task| task.mission_id == mission_id)
        .flat_map(|task| task_sessions(services, task))
        .collect()
}

fn team_nodes(
    team_projection: &serde_json::Value,
    graph_bindings: &BTreeMap<String, (String, String)>,
) -> Vec<MissionControlTeamNode> {
    team_projection["teams"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|team| {
            let graph_id = value_string(team, "graph_id")?;
            let binding = graph_bindings.get(&graph_id);
            Some(MissionControlTeamNode {
                team_id: value_string(team, "team_id")?,
                graph_id,
                mission_id: binding.map(|(mission_id, _)| mission_id.clone()),
                task_id: binding.map(|(_, task_id)| task_id.clone()),
                session_id: value_string(team, "session_id"),
                status: value_string(team, "status"),
                agent_count: team["agents"].as_array().map_or(0, Vec::len),
                detail: team.clone(),
            })
        })
        .collect()
}

fn agent_nodes(agent_projection: &serde_json::Value) -> Vec<MissionControlAgentNode> {
    agent_projection["agents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|agent| {
            Some(MissionControlAgentNode {
                agent_id: value_string(agent, "agent_id")?,
                mission_id: agent
                    .pointer("/execution_identity/mission_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                task_id: value_string(agent, "task_id").or_else(|| {
                    agent
                        .pointer("/execution_identity/task_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                }),
                execution_id: value_string(agent, "graph_id"),
                team_id: agent
                    .pointer("/execution_identity/team_run_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                session_id: value_string(agent, "session_id"),
                status: value_string(agent, "state").or_else(|| value_string(agent, "status")),
                backend: value_string(agent, "backend"),
                detail: agent.clone(),
            })
        })
        .collect()
}

fn mission_graph(
    services: &RuntimeServices,
    mission_id: &str,
    aggregate: Option<&harness_contract::mission::MissionAggregate>,
    sessions: &[MissionControlSessionNode],
    tasks: &[MissionControlTaskNode],
    teams: &[MissionControlTeamNode],
    agents: &[MissionControlAgentNode],
    approvals: &[MissionControlApprovalNode],
    graph_bindings: &BTreeMap<String, (String, String)>,
    schedule_projection: &serde_json::Value,
    conflicts: &serde_json::Value,
    event_digest: &MissionControlEventDigest,
    include_execution_details: bool,
) -> MissionControlGraphProjection {
    let mut nodes = BTreeMap::<String, MissionControlGraphNode>::new();
    let mut edges = BTreeMap::<String, MissionControlGraphEdge>::new();
    let mission_node_id = format!("mission:{mission_id}");
    let mission_status = aggregate
        .map(|mission| mission.status.as_str())
        .unwrap_or("unknown");
    nodes.insert(
        mission_node_id.clone(),
        MissionControlGraphNode {
            node_id: mission_node_id.clone(),
            kind: "mission".to_string(),
            label: aggregate
                .map(|mission| mission.objective.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| mission_id.to_string()),
            status: mission_status.to_string(),
            mission_id: mission_id.to_string(),
            session_id: None,
            task_id: None,
            execution_id: None,
            team_id: None,
            agent_id: None,
        },
    );

    for session in sessions {
        let node_id = format!("session:{}", session.session_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "session".to_string(),
                label: session.title.clone(),
                status: session.status.clone(),
                mission_id: mission_id.to_string(),
                session_id: Some(session.session_id.clone()),
                task_id: None,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
        );
    }
    for task in tasks.iter().filter(|task| task.kind == "root") {
        let node_id = format!("task:{}", task.task_id);
        insert_graph_edge(&mut edges, "contains", &mission_node_id, &node_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "task".to_string(),
                label: task.objective.clone(),
                status: task.status.clone(),
                mission_id: mission_id.to_string(),
                session_id: Some(task.origin_session_id.clone()),
                task_id: Some(task.task_id.clone()),
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
        );
    }
    for session in sessions {
        let session_node_id = format!("session:{}", session.session_id);
        let root_task_ids = session
            .contributing_task_ids
            .iter()
            .filter_map(|task_id| {
                services
                    .task_aggregate_service()
                    .get(task_id)
                    .ok()
                    .flatten()
            })
            .map(|task| task.root_task_id)
            .collect::<std::collections::BTreeSet<_>>();
        for root_task_id in root_task_ids {
            let task_node_id = format!("task:{root_task_id}");
            if nodes.contains_key(&task_node_id) {
                insert_graph_edge(&mut edges, "contributes", &session_node_id, &task_node_id);
            }
        }
    }
    for (graph_id, (bound_mission_id, task_id)) in graph_bindings {
        if bound_mission_id != mission_id {
            continue;
        }
        let graph = include_execution_details
            .then(|| services.graph_state_store().projection(graph_id).ok())
            .flatten();
        let node_id = format!("execution:{graph_id}");
        let status = graph
            .as_ref()
            .map(mission_execution_status)
            .or_else(|| {
                teams
                    .iter()
                    .find(|team| team.graph_id == *graph_id)
                    .and_then(|team| team.status.clone())
            })
            .unwrap_or_else(|| "unknown".to_string());
        let parent_task_id = services
            .task_aggregate_service()
            .get(task_id)
            .ok()
            .flatten()
            .map(|task| task.root_task_id)
            .unwrap_or_else(|| task_id.clone());
        insert_graph_edge(
            &mut edges,
            "contains",
            &format!("task:{parent_task_id}"),
            &node_id,
        );
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "execution".to_string(),
                label: graph
                    .as_ref()
                    .map(|projection| projection.objective.clone())
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| graph_id.clone()),
                status,
                mission_id: mission_id.to_string(),
                session_id: None,
                task_id: Some(task_id.clone()),
                execution_id: Some(graph_id.clone()),
                team_id: None,
                agent_id: None,
            },
        );
        if let Some(graph) = graph {
            for node in &graph.nodes {
                let Some(reference) = node.result_ref.as_deref() else {
                    continue;
                };
                let artifact_node_id = mission_reference_node_id("artifact", reference);
                nodes
                    .entry(artifact_node_id.clone())
                    .or_insert_with(|| MissionControlGraphNode {
                        node_id: artifact_node_id.clone(),
                        kind: "artifact".to_string(),
                        label: node
                            .summary
                            .clone()
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or_else(|| "Execution artifact".to_string()),
                        status: "completed".to_string(),
                        mission_id: mission_id.to_string(),
                        session_id: None,
                        task_id: Some(task_id.clone()),
                        execution_id: Some(graph_id.clone()),
                        team_id: None,
                        agent_id: None,
                    });
                insert_graph_edge(
                    &mut edges,
                    "produced",
                    &format!("execution:{graph_id}"),
                    &artifact_node_id,
                );
            }
            if let Some(reference) = graph.terminal_result_ref.as_deref() {
                let outcome_node_id = mission_reference_node_id("outcome", reference);
                nodes
                    .entry(outcome_node_id.clone())
                    .or_insert_with(|| MissionControlGraphNode {
                        node_id: outcome_node_id.clone(),
                        kind: "outcome".to_string(),
                        label: "Execution outcome".to_string(),
                        status: "completed".to_string(),
                        mission_id: mission_id.to_string(),
                        session_id: None,
                        task_id: Some(task_id.clone()),
                        execution_id: Some(graph_id.clone()),
                        team_id: None,
                        agent_id: None,
                    });
                insert_graph_edge(
                    &mut edges,
                    "produced",
                    &format!("execution:{graph_id}"),
                    &outcome_node_id,
                );
            }
        }
    }
    for team in teams {
        let node_id = format!("team:{}", team.team_id);
        let parent_id = format!("execution:{}", team.graph_id);
        insert_graph_edge(&mut edges, "contains", &parent_id, &node_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "team".to_string(),
                label: team.team_id.clone(),
                status: team.status.clone().unwrap_or_else(|| "unknown".to_string()),
                mission_id: mission_id.to_string(),
                session_id: team.session_id.clone(),
                task_id: team.task_id.clone(),
                execution_id: Some(team.graph_id.clone()),
                team_id: Some(team.team_id.clone()),
                agent_id: None,
            },
        );
    }
    for agent in agents {
        let node_id = format!("agent:{}", agent.agent_id);
        let parent_id = agent
            .team_id
            .as_ref()
            .map(|team_id| format!("team:{team_id}"))
            .or_else(|| {
                agent
                    .execution_id
                    .as_ref()
                    .map(|execution_id| format!("execution:{execution_id}"))
            })
            .unwrap_or_else(|| mission_node_id.clone());
        insert_graph_edge(&mut edges, "delegated_to", &parent_id, &node_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "agent".to_string(),
                label: agent.agent_id.clone(),
                status: agent
                    .status
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                mission_id: mission_id.to_string(),
                session_id: agent.session_id.clone(),
                task_id: agent.task_id.clone(),
                execution_id: agent.execution_id.clone(),
                team_id: agent.team_id.clone(),
                agent_id: Some(agent.agent_id.clone()),
            },
        );
    }
    for approval in approvals.iter().filter(|_| include_execution_details) {
        let node_id = format!("approval:{}", approval.approval_id);
        insert_graph_edge(&mut edges, "contains", &mission_node_id, &node_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "approval".to_string(),
                label: approval
                    .action
                    .clone()
                    .unwrap_or_else(|| approval.approval_id.clone()),
                status: approval.status.clone(),
                mission_id: mission_id.to_string(),
                session_id: approval.source_session_id.clone(),
                task_id: None,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
        );
    }
    for schedule in schedule_projection["schedules"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|_| include_execution_details)
        .filter(|schedule| schedule["mission_id"].as_str() == Some(mission_id))
    {
        let Some(schedule_id) = schedule["schedule_id"].as_str() else {
            continue;
        };
        let node_id = format!("schedule:{schedule_id}");
        insert_graph_edge(&mut edges, "contains", &mission_node_id, &node_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "schedule".to_string(),
                label: schedule["objective"]
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("Mission schedule")
                    .to_string(),
                status: schedule["status"].as_str().unwrap_or("unknown").to_string(),
                mission_id: mission_id.to_string(),
                session_id: schedule["target_session_id"].as_str().map(str::to_owned),
                task_id: None,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
        );
    }
    let conflict_count = if include_execution_details {
        conflict_receipts(conflicts).count()
    } else {
        0
    };
    if conflict_count > 0 {
        let node_id = format!("conflict-summary:{mission_id}");
        insert_graph_edge(&mut edges, "contains", &mission_node_id, &node_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "conflict".to_string(),
                label: format!("{conflict_count} governed conflict(s)"),
                status: if critical_conflict_count(conflicts) > 0 {
                    "critical"
                } else if high_or_critical_conflict_count(conflicts) > 0 {
                    "attention"
                } else {
                    "observed"
                }
                .to_string(),
                mission_id: mission_id.to_string(),
                session_id: None,
                task_id: None,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
        );
    }
    if include_execution_details && !event_digest.recovery_required.is_empty() {
        let node_id = format!("recovery-summary:{mission_id}");
        insert_graph_edge(&mut edges, "contains", &mission_node_id, &node_id);
        nodes.insert(
            node_id.clone(),
            MissionControlGraphNode {
                node_id,
                kind: "recovery".to_string(),
                label: format!(
                    "{} recovery action(s) required",
                    event_digest.recovery_required.len()
                ),
                status: "attention".to_string(),
                mission_id: mission_id.to_string(),
                session_id: None,
                task_id: None,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
        );
    }

    MissionControlGraphProjection {
        schema_version: 1,
        mission_id: mission_id.to_string(),
        nodes: nodes.into_values().collect(),
        edges: edges.into_values().collect(),
    }
}

fn mission_reference_node_id(kind: &str, reference: &str) -> String {
    let digest = Sha256::digest(reference.as_bytes());
    format!("{kind}:sha256:{digest:x}")
}

fn mission_execution_status(
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
) -> String {
    use harness_contract::execution_graph::ExecutionNodeStatus;

    if graph.nodes.iter().all(|node| node.status.is_terminal()) {
        if graph.nodes.iter().any(|node| {
            matches!(
                node.status,
                ExecutionNodeStatus::Failed | ExecutionNodeStatus::Blocked
            )
        }) {
            "failed"
        } else if graph
            .nodes
            .iter()
            .any(|node| node.status == ExecutionNodeStatus::Cancelled)
        {
            "cancelled"
        } else {
            "completed"
        }
    } else if graph.nodes.iter().any(|node| {
        matches!(
            node.status,
            ExecutionNodeStatus::Running
                | ExecutionNodeStatus::WaitingInput
                | ExecutionNodeStatus::WaitingApproval
                | ExecutionNodeStatus::WaitingExternal
        )
    }) {
        "running"
    } else {
        "planned"
    }
    .to_string()
}

fn insert_graph_edge(
    edges: &mut BTreeMap<String, MissionControlGraphEdge>,
    kind: &str,
    from_node_id: &str,
    to_node_id: &str,
) {
    let edge_id = format!("{kind}:{from_node_id}:{to_node_id}");
    edges
        .entry(edge_id.clone())
        .or_insert_with(|| MissionControlGraphEdge {
            edge_id,
            kind: kind.to_string(),
            from_node_id: from_node_id.to_string(),
            to_node_id: to_node_id.to_string(),
        });
}

fn mission_summary(
    services: &RuntimeServices,
    aggregate: &harness_contract::mission::MissionAggregate,
    tasks: &[harness_contract::task::TaskAggregate],
    team_projection: &serde_json::Value,
    agent_projection: &serde_json::Value,
) -> MissionControlMissionSummary {
    let mission_tasks = tasks
        .iter()
        .filter(|task| task.mission_id == aggregate.mission_id)
        .collect::<Vec<_>>();
    let task_count = mission_tasks.len();
    let session_count = mission_tasks
        .iter()
        .flat_map(|task| task_sessions(services, task))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let graph_count = mission_tasks
        .iter()
        .flat_map(|task| task.graph_refs.iter().map(|reference| &reference.graph_id))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let team_count = team_projection["teams"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|team| {
            team.get("mission_id").and_then(serde_json::Value::as_str)
                == Some(aggregate.mission_id.as_str())
        })
        .count();
    let agent_count = agent_projection["agents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|agent| {
            agent
                .pointer("/execution_identity/mission_id")
                .and_then(serde_json::Value::as_str)
                == Some(aggregate.mission_id.as_str())
        })
        .count();
    MissionControlMissionSummary {
        mission_id: aggregate.mission_id.clone(),
        objective: aggregate.objective.clone(),
        status: aggregate.status.as_str().to_string(),
        revision: aggregate.revision,
        session_count,
        task_count,
        graph_count,
        team_count,
        agent_count,
        created_at_ms: aggregate.created_at_ms,
        updated_at_ms: aggregate.updated_at_ms,
    }
}

fn approval_nodes(approval_projection: &serde_json::Value) -> Vec<MissionControlApprovalNode> {
    approval_projection["requests"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|approval| {
            Some(MissionControlApprovalNode {
                approval_id: value_string(approval, "approval_id")?,
                status: value_string(approval, "status").unwrap_or_else(|| "unknown".to_string()),
                action: value_string(approval, "action"),
                source_session_id: approval
                    .get("source")
                    .and_then(|source| value_string(source, "session_id")),
                detail: approval.clone(),
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn event_digest_for_mission(
    limit: usize,
    services: &RuntimeServices,
    mission_id: &str,
    task_ids: &std::collections::BTreeSet<String>,
    graph_ids: &std::collections::BTreeSet<String>,
    team_ids: &std::collections::BTreeSet<String>,
    agent_ids: &std::collections::BTreeSet<String>,
    unambiguous_session_ids: &std::collections::BTreeSet<String>,
) -> MissionControlEventDigest {
    let mut events = services
        .event_store()
        .list_stream(&format!("mission:{mission_id}"))
        .unwrap_or_default();
    for graph_id in graph_ids {
        let mut after = None;
        loop {
            let page = services
                .event_store()
                .events_for_root_execution(graph_id, after, limit.max(1))
                .unwrap_or_default();
            if page.is_empty() {
                break;
            }
            let next = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            let page_len = page.len();
            events.extend(page);
            if page_len < limit.max(1) || next == after {
                break;
            }
            after = next;
        }
    }
    for session_id in unambiguous_session_ids {
        events.extend(
            services
                .event_store()
                .execution_events_for_session(session_id, None, limit.max(1))
                .unwrap_or_default(),
        );
    }
    events.retain(|event| {
        event.stream_id == format!("mission:{mission_id}")
            || event
                .refs
                .iter()
                .any(|reference| match reference.kind.as_str() {
                    "mission" => reference.id == mission_id,
                    "task" => task_ids.contains(&reference.id),
                    "execution" | "execution_graph" => graph_ids.contains(&reference.id),
                    "team" | "team_run" => team_ids.contains(&reference.id),
                    "agent" | "agent_instance" | "agent_run" => {
                        agent_ids.contains(reference.id.as_str())
                    }
                    "session" => unambiguous_session_ids.contains(reference.id.as_str()),
                    _ => false,
                })
    });
    events.sort_by_key(|event| std::cmp::Reverse((event.commit_cursor, event.transaction_index)));
    events.dedup_by(|left, right| left.event_id == right.event_id);
    events.truncate(limit);
    let mut scope_counts = BTreeMap::new();
    for event in &events {
        *scope_counts
            .entry(event.scope.as_str().to_string())
            .or_insert(0) += 1;
    }
    let lines = events
        .iter()
        .map(|event| MissionControlEventLine {
            event_id: event.event_id.clone(),
            stream_id: event.stream_id.clone(),
            cursor: event.commit_cursor,
            transaction_index: event.transaction_index,
            scope: event.scope.as_str().to_string(),
            kind: event.kind.clone(),
            status: event.status.clone(),
            actor: event.actor.clone(),
            created_at_ms: event.created_at_ms,
        })
        .collect::<Vec<_>>();
    MissionControlEventDigest {
        total_recent_events: lines.len(),
        scope_counts,
        latest_errors: lines
            .iter()
            .filter(|line| {
                line.kind.contains("failed")
                    || line.kind.contains("error")
                    || line.status.as_deref() == Some("failed")
            })
            .take(10)
            .cloned()
            .collect(),
        recovery_required: lines
            .iter()
            .filter(|line| {
                line.kind.contains("recovery_required")
                    || line.status.as_deref() == Some("recovery_required")
            })
            .take(10)
            .cloned()
            .collect(),
        latest: lines.into_iter().take(20).collect(),
    }
}

fn workspace_recovery_required_count(services: &RuntimeServices) -> usize {
    services
        .event_store()
        .stream_ids_for_scope(RuntimeEventScope::Recovery)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|stream_id| {
            services
                .event_store()
                .latest_for_stream(&stream_id)
                .ok()
                .flatten()
        })
        .filter(|event| {
            event.scope == RuntimeEventScope::Recovery
                && (event.kind.contains("recovery_required")
                    || event.status.as_deref() == Some("recovery_required"))
        })
        .count()
}

fn high_or_critical_conflict_count(conflicts: &serde_json::Value) -> usize {
    conflict_receipts(conflicts)
        .filter(|receipt| {
            matches!(
                lower_value(receipt, "severity").as_deref(),
                Some("high" | "critical")
            ) || matches!(
                lower_value(receipt, "decision").as_deref(),
                Some("pause_affected_scope" | "require_approval")
            )
        })
        .count()
}

fn critical_conflict_count(conflicts: &serde_json::Value) -> usize {
    conflict_receipts(conflicts)
        .filter(|receipt| {
            lower_value(receipt, "severity").as_deref() == Some("critical")
                || lower_value(receipt, "decision").as_deref() == Some("require_approval")
        })
        .count()
}

fn conflict_receipts(conflicts: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    conflicts
        .get("receipts")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
}

fn lower_value(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase)
}

fn value_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeEventInput;

    #[test]
    fn projection_uses_gateway_supplied_canonical_sessions() {
        let services = RuntimeServices::in_memory().expect("services");
        let mission = services
            .mission_runtime()
            .ensure_default_mission()
            .expect("mission");
        services.publish_session_execution_policy(
            "session-a",
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Supervised,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                ),
            ),
        );
        let task_spec = services
            .task_runtime_port()
            .bind_task_spec(
                "session-a",
                None,
                harness_contract::task::TaskSpec::new("Canonical session work"),
            )
            .expect("bind canonical session Task policy");
        services
            .task_aggregate_service()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: "task-session-a".to_string(),
                mission_id: mission.mission_id,
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: "session-a".to_string(),
                origin_turn_id: "turn-session-a".to_string(),
                root_task_id: "task-session-a".to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: task_spec,
                evidence_refs: Vec::new(),
            })
            .expect("task");
        let session = MissionControlSessionNode {
            session_id: "session-a".to_string(),
            title: "Canonical".to_string(),
            status: "active".to_string(),
            lifecycle: "attached".to_string(),
            hydration: "ready".to_string(),
            active: true,
            attachment_count: 1,
            team_count: 0,
            agent_count: 0,
            contributing_task_count: 0,
            contributing_task_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 2,
            last_error: None,
        };
        let projection = MissionControlRuntime::projection(
            &services,
            vec![session],
            Some("session-a".into()),
            None,
        );
        assert_eq!(projection.schema_version, MISSION_CONTROL_SCHEMA_VERSION);
        assert_eq!(projection.summary.session_count, 1);
        assert_eq!(
            projection.workspace.active_session_id.as_deref(),
            Some("session-a")
        );
        assert_eq!(
            projection.health["session_owner"],
            "gateway.session_service"
        );
        let team_create = projection
            .control_readiness
            .actions
            .iter()
            .find(|action| action.action == "team.create")
            .expect("Team creation readiness");
        assert!(team_create.available);
        assert_eq!(team_create.target_count, 1);
        assert!(!team_create.reason.is_empty());
        assert!(projection
            .mission_graph
            .nodes
            .iter()
            .any(|node| node.node_id == "session:session-a"));
    }

    #[test]
    fn mission_graph_materializes_schedule_conflict_and_recovery_summaries() {
        let services = RuntimeServices::in_memory().expect("services");
        let aggregate = services
            .mission_runtime()
            .ensure_default_mission()
            .expect("mission");
        let recovery = MissionControlEventLine {
            event_id: "recovery-1".to_string(),
            stream_id: "mission:default".to_string(),
            cursor: 1,
            transaction_index: 0,
            scope: "recovery".to_string(),
            kind: "execution.recovery_required".to_string(),
            status: Some("recovery_required".to_string()),
            actor: Some("runtime".to_string()),
            created_at_ms: 1,
        };
        let digest = MissionControlEventDigest {
            total_recent_events: 1,
            scope_counts: BTreeMap::from([("recovery".to_string(), 1)]),
            latest_errors: Vec::new(),
            recovery_required: vec![recovery.clone()],
            latest: vec![recovery],
        };
        let graph = mission_graph(
            &services,
            &aggregate.mission_id,
            Some(&aggregate),
            &[],
            &[],
            &[],
            &[],
            &[],
            &BTreeMap::new(),
            &serde_json::json!({
                "schedules": [{
                    "schedule_id": "schedule-1",
                    "mission_id": aggregate.mission_id,
                    "target_session_id": "session-a",
                    "objective": "Continue overnight",
                    "status": "scheduled"
                }]
            }),
            &serde_json::json!({
                "receipts": [{
                    "severity": "high",
                    "decision": "pause_affected_scope"
                }]
            }),
            &digest,
            true,
        );
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.node_id == "schedule:schedule-1"));
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.kind == "conflict" && node.status == "attention"));
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.kind == "recovery" && node.status == "attention"));
    }

    #[test]
    fn recovery_workspace_count_uses_each_streams_latest_state() {
        let services = RuntimeServices::in_memory().expect("services");
        let stream_id = "recovery:execution-1";
        services
            .event_store()
            .append(RuntimeEventInput {
                stream_id: stream_id.to_string(),
                scope: RuntimeEventScope::Recovery,
                kind: "execution.recovery_required".to_string(),
                status: Some("recovery_required".to_string()),
                actor: Some("runtime".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("append recovery-required event");
        assert_eq!(workspace_recovery_required_count(&services), 1);

        services
            .event_store()
            .append(RuntimeEventInput {
                stream_id: stream_id.to_string(),
                scope: RuntimeEventScope::Recovery,
                kind: "execution.recovery_resolved".to_string(),
                status: Some("resolved".to_string()),
                actor: Some("runtime".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("append recovery-resolved event");
        assert_eq!(workspace_recovery_required_count(&services), 0);
    }
}
