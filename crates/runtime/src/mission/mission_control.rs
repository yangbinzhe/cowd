//! Pure Mission Control projection reducer.
//!
//! Gateway owns the cached materialized view and supplies canonical Session
//! nodes. Runtime contributes only projections from Runtime-owned aggregates.

use std::collections::BTreeMap;

use harness_contract::mission::{
    MissionControlActionReadiness, MissionControlAgentNode, MissionControlApprovalNode,
    MissionControlEventDigest, MissionControlEventLine, MissionControlProjection,
    MissionControlReadiness, MissionControlSessionNode, MissionControlSummary,
    MissionControlTaskNode, MissionControlTeamNode, MissionWorkspaceProjection,
    MISSION_CONTROL_SCHEMA_VERSION,
};

use crate::RuntimeServices;

#[derive(Debug, Default)]
pub struct MissionControlRuntime;

impl MissionControlRuntime {
    #[must_use]
    pub fn projection(
        services: &RuntimeServices,
        sessions: Vec<MissionControlSessionNode>,
        active_session_id: Option<String>,
    ) -> MissionControlProjection {
        build_projection(services, sessions, active_session_id)
    }
}

fn build_projection(
    services: &RuntimeServices,
    sessions: Vec<MissionControlSessionNode>,
    active_session_id: Option<String>,
) -> MissionControlProjection {
    let mission = services.mission_runtime().projection(
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
    let approval_projection = services.approval_queue().projection();
    let relations = services.session_relations().projection();
    let event_digest = event_digest(50, services);
    let tasks = task_nodes(services);
    let teams = team_nodes(&team_projection);
    let agents = agent_nodes(&agent_projection);
    let approvals = approval_nodes(&approval_projection);
    let execution_graphs = mission.execution_graph_projection.clone();
    let conflicts = services.conflict_resolver().projection();
    let evidence = services.mission_evidence().projection();
    let capabilities = mission.capability_projection.clone();
    let mission_health = mission.health_projection.clone();
    let summary = summary(
        &sessions,
        active_session_id.clone(),
        tasks.len(),
        teams.len(),
        agents.len(),
        approvals
            .iter()
            .filter(|approval| approval.status == "pending")
            .count(),
        event_digest.recovery_required.len(),
    );
    let workspace = MissionWorkspaceProjection {
        workspace_id: services.workspace_key().to_string(),
        title: "Mission Control".to_string(),
        active_session_id,
        session_count: summary.session_count,
        running_agent_count: agents
            .iter()
            .filter(|agent| agent.status.as_deref() == Some("running"))
            .count(),
        pending_approval_count: summary.pending_approval_count,
        recovery_required_count: summary.recovery_required_count,
    };
    let control_readiness =
        control_readiness(&summary, &sessions, &teams, &agents, &approvals, &conflicts);

    MissionControlProjection {
        schema_version: MISSION_CONTROL_SCHEMA_VERSION,
        kind: "mission_control.projection".to_string(),
        workspace,
        summary,
        control_readiness,
        mission: serde_json::to_value(mission).unwrap_or_default(),
        sessions,
        tasks,
        teams,
        agents,
        approvals,
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
        }),
    }
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
    }
}

fn task_nodes(services: &RuntimeServices) -> Vec<MissionControlTaskNode> {
    let mut tasks = services
        .task_aggregate_service()
        .list()
        .unwrap_or_default()
        .into_iter()
        .map(|task| MissionControlTaskNode {
            task_id: task.task_id,
            mission_id: task.mission_id,
            source_session_id: task.source_session_id,
            objective: task.objective,
            status: task.status.as_str().to_string(),
            revision: task.revision,
            current_phase_id: task.current_phase_id,
            phase_count: task.phases.len(),
            graph_count: task.graph_refs.len(),
            failure_count: task.failure_count,
            blocker_reason: task.blocker_reason,
            created_at_ms: task.created_at_ms,
            updated_at_ms: task.updated_at_ms,
        })
        .collect::<Vec<_>>();
    tasks.sort_by_key(|task| (std::cmp::Reverse(task.updated_at_ms), task.task_id.clone()));
    tasks
}

fn team_nodes(team_projection: &serde_json::Value) -> Vec<MissionControlTeamNode> {
    team_projection["teams"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|team| {
            Some(MissionControlTeamNode {
                team_id: value_string(team, "team_id")?,
                graph_id: value_string(team, "graph_id")?,
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
                session_id: value_string(agent, "session_id"),
                status: value_string(agent, "state").or_else(|| value_string(agent, "status")),
                backend: value_string(agent, "backend"),
                detail: agent.clone(),
            })
        })
        .collect()
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

fn event_digest(limit: usize, services: &RuntimeServices) -> MissionControlEventDigest {
    let events = services.event_store().all_events(limit).unwrap_or_default();
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

    #[test]
    fn projection_uses_gateway_supplied_canonical_sessions() {
        let services = RuntimeServices::in_memory().expect("services");
        services
            .mission_runtime()
            .ensure_default_mission()
            .expect("mission");
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
            created_at_ms: 1,
            updated_at_ms: 2,
            last_error: None,
        };
        let projection =
            MissionControlRuntime::projection(&services, vec![session], Some("session-a".into()));
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
    }
}
