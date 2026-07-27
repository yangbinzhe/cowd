//! Mission Control runtime projection and command routing.
//!
//! Mission Control is the top-level AI Harness control surface. It aggregates
//! mission, session, team, agent, approval, relation, and event state
//! inside runtime so Gateway and surfaces do not reassemble core state ad hoc.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{MissionEvent, MissionProjection, MissionSessionSnapshot, RuntimeServices};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionWorkspace {
    pub workspace_id: String,
    pub title: String,
    pub active_session_id: Option<String>,
    pub session_count: usize,
    pub running_agent_count: usize,
    pub pending_approval_count: usize,
    pub recovery_required_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlSummary {
    pub session_count: usize,
    pub active_session_id: Option<String>,
    pub background_session_count: usize,
    pub paused_session_count: usize,
    pub closed_session_count: usize,
    pub team_count: usize,
    pub agent_count: usize,
    pub pending_approval_count: usize,
    pub recovery_required_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlSessionNode {
    pub session: MissionSessionSnapshot,
    pub last_event: Option<MissionEvent>,
    pub health: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlTeamNode {
    pub team_id: String,
    pub graph_id: String,
    pub session_id: Option<String>,
    pub status: Option<String>,
    pub agent_count: usize,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlAgentNode {
    pub agent_id: String,
    pub session_id: Option<String>,
    pub status: Option<String>,
    pub backend: Option<String>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlApprovalNode {
    pub approval_id: String,
    pub status: String,
    pub action: Option<String>,
    pub source_session_id: Option<String>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlEventDigest {
    pub total_recent_events: usize,
    pub scope_counts: BTreeMap<String, usize>,
    pub latest_errors: Vec<MissionControlEventLine>,
    pub recovery_required: Vec<MissionControlEventLine>,
    pub latest: Vec<MissionControlEventLine>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlEventLine {
    pub event_id: String,
    pub stream_id: String,
    pub scope: String,
    pub kind: String,
    pub status: Option<String>,
    pub actor: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionControlProjection {
    pub kind: String,
    pub workspace: MissionWorkspace,
    pub summary: MissionControlSummary,
    pub control_readiness: MissionControlControlReadiness,
    pub mission: MissionProjection,
    pub sessions: Vec<MissionControlSessionNode>,
    pub teams: Vec<MissionControlTeamNode>,
    pub agents: Vec<MissionControlAgentNode>,
    pub approvals: Vec<MissionControlApprovalNode>,
    pub relations: serde_json::Value,
    pub execution_graphs: serde_json::Value,
    pub conflicts: serde_json::Value,
    pub evidence: serde_json::Value,
    pub capabilities: serde_json::Value,
    pub event_digest: MissionControlEventDigest,
    pub health: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlControlReadiness {
    pub kind: String,
    pub ready_count: usize,
    pub blocked_count: usize,
    pub actions: Vec<MissionControlActionReadiness>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlActionReadiness {
    pub action: String,
    pub available: bool,
    pub reason: String,
    pub requires_approval: bool,
    pub policy_marker: Option<String>,
    pub target_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlCommand {
    pub target: MissionControlCommandTarget,
    pub action: MissionControlAction,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionControlCommandTarget {
    Mission,
    Session { session_id: String },
    Team { team_id: String },
    Agent { agent_id: String },
    Approval { approval_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionControlAction {
    SwitchSession,
    BackgroundSession,
    PauseSession,
    CloseSession,
    RouteToAgent,
    StartTeam,
    DecideApproval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlCommandReceipt {
    pub kind: String,
    pub command_id: String,
    pub target: MissionControlCommandTarget,
    pub action: MissionControlAction,
    pub actor: String,
    pub status: MissionControlCommandStatus,
    pub message: String,
    pub result: serde_json::Value,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionControlCommandStatus {
    Executed,
    Queued,
    Rejected,
    ApprovalRequired,
    Failed,
}

#[derive(Debug, Default)]
pub struct MissionControlRuntime;

impl MissionControlRuntime {
    #[must_use]
    pub fn projection(services: &RuntimeServices) -> MissionControlProjection {
        build_projection(services)
    }

    pub async fn execute(
        command: MissionControlCommand,
        services: &RuntimeServices,
    ) -> MissionControlCommandReceipt {
        let actor = command
            .actor
            .clone()
            .filter(|actor| !actor.trim().is_empty())
            .unwrap_or_else(|| "mission_control".to_string());
        let command_id = format!("mission-control-command-{}", uuid::Uuid::new_v4());
        let outcome = match translate_command(&command, &actor, &command_id) {
            Ok(command) => {
                let receipt = crate::execute_mission_command(services, command).await;
                let status = match receipt.status.as_str() {
                    "accepted" => MissionControlCommandStatus::Executed,
                    "rejected" => MissionControlCommandStatus::Rejected,
                    _ => MissionControlCommandStatus::Failed,
                };
                (
                    status,
                    receipt
                        .reason
                        .clone()
                        .unwrap_or_else(|| "canonical mission command accepted".to_string()),
                    serde_json::json!({ "receipt": receipt }),
                )
            }
            Err(error) => (
                MissionControlCommandStatus::Rejected,
                error,
                serde_json::Value::Null,
            ),
        };
        let (status, message, result) = outcome;

        MissionControlCommandReceipt {
            kind: "mission_control.command_receipt".to_string(),
            command_id,
            target: command.target,
            action: command.action,
            actor,
            status,
            message,
            result,
            evidence_refs: command.evidence_refs,
        }
    }
}

fn build_projection(services: &RuntimeServices) -> MissionControlProjection {
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

    let sessions = mission
        .sessions
        .iter()
        .map(|session| session_node(session, &mission))
        .collect::<Vec<_>>();
    let teams = team_nodes(&team_projection, &mission);
    let agents = agent_nodes(&agent_projection, &mission);
    let approvals = approval_nodes(&approval_projection);
    let execution_graphs = mission.execution_graph_projection.clone();
    let conflicts = services.conflict_resolver().projection();
    let evidence = services.mission_evidence().projection();
    let capabilities = mission.capability_projection.clone();
    let mission_health = mission.health_projection.clone();
    let summary = summary(
        &mission,
        sessions.len(),
        teams.len(),
        agents.len(),
        approvals
            .iter()
            .filter(|approval| approval.status == "pending")
            .count(),
        event_digest.recovery_required.len(),
    );
    let workspace = MissionWorkspace {
        workspace_id: services.workspace_key().to_string(),
        title: "Mission Control".to_string(),
        active_session_id: mission.active_session_id.clone(),
        session_count: summary.session_count,
        running_agent_count: agents
            .iter()
            .filter(|agent| agent.status.as_deref() == Some("running"))
            .count(),
        pending_approval_count: summary.pending_approval_count,
        recovery_required_count: summary.recovery_required_count,
    };
    let control_readiness = control_readiness(
        &summary,
        &sessions,
        &teams,
        &agents,
        &approvals,
        &mission.conflict_projection,
    );

    MissionControlProjection {
        kind: "mission_control.projection".to_string(),
        workspace,
        summary,
        control_readiness,
        mission,
        sessions,
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
) -> MissionControlControlReadiness {
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
                .map(|status| matches!(status, "running" | "ready" | "working"))
                .unwrap_or(false)
        })
        .count();
    let active_or_background_sessions = sessions
        .iter()
        .filter(|session| {
            matches!(
                session.session.status,
                crate::MissionSessionStatus::Active | crate::MissionSessionStatus::Background
            )
        })
        .count();
    let high_conflict_count = high_or_critical_conflict_count(conflicts);
    let critical_conflict_count = critical_conflict_count(conflicts);

    let mut actions = vec![
        readiness(
            "team.execution.tick",
            runnable_teams > 0,
            if runnable_teams > 0 {
                "teams are available for execution tick"
            } else {
                "no team runtime is available"
            },
            false,
            Some("runtime.execution_graph_runner"),
            runnable_teams,
        ),
        readiness(
            "agent.route",
            routable_agents > 0 && active_or_background_sessions > 0,
            if routable_agents > 0 && active_or_background_sessions > 0 {
                "running agents and dispatchable sessions are available"
            } else {
                "requires a running agent and active/background session"
            },
            false,
            Some("runtime.agent_task_mailbox"),
            routable_agents,
        ),
        readiness(
            "approval.decide",
            pending_approvals > 0,
            if pending_approvals > 0 {
                "pending approval requests can be decided"
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
    ];
    let ready_count = actions.iter().filter(|action| action.available).count();
    let blocked_count = actions.len().saturating_sub(ready_count);
    actions.sort_by(|left, right| left.action.cmp(&right.action));
    MissionControlControlReadiness {
        kind: "mission_control.control_readiness".to_string(),
        ready_count,
        blocked_count,
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
            matches!(
                lower_value(receipt, "severity").as_deref(),
                Some("critical")
            ) || matches!(
                lower_value(receipt, "decision").as_deref(),
                Some("require_approval")
            )
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

fn session_node(
    session: &MissionSessionSnapshot,
    mission: &MissionProjection,
) -> MissionControlSessionNode {
    let last_event = mission
        .events
        .iter()
        .rev()
        .find(|event| event.session_id.as_deref() == Some(session.session_id.as_str()))
        .cloned();
    let health = match session.status {
        crate::MissionSessionStatus::Active | crate::MissionSessionStatus::Background => "active",
        crate::MissionSessionStatus::Paused => "paused",
        crate::MissionSessionStatus::Closed => "closed",
    }
    .to_string();

    MissionControlSessionNode {
        session: session.clone(),
        last_event,
        health,
    }
}

fn summary(
    mission: &MissionProjection,
    session_count: usize,
    team_count: usize,
    agent_count: usize,
    pending_approval_count: usize,
    recovery_required_count: usize,
) -> MissionControlSummary {
    let background_session_count = mission
        .sessions
        .iter()
        .filter(|session| session.status == crate::MissionSessionStatus::Background)
        .count();
    let paused_session_count = mission
        .sessions
        .iter()
        .filter(|session| session.status == crate::MissionSessionStatus::Paused)
        .count();
    let closed_session_count = mission
        .sessions
        .iter()
        .filter(|session| session.status == crate::MissionSessionStatus::Closed)
        .count();
    MissionControlSummary {
        session_count,
        active_session_id: mission.active_session_id.clone(),
        background_session_count,
        paused_session_count,
        closed_session_count,
        team_count,
        agent_count,
        pending_approval_count,
        recovery_required_count,
    }
}

fn team_nodes(
    team_projection: &serde_json::Value,
    mission: &MissionProjection,
) -> Vec<MissionControlTeamNode> {
    team_projection["teams"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|team| {
            let team_id = value_string(team, "team_id")?;
            let graph_id = value_string(team, "graph_id")?;
            let session_id = value_string(team, "session_id").or_else(|| {
                mission
                    .sessions
                    .iter()
                    .find(|session| session.active_team_ids.contains(&team_id))
                    .map(|session| session.session_id.clone())
            });
            let agent_count = team["agents"].as_array().map_or(0, Vec::len);
            Some(MissionControlTeamNode {
                team_id,
                graph_id,
                session_id,
                status: value_string(team, "status"),
                agent_count,
                raw: team.clone(),
            })
        })
        .collect()
}

fn agent_nodes(
    agent_projection: &serde_json::Value,
    mission: &MissionProjection,
) -> Vec<MissionControlAgentNode> {
    agent_projection["agents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|agent| {
            let agent_id = value_string(agent, "agent_id")?;
            let session_id = value_string(agent, "session_id").or_else(|| {
                mission
                    .sessions
                    .iter()
                    .find(|session| session.active_agent_ids.contains(&agent_id))
                    .map(|session| session.session_id.clone())
            });
            Some(MissionControlAgentNode {
                agent_id,
                session_id,
                status: value_string(agent, "state").or_else(|| value_string(agent, "status")),
                backend: value_string(agent, "backend"),
                raw: agent.clone(),
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
            let approval_id = value_string(approval, "approval_id")?;
            Some(MissionControlApprovalNode {
                approval_id,
                status: value_string(approval, "status").unwrap_or_else(|| "unknown".to_string()),
                action: value_string(approval, "action"),
                source_session_id: approval
                    .get("source")
                    .and_then(|source| value_string(source, "session_id")),
                raw: approval.clone(),
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
            scope: event.scope.as_str().to_string(),
            kind: event.kind.clone(),
            status: event.status.clone(),
            actor: event.actor.clone(),
            created_at_ms: event.created_at_ms,
        })
        .collect::<Vec<_>>();
    let latest_errors = lines
        .iter()
        .filter(|line| {
            line.kind.contains("failed")
                || line.kind.contains("error")
                || line.status.as_deref() == Some("failed")
        })
        .take(10)
        .cloned()
        .collect();
    let recovery_required = lines
        .iter()
        .filter(|line| {
            line.kind.contains("recovery_required")
                || line.status.as_deref() == Some("recovery_required")
        })
        .take(10)
        .cloned()
        .collect();

    MissionControlEventDigest {
        total_recent_events: lines.len(),
        scope_counts,
        latest_errors,
        recovery_required,
        latest: lines.into_iter().take(20).collect(),
    }
}

fn translate_command(
    command: &MissionControlCommand,
    actor: &str,
    command_id: &str,
) -> Result<harness_contract::mission::MissionCommand, String> {
    use harness_contract::mission::{
        MissionCommandAction as Action, MissionCommandTarget as Target,
    };

    let expected_revision = command
        .payload
        .get("expected_revision")
        .and_then(serde_json::Value::as_u64);
    let (target, action, payload) = match (&command.target, command.action.clone()) {
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::SwitchSession,
        ) => (
            Target::Session {
                session_id: session_id.clone(),
            },
            Action::Activate,
            command.payload.clone(),
        ),
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::BackgroundSession,
        ) => (
            Target::Session {
                session_id: session_id.clone(),
            },
            Action::Background,
            command.payload.clone(),
        ),
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::PauseSession,
        ) => (
            Target::Session {
                session_id: session_id.clone(),
            },
            Action::Pause,
            command.payload.clone(),
        ),
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::CloseSession,
        ) => (
            Target::Session {
                session_id: session_id.clone(),
            },
            Action::Close,
            command.payload.clone(),
        ),
        (
            MissionControlCommandTarget::Approval { approval_id },
            MissionControlAction::DecideApproval,
        ) => {
            let approved = command
                .payload
                .get("approved")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| "payload.approved must be a boolean".to_string())?;
            (
                Target::Approval {
                    approval_id: approval_id.clone(),
                },
                if approved {
                    Action::Approve
                } else {
                    Action::Reject
                },
                command.payload.clone(),
            )
        }
        (MissionControlCommandTarget::Session { .. }, MissionControlAction::RouteToAgent) => {
            let agent_id = command
                .payload
                .get("agent_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "payload.agent_id must be a non-empty string".to_string())?;
            let content = command
                .payload
                .get("command")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "payload.command must be a non-empty string".to_string())?;
            (
                Target::Agent {
                    agent_id: agent_id.to_string(),
                },
                Action::Input,
                serde_json::json!({ "content": content }),
            )
        }
        _ => {
            return Err(format!(
                "unsupported mission control command target/action: {:?} {:?}",
                command.target, command.action
            ));
        }
    };
    Ok(harness_contract::mission::MissionCommand {
        command_id: command_id.to_string(),
        action,
        target,
        actor: actor.to_string(),
        expected_revision,
        correlation_id: format!("mission-control:{command_id}"),
        payload,
        evidence_refs: command
            .evidence_refs
            .iter()
            .map(|reference| {
                harness_contract::reality::EvidenceRef::new(
                    "mission_control",
                    reference,
                    harness_contract::reality::RealityBoundary::Observed,
                )
                .with_source("runtime.mission_control")
            })
            .collect(),
    })
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
    use crate::{
        ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, ConflictResolutionRequest,
        ConflictSeverity, ConflictSourceKind, StartMissionSessionRequest,
        SubmitGlobalApprovalRequest,
    };
    use harness_contract::core::TaskRisk;

    #[test]
    fn mission_control_projection_and_command_cover_runtime_state() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("mission-control-stage-i-a-{suffix}");
        let session_b = format!("mission-control-stage-i-b-{suffix}");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "mission control stage i".to_string(),
                session_id: Some(session_a.clone()),
            })
            .expect("first mission view session");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "mission control stage i b".to_string(),
                session_id: Some(session_b.clone()),
            })
            .expect("second mission view session");
        let projection = MissionControlRuntime::projection(&services);
        assert_eq!(projection.kind, "mission_control.projection");
        assert!(projection.summary.session_count >= 2);
        assert!(projection.summary.background_session_count >= 1);
        assert!(projection
            .sessions
            .iter()
            .any(|session| session.session.session_id == session_b));
        assert!(projection
            .sessions
            .iter()
            .any(|session| session.session.session_id == session_a));
        assert_eq!(projection.mission.team_projection["kind"], "runtime.teams");
        assert_eq!(
            projection.mission.agent_projection["kind"],
            "runtime.agents"
        );
        assert_eq!(
            projection.mission.approval_projection["kind"],
            "runtime.global_approvals"
        );
        assert_eq!(projection.relations["kind"], "runtime.session_relations");
        assert_eq!(
            projection.control_readiness.kind,
            "mission_control.control_readiness"
        );
        assert!(projection.event_digest.total_recent_events > 0);
    }

    #[test]
    fn mission_control_readiness_surfaces_conflict_and_approval_actions() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("mission-control-readiness-{suffix}");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "readiness".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("mission view session");
        let approval = services
            .approval_queue()
            .submit(SubmitGlobalApprovalRequest {
                source: ApprovalSource {
                    kind: ApprovalSourceKind::Session,
                    session_id: Some(session_id.clone()),
                    agent_id: None,
                    team_id: None,
                    mission_id: Some("mission-control".to_string()),
                    resource_ref: None,
                    review_ref: None,
                    application: None,
                },
                action: "dangerous-test-action".to_string(),
                summary: "needs approval".to_string(),
                risk: TaskRisk::Critical,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .expect("approval");
        let conflict = services
            .conflict_resolver()
            .resolve(ConflictResolutionRequest {
                source: ConflictSourceKind::SessionRelation,
                severity: ConflictSeverity::Critical,
                summary: "critical readiness conflict".to_string(),
                evidence_refs: vec![approval.approval_id.clone()],
                affected_scope: vec![format!("session:{session_id}")],
            });

        let projection = MissionControlRuntime::projection(&services);
        assert!(projection
            .control_readiness
            .actions
            .iter()
            .any(|action| action.action == "approval.decide"
                && action.available
                && action.requires_approval));
        assert!(projection
            .control_readiness
            .actions
            .iter()
            .any(|action| action.action == "conflict.resolve"
                && action.available
                && action.requires_approval
                && action.target_count > 0));
        let conflict_receipts = projection
            .conflicts
            .get("receipts")
            .and_then(serde_json::Value::as_array)
            .expect("conflict receipts");
        assert!(conflict_receipts.iter().any(|item| {
            item.get("conflict_id").and_then(serde_json::Value::as_str)
                == Some(conflict.conflict_id.as_str())
        }));
    }
}
