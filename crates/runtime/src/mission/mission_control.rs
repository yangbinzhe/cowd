//! Mission Control runtime projection and command routing.
//!
//! Mission Control is the top-level AI Harness control surface. It aggregates
//! mission, session, team, agent, approval, steward, relation, and event state
//! inside runtime so Gateway and surfaces do not reassemble core state ad hoc.

use std::collections::BTreeMap;

use harness_contract::core::TaskRisk;
use serde::{Deserialize, Serialize};

use crate::{
    global_agent_event_bus, global_agent_lifecycle_service, global_agent_task_mailbox,
    global_approval_queue, global_mission_evidence_bus, global_mission_runtime,
    global_runtime_event_store, global_session_relation_graph, global_steward_runtime_service,
    global_team_runtime_service, record_runtime_event, AgentExecutionCommandKind,
    AgentProgressEvent, AgentTask, AgentTaskStatus, ApprovalSource, ApprovalSourceKind,
    ApprovalTimeoutPolicy, AutonomyProfileId, GlobalApprovalDecision, MissionEvent,
    MissionEvidenceRef, MissionProjection, MissionSessionCommandSummary, MissionSessionSnapshot,
    RuntimeEventInput, RuntimeEventScope, StartMissionSessionRequest, StartStewardRuntimeRequest,
    SubmitGlobalApprovalRequest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionWorkspace {
    pub workspace_id: String,
    pub title: String,
    pub active_session_id: Option<String>,
    pub session_count: usize,
    pub running_agent_count: usize,
    pub pending_approval_count: usize,
    pub steward_count: usize,
    pub recovery_required_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlSummary {
    pub session_count: usize,
    pub active_session_id: Option<String>,
    pub background_session_count: usize,
    pub paused_session_count: usize,
    pub closed_session_count: usize,
    pub pending_session_command_count: usize,
    pub running_session_command_count: usize,
    pub failed_session_command_count: usize,
    pub team_count: usize,
    pub agent_count: usize,
    pub pending_approval_count: usize,
    pub steward_count: usize,
    pub running_steward_count: usize,
    pub recovery_required_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlSessionNode {
    pub session: MissionSessionSnapshot,
    pub command_summary: MissionSessionCommandSummary,
    pub routed_command_count: usize,
    pub last_event: Option<MissionEvent>,
    pub health: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlTeamNode {
    pub team_id: String,
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
pub struct MissionControlStewardNode {
    pub steward_id: String,
    pub mission_id: String,
    pub root_session_id: Option<String>,
    pub status: String,
    pub pending_approval_count: usize,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub stewards: Vec<MissionControlStewardNode>,
    pub relations: serde_json::Value,
    pub workgraphs: serde_json::Value,
    pub conflicts: serde_json::Value,
    pub evidence: serde_json::Value,
    pub capabilities: serde_json::Value,
    pub steward_scheduler: serde_json::Value,
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
    Steward { steward_id: String },
    Approval { approval_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionControlAction {
    StartSession,
    SwitchSession,
    BackgroundSession,
    PauseSession,
    CloseSession,
    RouteToSession,
    RouteToAgent,
    StartTeam,
    StartSteward,
    PauseSteward,
    ResumeSteward,
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
    pub fn projection() -> MissionControlProjection {
        build_projection()
    }

    pub fn execute(command: MissionControlCommand) -> MissionControlCommandReceipt {
        let actor = command
            .actor
            .clone()
            .filter(|actor| !actor.trim().is_empty())
            .unwrap_or_else(|| "mission_control".to_string());
        let command_id = format!("mission-control-command-{}", uuid::Uuid::new_v4());
        let outcome = execute_command(&command);
        let (status, message, result) = match outcome {
            Ok((status, message, result)) => (status, message, result),
            Err(error) => (
                MissionControlCommandStatus::Failed,
                error,
                serde_json::Value::Null,
            ),
        };
        let receipt = MissionControlCommandReceipt {
            kind: "mission_control.command_receipt".to_string(),
            command_id,
            target: command.target,
            action: command.action,
            actor,
            status,
            message,
            result,
            evidence_refs: command.evidence_refs,
        };
        record_command_event(&receipt);
        receipt
    }
}

fn build_projection() -> MissionControlProjection {
    let mission = global_mission_runtime().projection();
    let team_projection = global_team_runtime_service().projection();
    let agent_projection = global_agent_lifecycle_service().projection();
    let approval_projection = global_approval_queue().projection();
    let steward_projection = global_steward_runtime_service().projection();
    let relations = global_session_relation_graph().projection();
    let event_digest = event_digest(50);

    let sessions = mission
        .sessions
        .iter()
        .map(|session| session_node(session, &mission))
        .collect::<Vec<_>>();
    let teams = team_nodes(&team_projection, &mission);
    let agents = agent_nodes(&agent_projection, &mission);
    let approvals = approval_nodes(&approval_projection);
    let stewards = steward_nodes(&serde_json::to_value(&steward_projection).unwrap_or_default());
    let workgraphs = mission.workgraph_projection.clone();
    let conflicts = mission.conflict_projection.clone();
    let evidence = mission.evidence_projection.clone();
    let capabilities = mission.capability_projection.clone();
    let steward_scheduler = mission.steward_projection.clone();
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
        &stewards,
        event_digest.recovery_required.len(),
    );
    let workspace = MissionWorkspace {
        workspace_id: "mission-control:default".to_string(),
        title: "Mission Control".to_string(),
        active_session_id: mission.active_session_id.clone(),
        session_count: summary.session_count,
        running_agent_count: agents
            .iter()
            .filter(|agent| agent.status.as_deref() == Some("running"))
            .count(),
        pending_approval_count: summary.pending_approval_count,
        steward_count: summary.steward_count,
        recovery_required_count: summary.recovery_required_count,
    };
    let control_readiness = control_readiness(
        &summary,
        &sessions,
        &teams,
        &agents,
        &approvals,
        &stewards,
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
        stewards,
        relations,
        workgraphs,
        conflicts,
        evidence,
        capabilities,
        steward_scheduler,
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
    stewards: &[MissionControlStewardNode],
    conflicts: &serde_json::Value,
) -> MissionControlControlReadiness {
    let pending_approvals = approvals
        .iter()
        .filter(|approval| approval.status == "pending")
        .count();
    let runnable_commands = summary.pending_session_command_count;
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
            "session.dispatch",
            runnable_commands > 0,
            if runnable_commands > 0 {
                "pending session commands can be dispatched"
            } else {
                "no pending session command"
            },
            false,
            Some("runtime.session_execution_plane"),
            runnable_commands,
        ),
        readiness(
            "session.dispatch_runtime_turn",
            runnable_commands > 0,
            if runnable_commands > 0 {
                "pending commands can request runtime turn execution through Gateway adapter"
            } else {
                "no pending command to start as runtime turn"
            },
            false,
            Some("runtime.session_execution_plane.start_runtime_turn"),
            runnable_commands,
        ),
        readiness(
            "team.execution.tick",
            runnable_teams > 0,
            if runnable_teams > 0 {
                "teams are available for execution tick"
            } else {
                "no team runtime is available"
            },
            false,
            Some("runtime.team_execution_loop"),
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
            Some("runtime.global_approval_queue"),
            pending_approvals,
        ),
        readiness(
            "steward.scheduler.tick",
            !stewards.is_empty() || runnable_commands > 0 || runnable_teams > 0,
            if !stewards.is_empty() || runnable_commands > 0 || runnable_teams > 0 {
                "scheduler can tick stewards, session commands, and teams"
            } else {
                "no steward, pending command, or team to tick"
            },
            false,
            Some("runtime.steward_scheduler"),
            stewards.len() + runnable_commands + runnable_teams,
        ),
        readiness(
            "recovery.plan",
            summary.recovery_required_count > 0 || summary.running_session_command_count > 0,
            if summary.recovery_required_count > 0 || summary.running_session_command_count > 0 {
                "runtime events contain recoverable or running work"
            } else {
                "no recovery candidate currently visible"
            },
            false,
            Some("runtime.recovery_planner"),
            summary.recovery_required_count + summary.running_session_command_count,
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
    let mut summary = MissionSessionCommandSummary {
        pending: 0,
        claimed: 0,
        running: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
        interrupted: 0,
    };
    for command in mission
        .session_commands
        .iter()
        .filter(|command| command.target_session_id == session.session_id)
    {
        match command.status {
            crate::MissionSessionCommandStatus::Pending => summary.pending += 1,
            crate::MissionSessionCommandStatus::Claimed => summary.claimed += 1,
            crate::MissionSessionCommandStatus::Running => summary.running += 1,
            crate::MissionSessionCommandStatus::Completed => summary.completed += 1,
            crate::MissionSessionCommandStatus::Failed => summary.failed += 1,
            crate::MissionSessionCommandStatus::Cancelled => summary.cancelled += 1,
            crate::MissionSessionCommandStatus::Interrupted => summary.interrupted += 1,
        }
    }
    let routed_command_count = mission
        .routed_commands
        .iter()
        .filter(|command| command.target_session_id == session.session_id)
        .count();
    let last_event = mission
        .events
        .iter()
        .rev()
        .find(|event| event.session_id.as_deref() == Some(session.session_id.as_str()))
        .cloned();
    let health = if summary.failed > 0 {
        "degraded"
    } else if summary.running > 0 || summary.pending > 0 {
        "active"
    } else {
        "ready"
    }
    .to_string();

    MissionControlSessionNode {
        session: session.clone(),
        command_summary: summary,
        routed_command_count,
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
    stewards: &[MissionControlStewardNode],
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
        pending_session_command_count: mission.session_command_summary.pending,
        running_session_command_count: mission.session_command_summary.running,
        failed_session_command_count: mission.session_command_summary.failed,
        team_count,
        agent_count,
        pending_approval_count,
        steward_count: stewards.len(),
        running_steward_count: stewards
            .iter()
            .filter(|steward| steward.status == "running")
            .count(),
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

fn steward_nodes(steward_projection: &serde_json::Value) -> Vec<MissionControlStewardNode> {
    steward_projection["sessions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|steward| {
            let steward_id = value_string(steward, "steward_id")?;
            Some(MissionControlStewardNode {
                steward_id,
                mission_id: value_string(steward, "mission_id").unwrap_or_default(),
                root_session_id: value_string(steward, "root_session_id"),
                status: value_string(steward, "status").unwrap_or_else(|| "unknown".to_string()),
                pending_approval_count: steward["pending_approval_ids"]
                    .as_array()
                    .map_or(0, Vec::len),
                raw: steward.clone(),
            })
        })
        .collect()
}

fn event_digest(limit: usize) -> MissionControlEventDigest {
    let events = global_runtime_event_store()
        .all_events(limit)
        .unwrap_or_default();
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

fn execute_command(
    command: &MissionControlCommand,
) -> Result<(MissionControlCommandStatus, String, serde_json::Value), String> {
    match (&command.target, &command.action) {
        (MissionControlCommandTarget::Mission, MissionControlAction::StartSession) => {
            let title = payload_string(&command.payload, "title")?;
            let session = global_mission_runtime().start_session(StartMissionSessionRequest {
                title,
                session_id: payload_optional_string(&command.payload, "session_id"),
            })?;
            Ok((
                MissionControlCommandStatus::Executed,
                "mission session started".to_string(),
                serde_json::json!({ "session": session }),
            ))
        }
        (MissionControlCommandTarget::Mission, MissionControlAction::StartSteward) => {
            let mission_id = payload_string(&command.payload, "mission_id")?;
            let objective = payload_string(&command.payload, "objective")?;
            let profile_id = payload_optional_string(&command.payload, "profile_id")
                .and_then(|profile| serde_json::from_value(serde_json::Value::String(profile)).ok())
                .unwrap_or(AutonomyProfileId::Stewarded);
            let steward = global_steward_runtime_service().start(StartStewardRuntimeRequest {
                mission_id,
                root_session_id: payload_optional_string(&command.payload, "root_session_id"),
                profile_id,
                objective,
            })?;
            Ok((
                MissionControlCommandStatus::Executed,
                "steward started".to_string(),
                serde_json::json!({ "steward": steward }),
            ))
        }
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::SwitchSession,
        ) => command_receipt_result(global_mission_runtime().switch_session(session_id)),
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::BackgroundSession,
        ) => command_receipt_result(global_mission_runtime().background_session(session_id)),
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::PauseSession,
        ) => command_receipt_result(global_mission_runtime().pause_session(session_id)),
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::CloseSession,
        ) => command_receipt_result(global_mission_runtime().close_session(session_id)),
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::RouteToSession,
        ) => {
            let target_session_id = payload_string(&command.payload, "target_session_id")?;
            let command_text = payload_string(&command.payload, "command")?;
            let routed = global_mission_runtime().enqueue_session_command(
                session_id,
                &target_session_id,
                command_text,
            )?;
            Ok((
                MissionControlCommandStatus::Queued,
                "command routed to session inbox".to_string(),
                serde_json::json!({ "command": routed }),
            ))
        }
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::StartSteward,
        ) => {
            let mission_id = payload_optional_string(&command.payload, "mission_id")
                .unwrap_or_else(|| "mission-control".to_string());
            let objective = payload_string(&command.payload, "objective")?;
            let steward = global_steward_runtime_service().start(StartStewardRuntimeRequest {
                mission_id,
                root_session_id: Some(session_id.clone()),
                profile_id: AutonomyProfileId::Stewarded,
                objective,
            })?;
            Ok((
                MissionControlCommandStatus::Executed,
                "steward started for session".to_string(),
                serde_json::json!({ "steward": steward }),
            ))
        }
        (
            MissionControlCommandTarget::Steward { steward_id },
            MissionControlAction::PauseSteward,
        ) => {
            let steward = global_steward_runtime_service().pause(steward_id)?;
            Ok((
                MissionControlCommandStatus::Executed,
                "steward paused".to_string(),
                serde_json::json!({ "steward": steward }),
            ))
        }
        (
            MissionControlCommandTarget::Steward { steward_id },
            MissionControlAction::ResumeSteward,
        ) => {
            let steward = global_steward_runtime_service().resume(steward_id)?;
            Ok((
                MissionControlCommandStatus::Executed,
                "steward resumed".to_string(),
                serde_json::json!({ "steward": steward }),
            ))
        }
        (
            MissionControlCommandTarget::Approval { approval_id },
            MissionControlAction::DecideApproval,
        ) => {
            let approved = command
                .payload
                .get("approved")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| "payload.approved must be a boolean".to_string())?;
            let decided_by = payload_optional_string(&command.payload, "decided_by")
                .or_else(|| command.actor.clone())
                .unwrap_or_else(|| "mission_control".to_string());
            let reason = payload_optional_string(&command.payload, "reason").unwrap_or_default();
            let receipt = global_approval_queue().decide(GlobalApprovalDecision {
                approval_id: approval_id.clone(),
                approved,
                decided_by,
                reason,
            })?;
            Ok((
                MissionControlCommandStatus::Executed,
                "approval decided".to_string(),
                serde_json::json!({ "receipt": receipt }),
            ))
        }
        (
            MissionControlCommandTarget::Session { session_id },
            MissionControlAction::RouteToAgent,
        ) => route_to_agent(session_id, command),
        _ => Err(format!(
            "unsupported mission control command target/action: {:?} {:?}",
            command.target, command.action
        )),
    }
}

fn command_receipt_result(
    receipt: Result<crate::MissionCommandReceipt, String>,
) -> Result<(MissionControlCommandStatus, String, serde_json::Value), String> {
    let receipt = receipt?;
    let status = if receipt.status == "accepted" {
        MissionControlCommandStatus::Executed
    } else {
        MissionControlCommandStatus::Rejected
    };
    Ok((
        status,
        receipt.message.clone(),
        serde_json::json!({ "receipt": receipt }),
    ))
}

fn route_to_agent(
    session_id: &str,
    command: &MissionControlCommand,
) -> Result<(MissionControlCommandStatus, String, serde_json::Value), String> {
    let agent_id = payload_string(&command.payload, "agent_id")?;
    let command_text = payload_string(&command.payload, "command")?;
    let require_approval = command
        .payload
        .get("require_approval")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let team_id = payload_optional_string(&command.payload, "team_id")
        .unwrap_or_else(|| format!("session:{session_id}:direct-agent-routes"));
    let role_id = payload_optional_string(&command.payload, "role_id")
        .unwrap_or_else(|| "direct_route".to_string());
    let objective = payload_optional_string(&command.payload, "objective")
        .unwrap_or_else(|| command_text.clone());
    let expected_output = payload_optional_string(&command.payload, "expected_output")
        .unwrap_or_else(|| {
            "Handle the routed Mission Control input and report progress".to_string()
        });
    let context_refs = payload_string_array(&command.payload, "context_refs")
        .filter(|refs| !refs.is_empty())
        .unwrap_or_else(|| vec![format!("session:{session_id}")]);

    if require_approval {
        let approval = global_approval_queue().submit(SubmitGlobalApprovalRequest {
            source: ApprovalSource {
                kind: ApprovalSourceKind::Session,
                session_id: Some(session_id.to_string()),
                agent_id: Some(agent_id),
                team_id: Some(team_id),
                mission_id: Some("mission-control".to_string()),
            },
            action: "route_to_agent".to_string(),
            summary: command_text,
            risk: TaskRisk::High,
            evidence_refs: command.evidence_refs.clone(),
            timeout_policy: ApprovalTimeoutPolicy::Pending,
        })?;
        return Ok((
            MissionControlCommandStatus::ApprovalRequired,
            "agent route requires approval by request policy".to_string(),
            serde_json::json!({ "approval": approval }),
        ));
    }

    if global_agent_lifecycle_service().get(&agent_id).is_none() {
        return Err(format!("agent not found: {agent_id}"));
    }

    let task = AgentTask {
        task_id: format!("agent-task-{}", uuid::Uuid::new_v4()),
        team_id: team_id.clone(),
        session_id: session_id.to_string(),
        role_id: role_id.clone(),
        agent_id: Some(agent_id.clone()),
        objective,
        expected_output,
        context_refs,
        evidence_refs: command.evidence_refs.clone(),
        status: AgentTaskStatus::Pending,
        outcome: None,
        created_at_ms: 0,
        updated_at_ms: 0,
    };
    let task_receipt = global_agent_task_mailbox().assign(task.clone());
    let progress = global_agent_event_bus().push(AgentProgressEvent {
        event_id: String::new(),
        team_id: team_id.clone(),
        session_id: session_id.to_string(),
        agent_id: Some(agent_id.clone()),
        role_id: role_id.clone(),
        task_id: Some(task_receipt.task_id.clone()),
        event_type: "agent.task.routed".to_string(),
        message: "Mission Control routed input to agent".to_string(),
        evidence_refs: command.evidence_refs.clone(),
        created_at_ms: 0,
    });
    let evidence = global_mission_evidence_bus().record(MissionEvidenceRef {
        evidence_id: String::new(),
        mission_id: Some("mission-control".to_string()),
        session_id: session_id.to_string(),
        team_id: Some(team_id.clone()),
        agent_id: Some(agent_id.clone()),
        kind: "agent_route".to_string(),
        summary: format!("Mission Control routed command to agent {agent_id}"),
        source_ref: Some(task_receipt.task_id.clone()),
        created_at_ms: 0,
    });

    let mut evidence_refs = command.evidence_refs.clone();
    evidence_refs.push(evidence.evidence_id.clone());
    let payload = serde_json::json!({
        "source": "mission_control",
        "session_id": session_id,
        "team_id": team_id,
        "role_id": role_id,
        "task_id": task_receipt.task_id,
        "command": command_text,
        "objective": task.objective,
        "expected_output": task.expected_output,
        "context_refs": task.context_refs,
        "evidence_refs": evidence_refs,
    });
    let delivery = global_agent_lifecycle_service().command(
        &agent_id,
        AgentExecutionCommandKind::Input,
        Some(payload),
    );
    let (status, message, delivery_json) = match delivery {
        Ok(receipt) => {
            let _ = global_agent_task_mailbox().set_status(
                &task_receipt.task_id,
                AgentTaskStatus::Running,
                "mission-control route delivered to agent",
            );
            (
                MissionControlCommandStatus::Executed,
                "agent route delivered".to_string(),
                serde_json::json!({ "receipt": receipt }),
            )
        }
        Err(error) => (
            MissionControlCommandStatus::Queued,
            format!("agent route queued; delivery pending: {error}"),
            serde_json::json!({ "error": error }),
        ),
    };

    Ok((
        status,
        message,
        serde_json::json!({
            "agent_id": agent_id,
            "task": task_receipt,
            "progress": progress,
            "evidence": evidence,
            "delivery": delivery_json,
        }),
    ))
}

fn record_command_event(receipt: &MissionControlCommandReceipt) {
    let _ = record_runtime_event(RuntimeEventInput {
        stream_id: "mission-control:default".to_string(),
        scope: RuntimeEventScope::Mission,
        kind: "mission_control.command".to_string(),
        status: Some(format!("{:?}", receipt.status).to_ascii_lowercase()),
        actor: Some(receipt.actor.clone()),
        refs: receipt
            .evidence_refs
            .iter()
            .map(|id| crate::RuntimeEventRef {
                kind: "evidence".to_string(),
                id: id.clone(),
            })
            .collect(),
        payload: serde_json::json!({
            "command_id": receipt.command_id,
            "target": receipt.target,
            "action": receipt.action,
            "message": receipt.message,
        }),
    });
}

fn value_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
}

fn payload_string(payload: &serde_json::Value, key: &str) -> Result<String, String> {
    payload_optional_string(payload, key)
        .ok_or_else(|| format!("payload.{key} must be a non-empty string"))
}

fn payload_optional_string(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
}

fn payload_string_array(payload: &serde_json::Value, key: &str) -> Option<Vec<String>> {
    payload.get(key).and_then(|value| {
        value.as_array().map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter(|item| !item.trim().is_empty())
                .map(ToString::to_string)
                .collect()
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        global_conflict_arbiter, AgentExecutionBackendKind, AgentSnapshot, CancellationToken,
        ConflictResolutionRequest, ConflictSeverity, ConflictSourceKind, DEFAULT_AGENT_MODEL,
    };

    #[test]
    fn mission_control_projection_and_command_cover_runtime_state() {
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("mission-control-stage-i-a-{suffix}");
        let session_b = format!("mission-control-stage-i-b-{suffix}");
        let first = MissionControlRuntime::execute(MissionControlCommand {
            target: MissionControlCommandTarget::Mission,
            action: MissionControlAction::StartSession,
            actor: Some("test-human".to_string()),
            payload: serde_json::json!({
                "title": "mission control stage i",
                "session_id": session_a,
            }),
            evidence_refs: vec!["plan:stage-i".to_string()],
        });
        assert_eq!(first.status, MissionControlCommandStatus::Executed);
        let second = MissionControlRuntime::execute(MissionControlCommand {
            target: MissionControlCommandTarget::Mission,
            action: MissionControlAction::StartSession,
            actor: Some("test-human".to_string()),
            payload: serde_json::json!({
                "title": "mission control stage i b",
                "session_id": session_b,
            }),
            evidence_refs: Vec::new(),
        });
        assert_eq!(second.status, MissionControlCommandStatus::Executed);
        let routed = MissionControlRuntime::execute(MissionControlCommand {
            target: MissionControlCommandTarget::Session {
                session_id: session_a.clone(),
            },
            action: MissionControlAction::RouteToSession,
            actor: Some("test-human".to_string()),
            payload: serde_json::json!({
                "target_session_id": session_b.clone(),
                "command": "review routed command",
            }),
            evidence_refs: Vec::new(),
        });
        assert_eq!(routed.status, MissionControlCommandStatus::Queued);
        let steward = MissionControlRuntime::execute(MissionControlCommand {
            target: MissionControlCommandTarget::Session {
                session_id: session_a.clone(),
            },
            action: MissionControlAction::StartSteward,
            actor: Some("test-human".to_string()),
            payload: serde_json::json!({
                "objective": "supervise stage i",
            }),
            evidence_refs: Vec::new(),
        });
        assert_eq!(steward.status, MissionControlCommandStatus::Executed);

        let projection = MissionControlRuntime::projection();
        assert_eq!(projection.kind, "mission_control.projection");
        assert!(projection.summary.session_count >= 2);
        assert!(projection.summary.background_session_count >= 1);
        assert!(projection.sessions.iter().any(|session| {
            session.session.session_id == session_b && session.routed_command_count >= 1
        }));
        assert!(projection.summary.steward_count >= 1);
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
        assert!(projection
            .control_readiness
            .actions
            .iter()
            .any(|action| action.action == "session.dispatch" && action.available));
        assert!(projection.event_digest.total_recent_events > 0);
    }

    #[test]
    fn mission_control_route_to_agent_assigns_task_and_evidence() {
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("mission-control-route-session-{suffix}");
        let agent_id = format!("mission-control-route-agent-{suffix}");
        let session = MissionControlRuntime::execute(MissionControlCommand {
            target: MissionControlCommandTarget::Mission,
            action: MissionControlAction::StartSession,
            actor: Some("test-human".to_string()),
            payload: serde_json::json!({
                "title": "route to agent",
                "session_id": session_id,
            }),
            evidence_refs: Vec::new(),
        });
        assert_eq!(session.status, MissionControlCommandStatus::Executed);
        global_agent_lifecycle_service().register_started(
            AgentSnapshot {
                agent_id: agent_id.clone(),
                name: "route-agent".to_string(),
                description: "route test".to_string(),
                subagent_type: Some("worker".to_string()),
                model: Some(DEFAULT_AGENT_MODEL.to_string()),
                status: "running".to_string(),
                backend: AgentExecutionBackendKind::InProcess,
                output_file: String::new(),
                manifest_file: String::new(),
                created_at: "1".to_string(),
                started_at: Some("1".to_string()),
                completed_at: None,
                lane_events: Vec::new(),
                current_blocker: None,
                derived_state: "working".to_string(),
                error: None,
            },
            CancellationToken::new(),
        );

        let routed = MissionControlRuntime::execute(MissionControlCommand {
            target: MissionControlCommandTarget::Session {
                session_id: session_id.clone(),
            },
            action: MissionControlAction::RouteToAgent,
            actor: Some("test-human".to_string()),
            payload: serde_json::json!({
                "agent_id": agent_id,
                "command": "inspect routed work",
                "team_id": "route-team",
                "role_id": "reviewer",
            }),
            evidence_refs: vec!["manual:evidence".to_string()],
        });

        assert_eq!(routed.status, MissionControlCommandStatus::Queued);
        assert!(routed.message.contains("queued"));
        assert!(routed.result["task"]["task_id"].as_str().is_some());
        assert_eq!(
            routed.result["progress"]["event_type"].as_str(),
            Some("agent.task.routed")
        );
        assert_eq!(
            routed.result["evidence"]["kind"].as_str(),
            Some("agent_route")
        );
        assert!(!global_agent_task_mailbox()
            .list_for_team("route-team")
            .is_empty());
        assert!(!global_mission_evidence_bus()
            .list_for_team("route-team")
            .is_empty());
    }

    #[test]
    fn mission_control_readiness_surfaces_conflict_and_approval_actions() {
        let suffix = uuid::Uuid::new_v4();
        let session_id = format!("mission-control-readiness-{suffix}");
        MissionControlRuntime::execute(MissionControlCommand {
            target: MissionControlCommandTarget::Mission,
            action: MissionControlAction::StartSession,
            actor: Some("test-human".to_string()),
            payload: serde_json::json!({
                "title": "readiness",
                "session_id": session_id,
            }),
            evidence_refs: Vec::new(),
        });
        let approval = global_approval_queue()
            .submit(SubmitGlobalApprovalRequest {
                source: ApprovalSource {
                    kind: ApprovalSourceKind::Session,
                    session_id: Some(session_id.clone()),
                    agent_id: None,
                    team_id: None,
                    mission_id: Some("mission-control".to_string()),
                },
                action: "dangerous-test-action".to_string(),
                summary: "needs approval".to_string(),
                risk: TaskRisk::Critical,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::Pending,
            })
            .expect("approval");
        let conflict = global_conflict_arbiter().resolve(ConflictResolutionRequest {
            source: ConflictSourceKind::SessionRelation,
            severity: ConflictSeverity::Critical,
            summary: "critical readiness conflict".to_string(),
            evidence_refs: vec![approval.approval_id.clone()],
            affected_scope: vec![format!("session:{session_id}")],
        });

        let projection = MissionControlRuntime::projection();
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
