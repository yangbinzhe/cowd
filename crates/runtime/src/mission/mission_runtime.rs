//! Mission Runtime global control plane.
//!
//! MissionRuntime sits above individual sessions and teams. It tracks active
//! and background sessions, emits a global event timeline, and exposes a single
//! projection that Mission Control surfaces can render.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{global_agent_lifecycle_service, global_team_runtime_service};
use crate::{
    RuntimeCapabilityCatalog, SessionRecoveryCandidate, SessionRelationGraph, StewardScheduler,
    TeamExecutionLoop,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionSessionStatus {
    Active,
    Background,
    Paused,
    Closed,
}

impl MissionSessionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Background => "background",
            Self::Paused => "paused",
            Self::Closed => "closed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionSessionSnapshot {
    pub session_id: String,
    pub title: String,
    pub status: MissionSessionStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub active_team_ids: Vec<String>,
    pub active_agent_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionEvent {
    pub sequence: u64,
    pub event_type: String,
    pub message: String,
    pub session_id: Option<String>,
    pub emitted_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionCommandReceipt {
    pub command: String,
    pub status: String,
    pub message: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionRoutedCommand {
    pub route_id: String,
    pub from_session_id: String,
    pub target_session_id: String,
    pub command: String,
    pub status: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionSessionCommandStatus {
    Pending,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl MissionSessionCommandStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionSessionCommandKind {
    UserInstruction,
    ReviewRequest,
    FollowUp,
    Summarize,
    Delegate,
    InspectEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionSessionCommand {
    pub command_id: String,
    pub route_id: String,
    pub from_session_id: String,
    pub target_session_id: String,
    pub kind: MissionSessionCommandKind,
    pub command: String,
    pub payload: serde_json::Value,
    pub status: MissionSessionCommandStatus,
    pub attempt: u32,
    pub created_at_ms: u64,
    pub claimed_at_ms: Option<u64>,
    pub started_at_ms: Option<u64>,
    pub completed_at_ms: Option<u64>,
    pub failed_at_ms: Option<u64>,
    pub result_ref: Option<String>,
    pub error: Option<String>,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionSessionCommandSummary {
    pub pending: usize,
    pub claimed: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub interrupted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionProjection {
    pub kind: String,
    pub schema_version: u32,
    pub active_session_id: Option<String>,
    pub sessions: Vec<MissionSessionSnapshot>,
    pub events: Vec<MissionEvent>,
    pub routed_commands: Vec<MissionRoutedCommand>,
    pub session_command_summary: MissionSessionCommandSummary,
    pub session_commands: Vec<MissionSessionCommand>,
    pub team_projection: serde_json::Value,
    pub agent_projection: serde_json::Value,
    pub approval_projection: serde_json::Value,
    pub relation_projection: serde_json::Value,
    pub execution_graph_projection: serde_json::Value,
    pub conflict_projection: serde_json::Value,
    pub evidence_projection: serde_json::Value,
    pub steward_projection: serde_json::Value,
    pub capability_projection: serde_json::Value,
    pub health_projection: serde_json::Value,
    pub recovery_projection: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartMissionSessionRequest {
    pub title: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionRuntimeState {
    active_session_id: Option<String>,
    sessions: BTreeMap<String, MissionSessionSnapshot>,
    events: Vec<MissionEvent>,
    routed_commands: Vec<MissionRoutedCommand>,
    session_commands: BTreeMap<String, MissionSessionCommand>,
    next_sequence: u64,
}

impl Default for MissionRuntimeState {
    fn default() -> Self {
        Self {
            active_session_id: None,
            sessions: BTreeMap::new(),
            events: Vec::new(),
            routed_commands: Vec::new(),
            session_commands: BTreeMap::new(),
            next_sequence: 0,
        }
    }
}

#[derive(Debug)]
pub struct MissionRuntime {
    state: Mutex<MissionRuntimeState>,
    state_path: Option<PathBuf>,
}

impl Default for MissionRuntime {
    fn default() -> Self {
        Self {
            state: Mutex::new(MissionRuntimeState::default()),
            state_path: None,
        }
    }
}

impl MissionRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn persistent_at(state_path: PathBuf) -> Result<Self, String> {
        Ok(Self {
            state: Mutex::new(load_state(&state_path)?),
            state_path: Some(state_path),
        })
    }

    pub fn start_session(
        &self,
        request: StartMissionSessionRequest,
    ) -> Result<MissionSessionSnapshot, String> {
        if request.title.trim().is_empty() {
            return Err("session title must not be empty".to_string());
        }
        let now = now_ms();
        let session_id = request
            .session_id
            .unwrap_or_else(|| format!("mission-session-{}", uuid::Uuid::new_v4()));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sessions.contains_key(&session_id) {
            return Err(format!("mission session already exists: {session_id}"));
        }
        if let Some(active_id) = state.active_session_id.clone() {
            if let Some(active) = state.sessions.get_mut(&active_id) {
                active.status = MissionSessionStatus::Background;
                active.updated_at_ms = now;
            }
        }
        let snapshot = MissionSessionSnapshot {
            session_id: session_id.clone(),
            title: request.title,
            status: MissionSessionStatus::Active,
            created_at_ms: now,
            updated_at_ms: now,
            active_team_ids: Vec::new(),
            active_agent_ids: Vec::new(),
        };
        state.active_session_id = Some(session_id.clone());
        state.sessions.insert(session_id.clone(), snapshot.clone());
        state.push_event(
            "mission.session.started",
            "mission session started",
            Some(session_id),
        );
        self.persist_if_enabled(&state)?;
        Ok(snapshot)
    }

    pub fn switch_session(&self, session_id: &str) -> Result<MissionCommandReceipt, String> {
        self.with_session(session_id, "switch_session", |state, session| {
            if let Some(active_id) = state.active_session_id.clone() {
                if active_id != session.session_id {
                    if let Some(active) = state.sessions.get_mut(&active_id) {
                        active.status = MissionSessionStatus::Background;
                        active.updated_at_ms = now_ms();
                    }
                }
            }
            session.status = MissionSessionStatus::Active;
            session.updated_at_ms = now_ms();
            state.active_session_id = Some(session.session_id.clone());
            state.push_event(
                "mission.session.switched",
                "active mission session switched",
                Some(session.session_id.clone()),
            );
            "active session switched".to_string()
        })
    }

    pub fn pause_session(&self, session_id: &str) -> Result<MissionCommandReceipt, String> {
        self.transition_session(
            session_id,
            "pause_session",
            MissionSessionStatus::Paused,
            "mission session paused",
        )
    }

    pub fn background_session(&self, session_id: &str) -> Result<MissionCommandReceipt, String> {
        self.transition_session(
            session_id,
            "background_session",
            MissionSessionStatus::Background,
            "mission session moved to background",
        )
    }

    pub fn close_session(&self, session_id: &str) -> Result<MissionCommandReceipt, String> {
        self.transition_session(
            session_id,
            "close_session",
            MissionSessionStatus::Closed,
            "mission session closed",
        )
    }

    pub fn attach_team(
        &self,
        session_id: &str,
        team_id: impl Into<String>,
    ) -> Result<MissionCommandReceipt, String> {
        let team_id = team_id.into();
        self.with_session(session_id, "attach_team", |state, session| {
            if !session.active_team_ids.contains(&team_id) {
                session.active_team_ids.push(team_id.clone());
            }
            session.updated_at_ms = now_ms();
            state.push_event(
                "mission.team.attached",
                format!("team {team_id} attached to mission session"),
                Some(session.session_id.clone()),
            );
            "team attached".to_string()
        })
    }

    pub fn attach_agent(
        &self,
        session_id: &str,
        agent_id: impl Into<String>,
    ) -> Result<MissionCommandReceipt, String> {
        let agent_id = agent_id.into();
        self.with_session(session_id, "attach_agent", |state, session| {
            if !session.active_agent_ids.contains(&agent_id) {
                session.active_agent_ids.push(agent_id.clone());
            }
            session.updated_at_ms = now_ms();
            state.push_event(
                "mission.agent.attached",
                format!("agent {agent_id} attached to mission session"),
                Some(session.session_id.clone()),
            );
            "agent attached".to_string()
        })
    }

    pub fn record_routed_session_command(
        &self,
        from_session_id: &str,
        target_session_id: &str,
        command: impl Into<String>,
    ) -> Result<MissionRoutedCommand, String> {
        let command = command.into();
        if command.trim().is_empty() {
            return Err("route command must not be empty".to_string());
        }
        let now = now_ms();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.sessions.contains_key(from_session_id) {
            return Err(format!("mission session not found: {from_session_id}"));
        }
        let Some(target) = state.sessions.get_mut(target_session_id) else {
            return Err(format!("mission session not found: {target_session_id}"));
        };
        target.updated_at_ms = now;
        let routed = MissionRoutedCommand {
            route_id: format!("mission-route-{}", uuid::Uuid::new_v4()),
            from_session_id: from_session_id.to_string(),
            target_session_id: target_session_id.to_string(),
            command,
            status: "queued".to_string(),
            created_at_ms: now,
        };
        state.routed_commands.push(routed.clone());
        state.push_event(
            "mission.session.command_routed",
            format!("command routed to mission session {target_session_id}"),
            Some(target_session_id.to_string()),
        );
        self.persist_if_enabled(&state)?;
        Ok(routed)
    }

    pub fn enqueue_session_command(
        &self,
        from_session_id: &str,
        target_session_id: &str,
        command: impl Into<String>,
    ) -> Result<MissionSessionCommand, String> {
        let command = command.into();
        let routed = self.record_routed_session_command(
            from_session_id,
            target_session_id,
            command.clone(),
        )?;
        let route_id = routed.route_id.clone();
        let now = now_ms();
        let session_command = MissionSessionCommand {
            command_id: format!("mission-command-{}", uuid::Uuid::new_v4()),
            route_id: route_id.clone(),
            from_session_id: from_session_id.to_string(),
            target_session_id: target_session_id.to_string(),
            kind: MissionSessionCommandKind::UserInstruction,
            command,
            payload: serde_json::json!({
                "route_id": route_id,
                "from_session_id": from_session_id,
                "target_session_id": target_session_id,
            }),
            status: MissionSessionCommandStatus::Pending,
            attempt: 0,
            created_at_ms: now,
            claimed_at_ms: None,
            started_at_ms: None,
            completed_at_ms: None,
            failed_at_ms: None,
            result_ref: None,
            error: None,
            evidence_refs: vec![format!("mission.route:{}", routed.route_id)],
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .session_commands
            .insert(session_command.command_id.clone(), session_command.clone());
        state.push_event(
            "mission.session.command_enqueued",
            format!("command enqueued for mission session {target_session_id}"),
            Some(target_session_id.to_string()),
        );
        self.persist_if_enabled(&state)?;
        Ok(session_command)
    }

    #[must_use]
    pub fn list_session_commands(&self, session_id: &str) -> Vec<MissionSessionCommand> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .session_commands
            .values()
            .filter(|command| command.target_session_id == session_id)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn get_session_command(&self, command_id: &str) -> Option<MissionSessionCommand> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .session_commands
            .get(command_id)
            .cloned()
    }

    pub fn claim_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<MissionSessionCommand, String> {
        self.update_session_command(session_id, command_id, |command| {
            if command.status != MissionSessionCommandStatus::Pending {
                return Err(format!(
                    "command {} is not pending: {}",
                    command.command_id,
                    command.status.as_str()
                ));
            }
            command.status = MissionSessionCommandStatus::Claimed;
            command.claimed_at_ms = Some(now_ms());
            Ok("mission.session.command_claimed")
        })
    }

    pub fn mark_session_command_running(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<MissionSessionCommand, String> {
        self.update_session_command(session_id, command_id, |command| {
            if !matches!(
                command.status,
                MissionSessionCommandStatus::Pending | MissionSessionCommandStatus::Claimed
            ) {
                return Err(format!(
                    "command {} cannot run from {}",
                    command.command_id,
                    command.status.as_str()
                ));
            }
            command.status = MissionSessionCommandStatus::Running;
            command.started_at_ms = Some(now_ms());
            Ok("mission.session.command_running")
        })
    }

    pub fn complete_session_command(
        &self,
        session_id: &str,
        command_id: &str,
        result_ref: Option<String>,
    ) -> Result<MissionSessionCommand, String> {
        self.update_session_command(session_id, command_id, |command| {
            if command.status.is_terminal() {
                return Err(format!(
                    "command {} is already terminal",
                    command.command_id
                ));
            }
            command.status = MissionSessionCommandStatus::Completed;
            command.completed_at_ms = Some(now_ms());
            command.result_ref = result_ref;
            Ok("mission.session.command_completed")
        })
    }

    pub fn fail_session_command(
        &self,
        session_id: &str,
        command_id: &str,
        error: impl Into<String>,
    ) -> Result<MissionSessionCommand, String> {
        let error = error.into();
        self.update_session_command(session_id, command_id, |command| {
            if command.status.is_terminal() {
                return Err(format!(
                    "command {} is already terminal",
                    command.command_id
                ));
            }
            command.status = MissionSessionCommandStatus::Failed;
            command.failed_at_ms = Some(now_ms());
            command.error = Some(error);
            Ok("mission.session.command_failed")
        })
    }

    pub fn cancel_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<MissionSessionCommand, String> {
        self.update_session_command(session_id, command_id, |command| {
            if command.status.is_terminal() {
                return Err(format!(
                    "command {} is already terminal",
                    command.command_id
                ));
            }
            command.status = MissionSessionCommandStatus::Cancelled;
            command.completed_at_ms = Some(now_ms());
            Ok("mission.session.command_cancelled")
        })
    }

    pub fn interrupt_session_command(
        &self,
        session_id: &str,
        command_id: &str,
        reason: impl Into<String>,
    ) -> Result<MissionSessionCommand, String> {
        let reason = reason.into();
        self.update_session_command(session_id, command_id, |command| {
            if command.status.is_terminal() {
                return Err(format!(
                    "command {} is already terminal",
                    command.command_id
                ));
            }
            command.status = MissionSessionCommandStatus::Interrupted;
            command.completed_at_ms = Some(now_ms());
            command.error = Some(reason);
            Ok("mission.session.command_interrupted")
        })
    }

    pub fn retry_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<MissionSessionCommand, String> {
        self.update_session_command(session_id, command_id, |command| {
            if command.status != MissionSessionCommandStatus::Failed {
                return Err(format!(
                    "command {} can retry only from failed status",
                    command.command_id
                ));
            }
            command.status = MissionSessionCommandStatus::Pending;
            command.attempt = command.attempt.saturating_add(1);
            command.claimed_at_ms = None;
            command.started_at_ms = None;
            command.completed_at_ms = None;
            command.failed_at_ms = None;
            command.error = None;
            Ok("mission.session.command_retried")
        })
    }

    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<MissionSessionSnapshot> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .get(session_id)
            .cloned()
    }

    #[must_use]
    pub fn list_sessions(&self) -> Vec<MissionSessionSnapshot> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .values()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn events(&self) -> Vec<MissionEvent> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .events
            .clone()
    }

    pub fn projection(&self, relations: &SessionRelationGraph) -> MissionProjection {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        MissionProjection {
            kind: "mission.runtime".to_string(),
            schema_version: 2,
            active_session_id: state.active_session_id.clone(),
            sessions: state.sessions.values().cloned().collect(),
            events: state.events.clone(),
            routed_commands: state.routed_commands.clone(),
            session_command_summary: session_command_summary(&state.session_commands),
            session_commands: state.session_commands.values().cloned().collect(),
            team_projection: global_team_runtime_service().projection(),
            agent_projection: global_agent_lifecycle_service().projection(),
            approval_projection: serde_json::Value::Null,
            relation_projection: relations.projection(),
            execution_graph_projection: mission_execution_graph_projection(),
            conflict_projection: serde_json::Value::Null,
            evidence_projection: serde_json::Value::Null,
            steward_projection: serde_json::json!(StewardScheduler::projection()),
            capability_projection: serde_json::json!(RuntimeCapabilityCatalog::current()),
            health_projection: mission_health_projection(&state),
            recovery_projection: mission_recovery_projection(&state),
        }
    }

    #[must_use]
    pub fn recovery_candidates(&self) -> Vec<SessionRecoveryCandidate> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recovery_candidates_for_state(&state)
    }

    pub fn recover_interrupted_work(&self) -> MissionRecoveryReport {
        let mut recovered = Vec::new();
        let mut errors = Vec::new();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let candidates = recovery_candidates_for_state(&state);
        let now = now_ms();
        for candidate in &candidates {
            let Some(command_id) = candidate.command_id.as_deref() else {
                continue;
            };
            let Some(command) = state.session_commands.get_mut(command_id) else {
                errors.push(format!("recovery candidate command missing: {command_id}"));
                continue;
            };
            match command.status {
                MissionSessionCommandStatus::Claimed => {
                    command.status = MissionSessionCommandStatus::Pending;
                    command.claimed_at_ms = None;
                    command.error = Some("recovered claimed command for redispatch".to_string());
                    command.attempt = command.attempt.saturating_add(1);
                    recovered.push(candidate.clone());
                }
                MissionSessionCommandStatus::Running => {
                    command.status = MissionSessionCommandStatus::Interrupted;
                    command.completed_at_ms = Some(now);
                    command.error = Some(
                        "runtime turn interrupted before completion; retry required".to_string(),
                    );
                    recovered.push(candidate.clone());
                }
                _ => {}
            }
        }
        if !recovered.is_empty() {
            state.push_event(
                "mission.recovery.applied",
                format!("{} mission recovery candidates applied", recovered.len()),
                None,
            );
        }
        if let Err(error) = self.persist_if_enabled(&state) {
            errors.push(error);
        }
        MissionRecoveryReport {
            kind: "runtime.mission_recovery_report".to_string(),
            candidates,
            recovered,
            errors,
        }
    }

    fn transition_session(
        &self,
        session_id: &str,
        command: &str,
        status: MissionSessionStatus,
        message: &str,
    ) -> Result<MissionCommandReceipt, String> {
        self.with_session(session_id, command, |state, session| {
            session.status = status.clone();
            session.updated_at_ms = now_ms();
            if matches!(
                status,
                MissionSessionStatus::Closed | MissionSessionStatus::Background
            ) && state.active_session_id.as_deref() == Some(session_id)
            {
                state.active_session_id = None;
            }
            state.push_event(
                format!("mission.session.{}", status.as_str()),
                message,
                Some(session.session_id.clone()),
            );
            message.to_string()
        })
    }

    fn with_session(
        &self,
        session_id: &str,
        command: &str,
        update: impl FnOnce(&mut MissionRuntimeState, &mut MissionSessionSnapshot) -> String,
    ) -> Result<MissionCommandReceipt, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut session = state
            .sessions
            .remove(session_id)
            .ok_or_else(|| format!("mission session not found: {session_id}"))?;
        if matches!(session.status, MissionSessionStatus::Closed) && command != "close_session" {
            let status = session.status.as_str().to_string();
            state.sessions.insert(session_id.to_string(), session);
            return Ok(MissionCommandReceipt {
                command: command.to_string(),
                status: "noop".to_string(),
                message: format!("session is already {status}"),
                session_id: Some(session_id.to_string()),
            });
        }
        let message = update(&mut state, &mut session);
        state.sessions.insert(session_id.to_string(), session);
        self.persist_if_enabled(&state)?;
        Ok(MissionCommandReceipt {
            command: command.to_string(),
            status: "accepted".to_string(),
            message,
            session_id: Some(session_id.to_string()),
        })
    }

    fn update_session_command(
        &self,
        session_id: &str,
        command_id: &str,
        update: impl FnOnce(&mut MissionSessionCommand) -> Result<&'static str, String>,
    ) -> Result<MissionSessionCommand, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.sessions.contains_key(session_id) {
            return Err(format!("mission session not found: {session_id}"));
        }
        let command = state
            .session_commands
            .get_mut(command_id)
            .ok_or_else(|| format!("mission session command not found: {command_id}"))?;
        if command.target_session_id != session_id {
            return Err(format!(
                "command {command_id} does not belong to session {session_id}"
            ));
        }
        let event_type = update(command)?;
        let updated = command.clone();
        state.push_event(
            event_type,
            format!(
                "mission session command {} -> {}",
                updated.command_id,
                updated.status.as_str()
            ),
            Some(session_id.to_string()),
        );
        self.persist_if_enabled(&state)?;
        Ok(updated)
    }

    fn persist_if_enabled(&self, state: &MissionRuntimeState) -> Result<(), String> {
        if let Some(path) = &self.state_path {
            persist_state(path, state)?;
        }
        Ok(())
    }
}

impl MissionRuntimeState {
    fn push_event(
        &mut self,
        event_type: impl Into<String>,
        message: impl Into<String>,
        session_id: Option<String>,
    ) {
        let event_type = event_type.into();
        let message = message.into();
        self.events.push(MissionEvent {
            sequence: self.next_sequence,
            event_type: event_type.clone(),
            message: message.clone(),
            session_id: session_id.clone(),
            emitted_at_ms: now_ms(),
        });
        self.next_sequence += 1;
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn mission_execution_graph_projection() -> serde_json::Value {
    let execution_graphs = global_team_runtime_service()
        .list()
        .into_iter()
        .map(|team| match TeamExecutionLoop::plan(&team.team_id) {
            Ok(plan) => serde_json::json!({
                "team_id": plan.team_id,
                "session_id": plan.session_id,
                "objective": plan.objective,
                "execution_graph_id": plan.execution_graph.id,
                "node_count": plan.execution_graph.nodes.len(),
                "edge_count": plan.execution_graph.edges.len(),
                "quality": plan.execution_graph_quality,
                "ready_node_ids": plan.ready_node_ids,
                "blocked_node_ids": plan.blocked_node_ids,
                "max_parallelism": plan.spec.max_parallelism,
            }),
            Err(error) => serde_json::json!({
                "team_id": team.team_id,
                "session_id": team.session_id,
                "error": error,
            }),
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "kind": "runtime.mission_execution_graphs",
        "count": execution_graphs.len(),
        "execution_graphs": execution_graphs,
    })
}

fn mission_health_projection(state: &MissionRuntimeState) -> serde_json::Value {
    let summary = session_command_summary(&state.session_commands);
    let degraded_reasons = [
        (summary.failed > 0).then(|| format!("failed_session_commands:{}", summary.failed)),
        (summary.interrupted > 0)
            .then(|| format!("interrupted_session_commands:{}", summary.interrupted)),
        (summary.running > 0).then(|| format!("running_session_commands:{}", summary.running)),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    serde_json::json!({
        "kind": "runtime.mission_health",
        "ok": summary.failed == 0 && summary.interrupted == 0,
        "status": if summary.failed > 0 || summary.interrupted > 0 {
            "degraded"
        } else {
            "ready"
        },
        "degraded_reasons": degraded_reasons,
        "session_count": state.sessions.len(),
        "event_count": state.events.len(),
        "session_command_summary": summary,
        "reload": {
            "supported": true,
            "mode": "gateway_config_auto_hot_reload_and_edge_reload_need_projection",
            "manual_reload_surface": "gateway/edge status surfaces"
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionRecoveryReport {
    pub kind: String,
    pub candidates: Vec<SessionRecoveryCandidate>,
    pub recovered: Vec<SessionRecoveryCandidate>,
    pub errors: Vec<String>,
}

fn mission_recovery_projection(state: &MissionRuntimeState) -> serde_json::Value {
    let candidates = recovery_candidates_for_state(state);
    serde_json::json!({
        "kind": "runtime.mission_recovery",
        "candidate_count": candidates.len(),
        "candidates": candidates,
    })
}

fn recovery_candidates_for_state(state: &MissionRuntimeState) -> Vec<SessionRecoveryCandidate> {
    state
        .session_commands
        .values()
        .filter_map(|command| match command.status {
            MissionSessionCommandStatus::Claimed => Some(SessionRecoveryCandidate {
                scope: "session_command".to_string(),
                session_id: Some(command.target_session_id.clone()),
                command_id: Some(command.command_id.clone()),
                agent_id: None,
                status: command.status.as_str().to_string(),
                reason: "claimed command may have been interrupted before dispatch".to_string(),
                suggested_action: "reset_to_pending_and_redispatch".to_string(),
            }),
            MissionSessionCommandStatus::Running => Some(SessionRecoveryCandidate {
                scope: "session_command".to_string(),
                session_id: Some(command.target_session_id.clone()),
                command_id: Some(command.command_id.clone()),
                agent_id: None,
                status: command.status.as_str().to_string(),
                reason: "running command has no durable turn completion result".to_string(),
                suggested_action: "mark_interrupted_then_retry_or_takeover".to_string(),
            }),
            MissionSessionCommandStatus::Interrupted | MissionSessionCommandStatus::Failed => {
                Some(SessionRecoveryCandidate {
                    scope: "session_command".to_string(),
                    session_id: Some(command.target_session_id.clone()),
                    command_id: Some(command.command_id.clone()),
                    agent_id: None,
                    status: command.status.as_str().to_string(),
                    reason: command
                        .error
                        .clone()
                        .unwrap_or_else(|| "command requires review".to_string()),
                    suggested_action: "review_then_retry_or_cancel".to_string(),
                })
            }
            MissionSessionCommandStatus::Pending
            | MissionSessionCommandStatus::Completed
            | MissionSessionCommandStatus::Cancelled => None,
        })
        .collect()
}

fn session_command_summary(
    commands: &BTreeMap<String, MissionSessionCommand>,
) -> MissionSessionCommandSummary {
    let mut summary = MissionSessionCommandSummary {
        pending: 0,
        claimed: 0,
        running: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
        interrupted: 0,
    };
    for command in commands.values() {
        match command.status {
            MissionSessionCommandStatus::Pending => summary.pending += 1,
            MissionSessionCommandStatus::Claimed => summary.claimed += 1,
            MissionSessionCommandStatus::Running => summary.running += 1,
            MissionSessionCommandStatus::Completed => summary.completed += 1,
            MissionSessionCommandStatus::Failed => summary.failed += 1,
            MissionSessionCommandStatus::Cancelled => summary.cancelled += 1,
            MissionSessionCommandStatus::Interrupted => summary.interrupted += 1,
        }
    }
    summary
}

fn load_state(path: &std::path::Path) -> Result<MissionRuntimeState, String> {
    if !path.exists() {
        return Ok(MissionRuntimeState::default());
    }
    let payload = std::fs::read_to_string(&path).map_err(|error| error.to_string())?;
    serde_json::from_str(&payload).map_err(|error| {
        format!(
            "failed to load mission runtime state {}: {error}",
            path.display()
        )
    })
}

fn persist_state(path: &std::path::Path, state: &MissionRuntimeState) -> Result<(), String> {
    let payload = serde_json::to_string_pretty(state).map_err(|error| error.to_string())?;
    write_state_file(path, payload.as_bytes()).map_err(|error| {
        format!(
            "failed to persist mission runtime state {}: {error}",
            path.display()
        )
    })
}

fn write_state_file(path: &std::path::Path, payload: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::write(path, payload) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, payload)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mission_runtime_tracks_multiple_sessions_and_projection() {
        let runtime = MissionRuntime::new();
        let first = runtime
            .start_session(StartMissionSessionRequest {
                title: "first task".to_string(),
                session_id: Some("mission-session-a".to_string()),
            })
            .expect("first session");
        let second = runtime
            .start_session(StartMissionSessionRequest {
                title: "second task".to_string(),
                session_id: Some("mission-session-b".to_string()),
            })
            .expect("second session");

        assert_eq!(first.status, MissionSessionStatus::Active);
        assert_eq!(
            runtime.get_session("mission-session-a").unwrap().status,
            MissionSessionStatus::Background
        );
        assert_eq!(second.status, MissionSessionStatus::Active);

        runtime
            .switch_session("mission-session-a")
            .expect("switch session");
        runtime
            .attach_team("mission-session-a", "team-1")
            .expect("attach team");
        runtime
            .attach_agent("mission-session-a", "agent-1")
            .expect("attach agent");
        runtime
            .background_session("mission-session-a")
            .expect("background session");
        let routed = runtime
            .record_routed_session_command("mission-session-a", "mission-session-b", "review")
            .expect("route command");
        assert_eq!(routed.status, "queued");
        assert_eq!(routed.target_session_id, "mission-session-b");

        let projection = runtime.projection(&SessionRelationGraph::new());
        assert_eq!(projection.active_session_id, None);
        assert_eq!(projection.routed_commands.len(), 1);
        let session = projection
            .sessions
            .iter()
            .find(|session| session.session_id == "mission-session-a")
            .expect("session a");
        assert_eq!(session.status, MissionSessionStatus::Background);
        assert_eq!(session.active_team_ids, vec!["team-1".to_string()]);
        assert_eq!(session.active_agent_ids, vec!["agent-1".to_string()]);
        assert!(projection.events.len() >= 5);
        assert_eq!(projection.kind, "mission.runtime");
        assert_eq!(projection.schema_version, 2);
        assert_eq!(projection.team_projection["kind"], "runtime.teams");
        assert_eq!(projection.agent_projection["kind"], "runtime.agents");
        assert_eq!(
            projection.approval_projection["kind"],
            "runtime.global_approvals"
        );
        assert_eq!(
            projection.relation_projection["kind"],
            "runtime.session_relations"
        );
    }

    #[test]
    fn mission_projection_v2_exposes_runtime_control_closure() {
        let runtime = MissionRuntime::new();
        runtime
            .start_session(StartMissionSessionRequest {
                title: "projection v2".to_string(),
                session_id: Some("mission-projection-v2".to_string()),
            })
            .expect("session");

        let projection = runtime.projection(&SessionRelationGraph::new());

        assert_eq!(projection.schema_version, 2);
        assert_eq!(
            projection.execution_graph_projection["kind"],
            "runtime.mission_execution_graphs"
        );
        assert_eq!(projection.conflict_projection["kind"], "runtime.conflicts");
        assert_eq!(
            projection.evidence_projection["kind"],
            "runtime.mission_evidence"
        );
        assert_eq!(
            projection.steward_projection["kind"],
            "runtime.steward_scheduler"
        );
        assert!(projection.capability_projection["action_contracts"]
            .as_array()
            .is_some_and(|actions| {
                actions
                    .iter()
                    .any(|action| action["runtime_action"] == "use_team_template")
            }));
        assert_eq!(
            projection.health_projection["kind"],
            "runtime.mission_health"
        );
    }
}
