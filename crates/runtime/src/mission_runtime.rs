//! Mission Runtime global control plane.
//!
//! MissionRuntime sits above individual sessions and teams. It tracks active
//! and background sessions, emits a global event timeline, and exposes a single
//! projection that Mission Control surfaces can render.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::global_session_relation_graph;
use crate::{global_agent_lifecycle_service, global_approval_queue, global_team_runtime_service};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionProjection {
    pub kind: String,
    pub active_session_id: Option<String>,
    pub sessions: Vec<MissionSessionSnapshot>,
    pub events: Vec<MissionEvent>,
    pub routed_commands: Vec<MissionRoutedCommand>,
    pub team_projection: serde_json::Value,
    pub agent_projection: serde_json::Value,
    pub approval_projection: serde_json::Value,
    pub relation_projection: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartMissionSessionRequest {
    pub title: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone)]
struct MissionRuntimeState {
    active_session_id: Option<String>,
    sessions: BTreeMap<String, MissionSessionSnapshot>,
    events: Vec<MissionEvent>,
    routed_commands: Vec<MissionRoutedCommand>,
    next_sequence: u64,
}

impl Default for MissionRuntimeState {
    fn default() -> Self {
        Self {
            active_session_id: None,
            sessions: BTreeMap::new(),
            events: Vec::new(),
            routed_commands: Vec::new(),
            next_sequence: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct MissionRuntime {
    state: Mutex<MissionRuntimeState>,
}

impl MissionRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
        Ok(routed)
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

    pub fn projection(&self) -> MissionProjection {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        MissionProjection {
            kind: "mission.runtime".to_string(),
            active_session_id: state.active_session_id.clone(),
            sessions: state.sessions.values().cloned().collect(),
            events: state.events.clone(),
            routed_commands: state.routed_commands.clone(),
            team_projection: global_team_runtime_service().projection(),
            agent_projection: global_agent_lifecycle_service().projection(),
            approval_projection: global_approval_queue().projection(),
            relation_projection: global_session_relation_graph().projection(),
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
        Ok(MissionCommandReceipt {
            command: command.to_string(),
            status: "accepted".to_string(),
            message,
            session_id: Some(session_id.to_string()),
        })
    }
}

impl MissionRuntimeState {
    fn push_event(
        &mut self,
        event_type: impl Into<String>,
        message: impl Into<String>,
        session_id: Option<String>,
    ) {
        self.events.push(MissionEvent {
            sequence: self.next_sequence,
            event_type: event_type.into(),
            message: message.into(),
            session_id,
            emitted_at_ms: now_ms(),
        });
        self.next_sequence += 1;
    }
}

pub fn global_mission_runtime() -> &'static MissionRuntime {
    static RUNTIME: OnceLock<MissionRuntime> = OnceLock::new();
    RUNTIME.get_or_init(MissionRuntime::new)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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

        let projection = runtime.projection();
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
}
