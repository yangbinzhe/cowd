//! Mission Runtime global control plane.
//!
//! MissionRuntime sits above individual sessions and teams. It tracks active
//! and background sessions, emits a global event timeline, and exposes a single
//! projection that Mission Control surfaces can render.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::{
    AgentRuntime, ApprovalQueue, ConflictArbiter, MissionEvidenceBus, RuntimeCapabilityCatalog,
    RuntimeEventInput, RuntimeEventScope, RuntimeEventStore, SessionRelationGraph, TeamRuntime,
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
/// Internal aggregate mutation result.
///
/// This is deliberately distinct from the public
/// `harness_contract::mission::MissionCommandReceipt`: the latter is the
/// revision-checked, durable command boundary exposed to surfaces, while this
/// value only records the state transition performed by the Mission aggregate
/// after that command is accepted.
pub struct MissionSessionStateReceipt {
    pub command: String,
    pub status: String,
    pub message: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionProjection {
    pub kind: String,
    pub schema_version: u32,
    /// Canonical workspace Mission aggregate identity. Session membership is
    /// event-sourced by this runtime; consumers must not infer membership
    /// from a shared workspace alone.
    pub mission_id: String,
    pub active_session_id: Option<String>,
    pub sessions: Vec<MissionSessionSnapshot>,
    pub events: Vec<MissionEvent>,
    pub team_projection: serde_json::Value,
    pub agent_projection: serde_json::Value,
    pub approval_projection: serde_json::Value,
    pub relation_projection: serde_json::Value,
    pub execution_graph_projection: serde_json::Value,
    pub conflict_projection: serde_json::Value,
    pub evidence_projection: serde_json::Value,
    pub schedule_projection: serde_json::Value,
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
    next_sequence: u64,
}

/// One durable Mission mutation. The current projected state is included so
/// startup can recover in O(1), while the typed operation is retained for
/// audit and SSE consumers. Mission never writes a parallel state file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionStateEvent {
    event: MissionEvent,
    state: MissionRuntimeState,
}

impl Default for MissionRuntimeState {
    fn default() -> Self {
        Self {
            active_session_id: None,
            sessions: BTreeMap::new(),
            events: Vec::new(),
            next_sequence: 0,
        }
    }
}

#[derive(Debug)]
pub struct MissionRuntime {
    state: Mutex<MissionRuntimeState>,
    event_store: Option<Arc<RuntimeEventStore>>,
    stream_id: Option<String>,
}

impl Default for MissionRuntime {
    fn default() -> Self {
        Self {
            state: Mutex::new(MissionRuntimeState::default()),
            event_store: None,
            stream_id: None,
        }
    }
}

impl MissionRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn event_sourced(
        event_store: Arc<RuntimeEventStore>,
        workspace_key: impl Into<String>,
    ) -> Result<Self, String> {
        let stream_id = format!("mission-runtime:{}", workspace_key.into());
        Ok(Self {
            state: Mutex::new(load_state(&event_store, &stream_id)?),
            event_store: Some(event_store),
            stream_id: Some(stream_id),
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
        if let Some(existing) = state.sessions.get(&session_id).cloned() {
            if state.active_session_id.as_deref() != Some(&session_id) {
                if let Some(active_id) = state.active_session_id.clone() {
                    if let Some(active) = state.sessions.get_mut(&active_id) {
                        active.status = MissionSessionStatus::Background;
                        active.updated_at_ms = now;
                    }
                }
                let snapshot = {
                    let snapshot = state.sessions.get_mut(&session_id).ok_or_else(|| {
                        format!("mission session `{session_id}` disappeared during activation")
                    })?;
                    snapshot.status = MissionSessionStatus::Active;
                    snapshot.updated_at_ms = now;
                    snapshot.clone()
                };
                state.active_session_id = Some(session_id.clone());
                state.push_event(
                    "mission.session.started",
                    "existing mission session activated idempotently",
                    Some(session_id),
                );
                self.commit_state(&mut state)?;
                return Ok(snapshot);
            }
            return Ok(existing);
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
        self.commit_state(&mut state)?;
        Ok(snapshot)
    }

    /// Register an already-created Session with the workspace Mission without
    /// stealing focus from an active session. Gateway uses this after the
    /// Session boundary has durably allocated the identity, so a projection
    /// can prove membership without duplicating session state.
    pub fn register_session(
        &self,
        request: StartMissionSessionRequest,
    ) -> Result<MissionSessionSnapshot, String> {
        if request.title.trim().is_empty() {
            return Err("session title must not be empty".to_string());
        }
        let session_id = request
            .session_id
            .ok_or_else(|| "mission session registration requires a session id".to_string())?;
        let now = now_ms();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = state.sessions.get(&session_id) {
            return Ok(existing.clone());
        }
        let status = if state.active_session_id.is_none() {
            MissionSessionStatus::Active
        } else {
            MissionSessionStatus::Background
        };
        let snapshot = MissionSessionSnapshot {
            session_id: session_id.clone(),
            title: request.title,
            status: status.clone(),
            created_at_ms: now,
            updated_at_ms: now,
            active_team_ids: Vec::new(),
            active_agent_ids: Vec::new(),
        };
        if matches!(status, MissionSessionStatus::Active) {
            state.active_session_id = Some(session_id.clone());
        }
        state.sessions.insert(session_id.clone(), snapshot.clone());
        state.push_event(
            "mission.session.registered",
            "session registered with workspace mission",
            Some(session_id),
        );
        self.commit_state(&mut state)?;
        Ok(snapshot)
    }

    pub fn switch_session(&self, session_id: &str) -> Result<MissionSessionStateReceipt, String> {
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

    pub fn pause_session(&self, session_id: &str) -> Result<MissionSessionStateReceipt, String> {
        self.transition_session(
            session_id,
            "pause_session",
            MissionSessionStatus::Paused,
            "mission session paused",
        )
    }

    pub fn background_session(
        &self,
        session_id: &str,
    ) -> Result<MissionSessionStateReceipt, String> {
        self.transition_session(
            session_id,
            "background_session",
            MissionSessionStatus::Background,
            "mission session moved to background",
        )
    }

    pub fn close_session(&self, session_id: &str) -> Result<MissionSessionStateReceipt, String> {
        self.transition_session(
            session_id,
            "close_session",
            MissionSessionStatus::Closed,
            "mission session closed",
        )
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

    /// Return the only Mission aggregate that owns a registered session.
    /// A session that has not entered the Mission event stream deliberately
    /// has no Mission identity, rather than being assigned a guessed global
    /// label by a surface or projection builder.
    #[must_use]
    pub fn mission_id_for_session(&self, session_id: &str) -> Option<String> {
        self.get_session(session_id)
            .map(|_| self.mission_id().to_string())
    }

    #[must_use]
    pub fn mission_id(&self) -> &str {
        self.stream_id
            .as_deref()
            .unwrap_or("mission-runtime:in-memory")
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

    /// Durable revision of the mission aggregate. Command adapters use this
    /// for optimistic concurrency; it is not a separate state owner.
    pub fn revision(&self) -> Result<u64, String> {
        match (&self.event_store, &self.stream_id) {
            (Some(event_store), Some(stream_id)) => event_store
                .stream_revision(stream_id)
                .map_err(|error| error.to_string()),
            _ => Ok(self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next_sequence),
        }
    }

    pub fn projection(
        &self,
        relations: &SessionRelationGraph,
        agent_runtime: &AgentRuntime,
        team_runtime: &TeamRuntime,
        approval_queue: &ApprovalQueue,
        conflict_resolver: &ConflictArbiter,
        mission_evidence: &MissionEvidenceBus,
        schedule_projection: serde_json::Value,
    ) -> MissionProjection {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let teams = team_runtime.list().unwrap_or_default();
        let agents = agent_runtime.list();
        let mut sessions = state.sessions.values().cloned().collect::<Vec<_>>();
        for session in &mut sessions {
            session.active_team_ids = teams
                .iter()
                .filter(|team| team.session_id == session.session_id && team.status == "running")
                .map(|team| team.team_id.clone())
                .collect();
            session.active_agent_ids = agents
                .iter()
                .filter(|agent| {
                    agent.session_id == session.session_id && !agent.status.is_terminal()
                })
                .map(|agent| agent.agent_id.clone())
                .collect();
        }
        MissionProjection {
            kind: "mission.runtime".to_string(),
            schema_version: 3,
            mission_id: self.mission_id().to_string(),
            active_session_id: state.active_session_id.clone(),
            sessions,
            events: state.events.clone(),
            team_projection: team_runtime.projection_json(),
            agent_projection: serde_json::json!({
                "kind": "runtime.agents",
                "agents": agent_runtime.list(),
            }),
            approval_projection: approval_queue.projection(),
            relation_projection: relations.projection(),
            execution_graph_projection: mission_execution_graph_projection(team_runtime),
            conflict_projection: conflict_resolver.projection(),
            evidence_projection: mission_evidence.projection(),
            schedule_projection,
            capability_projection: serde_json::json!(RuntimeCapabilityCatalog::current()),
            health_projection: mission_health_projection(&state),
            recovery_projection: mission_recovery_projection(),
        }
    }

    fn transition_session(
        &self,
        session_id: &str,
        command: &str,
        status: MissionSessionStatus,
        message: &str,
    ) -> Result<MissionSessionStateReceipt, String> {
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
    ) -> Result<MissionSessionStateReceipt, String> {
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
            return Ok(MissionSessionStateReceipt {
                command: command.to_string(),
                status: "noop".to_string(),
                message: format!("session is already {status}"),
                session_id: Some(session_id.to_string()),
            });
        }
        let message = update(&mut state, &mut session);
        state.sessions.insert(session_id.to_string(), session);
        self.commit_state(&mut state)?;
        Ok(MissionSessionStateReceipt {
            command: command.to_string(),
            status: "accepted".to_string(),
            message,
            session_id: Some(session_id.to_string()),
        })
    }

    fn commit_state(&self, state: &mut MissionRuntimeState) -> Result<(), String> {
        let (Some(event_store), Some(stream_id)) = (&self.event_store, &self.stream_id) else {
            return Ok(());
        };
        let revision = event_store
            .stream_revision(stream_id)
            .map_err(|error| error.to_string())?;
        let event = state
            .events
            .last()
            .cloned()
            .ok_or_else(|| "mission mutation has no typed event".to_string())?;
        let event_kind = format!("{}.v1", event.event_type);
        let payload = serde_json::to_value(MissionStateEvent {
            event,
            state: state.clone(),
        })
        .map_err(|error| error.to_string())?;
        if let Err(error) = event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!(
                    "mission-snapshot:{}:{}",
                    stream_id,
                    revision.saturating_add(1)
                ),
                vec![RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::Mission,
                    kind: event_kind,
                    status: Some("committed".to_string()),
                    actor: Some("mission_runtime".to_string()),
                    refs: Vec::new(),
                    payload,
                }
                .into()],
            )
            .map_err(|error| error.to_string())
        {
            // The event store is canonical. A stale append must never leave an
            // optimistic in-memory Mission projection ahead of durable truth.
            *state = load_state(event_store, stream_id)?;
            return Err(error);
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

fn mission_execution_graph_projection(team_runtime: &TeamRuntime) -> serde_json::Value {
    let execution_graphs = team_runtime
        .list()
        .unwrap_or_default()
        .into_iter()
        .map(|team| {
            serde_json::json!({
            "team_id": team.team_id,
            "session_id": team.session_id,
                "execution_graph_id": team.graph_id,
                "graph_revision": team.graph_revision,
                "status": team.status,
                "agent_count": team.tasks.len(),
                "terminal_result": team.terminal_result,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "kind": "runtime.mission_execution_graphs",
        "count": execution_graphs.len(),
        "execution_graphs": execution_graphs,
    })
}

fn mission_health_projection(state: &MissionRuntimeState) -> serde_json::Value {
    serde_json::json!({
        "kind": "runtime.mission_health",
        "ok": true,
        "status": "ready",
        "degraded_reasons": [],
        "session_count": state.sessions.len(),
        "event_count": state.events.len(),
        "reload": {
            "supported": true,
            "mode": "gateway_config_auto_hot_reload_and_edge_reload_need_projection",
            "manual_reload_surface": "gateway/edge status surfaces"
        }
    })
}

fn mission_recovery_projection() -> serde_json::Value {
    serde_json::json!({
        "kind": "runtime.mission_recovery",
        "candidate_count": 0,
        "candidates": [],
        "owner": "execution_graph_recovery",
        "note": "mission does not own an independent session-command lifecycle",
    })
}

fn load_state(
    event_store: &RuntimeEventStore,
    stream_id: &str,
) -> Result<MissionRuntimeState, String> {
    event_store
        .list_stream(stream_id)?
        .into_iter()
        .rev()
        .find_map(|event| event.kind.starts_with("mission.").then_some(event))
        .map(|event| {
            serde_json::from_value::<MissionStateEvent>(event.payload)
                .map(|event| event.state)
                .map_err(|error| error.to_string())
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mission_runtime_tracks_multiple_sessions_and_projection() {
        let runtime = MissionRuntime::new();
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
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
            .background_session("mission-session-a")
            .expect("background session");
        let projection = runtime.projection(
            &SessionRelationGraph::new(),
            services.agent_runtime(),
            services.team_runtime(),
            services.approval_queue(),
            services.conflict_resolver(),
            services.mission_evidence(),
            services.mission_schedules().projection(),
        );
        assert_eq!(projection.active_session_id, None);
        let session = projection
            .sessions
            .iter()
            .find(|session| session.session_id == "mission-session-a")
            .expect("session a");
        assert_eq!(session.status, MissionSessionStatus::Background);
        assert!(session.active_team_ids.is_empty());
        assert!(session.active_agent_ids.is_empty());
        assert!(projection.events.len() >= 4);
        assert_eq!(projection.kind, "mission.runtime");
        assert_eq!(projection.schema_version, 3);
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
    fn event_sourced_runtime_rebuilds_without_a_side_state_file() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let first = MissionRuntime::event_sourced(Arc::clone(&event_store), "workspace-a")
            .expect("event sourced mission");
        first
            .start_session(StartMissionSessionRequest {
                title: "durable session".to_string(),
                session_id: Some("durable-session".to_string()),
            })
            .expect("session");
        first
            .pause_session("durable-session")
            .expect("persisted transition");

        let rebuilt = MissionRuntime::event_sourced(event_store, "workspace-a")
            .expect("rebuild from event store");
        assert_eq!(
            rebuilt
                .get_session("durable-session")
                .map(|session| session.status),
            Some(MissionSessionStatus::Paused)
        );
        assert!(rebuilt
            .events()
            .iter()
            .any(|event| event.event_type == "mission.session.paused"));
    }

    #[test]
    fn mission_projection_v2_exposes_runtime_control_closure() {
        let runtime = MissionRuntime::new();
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        runtime
            .start_session(StartMissionSessionRequest {
                title: "projection v2".to_string(),
                session_id: Some("mission-projection-v2".to_string()),
            })
            .expect("session");

        let projection = runtime.projection(
            &SessionRelationGraph::new(),
            services.agent_runtime(),
            services.team_runtime(),
            services.approval_queue(),
            services.conflict_resolver(),
            services.mission_evidence(),
            services.mission_schedules().projection(),
        );

        assert_eq!(projection.schema_version, 3);
        assert_eq!(
            projection.execution_graph_projection["kind"],
            "runtime.mission_execution_graphs"
        );
        assert_eq!(projection.conflict_projection["kind"], "runtime.conflicts");
        assert_eq!(
            projection.evidence_projection["kind"],
            "runtime.mission_evidence"
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
