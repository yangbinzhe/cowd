//! Runtime-owned Mission aggregates plus a separate Session presence projection.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use harness_contract::mission::{
    MissionAggregate, MissionEntityRef, MissionMutationReceipt, MissionStatus,
};
use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    AgentRuntime, ApprovalQueue, ConflictArbiter, MissionEvidenceBus, RuntimeCapabilityCatalog,
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, SessionRelationGraph,
    TeamRuntime,
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

/// Presentation-only Session membership. Canonical Session lifecycle remains
/// in the Session domain and canonical Mission state stores only typed refs.
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
pub struct MissionSessionStateReceipt {
    pub command: String,
    pub status: String,
    pub message: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionProjection {
    pub kind: String,
    pub schema_version: u32,
    pub mission_id: Option<String>,
    pub aggregate: Option<MissionAggregate>,
    /// Transitional Surface presence projection. It is deliberately outside
    /// `MissionAggregate`; V607 moves its owner to Gateway Session presence.
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MissionPresenceState {
    active_session_id: Option<String>,
    sessions: BTreeMap<String, MissionSessionSnapshot>,
    events: Vec<MissionEvent>,
    next_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionPresenceEvent {
    event: MissionEvent,
    state: MissionPresenceState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MissionAggregateEvent {
    event_kind: String,
    aggregate: MissionAggregate,
    evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug)]
pub struct MissionRuntime {
    missions: Mutex<BTreeMap<String, MissionAggregate>>,
    presence: Mutex<MissionPresenceState>,
    event_store: Option<Arc<RuntimeEventStore>>,
    workspace_id: String,
    default_mission_id: String,
    presence_stream_id: Option<String>,
}

impl Default for MissionRuntime {
    fn default() -> Self {
        let workspace_id = "in-memory".to_string();
        let default_mission_id = deterministic_default_mission_id(&workspace_id);
        Self {
            missions: Mutex::new(BTreeMap::new()),
            presence: Mutex::new(MissionPresenceState::default()),
            event_store: None,
            workspace_id,
            default_mission_id,
            presence_stream_id: None,
        }
    }
}

impl MissionRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn event_sourced(
        event_store: Arc<RuntimeEventStore>,
        workspace_id: impl Into<String>,
    ) -> Result<Self, String> {
        let workspace_id = workspace_id.into();
        let default_mission_id = deterministic_default_mission_id(&workspace_id);
        let missions = load_missions(&event_store, &workspace_id)?;
        let presence_stream_id = format!("mission-presence:{workspace_id}");
        let presence = load_presence(&event_store, &presence_stream_id)?;
        Ok(Self {
            missions: Mutex::new(missions),
            presence: Mutex::new(presence),
            event_store: Some(event_store),
            workspace_id,
            default_mission_id,
            presence_stream_id: Some(presence_stream_id),
        })
    }

    #[must_use]
    pub fn default_mission_id(&self) -> &str {
        &self.default_mission_id
    }

    pub fn create_mission(
        &self,
        mission_id: impl Into<String>,
        objective: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionAggregate, String> {
        let mission_id = mission_id.into();
        let objective = objective.into();
        validate_required("mission_id", &mission_id)?;
        validate_required("objective", &objective)?;
        let mut missions = self
            .missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = missions.get(&mission_id) {
            if existing.objective == objective {
                return Ok(existing.clone());
            }
            return Err(format!(
                "mission `{mission_id}` already exists with another objective"
            ));
        }
        let aggregate = initial_aggregate(mission_id.clone(), self.workspace_id.clone(), objective);
        if let Some(event_store) = &self.event_store {
            append_aggregate_event(
                event_store,
                &aggregate,
                0,
                "mission.created",
                &evidence_refs,
            )?;
        }
        missions.insert(mission_id, aggregate.clone());
        Ok(aggregate)
    }

    pub fn ensure_default_mission(&self) -> Result<MissionAggregate, String> {
        if let Some(existing) = self.aggregate(self.default_mission_id()) {
            return Ok(existing);
        }
        self.create_mission(
            self.default_mission_id().to_string(),
            format!("Workspace mission for {}", self.workspace_id),
            Vec::new(),
        )
    }

    #[must_use]
    pub fn aggregate(&self, mission_id: &str) -> Option<MissionAggregate> {
        self.missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(mission_id)
            .cloned()
    }

    #[must_use]
    pub fn aggregates(&self) -> Vec<MissionAggregate> {
        self.missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    pub fn link_session(
        &self,
        mission_id: &str,
        expected_revision: u64,
        session_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.link_entity(
            mission_id,
            expected_revision,
            session_id,
            evidence_refs,
            "mission.session.linked",
            |aggregate| &mut aggregate.session_refs,
        )
    }

    pub fn link_task(
        &self,
        mission_id: &str,
        expected_revision: u64,
        task_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.link_entity(
            mission_id,
            expected_revision,
            task_id,
            evidence_refs,
            "mission.task.linked",
            |aggregate| &mut aggregate.task_refs,
        )
    }

    pub fn link_graph(
        &self,
        mission_id: &str,
        expected_revision: u64,
        graph_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.link_entity(
            mission_id,
            expected_revision,
            graph_id,
            evidence_refs,
            "mission.graph.linked",
            |aggregate| &mut aggregate.graph_refs,
        )
    }

    pub fn link_team_run(
        &self,
        mission_id: &str,
        expected_revision: u64,
        team_run_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.link_entity(
            mission_id,
            expected_revision,
            team_run_id,
            evidence_refs,
            "mission.team_run.linked",
            |aggregate| &mut aggregate.team_run_refs,
        )
    }

    pub fn link_agent_run(
        &self,
        mission_id: &str,
        expected_revision: u64,
        agent_run_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.link_entity(
            mission_id,
            expected_revision,
            agent_run_id,
            evidence_refs,
            "mission.agent_run.linked",
            |aggregate| &mut aggregate.agent_run_refs,
        )
    }

    pub(crate) fn activate_if_draft(
        &self,
        mission_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate_current(
            mission_id,
            "mission.active".to_string(),
            evidence_refs,
            |aggregate| {
                if aggregate.status == MissionStatus::Draft {
                    aggregate.status = MissionStatus::Active;
                }
                Ok(())
            },
        )
    }

    pub(crate) fn ensure_session_linked(
        &self,
        mission_id: &str,
        session_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.ensure_entity_linked(
            mission_id,
            session_id,
            evidence_refs,
            "mission.session.linked",
            |aggregate| &mut aggregate.session_refs,
        )
    }

    pub(crate) fn ensure_task_linked(
        &self,
        mission_id: &str,
        task_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.ensure_entity_linked(
            mission_id,
            task_id,
            evidence_refs,
            "mission.task.linked",
            |aggregate| &mut aggregate.task_refs,
        )
    }

    pub(crate) fn ensure_graph_linked(
        &self,
        mission_id: &str,
        graph_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.ensure_entity_linked(
            mission_id,
            graph_id,
            evidence_refs,
            "mission.graph.linked",
            |aggregate| &mut aggregate.graph_refs,
        )
    }

    pub(crate) fn ensure_team_run_linked(
        &self,
        mission_id: &str,
        team_run_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.ensure_entity_linked(
            mission_id,
            team_run_id,
            evidence_refs,
            "mission.team_run.linked",
            |aggregate| &mut aggregate.team_run_refs,
        )
    }

    pub(crate) fn ensure_agent_run_linked(
        &self,
        mission_id: &str,
        agent_run_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.ensure_entity_linked(
            mission_id,
            agent_run_id,
            evidence_refs,
            "mission.agent_run.linked",
            |aggregate| &mut aggregate.agent_run_refs,
        )
    }

    pub fn unlink_session(
        &self,
        mission_id: &str,
        expected_revision: u64,
        session_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.unlink_entity(
            mission_id,
            expected_revision,
            session_id,
            evidence_refs,
            "mission.session.unlinked",
            "session",
            |aggregate| &mut aggregate.session_refs,
        )
    }

    pub fn unlink_task(
        &self,
        mission_id: &str,
        expected_revision: u64,
        task_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.unlink_entity(
            mission_id,
            expected_revision,
            task_id,
            evidence_refs,
            "mission.task.unlinked",
            "task",
            |aggregate| &mut aggregate.task_refs,
        )
    }

    pub fn update_strategy_ref(
        &self,
        mission_id: &str,
        expected_revision: u64,
        strategy_ref: Option<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        if strategy_ref
            .as_deref()
            .is_some_and(|strategy_ref| strategy_ref.trim().is_empty())
        {
            return Err("strategy_ref must not be blank".to_string());
        }
        self.mutate_aggregate(
            mission_id,
            expected_revision,
            "mission.strategy.updated".to_string(),
            evidence_refs,
            move |aggregate| {
                aggregate.strategy_ref = strategy_ref;
                Ok(())
            },
        )
    }

    pub fn transition(
        &self,
        mission_id: &str,
        expected_revision: u64,
        status: MissionStatus,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate(
            mission_id,
            expected_revision,
            format!("mission.{}", status.as_str()),
            evidence_refs,
            move |aggregate| {
                validate_mission_transition(aggregate.status, status)?;
                aggregate.status = status;
                Ok(())
            },
        )
    }

    pub fn start_session(
        &self,
        request: StartMissionSessionRequest,
    ) -> Result<MissionSessionSnapshot, String> {
        self.upsert_presence(request, true)
    }

    pub fn register_session(
        &self,
        request: StartMissionSessionRequest,
    ) -> Result<MissionSessionSnapshot, String> {
        self.upsert_presence(request, false)
    }

    fn upsert_presence(
        &self,
        request: StartMissionSessionRequest,
        activate: bool,
    ) -> Result<MissionSessionSnapshot, String> {
        validate_required("session title", &request.title)?;
        let session_id = request
            .session_id
            .unwrap_or_else(|| format!("mission-session-{}", uuid::Uuid::new_v4()));
        let now = now_ms();
        let snapshot = {
            let mut state = self
                .presence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let target_status = if activate {
                MissionSessionStatus::Active
            } else if let Some(existing) = state.sessions.get(&session_id) {
                existing.status.clone()
            } else if state.active_session_id.is_none() {
                MissionSessionStatus::Active
            } else {
                MissionSessionStatus::Background
            };
            if let Some(existing) = state.sessions.get(&session_id) {
                if existing.title == request.title
                    && existing.status == target_status
                    && (!activate
                        || state.active_session_id.as_deref() == Some(session_id.as_str()))
                {
                    return Ok(existing.clone());
                }
            }
            if activate {
                if let Some(active_id) = state.active_session_id.clone() {
                    if let Some(active) = state.sessions.get_mut(&active_id) {
                        active.status = MissionSessionStatus::Background;
                        active.updated_at_ms = now;
                    }
                }
            }
            let status = target_status;
            let snapshot = state
                .sessions
                .entry(session_id.clone())
                .and_modify(|existing| {
                    existing.title.clone_from(&request.title);
                    existing.status = status.clone();
                    existing.updated_at_ms = now;
                })
                .or_insert_with(|| MissionSessionSnapshot {
                    session_id: session_id.clone(),
                    title: request.title,
                    status: status.clone(),
                    created_at_ms: now,
                    updated_at_ms: now,
                    active_team_ids: Vec::new(),
                    active_agent_ids: Vec::new(),
                })
                .clone();
            if status == MissionSessionStatus::Active {
                state.active_session_id = Some(session_id.clone());
            }
            state.push_event(
                if activate {
                    "mission.presence.activated"
                } else {
                    "mission.presence.registered"
                },
                "session presence linked to mission",
                Some(session_id.clone()),
            );
            self.commit_presence(&mut state)?;
            snapshot
        };
        Ok(snapshot)
    }

    pub fn switch_session(&self, session_id: &str) -> Result<MissionSessionStateReceipt, String> {
        self.with_session(session_id, "switch_session", |state, session| {
            if session.status == MissionSessionStatus::Active
                && state.active_session_id.as_deref() == Some(session.session_id.as_str())
            {
                return None;
            }
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
                "mission.presence.activated",
                "active session presence switched",
                Some(session.session_id.clone()),
            );
            Some("active session switched".to_string())
        })
    }

    pub fn pause_session(&self, session_id: &str) -> Result<MissionSessionStateReceipt, String> {
        self.transition_session(
            session_id,
            "pause_session",
            MissionSessionStatus::Paused,
            "session presence paused",
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
            "session presence moved to background",
        )
    }

    pub fn close_session(&self, session_id: &str) -> Result<MissionSessionStateReceipt, String> {
        self.transition_session(
            session_id,
            "close_session",
            MissionSessionStatus::Closed,
            "session presence closed",
        )
    }

    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<MissionSessionSnapshot> {
        self.presence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .get(session_id)
            .cloned()
    }

    #[must_use]
    pub fn mission_id_for_session(&self, session_id: &str) -> Option<String> {
        self.missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .find(|mission| {
                mission
                    .session_refs
                    .iter()
                    .any(|reference| reference.id == session_id)
            })
            .map(|mission| mission.mission_id.clone())
    }

    #[must_use]
    pub fn mission_id(&self) -> &str {
        self.default_mission_id()
    }

    #[must_use]
    pub fn list_sessions(&self) -> Vec<MissionSessionSnapshot> {
        self.presence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .values()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn events(&self) -> Vec<MissionEvent> {
        self.presence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .events
            .clone()
    }

    pub fn revision(&self) -> Result<u64, String> {
        self.aggregate(self.default_mission_id())
            .map(|aggregate| aggregate.revision)
            .ok_or_else(|| "default mission aggregate is missing".to_string())
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
            .presence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let aggregate = self.aggregate(self.default_mission_id());
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
            schema_version: 4,
            mission_id: aggregate
                .as_ref()
                .map(|aggregate| aggregate.mission_id.clone()),
            aggregate,
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

    fn link_entity(
        &self,
        mission_id: &str,
        expected_revision: u64,
        id: &str,
        evidence_refs: Vec<EvidenceRef>,
        event_kind: &str,
        select: impl Fn(&mut MissionAggregate) -> &mut Vec<MissionEntityRef>,
    ) -> Result<MissionMutationReceipt, String> {
        validate_required("linked entity id", id)?;
        let id = id.to_string();
        self.mutate_aggregate(
            mission_id,
            expected_revision,
            event_kind.to_string(),
            evidence_refs,
            move |aggregate| {
                let refs = select(aggregate);
                if refs.iter().any(|reference| reference.id == id) {
                    return Ok(());
                }
                refs.push(MissionEntityRef {
                    id: id.clone(),
                    linked_at_ms: now_ms(),
                });
                Ok(())
            },
        )
    }

    fn ensure_entity_linked(
        &self,
        mission_id: &str,
        id: &str,
        evidence_refs: Vec<EvidenceRef>,
        event_kind: &str,
        select: impl Fn(&mut MissionAggregate) -> &mut Vec<MissionEntityRef>,
    ) -> Result<MissionMutationReceipt, String> {
        validate_required("linked entity id", id)?;
        let id = id.to_string();
        self.mutate_aggregate_current(
            mission_id,
            event_kind.to_string(),
            evidence_refs,
            move |aggregate| {
                let refs = select(aggregate);
                if refs.iter().any(|reference| reference.id == id) {
                    return Ok(());
                }
                refs.push(MissionEntityRef {
                    id: id.clone(),
                    linked_at_ms: now_ms(),
                });
                Ok(())
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn unlink_entity(
        &self,
        mission_id: &str,
        expected_revision: u64,
        id: &str,
        mut evidence_refs: Vec<EvidenceRef>,
        event_kind: &str,
        ref_type: &str,
        select: impl Fn(&mut MissionAggregate) -> &mut Vec<MissionEntityRef>,
    ) -> Result<MissionMutationReceipt, String> {
        validate_required("unlinked entity id", id)?;
        evidence_refs.push(
            EvidenceRef::new(
                ref_type,
                id,
                harness_contract::reality::RealityBoundary::Observed,
            )
            .with_source("runtime.mission"),
        );
        let id = id.to_string();
        self.mutate_aggregate(
            mission_id,
            expected_revision,
            event_kind.to_string(),
            evidence_refs,
            move |aggregate| {
                let refs = select(aggregate);
                let Some(position) = refs.iter().position(|reference| reference.id == id) else {
                    return Ok(());
                };
                refs.remove(position);
                Ok(())
            },
        )
    }

    fn mutate_aggregate(
        &self,
        mission_id: &str,
        expected_revision: u64,
        event_kind: String,
        evidence_refs: Vec<EvidenceRef>,
        mutation: impl FnOnce(&mut MissionAggregate) -> Result<(), String>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate_guarded(
            mission_id,
            Some(expected_revision),
            event_kind,
            evidence_refs,
            mutation,
        )
    }

    fn mutate_aggregate_current(
        &self,
        mission_id: &str,
        event_kind: String,
        evidence_refs: Vec<EvidenceRef>,
        mutation: impl FnOnce(&mut MissionAggregate) -> Result<(), String>,
    ) -> Result<MissionMutationReceipt, String> {
        self.mutate_aggregate_guarded(mission_id, None, event_kind, evidence_refs, mutation)
    }

    fn mutate_aggregate_guarded(
        &self,
        mission_id: &str,
        expected_revision: Option<u64>,
        event_kind: String,
        evidence_refs: Vec<EvidenceRef>,
        mutation: impl FnOnce(&mut MissionAggregate) -> Result<(), String>,
    ) -> Result<MissionMutationReceipt, String> {
        let mut missions = self
            .missions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = missions
            .get(mission_id)
            .cloned()
            .ok_or_else(|| format!("mission `{mission_id}` not found"))?;
        if let Some(expected_revision) = expected_revision {
            if current.revision != expected_revision {
                return Err(format!(
                    "stale mission revision: expected {expected_revision}, actual {}",
                    current.revision
                ));
            }
        }
        let mut next = current.clone();
        mutation(&mut next)?;
        if next == current {
            return Ok(MissionMutationReceipt {
                mission_id: current.mission_id,
                accepted_revision: current.revision,
                status: current.status,
                evidence_refs,
            });
        }
        next.revision = current.revision.saturating_add(1);
        next.updated_at_ms = now_ms();
        if next.status.is_terminal() && evidence_refs.is_empty() {
            return Err("terminal Mission transition requires evidence".to_string());
        }
        if let Some(event_store) = &self.event_store {
            if let Err(error) = append_aggregate_event(
                event_store,
                &next,
                current.revision,
                &event_kind,
                &evidence_refs,
            ) {
                if let Some(restored) = load_mission(event_store, mission_id)? {
                    missions.insert(mission_id.to_string(), restored);
                }
                return Err(error);
            }
        }
        missions.insert(mission_id.to_string(), next.clone());
        Ok(MissionMutationReceipt {
            mission_id: next.mission_id,
            accepted_revision: next.revision,
            status: next.status,
            evidence_refs,
        })
    }

    fn transition_session(
        &self,
        session_id: &str,
        command: &str,
        status: MissionSessionStatus,
        message: &str,
    ) -> Result<MissionSessionStateReceipt, String> {
        self.with_session(session_id, command, |state, session| {
            if session.status == status
                && (!matches!(
                    status,
                    MissionSessionStatus::Closed | MissionSessionStatus::Background
                ) || state.active_session_id.as_deref() != Some(session_id))
            {
                return None;
            }
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
                format!("mission.presence.{}", status.as_str()),
                message,
                Some(session.session_id.clone()),
            );
            Some(message.to_string())
        })
    }

    fn with_session(
        &self,
        session_id: &str,
        command: &str,
        update: impl FnOnce(&mut MissionPresenceState, &mut MissionSessionSnapshot) -> Option<String>,
    ) -> Result<MissionSessionStateReceipt, String> {
        let mut state = self
            .presence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut session = state
            .sessions
            .remove(session_id)
            .ok_or_else(|| format!("mission session presence not found: {session_id}"))?;
        if session.status == MissionSessionStatus::Closed && command != "close_session" {
            state.sessions.insert(session_id.to_string(), session);
            return Ok(MissionSessionStateReceipt {
                command: command.to_string(),
                status: "noop".to_string(),
                message: "session presence is already closed".to_string(),
                session_id: Some(session_id.to_string()),
            });
        }
        let Some(message) = update(&mut state, &mut session) else {
            state.sessions.insert(session_id.to_string(), session);
            return Ok(MissionSessionStateReceipt {
                command: command.to_string(),
                status: "noop".to_string(),
                message: "session presence already satisfies the requested state".to_string(),
                session_id: Some(session_id.to_string()),
            });
        };
        state.sessions.insert(session_id.to_string(), session);
        self.commit_presence(&mut state)?;
        Ok(MissionSessionStateReceipt {
            command: command.to_string(),
            status: "accepted".to_string(),
            message,
            session_id: Some(session_id.to_string()),
        })
    }

    fn commit_presence(&self, state: &mut MissionPresenceState) -> Result<(), String> {
        let (Some(event_store), Some(stream_id)) = (&self.event_store, &self.presence_stream_id)
        else {
            return Ok(());
        };
        let revision = event_store
            .stream_revision(stream_id)
            .map_err(|error| error.to_string())?;
        let event = state
            .events
            .last()
            .cloned()
            .ok_or_else(|| "mission presence mutation has no event".to_string())?;
        let payload = serde_json::to_value(MissionPresenceEvent {
            event: event.clone(),
            state: state.clone(),
        })
        .map_err(|error| error.to_string())?;
        let mut refs = vec![RuntimeEventRef {
            kind: "workspace".to_string(),
            id: self.workspace_id.clone(),
        }];
        if let Some(session_id) = &event.session_id {
            refs.push(RuntimeEventRef {
                kind: "session".to_string(),
                id: session_id.clone(),
            });
        }
        if let Err(error) = event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("mission-presence:{stream_id}:{}", event.sequence),
                vec![RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::Mission,
                    kind: format!("{}.v1", event.event_type),
                    status: Some("committed".to_string()),
                    actor: Some("mission_presence".to_string()),
                    refs,
                    payload,
                }
                .into()],
            )
            .map_err(|error| error.to_string())
        {
            *state = load_presence(event_store, stream_id)?;
            return Err(error);
        }
        Ok(())
    }
}

impl MissionPresenceState {
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
        self.next_sequence = self.next_sequence.saturating_add(1);
    }
}

fn append_aggregate_event(
    event_store: &RuntimeEventStore,
    aggregate: &MissionAggregate,
    expected_revision: u64,
    event_kind: &str,
    evidence_refs: &[EvidenceRef],
) -> Result<(), String> {
    let mut refs = vec![
        RuntimeEventRef {
            kind: "mission".to_string(),
            id: aggregate.mission_id.clone(),
        },
        RuntimeEventRef {
            kind: "workspace".to_string(),
            id: aggregate.workspace_id.clone(),
        },
    ];
    for (kind, entities) in [
        ("session", aggregate.session_refs.as_slice()),
        ("task", aggregate.task_refs.as_slice()),
        ("execution_graph", aggregate.graph_refs.as_slice()),
        ("team_run", aggregate.team_run_refs.as_slice()),
        ("agent_run", aggregate.agent_run_refs.as_slice()),
    ] {
        refs.extend(entities.iter().map(|entity| RuntimeEventRef {
            kind: kind.to_string(),
            id: entity.id.clone(),
        }));
    }
    refs.extend(evidence_refs.iter().map(|reference| RuntimeEventRef {
        kind: reference.ref_type.clone(),
        id: reference.id.clone(),
    }));
    refs.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.id.cmp(&right.id))
    });
    refs.dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
    event_store
        .append_batch_if_revision(
            mission_stream_id(&aggregate.mission_id),
            expected_revision,
            format!(
                "mission:{}:revision:{}",
                aggregate.mission_id, aggregate.revision
            ),
            vec![RuntimeEventInput {
                stream_id: mission_stream_id(&aggregate.mission_id),
                scope: RuntimeEventScope::Mission,
                kind: format!("{event_kind}.v1"),
                status: Some(aggregate.status.as_str().to_string()),
                actor: Some("mission_runtime".to_string()),
                refs,
                payload: serde_json::to_value(MissionAggregateEvent {
                    event_kind: event_kind.to_string(),
                    aggregate: aggregate.clone(),
                    evidence_refs: evidence_refs.to_vec(),
                })
                .map_err(|error| error.to_string())?,
            }
            .into()],
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn load_missions(
    event_store: &RuntimeEventStore,
    workspace_id: &str,
) -> Result<BTreeMap<String, MissionAggregate>, String> {
    let mut missions = BTreeMap::new();
    for stream_id in event_store
        .stream_ids_for_scope(RuntimeEventScope::Mission)
        .map_err(|error| error.to_string())?
    {
        let Some(mission_id) = stream_id.strip_prefix("mission:") else {
            continue;
        };
        if let Some(aggregate) = load_mission(event_store, mission_id)? {
            if aggregate.workspace_id == workspace_id {
                missions.insert(mission_id.to_string(), aggregate);
            }
        }
    }
    Ok(missions)
}

fn load_mission(
    event_store: &RuntimeEventStore,
    mission_id: &str,
) -> Result<Option<MissionAggregate>, String> {
    event_store
        .list_stream(&mission_stream_id(mission_id))?
        .into_iter()
        .rev()
        .find_map(|event| {
            event
                .kind
                .starts_with("mission.")
                .then(|| serde_json::from_value::<MissionAggregateEvent>(event.payload))
        })
        .transpose()
        .map_err(|error| error.to_string())
        .map(|event| event.map(|event| event.aggregate))
}

fn load_presence(
    event_store: &RuntimeEventStore,
    stream_id: &str,
) -> Result<MissionPresenceState, String> {
    event_store
        .list_stream(stream_id)?
        .into_iter()
        .rev()
        .find_map(|event| {
            event
                .kind
                .starts_with("mission.presence.")
                .then(|| serde_json::from_value::<MissionPresenceEvent>(event.payload))
        })
        .transpose()
        .map_err(|error| error.to_string())
        .map(|event| event.map_or_else(MissionPresenceState::default, |event| event.state))
}

fn initial_aggregate(
    mission_id: String,
    workspace_id: String,
    objective: String,
) -> MissionAggregate {
    let now = now_ms();
    MissionAggregate {
        mission_id,
        workspace_id,
        objective,
        status: MissionStatus::Draft,
        revision: 1,
        strategy_ref: None,
        session_refs: Vec::new(),
        task_refs: Vec::new(),
        graph_refs: Vec::new(),
        team_run_refs: Vec::new(),
        agent_run_refs: Vec::new(),
        created_at_ms: now,
        updated_at_ms: now,
    }
}

fn deterministic_default_mission_id(workspace_id: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(workspace_id.as_bytes()));
    format!("mission-default-{}", &digest[..16])
}

fn mission_stream_id(mission_id: &str) -> String {
    format!("mission:{mission_id}")
}

fn validate_mission_transition(from: MissionStatus, to: MissionStatus) -> Result<(), String> {
    let allowed = matches!(
        (from, to),
        (MissionStatus::Draft, MissionStatus::Active)
            | (MissionStatus::Active, MissionStatus::Paused)
            | (MissionStatus::Paused, MissionStatus::Active)
            | (
                MissionStatus::Active | MissionStatus::Paused,
                MissionStatus::Completed | MissionStatus::Cancelled | MissionStatus::Failed
            )
    );
    if !allowed {
        return Err(format!(
            "illegal Mission transition {} -> {}",
            from.as_str(),
            to.as_str()
        ));
    }
    Ok(())
}

fn validate_required(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("Mission `{field}` is required"));
    }
    Ok(())
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

fn mission_health_projection(state: &MissionPresenceState) -> serde_json::Value {
    serde_json::json!({
        "kind": "runtime.mission_health",
        "ok": true,
        "status": "ready",
        "degraded_reasons": [],
        "session_count": state.sessions.len(),
        "event_count": state.events.len(),
    })
}

fn mission_recovery_projection() -> serde_json::Value {
    serde_json::json!({
        "kind": "runtime.mission_recovery",
        "candidate_count": 0,
        "candidates": [],
        "owner": "runtime_mission_aggregate",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_mission_links_all_owned_execution_entities() {
        let runtime = MissionRuntime::new();
        runtime
            .register_session(StartMissionSessionRequest {
                title: "session a".to_string(),
                session_id: Some("session-a".to_string()),
            })
            .expect("session a");
        runtime
            .register_session(StartMissionSessionRequest {
                title: "session b".to_string(),
                session_id: Some("session-b".to_string()),
            })
            .expect("session b");
        assert!(runtime.aggregates().is_empty());
        let mission = runtime.ensure_default_mission().expect("default mission");
        runtime
            .link_session(
                runtime.default_mission_id(),
                mission.revision,
                "session-a",
                Vec::new(),
            )
            .expect("session a link");
        let mission = runtime
            .aggregate(runtime.default_mission_id())
            .expect("default mission");
        runtime
            .link_session(
                runtime.default_mission_id(),
                mission.revision,
                "session-b",
                Vec::new(),
            )
            .expect("session b link");
        let mission = runtime
            .aggregate(runtime.default_mission_id())
            .expect("default mission");
        runtime
            .link_task(
                runtime.default_mission_id(),
                mission.revision,
                "task-a",
                Vec::new(),
            )
            .expect("task a");
        let mission = runtime
            .aggregate(runtime.default_mission_id())
            .expect("default mission");
        runtime
            .link_task(
                runtime.default_mission_id(),
                mission.revision,
                "task-b",
                Vec::new(),
            )
            .expect("task b");
        let mission = runtime
            .aggregate(runtime.default_mission_id())
            .expect("default mission");
        runtime
            .link_graph(
                runtime.default_mission_id(),
                mission.revision,
                "graph-a",
                Vec::new(),
            )
            .expect("graph");
        let mission = runtime
            .aggregate(runtime.default_mission_id())
            .expect("default mission");
        runtime
            .link_team_run(
                runtime.default_mission_id(),
                mission.revision,
                "team-run-a",
                Vec::new(),
            )
            .expect("team run");
        let mission = runtime
            .aggregate(runtime.default_mission_id())
            .expect("default mission");
        runtime
            .link_agent_run(
                runtime.default_mission_id(),
                mission.revision,
                "agent-run-a",
                Vec::new(),
            )
            .expect("agent run");
        let mission = runtime
            .aggregate(runtime.default_mission_id())
            .expect("default mission");
        assert_eq!(mission.session_refs.len(), 2);
        assert_eq!(mission.task_refs.len(), 2);
        assert_eq!(mission.graph_refs[0].id, "graph-a");
        assert_eq!(mission.team_run_refs[0].id, "team-run-a");
        assert_eq!(mission.agent_run_refs[0].id, "agent-run-a");
    }

    #[test]
    fn event_sourced_aggregate_and_presence_rebuild_independently() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let runtime = MissionRuntime::event_sourced(Arc::clone(&event_store), "workspace-a")
            .expect("mission runtime");
        runtime
            .register_session(StartMissionSessionRequest {
                title: "durable".to_string(),
                session_id: Some("session-durable".to_string()),
            })
            .expect("register");
        assert!(runtime.aggregates().is_empty());
        let mission = runtime.ensure_default_mission().expect("mission");
        let event = event_store
            .list_stream(&mission_stream_id(&mission.mission_id))
            .expect("mission event stream")
            .into_iter()
            .last()
            .expect("mission event");
        assert!(event
            .refs
            .iter()
            .any(|reference| reference.kind == "mission" && reference.id == mission.mission_id));
        assert!(
            event
                .refs
                .iter()
                .any(|reference| reference.kind == "workspace"
                    && reference.id == mission.workspace_id)
        );
        runtime
            .link_team_run(
                &mission.mission_id,
                mission.revision,
                "team-run-durable",
                Vec::new(),
            )
            .expect("link durable team run");
        let mission = runtime
            .aggregate(&mission.mission_id)
            .expect("linked mission");
        runtime
            .link_agent_run(
                &mission.mission_id,
                mission.revision,
                "agent-run-durable",
                Vec::new(),
            )
            .expect("link durable agent run");
        let event = event_store
            .list_stream(&mission_stream_id(&mission.mission_id))
            .expect("mission event stream")
            .into_iter()
            .last()
            .expect("mission agent event");
        assert!(event
            .refs
            .iter()
            .any(|reference| reference.kind == "team_run" && reference.id == "team-run-durable"));
        assert!(event
            .refs
            .iter()
            .any(|reference| reference.kind == "agent_run" && reference.id == "agent-run-durable"));
        let rebuilt = MissionRuntime::event_sourced(Arc::clone(&event_store), "workspace-a")
            .expect("rebuild runtime");
        let rebuilt_mission = rebuilt
            .aggregate(rebuilt.default_mission_id())
            .expect("rebuilt mission");
        assert_eq!(rebuilt_mission.revision, mission.revision + 1);
        assert_eq!(rebuilt_mission.team_run_refs[0].id, "team-run-durable");
        assert_eq!(rebuilt_mission.agent_run_refs[0].id, "agent-run-durable");
        assert!(rebuilt.get_session("session-durable").is_some());
    }

    #[test]
    fn mission_unlink_and_strategy_updates_are_revisioned_and_replayable() {
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let runtime = MissionRuntime::event_sourced(Arc::clone(&event_store), "workspace-policy")
            .expect("mission runtime");
        let mission = runtime.ensure_default_mission().expect("mission");
        runtime
            .link_session(
                &mission.mission_id,
                mission.revision,
                "session-policy",
                Vec::new(),
            )
            .expect("link session");
        let mission = runtime
            .aggregate(&mission.mission_id)
            .expect("linked mission");
        runtime
            .update_strategy_ref(
                &mission.mission_id,
                mission.revision,
                Some("strategy://bounded-parallel".to_string()),
                Vec::new(),
            )
            .expect("update strategy");
        let mission = runtime
            .aggregate(&mission.mission_id)
            .expect("strategy mission");
        runtime
            .unlink_session(
                &mission.mission_id,
                mission.revision,
                "session-policy",
                Vec::new(),
            )
            .expect("unlink session");

        let rebuilt = MissionRuntime::event_sourced(Arc::clone(&event_store), "workspace-policy")
            .expect("rebuild mission runtime");
        let rebuilt = rebuilt
            .aggregate(rebuilt.default_mission_id())
            .expect("rebuilt mission");
        assert_eq!(
            rebuilt.strategy_ref.as_deref(),
            Some("strategy://bounded-parallel")
        );
        assert!(rebuilt.session_refs.is_empty());
        let unlink_event = event_store
            .list_stream(&mission_stream_id(&rebuilt.mission_id))
            .expect("mission events")
            .into_iter()
            .last()
            .expect("unlink event");
        assert_eq!(unlink_event.kind, "mission.session.unlinked.v1");
        assert!(unlink_event
            .refs
            .iter()
            .any(|reference| reference.kind == "session" && reference.id == "session-policy"));
    }

    #[test]
    fn mission_revision_cas_and_terminal_evidence_are_enforced() {
        let runtime = MissionRuntime::new();
        let mission = runtime.ensure_default_mission().expect("mission");
        assert!(runtime
            .transition(
                runtime.default_mission_id(),
                mission.revision + 1,
                MissionStatus::Active,
                Vec::new()
            )
            .is_err());
        let activated = runtime
            .transition(
                runtime.default_mission_id(),
                mission.revision,
                MissionStatus::Active,
                Vec::new(),
            )
            .expect("activate");
        assert!(runtime
            .transition(
                runtime.default_mission_id(),
                activated.accepted_revision,
                MissionStatus::Completed,
                Vec::new()
            )
            .is_err());
    }
}
