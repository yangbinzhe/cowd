//! Canonical execution lineage shared by every durable Runtime entry.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ExecutionIdentityError {
    #[error("execution identity requires `{0}`")]
    Missing(&'static str),
    #[error("execution identity kind forbids `{0}`")]
    Unexpected(&'static str),
    #[error("execution identity kind does not match its required lineage")]
    InvalidLineage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionIdentityKind {
    SessionTurn,
    TaskGraph,
    AgentNode,
    TeamNode,
    ManagedInvocation,
    ScheduleFire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ExecutionIdentityWire", into = "ExecutionIdentityWire")]
pub struct ExecutionIdentity {
    kind: ExecutionIdentityKind,
    principal_id: String,
    workspace_id: String,
    mission_id: Option<String>,
    task_id: Option<String>,
    session_id: Option<String>,
    turn_id: Option<String>,
    graph_id: Option<String>,
    team_run_id: Option<String>,
    agent_run_id: Option<String>,
    node_id: Option<String>,
    invocation_id: Option<String>,
    schedule_id: Option<String>,
    fire_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ExecutionIdentityWire {
    kind: ExecutionIdentityKind,
    principal_id: String,
    workspace_id: String,
    mission_id: Option<String>,
    task_id: Option<String>,
    session_id: Option<String>,
    turn_id: Option<String>,
    graph_id: Option<String>,
    team_run_id: Option<String>,
    agent_run_id: Option<String>,
    node_id: Option<String>,
    invocation_id: Option<String>,
    schedule_id: Option<String>,
    fire_id: Option<String>,
}

impl ExecutionIdentity {
    pub fn for_session_turn(
        principal_id: impl Into<String>,
        workspace_id: impl Into<String>,
        session_id: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> Result<Self, ExecutionIdentityError> {
        Self::checked(ExecutionIdentityWire {
            kind: ExecutionIdentityKind::SessionTurn,
            principal_id: principal_id.into(),
            workspace_id: workspace_id.into(),
            mission_id: None,
            task_id: None,
            session_id: Some(session_id.into()),
            turn_id: Some(turn_id.into()),
            graph_id: None,
            team_run_id: None,
            agent_run_id: None,
            node_id: None,
            invocation_id: None,
            schedule_id: None,
            fire_id: None,
        })
    }

    pub fn for_task_graph(
        principal_id: impl Into<String>,
        workspace_id: impl Into<String>,
        mission_id: impl Into<String>,
        task_id: impl Into<String>,
        session_id: impl Into<String>,
        turn_id: impl Into<String>,
        graph_id: impl Into<String>,
    ) -> Result<Self, ExecutionIdentityError> {
        Self::checked(ExecutionIdentityWire {
            kind: ExecutionIdentityKind::TaskGraph,
            principal_id: principal_id.into(),
            workspace_id: workspace_id.into(),
            mission_id: Some(mission_id.into()),
            task_id: Some(task_id.into()),
            session_id: Some(session_id.into()),
            turn_id: Some(turn_id.into()),
            graph_id: Some(graph_id.into()),
            team_run_id: None,
            agent_run_id: None,
            node_id: None,
            invocation_id: None,
            schedule_id: None,
            fire_id: None,
        })
    }

    pub fn for_agent_node(
        task_graph: &Self,
        agent_run_id: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Result<Self, ExecutionIdentityError> {
        if !matches!(
            task_graph.kind,
            ExecutionIdentityKind::TaskGraph | ExecutionIdentityKind::TeamNode
        ) {
            return Err(ExecutionIdentityError::InvalidLineage);
        }
        let mut wire = ExecutionIdentityWire::from(task_graph.clone());
        wire.kind = ExecutionIdentityKind::AgentNode;
        wire.agent_run_id = Some(agent_run_id.into());
        wire.node_id = Some(node_id.into());
        Self::checked(wire)
    }

    pub fn for_team_node(
        task_graph: &Self,
        team_run_id: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Result<Self, ExecutionIdentityError> {
        if task_graph.kind != ExecutionIdentityKind::TaskGraph {
            return Err(ExecutionIdentityError::InvalidLineage);
        }
        let mut wire = ExecutionIdentityWire::from(task_graph.clone());
        wire.kind = ExecutionIdentityKind::TeamNode;
        wire.team_run_id = Some(team_run_id.into());
        wire.node_id = Some(node_id.into());
        wire.agent_run_id = None;
        Self::checked(wire)
    }

    pub fn for_managed_invocation(
        principal_id: impl Into<String>,
        workspace_id: impl Into<String>,
        mission_id: impl Into<String>,
        task_id: impl Into<String>,
        invocation_id: impl Into<String>,
    ) -> Result<Self, ExecutionIdentityError> {
        Self::checked(ExecutionIdentityWire {
            kind: ExecutionIdentityKind::ManagedInvocation,
            principal_id: principal_id.into(),
            workspace_id: workspace_id.into(),
            mission_id: Some(mission_id.into()),
            task_id: Some(task_id.into()),
            session_id: None,
            turn_id: None,
            graph_id: None,
            team_run_id: None,
            agent_run_id: None,
            node_id: None,
            invocation_id: Some(invocation_id.into()),
            schedule_id: None,
            fire_id: None,
        })
    }

    pub fn for_schedule_fire(
        principal_id: impl Into<String>,
        workspace_id: impl Into<String>,
        mission_id: impl Into<String>,
        task_id: impl Into<String>,
        schedule_id: impl Into<String>,
        fire_id: impl Into<String>,
    ) -> Result<Self, ExecutionIdentityError> {
        Self::checked(ExecutionIdentityWire {
            kind: ExecutionIdentityKind::ScheduleFire,
            principal_id: principal_id.into(),
            workspace_id: workspace_id.into(),
            mission_id: Some(mission_id.into()),
            task_id: Some(task_id.into()),
            session_id: None,
            turn_id: None,
            graph_id: None,
            team_run_id: None,
            agent_run_id: None,
            node_id: None,
            invocation_id: None,
            schedule_id: Some(schedule_id.into()),
            fire_id: Some(fire_id.into()),
        })
    }

    pub fn validate(&self) -> Result<(), ExecutionIdentityError> {
        Self::validate_wire(&ExecutionIdentityWire::from(self.clone()))
    }

    #[must_use]
    pub const fn kind(&self) -> ExecutionIdentityKind {
        self.kind
    }

    #[must_use]
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    #[must_use]
    pub fn mission_id(&self) -> Option<&str> {
        self.mission_id.as_deref()
    }

    #[must_use]
    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }

    #[must_use]
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    #[must_use]
    pub fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    #[must_use]
    pub fn graph_id(&self) -> Option<&str> {
        self.graph_id.as_deref()
    }

    #[must_use]
    pub fn team_run_id(&self) -> Option<&str> {
        self.team_run_id.as_deref()
    }

    #[must_use]
    pub fn agent_run_id(&self) -> Option<&str> {
        self.agent_run_id.as_deref()
    }

    #[must_use]
    pub fn node_id(&self) -> Option<&str> {
        self.node_id.as_deref()
    }

    #[must_use]
    pub fn invocation_id(&self) -> Option<&str> {
        self.invocation_id.as_deref()
    }

    #[must_use]
    pub fn schedule_id(&self) -> Option<&str> {
        self.schedule_id.as_deref()
    }

    #[must_use]
    pub fn fire_id(&self) -> Option<&str> {
        self.fire_id.as_deref()
    }

    fn checked(wire: ExecutionIdentityWire) -> Result<Self, ExecutionIdentityError> {
        Self::validate_wire(&wire)?;
        Ok(Self {
            kind: wire.kind,
            principal_id: wire.principal_id,
            workspace_id: wire.workspace_id,
            mission_id: wire.mission_id,
            task_id: wire.task_id,
            session_id: wire.session_id,
            turn_id: wire.turn_id,
            graph_id: wire.graph_id,
            team_run_id: wire.team_run_id,
            agent_run_id: wire.agent_run_id,
            node_id: wire.node_id,
            invocation_id: wire.invocation_id,
            schedule_id: wire.schedule_id,
            fire_id: wire.fire_id,
        })
    }

    fn validate_wire(wire: &ExecutionIdentityWire) -> Result<(), ExecutionIdentityError> {
        required("principal_id", &wire.principal_id)?;
        required("workspace_id", &wire.workspace_id)?;
        match wire.kind {
            ExecutionIdentityKind::SessionTurn => {
                required_opt("session_id", wire.session_id.as_deref())?;
                required_opt("turn_id", wire.turn_id.as_deref())?;
                forbid_task_execution_fields(wire)?;
            }
            ExecutionIdentityKind::TaskGraph => {
                validate_task_graph(wire)?;
                forbid_opt("team_run_id", wire.team_run_id.as_deref())?;
                forbid_opt("agent_run_id", wire.agent_run_id.as_deref())?;
                forbid_opt("node_id", wire.node_id.as_deref())?;
                forbid_external_entry_fields(wire)?;
            }
            ExecutionIdentityKind::AgentNode => {
                validate_task_graph(wire)?;
                required_opt("agent_run_id", wire.agent_run_id.as_deref())?;
                required_opt("node_id", wire.node_id.as_deref())?;
                forbid_external_entry_fields(wire)?;
            }
            ExecutionIdentityKind::TeamNode => {
                validate_task_graph(wire)?;
                required_opt("team_run_id", wire.team_run_id.as_deref())?;
                required_opt("node_id", wire.node_id.as_deref())?;
                forbid_opt("agent_run_id", wire.agent_run_id.as_deref())?;
                forbid_external_entry_fields(wire)?;
            }
            ExecutionIdentityKind::ManagedInvocation => {
                required_opt("mission_id", wire.mission_id.as_deref())?;
                required_opt("task_id", wire.task_id.as_deref())?;
                required_opt("invocation_id", wire.invocation_id.as_deref())?;
                forbid_session_graph_fields(wire)?;
                forbid_opt("schedule_id", wire.schedule_id.as_deref())?;
                forbid_opt("fire_id", wire.fire_id.as_deref())?;
            }
            ExecutionIdentityKind::ScheduleFire => {
                required_opt("mission_id", wire.mission_id.as_deref())?;
                required_opt("task_id", wire.task_id.as_deref())?;
                required_opt("schedule_id", wire.schedule_id.as_deref())?;
                required_opt("fire_id", wire.fire_id.as_deref())?;
                forbid_session_graph_fields(wire)?;
                forbid_opt("invocation_id", wire.invocation_id.as_deref())?;
            }
        }
        Ok(())
    }
}

impl TryFrom<ExecutionIdentityWire> for ExecutionIdentity {
    type Error = ExecutionIdentityError;

    fn try_from(value: ExecutionIdentityWire) -> Result<Self, Self::Error> {
        Self::checked(value)
    }
}

impl From<ExecutionIdentity> for ExecutionIdentityWire {
    fn from(value: ExecutionIdentity) -> Self {
        Self {
            kind: value.kind,
            principal_id: value.principal_id,
            workspace_id: value.workspace_id,
            mission_id: value.mission_id,
            task_id: value.task_id,
            session_id: value.session_id,
            turn_id: value.turn_id,
            graph_id: value.graph_id,
            team_run_id: value.team_run_id,
            agent_run_id: value.agent_run_id,
            node_id: value.node_id,
            invocation_id: value.invocation_id,
            schedule_id: value.schedule_id,
            fire_id: value.fire_id,
        }
    }
}

fn validate_task_graph(wire: &ExecutionIdentityWire) -> Result<(), ExecutionIdentityError> {
    required_opt("mission_id", wire.mission_id.as_deref())?;
    required_opt("task_id", wire.task_id.as_deref())?;
    required_opt("session_id", wire.session_id.as_deref())?;
    required_opt("turn_id", wire.turn_id.as_deref())?;
    required_opt("graph_id", wire.graph_id.as_deref())
}

fn forbid_task_execution_fields(
    wire: &ExecutionIdentityWire,
) -> Result<(), ExecutionIdentityError> {
    forbid_opt("mission_id", wire.mission_id.as_deref())?;
    forbid_opt("task_id", wire.task_id.as_deref())?;
    forbid_opt("graph_id", wire.graph_id.as_deref())?;
    forbid_opt("team_run_id", wire.team_run_id.as_deref())?;
    forbid_opt("agent_run_id", wire.agent_run_id.as_deref())?;
    forbid_opt("node_id", wire.node_id.as_deref())?;
    forbid_external_entry_fields(wire)
}

fn forbid_session_graph_fields(wire: &ExecutionIdentityWire) -> Result<(), ExecutionIdentityError> {
    forbid_opt("session_id", wire.session_id.as_deref())?;
    forbid_opt("turn_id", wire.turn_id.as_deref())?;
    forbid_opt("graph_id", wire.graph_id.as_deref())?;
    forbid_opt("team_run_id", wire.team_run_id.as_deref())?;
    forbid_opt("agent_run_id", wire.agent_run_id.as_deref())?;
    forbid_opt("node_id", wire.node_id.as_deref())
}

fn forbid_external_entry_fields(
    wire: &ExecutionIdentityWire,
) -> Result<(), ExecutionIdentityError> {
    forbid_opt("invocation_id", wire.invocation_id.as_deref())?;
    forbid_opt("schedule_id", wire.schedule_id.as_deref())?;
    forbid_opt("fire_id", wire.fire_id.as_deref())
}

fn required(field: &'static str, value: &str) -> Result<(), ExecutionIdentityError> {
    if value.trim().is_empty() {
        return Err(ExecutionIdentityError::Missing(field));
    }
    Ok(())
}

fn forbid_opt(field: &'static str, value: Option<&str>) -> Result<(), ExecutionIdentityError> {
    if value.is_some() {
        Err(ExecutionIdentityError::Unexpected(field))
    } else {
        Ok(())
    }
}

fn required_opt(field: &'static str, value: Option<&str>) -> Result<(), ExecutionIdentityError> {
    value.map_or_else(
        || Err(ExecutionIdentityError::Missing(field)),
        |value| required(field, value),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_entry_constructors_enforce_required_lineage() {
        assert!(ExecutionIdentity::for_session_turn("human", "ws", "session", "turn").is_ok());
        assert!(ExecutionIdentity::for_session_turn("", "ws", "session", "turn").is_err());
        assert!(ExecutionIdentity::for_session_turn("human", "", "session", "turn").is_err());
        assert!(ExecutionIdentity::for_session_turn("human", "ws", "", "turn").is_err());
        assert!(ExecutionIdentity::for_session_turn("human", "ws", "session", "").is_err());

        let graph = ExecutionIdentity::for_task_graph(
            "runtime", "ws", "mission", "task", "session", "turn", "graph",
        )
        .expect("task graph identity");
        for invalid in [
            ("", "ws", "mission", "task", "session", "turn", "graph"),
            ("runtime", "", "mission", "task", "session", "turn", "graph"),
            ("runtime", "ws", "", "task", "session", "turn", "graph"),
            ("runtime", "ws", "mission", "", "session", "turn", "graph"),
            ("runtime", "ws", "mission", "task", "", "turn", "graph"),
            ("runtime", "ws", "mission", "task", "session", "", "graph"),
            ("runtime", "ws", "mission", "task", "session", "turn", ""),
        ] {
            assert!(ExecutionIdentity::for_task_graph(
                invalid.0, invalid.1, invalid.2, invalid.3, invalid.4, invalid.5, invalid.6,
            )
            .is_err());
        }
        assert!(ExecutionIdentity::for_agent_node(&graph, "agent-run", "node").is_ok());
        assert!(ExecutionIdentity::for_team_node(&graph, "team-run", "node").is_ok());
        let team =
            ExecutionIdentity::for_team_node(&graph, "team-run", "node").expect("team identity");
        let team_agent = ExecutionIdentity::for_agent_node(&team, "agent-run", "node")
            .expect("team Agent identity");
        assert_eq!(team_agent.team_run_id(), Some("team-run"));
        assert!(ExecutionIdentity::for_team_node(&team, "nested-team", "node").is_err());
        assert!(ExecutionIdentity::for_agent_node(&team_agent, "nested-agent", "node").is_err());
        assert!(ExecutionIdentity::for_agent_node(&graph, "", "node").is_err());
        assert!(ExecutionIdentity::for_agent_node(&graph, "agent-run", "").is_err());
        assert!(ExecutionIdentity::for_team_node(&graph, "", "node").is_err());
        assert!(ExecutionIdentity::for_team_node(&graph, "team-run", "").is_err());
        assert!(ExecutionIdentity::for_managed_invocation(
            "runtime",
            "ws",
            "mission",
            "task",
            "invocation"
        )
        .is_ok());
        for invalid in [
            ("", "ws", "mission", "task", "invocation"),
            ("runtime", "", "mission", "task", "invocation"),
            ("runtime", "ws", "", "task", "invocation"),
            ("runtime", "ws", "mission", "", "invocation"),
            ("runtime", "ws", "mission", "task", ""),
        ] {
            assert!(ExecutionIdentity::for_managed_invocation(
                invalid.0, invalid.1, invalid.2, invalid.3, invalid.4,
            )
            .is_err());
        }
        assert!(ExecutionIdentity::for_schedule_fire(
            "runtime", "ws", "mission", "task", "schedule", "fire"
        )
        .is_ok());
        for invalid in [
            ("", "ws", "mission", "task", "schedule", "fire"),
            ("runtime", "", "mission", "task", "schedule", "fire"),
            ("runtime", "ws", "", "task", "schedule", "fire"),
            ("runtime", "ws", "mission", "", "schedule", "fire"),
            ("runtime", "ws", "mission", "task", "", "fire"),
            ("runtime", "ws", "mission", "task", "schedule", ""),
        ] {
            assert!(ExecutionIdentity::for_schedule_fire(
                invalid.0, invalid.1, invalid.2, invalid.3, invalid.4, invalid.5,
            )
            .is_err());
        }
    }

    #[test]
    fn deserialization_cannot_restore_an_invalid_identity() {
        let invalid = serde_json::json!({
            "kind": "agent_node",
            "principal_id": "runtime",
            "workspace_id": "workspace",
            "mission_id": null,
            "task_id": "task",
            "session_id": null,
            "turn_id": null,
            "graph_id": "graph",
            "team_run_id": null,
            "agent_run_id": "agent",
            "node_id": "node",
            "invocation_id": null,
            "schedule_id": null,
            "fire_id": null
        });
        assert!(serde_json::from_value::<ExecutionIdentity>(invalid).is_err());

        let mut smuggled = serde_json::to_value(
            ExecutionIdentity::for_session_turn("human", "workspace", "session", "turn")
                .expect("Session identity"),
        )
        .expect("serialize Session identity");
        smuggled["mission_id"] = serde_json::json!("unexpected-mission");
        assert!(serde_json::from_value::<ExecutionIdentity>(smuggled).is_err());
    }
}
