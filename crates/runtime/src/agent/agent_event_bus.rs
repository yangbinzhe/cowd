//! Runtime-owned agent progress event bus.

use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentProgressEvent {
    pub event_id: String,
    pub team_id: String,
    pub session_id: String,
    pub agent_id: Option<String>,
    pub role_id: String,
    pub task_id: Option<String>,
    pub event_type: String,
    pub message: String,
    pub evidence_refs: Vec<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Default)]
pub struct AgentEventBus {
    events: Mutex<Vec<AgentProgressEvent>>,
}

impl AgentEventBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, event: AgentProgressEvent) -> AgentProgressEvent {
        let event = AgentProgressEvent {
            event_id: if event.event_id.trim().is_empty() {
                format!("agent-event-{}", uuid::Uuid::new_v4())
            } else {
                event.event_id
            },
            created_at_ms: if event.created_at_ms == 0 {
                now_ms()
            } else {
                event.created_at_ms
            },
            ..event
        };
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
        event
    }

    #[must_use]
    pub fn list_for_team(&self, team_id: &str) -> Vec<AgentProgressEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|event| event.team_id == team_id)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn list_for_agent(&self, agent_id: &str) -> Vec<AgentProgressEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|event| event.agent_id.as_deref() == Some(agent_id))
            .cloned()
            .collect()
    }
}

pub fn global_agent_event_bus() -> &'static AgentEventBus {
    static BUS: OnceLock<AgentEventBus> = OnceLock::new();
    BUS.get_or_init(AgentEventBus::new)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
