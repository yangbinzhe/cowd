//! Runtime event protocol for the session event log.
//!
//! Runtime events are the canonical, typed envelope for new runtime state.
//! Existing legacy session events can still be projected into this shape so
//! projections and UI surfaces can consume one event model.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::store::session::SessionEvent;

/// Event type used for canonical runtime events in `session_events`.
pub const RUNTIME_EVENT_TYPE: &str = "RuntimeEvent";

/// Coarse-grained area affected by a runtime event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventScope {
    Session,
    Message,
    Turn,
    Context,
    Tool,
    Agent,
    Workgraph,
    Memory,
    Policy,
    Task,
    Approval,
    Scheduler,
}

/// Reference from a runtime event to a related runtime object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRef {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Canonical append-only runtime event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeEvent {
    pub event_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub scope: RuntimeEventScope,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default)]
    pub refs: Vec<RuntimeRef>,
    #[serde(default)]
    pub payload: Value,
    pub created_at_ms: u64,
}

/// Paged runtime-event projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeEventPage {
    pub total: usize,
    pub events: Vec<RuntimeEvent>,
    pub next_seq: Option<usize>,
    pub has_more: bool,
}

impl RuntimeEvent {
    pub fn new(
        session_id: impl Into<String>,
        sequence: usize,
        scope: RuntimeEventScope,
        kind: impl Into<String>,
        payload: Value,
        created_at_ms: u64,
    ) -> Self {
        Self {
            event_id: format!("event-{}", Uuid::new_v4()),
            session_id: session_id.into(),
            sequence,
            scope,
            kind: kind.into(),
            span_id: None,
            parent_span_id: None,
            correlation_id: None,
            status: None,
            refs: Vec::new(),
            payload,
            created_at_ms,
        }
    }

    pub fn to_session_event(&self) -> Result<SessionEvent, serde_json::Error> {
        Ok(SessionEvent {
            session_id: self.session_id.clone(),
            event_type: RUNTIME_EVENT_TYPE.to_string(),
            event_json: serde_json::to_string(self)?,
            sequence: self.sequence,
            created_at_ms: self.created_at_ms,
        })
    }

    pub fn from_session_event(event: &SessionEvent) -> Result<Self, serde_json::Error> {
        serde_json::from_str(&event.event_json)
    }

    pub fn from_session_event_lossy(event: &SessionEvent) -> Self {
        if event.event_type == RUNTIME_EVENT_TYPE {
            if let Ok(runtime_event) = Self::from_session_event(event) {
                return runtime_event;
            }
        }

        let (payload, raw_parse_failed) = match serde_json::from_str::<Value>(&event.event_json) {
            Ok(value) => (value, false),
            Err(_) => (serde_json::json!({ "raw": event.event_json }), true),
        };
        let payload = if event.event_type == RUNTIME_EVENT_TYPE && raw_parse_failed {
            serde_json::json!({
                "raw": event.event_json,
                "parse_error": "invalid_runtime_event_json"
            })
        } else {
            payload
        };

        let refs = refs_from_payload(&payload);
        let status = if event.event_type == RUNTIME_EVENT_TYPE {
            Some("degraded".to_string())
        } else {
            payload
                .get("status")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        };

        Self {
            event_id: format!("legacy:{}:{}", event.session_id, event.sequence),
            session_id: event.session_id.clone(),
            sequence: event.sequence,
            scope: scope_for_legacy_event_type(&event.event_type),
            kind: event.event_type.clone(),
            span_id: None,
            parent_span_id: None,
            correlation_id: None,
            status,
            refs,
            payload,
            created_at_ms: event.created_at_ms,
        }
    }
}

fn refs_from_payload(payload: &Value) -> Vec<RuntimeRef> {
    payload
        .get("refs")
        .and_then(Value::as_array)
        .map(|refs| {
            refs.iter()
                .filter_map(|reference| {
                    let ref_type = reference
                        .get("type")
                        .or_else(|| reference.get("ref_type"))
                        .and_then(Value::as_str)?;
                    let id = reference.get("id").and_then(Value::as_str)?;
                    let label = reference
                        .get("label")
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                    Some(RuntimeRef {
                        ref_type: ref_type.to_string(),
                        id: id.to_string(),
                        label,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn scope_for_legacy_event_type(event_type: &str) -> RuntimeEventScope {
    match event_type {
        "RuntimeRun" => RuntimeEventScope::Turn,
        "ContextEnvelope" => RuntimeEventScope::Context,
        "ToolStart" | "ToolComplete" => RuntimeEventScope::Tool,
        "TextDelta" | "message_appended" => RuntimeEventScope::Message,
        _ => RuntimeEventScope::Session,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_event_roundtrips_through_session_event() {
        let mut event = RuntimeEvent::new(
            "s-runtime",
            7,
            RuntimeEventScope::Tool,
            "tool.completed",
            serde_json::json!({"tool": "shell", "exit_code": 0}),
            1234,
        );
        event.span_id = Some("span-1".to_string());

        let stored = event.to_session_event().unwrap();
        assert_eq!(stored.event_type, RUNTIME_EVENT_TYPE);
        assert_eq!(stored.sequence, 7);

        let loaded = RuntimeEvent::from_session_event(&stored).unwrap();
        assert_eq!(loaded, event);
    }

    #[test]
    fn legacy_event_is_wrapped_as_runtime_event() {
        let stored = SessionEvent {
            session_id: "s-legacy".to_string(),
            event_type: "ToolStart".to_string(),
            event_json: serde_json::json!({"tool": "shell"}).to_string(),
            sequence: 3,
            created_at_ms: 44,
        };

        let event = RuntimeEvent::from_session_event_lossy(&stored);
        assert_eq!(event.event_id, "legacy:s-legacy:3");
        assert_eq!(event.scope, RuntimeEventScope::Tool);
        assert_eq!(event.kind, "ToolStart");
        assert_eq!(event.payload["tool"], "shell");
        assert_eq!(event.status, None);
    }

    #[test]
    fn legacy_runtime_run_projects_status_and_refs() {
        let stored = SessionEvent {
            session_id: "s-run".to_string(),
            event_type: "RuntimeRun".to_string(),
            event_json: serde_json::json!({
                "run_id": "run-1",
                "status": "completed",
                "refs": [
                    {"type": "context_envelope", "id": "ctx-1", "label": "main context"},
                    {"ref_type": "task", "id": "task-1"}
                ]
            })
            .to_string(),
            sequence: 11,
            created_at_ms: 66,
        };

        let event = RuntimeEvent::from_session_event_lossy(&stored);
        assert_eq!(event.scope, RuntimeEventScope::Turn);
        assert_eq!(event.kind, "RuntimeRun");
        assert_eq!(event.status.as_deref(), Some("completed"));
        assert_eq!(event.refs.len(), 2);
        assert_eq!(event.refs[0].ref_type, "context_envelope");
        assert_eq!(event.refs[0].id, "ctx-1");
        assert_eq!(event.refs[0].label.as_deref(), Some("main context"));
        assert_eq!(event.refs[1].ref_type, "task");
        assert_eq!(event.refs[1].id, "task-1");
    }

    #[test]
    fn corrupt_runtime_event_becomes_degraded_event() {
        let stored = SessionEvent {
            session_id: "s-corrupt".to_string(),
            event_type: RUNTIME_EVENT_TYPE.to_string(),
            event_json: "{not-json".to_string(),
            sequence: 9,
            created_at_ms: 55,
        };

        let event = RuntimeEvent::from_session_event_lossy(&stored);
        assert_eq!(event.event_id, "legacy:s-corrupt:9");
        assert_eq!(event.kind, RUNTIME_EVENT_TYPE);
        assert_eq!(event.status.as_deref(), Some("degraded"));
        assert_eq!(event.payload["parse_error"], "invalid_runtime_event_json");
    }
}
