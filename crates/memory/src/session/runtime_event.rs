//! Session-domain event protocol for the session event log.
//!
//! This log owns transcript and application-domain timelines only. Runtime
//! execution lifecycle state belongs to RuntimeEventStore and must never be
//! reconstructed from this protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::store::session::SessionEvent;

/// Wire event type used by canonical session-domain events.
pub const SESSION_DOMAIN_EVENT_TYPE: &str = "SessionDomainEvent";

/// Areas owned by the session-domain timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDomainScope {
    Session,
    Message,
    Turn,
    Context,
    /// Raw tool evidence and its transcript projection, not tool execution state.
    Tool,
    Memory,
    Policy,
    ApplicationTask,
    Mfg,
}

/// Reference from a domain event to a related durable object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDomainRef {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Canonical append-only session-domain event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionDomainEvent {
    pub event_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub scope: SessionDomainScope,
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
    pub refs: Vec<SessionDomainRef>,
    #[serde(default)]
    pub payload: Value,
    pub created_at_ms: u64,
}

/// Paged session-domain projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionDomainEventPage {
    pub total: usize,
    pub events: Vec<SessionDomainEvent>,
    pub next_seq: Option<usize>,
    pub has_more: bool,
}

impl SessionDomainEvent {
    pub fn new(
        session_id: impl Into<String>,
        sequence: usize,
        scope: SessionDomainScope,
        kind: impl Into<String>,
        payload: Value,
        created_at_ms: u64,
    ) -> Self {
        Self {
            event_id: format!("domain-event-{}", Uuid::new_v4()),
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
            event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
            event_json: serde_json::to_string(self)?,
            sequence: self.sequence,
            created_at_ms: self.created_at_ms,
        })
    }

    pub fn from_session_event(event: &SessionEvent) -> Result<Self, serde_json::Error> {
        serde_json::from_str(&event.event_json)
    }

    /// Project legacy non-lifecycle session events into the domain timeline.
    pub(crate) fn from_session_event_lossy(event: &SessionEvent) -> Self {
        if event.event_type == SESSION_DOMAIN_EVENT_TYPE {
            if let Ok(domain_event) = Self::from_session_event(event) {
                return domain_event;
            }
        }

        let (payload, raw_parse_failed) = match serde_json::from_str::<Value>(&event.event_json) {
            Ok(value) => (value, false),
            Err(_) => (serde_json::json!({ "raw": event.event_json }), true),
        };
        let payload = if event.event_type == SESSION_DOMAIN_EVENT_TYPE && raw_parse_failed {
            serde_json::json!({
                "raw": event.event_json,
                "parse_error": "invalid_session_domain_event_json"
            })
        } else {
            payload
        };
        let status = if event.event_type == SESSION_DOMAIN_EVENT_TYPE {
            Some("degraded".to_string())
        } else {
            payload
                .get("status")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        };

        Self {
            event_id: format!("legacy-domain:{}:{}", event.session_id, event.sequence),
            session_id: event.session_id.clone(),
            sequence: event.sequence,
            scope: scope_for_legacy_event_type(&event.event_type),
            kind: event.event_type.clone(),
            span_id: None,
            parent_span_id: None,
            correlation_id: None,
            status,
            refs: refs_from_payload(&payload),
            payload,
            created_at_ms: event.created_at_ms,
        }
    }
}

fn refs_from_payload(payload: &Value) -> Vec<SessionDomainRef> {
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
                    Some(SessionDomainRef {
                        ref_type: ref_type.to_string(),
                        id: id.to_string(),
                        label: reference
                            .get("label")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn scope_for_legacy_event_type(event_type: &str) -> SessionDomainScope {
    match event_type {
        "RuntimeRun" => SessionDomainScope::Turn,
        "ContextEnvelope" => SessionDomainScope::Context,
        "ToolObservationRaw" | "ToolStart" | "ToolComplete" => SessionDomainScope::Tool,
        "TextDelta" | "message_appended" => SessionDomainScope::Message,
        "MemoryPulse" => SessionDomainScope::Memory,
        "PolicyDecision" => SessionDomainScope::Policy,
        "MfgOutcome" => SessionDomainScope::Mfg,
        _ => SessionDomainScope::Session,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_domain_event_roundtrips() {
        let mut event = SessionDomainEvent::new(
            "s-domain",
            7,
            SessionDomainScope::Tool,
            "tool.raw_evidence.persisted",
            serde_json::json!({"evidence_ref": "sha256:abc"}),
            1234,
        );
        event.span_id = Some("span-1".to_string());

        let stored = event.to_session_event().unwrap();
        assert_eq!(stored.event_type, SESSION_DOMAIN_EVENT_TYPE);
        assert_eq!(
            SessionDomainEvent::from_session_event(&stored).unwrap(),
            event
        );
    }

    #[test]
    fn legacy_transcript_event_projects_without_execution_scope() {
        let stored = SessionEvent {
            session_id: "s-legacy".to_string(),
            event_type: "ToolStart".to_string(),
            event_json: serde_json::json!({"tool": "shell"}).to_string(),
            sequence: 3,
            created_at_ms: 44,
        };
        let event = SessionDomainEvent::from_session_event_lossy(&stored);
        assert_eq!(event.scope, SessionDomainScope::Tool);
        assert_eq!(event.event_id, "legacy-domain:s-legacy:3");
    }
}
