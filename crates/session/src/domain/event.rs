//! Session-domain event protocol for the session event log.
//!
//! This log owns transcript and application-domain timelines only. Runtime
//! execution lifecycle state belongs to RuntimeEventStore and must never be
//! reconstructed from this protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::persistence::SessionEvent;

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

    /// Compare the semantic content of two stored domain events while
    /// ignoring only the store-allocated sequence. `serde_json::Value`
    /// equality already ignores object-key insertion order while preserving
    /// array order and scalar values.
    pub fn semantically_equivalent(
        left: &SessionEvent,
        right: &SessionEvent,
    ) -> Result<bool, serde_json::Error> {
        if left.session_id != right.session_id
            || left.event_type != right.event_type
            || left.created_at_ms != right.created_at_ms
        {
            return Ok(false);
        }
        let mut left = Self::from_session_event(left)?;
        let mut right = Self::from_session_event(right)?;
        left.sequence = 0;
        right.sequence = 0;
        Ok(left == right)
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
    fn semantic_equivalence_ignores_sequence_and_object_key_order_only() {
        let mut first = SessionDomainEvent::new(
            "s-semantic",
            0,
            SessionDomainScope::ApplicationTask,
            "application.execution_outcome",
            serde_json::json!({"a": 1, "b": 2}),
            1234,
        );
        first.event_id = "app-outcome-1".to_string();
        let first = first.to_session_event().unwrap();

        let mut second = SessionDomainEvent::from_session_event(&first).unwrap();
        second.sequence = 99;
        second.payload = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        let second = second.to_session_event().unwrap();
        assert!(SessionDomainEvent::semantically_equivalent(&first, &second).unwrap());

        let mut changed = SessionDomainEvent::from_session_event(&second).unwrap();
        changed.payload["b"] = serde_json::json!(3);
        let changed = changed.to_session_event().unwrap();
        assert!(!SessionDomainEvent::semantically_equivalent(&first, &changed).unwrap());
    }
}
