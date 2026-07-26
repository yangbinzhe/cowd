use serde::Serialize;
use session::SessionDomainScope;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub(crate) enum ContextSessionJournalEvent {
    ContextRecommendationAction {
        session_id: String,
        envelope_id: String,
        recommendation: String,
        action: String,
        note: Option<String>,
    },
}

impl ContextSessionJournalEvent {
    pub(crate) fn recommendation_action(
        session_id: impl Into<String>,
        envelope_id: impl Into<String>,
        recommendation: impl Into<String>,
        action: impl Into<String>,
        note: Option<String>,
    ) -> Self {
        Self::ContextRecommendationAction {
            session_id: session_id.into(),
            envelope_id: envelope_id.into(),
            recommendation: recommendation.into(),
            action: action.into(),
            note,
        }
    }

    pub(crate) fn session_id(&self) -> &str {
        match self {
            Self::ContextRecommendationAction { session_id, .. } => session_id,
        }
    }

    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::ContextRecommendationAction { .. } => "context.recommendation_action",
        }
    }

    pub(crate) const fn scope(&self) -> SessionDomainScope {
        SessionDomainScope::Context
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub(crate) enum SurfaceSessionJournalEvent {
    SurfaceMessageReceived {
        surface: String,
        message_id: String,
        thread_id: Option<String>,
        user_id: Option<String>,
        content_preview: String,
        inbox_ref: String,
        payload_sha256: String,
    },
    SurfaceMessageAccepted {
        surface: String,
        message_id: String,
        turn_id: String,
        execution_id: String,
    },
    SurfaceMessageReplied {
        surface: String,
        message_id: String,
        turn_id: String,
        execution_id: String,
        terminal_id: String,
        status: String,
        empty_terminal: bool,
        outbox_ref: Option<String>,
        error_code: Option<String>,
    },
    SurfaceMessageResourcesRegistered {
        surface: String,
        message_id: String,
        current: Vec<SurfaceResourceEvidence>,
        recent: Vec<SurfaceResourceEvidence>,
    },
    SurfaceSessionRuntimeActivated {
        surface: String,
        session_id: String,
        message_id: String,
    },
}

impl SurfaceSessionJournalEvent {
    pub(crate) fn projection_draft(
        &self,
        session_id: impl Into<String>,
    ) -> Result<surface::SurfaceSessionProjectionDraft, serde_json::Error> {
        Ok(surface::SurfaceSessionProjectionDraft {
            phase: self.phase().to_string(),
            session_id: session_id.into(),
            scope: match self.scope() {
                SessionDomainScope::Session => "session",
                SessionDomainScope::Message => "message",
                SessionDomainScope::Turn => "turn",
                SessionDomainScope::Tool => "tool",
                _ => "extension",
            }
            .to_string(),
            kind: self.kind().to_string(),
            status: self.status().to_string(),
            payload_json: serde_json::to_value(self)?,
            phase_offset_ms: self.phase_offset_ms(),
        })
    }

    pub(crate) const fn phase(&self) -> &'static str {
        match self {
            Self::SurfaceMessageReceived { .. } => "received",
            Self::SurfaceMessageAccepted { .. } => "accepted",
            Self::SurfaceMessageReplied { .. } => "replied",
            Self::SurfaceMessageResourcesRegistered { .. } => "resources",
            Self::SurfaceSessionRuntimeActivated { .. } => "activated",
        }
    }

    const fn phase_offset_ms(&self) -> u64 {
        match self {
            Self::SurfaceMessageReceived { .. } => 0,
            Self::SurfaceSessionRuntimeActivated { .. } => 1,
            Self::SurfaceMessageResourcesRegistered { .. } => 2,
            Self::SurfaceMessageAccepted { .. } => 3,
            Self::SurfaceMessageReplied { .. } => 4,
        }
    }

    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::SurfaceMessageReceived { .. } => "surface.message_received",
            Self::SurfaceMessageAccepted { .. } => "surface.message_accepted",
            Self::SurfaceMessageReplied { .. } => "surface.message_replied",
            Self::SurfaceMessageResourcesRegistered { .. } => "surface.resources_registered",
            Self::SurfaceSessionRuntimeActivated { .. } => "surface.runtime_activated",
        }
    }

    pub(crate) const fn scope(&self) -> SessionDomainScope {
        match self {
            Self::SurfaceMessageReceived { .. } | Self::SurfaceMessageReplied { .. } => {
                SessionDomainScope::Message
            }
            Self::SurfaceMessageAccepted { .. } => SessionDomainScope::Turn,
            Self::SurfaceMessageResourcesRegistered { .. } => SessionDomainScope::Tool,
            Self::SurfaceSessionRuntimeActivated { .. } => SessionDomainScope::Session,
        }
    }

    pub(crate) fn status(&self) -> &str {
        match self {
            Self::SurfaceMessageReceived { .. } => "received",
            Self::SurfaceMessageAccepted { .. } => "accepted",
            Self::SurfaceMessageReplied { status, .. } => status,
            Self::SurfaceMessageResourcesRegistered { .. } => "registered",
            Self::SurfaceSessionRuntimeActivated { .. } => "active",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceResourceEvidence {
    pub(crate) source_message_id: String,
    pub(crate) media_type: String,
    pub(crate) resource: Option<SurfaceRegisteredResourceEvidence>,
    pub(crate) status: SurfaceResourceRegistrationStatus,
    pub(crate) error_code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceRegisteredResourceEvidence {
    pub(crate) resource_id: String,
    pub(crate) uri: String,
    pub(crate) kind: String,
    pub(crate) declared_mime: Option<String>,
    pub(crate) detected_mime: Option<String>,
    pub(crate) artifact_selector: String,
    pub(crate) sha256: String,
    pub(crate) bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SurfaceResourceRegistrationStatus {
    Registered,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_terminal_remains_a_replied_phase() {
        let event = SurfaceSessionJournalEvent::SurfaceMessageReplied {
            surface: "feishu".to_string(),
            message_id: "message-1".to_string(),
            turn_id: "turn-1".to_string(),
            execution_id: "execution-1".to_string(),
            terminal_id: "terminal-1".to_string(),
            status: "empty_terminal".to_string(),
            empty_terminal: true,
            outbox_ref: None,
            error_code: None,
        };

        assert_eq!(event.phase(), "replied");
        assert_eq!(event.kind(), "surface.message_replied");
        assert_eq!(event.status(), "empty_terminal");
    }

    #[test]
    fn configure_resources_admission_and_reply_use_the_durable_ledger_identity() {
        let events = [
            SurfaceSessionJournalEvent::SurfaceSessionRuntimeActivated {
                surface: "feishu".to_string(),
                session_id: "session-1".to_string(),
                message_id: "message-1".to_string(),
            },
            SurfaceSessionJournalEvent::SurfaceMessageResourcesRegistered {
                surface: "feishu".to_string(),
                message_id: "message-1".to_string(),
                current: Vec::new(),
                recent: Vec::new(),
            },
            SurfaceSessionJournalEvent::SurfaceMessageAccepted {
                surface: "feishu".to_string(),
                message_id: "message-1".to_string(),
                turn_id: "turn-1".to_string(),
                execution_id: "execution-1".to_string(),
            },
            SurfaceSessionJournalEvent::SurfaceMessageReplied {
                surface: "feishu".to_string(),
                message_id: "message-1".to_string(),
                turn_id: "turn-1".to_string(),
                execution_id: "execution-1".to_string(),
                terminal_id: "terminal-1".to_string(),
                status: "replied".to_string(),
                empty_terminal: false,
                outbox_ref: Some("surface-outbox:delivery-1".to_string()),
                error_code: None,
            },
        ];
        let drafts = events
            .iter()
            .map(|event| event.projection_draft("session-1").expect("draft encodes"))
            .collect::<Vec<_>>();
        let mut first = surface::SurfaceInboxRecord {
            id: "inbox-1".to_string(),
            surface: "feishu".to_string(),
            message_id: "message-1".to_string(),
            idempotency_key: "feishu:message-1".to_string(),
            thread_id: Some("chat-1".to_string()),
            sender_id: Some("user-1".to_string()),
            payload_hash: "abc".to_string(),
            payload_summary: "hello".to_string(),
            payload_json: serde_json::json!({"text":"hello"}),
            status: "received".to_string(),
            received_at_ms: 1_783_000_000_000,
            updated_at_ms: 1_783_000_000_000,
            runtime_session_id: Some("session-1".to_string()),
            runtime_turn_id: None,
            correlation: None,
            session_projections: Vec::new(),
            last_error: None,
        };
        let mut replay = first.clone();
        first
            .stage_session_projections(&drafts)
            .expect("first projection staging");
        replay
            .stage_session_projections(&drafts)
            .expect("replayed projection staging");

        assert_eq!(first.session_projections, replay.session_projections);
        assert_eq!(first.session_projections.len(), events.len());
        assert_eq!(
            first
                .session_projections
                .iter()
                .map(|projection| projection.event_id.as_str())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            events.len()
        );
    }
}
