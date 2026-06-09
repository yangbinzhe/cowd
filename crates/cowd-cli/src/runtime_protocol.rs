use serde::{Deserialize, Serialize};

pub(crate) const RUNTIME_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeCommandKind {
    RuntimeStatus,
    RuntimeSnapshot,
    SessionEnsure,
    SessionList,
    SessionLeaseAcquire,
    SessionLeaseRelease,
    SessionAttach,
    SessionDetach,
    SessionLifecycle,
    SessionReplay,
    SessionChat,
    Unknown,
}

impl RuntimeCommandKind {
    #[must_use]
    pub(crate) fn from_wire(raw: &str) -> Self {
        match raw {
            "status" | "runtime.status" => Self::RuntimeStatus,
            "runtime_snapshot" | "runtime.snapshot" => Self::RuntimeSnapshot,
            "ensure_session" | "session.ensure" | "create_session" => Self::SessionEnsure,
            "list_sessions" | "session.list" => Self::SessionList,
            "acquire_session_lease" | "session.lease.acquire" => Self::SessionLeaseAcquire,
            "release_session_lease" | "session.lease.release" => Self::SessionLeaseRelease,
            "attach_session" | "session.attach" => Self::SessionAttach,
            "detach_session" | "session.detach" => Self::SessionDetach,
            "session.lifecycle" | "session.lifecycle.snapshot" => Self::SessionLifecycle,
            "replay_session" | "session.replay" => Self::SessionReplay,
            "chat" | "chat_stream" | "session.chat" => Self::SessionChat,
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            Self::RuntimeStatus => "runtime.status",
            Self::RuntimeSnapshot => "runtime.snapshot",
            Self::SessionEnsure => "session.ensure",
            Self::SessionList => "session.list",
            Self::SessionLeaseAcquire => "session.lease.acquire",
            Self::SessionLeaseRelease => "session.lease.release",
            Self::SessionAttach => "session.attach",
            Self::SessionDetach => "session.detach",
            Self::SessionLifecycle => "session.lifecycle",
            Self::SessionReplay => "session.replay",
            Self::SessionChat => "session.chat",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeActorRef {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) surface: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RuntimeRequest {
    #[serde(default = "default_protocol_version")]
    pub(crate) protocol_version: u32,
    #[serde(default)]
    pub(crate) request_id: Option<String>,
    #[serde(rename = "cmd")]
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) actor: Option<RuntimeActorRef>,
    #[serde(default)]
    pub(crate) timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) idempotency_key: Option<String>,
    #[serde(flatten)]
    pub(crate) payload: serde_json::Map<String, serde_json::Value>,
}

impl RuntimeRequest {
    #[must_use]
    pub(crate) fn kind(&self) -> RuntimeCommandKind {
        RuntimeCommandKind::from_wire(&self.command)
    }

    #[must_use]
    pub(crate) fn is_supported_version(&self) -> bool {
        self.protocol_version == RUNTIME_PROTOCOL_VERSION
    }

    #[must_use]
    pub(crate) fn session_id_str(&self) -> Option<&str> {
        self.session_id
            .as_deref()
            .or_else(|| {
                self.payload
                    .get("session_id")
                    .and_then(|value| value.as_str())
            })
            .filter(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeErrorKind {
    InvalidRequest,
    UnsupportedProtocol,
    NotFound,
    Rejected,
    Timeout,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeErrorEnvelope {
    pub(crate) kind: RuntimeErrorKind,
    pub(crate) message: String,
    pub(crate) retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RuntimeResponse {
    pub(crate) ok: bool,
    #[serde(default = "default_protocol_version")]
    pub(crate) protocol_version: u32,
    #[serde(default)]
    pub(crate) request_id: Option<String>,
    #[serde(default)]
    pub(crate) event_sequence: Option<usize>,
    #[serde(default)]
    pub(crate) payload: serde_json::Value,
    #[serde(default)]
    pub(crate) error: Option<RuntimeErrorEnvelope>,
}

impl RuntimeResponse {
    #[must_use]
    pub(crate) fn ok(request: &RuntimeRequest, payload: serde_json::Value) -> Self {
        Self {
            ok: true,
            protocol_version: RUNTIME_PROTOCOL_VERSION,
            request_id: request.request_id.clone(),
            event_sequence: None,
            payload,
            error: None,
        }
    }

    #[must_use]
    pub(crate) fn error(
        request_id: Option<String>,
        kind: RuntimeErrorKind,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            ok: false,
            protocol_version: RUNTIME_PROTOCOL_VERSION,
            request_id,
            event_sequence: None,
            payload: serde_json::Value::Null,
            error: Some(RuntimeErrorEnvelope {
                kind,
                message: message.into(),
                retryable,
            }),
        }
    }

    #[must_use]
    pub(crate) fn unsupported_protocol(request: &RuntimeRequest) -> Self {
        Self::error(
            request.request_id.clone(),
            RuntimeErrorKind::UnsupportedProtocol,
            format!(
                "unsupported runtime protocol version {}; expected {}",
                request.protocol_version, RUNTIME_PROTOCOL_VERSION
            ),
            false,
        )
    }
}

fn default_protocol_version() -> u32 {
    RUNTIME_PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_protocol_serializes_status_request() {
        let request = RuntimeRequest {
            protocol_version: RUNTIME_PROTOCOL_VERSION,
            request_id: Some("req-1".to_string()),
            command: RuntimeCommandKind::RuntimeStatus.as_wire().to_string(),
            session_id: None,
            actor: Some(RuntimeActorRef {
                id: "tui-1".to_string(),
                surface: Some("tui".to_string()),
            }),
            timeout_ms: Some(800),
            idempotency_key: None,
            payload: serde_json::Map::new(),
        };

        let value = serde_json::to_value(&request).expect("request serializes");
        assert_eq!(value["protocol_version"], 1);
        assert_eq!(value["request_id"], "req-1");
        assert_eq!(value["cmd"], "runtime.status");
        assert_eq!(value["actor"]["surface"], "tui");
    }

    #[test]
    fn runtime_protocol_rejects_unknown_version() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "protocol_version": 999,
            "request_id": "req-bad",
            "cmd": "runtime.status"
        }))
        .expect("request parses");

        let response = RuntimeResponse::unsupported_protocol(&request);
        assert!(!response.ok);
        assert_eq!(response.request_id.as_deref(), Some("req-bad"));
        assert_eq!(
            response.error.as_ref().map(|error| &error.kind),
            Some(&RuntimeErrorKind::UnsupportedProtocol)
        );
    }

    #[test]
    fn runtime_response_carries_request_id() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "request_id": "req-42",
            "cmd": "list_sessions"
        }))
        .expect("request parses");

        let response = RuntimeResponse::ok(&request, serde_json::json!({"sessions": []}));
        assert!(response.ok);
        assert_eq!(response.request_id.as_deref(), Some("req-42"));
        assert_eq!(response.payload["sessions"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn socket_control_and_http_projection_share_session_id_schema() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "cmd": "session.ensure",
            "session_id": "session-a",
            "model": "test-model"
        }))
        .expect("request parses");

        assert_eq!(request.kind(), RuntimeCommandKind::SessionEnsure);
        assert_eq!(request.session_id_str(), Some("session-a"));
    }

    #[test]
    fn session_lifecycle_commands_have_stable_wire_aliases() {
        assert_eq!(
            RuntimeCommandKind::from_wire("attach_session"),
            RuntimeCommandKind::SessionAttach
        );
        assert_eq!(
            RuntimeCommandKind::from_wire("session.detach").as_wire(),
            "session.detach"
        );
        assert_eq!(
            RuntimeCommandKind::from_wire("session.replay"),
            RuntimeCommandKind::SessionReplay
        );
    }
}
