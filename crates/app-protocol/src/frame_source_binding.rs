use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const HEADER_FRAME_BINDING_V1: &str = "x-cowd-frame-binding";
pub const SESSION_PUBLIC_PAYLOAD_V1: &str = "session-public-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppFrameSourceBindingRequestV1 {
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub root_task_id: String,
    pub lineage_generation: String,
    pub frame_nonce: String,
    pub frame_binding_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppFrameSourceBindingV1 {
    pub frame_binding_ref: String,
    pub client_id: String,
    pub client_expires_at_ms: u64,
    pub receipt_protocol: String,
    pub presence_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "selection", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionTerminalReceiptReadInputV1 {
    BoundSourceTerminal {},
    Message { message_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionTerminalReceiptAckInputV1 {
    pub message_id: String,
    pub message_sequence: String,
    pub runtime_commit_cursor: String,
    pub content_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FrameTerminalSaveV1 {
    pub session_id: String,
    pub message_id: String,
    pub client_id: String,
    pub message_sequence: String,
    pub runtime_commit_cursor: String,
    pub content_digest: String,
    pub payload_kind: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FrameTerminalSavedV1 {
    pub session_id: String,
    pub message_id: String,
    pub client_id: String,
    pub message_sequence: String,
    pub runtime_commit_cursor: String,
    pub content_digest: String,
    pub payload_kind: String,
}

impl FrameTerminalSaveV1 {
    pub fn validate_exact(&self) -> Result<(), String> {
        for value in [&self.message_sequence, &self.runtime_commit_cursor] {
            if value.starts_with('0') || value.parse::<i64>().ok().is_none_or(|number| number <= 0)
            {
                return Err("invalid decimal receipt identity".into());
            }
        }
        let prefix = "session-public-v1:sha256:";
        if self.payload_kind != SESSION_PUBLIC_PAYLOAD_V1
            || self.payload_json.len() > 8 * 1024 * 1024
            || !self.content_digest.starts_with(prefix)
            || self.content_digest.len() != prefix.len() + 64
            || !self.content_digest[prefix.len()..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || [&self.session_id, &self.message_id, &self.client_id]
                .iter()
                .any(|value| {
                    value.is_empty() || value.len() > 256 || value.chars().any(char::is_control)
                })
        {
            return Err("invalid public receipt payload".into());
        }
        Ok(())
    }
}
