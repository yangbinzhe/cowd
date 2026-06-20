use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelMessageId(String);

impl ChannelMessageId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("channel-message-{}", Uuid::new_v4()))
    }
}

impl Default for ChannelMessageId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundChannelMessage {
    pub id: ChannelMessageId,
    pub channel: String,
    pub sender: String,
    pub thread: Option<String>,
    pub text: String,
    pub received_at: DateTime<Utc>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundChannelMessage {
    pub channel: String,
    pub recipient: String,
    pub thread: Option<String>,
    pub text: String,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelCapability {
    pub id: String,
    pub channel: String,
    pub operation: String,
}
