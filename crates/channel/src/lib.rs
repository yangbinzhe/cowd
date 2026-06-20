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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelCapability {
    pub id: String,
    pub channel: String,
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelContract {
    pub channel: String,
    pub required_fields: Vec<String>,
    pub capabilities: Vec<ChannelCapability>,
}

impl ChannelContract {
    #[must_use]
    pub fn for_channel(channel: impl AsRef<str>) -> Self {
        let channel = normalize_channel(channel.as_ref());
        let required_fields = channel_required_fields(&channel)
            .into_iter()
            .map(str::to_string)
            .collect();
        let capabilities = channel_operations(&channel)
            .into_iter()
            .map(|operation| ChannelCapability::new(&channel, operation))
            .collect();
        Self {
            channel,
            required_fields,
            capabilities,
        }
    }

    #[must_use]
    pub fn operation_names(&self) -> Vec<String> {
        self.capabilities
            .iter()
            .map(|capability| capability.operation.clone())
            .collect()
    }
}

impl ChannelCapability {
    #[must_use]
    pub fn new(channel: impl AsRef<str>, operation: impl AsRef<str>) -> Self {
        let channel = normalize_channel(channel.as_ref());
        let operation = operation.as_ref().to_string();
        Self {
            id: format!("channel.{channel}.{operation}"),
            channel,
            operation,
        }
    }
}

#[must_use]
pub fn normalize_channel(channel: &str) -> String {
    match channel.trim().to_ascii_lowercase().as_str() {
        "wechat_ilink" | "wechat" => "wechat-ilink".to_string(),
        other => other.to_string(),
    }
}

#[must_use]
pub fn channel_required_fields(channel: &str) -> Vec<&'static str> {
    match normalize_channel(channel).as_str() {
        "feishu" => vec!["app_id", "app_secret"],
        "wecom" => vec!["corp_id", "corp_secret", "agent_id"],
        "wechat-ilink" => Vec::new(),
        "email" => vec!["smtp_server", "username", "password"],
        _ => Vec::new(),
    }
}

#[must_use]
pub fn channel_operations(channel: &str) -> Vec<&'static str> {
    match normalize_channel(channel).as_str() {
        "feishu" => vec!["send_text", "send_image", "send_file", "doc_ops"],
        "wecom" => vec!["send_text", "callback"],
        "wechat-ilink" => vec!["qr_login", "send_text"],
        "email" => vec!["send_email"],
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_contract_normalizes_wechat_ilink() {
        let contract = ChannelContract::for_channel("wechat_ilink");

        assert_eq!(contract.channel, "wechat-ilink");
        assert!(contract.required_fields.is_empty());
        assert_eq!(
            contract.operation_names(),
            vec!["qr_login".to_string(), "send_text".to_string()]
        );
        assert_eq!(contract.capabilities[0].id, "channel.wechat-ilink.qr_login");
    }
}
