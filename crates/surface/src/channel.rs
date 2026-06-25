use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelKind {
    InteractiveSurface,
    ExternalIntegration,
    AutomationEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelId(String);

impl ChannelId {
    #[must_use]
    pub fn new(channel: impl AsRef<str>) -> Self {
        Self(normalize_channel(channel.as_ref()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelSurface {
    pub id: ChannelId,
    pub kind: ChannelKind,
    pub supports_ingress: bool,
    pub supports_delivery: bool,
    pub supports_subscription: bool,
}

impl ChannelSurface {
    #[must_use]
    pub fn interactive(id: impl AsRef<str>) -> Self {
        Self {
            id: ChannelId::new(id),
            kind: ChannelKind::InteractiveSurface,
            supports_ingress: true,
            supports_delivery: true,
            supports_subscription: true,
        }
    }

    #[must_use]
    pub fn integration(id: impl AsRef<str>) -> Self {
        Self {
            id: ChannelId::new(id),
            kind: ChannelKind::ExternalIntegration,
            supports_ingress: true,
            supports_delivery: true,
            supports_subscription: false,
        }
    }
}

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
pub struct ChannelEnvelope<T> {
    pub channel: String,
    pub actor: Option<String>,
    pub thread: Option<String>,
    pub run_id: Option<String>,
    pub payload: T,
    pub metadata: Value,
}

impl<T> ChannelEnvelope<T> {
    #[must_use]
    pub fn new(channel: impl AsRef<str>, payload: T) -> Self {
        Self {
            channel: normalize_channel(channel.as_ref()),
            actor: None,
            thread: None,
            run_id: None,
            payload,
            metadata: Value::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryPolicy {
    OriginOnly,
    Subscribers,
    ExplicitTargets,
    OriginAndSubscribers,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryTarget {
    pub channel: String,
    pub recipient: String,
    pub thread: Option<String>,
}

impl DeliveryTarget {
    #[must_use]
    pub fn new(channel: impl AsRef<str>, recipient: impl Into<String>) -> Self {
        Self {
            channel: normalize_channel(channel.as_ref()),
            recipient: recipient.into(),
            thread: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionPolicy {
    pub allow_cross_surface_watch: bool,
    pub allow_external_channel_watch: bool,
}

impl Default for SubscriptionPolicy {
    fn default() -> Self {
        Self {
            allow_cross_surface_watch: true,
            allow_external_channel_watch: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VisibilityPolicy {
    PrivateOrigin,
    SameActor,
    SameWorkspace,
    ExplicitSubscribers,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelCapability {
    pub id: String,
    pub channel: String,
    pub capability: String,
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
        let capabilities = channel_transport_capabilities(&channel)
            .into_iter()
            .map(|capability| ChannelCapability::new(&channel, capability))
            .collect();
        Self {
            channel,
            required_fields,
            capabilities,
        }
    }

    #[must_use]
    pub fn capability_names(&self) -> Vec<String> {
        self.capabilities
            .iter()
            .map(|capability| capability.capability.clone())
            .collect()
    }
}

impl ChannelCapability {
    #[must_use]
    pub fn new(channel: impl AsRef<str>, capability: impl AsRef<str>) -> Self {
        let channel = normalize_channel(channel.as_ref());
        let capability = capability.as_ref().to_string();
        Self {
            id: format!("channel.{channel}.{capability}"),
            channel,
            capability,
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
pub fn channel_transport_capabilities(channel: &str) -> Vec<&'static str> {
    match normalize_channel(channel).as_str() {
        "feishu" | "wecom" | "wechat-ilink" | "email" => vec!["ingress", "delivery"],
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
            contract.capability_names(),
            vec!["ingress".to_string(), "delivery".to_string()]
        );
        assert_eq!(contract.capabilities[0].id, "channel.wechat-ilink.ingress");
    }

    #[test]
    fn channel_surface_models_ui_and_external_channels() {
        let tui = ChannelSurface::interactive("tui");
        assert_eq!(tui.id.as_str(), "tui");
        assert_eq!(tui.kind, ChannelKind::InteractiveSurface);
        assert!(tui.supports_subscription);

        let feishu = ChannelSurface::integration("feishu");
        assert_eq!(feishu.kind, ChannelKind::ExternalIntegration);
        assert!(!feishu.supports_subscription);
    }

    #[test]
    fn delivery_target_normalizes_channel_names() {
        let target = DeliveryTarget::new("wechat_ilink", "user-1");
        assert_eq!(target.channel, "wechat-ilink");
        assert_eq!(target.recipient, "user-1");
    }
}
