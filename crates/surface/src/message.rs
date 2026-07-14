use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MessageEndpointKind {
    Direct,
    Group,
    Thread,
    Mailbox,
    Account,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MessageResourceMode {
    Text,
    Image,
    Voice,
    Document,
    Video,
    Card,
    Event,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageConnectorCapability {
    pub id: String,
    pub connector: String,
    pub capability: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageConnectorContract {
    pub connector: String,
    pub required_fields: Vec<String>,
    pub capabilities: Vec<MessageConnectorCapability>,
    pub endpoint_kinds: Vec<MessageEndpointKind>,
    pub resource_modes: Vec<MessageResourceMode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageConnectorDescriptor {
    pub descriptor_version: u32,
    pub connector: String,
    pub display_name: String,
    pub message_contract: MessageConnectorContract,
    pub markdown_dialect: String,
    pub max_message_length: usize,
    pub supports_threads: bool,
    pub supports_attachments: bool,
    pub supported_actions: Vec<String>,
    pub status: String,
    pub reload_required: bool,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MessageActionKind {
    SendText,
    SendImage,
    SendVoice,
    SendDocument,
    SendVideo,
    SendCard,
    Edit,
    Delete,
    ChatInfo,
    CallbackDispatch,
    ProcessingComplete,
    ProcessingFailed,
}

impl MessageActionKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SendText => "message.send.text",
            Self::SendImage => "message.send.image",
            Self::SendVoice => "message.send.voice",
            Self::SendDocument => "message.send.document",
            Self::SendVideo => "message.send.video",
            Self::SendCard => "message.send.card",
            Self::Edit => "message.edit",
            Self::Delete => "message.delete",
            Self::ChatInfo => "message.chat.info",
            Self::CallbackDispatch => "message.callback.dispatch",
            Self::ProcessingComplete => "message.processing.complete",
            Self::ProcessingFailed => "message.processing.failed",
        }
    }

    #[must_use]
    pub fn parse(action: &str) -> Option<Self> {
        match action {
            "message.send.text" => Some(Self::SendText),
            "message.send.image" => Some(Self::SendImage),
            "message.send.voice" => Some(Self::SendVoice),
            "message.send.document" => Some(Self::SendDocument),
            "message.send.video" => Some(Self::SendVideo),
            "message.send.card" => Some(Self::SendCard),
            "message.edit" => Some(Self::Edit),
            "message.delete" => Some(Self::Delete),
            "message.chat.info" => Some(Self::ChatInfo),
            "message.callback.dispatch" => Some(Self::CallbackDispatch),
            "message.processing.complete" => Some(Self::ProcessingComplete),
            "message.processing.failed" => Some(Self::ProcessingFailed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageActionRequest {
    pub action: MessageActionKind,
    pub recipient: String,
    pub thread: Option<String>,
    pub payload_ref: Option<String>,
    pub text: Option<String>,
    pub caption: Option<String>,
    pub file_name: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl MessageConnectorContract {
    #[must_use]
    pub fn for_connector(connector: impl AsRef<str>) -> Self {
        let connector = normalize_message_connector(connector.as_ref());
        let required_fields = message_connector_required_fields(&connector)
            .into_iter()
            .map(str::to_string)
            .collect();
        let capabilities = message_connector_capabilities(&connector)
            .into_iter()
            .map(|capability| MessageConnectorCapability::new(&connector, capability))
            .collect();
        Self {
            endpoint_kinds: message_connector_endpoint_kinds(&connector),
            resource_modes: message_connector_resource_modes(&connector),
            connector,
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

impl MessageConnectorDescriptor {
    #[must_use]
    pub fn for_connector(connector: impl AsRef<str>, status: impl Into<String>) -> Self {
        let contract = MessageConnectorContract::for_connector(connector);
        let status = status.into();
        let degraded_reasons = if matches!(status.as_str(), "ready" | "connected") {
            Vec::new()
        } else {
            vec![format!("connector status is {status}")]
        };
        Self {
            descriptor_version: 1,
            display_name: message_connector_display_name(&contract.connector).to_string(),
            markdown_dialect: message_connector_markdown_dialect(&contract.connector).to_string(),
            max_message_length: message_connector_max_message_length(&contract.connector),
            supports_threads: contract
                .endpoint_kinds
                .contains(&MessageEndpointKind::Thread),
            supports_attachments: contract.resource_modes.iter().any(|mode| {
                !matches!(mode, MessageResourceMode::Text | MessageResourceMode::Event)
            }),
            supported_actions: contract.capability_names(),
            connector: contract.connector.clone(),
            message_contract: contract,
            status,
            reload_required: false,
            degraded_reasons,
        }
    }

    #[must_use]
    pub fn with_reload_required(mut self, reload_required: bool) -> Self {
        self.reload_required = reload_required;
        if reload_required {
            self.degraded_reasons
                .push("manifest or runtime configuration changed; reload required".to_string());
        }
        self
    }
}

impl MessageConnectorCapability {
    #[must_use]
    pub fn new(connector: impl AsRef<str>, capability: impl AsRef<str>) -> Self {
        let connector = normalize_message_connector(connector.as_ref());
        let capability = capability.as_ref().to_string();
        Self {
            id: format!("message.{connector}.{capability}"),
            connector,
            capability,
        }
    }
}

#[must_use]
pub fn normalize_message_connector(connector: &str) -> String {
    match connector.trim().to_ascii_lowercase().as_str() {
        "lark" => "feishu".to_string(),
        "wechat" | "wechat_ilink" => "wechat-ilink".to_string(),
        other => other.to_string(),
    }
}

#[must_use]
pub fn message_connector_required_fields(connector: &str) -> Vec<&'static str> {
    match normalize_message_connector(connector).as_str() {
        "feishu" => vec!["app_id", "app_secret"],
        "wecom" => vec!["corp_id", "corp_secret", "agent_id"],
        "wechat-ilink" => vec!["bot_id", "bot_secret"],
        "email" => vec!["smtp_host", "smtp_user", "smtp_password"],
        _ => Vec::new(),
    }
}

#[must_use]
pub fn message_connector_capabilities(connector: &str) -> Vec<&'static str> {
    match normalize_message_connector(connector).as_str() {
        "feishu" => vec![
            "message.ingress",
            "message.send.text",
            "message.send.image",
            "message.send.voice",
            "message.send.document",
            "message.send.video",
            "message.send.card",
            "message.edit",
            "message.delete",
            "message.chat.info",
            "message.callback",
        ],
        "wechat-ilink" => vec![
            "message.ingress",
            "message.send.text",
            "message.send.image",
            "message.chat.info",
        ],
        "wecom" => vec!["message.ingress", "message.send.text", "message.callback"],
        "email" => vec![
            "message.ingress",
            "message.send.text",
            "message.send.document",
        ],
        _ => Vec::new(),
    }
}

#[must_use]
pub fn message_connector_endpoint_kinds(connector: &str) -> Vec<MessageEndpointKind> {
    match normalize_message_connector(connector).as_str() {
        "feishu" | "wechat-ilink" | "wecom" => vec![
            MessageEndpointKind::Direct,
            MessageEndpointKind::Group,
            MessageEndpointKind::Thread,
            MessageEndpointKind::Account,
        ],
        "email" => vec![MessageEndpointKind::Mailbox, MessageEndpointKind::Thread],
        _ => Vec::new(),
    }
}

#[must_use]
pub fn message_connector_resource_modes(connector: &str) -> Vec<MessageResourceMode> {
    match normalize_message_connector(connector).as_str() {
        "feishu" => vec![
            MessageResourceMode::Text,
            MessageResourceMode::Image,
            MessageResourceMode::Voice,
            MessageResourceMode::Document,
            MessageResourceMode::Video,
            MessageResourceMode::Card,
            MessageResourceMode::Event,
        ],
        "wechat-ilink" => vec![
            MessageResourceMode::Text,
            MessageResourceMode::Image,
            MessageResourceMode::Event,
        ],
        "wecom" => vec![MessageResourceMode::Text, MessageResourceMode::Event],
        "email" => vec![
            MessageResourceMode::Text,
            MessageResourceMode::Document,
            MessageResourceMode::Event,
        ],
        _ => Vec::new(),
    }
}

#[must_use]
pub fn message_connector_display_name(connector: &str) -> &'static str {
    match normalize_message_connector(connector).as_str() {
        "feishu" => "Feishu/Lark",
        "wechat-ilink" => "WeChat iLink",
        "wecom" => "WeCom",
        "email" => "Email",
        _ => "Message Connector",
    }
}

#[must_use]
pub fn message_connector_markdown_dialect(connector: &str) -> &'static str {
    match normalize_message_connector(connector).as_str() {
        "feishu" => "feishu-post-markdown",
        "email" => "commonmark",
        _ => "plain-text-with-links",
    }
}

#[must_use]
pub fn message_connector_max_message_length(connector: &str) -> usize {
    match normalize_message_connector(connector).as_str() {
        "feishu" => 20_000,
        "wechat-ilink" | "wecom" => 4_096,
        "email" => 250_000,
        _ => 8_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_connector_contract_normalizes_wechat_ilink() {
        let contract = MessageConnectorContract::for_connector("wechat_ilink");

        assert_eq!(contract.connector, "wechat-ilink");
        assert_eq!(contract.required_fields, vec!["bot_id", "bot_secret"]);
        assert!(contract
            .capability_names()
            .contains(&"message.send.text".to_string()));
        assert_eq!(
            contract.capabilities[0].id,
            "message.wechat-ilink.message.ingress"
        );
    }

    #[test]
    fn message_connector_contract_models_feishu_rich_message_capabilities() {
        let contract = MessageConnectorContract::for_connector("lark");

        assert_eq!(contract.connector, "feishu");
        assert!(contract
            .capability_names()
            .contains(&"message.send.card".to_string()));
        assert!(contract
            .endpoint_kinds
            .contains(&MessageEndpointKind::Group));
        assert!(contract
            .resource_modes
            .contains(&MessageResourceMode::Video));
    }

    #[test]
    fn message_descriptor_exposes_runtime_capability_status() {
        let descriptor = MessageConnectorDescriptor::for_connector("lark", "degraded")
            .with_reload_required(true);

        assert_eq!(descriptor.descriptor_version, 1);
        assert_eq!(descriptor.connector, "feishu");
        assert_eq!(descriptor.display_name, "Feishu/Lark");
        assert_eq!(descriptor.markdown_dialect, "feishu-post-markdown");
        assert_eq!(descriptor.max_message_length, 20_000);
        assert!(descriptor.supports_threads);
        assert!(descriptor.supports_attachments);
        assert!(descriptor.reload_required);
        assert!(descriptor
            .supported_actions
            .contains(&"message.send.image".to_string()));
        assert!(!descriptor.degraded_reasons.is_empty());
    }

    #[test]
    fn email_required_fields_match_current_config_template() {
        let contract = MessageConnectorContract::for_connector("email");

        assert_eq!(
            contract.required_fields,
            vec!["smtp_host", "smtp_user", "smtp_password"]
        );
        assert!(contract
            .resource_modes
            .contains(&MessageResourceMode::Document));
    }

    #[test]
    fn message_action_kind_uses_terminal_names() {
        assert_eq!(
            MessageActionKind::CallbackDispatch.as_str(),
            "message.callback.dispatch"
        );
        assert_eq!(
            MessageActionKind::parse("message.processing.complete"),
            Some(MessageActionKind::ProcessingComplete)
        );
        assert_eq!(MessageActionKind::parse("callback.dispatch"), None);
    }
}
