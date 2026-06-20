//! Common types shared across platform adapters.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Platform type enumeration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Platform {
    Feishu,
    WeChat,
    Email,
    /// Custom platform identified by name.
    Custom(String),
}

impl Platform {
    /// Get the platform name as a string.
    pub fn name(&self) -> &str {
        match self {
            Platform::Feishu => "feishu",
            Platform::WeChat => "wecom",
            Platform::Email => "email",
            Platform::Custom(name) => name.as_str(),
        }
    }

    /// Parse a platform from a string.
    pub fn parse(s: &str) -> Self {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "feishu" | "lark" => Platform::Feishu,
            "wecom" | "wechat" => Platform::WeChat,
            "email" | "mail" => Platform::Email,
            other => Platform::Custom(other.to_string()),
        }
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Message type classification for inbound messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MessageType {
    Text,
    Photo,
    Video,
    Audio,
    Voice,
    Document,
    Sticker,
    Command,
    Location,
}

impl Default for MessageType {
    fn default() -> Self {
        MessageType::Text
    }
}

/// Result of a message send operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendResult {
    pub success: bool,
    pub message_id: Option<String>,
    pub error: Option<String>,
}

impl SendResult {
    #[must_use]
    pub fn success(message_id: Option<String>) -> Self {
        Self {
            success: true,
            message_id,
            error: None,
        }
    }

    #[must_use]
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            message_id: None,
            error: Some(error.into()),
        }
    }
}

/// Typed payload kind for runtime dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundPayloadKind {
    Text,
    Image,
    File,
}

impl OutboundPayloadKind {
    #[must_use]
    pub fn operation(self) -> &'static str {
        match self {
            Self::Text => "send_text",
            Self::Image => "send_image",
            Self::File => "send_file",
        }
    }
}

/// Basic chat/group information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatInfo {
    pub chat_id: String,
    pub name: String,
    pub chat_type: String,
}

/// Platform event received from an external platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformEvent {
    pub event_type: String,
    pub platform: Platform,
    pub data: serde_json::Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Unique identifier for a platform session.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionKey {
    /// The platform identifier (e.g., "feishu", "wecom", "email").
    pub platform: String,
    /// The user/session identifier on that platform.
    pub user_id: String,
    /// Optional conversation thread identifier.
    pub thread_id: Option<String>,
}

impl SessionKey {
    /// Create a new session key.
    pub fn new(platform: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            platform: platform.into(),
            user_id: user_id.into(),
            thread_id: None,
        }
    }

    /// Create a session key with a thread ID.
    pub fn with_thread(
        platform: impl Into<String>,
        user_id: impl Into<String>,
        thread_id: impl Into<String>,
    ) -> Self {
        Self {
            platform: platform.into(),
            user_id: user_id.into(),
            thread_id: Some(thread_id.into()),
        }
    }

    /// Convert to a string representation for logging/debugging.
    pub fn as_str(&self) -> String {
        match &self.thread_id {
            Some(thread) => format!("{}:{}:{}", self.platform, self.user_id, thread),
            None => format!("{}:{}", self.platform, self.user_id),
        }
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for SessionKey {
    fn from(s: &str) -> Self {
        let parts: Vec<&str> = s.split(':').collect();
        match parts.len() {
            0 => Self::new("unknown", "unknown"),
            1 => Self::new(parts[0], "unknown"),
            2 => Self::new(parts[0], parts[1]),
            _ => Self::with_thread(parts[0], parts[1], parts[2]),
        }
    }
}

/// Session metadata associated with a platform session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformSession {
    /// The session key.
    pub key: SessionKey,
    /// Session creation timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last activity timestamp.
    pub last_activity: chrono::DateTime<chrono::Utc>,
    /// Number of messages exchanged.
    pub message_count: usize,
    /// Optional user display name.
    pub display_name: Option<String>,
}

impl PlatformSession {
    /// Create a new platform session.
    pub fn new(key: SessionKey) -> Self {
        let now = chrono::Utc::now();
        Self {
            key,
            created_at: now,
            last_activity: now,
            message_count: 0,
            display_name: None,
        }
    }

    /// Update the last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = chrono::Utc::now();
        self.message_count += 1;
    }

    /// Set the display name.
    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }
}
