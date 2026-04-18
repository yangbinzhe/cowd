//! Platform adapter trait and message types.

use crate::platform::types::SessionKey;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Errors that can occur during platform operations.
#[derive(Error, Debug)]
pub enum PlatformError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("message send failed: {0}")]
    SendFailed(String),

    #[error("message receive failed: {0}")]
    ReceiveFailed(String),

    #[error("session not found: {0}")]
    SessionNotFound(String),

    #[error("rate limited: {0}")]
    RateLimited(String),

    #[error("configuration error: {0}")]
    ConfigError(String),

    #[error("unknown platform error: {0}")]
    Unknown(String),
}

/// Inbound message from a platform.
#[derive(Debug, Clone)]
pub struct InboundMessage {
    /// The platform this message came from.
    pub platform: Platform,
    /// The session this message belongs to.
    pub session_key: SessionKey,
    /// The text content of the message.
    pub text: String,
    /// Optional sender display name.
    pub sender_name: Option<String>,
    /// Message timestamp.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Additional metadata.
    pub metadata: serde_json::Value,
}

/// Outbound message to a platform.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// The target session.
    pub session_key: SessionKey,
    /// The text content to send.
    pub text: String,
    /// Optional message ID for threading.
    pub reply_to: Option<String>,
    /// Additional metadata.
    pub metadata: serde_json::Value,
}

/// Platform type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Platform {
    Feishu,
    WeChat,
    Email,
    /// Custom platform identified by name.
    Custom(&'static str),
}

impl Platform {
    /// Get the platform name as a string.
    pub fn name(&self) -> &str {
        match self {
            Platform::Feishu => "feishu",
            Platform::WeChat => "wecom",
            Platform::Email => "email",
            Platform::Custom(name) => name,
        }
    }

    /// Parse a platform from a string.
    pub fn parse(s: &str) -> Self {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "feishu" | "lark" => Platform::Feishu,
            "wecom" | "wechat" => Platform::WeChat,
            "email" | "mail" => Platform::Email,
            other => Platform::Custom(Box::leak(other.to_string().into_boxed_str())),
        }
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Trait for platform adapters.
///
/// Implement this trait to add support for new platforms.
#[async_trait]
pub trait PlatformAdapter: Send + Sync {
    /// Get the platform type.
    fn platform(&self) -> Platform;

    /// Get the platform name for logging.
    fn platform_name(&self) -> &str;

    /// Initialize and connect to the platform.
    async fn connect(&mut self) -> Result<(), PlatformError>;

    /// Disconnect from the platform.
    async fn disconnect(&mut self) -> Result<(), PlatformError>;

    /// Check if connected.
    fn is_connected(&self) -> bool;

    /// Receive the next inbound message.
    ///
    /// Returns `None` if there are no messages pending (non-blocking).
    /// Returns an error if the receive operation fails.
    async fn receive(&mut self) -> Result<Option<InboundMessage>, PlatformError>;

    /// Send an outbound message.
    async fn send(&self, msg: &OutboundMessage) -> Result<(), PlatformError>;
}

/// Result type alias for platform operations.
pub type PlatformResult<T> = Result<T, PlatformError>;

/// A no-op adapter used as a placeholder.
pub struct NullAdapter;

#[async_trait]
impl PlatformAdapter for NullAdapter {
    fn platform(&self) -> Platform {
        Platform::Custom("null")
    }

    fn platform_name(&self) -> &str {
        "null"
    }

    async fn connect(&mut self) -> PlatformResult<()> {
        Ok(())
    }

    async fn disconnect(&mut self) -> PlatformResult<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        true
    }

    async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
    }

    async fn send(&self, _msg: &OutboundMessage) -> PlatformResult<()> {
        Ok(())
    }
}
