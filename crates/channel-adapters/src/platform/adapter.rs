//! Platform adapter trait and message types.

use crate::platform::types::SessionKey;
use async_trait::async_trait;
use thiserror::Error;

// Re-export types for backward compatibility.
pub use crate::platform::types::{
    ChatInfo, MessageType, OutboundPayloadKind, Platform, PlatformEvent, SendResult,
};

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

    #[error("not implemented: {0}")]
    NotImplemented(String),
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
    /// Message type classification.
    pub message_type: MessageType,
    /// Platform-specific message identifier.
    pub message_id: Option<String>,
    /// ID of the message this is replying to.
    pub reply_to_message_id: Option<String>,
    /// URLs of media attachments.
    pub media_urls: Vec<String>,
    /// MIME types of media attachments.
    pub media_types: Vec<String>,
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

/// Typed outbound dispatch request.
#[derive(Debug, Clone)]
pub struct OutboundDispatch {
    pub session_key: SessionKey,
    pub kind: OutboundPayloadKind,
    pub payload_ref: String,
    pub caption: Option<String>,
    pub file_name: Option<String>,
    pub reply_to: Option<String>,
    pub metadata: serde_json::Value,
}

impl OutboundDispatch {
    #[must_use]
    pub fn text(msg: OutboundMessage) -> Self {
        Self {
            session_key: msg.session_key,
            kind: OutboundPayloadKind::Text,
            payload_ref: msg.text,
            caption: None,
            file_name: None,
            reply_to: msg.reply_to,
            metadata: msg.metadata,
        }
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
    async fn send(&self, msg: &OutboundMessage) -> Result<SendResult, PlatformError>;

    /// Send a typing indicator to the chat.
    #[allow(unused_variables)]
    async fn send_typing(&self, chat_id: &str) -> Result<(), PlatformError> {
        Err(PlatformError::NotImplemented("send_typing".into()))
    }

    /// Send an image from a URL.
    #[allow(unused_variables)]
    async fn send_image(
        &self,
        chat_id: &str,
        image_url: &str,
        caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_image".into()))
    }

    /// Send an image from a local file path.
    #[allow(unused_variables)]
    async fn send_image_file(
        &self,
        chat_id: &str,
        image_path: &str,
        caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_image_file".into()))
    }

    /// Send a voice message from an audio file.
    #[allow(unused_variables)]
    async fn send_voice(
        &self,
        chat_id: &str,
        audio_path: &str,
        caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_voice".into()))
    }

    /// Send a document/file.
    #[allow(unused_variables)]
    async fn send_document(
        &self,
        chat_id: &str,
        file_path: &str,
        file_name: Option<&str>,
        caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_document".into()))
    }

    /// Send a video message.
    #[allow(unused_variables)]
    async fn send_video(
        &self,
        chat_id: &str,
        video_path: &str,
        caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_video".into()))
    }

    /// Send an animation/GIF from a URL.
    #[allow(unused_variables)]
    async fn send_animation(
        &self,
        chat_id: &str,
        animation_url: &str,
        caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_animation".into()))
    }

    /// Edit an existing message.
    #[allow(unused_variables)]
    async fn edit_message(
        &self,
        chat_id: &str,
        message_id: &str,
        content: &str,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("edit_message".into()))
    }

    /// Delete an existing message.
    #[allow(unused_variables)]
    async fn delete_message(&self, chat_id: &str, message_id: &str) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("delete_message".into()))
    }

    /// Get information about a chat/group.
    #[allow(unused_variables)]
    async fn get_chat_info(&self, chat_id: &str) -> PlatformResult<ChatInfo> {
        Err(PlatformError::NotImplemented("get_chat_info".into()))
    }

    /// Send an interactive card message. Returns the message ID.
    #[allow(unused_variables)]
    async fn send_card(&self, chat_id: &str, card_json: &str) -> PlatformResult<String> {
        Err(PlatformError::NotImplemented("send_card".into()))
    }

    /// Handle an incoming platform event. Returns an optional InboundMessage.
    #[allow(unused_variables)]
    async fn on_event(&self, event: &PlatformEvent) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
    }
}

/// Result type alias for platform operations.
pub type PlatformResult<T> = Result<T, PlatformError>;

/// A no-op adapter used as a placeholder.
pub struct NullAdapter;

#[async_trait]
impl PlatformAdapter for NullAdapter {
    fn platform(&self) -> Platform {
        Platform::Custom("null".to_string())
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

    async fn send(&self, _msg: &OutboundMessage) -> PlatformResult<SendResult> {
        Ok(SendResult::success(None))
    }

    async fn send_typing(&self, _chat_id: &str) -> Result<(), PlatformError> {
        Ok(())
    }

    async fn send_image(
        &self,
        _chat_id: &str,
        _image_url: &str,
        _caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_image".into()))
    }

    async fn send_image_file(
        &self,
        _chat_id: &str,
        _image_path: &str,
        _caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_image_file".into()))
    }

    async fn send_voice(
        &self,
        _chat_id: &str,
        _audio_path: &str,
        _caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_voice".into()))
    }

    async fn send_document(
        &self,
        _chat_id: &str,
        _file_path: &str,
        _file_name: Option<&str>,
        _caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_document".into()))
    }

    async fn send_video(
        &self,
        _chat_id: &str,
        _video_path: &str,
        _caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_video".into()))
    }

    async fn send_animation(
        &self,
        _chat_id: &str,
        _animation_url: &str,
        _caption: Option<&str>,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("send_animation".into()))
    }

    async fn edit_message(
        &self,
        _chat_id: &str,
        _message_id: &str,
        _content: &str,
    ) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("edit_message".into()))
    }

    async fn delete_message(&self, _chat_id: &str, _message_id: &str) -> PlatformResult<()> {
        Err(PlatformError::NotImplemented("delete_message".into()))
    }

    async fn get_chat_info(&self, _chat_id: &str) -> PlatformResult<ChatInfo> {
        Ok(ChatInfo {
            chat_id: _chat_id.to_string(),
            name: "Null Chat".into(),
            chat_type: "null".into(),
        })
    }

    async fn send_card(&self, _chat_id: &str, _card_json: &str) -> PlatformResult<String> {
        Err(PlatformError::NotImplemented("send_card".into()))
    }

    async fn on_event(&self, _event: &PlatformEvent) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
    }
}
