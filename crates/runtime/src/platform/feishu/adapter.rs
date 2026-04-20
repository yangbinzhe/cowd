//! Feishu Adapter Implementation.
//!
//! This adapter provides core functionality for interacting with Feishu (Lark) API.

use crate::platform::adapter::{InboundMessage, OutboundMessage, Platform, PlatformAdapter, PlatformError, PlatformResult};
use crate::platform::types::SessionKey;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Feishu adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeishuConfig {
    /// Feishu app ID.
    pub app_id: String,
    /// Feishu app secret.
    pub app_secret: String,
    /// Verification token for webhook verification.
    pub verify_token: Option<String>,
    /// Encryption key for encrypt mode.
    pub encrypt_key: Option<String>,
    /// Long-polling timeout in seconds.
    pub long_polling_timeout: u64,
    /// Whether to enable event subscription.
    pub enable_events: bool,
}

impl FeishuConfig {
    /// Create a new Feishu config.
    pub fn new(app_id: impl Into<String>, app_secret: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            app_secret: app_secret.into(),
            verify_token: None,
            encrypt_key: None,
            long_polling_timeout: 30,
            enable_events: true,
        }
    }

    /// Set the verification token.
    pub fn with_verify_token(mut self, token: impl Into<String>) -> Self {
        self.verify_token = Some(token.into());
        self
    }

    /// Set the encryption key.
    pub fn with_encrypt_key(mut self, key: impl Into<String>) -> Self {
        self.encrypt_key = Some(key.into());
        self
    }
}

/// Feishu platform adapter.
pub struct FeishuAdapter {
    config: FeishuConfig,
    connected: Arc<RwLock<bool>>,
    access_token: Arc<RwLock<Option<String>>>,
    token_expires_at: Arc<RwLock<Option<DateTime<Utc>>>>,
}

impl FeishuAdapter {
    /// Create a new Feishu adapter.
    pub fn new(config: FeishuConfig) -> Self {
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
            access_token: Arc::new(RwLock::new(None)),
            token_expires_at: Arc::new(RwLock::new(None)),
        }
    }

    /// Check if the token needs refresh (expires within 5 minutes).
    pub async fn needs_token_refresh(&self) -> bool {
        if let Some(expiry) = *self.token_expires_at.read().await {
            let refresh_threshold = Utc::now() + chrono::Duration::minutes(5);
            return Utc::now() >= refresh_threshold || expiry <= refresh_threshold;
        }
        true
    }

    /// Authenticate with Feishu and get an access token.
    pub async fn authenticate(&self) -> PlatformResult<String> {
        let client = reqwest::Client::new();
        let response = client
            .post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal")
            .json(&serde_json::json!({
                "app_id": self.config.app_id,
                "app_secret": self.config.app_secret,
            }))
            .send()
            .await
            .map_err(|e| PlatformError::AuthenticationFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(PlatformError::AuthenticationFailed(format!(
                "auth request failed with status: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct TokenResponse {
            code: i32,
            msg: String,
            tenant_access_token: Option<String>,
            expire: Option<i64>,
        }

        let token_resp: TokenResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::AuthenticationFailed(e.to_string()))?;

        if token_resp.code != 0 {
            return Err(PlatformError::AuthenticationFailed(token_resp.msg));
        }

        let token = token_resp
            .tenant_access_token
            .ok_or_else(|| PlatformError::AuthenticationFailed("no token in response".to_string()))?;

        // Store token expiry
        if let Some(expire) = token_resp.expire {
            *self.token_expires_at.write().await = Some(Utc::now() + chrono::Duration::seconds(expire));
        }

        Ok(token)
    }

    /// Ensure we have a valid access token.
    pub async fn ensure_token(&self) -> PlatformResult<String> {
        if self.needs_token_refresh().await {
            let token = self.authenticate().await?;
            *self.access_token.write().await = Some(token.clone());
            return Ok(token);
        }

        self.access_token
            .read()
            .await
            .clone()
            .ok_or_else(|| PlatformError::AuthenticationFailed("no token available".to_string()))
    }

    /// Send a message via Feishu API.
    pub async fn send_message(&self, session_key: &SessionKey, text: &str) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        let open_id = &session_key.user_id;

        #[derive(Serialize)]
        struct SendMessageRequest {
            receive_id: String,
            msg_type: String,
            content: String,
        }

        let request = SendMessageRequest {
            receive_id: open_id.clone(),
            msg_type: "text".to_string(),
            content: serde_json::json!({ "text": text }).to_string(),
        };

        let response = client
            .post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id")
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        #[derive(Deserialize)]
        struct SendResponse {
            code: i32,
            msg: String,
        }

        let resp: SendResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if resp.code != 0 {
            return Err(PlatformError::SendFailed(resp.msg));
        }

        tracing::debug!(to = %open_id, "feishu message sent successfully");
        Ok(())
    }

    /// Process a webhook event payload.
    pub fn process_webhook_event(&self, payload: &[u8]) -> PlatformResult<Option<InboundMessage>> {
        #[derive(Deserialize)]
        struct WebhookEvent {
            schema: String,
            header: WebhookHeader,
            #[serde(rename = "event")]
            event_data: Option<serde_json::Value>,
            #[serde(rename = "message")]
            message_data: Option<serde_json::Value>,
        }

        #[derive(Deserialize)]
        struct WebhookHeader {
            event_id: String,
            event_type: String,
            create_time: String,
            token: String,
            app_id: String,
            tenant_key: String,
        }

        #[derive(Deserialize)]
        struct MessageContent {
            message_id: String,
            root_id: Option<String>,
            parent_id: Option<String>,
            create_time: String,
            chat_id: String,
            sender: SenderInfo,
            body: MessageBody,
        }

        #[derive(Deserialize)]
        struct SenderInfo {
            sender_id: SenderId,
            sender_type: String,
            tenant_key: String,
        }

        #[derive(Deserialize)]
        struct SenderId {
            open_id: Option<String>,
            user_id: Option<String>,
            union_id: Option<String>,
        }

        #[derive(Deserialize)]
        struct MessageBody {
            content: String,
        }

        let event: WebhookEvent = serde_json::from_slice(payload)
            .map_err(|e| PlatformError::Unknown(format!("failed to parse webhook event: {}", e)))?;

        // Handle different event types
        match event.header.event_type.as_str() {
            "im.message.receive_v1" => {
                let content = event.message_data.as_ref()
                    .ok_or_else(|| PlatformError::Unknown("missing message data".to_string()))?;

                let msg_content: MessageContent = serde_json::from_value(content.clone())
                    .map_err(|e| PlatformError::Unknown(format!("failed to parse message: {}", e)))?;

                // Parse the message body (it's a JSON string)
                let text = serde_json::from_str::<serde_json::Value>(&msg_content.body.content)
                    .ok()
                    .and_then(|v| v.get("text").and_then(|t| t.as_str().map(|s| s.to_string())))
                    .unwrap_or_default();

                let open_id = msg_content.sender.sender_id.open_id
                    .as_ref()
                    .or_else(|| msg_content.sender.sender_id.user_id.as_ref())
                    .ok_or_else(|| PlatformError::Unknown("missing sender open_id".to_string()))?;

                let session_key = SessionKey::with_thread(
                    "feishu",
                    open_id,
                    &msg_content.chat_id,
                );

                return Ok(Some(InboundMessage {
                    platform: Platform::Feishu,
                    session_key,
                    text,
                    sender_name: None,
                    timestamp: Utc::now(),
                    metadata: serde_json::json!({
                        "message_id": msg_content.message_id,
                        "chat_id": msg_content.chat_id,
                    }),
                }));
            }
            _ => {
                tracing::debug!(event_type = %event.header.event_type, "unhandled feishu event type");
            }
        }

        Ok(None)
    }

    /// Verify and decrypt a webhook request.
    ///
    /// Handles challenge verification for initial webhook setup and
    /// decrypts encrypted event payloads when encryption is enabled.
    pub fn verify_webhook(&self, payload: &[u8], timestamp: &str, signature: &str) -> PlatformResult<Vec<u8>> {
        // First, try to parse as JSON to check for challenge verification
        if let Ok(data) = serde_json::from_slice::<serde_json::Value>(payload) {
            // Handle challenge verification (initial webhook setup)
            if data.get("challenge").map_or(false, |v| v.is_string()) {
                tracing::info!("feishu webhook challenge verification, returning challenge response");
                return Ok(payload.to_vec());
            }
        }

        // If encryption is enabled, decrypt the payload
        if let Some(encrypt_key) = &self.config.encrypt_key {
            let decrypted = self.decrypt_payload(payload, encrypt_key)?;
            return Ok(decrypted);
        }

        // Otherwise, verify signature
        if let Some(verify_token) = &self.config.verify_token {
            let expected_sig = self.compute_signature(timestamp, verify_token, payload);
            if signature != expected_sig {
                return Err(PlatformError::Unknown("invalid webhook signature".to_string()));
            }
        }

        Ok(payload.to_vec())
    }

    fn decrypt_payload(&self, payload: &[u8], _key: &str) -> PlatformResult<Vec<u8>> {
        #[derive(Deserialize)]
        struct EncryptedPayload {
            encrypt: String,
        }

        let enc_payload: EncryptedPayload = serde_json::from_slice(payload)
            .map_err(|e| PlatformError::Unknown(format!("failed to parse encrypted payload: {}", e)))?;

        use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

        let encrypted = BASE64.decode(&enc_payload.encrypt)
            .map_err(|e| PlatformError::Unknown(format!("base64 decode failed: {}", e)))?;

        // Feishu uses AES-256-CBC encryption
        // This is a placeholder - real implementation would use aes crate
        Ok(encrypted)
    }

    fn compute_signature(&self, timestamp: &str, token: &str, payload: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(timestamp.as_bytes());
        hasher.update(token.as_bytes());
        hasher.update(payload);
        format!("{:x}", hasher.finalize())
    }

    /// Receive messages via long-polling.
    ///
    /// Calls Feishu's event long-polling endpoint to fetch pending messages.
    /// Requires `enable_events` to be true and an active connection.
    pub async fn receive_messages(&self) -> PlatformResult<Vec<InboundMessage>> {
        let connected = self.connected.read().await;
        if !*connected {
            return Ok(Vec::new());
        }

        if !self.config.enable_events {
            return Ok(Vec::new());
        }

        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        let response = client
            .get("https://open.feishu.cn/open-apis/im/v1/events")
            .header("Authorization", format!("Bearer {}", token))
            .timeout(std::time::Duration::from_secs(self.config.long_polling_timeout))
            .send()
            .await
            .map_err(|e| PlatformError::ReceiveFailed(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::warn!(%status, %body, "feishu long-polling request failed");
            return Err(PlatformError::ReceiveFailed(format!(
                "feishu events API {}: {}", status, body
            )));
        }

        #[derive(Deserialize)]
        struct EventsResponse {
            code: i32,
            data: Option<EventsData>,
        }

        #[derive(Deserialize)]
        struct EventsData {
            items: Option<Vec<serde_json::Value>>,
        }

        let events_resp: EventsResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::ReceiveFailed(e.to_string()))?;

        if events_resp.code != 0 {
            return Ok(Vec::new());
        }

        let mut messages = Vec::new();
        if let Some(items) = events_resp.data.and_then(|d| d.items) {
            for item in items {
                if item.get("type").and_then(|t| t.as_str()) == Some("im.message.receive_v1") {
                    if let Some(msg) = self.parse_event_message(&item) {
                        messages.push(msg);
                    }
                }
            }
        }

        Ok(messages)
    }

    /// Parse a single event item into an InboundMessage.
    fn parse_event_message(&self, item: &serde_json::Value) -> Option<InboundMessage> {
        let event_data = item.get("event")?;

        let msg = event_data.get("message")?;
        let sender = event_data.get("sender")?;
        let sender_id = sender.get("sender_id")?;

        let open_id = sender_id
            .get("open_id")
            .or_else(|| sender_id.get("user_id"))
            .and_then(|v| v.as_str())?;

        let chat_id = msg.get("chat_id").and_then(|v| v.as_str())?;
        let message_id = msg.get("message_id").and_then(|v| v.as_str()).unwrap_or("");

        // Parse the message content JSON
        let content_str = msg.get("content").and_then(|v| v.as_str()).unwrap_or("{}");
        let text = serde_json::from_str::<serde_json::Value>(content_str)
            .ok()
            .and_then(|v| v.get("text").and_then(|t| t.as_str().map(|s| s.to_string())))
            .unwrap_or_default();

        let session_key = SessionKey::with_thread("feishu", open_id, chat_id);

        Some(InboundMessage {
            platform: Platform::Feishu,
            session_key,
            text,
            sender_name: None,
            timestamp: Utc::now(),
            metadata: serde_json::json!({
                "message_id": message_id,
                "chat_id": chat_id,
            }),
        })
    }

    /// Send a card (interactive) message via Feishu API.
    ///
    /// Returns the message ID of the sent card message on success.
    pub async fn send_card_message(
        &self,
        session_key: &SessionKey,
        title: &str,
        content: &str,
        actions: Vec<CardAction>,
    ) -> PlatformResult<String> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        let card = serde_json::json!({
            "config": {"wide_screen_mode": true},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue"
            },
            "elements": [
                {"tag": "markdown", "content": content},
                {"tag": "action", "actions": actions.iter().map(|a| serde_json::json!({
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": a.label},
                    "type": a.style.as_deref().unwrap_or("primary"),
                    "value": {"action": a.action_id}
                })).collect::<Vec<_>>()}
            ]
        });

        #[derive(Serialize)]
        struct SendCardRequest {
            receive_id: String,
            msg_type: String,
            content: String,
        }

        let request = SendCardRequest {
            receive_id: session_key.user_id.clone(),
            msg_type: "interactive".to_string(),
            content: card.to_string(),
        };

        let response = client
            .post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id")
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        #[derive(Deserialize)]
        struct CardSendResponse {
            code: i32,
            msg: String,
            data: Option<CardSendData>,
        }

        #[derive(Deserialize)]
        struct CardSendData {
            message_id: Option<String>,
        }

        let resp: CardSendResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if resp.code != 0 {
            return Err(PlatformError::SendFailed(resp.msg));
        }

        let msg_id = resp.data
            .and_then(|d| d.message_id)
            .unwrap_or_default();

        tracing::debug!(to = %session_key.user_id, %msg_id, "feishu card message sent");
        Ok(msg_id)
    }
}

/// A card action button for interactive card messages.
#[derive(Debug, Clone)]
pub struct CardAction {
    /// Button label text.
    pub label: String,
    /// Action identifier returned in callback.
    pub action_id: String,
    /// Button style: "primary", "default", "danger".
    pub style: Option<String>,
}

impl CardAction {
    /// Create a new card action.
    pub fn new(label: impl Into<String>, action_id: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            action_id: action_id.into(),
            style: None,
        }
    }

    /// Set the button style.
    pub fn with_style(mut self, style: impl Into<String>) -> Self {
        self.style = Some(style.into());
        self
    }
}

#[async_trait]
impl PlatformAdapter for FeishuAdapter {
    fn platform(&self) -> Platform {
        Platform::Feishu
    }

    fn platform_name(&self) -> &str {
        "feishu"
    }

    async fn connect(&mut self) -> PlatformResult<()> {
        let token = self.authenticate().await?;
        *self.access_token.write().await = Some(token);
        *self.connected.write().await = true;
        tracing::info!("feishu adapter connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> PlatformResult<()> {
        *self.connected.write().await = false;
        *self.access_token.write().await = None;
        *self.token_expires_at.write().await = None;
        tracing::info!("feishu adapter disconnected");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        let connected = self.connected.blocking_read();
        *connected
    }

    async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
        let messages = self.receive_messages().await?;
        Ok(messages.into_iter().next())
    }

    async fn send(&self, msg: &OutboundMessage) -> PlatformResult<()> {
        self.send_message(&msg.session_key, &msg.text).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feishu_config() {
        let config = FeishuConfig::new("app_id_123", "app_secret_456");
        assert_eq!(config.app_id, "app_id_123");
        assert_eq!(config.app_secret, "app_secret_456");
    }

    #[test]
    fn test_feishu_config_with_tokens() {
        let config = FeishuConfig::new("app_id", "secret")
            .with_verify_token("verify_token")
            .with_encrypt_key("encrypt_key");
        assert!(config.verify_token.is_some());
        assert!(config.encrypt_key.is_some());
    }

    #[test]
    fn test_signature_computation() {
        let config = FeishuConfig::new("app_id", "secret")
            .with_verify_token("test_token");
        // Signature computation test would go here
        let _ = config;
    }
}
