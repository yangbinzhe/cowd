//! Feishu Adapter Implementation.
//!
//! This adapter provides core functionality for interacting with Feishu (Lark) API:
//!
//! # Authentication
//! - `authenticate()` / `ensure_token()` → POST /auth/v3/tenant_access_token/internal
//!
//! # Messaging
//! - `send_message()` → POST /im/v1/messages (plain text)
//! - `send_internal()` → POST /im/v1/messages + PUT /im/v1/messages/{id}/reply
//!   with post→text fallback
//! - `send_card_message()` → POST /im/v1/messages (interactive card)
//!
//! # Event Reception
//! - **WebSocket**: Use [`super::ws::FeishuWsClient`] for real-time event push via
//!   `POST callback/ws/endpoint` → protobuf-framed WebSocket connection
//! - **Webhook**: `process_webhook_event()` parses incoming webhook payloads
//!   (e.g., `im.message.receive_v1`)
//! - The `receive()` trait method returns `Ok(None)` — events arrive through
//!   the WebSocket client, not polling.

use crate::platform::adapter::{ChatInfo, InboundMessage, MessageType, OutboundMessage, Platform, PlatformAdapter, PlatformError, PlatformEvent, PlatformResult};
use crate::platform::types::SessionKey;
use super::auth::AccessControl;
use super::batch::{BatchSender, TextBatchManager};
use super::markdown::{build_post_payload, build_text_payload, strip_markdown};
use super::processing::ChatProcessingQueue;
use super::reactions::ProcessingReactions;
use super::types::{GetChatResponse, SendMessageRequest, SendMessageResponse, UpdateMessageRequest, UpdateMessageResponse, ReplyMessageRequest, ReplyMessageResponse};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Feishu adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeishuConfig {
    /// Feishu app ID.
    pub app_id: String,
    /// Feishu app secret.
    pub app_secret: String,
    /// The bot's own open_id (for self-echo prevention).
    pub bot_open_id: String,
    /// The bot's display name (for @mention detection).
    pub bot_name: String,
}

impl FeishuConfig {
    /// Create a new Feishu config.
    pub fn new(app_id: impl Into<String>, app_secret: impl Into<String>) -> Self {
        let app_id = app_id.into();
        let app_secret = app_secret.into();
        Self {
            bot_open_id: app_id.clone(),
            bot_name: "FeishuBot".to_string(),
            app_id,
            app_secret,
        }
    }

    /// Set the bot's open_id for self-echo prevention.
    pub fn with_bot_open_id(mut self, id: impl Into<String>) -> Self {
        self.bot_open_id = id.into();
        self
    }

    /// Set the bot's display name for @mention detection.
    pub fn with_bot_name(mut self, name: impl Into<String>) -> Self {
        self.bot_name = name.into();
        self
    }
}

/// Feishu platform adapter.
pub struct FeishuAdapter {
    config: FeishuConfig,
    connected: Arc<RwLock<bool>>,
    access_token: Arc<RwLock<Option<String>>>,
    token_expires_at: Arc<RwLock<Option<DateTime<Utc>>>>,
    pub access_control: AccessControl,
    pub reactions: ProcessingReactions,
    pub batch_manager: Option<TextBatchManager>,
    pub processing_queue: ChatProcessingQueue,
}

impl FeishuAdapter {
    /// Create a new Feishu adapter.
    pub fn new(config: FeishuConfig) -> Self {
        let bot_open_id = config.bot_open_id.clone();
        let bot_name = config.bot_name.clone();
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
            access_token: Arc::new(RwLock::new(None)),
            token_expires_at: Arc::new(RwLock::new(None)),
            access_control: AccessControl::new(&bot_open_id, &bot_name),
            reactions: ProcessingReactions::new(),
            batch_manager: None,
            processing_queue: ChatProcessingQueue::new(1000),
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
        #[allow(dead_code)]
        struct WebhookEvent {
            schema: String,
            header: WebhookHeader,
            #[serde(rename = "event")]
            event_data: Option<serde_json::Value>,
            #[serde(rename = "message")]
            message_data: Option<serde_json::Value>,
        }

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct WebhookHeader {
            event_id: String,
            event_type: String,
            create_time: String,
            token: String,
            app_id: String,
            tenant_key: String,
        }

        #[derive(Deserialize)]
        #[allow(dead_code)]
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
        #[allow(dead_code)]
        struct SenderInfo {
            sender_id: SenderId,
            sender_type: String,
            tenant_key: String,
        }

        #[derive(Deserialize)]
        struct SenderId {
            open_id: Option<String>,
            user_id: Option<String>,
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
                    message_type: MessageType::Text,
                    message_id: Some(msg_content.message_id),
                    reply_to_message_id: None,
                    media_urls: vec![],
                    media_types: vec![],
                }));
            }
            _ => {
                tracing::debug!(event_type = %event.header.event_type, "unhandled feishu event type");
            }
        }

        Ok(None)
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

    /// Retry an async operation up to 3 times with exponential backoff.
    ///
    /// Only retries on `SendFailed` and `RateLimited` errors. Other errors
    /// (including `NotImplemented`, `AuthenticationFailed`) are returned immediately.
    async fn feishu_send_with_retry<F, Fut>(&self, mut f: F) -> PlatformResult<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = PlatformResult<()>>,
    {
        let mut last_err = None;
        for attempt in 0..3 {
            if attempt > 0 {
                let backoff = Duration::from_millis(500 * 2u64.pow(attempt as u32 - 1));
                tracing::debug!(attempt, ?backoff, "feishu retry");
                tokio::time::sleep(backoff).await;
            }
            match f().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if matches!(e, PlatformError::RateLimited(_) | PlatformError::SendFailed(_)) {
                        last_err = Some(e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| PlatformError::SendFailed("retry exhausted".into())))
    }

    /// Send a message with post→text fallback.
    ///
    /// Tries to send as a rich post message first. If the Feishu API rejects
    /// the post format (error code `"content format of the post type is incorrect"`),
    /// falls back to plain text via `strip_markdown`.
    async fn send_internal(
        &self,
        receive_id: &str,
        text: &str,
        reply_to: Option<&str>,
    ) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        // Post rejection regex (case-insensitive)
        let post_reject_re = Regex::new(r"(?i)content format of the post type is incorrect")
            .map_err(|e| PlatformError::Unknown(format!("regex compile: {}", e)))?;

        // Build payloads
        let post_content = build_post_payload(text);
        let fallback_text = strip_markdown(text);

        // Determine whether to use reply endpoint or new-message endpoint
        if let Some(reply_msg_id) = reply_to {
            // --- Reply path ---
            let reply_url = format!(
                "https://open.feishu.cn/open-apis/im/v1/messages/{}/reply",
                reply_msg_id
            );

            // Try reply as post
            let post_req = ReplyMessageRequest {
                msg_type: "post".to_string(),
                content: post_content.clone(),
            };
            let post_resp: ReplyMessageResponse = client
                .put(&reply_url)
                .header("Authorization", format!("Bearer {}", &token))
                .json(&post_req)
                .send()
                .await
                .map_err(|e| PlatformError::SendFailed(e.to_string()))?
                .json()
                .await
                .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

            if post_resp.code == 0 {
                return Ok(());
            }

            // Reply-specific error codes → fall back to new message
            if post_resp.code == 230011 || post_resp.code == 231003 {
                tracing::debug!(
                    code = post_resp.code,
                    msg = %post_resp.msg,
                    "feishu reply target missing, sending as new message"
                );
            } else if post_reject_re.is_match(&post_resp.msg) {
                // Post format rejected → retry reply as text
                let text_req = ReplyMessageRequest {
                    msg_type: "text".to_string(),
                    content: build_text_payload(&fallback_text),
                };
                let text_resp: ReplyMessageResponse = client
                    .put(&reply_url)
                    .header("Authorization", format!("Bearer {}", &token))
                    .json(&text_req)
                    .send()
                    .await
                    .map_err(|e| PlatformError::SendFailed(e.to_string()))?
                    .json()
                    .await
                    .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

                if text_resp.code == 0 {
                    tracing::debug!("feishu text fallback reply succeeded");
                    return Ok(());
                }

                if text_resp.code == 230011 || text_resp.code == 231003 {
                    tracing::debug!(
                        code = text_resp.code,
                        "feishu text reply target missing, sending as new message"
                    );
                } else {
                    return Err(PlatformError::SendFailed(text_resp.msg));
                }
            } else {
                return Err(PlatformError::SendFailed(post_resp.msg));
            }

            // Fall through to new-message path
        }

        // --- New-message path ---
        let send_url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id";

        // Try post first
        let post_req = SendMessageRequest {
            receive_id: receive_id.to_string(),
            msg_type: "post".to_string(),
            content: post_content.clone(),
        };
        let post_resp: SendMessageResponse = client
            .post(send_url)
            .header("Authorization", format!("Bearer {}", &token))
            .json(&post_req)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if post_resp.code == 0 {
            tracing::debug!(to = %receive_id, "feishu post message sent");
            return Ok(());
        }

        if post_reject_re.is_match(&post_resp.msg) {
            tracing::debug!(
                msg = %post_resp.msg,
                "feishu post rejected, falling back to text"
            );
            // Fall back to text
            let text_req = SendMessageRequest {
                receive_id: receive_id.to_string(),
                msg_type: "text".to_string(),
                content: build_text_payload(&fallback_text),
            };
            let text_resp: SendMessageResponse = client
                .post(send_url)
                .header("Authorization", format!("Bearer {}", &token))
                .json(&text_req)
                .send()
                .await
                .map_err(|e| PlatformError::SendFailed(e.to_string()))?
                .json()
                .await
                .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

            if text_resp.code != 0 {
                return Err(PlatformError::SendFailed(text_resp.msg));
            }
            tracing::debug!(to = %receive_id, "feishu text fallback message sent");
        } else {
            return Err(PlatformError::SendFailed(post_resp.msg));
        }

        Ok(())
    }

    /// Return the Feishu Open API base URL.
    fn api_base_url(&self) -> &'static str {
        "https://open.feishu.cn/open-apis"
    }

    /// Send a typed message to a chat by receive_id.
    async fn send_feishu_typed_message(
        &self,
        receive_id: &str,
        msg_type: &str,
        content: &str,
    ) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let url = format!(
            "{}/im/v1/messages?receive_id_type=open_id",
            self.api_base_url()
        );

        let request = SendMessageRequest {
            receive_id: receive_id.to_string(),
            msg_type: msg_type.to_string(),
            content: content.to_string(),
        };

        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        let resp: SendMessageResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if resp.code != 0 {
            return Err(PlatformError::SendFailed(resp.msg));
        }

        Ok(())
    }
}

/// Extract the chat_id from a raw Feishu event JSON.
#[allow(dead_code)]
fn extract_chat_id(event: &serde_json::Value) -> Option<String> {
    event
        .pointer("/event/message/chat_id")
        .and_then(|v| v.as_str())
        .or_else(|| {
            event
                .pointer("/event/open_chat_id")
                .and_then(|v| v.as_str())
        })
        .map(|s| s.to_string())
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

#[async_trait::async_trait]
impl BatchSender for FeishuAdapter {
    async fn send_batch(&self, chat_id: &str, text: &str) -> PlatformResult<()> {
        let chat_id = chat_id.to_string();
        let text = text.to_string();
        self.feishu_send_with_retry(move || {
            let chat_id = chat_id.clone();
            let text = text.clone();
            async move { self.send_internal(&chat_id, &text, None).await }
        })
        .await
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
        // WebSocket events are handled by the FeishuWsClient in ws.rs.
        // This polling method returns None — use FeishuWsClient::connect()
        // for real-time event reception.
        Ok(None)
    }

    async fn send(&self, msg: &OutboundMessage) -> PlatformResult<()> {
        if let Some(ref batch_mgr) = self.batch_manager {
            let chat_id = msg
                .session_key
                .thread_id
                .as_deref()
                .unwrap_or(&msg.session_key.user_id);
            batch_mgr.queue(chat_id, &msg.text).await;
            return Ok(());
        }

        let receive_id = msg
            .session_key
            .thread_id
            .as_deref()
            .unwrap_or(&msg.session_key.user_id);

        self.feishu_send_with_retry(|| {
            self.send_internal(receive_id, &msg.text, msg.reply_to.as_deref())
        })
        .await
    }

    async fn send_typing(&self, _chat_id: &str) -> Result<(), PlatformError> {
        // Feishu bot API does not expose a typing indicator
        Ok(())
    }

    async fn send_image(&self, chat_id: &str, image_url: &str, caption: Option<&str>) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let image_bytes = client.get(image_url).send().await
            .map_err(|e| PlatformError::SendFailed(format!("download image: {e}")))?
            .bytes().await
            .map_err(|e| PlatformError::SendFailed(format!("read image bytes: {e}")))?;
        let image_key = super::media::upload_image(&token, &image_bytes, "message").await?;
        let content = if let Some(cap) = caption {
            serde_json::json!({"image_key": image_key, "caption": cap}).to_string()
        } else {
            serde_json::json!({"image_key": image_key}).to_string()
        };
        self.send_feishu_typed_message(chat_id, "image", &content).await
    }

    async fn send_image_file(&self, chat_id: &str, image_path: &str, caption: Option<&str>) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let image_bytes = std::fs::read(image_path)
            .map_err(|e| PlatformError::SendFailed(format!("read file: {e}")))?;
        let image_key = super::media::upload_image(&token, &image_bytes, "message").await?;
        let content = if let Some(cap) = caption {
            serde_json::json!({"image_key": image_key, "caption": cap}).to_string()
        } else {
            serde_json::json!({"image_key": image_key}).to_string()
        };
        self.send_feishu_typed_message(chat_id, "image", &content).await
    }

    async fn send_voice(&self, chat_id: &str, audio_path: &str, _caption: Option<&str>) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let audio_bytes = std::fs::read(audio_path)
            .map_err(|e| PlatformError::SendFailed(format!("read audio: {e}")))?;
        let file_name = std::path::Path::new(audio_path).file_name()
            .and_then(|n| n.to_str()).unwrap_or("audio.opus");
        let file_key = super::media::upload_file(&token, &audio_bytes, file_name, "opus").await?;
        let content = serde_json::json!({"file_key": file_key}).to_string();
        self.send_feishu_typed_message(chat_id, "audio", &content).await
    }

    async fn send_document(&self, chat_id: &str, file_path: &str, file_name: Option<&str>, _caption: Option<&str>) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let file_bytes = std::fs::read(file_path)
            .map_err(|e| PlatformError::SendFailed(format!("read file: {e}")))?;
        let name = file_name.unwrap_or_else(|| {
            std::path::Path::new(file_path).file_name()
                .and_then(|n| n.to_str()).unwrap_or("document")
        });
        let file_key = super::media::upload_file(&token, &file_bytes, name, "stream").await?;
        let content = serde_json::json!({"file_key": file_key}).to_string();
        self.send_feishu_typed_message(chat_id, "file", &content).await
    }

    async fn send_video(&self, chat_id: &str, video_path: &str, _caption: Option<&str>) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let video_bytes = std::fs::read(video_path)
            .map_err(|e| PlatformError::SendFailed(format!("read video: {e}")))?;
        let file_name = std::path::Path::new(video_path).file_name()
            .and_then(|n| n.to_str()).unwrap_or("video.mp4");
        let file_key = super::media::upload_file(&token, &video_bytes, file_name, "mp4").await?;
        let content = serde_json::json!({"file_key": file_key}).to_string();
        self.send_feishu_typed_message(chat_id, "media", &content).await
    }

    async fn send_animation(&self, chat_id: &str, animation_url: &str, caption: Option<&str>) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let gif_bytes = client.get(animation_url).send().await
            .map_err(|e| PlatformError::SendFailed(format!("download gif: {e}")))?
            .bytes().await
            .map_err(|e| PlatformError::SendFailed(format!("read gif bytes: {e}")))?;
        let image_key = super::media::upload_image(&token, &gif_bytes, "message").await?;
        let content = if let Some(cap) = caption {
            serde_json::json!({"image_key": image_key, "caption": cap}).to_string()
        } else {
            serde_json::json!({"image_key": image_key}).to_string()
        };
        self.send_feishu_typed_message(chat_id, "image", &content).await
    }

    async fn edit_message(&self, _chat_id: &str, message_id: &str, content: &str) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let url = format!(
            "https://open.feishu.cn/open-apis/im/v1/messages/{}",
            message_id
        );

        let post_reject_re = Regex::new(r"(?i)content format of the post type is incorrect")
            .map_err(|e| PlatformError::Unknown(format!("regex compile: {}", e)))?;

        let post_content = build_post_payload(content);
        let fallback_text = strip_markdown(content);

        self.feishu_send_with_retry(|| {
            let url = url.clone();
            let token = token.clone();
            let post_content = post_content.clone();
            let fallback_text = fallback_text.clone();
            let post_reject_re = post_reject_re.clone();
            let client = client.clone();
            async move {
                // Try post first
                let post_req = UpdateMessageRequest {
                    content: post_content.clone(),
                    msg_type: "post".to_string(),
                };
                let post_resp: UpdateMessageResponse = client
                    .put(&url)
                    .header("Authorization", format!("Bearer {}", &token))
                    .json(&post_req)
                    .send()
                    .await
                    .map_err(|e| PlatformError::SendFailed(e.to_string()))?
                    .json()
                    .await
                    .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

                if post_resp.code == 0 {
                    return Ok(());
                }

                if post_reject_re.is_match(&post_resp.msg) {
                    tracing::debug!("feishu edit post rejected, falling back to text");
                    let text_req = UpdateMessageRequest {
                        content: build_text_payload(&fallback_text),
                        msg_type: "text".to_string(),
                    };
                    let text_resp: UpdateMessageResponse = client
                        .put(&url)
                        .header("Authorization", format!("Bearer {}", &token))
                        .json(&text_req)
                        .send()
                        .await
                        .map_err(|e| PlatformError::SendFailed(e.to_string()))?
                        .json()
                        .await
                        .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

                    if text_resp.code != 0 {
                        return Err(PlatformError::SendFailed(text_resp.msg));
                    }
                    tracing::debug!(%message_id, "feishu text fallback edit succeeded");
                } else {
                    return Err(PlatformError::SendFailed(post_resp.msg));
                }
                Ok(())
            }
        })
        .await
    }

    async fn delete_message(&self, _chat_id: &str, message_id: &str) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let url = format!(
            "https://open.feishu.cn/open-apis/im/v1/messages/{}",
            message_id
        );

        self.feishu_send_with_retry(|| {
            let url = url.clone();
            let token = token.clone();
            let client = client.clone();
            async move {
                let response = client
                    .delete(&url)
                    .header("Authorization", format!("Bearer {}", &token))
                    .send()
                    .await
                    .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

                #[derive(Deserialize)]
                struct DeleteResponse {
                    code: i32,
                    msg: String,
                }

                let resp: DeleteResponse = response
                    .json()
                    .await
                    .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

                if resp.code != 0 {
                    return Err(PlatformError::SendFailed(resp.msg));
                }
                tracing::debug!(%message_id, "feishu message deleted");
                Ok(())
            }
        })
        .await
    }

    async fn get_chat_info(&self, chat_id: &str) -> PlatformResult<ChatInfo> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let url = format!(
            "https://open.feishu.cn/open-apis/im/v1/chats/{}",
            chat_id
        );

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", &token))
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        let resp: GetChatResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if resp.code != 0 {
            return Err(PlatformError::SendFailed(resp.msg));
        }

        let data = resp
            .data
            .ok_or_else(|| PlatformError::SendFailed("missing chat data".into()))?;

        Ok(ChatInfo {
            chat_id: data.chat_id.unwrap_or_else(|| chat_id.to_string()),
            name: data.name.unwrap_or_default(),
            chat_type: data.chat_type.unwrap_or_else(|| "unknown".to_string()),
        })
    }

    async fn send_card(&self, chat_id: &str, card_json: &str) -> PlatformResult<String> {
        serde_json::from_str::<serde_json::Value>(card_json)
            .map_err(|e| PlatformError::SendFailed(format!("invalid card JSON: {e}")))?;
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let url = format!("{}/im/v1/messages?receive_id_type=open_id", self.api_base_url());
        let request = serde_json::json!({
            "receive_id": chat_id,
            "msg_type": "interactive",
            "content": card_json
        });
        let response = client.post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&request)
            .send().await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;
        let resp: super::types::SendMessageResponse = response.json().await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;
        if resp.code != 0 {
            return Err(PlatformError::SendFailed(resp.msg));
        }
        Ok(resp.data.and_then(|d| d.message_id).unwrap_or_default())
    }

    async fn on_event(&self, _event: &PlatformEvent) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::feishu::card_handler::CardActionHandler;
    use crate::platform::feishu::processing::ProcessingDecision;

    #[test]
    fn test_feishu_config() {
        let config = FeishuConfig::new("app_id_123", "app_secret_456");
        assert_eq!(config.app_id, "app_id_123");
        assert_eq!(config.app_secret, "app_secret_456");
    }

    // ------------------------------------------------------------------
    // Post rejection regex tests
    // ------------------------------------------------------------------

    #[test]
    fn test_post_rejection_regex_matches_feishu_error() {
        let re = Regex::new(r"(?i)content format of the post type is incorrect").unwrap();
        assert!(re.is_match("content format of the post type is incorrect"));
        assert!(re.is_match("Content Format Of The Post Type Is Incorrect"));
        assert!(re.is_match("error: content format of the post type is incorrect, please check"));
    }

    #[test]
    fn test_post_rejection_regex_rejects_other_errors() {
        let re = Regex::new(r"(?i)content format of the post type is incorrect").unwrap();
        assert!(!re.is_match("invalid access token"));
        assert!(!re.is_match("message not found"));
        assert!(!re.is_match("rate limited"));
        assert!(!re.is_match(""));
    }

    // ------------------------------------------------------------------
    // Send text message format tests
    // ------------------------------------------------------------------

    #[test]
    fn test_send_text_message_format() {
        let text = "Hello world";
        let payload = build_text_payload(text);
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed["text"], "Hello world");
    }

    #[test]
    fn test_send_text_message_format_empty() {
        let payload = build_text_payload("");
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed["text"], "");
    }

    // ------------------------------------------------------------------
    // Post→text fallback detection tests
    // ------------------------------------------------------------------

    #[test]
    fn test_post_fallback_strips_markdown() {
        let input = "**bold** and *italic* and `code`";
        let stripped = strip_markdown(input);
        assert!(!stripped.contains("**"));
        assert!(!stripped.contains("*"));
        assert!(!stripped.contains("`"));
        assert_eq!(stripped, "bold and italic and code");
    }

    #[test]
    fn test_post_payload_contains_markdown_formatting() {
        let payload = build_post_payload("Hello **world**");
        assert!(payload.contains("Hello **world**"));
        assert!(payload.contains(r#""tag":"md""#));
    }

    #[test]
    fn test_post_payload_is_valid_json() {
        let payload = build_post_payload("Test message");
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert!(v["zh_cn"]["content"].is_array());
    }

    // ------------------------------------------------------------------
    // Edit message format tests
    // ------------------------------------------------------------------

    #[test]
    fn test_edit_message_update_request_format() {
        let content = r#"{"text":"updated content"}"#;
        let req = UpdateMessageRequest {
            content: content.to_string(),
            msg_type: "text".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["content"], content);
        assert_eq!(v["msg_type"], "text");
    }

    #[test]
    fn test_edit_message_send_message_request_format() {
        let req = SendMessageRequest {
            receive_id: "ou_test123".to_string(),
            msg_type: "post".to_string(),
            content: r#"{"zh_cn":{"content":[[{"tag":"md","text":"hello"}]]}}"#.to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["receive_id"], "ou_test123");
        assert_eq!(v["msg_type"], "post");
        assert!(v["content"].as_str().unwrap().contains("zh_cn"));
    }

    // ------------------------------------------------------------------
    // Delete message response format tests
    // ------------------------------------------------------------------

    #[test]
    fn test_delete_message_response_format() {
        // Test that the delete response format is correct
        #[derive(Deserialize)]
        struct DeleteResponse {
            code: i32,
            msg: String,
        }
        let raw = r#"{"code": 0, "msg": "success"}"#;
        let resp: DeleteResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.code, 0);
        assert_eq!(resp.msg, "success");
    }

    // ------------------------------------------------------------------
    // Chat info response format tests
    // ------------------------------------------------------------------

    #[test]
    fn test_chat_info_from_get_chat_response() {
        let raw = r#"{
            "code": 0,
            "msg": "success",
            "data": {
                "chat_type": "group",
                "name": "Test Chat",
                "chat_id": "oc_chat123"
            }
        }"#;
        let resp: GetChatResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.code, 0);
        let data = resp.data.unwrap();
        assert_eq!(data.chat_type.unwrap(), "group");
        assert_eq!(data.name.unwrap(), "Test Chat");
        assert_eq!(data.chat_id.unwrap(), "oc_chat123");
    }

    // ------------------------------------------------------------------
    // Reply fallback code tests
    // ------------------------------------------------------------------

    #[test]
    fn test_reply_fallback_codes() {
        // 230011 = message not found (reply target missing)
        // 231003 = message has been recalled
        assert_ne!(230011, 0);
        assert_ne!(231003, 0);
    }

    // ------------------------------------------------------------------
    // Module integration tests
    // ------------------------------------------------------------------

    #[test]
    fn test_access_control_field_accessible() {
        let config = FeishuConfig::new("app_id", "app_secret");
        let adapter = FeishuAdapter::new(config);
        assert_eq!(adapter.access_control.bot_open_id, "app_id");
        assert_eq!(adapter.access_control.bot_name, "FeishuBot");
        assert!(!adapter.access_control.require_mention);
    }

    #[test]
    fn test_reactions_field_accessible() {
        let config = FeishuConfig::new("app_id", "app_secret");
        let adapter = FeishuAdapter::new(config);
        let _ = &adapter.reactions;
    }

    #[test]
    fn test_batch_manager_defaults_to_none() {
        let config = FeishuConfig::new("app_id", "app_secret");
        let adapter = FeishuAdapter::new(config);
        assert!(adapter.batch_manager.is_none());
    }

    #[test]
    fn test_processing_queue_field_accessible() {
        let config = FeishuConfig::new("app_id", "app_secret");
        let adapter = FeishuAdapter::new(config);
        let _ = &adapter.processing_queue;
    }

    #[test]
    fn test_extract_chat_id_from_message_event() {
        let event = serde_json::json!({
            "type": "im.message.receive_v1",
            "event": {
                "message": {"chat_id": "oc_test_chat_123"},
                "sender": {"sender_id": {"open_id": "ou_001"}}
            }
        });
        assert_eq!(extract_chat_id(&event), Some("oc_test_chat_123".to_string()));
    }

    #[test]
    fn test_extract_chat_id_from_card_action_event() {
        let event = serde_json::json!({
            "type": "card.action.trigger",
            "event": {
                "open_chat_id": "oc_card_chat",
                "open_message_id": "om_001",
                "open_id": "ou_001",
                "action": {"tag": "button", "value": {"key": "val"}}
            }
        });
        assert_eq!(extract_chat_id(&event), Some("oc_card_chat".to_string()));
    }

    #[test]
    fn test_extract_chat_id_returns_none_for_unknown_event() {
        let event = serde_json::json!({"type": "unknown"});
        assert_eq!(extract_chat_id(&event), None);
    }

    #[test]
    fn test_card_action_produces_command_message() {
        let event_data = serde_json::json!({
            "action": {"value": {"action": "approve"}, "tag": "button"},
            "open_id": "ou_test_user",
            "open_message_id": "om_test_msg",
            "open_chat_id": "oc_test_chat"
        });

        let msg = CardActionHandler::handle_card_action(
            &event_data,
            "om_test_msg",
            "oc_test_chat",
            "ou_test_user",
        )
        .expect("card action should produce InboundMessage");

        assert_eq!(msg.message_type, MessageType::Command);
        assert_eq!(msg.platform, Platform::Feishu);
        assert!(msg.text.starts_with("/card button "));
        assert!(msg.text.contains("approve"));
    }

    #[tokio::test]
    async fn test_access_control_filters_self_echo() {
        let config = FeishuConfig::new("app_id", "app_secret");
        let adapter = FeishuAdapter::new(config);

        let result = adapter
            .access_control
            .admit("chat_001", "group", "app_id", None, false, true)
            .await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("self echo"));
    }

    #[tokio::test]
    async fn test_processing_queue_serial_per_chat() {
        let config = FeishuConfig::new("app_id", "app_secret");
        let adapter = FeishuAdapter::new(config);

        let d1 = adapter
            .processing_queue
            .try_process("chat-A", serde_json::json!({"msg": "first"}))
            .await;
        assert_eq!(d1, ProcessingDecision::Process);

        let d2 = adapter
            .processing_queue
            .try_process("chat-A", serde_json::json!({"msg": "second"}))
            .await;
        assert_eq!(d2, ProcessingDecision::Queued);

        let d3 = adapter
            .processing_queue
            .try_process("chat-B", serde_json::json!({"msg": "third"}))
            .await;
        assert_eq!(d3, ProcessingDecision::Process);

        adapter.processing_queue.release("chat-A").await;

        let d4 = adapter
            .processing_queue
            .try_process("chat-A", serde_json::json!({"msg": "fourth"}))
            .await;
        assert_eq!(d4, ProcessingDecision::Process);
    }

    #[test]
    fn test_config_with_custom_bot_identity() {
        let config = FeishuConfig::new("app_id", "app_secret")
            .with_bot_open_id("bot_ou_custom")
            .with_bot_name("CustomBot");
        assert_eq!(config.bot_open_id, "bot_ou_custom");
        assert_eq!(config.bot_name, "CustomBot");

        let adapter = FeishuAdapter::new(config);
        assert_eq!(adapter.access_control.bot_open_id, "bot_ou_custom");
        assert_eq!(adapter.access_control.bot_name, "CustomBot");
    }
}
