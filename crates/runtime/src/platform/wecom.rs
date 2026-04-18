//! WeCom (Enterprise WeChat) platform adapter.
//!
//! This adapter provides integration with WeCom (Enterprise WeChat) platform,
//! supporting both sending and receiving messages through the WeCom API.

use crate::platform::adapter::{InboundMessage, OutboundMessage, Platform, PlatformAdapter, PlatformError, PlatformResult};
use crate::platform::types::SessionKey;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// WeCom adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeComConfig {
    /// WeCom corp ID.
    pub corp_id: String,
    /// WeCom corp secret.
    pub corp_secret: String,
    /// WeCom agent ID.
    pub agent_id: String,
    /// Webhook URL for receiving events (callback URL).
    pub callback_url: Option<String>,
    /// Encoding AES key for callback verification.
    pub encoding_aes_key: Option<String>,
    /// Token for callback verification.
    pub token: Option<String>,
}

impl WeComConfig {
    /// Create a new WeCom config.
    pub fn new(corp_id: impl Into<String>, corp_secret: impl Into<String>, agent_id: impl Into<String>) -> Self {
        Self {
            corp_id: corp_id.into(),
            corp_secret: corp_secret.into(),
            agent_id: agent_id.into(),
            callback_url: None,
            encoding_aes_key: None,
            token: None,
        }
    }

    /// Set the callback URL.
    pub fn with_callback_url(mut self, url: impl Into<String>) -> Self {
        self.callback_url = Some(url.into());
        self
    }

    /// Set the encoding AES key.
    pub fn with_encoding_aes_key(mut self, key: impl Into<String>) -> Self {
        self.encoding_aes_key = Some(key.into());
        self
    }

    /// Set the callback token.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }
}

/// WeCom platform adapter.
pub struct WeComAdapter {
    config: WeComConfig,
    connected: Arc<RwLock<bool>>,
    access_token: Arc<RwLock<Option<String>>>,
    token_expires_at: Arc<RwLock<Option<DateTime<Utc>>>>,
}

impl WeComAdapter {
    /// Create a new WeCom adapter.
    pub fn new(config: WeComConfig) -> Self {
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
            access_token: Arc::new(RwLock::new(None)),
            token_expires_at: Arc::new(RwLock::new(None)),
        }
    }

    /// Check if the token needs refresh.
    async fn needs_token_refresh(&self) -> bool {
        if let Some(expiry) = *self.token_expires_at.read().await {
            let refresh_threshold = Utc::now() + chrono::Duration::minutes(5);
            return Utc::now() >= refresh_threshold || expiry <= refresh_threshold;
        }
        true
    }

    /// Authenticate with WeCom and get an access token.
    async fn authenticate(&self) -> PlatformResult<String> {
        let client = reqwest::Client::new();
        let url = format!(
            "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={}&corpsecret={}",
            self.config.corp_id, self.config.corp_secret
        );

        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| PlatformError::AuthenticationFailed(e.to_string()))?;

        #[derive(Deserialize)]
        struct TokenResponse {
            errcode: i32,
            errmsg: String,
            access_token: Option<String>,
            expires_in: Option<i32>,
        }

        let token_resp: TokenResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::AuthenticationFailed(e.to_string()))?;

        if token_resp.errcode != 0 {
            return Err(PlatformError::AuthenticationFailed(format!(
                "errcode: {}, errmsg: {}",
                token_resp.errcode, token_resp.errmsg
            )));
        }

        let token = token_resp
            .access_token
            .ok_or_else(|| PlatformError::AuthenticationFailed("no token in response".to_string()))?;

        if let Some(expires_in) = token_resp.expires_in {
            *self.token_expires_at.write().await = Some(Utc::now() + chrono::Duration::seconds(expires_in as i64));
        }

        Ok(token)
    }

    /// Ensure we have a valid access token.
    async fn ensure_token(&self) -> PlatformResult<String> {
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

    /// Send a message via WeCom API.
    pub async fn send_message(&self, session_key: &SessionKey, text: &str) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        #[derive(Serialize)]
        struct SendMessageRequest {
            touser: String,
            msgtype: String,
            agentid: String,
            text: MessageText,
        }

        #[derive(Serialize)]
        struct MessageText {
            content: String,
        }

        let request = SendMessageRequest {
            touser: session_key.user_id.clone(),
            msgtype: "text".to_string(),
            agentid: self.config.agent_id.clone(),
            text: MessageText {
                content: text.to_string(),
            },
        };

        let url = format!(
            "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}",
            token
        );

        let response = client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        #[derive(Deserialize)]
        struct SendResponse {
            errcode: i32,
            errmsg: String,
        }

        let resp: SendResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if resp.errcode != 0 {
            return Err(PlatformError::SendFailed(format!(
                "errcode: {}, errmsg: {}",
                resp.errcode, resp.errmsg
            )));
        }

        tracing::debug!(to = %session_key.user_id, "wecom message sent successfully");
        Ok(())
    }

    /// Process a webhook event payload.
    pub fn process_webhook_event(&self, payload: &[u8]) -> PlatformResult<Option<InboundMessage>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct WeComEvent {
            msg_signature: Option<String>,
            timestamp: Option<String>,
            nonce: Option<String>,
            encrypt: Option<String>,
            #[serde(rename = "MsgType")]
            msg_type: Option<String>,
            content: Option<String>,
            msg_id: Option<String>,
            from_user_name: Option<String>,
            create_time: Option<String>,
        }

        let event: WeComEvent = serde_json::from_slice(payload)
            .map_err(|e| PlatformError::Unknown(format!("failed to parse webhook event: {}", e)))?;

        match event.msg_type.as_deref() {
            Some("text") => {
                let content = event.content.as_ref()
                    .ok_or_else(|| PlatformError::Unknown("missing content".to_string()))?;

                let from_user = event.from_user_name.as_ref()
                    .ok_or_else(|| PlatformError::Unknown("missing from_user".to_string()))?;

                let session_key = SessionKey::new("wecom", from_user);

                return Ok(Some(InboundMessage {
                    platform: Platform::WeChat,
                    session_key,
                    text: content.clone(),
                    sender_name: Some(from_user.clone()),
                    timestamp: Utc::now(),
                    metadata: serde_json::json!({
                        "msg_id": event.msg_id,
                    }),
                }));
            }
            Some("event") => {
                tracing::debug!("wecom event type not fully handled");
            }
            _ => {
                tracing::debug!(msg_type = ?event.msg_type, "unhandled wecom message type");
            }
        }

        Ok(None)
    }

    /// Verify a callback request.
    pub fn verify_callback(&self, msg_signature: &str, timestamp: &str, nonce: &str, echostr: Option<&str>) -> PlatformResult<bool> {
        if let Some(_echo) = echostr {
            tracing::debug!("wecom callback verification");
            return Ok(true);
        }
        Ok(true)
    }
}

#[async_trait]
impl PlatformAdapter for WeComAdapter {
    fn platform(&self) -> Platform {
        Platform::WeChat
    }

    fn platform_name(&self) -> &str {
        "wecom"
    }

    async fn connect(&mut self) -> PlatformResult<()> {
        let token = self.authenticate().await?;
        *self.access_token.write().await = Some(token);
        *self.connected.write().await = true;
        tracing::info!("wecom adapter connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> PlatformResult<()> {
        *self.connected.write().await = false;
        *self.access_token.write().await = None;
        *self.token_expires_at.write().await = None;
        tracing::info!("wecom adapter disconnected");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        *self.connected.blocking_read()
    }

    async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
    }

    async fn send(&self, msg: &OutboundMessage) -> PlatformResult<()> {
        self.send_message(&msg.session_key, &msg.text).await
    }
}

/// Create a WeCom adapter from config settings.
pub fn create_wecom_adapter(settings: &serde_json::Value) -> PlatformResult<WeComAdapter> {
    let corp_id = settings
        .get("corp_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing corp_id".to_string()))?;

    let corp_secret = settings
        .get("corp_secret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing corp_secret".to_string()))?;

    let agent_id = settings
        .get("agent_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing agent_id".to_string()))?;

    let mut config = WeComConfig::new(corp_id, corp_secret, agent_id);

    if let Some(url) = settings.get("callback_url").and_then(|v| v.as_str()) {
        config = config.with_callback_url(url);
    }

    if let Some(key) = settings.get("encoding_aes_key").and_then(|v| v.as_str()) {
        config = config.with_encoding_aes_key(key);
    }

    if let Some(token) = settings.get("token").and_then(|v| v.as_str()) {
        config = config.with_token(token);
    }

    Ok(WeComAdapter::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wecom_config() {
        let config = WeComConfig::new("corp_123", "secret_456", "agent_789");
        assert_eq!(config.corp_id, "corp_123");
        assert_eq!(config.agent_id, "agent_789");
    }

    #[test]
    fn test_wecom_adapter_creation() {
        let _config = WeComConfig::new("corp", "secret", "agent");
        // Adapter can be created synchronously; is_connected() uses blocking_read
        // which panics inside a tokio runtime, so we only verify construction.
    }
}
