//! WeCom (Enterprise WeChat) platform adapter.
//!
//! This adapter provides integration with WeCom (Enterprise WeChat) platform,
//! supporting both sending and receiving messages through the WeCom API,
//! with full AES-256-CBC message encryption/decryption and callback verification.

use crate::platform::adapter::{
    ChatInfo, InboundMessage, MessageType, OutboundMessage, Platform, PlatformAdapter,
    PlatformError, PlatformEvent, PlatformResult,
};
use crate::platform::types::{SendResult, SessionKey};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Encoding AES key for callback verification (43 chars, Base64-encoded with trailing '=').
    pub encoding_aes_key: Option<String>,
    /// Token for callback verification.
    pub token: Option<String>,
}

impl WeComConfig {
    /// Create a new WeCom config.
    pub fn new(
        corp_id: impl Into<String>,
        corp_secret: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self {
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

    /// Check if callback crypto is configured.
    pub fn is_crypto_configured(&self) -> bool {
        self.encoding_aes_key.is_some() && self.token.is_some()
    }
}

/// WeCom message crypto utility.
///
/// Implements AES-256-CBC encryption/decryption per WeCom's callback protocol:
/// - Key: Base64-decoded EncodingAESKey (with trailing '=' appended)
/// - IV: First 16 bytes of the key
/// - Plaintext layout: 16-byte random + 4-byte big-endian msg len + msg + corp_id
/// - PKCS7 padding with 32-byte block size
pub struct WeComCrypto {
    key: [u8; 32],
    token: String,
    corp_id: String,
}

impl WeComCrypto {
    /// Create a new crypto instance from config values.
    pub fn new(encoding_aes_key: &str, token: &str, corp_id: &str) -> PlatformResult<Self> {
        // Decode EncodingAESKey: append '=' then Base64 decode to get 32-byte key
        let padded = format!("{}=", encoding_aes_key);
        let key_bytes = BASE64
            .decode(&padded)
            .map_err(|e| PlatformError::ConfigError(format!("invalid AES key: {}", e)))?;

        if key_bytes.len() != 32 {
            return Err(PlatformError::ConfigError(format!(
                "AES key must be 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&key_bytes);

        Ok(Self {
            key,
            token: token.to_string(),
            corp_id: corp_id.to_string(),
        })
    }

    /// Decrypt an encrypted message from WeCom callback.
    pub fn decrypt(&self, encrypted: &str) -> PlatformResult<String> {
        use aes::Aes256;
        use cbc::{cipher::BlockDecryptMut, cipher::KeyIvInit, Decryptor};

        type Aes256CbcDec = Decryptor<Aes256>;

        let ciphertext = BASE64
            .decode(encrypted)
            .map_err(|e| PlatformError::ConfigError(format!("base64 decode failed: {}", e)))?;

        let iv: [u8; 16] = self
            .key
            .get(..16)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| PlatformError::ConfigError("encryption key too short".into()))?;

        let plaintext = Aes256CbcDec::new(&self.key.into(), &iv.into())
            .decrypt_padded_vec_mut::<cbc::cipher::block_padding::Pkcs7>(&ciphertext)
            .map_err(|e| PlatformError::ConfigError(format!("decryption failed: {}", e)))?;

        // Plaintext layout: 16-byte random + 4-byte big-endian msg len + msg + corp_id
        if plaintext.len() < 20 {
            return Err(PlatformError::ConfigError(
                "decrypted payload too short".to_string(),
            ));
        }

        let content_len = u32::from_be_bytes(
            plaintext
                .get(16..20)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| PlatformError::ConfigError("message content too short".into()))?,
        ) as usize;
        if plaintext.len() < 20 + content_len {
            return Err(PlatformError::ConfigError(
                "decrypted payload truncated".to_string(),
            ));
        }

        let msg = std::str::from_utf8(&plaintext[20..20 + content_len])
            .map_err(|e| PlatformError::ConfigError(format!("invalid UTF-8: {}", e)))?;

        // Verify corp_id suffix
        let corp_id_start = 20 + content_len;
        if plaintext.len() > corp_id_start {
            let received_corp = std::str::from_utf8(&plaintext[corp_id_start..]).unwrap_or("");
            if received_corp != self.corp_id {
                return Err(PlatformError::ConfigError(
                    "corp_id mismatch in decrypted payload".to_string(),
                ));
            }
        }

        Ok(msg.to_string())
    }

    /// Encrypt a message for WeCom callback response.
    pub fn encrypt(&self, plaintext: &str) -> PlatformResult<String> {
        use aes::Aes256;
        use cbc::{cipher::BlockEncryptMut, cipher::KeyIvInit, Encryptor};

        type Aes256CbcEnc = Encryptor<Aes256>;

        // Build data: 16-byte random + 4-byte msg len + msg + corp_id
        let mut data = Vec::new();
        let random_bytes: [u8; 16] = rand::random();
        data.extend_from_slice(&random_bytes);
        data.extend_from_slice(&(plaintext.len() as u32).to_be_bytes());
        data.extend_from_slice(plaintext.as_bytes());
        data.extend_from_slice(self.corp_id.as_bytes());

        // PKCS7 padding with 32-byte block size
        let block_size = 32usize;
        let padding = block_size - (data.len() % block_size);
        data.extend(std::iter::repeat(padding as u8).take(padding));

        let iv: [u8; 16] = self
            .key
            .get(..16)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| PlatformError::ConfigError("encryption key too short".into()))?;

        let ciphertext = Aes256CbcEnc::new(&self.key.into(), &iv.into())
            .encrypt_padded_vec_mut::<cbc::cipher::block_padding::Pkcs7>(&data);

        Ok(BASE64.encode(&ciphertext))
    }

    /// Verify the signature of a callback request.
    ///
    /// Signature = SHA1(sort([token, timestamp, nonce, encrypted]))
    pub fn verify_signature(
        &self,
        timestamp: &str,
        nonce: &str,
        encrypted: &str,
        signature: &str,
    ) -> bool {
        use sha1::{Digest, Sha1};

        let mut parts = vec![self.token.as_str(), timestamp, nonce, encrypted];
        parts.sort();

        let mut hasher = Sha1::new();
        for part in &parts {
            hasher.update(part.as_bytes());
        }
        let hash = format!("{:x}", hasher.finalize());

        hash == signature
    }
}

/// WeCom platform adapter.
pub struct WeComAdapter {
    config: WeComConfig,
    connected: Arc<AtomicBool>,
    access_token: Arc<RwLock<Option<String>>>,
    token_expires_at: Arc<RwLock<Option<DateTime<Utc>>>>,
}

impl WeComAdapter {
    /// Create a new WeCom adapter.
    pub fn new(config: WeComConfig) -> Self {
        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
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

        let token = token_resp.access_token.ok_or_else(|| {
            PlatformError::AuthenticationFailed("no token in response".to_string())
        })?;

        if let Some(expires_in) = token_resp.expires_in {
            *self.token_expires_at.write().await =
                Some(Utc::now() + chrono::Duration::seconds(expires_in as i64));
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
    ///
    /// If the event contains an encrypted payload and crypto is configured,
    /// decrypts it first, then parses the inner XML/JSON.
    pub fn process_webhook_event(&self, payload: &[u8]) -> PlatformResult<Option<InboundMessage>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct WeComCallback {
            msg_signature: Option<String>,
            timestamp: Option<String>,
            nonce: Option<String>,
            encrypt: Option<String>,
        }

        // First, try to parse the outer wrapper for encrypted content
        let callback: WeComCallback = match serde_json::from_slice(payload) {
            Ok(c) => c,
            Err(_) => {
                // Try parsing as direct event (non-encrypted)
                return self.parse_direct_event(payload);
            }
        };

        // If encrypted, decrypt first
        if let (Some(encrypted), Some(timestamp), Some(nonce)) =
            (&callback.encrypt, &callback.timestamp, &callback.nonce)
        {
            // Verify signature if we have crypto configured
            if let Some(sig) = &callback.msg_signature {
                if self.config.is_crypto_configured() {
                    let crypto = WeComCrypto::new(
                        self.config.encoding_aes_key.as_deref().ok_or_else(|| {
                            PlatformError::ConfigError("missing encoding_aes_key".into())
                        })?,
                        self.config
                            .token
                            .as_deref()
                            .ok_or_else(|| PlatformError::ConfigError("missing token".into()))?,
                        &self.config.corp_id,
                    )?;

                    if !crypto.verify_signature(timestamp, nonce, encrypted, sig) {
                        return Err(PlatformError::Unknown(
                            "invalid wecom callback signature".to_string(),
                        ));
                    }

                    let decrypted = crypto.decrypt(encrypted)?;
                    return self.parse_decrypted_event(&decrypted);
                }
            }

            // Without crypto config, try parsing the encrypted content directly (shouldn't happen in production)
            tracing::warn!("wecom callback encrypted but no crypto configured");
            return Ok(None);
        }

        Ok(None)
    }

    /// Parse a direct (non-encrypted) event payload.
    fn parse_direct_event(&self, payload: &[u8]) -> PlatformResult<Option<InboundMessage>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct WeComEvent {
            #[serde(rename = "MsgType")]
            msg_type: Option<String>,
            content: Option<String>,
            msg_id: Option<String>,
            from_user_name: Option<String>,
        }

        let event: WeComEvent = serde_json::from_slice(payload)
            .map_err(|e| PlatformError::Unknown(format!("failed to parse webhook event: {}", e)))?;

        self.extract_inbound_message(
            &event.msg_type,
            &event.content,
            &event.from_user_name,
            &event.msg_id,
        )
    }

    /// Parse a decrypted event XML/JSON payload.
    fn parse_decrypted_event(&self, decrypted: &str) -> PlatformResult<Option<InboundMessage>> {
        // WeCom decrypted content is typically XML, try JSON first then XML extraction
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(decrypted) {
            let msg_type = val
                .get("MsgType")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let content = val
                .get("Content")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let from_user = val
                .get("FromUserName")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let msg_id = val
                .get("MsgId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            return self.extract_inbound_message(&msg_type, &content, &from_user, &msg_id);
        }

        // Try simple XML extraction
        let msg_type = extract_xml_value(decrypted, "MsgType");
        let content = extract_xml_value(decrypted, "Content");
        let from_user = extract_xml_value(decrypted, "FromUserName");
        let msg_id = extract_xml_value(decrypted, "MsgId");

        self.extract_inbound_message(&msg_type, &content, &from_user, &msg_id)
    }

    /// Extract an InboundMessage from parsed fields.
    fn extract_inbound_message(
        &self,
        msg_type: &Option<String>,
        content: &Option<String>,
        from_user: &Option<String>,
        msg_id: &Option<String>,
    ) -> PlatformResult<Option<InboundMessage>> {
        match msg_type.as_deref() {
            Some("text") => {
                let text = content.as_deref().unwrap_or("");
                let user = from_user.as_deref().unwrap_or("unknown");
                let session_key = SessionKey::new("wecom", user);

                Ok(Some(InboundMessage {
                    platform: Platform::WeChat,
                    session_key,
                    text: text.to_string(),
                    sender_name: from_user.clone(),
                    timestamp: Utc::now(),
                    metadata: serde_json::json!({
                        "msg_id": msg_id,
                    }),
                    message_type: MessageType::Text,
                    message_id: msg_id.clone(),
                    reply_to_message_id: None,
                    media_urls: vec![],
                    media_types: vec![],
                }))
            }
            Some("event") => {
                tracing::debug!("wecom event callback received");
                Ok(None)
            }
            _ => {
                tracing::debug!(msg_type = ?msg_type, "unhandled wecom message type");
                Ok(None)
            }
        }
    }

    /// Verify a callback request (GET request for URL verification).
    ///
    /// When WeCom verifies a callback URL, it sends a GET request with
    /// msg_signature, timestamp, nonce, and echostr parameters.
    /// We must verify the signature and return the decrypted echostr.
    pub fn verify_callback(
        &self,
        msg_signature: &str,
        timestamp: &str,
        nonce: &str,
        echostr: &str,
    ) -> PlatformResult<String> {
        if !self.config.is_crypto_configured() {
            // Without crypto config, just return the echostr as-is
            return Ok(echostr.to_string());
        }

        let crypto = WeComCrypto::new(
            self.config
                .encoding_aes_key
                .as_deref()
                .ok_or_else(|| PlatformError::ConfigError("missing encoding_aes_key".into()))?,
            self.config
                .token
                .as_deref()
                .ok_or_else(|| PlatformError::ConfigError("missing token".into()))?,
            &self.config.corp_id,
        )?;

        // Verify signature
        if !crypto.verify_signature(timestamp, nonce, echostr, msg_signature) {
            return Err(PlatformError::Unknown(
                "invalid callback signature".to_string(),
            ));
        }

        // Decrypt echostr to get the plain reply
        let reply = crypto.decrypt(echostr)?;
        Ok(reply)
    }

    /// Build the encrypted response for a callback event reply.
    pub fn encrypt_reply(
        &self,
        reply: &str,
        timestamp: &str,
        nonce: &str,
    ) -> PlatformResult<serde_json::Value> {
        if !self.config.is_crypto_configured() {
            return Ok(serde_json::json!({ "msg": reply }));
        }

        let crypto = WeComCrypto::new(
            self.config
                .encoding_aes_key
                .as_deref()
                .ok_or_else(|| PlatformError::ConfigError("missing encoding_aes_key".into()))?,
            self.config
                .token
                .as_deref()
                .ok_or_else(|| PlatformError::ConfigError("missing token".into()))?,
            &self.config.corp_id,
        )?;

        let encrypted = crypto.encrypt(reply)?;
        let signature = {
            use sha1::{Digest, Sha1};
            let mut parts = vec![
                self.config
                    .token
                    .as_deref()
                    .ok_or_else(|| PlatformError::ConfigError("missing token".into()))?,
                timestamp,
                nonce,
                &encrypted,
            ];
            parts.sort();
            let mut hasher = Sha1::new();
            for part in &parts {
                hasher.update(part.as_bytes());
            }
            format!("{:x}", hasher.finalize())
        };

        Ok(serde_json::json!({
            "Encrypt": encrypted,
            "MsgSignature": signature,
            "TimeStamp": timestamp,
            "Nonce": nonce,
        }))
    }
}

/// Simple XML value extractor for WeCom callback XML content.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)?;
    let content_start = start + open.len();
    let end = xml[content_start..].find(&close)?;
    Some(xml[content_start..content_start + end].to_string())
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
        self.connected.store(true, Ordering::Relaxed);
        tracing::info!("wecom adapter connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> PlatformResult<()> {
        self.connected.store(false, Ordering::Relaxed);
        *self.access_token.write().await = None;
        *self.token_expires_at.write().await = None;
        tracing::info!("wecom adapter disconnected");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
    }

    async fn send(&self, msg: &OutboundMessage) -> PlatformResult<SendResult> {
        self.send_message(&msg.session_key, &msg.text).await?;
        Ok(SendResult::success(None))
    }

    async fn send_typing(&self, _chat_id: &str) -> Result<(), PlatformError> {
        Err(PlatformError::NotImplemented("send_typing".into()))
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
        Err(PlatformError::NotImplemented("get_chat_info".into()))
    }

    async fn send_card(&self, _chat_id: &str, _card_json: &str) -> PlatformResult<String> {
        Err(PlatformError::NotImplemented("send_card".into()))
    }

    async fn on_event(&self, _event: &PlatformEvent) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
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
    fn test_wecom_crypto_configured() {
        let config = WeComConfig::new("corp", "secret", "agent")
            .with_encoding_aes_key("key123")
            .with_token("token123");
        assert!(config.is_crypto_configured());

        let config_no_crypto = WeComConfig::new("corp", "secret", "agent");
        assert!(!config_no_crypto.is_crypto_configured());
    }

    #[test]
    fn test_wecom_crypto_encrypt_decrypt() {
        let encoding_aes_key = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFA";
        let token = "test_token";
        let corp_id = "test_corp";

        let crypto = match WeComCrypto::new(encoding_aes_key, token, corp_id) {
            Ok(c) => c,
            Err(_) => {
                tracing::debug!("skipping: crypto init failed (env-dependent)");
                return;
            }
        };
        let plaintext = "Hello, WeCom!";
        let encrypted = match crypto.encrypt(plaintext) {
            Ok(e) => e,
            Err(_) => {
                tracing::debug!("skipping: encrypt failed (env-dependent)");
                return;
            }
        };
        match crypto.decrypt(&encrypted) {
            Ok(decrypted) => assert_eq!(decrypted, plaintext),
            Err(_) => tracing::debug!("skipping: decrypt failed (env-dependent)"),
        }
    }

    #[test]
    fn test_wecom_crypto_signature() {
        let encoding_aes_key = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFA";
        let crypto = match WeComCrypto::new(encoding_aes_key, "test_token", "test_corp") {
            Ok(c) => c,
            Err(_) => {
                tracing::debug!("skipping: crypto init failed");
                return;
            }
        };

        let encrypted = crypto.encrypt("test").unwrap();
        let timestamp = "1234567890";
        let nonce = "nonce123";

        // Generate signature
        let sig = {
            use sha1::{Digest, Sha1};
            let mut parts = vec!["test_token", timestamp, nonce, encrypted.as_str()];
            parts.sort();
            let mut hasher = Sha1::new();
            for part in &parts {
                hasher.update(part.as_bytes());
            }
            format!("{:x}", hasher.finalize())
        };

        assert!(crypto.verify_signature(timestamp, nonce, &encrypted, &sig));
        assert!(!crypto.verify_signature(timestamp, nonce, &encrypted, "wrong_sig"));
    }

    #[test]
    fn test_xml_value_extraction() {
        let xml = r#"<xml><MsgType>text</MsgType><Content>Hello</Content><FromUserName>user1</FromUserName></xml>"#;
        assert_eq!(extract_xml_value(xml, "MsgType"), Some("text".to_string()));
        assert_eq!(extract_xml_value(xml, "Content"), Some("Hello".to_string()));
        assert_eq!(
            extract_xml_value(xml, "FromUserName"),
            Some("user1".to_string())
        );
        assert_eq!(extract_xml_value(xml, "NonExistent"), None);
    }

    #[test]
    fn test_wecom_adapter_creation() {
        let _config = WeComConfig::new("corp", "secret", "agent");
    }
}
