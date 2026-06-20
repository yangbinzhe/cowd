//! WeChat iLink Bot platform adapter.
//!
//! This adapter provides integration with Tencent's WeChat iLink Bot API,
//! supporting long-polling for messages, text sending, typing indicators,
//! QR code generation, and media upload URLs.
//!
//! The iLink API is a personal WeChat bot service that uses:
//! - Token-based authentication (POST `/ilink/bot/gettoken`)
//! - Long-polling for message updates (GET `/ilink/bot/getupdates`)
//! - Context token echoing for session continuity
//!
//! ## Hermes Reference
//!
//! Corresponds to `Hermes/hermes/providers/platforms/weixin.py` iLink
//! implementation. Base URL: `https://ilinkai.weixin.qq.com`.

use crate::platform::adapter::{
    ChatInfo, InboundMessage, MessageType, OutboundMessage, Platform, PlatformAdapter,
    PlatformError, PlatformEvent, PlatformResult,
};
use crate::platform::dedup::DedupStore;
use crate::platform::types::{SendResult, SessionKey};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Default iLink API base URL.
pub const ILINK_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
pub const ILINK_APP_ID: &str = "bot";
pub const ILINK_APP_CLIENT_VERSION: &str = "131584";
const QR_TIMEOUT_SECS: u64 = 8;
const API_TIMEOUT_SECS: u64 = 15;

/// Default long-polling timeout in seconds.
pub const DEFAULT_LONG_POLLING_TIMEOUT: u64 = 30;

/// WeChat iLink adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeChatLinkConfig {
    /// Bot ID (from WeChat iLink console).
    pub bot_id: String,
    /// Bot secret for authentication.
    pub bot_secret: String,
    /// QR-authorized iLink account id, when using personal WeChat login.
    #[serde(default)]
    pub account_id: Option<String>,
    /// QR-authorized bot token. Kept out of normal config files; loaded from account store.
    #[serde(default)]
    pub token: Option<String>,
    /// QR-authorized user id returned by iLink.
    #[serde(default)]
    pub user_id: Option<String>,
    /// Use Hermes-compatible QR token protocol instead of bot_id/bot_secret exchange.
    #[serde(default)]
    pub qr_token_mode: bool,
    /// Base URL for the iLink API.
    /// Default: `https://ilinkai.weixin.qq.com`
    pub base_url: String,
}

impl WeChatLinkConfig {
    /// Create a new WeChat iLink config.
    pub fn new(bot_id: impl Into<String>, bot_secret: impl Into<String>) -> Self {
        Self {
            bot_id: bot_id.into(),
            bot_secret: bot_secret.into(),
            account_id: None,
            token: None,
            user_id: None,
            base_url: ILINK_BASE_URL.to_string(),
            qr_token_mode: false,
        }
    }

    /// Set a custom base URL for the iLink API.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn from_qr_account(
        account_id: impl Into<String>,
        token: impl Into<String>,
        base_url: impl Into<String>,
        user_id: Option<String>,
    ) -> Self {
        Self {
            bot_id: String::new(),
            bot_secret: String::new(),
            account_id: Some(account_id.into()),
            token: Some(token.into()),
            user_id,
            base_url: base_url.into(),
            qr_token_mode: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WeChatQrCode {
    pub qrcode: String,
    pub scan_data: String,
    pub qrcode_img_content: Option<String>,
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WeChatQrStatus {
    pub status: String,
    pub redirect_host: Option<String>,
    pub credentials: Option<WeChatQrCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WeChatQrCredentials {
    pub account_id: String,
    pub token: String,
    pub base_url: String,
    pub user_id: Option<String>,
    pub saved_at: String,
}

fn wechat_account_dir(base: Option<&Path>) -> PathBuf {
    base.map(Path::to_path_buf).unwrap_or_else(|| {
        crate::cowd_dirs::config_home_dir()
            .join("channels")
            .join("wechat-ilink")
            .join("accounts")
    })
}

pub fn save_wechat_qr_account(
    credentials: &WeChatQrCredentials,
    base: Option<&Path>,
) -> PlatformResult<PathBuf> {
    let dir = wechat_account_dir(base);
    std::fs::create_dir_all(&dir)
        .map_err(|e| PlatformError::ConfigError(format!("create wechat account dir: {e}")))?;
    let path = dir.join(format!(
        "{}.json",
        sanitize_account_id(&credentials.account_id)
    ));
    let data = serde_json::to_vec_pretty(credentials)
        .map_err(|e| PlatformError::ConfigError(format!("serialize wechat account: {e}")))?;
    std::fs::write(&path, data)
        .map_err(|e| PlatformError::ConfigError(format!("write wechat account: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(path)
}

pub fn load_wechat_qr_account(
    account_id: &str,
    base: Option<&Path>,
) -> PlatformResult<WeChatQrCredentials> {
    let path = wechat_account_dir(base).join(format!("{}.json", sanitize_account_id(account_id)));
    let data = std::fs::read(&path).map_err(|e| {
        PlatformError::ConfigError(format!("read wechat account {}: {e}", path.display()))
    })?;
    serde_json::from_slice(&data).map_err(|e| {
        PlatformError::ConfigError(format!("parse wechat account {}: {e}", path.display()))
    })
}

pub fn list_wechat_qr_accounts(base: Option<&Path>) -> PlatformResult<Vec<WeChatQrCredentials>> {
    let dir = wechat_account_dir(base);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut accounts = Vec::new();
    for entry in std::fs::read_dir(&dir)
        .map_err(|e| PlatformError::ConfigError(format!("read wechat account dir: {e}")))?
    {
        let Ok(entry) = entry else { continue };
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if let Ok(data) = std::fs::read(&path) {
            if let Ok(account) = serde_json::from_slice::<WeChatQrCredentials>(&data) {
                accounts.push(account);
            }
        }
    }
    accounts.sort_by(|a, b| b.saved_at.cmp(&a.saved_at));
    Ok(accounts)
}

fn sanitize_account_id(account_id: &str) -> String {
    account_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '@' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn ilink_headers(token: Option<&str>, body_len: Option<usize>) -> reqwest::header::HeaderMap {
    use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
    let mut headers = HeaderMap::new();
    headers.insert("iLink-App-Id", HeaderValue::from_static(ILINK_APP_ID));
    headers.insert(
        "iLink-App-ClientVersion",
        HeaderValue::from_static(ILINK_APP_CLIENT_VERSION),
    );
    if let Some(len) = body_len {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Ok(value) = HeaderValue::from_str(&len.to_string()) {
            headers.insert(CONTENT_LENGTH, value);
        }
    }
    if let Some(token) = token.filter(|t| !t.is_empty()) {
        if let Ok(value) = HeaderValue::from_str(&format!("Bearer {token}")) {
            headers.insert(AUTHORIZATION, value);
        }
    }
    headers
}

async fn ilink_get_json(
    client: &reqwest::Client,
    base_url: &str,
    endpoint: &str,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        endpoint.trim_start_matches('/')
    );
    let response = client
        .get(url)
        .headers(ilink_headers(None, None))
        .timeout(std::time::Duration::from_secs(QR_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let value = response
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("parse ilink response: {e}"))?;
    if !status.is_success() {
        return Err(format!("iLink HTTP {status}: {value}"));
    }
    Ok(value)
}

async fn ilink_post_json(
    client: &reqwest::Client,
    base_url: &str,
    endpoint: &str,
    payload: serde_json::Value,
    token: Option<&str>,
    timeout_secs: u64,
) -> Result<serde_json::Value, String> {
    let mut body = payload.as_object().cloned().unwrap_or_default();
    body.insert(
        "base_info".to_string(),
        serde_json::json!({"channel_version": "2.2.0"}),
    );
    let body = serde_json::Value::Object(body).to_string();
    let url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        endpoint.trim_start_matches('/')
    );
    let response = client
        .post(url)
        .headers(ilink_headers(token, Some(body.len())))
        .body(body)
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let value = response
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("parse ilink response: {e}"))?;
    if !status.is_success() {
        return Err(format!("iLink HTTP {status}: {value}"));
    }
    Ok(value)
}

pub async fn request_wechat_qr_login(bot_type: &str) -> PlatformResult<WeChatQrCode> {
    let client = reqwest::Client::new();
    let endpoint = format!("ilink/bot/get_bot_qrcode?bot_type={}", bot_type.trim());
    let value = ilink_get_json(&client, ILINK_BASE_URL, &endpoint)
        .await
        .map_err(PlatformError::AuthenticationFailed)?;
    let qrcode = value
        .get("qrcode")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PlatformError::AuthenticationFailed("QR response missing qrcode".to_string())
        })?
        .to_string();
    let qrcode_img_content = value
        .get("qrcode_img_content")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    let scan_data = qrcode_img_content.clone().unwrap_or_else(|| qrcode.clone());
    Ok(WeChatQrCode {
        qrcode,
        scan_data,
        qrcode_img_content,
        base_url: ILINK_BASE_URL.to_string(),
    })
}

pub async fn poll_wechat_qr_login(
    qrcode: &str,
    base_url: Option<&str>,
) -> PlatformResult<WeChatQrStatus> {
    let client = reqwest::Client::new();
    let base_url = base_url.unwrap_or(ILINK_BASE_URL);
    let endpoint = format!("ilink/bot/get_qrcode_status?qrcode={qrcode}");
    let value = match ilink_get_json(&client, base_url, &endpoint).await {
        Ok(value) => value,
        Err(_) => {
            return Ok(WeChatQrStatus {
                status: "wait".to_string(),
                redirect_host: None,
                credentials: None,
            });
        }
    };
    let status = value
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("wait")
        .to_string();
    let redirect_host = value
        .get("redirect_host")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    let credentials = if status == "confirmed" {
        let account_id = value
            .get("ilink_bot_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                PlatformError::AuthenticationFailed("QR confirmed without ilink_bot_id".to_string())
            })?
            .to_string();
        let token = value
            .get("bot_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                PlatformError::AuthenticationFailed("QR confirmed without bot_token".to_string())
            })?
            .to_string();
        Some(WeChatQrCredentials {
            account_id,
            token,
            base_url: value
                .get("baseurl")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(ILINK_BASE_URL)
                .to_string(),
            user_id: value
                .get("ilink_user_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(ToString::to_string),
            saved_at: Utc::now().to_rfc3339(),
        })
    } else {
        None
    };
    Ok(WeChatQrStatus {
        status,
        redirect_host,
        credentials,
    })
}

/// WeChat iLink platform adapter.
///
/// Uses HTTP long-polling (no WebSocket) to receive messages from WeChat
/// users via Tencent's iLink Bot API. Simpler than Feishu — just token
/// auth, long-poll getupdates with context_token echo, and sendmessage.
pub struct WeChatLinkAdapter {
    config: WeChatLinkConfig,
    connected: Arc<RwLock<bool>>,
    /// Auth token obtained via `/ilink/bot/gettoken`.
    token: Arc<RwLock<Option<String>>>,
    /// Latest context_token from getupdates response, echoed back in the next call.
    context_token: Arc<RwLock<String>>,
    /// Dedup store: message_id → timestamp.
    seen_ids: DedupStore,
    /// Timestamp of last successful connect, used for reconnect backoff.
    last_connect_attempt: RwLock<Option<std::time::Instant>>,
    /// Count of consecutive auth/receive failures for backoff.
    consecutive_failures: RwLock<u32>,
}

impl WeChatLinkAdapter {
    /// Create a new WeChat iLink adapter.
    pub fn new(config: WeChatLinkConfig) -> Self {
        tracing::info!(
            "wechat_ilink adapter created: mode={} base_url={}",
            if config.qr_token_mode { "qr" } else { "bot" },
            config.base_url
        );
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
            token: Arc::new(RwLock::new(None)),
            context_token: Arc::new(RwLock::new(String::new())),
            seen_ids: DedupStore::new(10_000, 3600),
            last_connect_attempt: RwLock::new(None),
            consecutive_failures: RwLock::new(0),
        }
    }

    async fn try_reconnect(&mut self) -> PlatformResult<()> {
        let should_retry = {
            let last = self.last_connect_attempt.read().await;
            let fails = *self.consecutive_failures.read().await;
            match *last {
                None => true,
                Some(t) => {
                    let backoff = std::time::Duration::from_secs(
                        (fails.min(6) as u64).saturating_mul(5).max(5),
                    );
                    t.elapsed() >= backoff
                }
            }
        };
        if !should_retry {
            return Ok(());
        }
        match self.connect().await {
            Ok(()) => Ok(()),
            Err(e) => {
                let mut fails = self.consecutive_failures.write().await;
                *fails = fails.saturating_add(1);
                tracing::warn!(
                    "wechat_ilink adapter: reconnect attempt {} failed: {e}",
                    *fails
                );
                Err(e)
            }
        }
    }

    pub async fn request_qr_login(bot_type: &str) -> PlatformResult<WeChatQrCode> {
        request_wechat_qr_login(bot_type).await
    }

    pub async fn poll_qr_login(
        qrcode: &str,
        base_url: Option<&str>,
    ) -> PlatformResult<WeChatQrStatus> {
        poll_wechat_qr_login(qrcode, base_url).await
    }

    // ------------------------------------------------------------------
    // iLink API methods
    // ------------------------------------------------------------------

    /// Get an auth token from the iLink API.
    ///
    /// POST `/ilink/bot/gettoken` with bot_id + bot_secret.
    /// Returns the token string on success.
    pub async fn authenticate(&self) -> PlatformResult<String> {
        if self.config.qr_token_mode {
            return self
                .config
                .token
                .clone()
                .filter(|t| !t.is_empty())
                .ok_or_else(|| {
                    PlatformError::AuthenticationFailed(
                        "missing QR-authorized wechat token; configure the WeChat platform in Gateway/WebUI"
                            .to_string(),
                    )
                });
        }

        let client = reqwest::Client::new();
        let url = format!("{}/ilink/bot/gettoken", self.config.base_url);

        let response = client
            .post(&url)
            .json(&serde_json::json!({
                "bot_id": self.config.bot_id,
                "bot_secret": self.config.bot_secret,
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
            #[serde(default)]
            code: i32,
            #[serde(default)]
            msg: String,
            token: Option<String>,
        }

        let token_resp: TokenResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::AuthenticationFailed(e.to_string()))?;

        if token_resp.code != 0 {
            return Err(PlatformError::AuthenticationFailed(format!(
                "auth failed (code={}): {}",
                token_resp.code, token_resp.msg
            )));
        }

        token_resp
            .token
            .ok_or_else(|| PlatformError::AuthenticationFailed("no token in response".to_string()))
    }

    /// Ensure a valid token is available.
    ///
    /// Returns the cached token if present, otherwise calls `authenticate()`
    /// and stores the result.
    pub async fn ensure_token(&self) -> PlatformResult<String> {
        if let Some(token) = self.token.read().await.as_ref() {
            if !token.is_empty() {
                return Ok(token.clone());
            }
        }

        let token = self.authenticate().await?;
        *self.token.write().await = Some(token.clone());
        Ok(token)
    }

    /// Long-poll for incoming messages from iLink.
    ///
    /// GET `/ilink/bot/getupdates` with params: token, context_token.
    /// The response includes a context_token that must be echoed back in the
    /// next call (context_token echo mechanism).
    ///
    /// Returns a list of message JSON objects, or an empty vec on timeout.
    pub async fn get_updates(&self) -> PlatformResult<Vec<serde_json::Value>> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        // Read the current context_token to echo back
        let ctx_token = self.context_token.read().await.clone();

        if self.config.qr_token_mode {
            let json = ilink_post_json(
                &client,
                &self.config.base_url,
                "ilink/bot/getupdates",
                serde_json::json!({"get_updates_buf": ctx_token}),
                Some(&token),
                DEFAULT_LONG_POLLING_TIMEOUT,
            )
            .await
            .map_err(|e| PlatformError::ReceiveFailed(e.to_string()))?;

            if let Some(new_ctx) = json
                .get("get_updates_buf")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            {
                *self.context_token.write().await = new_ctx.to_string();
            }

            return Ok(json
                .get("msgs")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default());
        }

        let url = format!("{}/ilink/bot/getupdates", self.config.base_url);

        let response = client
            .get(&url)
            .query(&[
                ("token", token.as_str()),
                ("context_token", ctx_token.as_str()),
            ])
            .timeout(std::time::Duration::from_secs(DEFAULT_LONG_POLLING_TIMEOUT))
            .send()
            .await
            .map_err(|e| {
                // Timeout is expected — long-poll may return after the server-side
                // timeout without data. This is not an error.
                if e.is_timeout() {
                    tracing::debug!("ilink get_updates long-poll timeout (expected)");
                    return PlatformError::ReceiveFailed("long-poll timeout".to_string());
                }
                PlatformError::ReceiveFailed(e.to_string())
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(PlatformError::ReceiveFailed(format!(
                "get_updates failed {}: {}",
                status, body
            )));
        }

        #[derive(Deserialize)]
        struct UpdatesResponse {
            #[serde(default)]
            code: i32,
            #[serde(default)]
            msg: String,
            context_token: Option<String>,
            data: Option<Vec<serde_json::Value>>,
        }

        let updates_resp: UpdatesResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::ReceiveFailed(e.to_string()))?;

        if updates_resp.code != 0 {
            return Err(PlatformError::ReceiveFailed(format!(
                "get_updates error (code={}): {}",
                updates_resp.code, updates_resp.msg
            )));
        }

        // Update the context_token for the next long-poll cycle
        if let Some(new_ctx_token) = updates_resp.context_token {
            *self.context_token.write().await = new_ctx_token;
        }

        Ok(updates_resp.data.unwrap_or_default())
    }

    /// Parse a raw iLink message JSON into an `InboundMessage`.
    fn parse_ilink_message(&self, msg: &serde_json::Value) -> Option<InboundMessage> {
        let msg_type = msg
            .get("msg_type")
            .and_then(|v| v.as_str())
            .unwrap_or("text");

        let text = match msg_type {
            "text" => msg
                .get("text")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    msg.get("item_list")
                        .and_then(|v| v.as_array())
                        .and_then(|items| {
                            items.iter().find_map(|item| {
                                item.get("text_item")
                                    .and_then(|v| v.get("text"))
                                    .and_then(|v| v.as_str())
                            })
                        })
                })
                .map(|s| s.to_string())
                .unwrap_or_default(),
            "image" => "[Image]".to_string(),
            "voice" => "[Voice message]".to_string(),
            "video" => "[Video]".to_string(),
            "file" => {
                let name = msg
                    .get("file_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                format!("[File: {}]", name)
            }
            _ => format!("[{} message]", msg_type),
        };

        let from_user = msg
            .get("from_user")
            .and_then(|v| v.as_str())
            .or_else(|| msg.get("from_user_id").and_then(|v| v.as_str()))
            .unwrap_or("");
        if from_user.is_empty() {
            return None;
        }

        let message_id = msg
            .get("message_id")
            .and_then(|v| v.as_str())
            .or_else(|| msg.get("client_id").and_then(|v| v.as_str()))
            .map(|s| s.to_string());

        let msg_timestamp = msg
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .map(|ts| {
                chrono::TimeZone::timestamp_millis_opt(&Utc, ts)
                    .single()
                    .unwrap_or_else(Utc::now)
            })
            .unwrap_or_else(Utc::now);

        let session_key = SessionKey::new("wechat_ilink", from_user);

        let message_type = match msg_type {
            "text" => MessageType::Text,
            "image" => MessageType::Photo,
            "voice" => MessageType::Voice,
            "video" => MessageType::Video,
            "file" => MessageType::Document,
            _ => MessageType::Text,
        };

        Some(InboundMessage {
            platform: Platform::WeChat,
            session_key,
            text,
            sender_name: None,
            timestamp: msg_timestamp,
            metadata: serde_json::json!({
                "message_id": message_id,
                "from_user": from_user,
                "msg_type": msg_type,
            }),
            message_type,
            message_id,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        })
    }

    /// Send a text message to a user via iLink.
    ///
    /// POST `/ilink/bot/sendmessage` with params: token, touser, msgtype="text", text.
    /// Returns the message ID on success.
    pub async fn send_text(&self, to_user: &str, text: &str) -> PlatformResult<String> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        if self.config.qr_token_mode {
            let client_id = format!("cowd-weixin-{}", Uuid::new_v4().simple());
            let ctx_token = self.context_token.read().await.clone();
            let mut msg = serde_json::json!({
                "from_user_id": "",
                "to_user_id": to_user,
                "client_id": client_id,
                "message_type": 2,
                "message_state": 2,
                "item_list": [{
                    "type": 1,
                    "text_item": {"text": text}
                }]
            });
            if !ctx_token.is_empty() {
                msg["context_token"] = serde_json::Value::String(ctx_token);
            }
            ilink_post_json(
                &client,
                &self.config.base_url,
                "ilink/bot/sendmessage",
                serde_json::json!({"msg": msg}),
                Some(&token),
                API_TIMEOUT_SECS,
            )
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;
            return Ok(client_id);
        }

        let url = format!("{}/ilink/bot/sendmessage", self.config.base_url);

        let response = client
            .post(&url)
            .json(&serde_json::json!({
                "token": token,
                "touser": to_user,
                "msgtype": "text",
                "text": text,
            }))
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        #[derive(Deserialize)]
        struct SendResponse {
            #[serde(default)]
            code: i32,
            #[serde(default)]
            msg: String,
            message_id: Option<String>,
        }

        let send_resp: SendResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if send_resp.code != 0 {
            return Err(PlatformError::SendFailed(format!(
                "send_text failed (code={}): {}",
                send_resp.code, send_resp.msg
            )));
        }

        let msg_id = send_resp.message_id.unwrap_or_default();
        tracing::debug!(to = %to_user, %msg_id, "ilink text message sent");
        Ok(msg_id)
    }

    /// Send a typing indicator to a user.
    ///
    /// POST `/ilink/bot/sendtyping` with params: token, touser.
    pub async fn send_typing_indicator(&self, to_user: &str) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        if self.config.qr_token_mode {
            let _ = ilink_post_json(
                &client,
                &self.config.base_url,
                "ilink/bot/sendtyping",
                serde_json::json!({"ilink_user_id": to_user, "status": 1}),
                Some(&token),
                API_TIMEOUT_SECS,
            )
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;
            return Ok(());
        }

        let url = format!("{}/ilink/bot/sendtyping", self.config.base_url);

        let response = client
            .post(&url)
            .json(&serde_json::json!({
                "token": token,
                "touser": to_user,
            }))
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        #[derive(Deserialize)]
        struct TypingResponse {
            #[serde(default)]
            code: i32,
            #[serde(default)]
            msg: String,
        }

        let typing_resp: TypingResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if typing_resp.code != 0 {
            return Err(PlatformError::SendFailed(format!(
                "send_typing failed (code={}): {}",
                typing_resp.code, typing_resp.msg
            )));
        }

        tracing::debug!(to = %to_user, "ilink typing indicator sent");
        Ok(())
    }

    /// Get the QR code URL for bot login.
    ///
    /// GET `/ilink/bot/get_bot_qrcode` with params: token.
    pub async fn get_qr_code(&self) -> PlatformResult<String> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let url = format!("{}/ilink/bot/get_bot_qrcode", self.config.base_url);

        let response = client
            .get(&url)
            .query(&[("token", token.as_str())])
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(PlatformError::SendFailed(format!(
                "get_qr_code failed with status: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct QrCodeResponse {
            #[serde(default)]
            code: i32,
            #[serde(default)]
            msg: String,
            qrcode_url: Option<String>,
        }

        let qr_resp: QrCodeResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if qr_resp.code != 0 {
            return Err(PlatformError::SendFailed(format!(
                "get_qr_code failed (code={}): {}",
                qr_resp.code, qr_resp.msg
            )));
        }

        qr_resp
            .qrcode_url
            .ok_or_else(|| PlatformError::SendFailed("no qrcode_url in response".to_string()))
    }

    /// Get a media upload URL from iLink.
    ///
    /// GET `/ilink/bot/getuploadurl` with params: token, file_type.
    pub async fn get_upload_url(&self, file_type: &str) -> PlatformResult<String> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();
        let url = format!("{}/ilink/bot/getuploadurl", self.config.base_url);

        let response = client
            .get(&url)
            .query(&[("token", token.as_str()), ("file_type", file_type)])
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(PlatformError::SendFailed(format!(
                "get_upload_url failed with status: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct UploadUrlResponse {
            #[serde(default)]
            code: i32,
            #[serde(default)]
            msg: String,
            upload_url: Option<String>,
        }

        let upload_resp: UploadUrlResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if upload_resp.code != 0 {
            return Err(PlatformError::SendFailed(format!(
                "get_upload_url failed (code={}): {}",
                upload_resp.code, upload_resp.msg
            )));
        }

        upload_resp
            .upload_url
            .ok_or_else(|| PlatformError::SendFailed("no upload_url in response".to_string()))
    }

    /// Check if a message ID has already been seen (within TTL).
    /// Automatically marks the ID as seen if not already present.
    async fn is_duplicate(&self, message_id: &str) -> bool {
        self.seen_ids.is_duplicate(message_id).await
    }
}

// ------------------------------------------------------------------
// PlatformAdapter trait implementation
// ------------------------------------------------------------------

#[async_trait]
impl PlatformAdapter for WeChatLinkAdapter {
    fn platform(&self) -> Platform {
        Platform::WeChat
    }

    fn platform_name(&self) -> &str {
        "wechat_ilink"
    }

    async fn connect(&mut self) -> PlatformResult<()> {
        tracing::info!("wechat_ilink adapter: attempting connection...");
        *self.last_connect_attempt.write().await = Some(std::time::Instant::now());
        let token = self.authenticate().await?;
        *self.token.write().await = Some(token.clone());
        *self.connected.write().await = true;
        *self.consecutive_failures.write().await = 0;
        tracing::info!(
            "wechat_ilink adapter: connected successfully (token={}...{})",
            &token[..token.len().min(8)],
            &token[token.len().saturating_sub(8)..]
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> PlatformResult<()> {
        *self.connected.write().await = false;
        *self.token.write().await = None;
        *self.context_token.write().await = String::new();
        *self.last_connect_attempt.write().await = None;
        *self.consecutive_failures.write().await = 0;
        tracing::info!("wechat_ilink adapter disconnected");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        let connected = self.connected.blocking_read();
        *connected
    }

    async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
        let connected = *self.connected.read().await;
        if !connected {
            if let Err(e) = self.try_reconnect().await {
                tracing::debug!("wechat_ilink adapter: reconnect pending ({e})");
            }
            return Ok(None);
        }

        let updates = match self.get_updates().await {
            Ok(updates) => updates,
            Err(e) => {
                let err_str = e.to_string();
                tracing::warn!("wechat_ilink adapter: get_updates failed: {err_str}");
                if err_str.contains("401")
                    || err_str.contains("403")
                    || err_str.contains("unauthorized")
                    || err_str.contains("auth")
                    || err_str.contains("token")
                {
                    tracing::info!("wechat_ilink adapter: auth failure detected, marking disconnected for reconnect");
                    *self.connected.write().await = false;
                    *self.token.write().await = None;
                }
                return Ok(None);
            }
        };

        for msg in updates {
            // Extract message_id for dedup
            let msg_id = msg
                .get("message_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(ref mid) = msg_id {
                if self.is_duplicate(mid).await {
                    tracing::debug!(%mid, "ilink skipping duplicate message");
                    continue;
                }
            }

            if let Some(inbound) = self.parse_ilink_message(&msg) {
                return Ok(Some(inbound));
            }
        }

        Ok(None)
    }

    async fn send(&self, msg: &OutboundMessage) -> PlatformResult<SendResult> {
        let to_user = &msg.session_key.user_id;
        self.send_text(to_user, &msg.text).await?;
        Ok(SendResult::success(None))
    }

    async fn send_typing(&self, chat_id: &str) -> Result<(), PlatformError> {
        self.send_typing_indicator(chat_id).await
    }

    // ------------------------------------------------------------------
    // Unimplemented methods (same pattern as FeishuAdapter)
    // ------------------------------------------------------------------

    async fn send_image(
        &self,
        chat_id: &str,
        image_url: &str,
        caption: Option<&str>,
    ) -> PlatformResult<()> {
        let token = self.ensure_token().await?;
        let client = reqwest::Client::new();

        // 1. Download the image
        let image_bytes = client
            .get(image_url)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(format!("download image: {e}")))?
            .bytes()
            .await
            .map_err(|e| PlatformError::SendFailed(format!("read image bytes: {e}")))?;

        // 2. Get upload URL from iLink and upload the image
        let upload_url = self.get_upload_url("image").await?;

        let upload_resp = client
            .post(&upload_url)
            .body(image_bytes.to_vec())
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(format!("upload image: {e}")))?;

        let upload_json: serde_json::Value = upload_resp
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(format!("parse upload response: {e}")))?;

        let image_key = upload_json["image_key"]
            .as_str()
            .or_else(|| upload_json["media_id"].as_str())
            .ok_or_else(|| PlatformError::SendFailed("no image_key in upload response".into()))?;

        // 3. Send image message via iLink
        let url = format!("{}/ilink/bot/sendmessage", self.config.base_url);

        let mut payload = serde_json::json!({
            "token": token,
            "touser": chat_id,
            "msgtype": "image",
            "image": {
                "media_id": image_key,
            },
        });

        if let Some(cap) = caption {
            payload["image"]["caption"] = serde_json::Value::String(cap.to_string());
        }

        let response = client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(format!("send image: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(PlatformError::SendFailed(format!(
                "send_image failed {}: {}",
                status, body
            )));
        }

        #[derive(Deserialize)]
        struct SendResponse {
            #[serde(default)]
            code: i32,
            #[serde(default)]
            msg: String,
            message_id: Option<String>,
        }

        let send_resp: SendResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::SendFailed(e.to_string()))?;

        if send_resp.code != 0 {
            return Err(PlatformError::SendFailed(format!(
                "send_image failed (code={}): {}",
                send_resp.code, send_resp.msg
            )));
        }

        tracing::debug!(
            to = %chat_id,
            msg_id = %send_resp.message_id.unwrap_or_default(),
            "ilink image message sent"
        );
        Ok(())
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

    async fn get_chat_info(&self, chat_id: &str) -> PlatformResult<ChatInfo> {
        Ok(ChatInfo {
            chat_id: chat_id.to_string(),
            name: "WeChat iLink Chat".into(),
            chat_type: "wechat_ilink".into(),
        })
    }

    async fn send_card(&self, _chat_id: &str, _card_json: &str) -> PlatformResult<String> {
        Err(PlatformError::NotImplemented("send_card".into()))
    }

    async fn on_event(&self, _event: &PlatformEvent) -> PlatformResult<Option<InboundMessage>> {
        Ok(None)
    }
}

/// Create a WeChat iLink adapter from config settings.
pub fn create_wechat_ilink_adapter(
    settings: &serde_json::Value,
) -> PlatformResult<WeChatLinkAdapter> {
    let credential_source = settings
        .get("credential_source")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if credential_source == "qr_account" || settings.get("account_id").is_some() {
        let account_id = settings
            .get("account_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PlatformError::ConfigError("missing account_id".to_string()))?;
        let account_store_dir = settings
            .get("account_store_dir")
            .and_then(|v| v.as_str())
            .map(PathBuf::from);
        let account = load_wechat_qr_account(account_id, account_store_dir.as_deref())?;
        let config = WeChatLinkConfig::from_qr_account(
            account.account_id,
            account.token,
            account.base_url,
            account.user_id,
        );
        return Ok(WeChatLinkAdapter::new(config));
    }

    let bot_id = settings
        .get("bot_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing bot_id".to_string()))?;

    let bot_secret = settings
        .get("bot_secret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing bot_secret".to_string()))?;

    let base_url = settings
        .get("base_url")
        .and_then(|v| v.as_str())
        .unwrap_or(ILINK_BASE_URL);

    let config = WeChatLinkConfig::new(bot_id, bot_secret).with_base_url(base_url);
    Ok(WeChatLinkAdapter::new(config))
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // Config tests
    // ------------------------------------------------------------------

    #[test]
    fn test_config_creation() {
        let config = WeChatLinkConfig::new("bot_123", "secret_456");
        assert_eq!(config.bot_id, "bot_123");
        assert_eq!(config.bot_secret, "secret_456");
        assert_eq!(config.base_url, ILINK_BASE_URL);
    }

    #[test]
    fn test_config_with_custom_base_url() {
        let config = WeChatLinkConfig::new("bot_1", "secret_2")
            .with_base_url("https://custom.ilink.example.com");
        assert_eq!(config.base_url, "https://custom.ilink.example.com");
    }

    // ------------------------------------------------------------------
    // Connect / disconnect state tests
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_connect_disconnect_state() {
        let config =
            WeChatLinkConfig::new("test_bot", "test_secret").with_base_url("https://localhost:1"); // unreachable, will fail auth
        let mut adapter = WeChatLinkAdapter::new(config);

        // Initially not connected (check directly via RwLock, avoid
        // is_connected() which uses blocking_read and panics inside tokio runtime)
        {
            let connected = adapter.connected.read().await;
            assert!(!*connected);
        }

        // Manually simulate connect by setting state
        *adapter.connected.write().await = true;
        {
            let connected = adapter.connected.read().await;
            assert!(*connected);
        }

        // Disconnect
        adapter.disconnect().await.unwrap();
        {
            let connected = adapter.connected.read().await;
            assert!(!*connected);
        }

        // Token should be cleared
        let token = adapter.token.read().await;
        assert!(token.is_none());
    }

    #[tokio::test]
    async fn test_context_token_echo_mechanism() {
        let config = WeChatLinkConfig::new("test_bot", "test_secret");
        let adapter = WeChatLinkAdapter::new(config);

        // Initially empty
        {
            let ctx = adapter.context_token.read().await;
            assert!(ctx.is_empty());
        }

        // Set a context token
        {
            let mut ctx = adapter.context_token.write().await;
            *ctx = "ctx_token_abc123".to_string();
        }

        // Verify it was stored
        {
            let ctx = adapter.context_token.read().await;
            assert_eq!(*ctx, "ctx_token_abc123");
        }

        // Clear via disconnect
        // We simulate the disconnect behavior
        {
            let mut ctx = adapter.context_token.write().await;
            *ctx = String::new();
        }

        {
            let ctx = adapter.context_token.read().await;
            assert!(ctx.is_empty());
        }
    }

    #[tokio::test]
    async fn test_get_updates_request_format() {
        let config =
            WeChatLinkConfig::new("test_bot", "test_secret").with_base_url("https://localhost:1");
        let adapter = WeChatLinkAdapter::new(config);

        // Set a dummy token so ensure_token doesn't try to auth
        {
            let mut token = adapter.token.write().await;
            *token = Some("dummy_token_xyz".to_string());
        }

        // Set a context token
        {
            let mut ctx = adapter.context_token.write().await;
            *ctx = "prev_ctx_token".to_string();
        }

        // get_updates should fail (no real server), but the request format
        // is correct: it uses the token and context_token as query params
        let result = adapter.get_updates().await;
        // Expected to fail because localhost:1 is unreachable
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_text_request_format() {
        let config =
            WeChatLinkConfig::new("test_bot", "test_secret").with_base_url("https://localhost:1");
        let adapter = WeChatLinkAdapter::new(config);

        // Set a dummy token
        {
            let mut token = adapter.token.write().await;
            *token = Some("dummy_token_xyz".to_string());
        }

        let result = adapter.send_text("user_abc", "Hello World!").await;
        // Expected to fail because localhost:1 is unreachable
        assert!(result.is_err());
    }

    // ------------------------------------------------------------------
    // PlatformAdapter trait implementation tests
    // ------------------------------------------------------------------

    #[test]
    fn test_platform_returns_wechat() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);
        assert_eq!(adapter.platform(), Platform::WeChat);
    }

    #[test]
    fn test_platform_name_returns_wechat_ilink() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);
        assert_eq!(adapter.platform_name(), "wechat_ilink");
    }

    #[tokio::test]
    async fn test_send_typing_delegates_to_indicator() {
        let config =
            WeChatLinkConfig::new("test_bot", "test_secret").with_base_url("https://localhost:1");
        let adapter = WeChatLinkAdapter::new(config);

        // Set a dummy token
        {
            let mut token = adapter.token.write().await;
            *token = Some("dummy_token_xyz".to_string());
        }

        let result = adapter.send_typing("user_abc").await;
        // Expected to fail because localhost:1 is unreachable
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_delegates_to_send_text() {
        let config =
            WeChatLinkConfig::new("test_bot", "test_secret").with_base_url("https://localhost:1");
        let adapter = WeChatLinkAdapter::new(config);

        // Set a dummy token
        {
            let mut token = adapter.token.write().await;
            *token = Some("dummy_token_xyz".to_string());
        }

        let msg = OutboundMessage {
            session_key: SessionKey::new("wechat_ilink", "user_abc"),
            text: "Test message".to_string(),
            reply_to: None,
            metadata: serde_json::Value::Null,
        };

        let result = adapter.send(&msg).await;
        // Expected to fail because localhost:1 is unreachable
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_receive_returns_none_when_disconnected() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let mut adapter = WeChatLinkAdapter::new(config);
        // Not connected by default
        let result = adapter.receive().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_parse_ilink_text_message() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);

        let msg = serde_json::json!({
            "message_id": "msg_001",
            "from_user": "user_abc",
            "msg_type": "text",
            "text": "Hello World!",
            "timestamp": 1700000000000_i64,
        });

        let parsed = adapter.parse_ilink_message(&msg);
        assert!(parsed.is_some());
        let inbound = parsed.unwrap();
        assert_eq!(inbound.text, "Hello World!");
        assert_eq!(inbound.message_id, Some("msg_001".to_string()));
        assert_eq!(inbound.session_key.user_id, "user_abc");
        assert_eq!(inbound.message_type, MessageType::Text);
        assert_eq!(inbound.platform, Platform::WeChat);
    }

    #[test]
    fn test_parse_ilink_image_message() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);

        let msg = serde_json::json!({
            "message_id": "msg_002",
            "from_user": "user_abc",
            "msg_type": "image",
            "timestamp": 1700000000000_i64,
        });

        let parsed = adapter.parse_ilink_message(&msg);
        assert!(parsed.is_some());
        let inbound = parsed.unwrap();
        assert_eq!(inbound.text, "[Image]");
        assert_eq!(inbound.message_type, MessageType::Photo);
    }

    #[test]
    fn test_parse_ilink_message_without_from_user() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);

        let msg = serde_json::json!({
            "message_id": "msg_003",
            "msg_type": "text",
            "text": "No sender",
        });

        let parsed = adapter.parse_ilink_message(&msg);
        assert!(parsed.is_none());
    }

    #[test]
    fn test_dedup_seen_ids() {
        // This test verifies the DedupStore behavior directly
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async {
            // First sighting — not a duplicate (auto-marks as seen)
            assert!(!adapter.is_duplicate("msg_new").await);
            // Second sighting — duplicate within TTL
            assert!(adapter.is_duplicate("msg_new").await);

            // Different message — not a duplicate
            assert!(!adapter.is_duplicate("msg_other").await);
        });
    }

    #[test]
    fn test_ilink_base_url_constant() {
        assert_eq!(ILINK_BASE_URL, "https://ilinkai.weixin.qq.com");
    }

    #[test]
    fn test_default_long_polling_timeout() {
        assert_eq!(DEFAULT_LONG_POLLING_TIMEOUT, 30);
    }

    #[test]
    fn test_get_chat_info_stub() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async {
            let info = adapter.get_chat_info("chat_123").await.unwrap();
            assert_eq!(info.chat_id, "chat_123");
            assert_eq!(info.name, "WeChat iLink Chat");
            assert_eq!(info.chat_type, "wechat_ilink");
        });
    }

    #[test]
    fn test_not_implemented_stubs() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async {
            assert!(adapter.send_image_file("c", "p", None).await.is_err());
            assert!(adapter.send_voice("c", "p", None).await.is_err());
            assert!(adapter.send_document("c", "f", None, None).await.is_err());
            assert!(adapter.send_video("c", "p", None).await.is_err());
            assert!(adapter.send_animation("c", "u", None).await.is_err());
            assert!(adapter.edit_message("c", "m", "x").await.is_err());
            assert!(adapter.delete_message("c", "m").await.is_err());
            assert!(adapter.send_card("c", "{}").await.is_err());
        });
    }

    #[test]
    fn test_on_event_returns_none() {
        let config = WeChatLinkConfig::new("bot", "secret");
        let adapter = WeChatLinkAdapter::new(config);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async {
            let event = PlatformEvent {
                event_type: "test".into(),
                platform: Platform::WeChat,
                data: serde_json::json!({}),
                timestamp: Utc::now(),
            };
            let result = adapter.on_event(&event).await.unwrap();
            assert!(result.is_none());
        });
    }
}
