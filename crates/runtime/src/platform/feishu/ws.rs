//! Feishu WebSocket event push client.
//!
//! Implements Feishu's official WebSocket long-connection event subscription,
//! matching the Hermes `_run_official_feishu_ws_client` pattern.
//!
//! # Flow
//! 1. POST tenant_access_token/internal to get bearer token
//! 2. POST event/v1/app/report_pin to register and receive a WebSocket URL
//! 3. Connect to the URL via tokio-tungstenite
//! 4. Spawn a background reader task that forwards incoming events through a mpsc channel
//! 5. Auto-reconnect on disconnect (configurable attempts + interval)
//!
//! # Graceful shutdown
//! Drop the receiver returned by [`FeishuWsClient::connect`]; the background task exits
//! after the next send attempt fails.

use crate::platform::adapter::{PlatformError, PlatformResult};
use crate::platform::feishu::types::{TenantTokenRequest, TenantTokenResponse};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Pin registration types
// ---------------------------------------------------------------------------

/// Request body sent to the `report_pin` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PinRegisterRequest {
    app_id: String,
}

/// Response from the `report_pin` endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct PinRegisterResponse {
    code: i32,
    msg: String,
    data: Option<PinRegisterData>,
}

impl Default for PinRegisterResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Data payload inside the `report_pin` response.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct PinRegisterData {
    ws_url: Option<String>,
    pin: Option<String>,
}

// ---------------------------------------------------------------------------
// FeishuWsClient
// ---------------------------------------------------------------------------

/// Feishu WebSocket event push client.
///
/// Connects to Feishu's event push service via a WebSocket long connection,
/// receiving real-time events such as message notifications, reaction events, etc.
///
/// # Example (conceptual — requires valid credentials)
///
/// ```ignore
/// let client = FeishuWsClient::new("cli_xxx", "secret_xxx")
///     .with_reconnect(30, 120);
/// let mut rx = client.connect().await?;
/// while let Some(event) = rx.recv().await {
///     println!("received: {:?}", event);
/// }
/// ```
pub struct FeishuWsClient {
    app_id: String,
    app_secret: String,
    ws_url: String,
    reconnect_max_attempts: u32,
    reconnect_interval_secs: u64,
}

impl FeishuWsClient {
    /// Create a new client with default reconnect settings
    /// (30 attempts, 120-second interval).
    pub fn new(app_id: &str, app_secret: &str) -> Self {
        Self {
            app_id: app_id.to_string(),
            app_secret: app_secret.to_string(),
            ws_url: String::new(),
            reconnect_max_attempts: 30,
            reconnect_interval_secs: 120,
        }
    }

    /// Override reconnect behaviour.
    ///
    /// * `max_attempts` — total reconnect tries before giving up (0 = no reconnect).
    /// * `interval_secs` — seconds to wait between reconnect attempts.
    pub fn with_reconnect(mut self, max_attempts: u32, interval_secs: u64) -> Self {
        self.reconnect_max_attempts = max_attempts;
        self.reconnect_interval_secs = interval_secs;
        self
    }

    /// Connect to Feishu event push and start receiving events.
    ///
    /// Returns an unbounded receiver that yields [`serde_json::Value`] for every
    /// incoming WebSocket text message (after challenge handshake).
    ///
    /// The background reader task automatically reconnects on disconnect up to
    /// `reconnect_max_attempts` times.  Drop the receiver to trigger graceful
    /// shutdown.
    pub async fn connect(&self) -> PlatformResult<mpsc::UnboundedReceiver<serde_json::Value>> {
        // 1. Authenticate — obtain tenant access token
        let token = get_tenant_access_token(&self.app_id, &self.app_secret).await?;

        // 2. Register pin — get WebSocket URL (use stored if set)
        let ws_url = if self.ws_url.is_empty() {
            register_pin(&token, &self.app_id).await?
        } else {
            self.ws_url.clone()
        };

        // 3. Create event channel
        let (tx, rx) = mpsc::unbounded_channel();

        // 4. Spawn background reader with reconnect loop
        let app_id = self.app_id.clone();
        let app_secret = self.app_secret.clone();
        let max_attempts = self.reconnect_max_attempts;
        let interval_secs = self.reconnect_interval_secs;

        tokio::spawn(async move {
            reader_loop(ws_url, app_id, app_secret, tx, max_attempts, interval_secs).await;
        });

        Ok(rx)
    }
}

// ---------------------------------------------------------------------------
// Auth helper
// ---------------------------------------------------------------------------

/// Obtain a tenant access token from Feishu.
async fn get_tenant_access_token(app_id: &str, app_secret: &str) -> PlatformResult<String> {
    let client = reqwest::Client::new();
    let req_body = TenantTokenRequest {
        app_id: app_id.to_string(),
        app_secret: app_secret.to_string(),
    };

    let resp = client
        .post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal")
        .json(&req_body)
        .send()
        .await
        .map_err(|e| PlatformError::ConnectionFailed(format!("auth request failed: {e}")))?;

    let body: TenantTokenResponse = resp
        .json()
        .await
        .map_err(|e| PlatformError::AuthenticationFailed(format!("parse auth response: {e}")))?;

    if body.code != 0 {
        return Err(PlatformError::AuthenticationFailed(format!(
            "auth error {}: {}",
            body.code, body.msg
        )));
    }

    body.tenant_access_token
        .ok_or_else(|| PlatformError::AuthenticationFailed("no tenant_access_token in response".into()))
}

// ---------------------------------------------------------------------------
// Pin registration helper
// ---------------------------------------------------------------------------

/// Register the app with Feishu's event push service and return the WebSocket URL.
///
/// POST `https://open.feishu.cn/open-apis/event/v1/app/report_pin`
/// Body: `{"app_id": app_id}`
/// Response `data.ws_url` contains the WebSocket endpoint.
pub async fn register_pin(token: &str, app_id: &str) -> PlatformResult<String> {
    let client = reqwest::Client::new();
    let req_body = PinRegisterRequest {
        app_id: app_id.to_string(),
    };

    let resp = client
        .post("https://open.feishu.cn/open-apis/event/v1/app/report_pin")
        .bearer_auth(token)
        .json(&req_body)
        .send()
        .await
        .map_err(|e| PlatformError::ConnectionFailed(format!("pin register request failed: {e}")))?;

    let body: PinRegisterResponse = resp
        .json()
        .await
        .map_err(|e| PlatformError::ConnectionFailed(format!("parse pin response: {e}")))?;

    if body.code != 0 {
        return Err(PlatformError::ConnectionFailed(format!(
            "pin register error {}: {}",
            body.code, body.msg
        )));
    }

    body.data
        .and_then(|d| d.ws_url)
        .ok_or_else(|| PlatformError::ConnectionFailed("no ws_url in pin response".into()))
}

// ---------------------------------------------------------------------------
// Background reader / reconnect loop
// ---------------------------------------------------------------------------

/// Long-running background task that reads from the WebSocket, auto-reconnects,
/// and pushes every text frame into the mpsc channel.
async fn reader_loop(
    mut ws_url: String,
    app_id: String,
    app_secret: String,
    tx: mpsc::UnboundedSender<serde_json::Value>,
    max_attempts: u32,
    interval_secs: u64,
) {
    let mut attempt: u32 = 0;

    'outer: loop {
        if max_attempts > 0 && attempt >= max_attempts {
            tracing::warn!(
                "Feishu WS: reached max reconnect attempts ({max_attempts}), exiting reader"
            );
            break;
        }

        if attempt > 0 {
            tracing::info!(
                "Feishu WS: reconnecting in {interval_secs}s (attempt {attempt}/{max_attempts})"
            );
            tokio::time::sleep(Duration::from_secs(interval_secs)).await;

            // Re-authenticate and re-register on every reconnect
            match get_tenant_access_token(&app_id, &app_secret).await {
                Ok(token) => match register_pin(&token, &app_id).await {
                    Ok(url) => ws_url = url,
                    Err(e) => {
                        tracing::warn!("Feishu WS: pin re-register failed: {e}");
                        attempt += 1;
                        continue 'outer;
                    }
                },
                Err(e) => {
                    tracing::warn!("Feishu WS: re-auth failed: {e}");
                    attempt += 1;
                    continue 'outer;
                }
            }
        }

        // Connect WebSocket
        let ws_stream = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!("Feishu WS: connect failed: {e}");
                attempt += 1;
                continue 'outer;
            }
        };

        tracing::info!("Feishu WS: connected to {ws_url}");
        attempt = 0; // Reset counter on successful connection

        // Inner read loop
        match ws_read_loop(ws_stream, &tx).await {
            Ok(true) => {
                // Receiver dropped — clean exit, no reconnect
                tracing::info!("Feishu WS: receiver closed, shutting down reader");
                return;
            }
            Ok(false) | Err(()) => {
                // Connection lost — fall through to reconnect
            }
        }

        // Connection lost — increment and retry
        attempt += 1;
    }
}

/// Read loop for a single WebSocket connection.
///
/// Returns `Ok(true)` when the receiver is closed (graceful shutdown).
/// Returns `Ok(false)` / `Err(...)` when the connection dropped and should reconnect.
async fn ws_read_loop(
    mut ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    tx: &mpsc::UnboundedSender<serde_json::Value>,
) -> Result<bool, ()> {
    let mut first_message = true;

    loop {
        let msg = match ws_stream.next().await {
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(Message::Ping(_))) => {
                // Respond with pong
                let _ = ws_stream
                    .send(Message::Pong(vec![]))
                    .await;
                continue;
            }
            Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | None => {
                tracing::info!("Feishu WS: connection closed by server");
                return Ok(false);
            }
            Some(Err(e)) => {
                tracing::warn!("Feishu WS: read error: {e}");
                return Err(());
            }
            _ => continue,
        };

        // Parse the message as JSON
        let value: serde_json::Value = match serde_json::from_str(&msg) {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("Feishu WS: non-JSON message: {e}");
                continue;
            }
        };

        // Challenge verification — first message may contain a challenge token
        if first_message {
            first_message = false;
            if let Some(challenge) = value.get("challenge").and_then(|v| v.as_str()) {
                let response = serde_json::json!({"challenge": challenge});
                if let Ok(resp_text) = serde_json::to_string(&response) {
                    let _ = ws_stream.send(Message::Text(resp_text)).await;
                }
                tracing::info!("Feishu WS: challenge handshake completed");
                continue;
            }
        }

        // Forward event to the channel
        if tx.send(value).is_err() {
            // Receiver was dropped — graceful shutdown
            return Ok(true);
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Construction ---------------------------------------------------------

    #[test]
    fn test_feishu_ws_client_construction() {
        let client = FeishuWsClient::new("cli_test", "secret_test");
        assert_eq!(client.app_id, "cli_test");
        assert_eq!(client.app_secret, "secret_test");
        assert_eq!(client.reconnect_max_attempts, 30);
        assert_eq!(client.reconnect_interval_secs, 120);
        assert!(client.ws_url.is_empty());
    }

    #[test]
    fn test_reconnect_settings_are_stored() {
        let client = FeishuWsClient::new("app", "sec")
            .with_reconnect(5, 30);
        assert_eq!(client.reconnect_max_attempts, 5);
        assert_eq!(client.reconnect_interval_secs, 30);
    }

    #[test]
    fn test_with_reconnect_zero_attempts() {
        let client = FeishuWsClient::new("app", "sec")
            .with_reconnect(0, 60);
        assert_eq!(client.reconnect_max_attempts, 0);
        assert_eq!(client.reconnect_interval_secs, 60);
    }

    // -- Pin registration request format -------------------------------------

    #[test]
    fn test_pin_registration_request_body_format() {
        let req = PinRegisterRequest {
            app_id: "cli_abc123".to_string(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(value["app_id"], "cli_abc123");

        // Roundtrip
        let parsed: PinRegisterRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.app_id, "cli_abc123");
    }

    #[test]
    fn test_pin_register_response_deserialization() {
        let raw = r#"{
            "code": 0,
            "msg": "ok",
            "data": {
                "ws_url": "wss://open.feishu.cn/ws/event/abc",
                "pin": "12345"
            }
        }"#;
        let parsed: PinRegisterResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "ok");
        let data = parsed.data.expect("data present");
        assert_eq!(data.ws_url.as_deref(), Some("wss://open.feishu.cn/ws/event/abc"));
        assert_eq!(data.pin.as_deref(), Some("12345"));
    }

    #[test]
    fn test_pin_register_response_error() {
        let raw = r#"{"code": 99991663, "msg": "invalid app_id"}"#;
        let parsed: PinRegisterResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 99991663);
        assert_eq!(parsed.msg, "invalid app_id");
        assert!(parsed.data.is_none());
    }

    // -- Channel creation (no network) ----------------------------------------

    #[tokio::test]
    async fn test_channel_creation_and_single_event() {
        // Verify mpsc channel mechanics without actual network
        let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();

        let event = serde_json::json!({"type": "im.message.receive_v1", "data": {}});
        tx.send(event.clone()).expect("send event");

        let received = rx.recv().await.expect("receive event");
        assert_eq!(received, event);
    }

    // -- Shutdown propagation ------------------------------------------------

    #[tokio::test]
    async fn test_shutdown_drop_receiver_propagates_to_sender() {
        let (tx, rx) = mpsc::unbounded_channel::<serde_json::Value>();

        // Drop receiver
        drop(rx);

        // Sender should detect closed channel
        let result = tx.send(serde_json::Value::Null);
        assert!(result.is_err(), "send should fail after receiver dropped");
    }

    #[tokio::test]
    async fn test_shutdown_receiver_returns_none_after_drop() {
        let (tx, rx) = mpsc::unbounded_channel::<serde_json::Value>();

        // Send one event then drop sender
        tx.send(serde_json::json!({"msg": "hello"})).expect("send");
        drop(tx);

        let mut rx = rx;
        let first = rx.recv().await;
        assert!(first.is_some(), "should receive the sent event");

        let second = rx.recv().await;
        assert!(second.is_none(), "should return None after sender dropped");
    }

    // -- reader_loop not tested here — requires a live WebSocket -----------
}
