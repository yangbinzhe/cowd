//! Feishu WebSocket event push client.
//!
//! Implements Feishu's official WebSocket v2 long-connection event subscription,
//! matching the Hermes `_run_official_feishu_ws_client` pattern and the Python
//! SDK's `lark_oapi` protocol stack.
//!
//! # Flow
//! 1. POST callback/ws/endpoint with AppID/AppSecret to get WebSocket URL
//! 2. Connect to the URL via tokio-tungstenite
//! 3. Spawn a background reader task that decodes protobuf frames and forwards
//!    JSON events through an mpsc channel
//! 4. Spawn a writer task for sending response frames and PING heartbeats
//! 5. Auto-reconnect on disconnect (configurable attempts + interval)
//!
//! # Protocol
//!
//! Feishu WS v2 wraps every message in a protobuf [`Frame`].  The frame method
//! determines the message type:
//!
//! - `FRAME_CONTROL` (0): PING / PONG heartbeat
//! - `FRAME_DATA` (1): event / card notification
//!
//! Response frames MUST be sent back after processing DATA frames to
//! acknowledge receipt.  The client also sends periodic protobuf PING frames
//! to keep the connection alive.
//!
//! # Graceful shutdown
//! Drop the receiver returned by [`FeishuWsClient::connect`]; the background task exits
//! after the next send attempt fails.

use super::decode_feishu_response;
use crate::platform::adapter::{PlatformError, PlatformResult};
use crate::platform::feishu::proto::{
    Frame, Header, FRAME_CONTROL, FRAME_DATA, HEADER_BIZ_RT, HEADER_MESSAGE_ID, HEADER_SEQ,
    HEADER_SUM, HEADER_TYPE, MSG_PING, MSG_PONG,
};
use futures::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Pin registration types
// ---------------------------------------------------------------------------

/// Public result returned by [`register_pin`].
pub struct RegisterResult {
    pub ws_url: String,
    /// From ClientConfig.PingInterval
    pub ping_interval: Option<i64>,
    /// From ClientConfig.ReconnectCount (-1 = unlimited)
    pub reconnect_count: Option<i64>,
    /// From ClientConfig.ReconnectInterval
    pub reconnect_interval: Option<i64>,
    /// From ClientConfig.ReconnectNonce
    pub reconnect_nonce: Option<i64>,
}

/// Response from `POST https://open.feishu.cn/callback/ws/endpoint`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct EndpointResponse {
    code: i32,
    msg: String,
    data: Option<EndpointData>,
}

/// Data payload inside the `callback/ws/endpoint` response.
#[derive(Debug, Clone, Default, Deserialize)]
struct EndpointData {
    #[serde(rename = "URL")]
    url: Option<String>,
    #[serde(rename = "ClientConfig")]
    client_config: Option<ClientConfigData>,
}

/// ClientConfig fields returned by the endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
struct ClientConfigData {
    #[serde(rename = "PingInterval")]
    ping_interval: Option<i64>,
    #[serde(rename = "ReconnectCount")]
    reconnect_count: Option<i64>,
    #[serde(rename = "ReconnectInterval")]
    reconnect_interval: Option<i64>,
    #[serde(rename = "ReconnectNonce")]
    reconnect_nonce: Option<i64>,
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
    base_url: String,
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
            base_url: "https://open.feishu.cn".to_string(),
            reconnect_max_attempts: 30,
            reconnect_interval_secs: 120,
        }
    }

    /// Set the base API domain (e.g. "https://open.larksuite.com" for Lark).
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into().trim_end_matches('/').to_string();
        self
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
    /// incoming data-frame payload (decoded from protobuf).
    ///
    /// The background reader task decodes protobuf [`Frame`] messages, handles
    /// PING/PONG control frames, assembles multi-part messages, and
    /// auto-reconnects on disconnect up to `reconnect_max_attempts` times.
    /// Drop the receiver to trigger graceful shutdown.
    pub async fn connect(&self) -> PlatformResult<mpsc::UnboundedReceiver<serde_json::Value>> {
        // Register pin — get WebSocket URL and client config
        let result = register_pin(&self.app_id, &self.app_secret, &self.base_url).await?;
        let ws_url = result.ws_url;
        let ping_interval_secs = result.ping_interval.map(|v| v.max(1) as u64).unwrap_or(90);

        // Create event channel
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn background reader with reconnect loop
        let app_id = self.app_id.clone();
        let app_secret = self.app_secret.clone();
        let base_url = self.base_url.clone();
        let max_attempts = self.reconnect_max_attempts;
        let interval_secs = self.reconnect_interval_secs;

        tokio::spawn(async move {
            reader_loop(
                ws_url,
                app_id,
                app_secret,
                base_url,
                tx,
                max_attempts,
                interval_secs,
                ping_interval_secs,
            )
            .await;
        });

        Ok(rx)
    }
}

// ---------------------------------------------------------------------------
// Pin registration helper
// ---------------------------------------------------------------------------

/// Register with Feishu's WebSocket event push and return connection details.
///
/// POST `https://open.feishu.cn/callback/ws/endpoint` with AppID/AppSecret in body
/// (NO Bearer token).  Returns [`RegisterResult`] with the WS URL and client config.
pub async fn register_pin(
    app_id: &str,
    app_secret: &str,
    base_url: &str,
) -> PlatformResult<RegisterResult> {
    let client = reqwest::Client::new();
    let endpoint = format!("{}/callback/ws/endpoint", base_url);

    let resp = client
        .post(&endpoint)
        .header("locale", "zh")
        .json(&serde_json::json!({
            "AppID": app_id,
            "AppSecret": app_secret,
        }))
        .send()
        .await
        .map_err(|e| {
            PlatformError::ConnectionFailed(format!("pin register request failed: {e}"))
        })?;

    let body: EndpointResponse = decode_feishu_response(resp, "register pin").await?;

    if body.code != 0 {
        return Err(PlatformError::ConnectionFailed(format!(
            "pin register error {}: {}",
            body.code, body.msg
        )));
    }

    let data = body
        .data
        .ok_or_else(|| PlatformError::ConnectionFailed("no data in pin response".into()))?;

    let ws_url = data
        .url
        .ok_or_else(|| PlatformError::ConnectionFailed("no URL in pin response".into()))?;

    Ok(RegisterResult {
        ws_url,
        ping_interval: data.client_config.as_ref().and_then(|c| c.ping_interval),
        reconnect_count: data.client_config.as_ref().and_then(|c| c.reconnect_count),
        reconnect_interval: data
            .client_config
            .as_ref()
            .and_then(|c| c.reconnect_interval),
        reconnect_nonce: data.client_config.as_ref().and_then(|c| c.reconnect_nonce),
    })
}

// ---------------------------------------------------------------------------
// Background reader / reconnect loop
// ---------------------------------------------------------------------------

/// Long-running background task that reads from the WebSocket, auto-reconnects,
/// and pushes every data-frame payload into the mpsc channel.
///
/// On each connection it:
/// 1. Splits the WebSocket into a sink (writer) and source (reader).
/// 2. Spawns a writer task that forwards bytes from a channel to the sink.
/// 3. Spawns a PING heartbeat task that sends protobuf PING frames.
/// 4. Runs the protobuf frame read loop on the source.
async fn reader_loop(
    mut ws_url: String,
    app_id: String,
    app_secret: String,
    base_url: String,
    tx: mpsc::UnboundedSender<serde_json::Value>,
    max_attempts: u32,
    interval_secs: u64,
    mut ping_interval_secs: u64,
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

            // Re-register on every reconnect
            match register_pin(&app_id, &app_secret, &base_url).await {
                Ok(result) => {
                    ws_url = result.ws_url;
                    if let Some(pi) = result.ping_interval {
                        ping_interval_secs = pi.max(1) as u64;
                    }
                }
                Err(e) => {
                    tracing::warn!("Feishu WS: pin re-register failed: {e}");
                    attempt += 1;
                    continue 'outer;
                }
            }
        }

        let service_id = extract_service_id(&ws_url);

        // Connect WebSocket
        let (ws_stream, _) = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok(tuple) => tuple,
            Err(e) => {
                tracing::warn!("Feishu WS: connect failed: {e}");
                attempt += 1;
                continue 'outer;
            }
        };

        tracing::info!("Feishu WS: connected to {ws_url} (service_id={service_id})");
        attempt = 0;

        let (ws_sink, ws_source) = ws_stream.split();
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        // Writer task: forward outgoing protobuf bytes to WebSocket
        tokio::spawn(async move {
            let mut sink = ws_sink;
            while let Some(bytes) = write_rx.recv().await {
                if sink.send(Message::Binary(bytes.into())).await.is_err() {
                    break;
                }
            }
        });

        // PING heartbeat task
        let ping_tx = write_tx.clone();
        let ping_secs = ping_interval_secs;
        let ping_service_id = service_id;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(ping_secs)).await;
                let ping_frame = create_ping_frame(ping_service_id);
                if ping_tx.send(ping_frame.encode_to_vec()).is_err() {
                    break;
                }
            }
        });

        // Inner read loop
        match ws_read_loop(ws_source, &tx, write_tx, service_id).await {
            Ok(true) => {
                tracing::info!("Feishu WS: receiver closed, shutting down reader");
                return;
            }
            Ok(false) | Err(()) => {}
        }

        // Connection lost — increment and retry
        attempt += 1;
    }
}

/// Read loop for a single WebSocket connection.
///
/// Decodes every binary frame as a protobuf [`Frame`], dispatches by method
/// (CONTROL / DATA), assembles multi-part messages, and sends response frames
/// back for every DATA frame.
///
/// Returns `Ok(true)` when the receiver is closed (graceful shutdown).
/// Returns `Ok(false)` / `Err(...)` when the connection dropped and should reconnect.
async fn ws_read_loop(
    mut ws_source: impl futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
    tx: &mpsc::UnboundedSender<serde_json::Value>,
    ws_sender: mpsc::UnboundedSender<Vec<u8>>,
    service_id: i32,
) -> Result<bool, ()> {
    let mut fragment_buffer = FragmentBuffer::new();

    loop {
        let _msg = match ws_source.next().await {
            // ── Binary frame: decode as protobuf Frame ──────────────
            Some(Ok(Message::Binary(data))) => {
                let frame = match Frame::decode(data.as_ref()) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!("Feishu WS: protobuf decode error: {e}");
                        continue;
                    }
                };

                match frame.method {
                    FRAME_CONTROL => {
                        let msg_type = frame.get_header(HEADER_TYPE).unwrap_or("");
                        match msg_type {
                            MSG_PING => {
                                tracing::debug!("Feishu WS: received server ping");
                            }
                            MSG_PONG => {
                                tracing::debug!("Feishu WS: received pong");
                            }
                            _ => {
                                tracing::debug!(
                                    "Feishu WS: unknown control frame type: {msg_type}"
                                );
                            }
                        }
                        continue;
                    }
                    FRAME_DATA => {
                        let msg_type = frame.get_header(HEADER_TYPE).unwrap_or("").to_string();

                        // Handle multi-part messages
                        let msg_id = frame
                            .get_header(HEADER_MESSAGE_ID)
                            .unwrap_or("")
                            .to_string();
                        let sum: i32 = frame
                            .get_header(HEADER_SUM)
                            .and_then(|v: &str| v.parse().ok())
                            .unwrap_or(1);
                        let seq: usize = frame
                            .get_header(HEADER_SEQ)
                            .and_then(|v: &str| v.parse().ok())
                            .unwrap_or(0);

                        let raw_payload = frame.payload.clone().unwrap_or_default();
                        let payload_bytes = if sum > 1 {
                            match fragment_buffer.add(&msg_id, sum, seq, raw_payload) {
                                Some(combined) => combined,
                                None => continue,
                            }
                        } else {
                            raw_payload
                        };

                        // Parse payload as JSON
                        let json_str = String::from_utf8_lossy(&payload_bytes);
                        match serde_json::from_str::<serde_json::Value>(&json_str) {
                            Ok(value) => {
                                send_response_frame(&ws_sender, &frame, service_id, 200);

                                if tx.send(value).is_err() {
                                    return Ok(true);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Feishu WS: payload JSON parse error: {e}, type={msg_type}"
                                );
                                send_response_frame(&ws_sender, &frame, service_id, 500);
                            }
                        }
                        continue;
                    }
                    _ => {
                        tracing::debug!("Feishu WS: unknown frame method {}", frame.method);
                        continue;
                    }
                }
            }
            // ── WebSocket-level ping (unexpected in WS v2, but handled for robustness) ──
            Some(Ok(Message::Ping(_))) => {
                tracing::debug!("Feishu WS: received unexpected WebSocket ping");
                continue;
            }
            Some(Ok(Message::Pong(_))) => {
                continue;
            }
            // ── Connection close ────────────────────────────────────
            Some(Ok(Message::Close(_))) | None => {
                tracing::info!("Feishu WS: connection closed by server");
                return Ok(false);
            }
            // ── Read error ──────────────────────────────────────────
            Some(Err(e)) => {
                tracing::warn!("Feishu WS: read error: {e}");
                return Err(());
            }
            _ => continue,
        };
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Send a response frame back through the write channel.
///
/// Feishu requires an acknowledgement frame for every DATA frame received.
/// The response carries the same `seq_id`, `log_id`, `service`, and `method`
/// as the request, with a JSON payload `{"code": status_code}`.
fn send_response_frame(
    sender: &mpsc::UnboundedSender<Vec<u8>>,
    request_frame: &Frame,
    service_id: i32,
    status_code: i32,
) {
    let mut response = Frame {
        seq_id: request_frame.seq_id,
        log_id: request_frame.log_id,
        service: service_id,
        method: request_frame.method,
        headers: request_frame.headers.clone(),
        payload_encoding: None,
        payload_type: None,
        payload: Some(
            serde_json::to_vec(&serde_json::json!({"code": status_code})).unwrap_or_default(),
        ),
        log_id_new: None,
    };

    response.headers.push(Header {
        key: HEADER_BIZ_RT.to_string(),
        value: "0".to_string(),
    });

    let _ = sender.send(response.encode_to_vec());
}

/// Build a protobuf PING frame for the heartbeat task.
fn create_ping_frame(service_id: i32) -> Frame {
    Frame {
        seq_id: 0,
        log_id: 0,
        service: service_id,
        method: FRAME_CONTROL,
        headers: vec![Header {
            key: HEADER_TYPE.to_string(),
            value: MSG_PING.to_string(),
        }],
        payload_encoding: None,
        payload_type: None,
        payload: None,
        log_id_new: None,
    }
}

/// Extract the `service_id` query parameter from a WebSocket URL.
///
/// Returns 0 when the parameter is absent or unparseable.
fn extract_service_id(ws_url: &str) -> i32 {
    if let Some(pos) = ws_url.find("service_id=") {
        let after = &ws_url[pos + 11..];
        let end = after.find('&').unwrap_or(after.len());
        after[..end].parse().unwrap_or(0)
    } else {
        0
    }
}

/// Buffer for assembling multi-part protobuf frames.
///
/// Feishu WS v2 splits large messages across multiple DATA frames.  Each
/// fragment carries `sum` (total count) and `seq` (0-based index) headers.
/// The buffer collects fragments keyed by `message_id` and returns the
/// combined payload once all fragments have arrived.
struct FragmentBuffer {
    fragments: HashMap<String, (i32, Vec<(usize, Vec<u8>)>)>,
    //                 message_id → (total_sum, [(seq, data), ...])
}

impl FragmentBuffer {
    fn new() -> Self {
        Self {
            fragments: HashMap::new(),
        }
    }

    /// Add a fragment. Returns `Some(combined_bytes)` when all fragments
    /// have been received, `None` while waiting for more.
    fn add(&mut self, msg_id: &str, sum: i32, seq: usize, data: Vec<u8>) -> Option<Vec<u8>> {
        if sum <= 1 {
            return Some(data);
        }
        let entry = self
            .fragments
            .entry(msg_id.to_string())
            .or_insert_with(|| (sum, Vec::new()));
        entry.1.push((seq, data));
        if entry.1.len() as i32 == sum {
            entry.1.sort_by_key(|(s, _)| *s);
            let combined: Vec<u8> = entry
                .1
                .iter()
                .flat_map(|(_, d)| d.iter().copied())
                .collect();
            self.fragments.remove(msg_id);
            Some(combined)
        } else {
            None
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
    }

    #[test]
    fn test_reconnect_settings_are_stored() {
        let client = FeishuWsClient::new("app", "sec").with_reconnect(5, 30);
        assert_eq!(client.reconnect_max_attempts, 5);
        assert_eq!(client.reconnect_interval_secs, 30);
    }

    #[test]
    fn test_with_reconnect_zero_attempts() {
        let client = FeishuWsClient::new("app", "sec").with_reconnect(0, 60);
        assert_eq!(client.reconnect_max_attempts, 0);
        assert_eq!(client.reconnect_interval_secs, 60);
    }

    // -- Endpoint response deserialization ---------------------------------

    #[test]
    fn test_endpoint_response_deserialization() {
        let raw = r#"{
            "code": 0,
            "msg": "",
            "data": {
                "URL": "wss://msg-frontier.feishu.cn/ws/v2?token=abc",
                "ClientConfig": {
                    "PingInterval": 90,
                    "ReconnectCount": -1,
                    "ReconnectInterval": 90,
                    "ReconnectNonce": 25
                }
            }
        }"#;
        let parsed: EndpointResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "");
        let data = parsed.data.expect("data present");
        assert_eq!(
            data.url.as_deref(),
            Some("wss://msg-frontier.feishu.cn/ws/v2?token=abc")
        );
        let cfg = data.client_config.expect("client config present");
        assert_eq!(cfg.ping_interval, Some(90));
        assert_eq!(cfg.reconnect_count, Some(-1));
        assert_eq!(cfg.reconnect_interval, Some(90));
        assert_eq!(cfg.reconnect_nonce, Some(25));
    }

    #[test]
    fn test_endpoint_response_error() {
        let raw = r#"{"code": 99991663, "msg": "invalid app_id"}"#;
        let parsed: EndpointResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 99991663);
        assert_eq!(parsed.msg, "invalid app_id");
        assert!(parsed.data.is_none());
    }

    #[test]
    fn test_register_result_from_data() {
        let raw = r#"{
            "code": 0,
            "msg": "",
            "data": {
                "URL": "wss://open.feishu.cn/ws/event/test",
                "ClientConfig": {
                    "PingInterval": 30,
                    "ReconnectCount": 5,
                    "ReconnectInterval": 60,
                    "ReconnectNonce": 10
                }
            }
        }"#;
        let parsed: EndpointResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        let data = parsed.data.unwrap();
        let cfg = data.client_config.unwrap();
        assert_eq!(data.url.unwrap(), "wss://open.feishu.cn/ws/event/test");
        assert_eq!(cfg.ping_interval, Some(30));
        assert_eq!(cfg.reconnect_count, Some(5));
        assert_eq!(cfg.reconnect_interval, Some(60));
        assert_eq!(cfg.reconnect_nonce, Some(10));
    }

    #[test]
    fn test_endpoint_response_no_client_config() {
        let raw = r#"{
            "code": 0,
            "msg": "",
            "data": {
                "URL": "wss://open.feishu.cn/ws/event/minimal"
            }
        }"#;
        let parsed: EndpointResponse = serde_json::from_str(raw).expect("deserialize");
        let data = parsed.data.expect("data present");
        assert_eq!(
            data.url.as_deref(),
            Some("wss://open.feishu.cn/ws/event/minimal")
        );
        assert!(data.client_config.is_none());
    }

    #[test]
    fn test_endpoint_response_default_missing_fields() {
        let raw = r#"{}"#;
        let parsed: EndpointResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "");
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

    // -- Protobuf frame encoding / decoding ----------------------------------

    #[test]
    fn test_extract_service_id_with_param() {
        let url = "wss://msg-frontier.feishu.cn/ws/v2?token=abc&service_id=5";
        assert_eq!(extract_service_id(url), 5);
    }

    #[test]
    fn test_extract_service_id_first_param() {
        let url = "wss://open.feishu.cn/ws/v2?service_id=42&token=xyz";
        assert_eq!(extract_service_id(url), 42);
    }

    #[test]
    fn test_extract_service_id_missing() {
        let url = "wss://open.feishu.cn/ws/v2?token=abc";
        assert_eq!(extract_service_id(url), 0);
    }

    #[test]
    fn test_extract_service_id_empty_url() {
        assert_eq!(extract_service_id(""), 0);
    }

    #[test]
    fn test_extract_service_id_non_numeric() {
        let url = "wss://open.feishu.cn/ws/v2?service_id=not_a_number";
        assert_eq!(extract_service_id(url), 0);
    }

    #[test]
    fn test_create_ping_frame_structure() {
        let frame = create_ping_frame(7);
        assert_eq!(frame.service, 7);
        assert_eq!(frame.method, FRAME_CONTROL);
        assert_eq!(frame.seq_id, 0);
        assert_eq!(frame.log_id, 0);
        assert_eq!(frame.get_header(HEADER_TYPE), Some(MSG_PING));
        assert!(frame.payload.is_none());
    }

    #[test]
    fn test_send_response_frame() {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let request = Frame {
            seq_id: 99,
            log_id: 888,
            service: 5,
            method: FRAME_DATA,
            headers: vec![Header {
                key: HEADER_TYPE.to_string(),
                value: "event".to_string(),
            }],
            payload_encoding: None,
            payload_type: None,
            payload: Some(b"original".to_vec()),
            log_id_new: None,
        };

        send_response_frame(&tx, &request, 5, 200);
        drop(tx);

        let bytes = rx.blocking_recv().expect("should receive response");
        let response = Frame::decode(bytes.as_ref()).expect("valid protobuf");

        assert_eq!(response.seq_id, 99);
        assert_eq!(response.log_id, 888);
        assert_eq!(response.service, 5);
        assert_eq!(response.method, FRAME_DATA);

        // Should still have the original header plus biz_rt
        assert!(response.headers.iter().any(|h| h.key == HEADER_BIZ_RT));

        // Payload should be JSON {"code": 200}
        let payload: serde_json::Value =
            serde_json::from_slice(response.payload.as_deref().unwrap_or(b"{}"))
                .expect("JSON payload");
        assert_eq!(payload["code"], 200);
    }

    // -- Fragment buffer -----------------------------------------------------

    #[test]
    fn test_fragment_buffer_single_part() {
        let mut buf = FragmentBuffer::new();
        let data = b"hello".to_vec();
        let result = buf.add("msg1", 1, 0, data.clone());
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_fragment_buffer_two_parts_in_order() {
        let mut buf = FragmentBuffer::new();
        assert!(buf.add("msg1", 2, 0, b"hel".to_vec()).is_none());
        assert_eq!(
            buf.add("msg1", 2, 1, b"lo".to_vec()),
            Some(b"hello".to_vec())
        );
    }

    #[test]
    fn test_fragment_buffer_two_parts_out_of_order() {
        let mut buf = FragmentBuffer::new();
        assert!(buf.add("msg1", 2, 1, b"world".to_vec()).is_none());
        assert_eq!(
            buf.add("msg1", 2, 0, b"hello ".to_vec()),
            Some(b"hello world".to_vec())
        );
    }

    #[test]
    fn test_fragment_buffer_three_parts() {
        let mut buf = FragmentBuffer::new();
        assert!(buf.add("msg", 3, 2, b"!".to_vec()).is_none());
        assert!(buf.add("msg", 3, 0, b"ab".to_vec()).is_none());
        assert_eq!(
            buf.add("msg", 3, 1, b"cd".to_vec()),
            Some(b"abcd!".to_vec())
        );
    }

    // -- ws_read_loop (unit test with a memory stream) -----------------------
    // We test the loop with a futures::stream that simulates WebSocket frames.

    #[tokio::test]
    async fn test_ws_read_loop_data_frame() {
        use futures::stream;
        use prost::Message as ProstMessage;

        let data_frame = Frame {
            seq_id: 1,
            log_id: 100,
            service: 5,
            method: FRAME_DATA,
            headers: vec![
                Header {
                    key: HEADER_TYPE.to_string(),
                    value: "event".to_string(),
                },
                Header {
                    key: HEADER_MESSAGE_ID.to_string(),
                    value: "evt_001".to_string(),
                },
            ],
            payload_encoding: Some("json".to_string()),
            payload_type: Some("im.message.receive_v1".to_string()),
            payload: Some(serde_json::to_vec(&serde_json::json!({"text": "hello"})).unwrap()),
            log_id_new: None,
        };

        let encoded = data_frame.encode_to_vec();
        let messages: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Binary(encoded.into()))];

        let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let handle =
            tokio::spawn(
                async move { ws_read_loop(stream::iter(messages), &tx, write_tx, 5).await },
            );

        // Should receive the JSON event
        let event = rx.recv().await.expect("should receive event");
        assert_eq!(event["text"], "hello");

        // A response frame should have been sent back
        let resp_bytes = write_rx.recv().await.expect("should receive response");
        let resp = Frame::decode(resp_bytes.as_ref()).expect("valid response frame");
        assert_eq!(resp.seq_id, 1);

        // Clean up
        drop(rx);
        drop(write_rx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_ws_read_loop_control_ping_ignored() {
        use futures::stream;

        let ping_frame = Frame {
            seq_id: 0,
            log_id: 0,
            service: 5,
            method: FRAME_CONTROL,
            headers: vec![Header {
                key: HEADER_TYPE.to_string(),
                value: MSG_PING.to_string(),
            }],
            payload_encoding: None,
            payload_type: None,
            payload: None,
            log_id_new: None,
        };

        let encoded = ping_frame.encode_to_vec();
        let messages: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Binary(encoded.into())),
            // Then a close to stop the loop
            Ok(Message::Close(None)),
        ];

        let (tx, rx) = mpsc::unbounded_channel::<serde_json::Value>();
        let (write_tx, _write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        drop(rx); // No events expected

        let result = ws_read_loop(stream::iter(messages), &tx, write_tx, 5).await;

        // Should get Ok(false) because of the Close frame
        assert_eq!(result, Ok(false));
    }

    #[tokio::test]
    async fn test_ws_read_loop_close_frame_returns_false() {
        use futures::stream;

        let messages: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Close(None))];

        let (tx, _rx) = mpsc::unbounded_channel::<serde_json::Value>();
        let (write_tx, _write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let result = ws_read_loop(stream::iter(messages), &tx, write_tx, 5).await;

        assert_eq!(result, Ok(false));
    }

    #[tokio::test]
    async fn test_ws_read_loop_receiver_dropped_returns_true() {
        use futures::stream;
        use prost::Message as ProstMessage;

        let data_frame = Frame {
            seq_id: 1,
            log_id: 1,
            service: 5,
            method: FRAME_DATA,
            headers: vec![Header {
                key: HEADER_TYPE.to_string(),
                value: "event".to_string(),
            }],
            payload_encoding: None,
            payload_type: None,
            payload: Some(b"{}".to_vec()),
            log_id_new: None,
        };

        let encoded = data_frame.encode_to_vec();
        let messages: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Binary(encoded.into()))];

        let (tx, rx) = mpsc::unbounded_channel::<serde_json::Value>();
        let (write_tx, mut _write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        drop(rx); // Receiver already dropped

        let result = ws_read_loop(stream::iter(messages), &tx, write_tx, 5).await;

        // Should detect receiver is gone and return Ok(true) after trying to send
        assert_eq!(result, Ok(true));
    }

    #[tokio::test]
    async fn test_ws_read_loop_garbage_binary_skipped() {
        use futures::stream;

        let garbage = vec![0xFFu8, 0xFE, 0xFD]; // Not valid protobuf
        let messages: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Binary(garbage.into())),
            Ok(Message::Close(None)),
        ];

        let (tx, rx) = mpsc::unbounded_channel::<serde_json::Value>();
        let (write_tx, _write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        drop(rx);

        let result = ws_read_loop(stream::iter(messages), &tx, write_tx, 5).await;

        // Should skip garbage, hit Close, return false
        assert_eq!(result, Ok(false));
    }

    #[tokio::test]
    async fn test_ws_read_loop_read_error_returns_err() {
        use futures::stream;

        let messages: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed)];

        let (tx, _rx) = mpsc::unbounded_channel::<serde_json::Value>();
        let (write_tx, _write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let result = ws_read_loop(stream::iter(messages), &tx, write_tx, 5).await;

        assert_eq!(result, Err(()));
    }

    #[tokio::test]
    async fn test_ws_read_loop_multi_part_data_frame() {
        use futures::stream;
        use prost::Message as ProstMessage;

        let json_payload =
            serde_json::to_vec(&serde_json::json!({"multi": "part", "test": true})).unwrap();
        let (part1, part2) = json_payload.split_at(json_payload.len() / 2);

        let frame1 = Frame {
            seq_id: 1,
            log_id: 200,
            service: 5,
            method: FRAME_DATA,
            headers: vec![
                Header {
                    key: HEADER_TYPE.to_string(),
                    value: "event".to_string(),
                },
                Header {
                    key: HEADER_MESSAGE_ID.to_string(),
                    value: "multi_001".to_string(),
                },
                Header {
                    key: HEADER_SUM.to_string(),
                    value: "2".to_string(),
                },
                Header {
                    key: HEADER_SEQ.to_string(),
                    value: "0".to_string(),
                },
            ],
            payload_encoding: None,
            payload_type: None,
            payload: Some(part1.to_vec()),
            log_id_new: None,
        };

        let frame2 = Frame {
            seq_id: 2,
            log_id: 200,
            service: 5,
            method: FRAME_DATA,
            headers: vec![
                Header {
                    key: HEADER_TYPE.to_string(),
                    value: "event".to_string(),
                },
                Header {
                    key: HEADER_MESSAGE_ID.to_string(),
                    value: "multi_001".to_string(),
                },
                Header {
                    key: HEADER_SUM.to_string(),
                    value: "2".to_string(),
                },
                Header {
                    key: HEADER_SEQ.to_string(),
                    value: "1".to_string(),
                },
            ],
            payload_encoding: None,
            payload_type: None,
            payload: Some(part2.to_vec()),
            log_id_new: None,
        };

        let messages: Vec<Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Binary(frame1.encode_to_vec().into())),
            Ok(Message::Binary(frame2.encode_to_vec().into())),
        ];

        let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();
        let (write_tx, mut _write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let handle =
            tokio::spawn(
                async move { ws_read_loop(stream::iter(messages), &tx, write_tx, 5).await },
            );

        let event = rx.recv().await.expect("should receive combined event");
        assert_eq!(event["multi"], "part");
        assert_eq!(event["test"], true);

        drop(rx);
        let _ = handle.await;
    }
}
