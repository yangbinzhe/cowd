use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

pub const DEFAULT_DAEMON_SOCKET: &str = "/tmp/cowd.sock";
const CONTROL_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct DaemonControlClient {
    socket_path: PathBuf,
    timeout: Duration,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DaemonStatus {
    pub ok: bool,
    pub protocol_version: u32,
    pub daemon: String,
    pub active_sessions: usize,
    pub uptime_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DaemonSessionList {
    pub ok: bool,
    pub sessions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DaemonEnsureSession {
    pub ok: bool,
    pub session_id: String,
    pub created: bool,
    pub active_sessions: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DaemonSessionLease {
    pub ok: bool,
    pub session_id: String,
    pub owner: String,
    pub mode: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DaemonRuntimeSnapshot {
    pub ok: bool,
    pub kind: String,
    pub protocol_version: u32,
    pub daemon: String,
    pub active_sessions: usize,
    pub uptime_secs: u64,
    #[serde(default)]
    pub sessions: Vec<String>,
    #[serde(default)]
    pub leases: DaemonLeaseSnapshot,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct DaemonLeaseSnapshot {
    #[serde(default)]
    pub total: usize,
    #[serde(default)]
    pub items: Vec<DaemonSessionLease>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DaemonChatResponse {
    pub ok: bool,
    pub response: String,
    pub iterations: u32,
}

#[derive(Debug)]
pub enum DaemonControlError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Timeout,
    Rejected(String),
    Protocol(String),
}

impl DaemonControlClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            timeout: Duration::from_millis(800),
        }
    }

    pub fn default_local() -> Self {
        Self::new(
            std::env::var("COWD_DAEMON_SOCKET").unwrap_or_else(|_| DEFAULT_DAEMON_SOCKET.into()),
        )
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn status(&self) -> Result<DaemonStatus, DaemonControlError> {
        let value = self
            .send_json(serde_json::json!({
                "cmd": "status",
                "protocol_version": CONTROL_PROTOCOL_VERSION,
            }))
            .await?;

        if !value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(DaemonControlError::Rejected(
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("daemon rejected status request")
                    .to_string(),
            ));
        }

        serde_json::from_value(value).map_err(DaemonControlError::Json)
    }

    pub async fn runtime_snapshot(&self) -> Result<DaemonRuntimeSnapshot, DaemonControlError> {
        let value = self
            .send_json(serde_json::json!({
                "cmd": "runtime_snapshot",
                "protocol_version": CONTROL_PROTOCOL_VERSION,
            }))
            .await?;

        if !value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(DaemonControlError::Rejected(
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("daemon rejected runtime_snapshot request")
                    .to_string(),
            ));
        }

        serde_json::from_value(value).map_err(DaemonControlError::Json)
    }

    pub async fn list_sessions(&self) -> Result<DaemonSessionList, DaemonControlError> {
        let value = self
            .send_json(serde_json::json!({
                "cmd": "list_sessions",
                "protocol_version": CONTROL_PROTOCOL_VERSION,
            }))
            .await?;

        if !value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(DaemonControlError::Rejected(
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("daemon rejected list_sessions request")
                    .to_string(),
            ));
        }

        serde_json::from_value(value).map_err(DaemonControlError::Json)
    }

    pub async fn ensure_session(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<DaemonEnsureSession, DaemonControlError> {
        let value = self
            .send_json(serde_json::json!({
                "cmd": "ensure_session",
                "protocol_version": CONTROL_PROTOCOL_VERSION,
                "session_id": session_id,
                "model": model,
            }))
            .await?;

        if !value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(DaemonControlError::Rejected(
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("daemon rejected ensure_session request")
                    .to_string(),
            ));
        }

        serde_json::from_value(value).map_err(DaemonControlError::Json)
    }

    pub async fn acquire_session_lease(
        &self,
        session_id: &str,
        owner: &str,
        mode: &str,
    ) -> Result<DaemonSessionLease, DaemonControlError> {
        let value = self
            .send_json(serde_json::json!({
                "cmd": "acquire_session_lease",
                "protocol_version": CONTROL_PROTOCOL_VERSION,
                "session_id": session_id,
                "owner": owner,
                "mode": mode,
            }))
            .await?;

        if !value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(DaemonControlError::Rejected(
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("daemon rejected acquire_session_lease request")
                    .to_string(),
            ));
        }

        serde_json::from_value(value).map_err(DaemonControlError::Json)
    }

    pub async fn release_session_lease(
        &self,
        session_id: &str,
        owner: &str,
    ) -> Result<serde_json::Value, DaemonControlError> {
        let value = self
            .send_json(serde_json::json!({
                "cmd": "release_session_lease",
                "protocol_version": CONTROL_PROTOCOL_VERSION,
                "session_id": session_id,
                "owner": owner,
            }))
            .await?;

        if !value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(DaemonControlError::Rejected(
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("daemon rejected release_session_lease request")
                    .to_string(),
            ));
        }

        Ok(value)
    }

    pub async fn chat_session(
        &self,
        session_id: &str,
        content: &str,
    ) -> Result<DaemonChatResponse, DaemonControlError> {
        let value = self
            .send_json(serde_json::json!({
                "cmd": "chat",
                "protocol_version": CONTROL_PROTOCOL_VERSION,
                "session_id": session_id,
                "content": content,
            }))
            .await?;

        if !value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(DaemonControlError::Rejected(
                value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("daemon rejected chat request")
                    .to_string(),
            ));
        }

        serde_json::from_value(value).map_err(DaemonControlError::Json)
    }

    pub async fn subscribe_session_events(
        &self,
        session_id: &str,
        tx: std::sync::mpsc::SyncSender<runtime::CowdEvent>,
    ) -> Result<(), DaemonControlError> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(DaemonControlError::Io)?;
        let mut payload = serde_json::to_vec(&serde_json::json!({
            "cmd": "subscribe_session",
            "protocol_version": CONTROL_PROTOCOL_VERSION,
            "session_id": session_id,
        }))
        .map_err(DaemonControlError::Json)?;
        payload.push(b'\n');
        stream
            .write_all(&payload)
            .await
            .map_err(DaemonControlError::Io)?;
        stream.flush().await.map_err(DaemonControlError::Io)?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader
                .read_line(&mut line)
                .await
                .map_err(DaemonControlError::Io)?;
            if read == 0 {
                return Ok(());
            }
            let value: serde_json::Value =
                serde_json::from_str(line.trim()).map_err(DaemonControlError::Json)?;
            if value.get("ok").and_then(|v| v.as_bool()) == Some(false) {
                return Err(DaemonControlError::Rejected(
                    value
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("daemon rejected subscribe_session request")
                        .to_string(),
                ));
            }
            if let Some(event) = daemon_json_to_cowd_event(&value) {
                if tx.send(event).is_err() {
                    return Ok(());
                }
            }
        }
    }

    async fn send_json(
        &self,
        command: serde_json::Value,
    ) -> Result<serde_json::Value, DaemonControlError> {
        let socket_path = self.socket_path.clone();
        let fut = async move {
            let mut stream = UnixStream::connect(socket_path)
                .await
                .map_err(DaemonControlError::Io)?;
            let mut payload = serde_json::to_vec(&command).map_err(DaemonControlError::Json)?;
            payload.push(b'\n');
            stream
                .write_all(&payload)
                .await
                .map_err(DaemonControlError::Io)?;
            stream.flush().await.map_err(DaemonControlError::Io)?;

            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            let n = reader
                .read_line(&mut line)
                .await
                .map_err(DaemonControlError::Io)?;
            if n == 0 {
                return Err(DaemonControlError::Protocol(
                    "daemon closed control socket without response".to_string(),
                ));
            }
            serde_json::from_str(line.trim()).map_err(DaemonControlError::Json)
        };

        match tokio::time::timeout(self.timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(DaemonControlError::Timeout),
        }
    }
}

fn daemon_json_to_cowd_event(value: &serde_json::Value) -> Option<runtime::CowdEvent> {
    match value.get("type").and_then(|v| v.as_str()) {
        Some("TurnStarted") => Some(runtime::CowdEvent::TurnStarted),
        Some("TextDelta") => Some(runtime::CowdEvent::TextDelta {
            text: value
                .get("content")
                .or_else(|| value.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        Some("ThinkingDelta") => Some(runtime::CowdEvent::ThinkingDelta {
            thinking: value
                .get("content")
                .or_else(|| value.get("thinking"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        Some("ThinkingComplete") => Some(runtime::CowdEvent::ThinkingComplete),
        Some("ToolStart") => Some(runtime::CowdEvent::ToolStart {
            id: value
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            name: value
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            preview: value
                .get("preview")
                .or_else(|| value.get("input"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        Some("ToolProgress") => Some(runtime::CowdEvent::ToolProgress {
            id: value
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            name: value
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            progress: value
                .get("progress")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        Some("ToolComplete") => Some(runtime::CowdEvent::ToolComplete {
            id: value
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            name: value
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            summary: value
                .get("summary")
                .or_else(|| value.get("result_summary"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            exit_code: value
                .get("exit_code")
                .and_then(|v| v.as_i64())
                .map(|code| code as i32),
        }),
        Some("TurnComplete") => Some(runtime::CowdEvent::TurnComplete {
            assistant_text: value
                .get("response")
                .or_else(|| value.get("assistant_text"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            iterations: value
                .get("iterations")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as u32,
        }),
        Some("TurnError") => Some(runtime::CowdEvent::TurnError {
            error: value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        _ => None,
    }
}

impl fmt::Display for DaemonControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "control socket I/O failed: {err}"),
            Self::Json(err) => write!(f, "control protocol JSON failed: {err}"),
            Self::Timeout => write!(f, "control socket timed out"),
            Self::Rejected(err) => write!(f, "daemon rejected request: {err}"),
            Self::Protocol(err) => write!(f, "daemon protocol error: {err}"),
        }
    }
}

impl std::error::Error for DaemonControlError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tokio::net::UnixListener;

    fn temp_socket(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cowd-control-client-{label}-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ))
    }

    #[test]
    #[serial]
    fn daemon_control_client_default_local_uses_env_socket() {
        let socket = temp_socket("env-default");
        unsafe {
            std::env::set_var("COWD_DAEMON_SOCKET", &socket);
        }
        let client = DaemonControlClient::default_local();
        assert_eq!(client.socket_path(), socket.as_path());
        unsafe {
            std::env::remove_var("COWD_DAEMON_SOCKET");
        }
    }

    #[tokio::test]
    async fn status_reads_single_line_json_response() {
        let socket = temp_socket("status");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read command");
            let command: serde_json::Value =
                serde_json::from_str(line.trim()).expect("command json");
            assert_eq!(command.get("cmd").and_then(|v| v.as_str()), Some("status"));
            writer
                .write_all(
                    br#"{"ok":true,"protocol_version":1,"daemon":"cowd","active_sessions":2,"uptime_secs":7}"#,
                )
                .await
                .expect("write response");
            writer.write_all(b"\n").await.expect("write newline");
        });

        let status = DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .status()
            .await
            .expect("status");
        assert_eq!(status.protocol_version, 1);
        assert_eq!(status.active_sessions, 2);
        assert_eq!(status.uptime_secs, 7);

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[tokio::test]
    async fn runtime_snapshot_reads_sessions_and_leases() {
        let socket = temp_socket("runtime-snapshot");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read command");
            let command: serde_json::Value =
                serde_json::from_str(line.trim()).expect("command json");
            assert_eq!(
                command.get("cmd").and_then(|v| v.as_str()),
                Some("runtime_snapshot")
            );
            writer
                .write_all(
                    br#"{"ok":true,"kind":"daemon_runtime_snapshot","protocol_version":1,"daemon":"cowd","active_sessions":1,"uptime_secs":9,"sessions":["s1"],"leases":{"total":1,"items":[{"ok":true,"session_id":"s1","owner":"tui:test","mode":"collaborative"}]}}"#,
                )
                .await
                .expect("write response");
            writer.write_all(b"\n").await.expect("write newline");
        });

        let snapshot = DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .runtime_snapshot()
            .await
            .expect("runtime snapshot");
        assert_eq!(snapshot.active_sessions, 1);
        assert_eq!(snapshot.sessions, vec!["s1"]);
        assert_eq!(snapshot.leases.total, 1);
        assert_eq!(snapshot.leases.items[0].owner, "tui:test");

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[tokio::test]
    async fn status_rejects_error_response() {
        let socket = temp_socket("rejected");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            stream
                .write_all(br#"{"ok":false,"error":"not ready"}"#)
                .await
                .expect("write response");
            stream.write_all(b"\n").await.expect("write newline");
        });

        let err = DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .status()
            .await
            .expect_err("status should reject");
        assert!(matches!(err, DaemonControlError::Rejected(_)));

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[tokio::test]
    async fn ensure_session_sends_requested_session_id_and_model() {
        let socket = temp_socket("ensure");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read command");
            let command: serde_json::Value =
                serde_json::from_str(line.trim()).expect("command json");
            assert_eq!(
                command.get("cmd").and_then(|v| v.as_str()),
                Some("ensure_session")
            );
            assert_eq!(
                command.get("session_id").and_then(|v| v.as_str()),
                Some("session-1")
            );
            assert_eq!(
                command.get("model").and_then(|v| v.as_str()),
                Some("model-1")
            );
            writer
                .write_all(
                    br#"{"ok":true,"session_id":"session-1","created":true,"active_sessions":1}"#,
                )
                .await
                .expect("write response");
            writer.write_all(b"\n").await.expect("write newline");
        });

        let ensured = DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .ensure_session("session-1", "model-1")
            .await
            .expect("ensure");
        assert_eq!(ensured.session_id, "session-1");
        assert!(ensured.created);
        assert_eq!(ensured.active_sessions, 1);

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[tokio::test]
    async fn subscribe_session_events_maps_turn_complete() {
        let socket = temp_socket("subscribe");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read command");
            let command: serde_json::Value =
                serde_json::from_str(line.trim()).expect("command json");
            assert_eq!(
                command.get("cmd").and_then(|v| v.as_str()),
                Some("subscribe_session")
            );
            writer
                .write_all(br#"{"ok":true,"type":"Subscribed","session_id":"session-1"}"#)
                .await
                .expect("write subscribed");
            writer.write_all(b"\n").await.expect("write newline");
            writer
                .write_all(
                    br#"{"type":"TurnComplete","session_id":"session-1","response":"done","iterations":3}"#,
                )
                .await
                .expect("write event");
            writer.write_all(b"\n").await.expect("write newline");
        });

        let (tx, rx) = std::sync::mpsc::sync_channel(8);
        DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .subscribe_session_events("session-1", tx)
            .await
            .expect("subscribe");

        let event = rx.recv_timeout(Duration::from_secs(1)).expect("event");
        match event {
            runtime::CowdEvent::TurnComplete {
                assistant_text,
                iterations,
            } => {
                assert_eq!(assistant_text, "done");
                assert_eq!(iterations, 3);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[tokio::test]
    async fn acquire_session_lease_sends_owner_and_mode() {
        let socket = temp_socket("lease");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read command");
            let command: serde_json::Value =
                serde_json::from_str(line.trim()).expect("command json");
            assert_eq!(
                command.get("cmd").and_then(|v| v.as_str()),
                Some("acquire_session_lease")
            );
            assert_eq!(command.get("owner").and_then(|v| v.as_str()), Some("tui:1"));
            assert_eq!(
                command.get("mode").and_then(|v| v.as_str()),
                Some("collaborative")
            );
            writer
                .write_all(
                    br#"{"ok":true,"session_id":"session-1","owner":"tui:1","mode":"collaborative"}"#,
                )
                .await
                .expect("write response");
            writer.write_all(b"\n").await.expect("write newline");
        });

        let lease = DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .acquire_session_lease("session-1", "tui:1", "collaborative")
            .await
            .expect("lease");
        assert_eq!(lease.owner, "tui:1");
        assert_eq!(lease.mode, "collaborative");

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[tokio::test]
    async fn chat_session_sends_content_to_daemon() {
        let socket = temp_socket("chat");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read command");
            let command: serde_json::Value =
                serde_json::from_str(line.trim()).expect("command json");
            assert_eq!(command.get("cmd").and_then(|v| v.as_str()), Some("chat"));
            assert_eq!(
                command.get("session_id").and_then(|v| v.as_str()),
                Some("session-1")
            );
            assert_eq!(
                command.get("content").and_then(|v| v.as_str()),
                Some("hello")
            );
            writer
                .write_all(br#"{"ok":true,"response":"world","iterations":1}"#)
                .await
                .expect("write response");
            writer.write_all(b"\n").await.expect("write newline");
        });

        let response = DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .chat_session("session-1", "hello")
            .await
            .expect("chat");
        assert_eq!(response.response, "world");
        assert_eq!(response.iterations, 1);

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[tokio::test]
    async fn release_session_lease_sends_owner() {
        let socket = temp_socket("release-lease");
        let listener = UnixListener::bind(&socket).expect("bind test socket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read command");
            let command: serde_json::Value =
                serde_json::from_str(line.trim()).expect("command json");
            assert_eq!(
                command.get("cmd").and_then(|v| v.as_str()),
                Some("release_session_lease")
            );
            assert_eq!(
                command.get("session_id").and_then(|v| v.as_str()),
                Some("session-1")
            );
            assert_eq!(command.get("owner").and_then(|v| v.as_str()), Some("tui:1"));
            writer
                .write_all(br#"{"ok":true,"session_id":"session-1","released":true}"#)
                .await
                .expect("write response");
            writer.write_all(b"\n").await.expect("write newline");
        });

        let response = DaemonControlClient::new(&socket)
            .with_timeout(Duration::from_secs(1))
            .release_session_lease("session-1", "tui:1")
            .await
            .expect("release");
        assert_eq!(response["released"], true);

        server.await.expect("server task");
        let _ = std::fs::remove_file(socket);
    }

    #[test]
    fn daemon_json_to_cowd_event_maps_core_events() {
        let text = daemon_json_to_cowd_event(&serde_json::json!({
            "type": "TextDelta",
            "content": "hello",
        }))
        .expect("text event");
        assert!(matches!(text, runtime::CowdEvent::TextDelta { text } if text == "hello"));

        let thinking = daemon_json_to_cowd_event(&serde_json::json!({
            "type": "ThinkingDelta",
            "thinking": "step",
        }))
        .expect("thinking event");
        assert!(matches!(
            thinking,
            runtime::CowdEvent::ThinkingDelta { thinking } if thinking == "step"
        ));

        let tool_start = daemon_json_to_cowd_event(&serde_json::json!({
            "type": "ToolStart",
            "id": "tool-1",
            "name": "read",
            "preview": "file.txt",
        }))
        .expect("tool start");
        assert!(matches!(
            tool_start,
            runtime::CowdEvent::ToolStart { id, name, preview }
                if id == "tool-1" && name == "read" && preview == "file.txt"
        ));

        let tool_progress = daemon_json_to_cowd_event(&serde_json::json!({
            "type": "ToolProgress",
            "id": "tool-1",
            "name": "read",
            "progress": "50%",
        }))
        .expect("tool progress");
        assert!(matches!(
            tool_progress,
            runtime::CowdEvent::ToolProgress { id, name, progress }
                if id == "tool-1" && name == "read" && progress == "50%"
        ));

        let tool_complete = daemon_json_to_cowd_event(&serde_json::json!({
            "type": "ToolComplete",
            "id": "tool-1",
            "name": "read",
            "summary": "done",
            "exit_code": 0,
        }))
        .expect("tool complete");
        assert!(matches!(
            tool_complete,
            runtime::CowdEvent::ToolComplete { id, name, summary, exit_code }
                if id == "tool-1" && name == "read" && summary == "done" && exit_code == Some(0)
        ));

        let turn_complete = daemon_json_to_cowd_event(&serde_json::json!({
            "type": "TurnComplete",
            "assistant_text": "ok",
            "iterations": 2,
        }))
        .expect("turn complete");
        assert!(matches!(
            turn_complete,
            runtime::CowdEvent::TurnComplete { assistant_text, iterations }
                if assistant_text == "ok" && iterations == 2
        ));
    }
}
