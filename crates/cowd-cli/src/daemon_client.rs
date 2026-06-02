use std::time::Duration;
use tokio::net::UnixStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use serde_json::{json, Value};
use runtime::CowdEvent;

pub struct DaemonClient {
    stream: BufReader<UnixStream>,
    pub session_id: String,
    buf: String,
    drain_buf: String,
}

impl DaemonClient {
    pub async fn connect(model: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = UnixStream::connect(crate::server::socket_file()).await?;
        let mut client = Self {
            stream: BufReader::new(stream),
            session_id: String::new(),
            buf: String::new(),
            drain_buf: String::new(),
        };

        // Create session — daemon returns plain JSON {"ok":true,"session_id":"..."}
        let cmd = json!({"cmd":"create_session","model":model});
        client.write_cmd(&cmd).await?;
        let resp = client.read_json_value().await?;
        client.session_id = resp["session_id"].as_str().unwrap_or_default().to_string();
        Ok(client)
    }

    async fn write_cmd(&mut self, cmd: &Value) -> Result<(), std::io::Error> {
        let mut json = cmd.to_string();
        json.push('\n');
        self.stream.get_mut().write_all(json.as_bytes()).await
    }

    /// Read a raw JSON value (not necessarily a CowdEvent)
    async fn read_json_value(&mut self) -> Result<serde_json::Value, std::io::Error> {
        self.buf.clear();
        self.stream.read_line(&mut self.buf).await?;
        Ok(serde_json::from_str(&self.buf).unwrap_or(serde_json::Value::Null))
    }

    async fn read_json(&mut self) -> Option<CowdEvent> {
        self.buf.clear();
        match tokio::time::timeout(Duration::from_secs(30), self.stream.read_line(&mut self.buf)).await {
            Ok(Ok(0)) => None,  // connection closed
            Ok(Ok(_)) => serde_json::from_str(&self.buf).ok(),
            Ok(Err(_)) => None,
            Err(_) => {
                tracing::warn!("daemon response timeout, connection will be re-established");
                None
            }
        }
    }

    pub async fn send_chat(&mut self, content: &str) -> Result<(), std::io::Error> {
        let cmd = json!({"cmd":"chat_stream","session_id":self.session_id,"content":content});
        self.write_cmd(&cmd).await
    }

    /// Blocking receive — waits for next event
    pub async fn recv_event(&mut self) -> Option<CowdEvent> {
        self.read_json().await
    }

    /// Non-blocking drain — reads all available events without blocking
    pub fn try_recv_events(&mut self) -> Vec<CowdEvent> {
        let mut buf = [0u8; 4096];
        match self.stream.get_mut().try_read(&mut buf) {
            Ok(n) if n > 0 => {
                self.drain_buf.push_str(&String::from_utf8_lossy(&buf[..n]));
                let mut events = Vec::new();
                while let Some(pos) = self.drain_buf.find('\n') {
                    let line = self.drain_buf[..pos].to_string();
                    self.drain_buf = self.drain_buf[pos + 1..].to_string();
                    if let Ok(event) = serde_json::from_str::<CowdEvent>(&line) {
                        events.push(event);
                    }
                }
                events
            }
            _ => Vec::new(),
        }
    }
}


