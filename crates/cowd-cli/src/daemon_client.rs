use std::path::Path;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use serde_json::{json, Value};
use runtime::CowdEvent;

pub struct DaemonClient {
    stream: BufReader<UnixStream>,
    pub session_id: String,
    buf: String,
}

impl DaemonClient {
    pub async fn connect(model: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let sock = Path::new("/tmp/cowd.sock");
        let stream = UnixStream::connect(sock).await?;
        let mut client = Self {
            stream: BufReader::new(stream),
            session_id: String::new(),
            buf: String::new(),
        };

        // Create session
        let cmd = json!({"cmd":"create_session","model":model});
        client.write_cmd(&cmd).await?;
        client.session_id = match client.read_json().await {
            Some(CowdEvent::SessionCreated { id, .. }) => id,
            _ => String::new(),
        };
        Ok(client)
    }

    async fn write_cmd(&mut self, cmd: &Value) -> Result<(), std::io::Error> {
        let mut json = cmd.to_string();
        json.push('\n');
        self.stream.get_mut().write_all(json.as_bytes()).await
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
                self.buf.push_str(&String::from_utf8_lossy(&buf[..n]));
                let mut events = Vec::new();
                while let Some(pos) = self.buf.find('\n') {
                    let line = self.buf[..pos].to_string();
                    self.buf = self.buf[pos + 1..].to_string();
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


