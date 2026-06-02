use std::path::Path;
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
        let resp: Value = client.read_json().await?;
        client.session_id = resp["session_id"].as_str().unwrap_or_default().to_string();
        Ok(client)
    }

    async fn write_cmd(&mut self, cmd: &Value) -> Result<(), std::io::Error> {
        let mut json = cmd.to_string();
        json.push('\n');
        self.stream.get_mut().write_all(json.as_bytes()).await
    }

    async fn read_json(&mut self) -> Result<Value, std::io::Error> {
        self.buf.clear();
        self.stream.read_line(&mut self.buf).await?;
        Ok(serde_json::from_str(&self.buf).unwrap_or(Value::Null))
    }

    pub async fn send_chat(&mut self, content: &str) -> Result<(), std::io::Error> {
        let cmd = json!({"cmd":"chat_stream","session_id":self.session_id,"content":content});
        self.write_cmd(&cmd).await
    }

    /// Blocking receive — waits for next event
    pub async fn recv_event(&mut self) -> Option<CowdEvent> {
        match self.read_json().await {
            Ok(v) => parse_cowd_event(&v),
            Err(_) => None,
        }
    }

    /// Non-blocking drain — reads all available events without blocking
    pub fn try_recv_events(&mut self) -> Vec<CowdEvent> {
        let mut events = Vec::new();
        // Read available lines from buffer
        while let Ok(line) = self.try_read_line() {
            if let Ok(v) = serde_json::from_str::<Value>(&line) {
                if let Some(event) = parse_cowd_event(&v) {
                    events.push(event);
                }
            }
        }
        events
    }

    fn try_read_line(&mut self) -> Result<String, std::io::Error> {
        let mut buf = [0u8; 4096];
        match self.stream.get_mut().try_read(&mut buf) {
            Ok(n) if n > 0 => Ok(String::from_utf8_lossy(&buf[..n]).to_string()),
            _ => Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "")),
        }
    }
}

fn parse_cowd_event(v: &Value) -> Option<CowdEvent> {
    match v.get("type")?.as_str()? {
        "TextDelta" => Some(CowdEvent::TextDelta {
            text: v["content"].as_str()?.to_string(),
        }),
        "TurnComplete" => Some(CowdEvent::TurnComplete {
            assistant_text: v
                .get("text")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string(),
            iterations: v
                .get("iterations")
                .and_then(|i| i.as_u64())
                .unwrap_or(0) as u32,
        }),
        "TurnError" => Some(CowdEvent::TurnError {
            error: v["error"].as_str()?.to_string(),
        }),
        _ => {
            // Try serde deserialization for other variants
            serde_json::from_value(v.clone()).ok()
        }
    }
}
