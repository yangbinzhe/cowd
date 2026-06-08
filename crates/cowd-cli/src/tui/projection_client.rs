use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DaemonProjectionClient {
    base_url: String,
    auth_token: Option<String>,
    client: reqwest::Client,
}

#[derive(Debug)]
pub enum ProjectionError {
    Http(reqwest::Error),
    Status(reqwest::StatusCode, String),
    Url(String),
}

impl DaemonProjectionClient {
    pub fn new(
        base_url: impl Into<String>,
        auth_token: Option<String>,
    ) -> Result<Self, ProjectionError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .map_err(ProjectionError::Http)?;
        Ok(Self {
            base_url: normalize_base_url(base_url.into())?,
            auth_token,
            client,
        })
    }

    pub fn from_running_gateway(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, ProjectionError> {
        let Some(info) =
            crate::server::get_server_status().map_err(|e| ProjectionError::Url(e.to_string()))?
        else {
            return Ok(None);
        };
        Self::new(info.address, auth_token).map(Some)
    }

    pub async fn runtime_control_plane(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/runtime/control-plane").await
    }

    pub async fn runtime_effective_config(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/runtime/config/effective").await
    }

    pub async fn current_context(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, ProjectionError> {
        let path = match session_id {
            Some(id) if !id.trim().is_empty() => {
                format!("/api/context/current?session_id={}", url_encode(id))
            }
            _ => "/api/context/current".to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn memory_status(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/memory/status").await
    }

    pub async fn task_status(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/tasks").await
    }

    pub async fn pending_approvals(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/approval/pending").await
    }

    pub async fn respond_approval(
        &self,
        id: &str,
        approved: bool,
        persistence: Option<&str>,
        reason: Option<&str>,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            "/api/approval/respond",
            serde_json::json!({
                "id": id,
                "approved": approved,
                "persistence": persistence.unwrap_or("once"),
                "reason": reason,
            }),
        )
        .await
    }

    pub async fn start_task(
        &self,
        objective: &str,
        yolo_mode: bool,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            "/api/tasks/start",
            serde_json::json!({
                "objective": objective,
                "yolo_mode": yolo_mode,
            }),
        )
        .await
    }

    pub async fn cancel_task(&self, id: &str) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            &format!("/api/tasks/{}/cancel", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn complete_task(&self, id: &str) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            &format!("/api/tasks/{}/complete", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn cross_plane_summary(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/cross-plane/summary").await
    }

    pub async fn connector_accounts(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/connectors/accounts").await
    }

    pub async fn connector_capabilities(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/connectors/capabilities").await
    }

    pub async fn connector_resources(
        &self,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<serde_json::Value, ProjectionError> {
        let mut path = format!("/api/connectors/resources?limit={limit}&offset={offset}");
        if let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) {
            path.push_str("&q=");
            path.push_str(&url_encode(query));
        }
        self.get_json(&path).await
    }

    pub async fn preflight_cross_plane_action(
        &self,
        action: serde_json::Value,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json("/api/cross-plane/action/preflight", action)
            .await
    }

    pub async fn execute_cross_plane_action(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json("/api/cross-plane/action/execute", request)
            .await
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, ProjectionError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(url);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(ProjectionError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ProjectionError::Status(status, body));
        }
        response.json().await.map_err(ProjectionError::Http)
    }

    async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, ProjectionError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.post(url).json(&body);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(ProjectionError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ProjectionError::Status(status, body));
        }
        response.json().await.map_err(ProjectionError::Http)
    }
}

fn normalize_base_url(mut base_url: String) -> Result<String, ProjectionError> {
    if base_url.trim().is_empty() {
        return Err(ProjectionError::Url(
            "empty daemon API base URL".to_string(),
        ));
    }
    base_url = base_url.trim().trim_end_matches('/').to_string();
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return Err(ProjectionError::Url(format!(
            "daemon API base URL must start with http:// or https://: {base_url}"
        )));
    }
    Ok(base_url)
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{b:02X}").chars().collect(),
        })
        .collect()
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(err) => write!(f, "daemon projection HTTP failed: {err}"),
            Self::Status(status, body) => {
                write!(f, "daemon projection returned {status}: {body}")
            }
            Self::Url(err) => write!(f, "daemon projection URL error: {err}"),
        }
    }
}

impl std::error::Error for ProjectionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn normalize_base_url_trims_trailing_slashes() {
        assert_eq!(
            normalize_base_url(" http://127.0.0.1:8642/// ".to_string()).unwrap(),
            "http://127.0.0.1:8642"
        );
        assert!(normalize_base_url("127.0.0.1:8642".to_string()).is_err());
    }

    #[test]
    fn url_encode_encodes_session_ids() {
        assert_eq!(url_encode("session a/b"), "session%20a%2Fb");
    }

    #[tokio::test]
    async fn runtime_control_plane_gets_json_with_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/runtime/control-plane HTTP/1.1"));
            assert!(req.contains("authorization: Bearer test-token"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write");
        });

        let client =
            DaemonProjectionClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let json = client.runtime_control_plane().await.expect("json");
        assert_eq!(json["ok"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn preflight_cross_plane_action_posts_json() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/cross-plane/action/preflight HTTP/1.1"));
            assert!(req.contains("authorization: Bearer test-token"));
            assert!(req.contains("\"operation\":\"send_text\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 14\r\n\r\n{\"ready\":true}",
                )
                .await
                .expect("write");
        });

        let client =
            DaemonProjectionClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let json = client
            .preflight_cross_plane_action(serde_json::json!({
                "operation": "send_text",
                "capability": "channel.feishu.send_text",
            }))
            .await
            .expect("json");
        assert_eq!(json["ready"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn respond_approval_posts_decision() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/approval/respond HTTP/1.1"));
            assert!(req.contains("\"id\":\"approval-1\""));
            assert!(req.contains("\"approved\":true"));
            assert!(req.contains("\"persistence\":\"session\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 17\r\n\r\n{\"resolved\":true}",
                )
                .await
                .expect("write");
        });

        let client = DaemonProjectionClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .respond_approval("approval-1", true, Some("session"), None)
            .await
            .expect("json");
        assert_eq!(json["resolved"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn start_task_posts_objective() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/tasks/start HTTP/1.1"));
            assert!(req.contains("\"objective\":\"ship tui\""));
            assert!(req.contains("\"yolo_mode\":true"));
            socket
                .write_all(
                    b"HTTP/1.1 201 Created\r\ncontent-type: application/json\r\ncontent-length: 15\r\n\r\n{\"id\":\"task-1\"}",
                )
                .await
                .expect("write");
        });

        let client = DaemonProjectionClient::new(format!("http://{addr}"), None).expect("client");
        let json = client.start_task("ship tui", true).await.expect("json");
        assert_eq!(json["id"], "task-1");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn connector_resources_gets_search_page() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with(
                "GET /api/connectors/resources?limit=20&offset=40&q=Ready%20Doc HTTP/1.1"
            ));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 16\r\n\r\n{\"resources\":[]}",
                )
                .await
                .expect("write");
        });

        let client = DaemonProjectionClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .connector_resources(Some("Ready Doc"), 20, 40)
            .await
            .expect("json");
        assert!(json["resources"].as_array().unwrap().is_empty());
        server.await.expect("server task");
    }
}
