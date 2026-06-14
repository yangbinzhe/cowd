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

    pub async fn cowd_capabilities(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/cowd/capabilities").await
    }

    pub async fn cowd_projection(
        &self,
        surface: &str,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.get_json(&format!(
            "/api/cowd/projection?surface={}",
            url_encode(surface)
        ))
        .await
    }

    pub async fn structured_sources(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/cowd/structured/sources").await
    }

    pub async fn structured_ingest_plan(
        &self,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json("/api/cowd/structured/ingest-plan", input)
            .await
    }

    pub async fn runtime_session_leases(&self) -> Result<serde_json::Value, ProjectionError> {
        self.get_json("/api/runtime/session-leases").await
    }

    pub async fn acquire_runtime_session_lease(
        &self,
        session_id: &str,
        owner: &str,
        mode: &str,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            "/api/runtime/session-leases/acquire",
            serde_json::json!({
                "session_id": session_id,
                "owner": owner,
                "mode": mode,
            }),
        )
        .await
    }

    pub async fn release_runtime_session_lease(
        &self,
        session_id: &str,
        owner: &str,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            "/api/runtime/session-leases/release",
            serde_json::json!({
                "session_id": session_id,
                "owner": owner,
            }),
        )
        .await
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

    pub async fn connector_service_tools(
        &self,
        service: &str,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.get_json(&format!(
            "/api/connectors/services/{}/tools",
            url_encode(service)
        ))
        .await
    }

    pub async fn execute_connector_service(
        &self,
        service: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            &format!("/api/connectors/services/{}/execute", url_encode(service)),
            request,
        )
        .await
    }

    pub async fn revalidate_connector_resource(
        &self,
        reference: &str,
        state: &str,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            "/api/connectors/resources/revalidate",
            serde_json::json!({
                "reference": reference,
                "state": state,
            }),
        )
        .await
    }

    pub async fn promote_connector_resource_to_memory(
        &self,
        reference: &str,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, ProjectionError> {
        self.post_json(
            "/api/connectors/resources/promote-memory",
            serde_json::json!({
                "reference": reference,
                "session_id": session_id,
            }),
        )
        .await
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
    async fn cowd_projection_gets_surface_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept projection");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read projection");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/cowd/projection?surface=tui HTTP/1.1"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\n\r\n{\"surface\":\"tui\",\"capability_count\":1,\"capabilities\":[]}",
                )
                .await
                .expect("write projection");
        });

        let client = DaemonProjectionClient::new(format!("http://{addr}"), None).expect("client");
        let json = client.cowd_projection("tui").await.expect("json");
        assert_eq!(json["surface"], "tui");
        assert_eq!(json["capability_count"], 1);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn runtime_session_lease_control_uses_http_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept acquire");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read acquire");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/runtime/session-leases/acquire HTTP/1.1"));
            assert!(req.contains("\"session_id\":\"session-1\""));
            assert!(req.contains("\"owner\":\"tui:1\""));
            assert!(req.contains("\"mode\":\"collaborative\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 22\r\n\r\n{\"ok\":true,\"lease\":{}}",
                )
                .await
                .expect("write acquire");

            let (mut socket, _) = listener.accept().await.expect("accept release");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read release");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/runtime/session-leases/release HTTP/1.1"));
            assert!(req.contains("\"session_id\":\"session-1\""));
            assert!(req.contains("\"owner\":\"tui:1\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 27\r\n\r\n{\"ok\":true,\"released\":true}",
                )
                .await
                .expect("write release");
        });

        let client = DaemonProjectionClient::new(format!("http://{addr}"), None).expect("client");
        let acquired = client
            .acquire_runtime_session_lease("session-1", "tui:1", "collaborative")
            .await
            .expect("acquire");
        assert_eq!(acquired["ok"], true);
        let released = client
            .release_runtime_session_lease("session-1", "tui:1")
            .await
            .expect("release");
        assert_eq!(released["released"], true);
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

    #[tokio::test]
    async fn connector_service_tools_and_execute_use_management_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept tools");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read tools");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/connectors/services/mock.docs/tools HTTP/1.1"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 12\r\n\r\n{\"tools\":[]}",
                )
                .await
                .expect("write tools");

            let (mut socket, _) = listener.accept().await.expect("accept execute");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read execute");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/connectors/services/mock.docs/execute HTTP/1.1"));
            assert!(req.contains("\"tool_id\":\"service.mock.docs.read\""));
            assert!(req.contains("\"mode\":\"dry_run\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write execute");
        });

        let client = DaemonProjectionClient::new(format!("http://{addr}"), None).expect("client");
        let tools = client
            .connector_service_tools("mock.docs")
            .await
            .expect("tools");
        assert!(tools["tools"].as_array().unwrap().is_empty());
        let result = client
            .execute_connector_service(
                "mock.docs",
                serde_json::json!({
                    "actor_principal": "tui:operator",
                    "tool_id": "service.mock.docs.read",
                    "resource_id": "tui-doc",
                    "title": "TUI Doc",
                    "mode": "dry_run",
                }),
            )
            .await
            .expect("execute");
        assert_eq!(result["ok"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn connector_resource_lifecycle_routes_use_management_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept revalidate");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read revalidate");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/connectors/resources/revalidate HTTP/1.1"));
            assert!(req.contains("\"reference\":\"service://mock.docs/document/tui-doc\""));
            assert!(req.contains("\"state\":\"stale\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write revalidate");

            let (mut socket, _) = listener.accept().await.expect("accept promote");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read promote");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/connectors/resources/promote-memory HTTP/1.1"));
            assert!(req.contains("\"reference\":\"service://mock.docs/document/tui-doc\""));
            assert!(req.contains("\"session_id\":\"session-tui\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write promote");
        });

        let client = DaemonProjectionClient::new(format!("http://{addr}"), None).expect("client");
        let revalidated = client
            .revalidate_connector_resource("service://mock.docs/document/tui-doc", "stale")
            .await
            .expect("revalidate");
        assert_eq!(revalidated["ok"], true);
        let promoted = client
            .promote_connector_resource_to_memory(
                "service://mock.docs/document/tui-doc",
                Some("session-tui"),
            )
            .await
            .expect("promote");
        assert_eq!(promoted["ok"], true);
        server.await.expect("server task");
    }
}
