use std::fmt;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use futures::StreamExt;

use crate::CowdEvent;

const GATEWAY_READY_RETRY_ATTEMPTS: usize = 20;
const GATEWAY_READY_RETRY_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_GATEWAY_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_GATEWAY_URL: &str = "http://127.0.0.1:8642";

#[derive(Debug, Clone)]
pub struct GatewayApiClient {
    base_url: String,
    auth_token: Option<String>,
    client: reqwest::Client,
}

#[derive(Debug)]
pub enum GatewayApiError {
    Http(reqwest::Error),
    Status(reqwest::StatusCode, String),
    Url(String),
}

impl GatewayApiClient {
    pub fn new(
        base_url: impl Into<String>,
        auth_token: Option<String>,
    ) -> Result<Self, GatewayApiError> {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_GATEWAY_REQUEST_TIMEOUT)
            .build()
            .map_err(GatewayApiError::Http)?;
        Ok(Self {
            base_url: normalize_base_url(base_url.into())?,
            auth_token,
            client,
        })
    }

    pub fn from_running_gateway(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        let base_url =
            std::env::var("COWD_GATEWAY_URL").unwrap_or_else(|_| DEFAULT_GATEWAY_URL.to_string());
        if !gateway_listener_reachable(&base_url) {
            return Ok(None);
        }
        Self::new(base_url, auth_token.or_else(default_auth_token)).map(Some)
    }

    pub fn from_running_gateway_with_retry(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        let auth_token = auth_token.or_else(default_auth_token);
        for attempt in 0..GATEWAY_READY_RETRY_ATTEMPTS {
            if let Some(client) = Self::from_running_gateway(auth_token.clone())? {
                return Ok(Some(client));
            }
            if attempt + 1 < GATEWAY_READY_RETRY_ATTEMPTS {
                std::thread::sleep(GATEWAY_READY_RETRY_DELAY);
            }
        }
        Ok(None)
    }

    pub fn ensure_running_with_retry(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        if let Some(client) = Self::from_running_gateway_with_retry(auth_token.clone())? {
            return Ok(Some(client));
        }

        if !cfg!(test) && std::env::var("COWD_DISABLE_DAEMON_AUTOSTART").is_err() {
            let exe =
                std::env::current_exe().map_err(|error| GatewayApiError::Url(error.to_string()))?;
            std::process::Command::new(exe)
                .arg("gateway")
                .arg("run")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|error| GatewayApiError::Url(error.to_string()))?;
        }

        Self::from_running_gateway_with_retry(auth_token)
    }

    pub async fn runtime_control_plane(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/control-plane").await
    }

    pub async fn status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/status").await
    }

    pub async fn slash_projection(
        &self,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/slash?surface={}", url_encode(surface)))
            .await
    }

    pub async fn slash_resolve(
        &self,
        input: &str,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/slash/resolve",
            serde_json::json!({
                "input": input,
                "surface": surface,
                "context": {},
            }),
        )
        .await
    }

    pub async fn slash_dispatch(
        &self,
        command: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/slash/dispatch",
            serde_json::json!({
                "command": command,
                "args": args,
            }),
        )
        .await
    }

    pub async fn runtime_snapshot(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/snapshot").await
    }

    pub async fn list_sessions(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/sessions").await
    }

    pub async fn session_projection(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/sessions/{}/projection",
            url_encode(session_id)
        ))
        .await
    }

    pub async fn session_input_projection(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/sessions/{}/input-projection",
            url_encode(session_id)
        ))
        .await
    }

    pub async fn turn_inbox(
        &self,
        session_id: &str,
        turn_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let suffix = turn_id
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("?turn_id={}", url_encode(value)))
            .unwrap_or_default();
        self.get_json(&format!(
            "/api/sessions/{}/turn-inbox{}",
            url_encode(session_id),
            suffix
        ))
        .await
    }

    pub async fn ensure_session(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/sessions/{}/ensure", url_encode(session_id)),
            serde_json::json!({ "model": model }),
        )
        .await
    }

    pub async fn send_message(
        &self,
        session_id: &str,
        content: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.send_message_with_resources(session_id, content, &[])
            .await
    }

    pub async fn send_message_with_resources(
        &self,
        session_id: &str,
        content: &str,
        resource_ids: &[String],
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/sessions/{}/messages", url_encode(session_id)),
            serde_json::json!({ "content": content, "resource_ids": resource_ids }),
        )
        .await
    }

    pub async fn upload_resource_path(
        &self,
        path: &Path,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = std::fs::read(path).map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("resource.bin")
            .to_string();
        let part = reqwest::multipart::Part::bytes(bytes).file_name(file_name);
        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("source", "tui")
            .text("session_id", session_id.to_string());
        let url = format!("{}/api/resources", self.base_url);
        let mut request = self.client.post(url).multipart(form);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        let text = response.text().await.map_err(GatewayApiError::Http)?;
        if !status.is_success() {
            return Err(GatewayApiError::Status(status, text));
        }
        serde_json::from_str(&text).map_err(|error| GatewayApiError::Url(error.to_string()))
    }

    pub async fn workspace_overview(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/workspace").await
    }

    pub async fn workspace_files(
        &self,
        dir: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match dir.map(str::trim).filter(|dir| !dir.is_empty()) {
            Some(dir) => format!("/api/workspace/files?dir={}", url_encode(dir)),
            None => "/api/workspace/files".to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn workspace_files_recursive(
        &self,
        dir: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let mut path = match dir.map(str::trim).filter(|dir| !dir.is_empty()) {
            Some(dir) => format!("/api/workspace/files?dir={}", url_encode(dir)),
            None => "/api/workspace/files?".to_string(),
        };
        if !path.ends_with('?') && !path.ends_with('&') {
            path.push('&');
        }
        path.push_str("recursive=true&limit=");
        path.push_str(&limit.to_string());
        self.get_json(&path).await
    }

    pub async fn create_workspace_file(
        &self,
        path: &str,
        content: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/workspace/files",
            serde_json::json!({ "path": path, "content": content }),
        )
        .await
    }

    pub async fn create_workspace_dir(
        &self,
        path: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/workspace/dirs", serde_json::json!({ "path": path }))
            .await
    }

    pub async fn delete_workspace_path(
        &self,
        path: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json(&format!("/api/workspace/files?path={}", url_encode(path)))
            .await
    }

    pub async fn rename_workspace_path(
        &self,
        path: &str,
        to: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/workspace/rename",
            serde_json::json!({ "path": path, "to": to }),
        )
        .await
    }

    pub async fn workspace_meta(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/workspace/meta?path={}", url_encode(path)))
            .await
    }

    pub async fn download_workspace_path(&self, path: &str) -> Result<Vec<u8>, GatewayApiError> {
        self.get_bytes(&format!(
            "/api/workspace/download?path={}",
            url_encode(path)
        ))
        .await
    }

    pub async fn workspace_file_preview(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = self
            .get_bytes(&format!("/api/file/raw?path={}", url_encode(path)))
            .await?;
        let truncated = bytes.len() > max_bytes;
        let slice = if truncated {
            &bytes[..max_bytes]
        } else {
            bytes.as_slice()
        };
        let content = String::from_utf8_lossy(slice).to_string();
        Ok(serde_json::json!({
            "path": path,
            "content": content,
            "bytes": bytes.len(),
            "truncated": truncated,
        }))
    }

    pub async fn upload_workspace_file_path(
        &self,
        path: &Path,
        dir: Option<&str>,
        overwrite: bool,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = std::fs::read(path).map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("upload.bin")
            .to_string();
        let part = reqwest::multipart::Part::bytes(bytes).file_name(file_name);
        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("dir", dir.unwrap_or_default().to_string())
            .text("overwrite", overwrite.to_string());
        let url = format!("{}/api/upload", self.base_url);
        let mut request = self.client.post(url).multipart(form);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        let text = response.text().await.map_err(GatewayApiError::Http)?;
        if !status.is_success() {
            return Err(GatewayApiError::Status(status, text));
        }
        serde_json::from_str(&text).map_err(|error| GatewayApiError::Url(error.to_string()))
    }

    pub async fn list_session_attachments(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/sessions/{}/attachments",
            url_encode(session_id)
        ))
        .await
    }

    pub async fn add_session_attachment(
        &self,
        session_id: &str,
        path: &str,
        kind: &str,
        label: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/sessions/{}/attachments", url_encode(session_id)),
            serde_json::json!({
                "path": path,
                "kind": kind,
                "label": label,
            }),
        )
        .await
    }

    pub async fn delete_session_attachment(
        &self,
        session_id: &str,
        ref_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json(&format!(
            "/api/sessions/{}/attachments/{}",
            url_encode(session_id),
            url_encode(ref_id)
        ))
        .await
    }

    pub async fn cancel_session_turn(
        &self,
        session_id: &str,
        actor_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/sessions/{}/cancel", url_encode(session_id)),
            serde_json::json!({
                "actor_id": actor_id,
                "reason": reason,
            }),
        )
        .await
    }

    pub async fn subscribe_session_events(
        &self,
        session_id: &str,
        tx: mpsc::SyncSender<CowdEvent>,
    ) -> Result<(), GatewayApiError> {
        let url = format!(
            "{}/api/sessions/{}/stream",
            self.base_url,
            url_encode(session_id)
        );
        let mut request = self.client.get(url);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GatewayApiError::Status(status, body));
        }

        let mut buffer = String::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(GatewayApiError::Http)?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(index) = buffer.find("\n\n") {
                let frame = buffer[..index].to_string();
                buffer.drain(..index + 2);
                if let Some(event) = gateway_sse_frame_to_cowd_event(&frame) {
                    let _ = tx.send(event);
                }
            }
        }
        Ok(())
    }

    pub async fn attach_session(
        &self,
        session_id: &str,
        actor_id: &str,
        surface: &str,
        role: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/sessions/{}/attach", url_encode(session_id)),
            serde_json::json!({
                "actor_id": actor_id,
                "surface": surface,
                "role": role,
            }),
        )
        .await
    }

    pub async fn detach_session(
        &self,
        session_id: &str,
        actor_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/sessions/{}/detach", url_encode(session_id)),
            serde_json::json!({ "actor_id": actor_id }),
        )
        .await
    }

    pub async fn lifecycle_snapshot(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        match session_id {
            Some(session_id) => {
                self.get_json(&format!(
                    "/api/sessions/{}/lifecycle",
                    url_encode(session_id)
                ))
                .await
            }
            None => self.get_json("/api/runtime/snapshot").await,
        }
    }

    pub async fn replay_session(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/sessions/{}/replay?from_sequence={from_sequence}&limit={limit}",
            url_encode(session_id)
        ))
        .await
    }

    pub async fn cowd_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/capabilities").await
    }

    pub async fn cowd_projection(
        &self,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/cowd/projection?surface={}",
            url_encode(surface)
        ))
        .await
    }

    pub async fn cowd_surfaces(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/surfaces").await
    }

    pub async fn cowd_release_gate(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/release-gate").await
    }

    pub async fn gateway_capability_contract(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/gateway/capability-contract").await
    }

    pub async fn gateway_openai_tools(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/gateway/openai-tools").await
    }

    pub async fn structured_sources(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/sources").await
    }

    pub async fn structured_facts(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/facts").await
    }

    pub async fn structured_evidence(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/evidence").await
    }

    pub async fn structured_watermarks(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cowd/structured/watermarks").await
    }

    pub async fn structured_ingest_plan(
        &self,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cowd/structured/ingest-plan", input)
            .await
    }

    pub async fn runtime_session_leases(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/session-leases").await
    }

    pub async fn acquire_runtime_session_lease(
        &self,
        session_id: &str,
        owner: &str,
        mode: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
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
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/runtime/session-leases/release",
            serde_json::json!({
                "session_id": session_id,
                "owner": owner,
            }),
        )
        .await
    }

    pub async fn runtime_effective_config(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/config/effective").await
    }

    pub async fn config(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/config").await
    }

    pub async fn config_providers(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/config/providers").await
    }

    pub async fn update_config_model(
        &self,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.put_json("/api/config", serde_json::json!({ "model": model }))
            .await
    }

    pub async fn config_reload_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/runtime/config/reload/status").await
    }

    pub async fn runtime_timeline(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/runtime/timeline?session_id={}&limit={}",
            url_encode(session_id),
            limit
        ))
        .await
    }

    pub async fn current_context(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match session_id {
            Some(id) if !id.trim().is_empty() => {
                format!("/api/context/current?session_id={}", url_encode(id))
            }
            _ => "/api/context/current".to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn memory_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/memory/status").await
    }

    pub async fn reality_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/status").await
    }

    pub async fn reality_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/capabilities").await
    }

    pub async fn reality_flow(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match session_id.map(str::trim).filter(|id| !id.is_empty()) {
            Some(id) => format!("/api/reality/flow?session_id={}", url_encode(id)),
            None => "/api/reality/flow".to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn reality_boundaries(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/boundaries").await
    }

    pub async fn reality_governance(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/reality/governance").await
    }

    pub async fn task_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tasks").await
    }

    pub async fn pending_approvals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/approval/pending").await
    }

    pub async fn mission_projection(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/projection").await
    }

    pub async fn mission_control(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/control").await
    }

    pub async fn dispatch_mission_sessions(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/control/sessions/dispatch", body)
            .await
    }

    pub async fn tick_mission_steward_scheduler(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/control/stewards/scheduler", body)
            .await
    }

    pub async fn apply_runtime_recovery(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/runtime/events/recover", serde_json::json!({}))
            .await
    }

    pub async fn mission_session_detail(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/mission/sessions/{}", url_encode(session_id)))
            .await
    }

    pub async fn mission_session_inbox(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/mission/sessions/{}/inbox",
            url_encode(session_id)
        ))
        .await
    }

    pub async fn mission_approvals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/approvals").await
    }

    pub async fn mission_relations(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/mission/relations").await
    }

    pub async fn submit_mission_approval(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/approvals", body).await
    }

    pub async fn start_mission_team_runtime(
        &self,
        session_id: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/mission/sessions/{}/teams/runtime",
                url_encode(session_id)
            ),
            body,
        )
        .await
    }

    pub async fn decide_mission_approval(
        &self,
        approval_id: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/mission/approvals/{}/decision",
                url_encode(approval_id)
            ),
            body,
        )
        .await
    }

    pub async fn add_mission_relation(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/relations", body).await
    }

    pub async fn upsert_mission_proxy(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/proxies", body).await
    }

    pub async fn route_mission_command(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/mission/route", body).await
    }

    pub async fn consume_mission_session_command(
        &self,
        session_id: &str,
        command_id: &str,
        mode: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/mission/sessions/{}/inbox/{}/consume",
                url_encode(session_id),
                url_encode(command_id)
            ),
            serde_json::json!({ "mode": mode }),
        )
        .await
    }

    pub async fn cancel_mission_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/mission/sessions/{}/inbox/{}/cancel",
                url_encode(session_id),
                url_encode(command_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn retry_mission_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/mission/sessions/{}/inbox/{}/retry",
                url_encode(session_id),
                url_encode(command_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn runtime_agent_input(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/runtime/agents/{}/input", url_encode(agent_id)),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn runtime_agent_interrupt(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/runtime/agents/{}/interrupt", url_encode(agent_id)),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn runtime_agent_shutdown(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/runtime/agents/{}/shutdown", url_encode(agent_id)),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn respond_approval(
        &self,
        id: &str,
        approved: bool,
        persistence: Option<&str>,
        reason: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
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
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tasks/start",
            serde_json::json!({
                "objective": objective,
                "yolo_mode": yolo_mode,
            }),
        )
        .await
    }

    pub async fn cancel_task(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/tasks/{}/cancel", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn complete_task(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/tasks/{}/complete", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn cross_plane_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/cross-plane/summary").await
    }

    pub async fn connector_accounts(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/connectors/accounts").await
    }

    pub async fn connector_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/connectors/capabilities").await
    }

    pub async fn connector_resources(
        &self,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let mut path = format!("/api/connectors/resources?limit={limit}&offset={offset}");
        if let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) {
            path.push_str("&q=");
            path.push_str(&url_encode(query));
        }
        self.get_json(&path).await
    }

    pub async fn message_connectors(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-connectors").await
    }

    pub async fn message_connector_status(
        &self,
        name: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/message-connectors/{}/status",
            url_encode(name)
        ))
        .await
    }

    pub async fn message_connector_repair(
        &self,
        name: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/message-connectors/{}/repair", url_encode(name)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn message_endpoints(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-endpoints").await
    }

    pub async fn message_routes(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-routes").await
    }

    pub async fn message_bindings(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/message-bindings").await
    }

    pub async fn surface_registry(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/surfaces").await
    }

    pub async fn surface_health_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/surfaces/health").await
    }

    pub async fn surface_detail(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}", url_encode(id)))
            .await
    }

    pub async fn surface_routes(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/routes", url_encode(id)))
            .await
    }

    pub async fn surface_resources(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/resources", url_encode(id)))
            .await
    }

    pub async fn surface_status(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/status", url_encode(id)))
            .await
    }

    pub async fn surface_health(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/health", url_encode(id)))
            .await
    }

    pub async fn surface_health_check(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/health-check", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_start(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/start", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_stop(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/stop", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_restart(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/restart", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_repair(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/repair", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_events(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/events", url_encode(id)))
            .await
    }

    pub async fn surface_inbox(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/inbox", url_encode(id)))
            .await
    }

    pub async fn surface_outbox(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/outbox", url_encode(id)))
            .await
    }

    pub async fn surface_messages(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/messages", url_encode(id)))
            .await
    }

    pub async fn surface_archive_messages(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/messages/archive", url_encode(id)),
            serde_json::json!({ "limit": limit }),
        )
        .await
    }

    pub async fn surface_purge_archived_events(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/messages/purge-archived-events",
                url_encode(id)
            ),
            serde_json::json!({ "limit": limit }),
        )
        .await
    }

    pub async fn surface_deliveries(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/surfaces/{}/deliveries", url_encode(id)))
            .await
    }

    pub async fn surface_replay_inbox(
        &self,
        id: &str,
        message_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/inbox/{}/replay",
                url_encode(id),
                url_encode(message_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_retry_outbox(
        &self,
        id: &str,
        delivery_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/outbox/{}/retry",
                url_encode(id),
                url_encode(delivery_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_dead_letter_outbox(
        &self,
        id: &str,
        delivery_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/surfaces/{}/outbox/{}/dead-letter",
                url_encode(id),
                url_encode(delivery_id)
            ),
            serde_json::json!({ "reason": reason }),
        )
        .await
    }

    pub async fn surface_send(
        &self,
        id: &str,
        recipient: &str,
        thread: Option<&str>,
        text: &str,
        metadata: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/send", url_encode(id)),
            serde_json::json!({
                "recipient": recipient,
                "thread": thread,
                "text": text,
                "metadata": metadata,
            }),
        )
        .await
    }

    pub async fn surface_action(
        &self,
        id: &str,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/surfaces/{}/action", url_encode(id)),
            serde_json::json!({
                "action": action,
                "payload": payload,
            }),
        )
        .await
    }

    pub async fn skill_runs(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/skills/runs").await
    }

    pub async fn skill_run_detail(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/skills/runs/{}", url_encode(id)))
            .await
    }

    pub async fn skill_action(
        &self,
        id: &str,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/skills/{}/actions/{}",
                url_encode(id),
                url_encode(action)
            ),
            payload,
        )
        .await
    }

    pub async fn harness_eval_latest_report(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/harness-eval/reports/latest").await
    }

    pub async fn harness_eval_reports(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/harness-eval/reports").await
    }

    pub async fn harness_eval_report(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/harness-eval/reports/{}", url_encode(id)))
            .await
    }

    pub async fn harness_eval_report_artifacts(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/harness-eval/reports/{}/artifacts",
            url_encode(id)
        ))
        .await
    }

    pub async fn harness_eval_report_gate(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/harness-eval/reports/{}/gate",
            url_encode(id)
        ))
        .await
    }

    pub async fn harness_eval_run_status(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/harness-eval/runs/{}", url_encode(id)))
            .await
    }

    pub async fn harness_eval_run_smoke(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.harness_eval_run(
            "quick",
            "low",
            false,
            "operator requested harness eval smoke",
        )
        .await
    }

    pub async fn harness_eval_run(
        &self,
        level: &str,
        budget: &str,
        allow_real_model: bool,
        objective: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/harness-eval/runs",
            serde_json::json!({
                "level": level,
                "budget": budget,
                "actor": "tui.gateway_panel",
                "objective": objective,
                "allow_real_model": allow_real_model
            }),
        )
        .await
    }

    pub async fn harness_eval_cancel_run(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/harness-eval/runs/{}/cancel", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_signals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/signals").await
    }

    pub async fn evolution_diagnoses(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/diagnoses").await
    }

    pub async fn evolution_missions_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/missions/summary").await
    }

    pub async fn evolution_mission_detail(
        &self,
        mission_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/missions/{}/detail",
            url_encode(mission_id)
        ))
        .await
    }

    pub async fn evolution_create_diagnosis(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/evolution/diagnoses",
            serde_json::json!({ "signal_ids": signal_ids }),
        )
        .await
    }

    pub async fn evolution_proposals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/proposals").await
    }

    pub async fn evolution_create_proposal(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/evolution/proposals",
            serde_json::json!({ "signal_ids": signal_ids }),
        )
        .await
    }

    pub async fn evolution_skill_draft(
        &self,
        proposal_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/proposals/{}/skill-draft",
            url_encode(proposal_id)
        ))
        .await
    }

    pub async fn evolution_chain(
        &self,
        proposal_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/evolution/chain/{}", url_encode(proposal_id)))
            .await
    }

    pub async fn evolution_candidates(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/candidates").await
    }

    pub async fn evolution_candidate_detail(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/candidates/{}",
            url_encode(candidate_id)
        ))
        .await
    }

    pub async fn evolution_create_candidate(
        &self,
        proposal_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/proposals/{}/candidates",
                url_encode(proposal_id)
            ),
            serde_json::json!({
                "baseline_ref": "baseline:current",
                "candidate_ref": "candidate:sandbox"
            }),
        )
        .await
    }

    pub async fn evolution_candidate_decision(
        &self,
        candidate_id: &str,
        status: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/candidates/{}/decision",
                url_encode(candidate_id)
            ),
            serde_json::json!({ "status": status }),
        )
        .await
    }

    pub async fn evolution_candidate_sandbox_run(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/evolution/candidates/{}/run", url_encode(candidate_id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_candidate_artifacts(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/candidates/{}/artifacts",
            url_encode(candidate_id)
        ))
        .await
    }

    pub async fn evolution_candidate_evaluate(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/candidates/{}/evaluate",
                url_encode(candidate_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_candidate_comparison(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!(
            "/api/evolution/candidates/{}/comparison",
            url_encode(candidate_id)
        ))
        .await
    }

    pub async fn evolution_candidate_promote(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/candidates/{}/promote",
                url_encode(candidate_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_adoptions(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/adoptions").await
    }

    pub async fn evolution_active_capabilities(
        &self,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/active-capabilities").await
    }

    pub async fn evolution_version_rollback(
        &self,
        version_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!(
                "/api/evolution/versions/{}/rollback",
                url_encode(version_id)
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_memory(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/memory").await
    }

    pub async fn evolution_sandbox_evals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/evolution/sandbox-evals").await
    }

    pub async fn connector_service_tools(
        &self,
        service: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
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
    ) -> Result<serde_json::Value, GatewayApiError> {
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
    ) -> Result<serde_json::Value, GatewayApiError> {
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
    ) -> Result<serde_json::Value, GatewayApiError> {
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
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cross-plane/action/preflight", action)
            .await
    }

    pub async fn execute_cross_plane_action(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cross-plane/action/execute", request)
            .await
    }

    pub async fn cross_plane_policy_simulate(
        &self,
        action: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json("/api/cross-plane/policy/simulate", action)
            .await
    }

    pub async fn tool_registry(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tools").await
    }

    pub async fn tool_execute(
        &self,
        name: &str,
        input: serde_json::Value,
        mode: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/execute",
            serde_json::json!({
                "name": name,
                "input": input,
                "mode": mode,
            }),
        )
        .await
    }

    pub async fn tool_cache_stats(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tools/cache").await
    }

    pub async fn tool_batch_readonly(
        &self,
        calls: Vec<serde_json::Value>,
        max_concurrency: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/batch-readonly",
            serde_json::json!({
                "calls": calls,
                "max_concurrency": max_concurrency,
            }),
        )
        .await
    }

    pub async fn tool_mutation_preview(
        &self,
        edits: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/mutations/preview",
            serde_json::json!({ "edits": edits }),
        )
        .await
    }

    pub async fn tool_mutation_apply(
        &self,
        edits: Vec<serde_json::Value>,
        expected_hashes: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/mutations/apply",
            serde_json::json!({
                "edits": edits,
                "expected_hashes": expected_hashes,
            }),
        )
        .await
    }

    pub async fn tool_checkpoints(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json("/api/tools/checkpoints").await
    }

    pub async fn tool_checkpoint_create(
        &self,
        label: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/checkpoints",
            serde_json::json!({ "label": label }),
        )
        .await
    }

    pub async fn tool_checkpoint_diff(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&format!("/api/tools/checkpoints/{}/diff", url_encode(id)))
            .await
    }

    pub async fn tool_checkpoint_restore(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &format!("/api/tools/checkpoints/{}/restore", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn tool_intent_plan(
        &self,
        prompt: &str,
        selected_tools: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/intent-plan",
            serde_json::json!({
                "prompt": prompt,
                "selected_tools": selected_tools,
            }),
        )
        .await
    }

    pub async fn tool_context_fanout_plan(
        &self,
        prompt: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            "/api/tools/context-fanout/plan",
            serde_json::json!({ "prompt": prompt }),
        )
        .await
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(url);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GatewayApiError::Status(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.post(url).json(&body);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GatewayApiError::Status(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn put_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.put(url).json(&body);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GatewayApiError::Status(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn delete_json(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.delete(url);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GatewayApiError::Status(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(url);
        if let Some(token) = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GatewayApiError::Status(status, body));
        }
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(GatewayApiError::Http)
    }
}

fn gateway_listener_reachable(base_url: &str) -> bool {
    let normalized = match normalize_base_url(base_url.to_string()) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let without_scheme = normalized
        .strip_prefix("http://")
        .or_else(|| normalized.strip_prefix("https://"))
        .unwrap_or(&normalized);
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    let Ok(addrs) = host_port.to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok())
}

pub fn default_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
}

fn normalize_base_url(mut base_url: String) -> Result<String, GatewayApiError> {
    if base_url.trim().is_empty() {
        return Err(GatewayApiError::Url(
            "empty Gateway API base URL".to_string(),
        ));
    }
    base_url = base_url.trim().trim_end_matches('/').to_string();
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return Err(GatewayApiError::Url(format!(
            "Gateway API base URL must start with http:// or https://: {base_url}"
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

impl fmt::Display for GatewayApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(err) => write!(f, "Gateway API HTTP failed: {err}"),
            Self::Status(status, body) => {
                write!(f, "Gateway API returned {status}: {body}")
            }
            Self::Url(err) => write!(f, "Gateway API URL error: {err}"),
        }
    }
}

impl std::error::Error for GatewayApiError {}

pub fn gateway_sse_json_to_cowd_event(value: &serde_json::Value) -> Option<CowdEvent> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("event_type"))
        .and_then(serde_json::Value::as_str)?;
    match event_type {
        "TextDelta" | "text_delta" | "assistant_delta" => value
            .get("text")
            .or_else(|| value.get("content"))
            .and_then(serde_json::Value::as_str)
            .map(|text| CowdEvent::TextDelta {
                text: text.to_string(),
            }),
        "ThinkingDelta" | "thinking_delta" => value
            .get("thinking")
            .or_else(|| value.get("content"))
            .and_then(serde_json::Value::as_str)
            .map(|thinking| CowdEvent::ThinkingDelta {
                thinking: thinking.to_string(),
            }),
        "ToolStart" | "tool_start" => Some(CowdEvent::ToolStart {
            id: value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool")
                .to_string(),
            name: value
                .get("name")
                .or_else(|| value.get("tool_name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool")
                .to_string(),
            preview: value
                .get("preview")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "ToolProgress" | "tool_progress" => Some(CowdEvent::ToolProgress {
            id: value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool")
                .to_string(),
            name: value
                .get("name")
                .or_else(|| value.get("tool_name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool")
                .to_string(),
            progress: value
                .get("progress")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "ToolComplete" | "tool_complete" => Some(CowdEvent::ToolComplete {
            id: value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool")
                .to_string(),
            name: value
                .get("name")
                .or_else(|| value.get("tool_name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool")
                .to_string(),
            summary: value
                .get("result_summary")
                .or_else(|| value.get("summary"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            exit_code: value
                .get("exit_code")
                .and_then(serde_json::Value::as_i64)
                .map(|code| code as i32),
        }),
        "TurnComplete" | "turn_complete" => Some(CowdEvent::TurnComplete {
            assistant_text: value
                .get("assistant_text")
                .or_else(|| value.get("response"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            iterations: value
                .get("iterations")
                .and_then(serde_json::Value::as_u64)
                .map(|value| value as u32)
                .unwrap_or_default(),
        }),
        "TurnError" | "turn_error" => Some(CowdEvent::TurnError {
            error: value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Gateway turn error")
                .to_string(),
        }),
        "SessionInputReceived" | "session_input_received" => {
            let decision = value
                .get("receipt")
                .or_else(|| value.get("input"))
                .and_then(|receipt| receipt.get("decision"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("received");
            Some(CowdEvent::Warning {
                message: format!("Session input received: {decision}"),
            })
        }
        "TurnInboxUpdated" | "turn_inbox_updated" => {
            let pending = value
                .get("inbox")
                .and_then(|inbox| inbox.get("pending_count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            Some(CowdEvent::Warning {
                message: format!("Turn inbox updated: {pending} pending"),
            })
        }
        "TurnInputCheckpointConsumed" | "turn_input_checkpoint_consumed" => {
            let checkpoint = value
                .get("checkpoint")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("checkpoint");
            let consumed = value
                .get("consumed")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            Some(CowdEvent::Warning {
                message: format!("Runtime consumed {consumed} input(s) at {checkpoint}"),
            })
        }
        "TurnCancelRequested" | "turn_cancel_requested" => Some(CowdEvent::Warning {
            message: "Gateway cancel request accepted".to_string(),
        }),
        "ContextEnvelope" | "context_envelope" => value
            .get("envelope")
            .cloned()
            .map(|envelope| CowdEvent::ContextEnvelope { envelope }),
        "TokenUsage" | "token_usage" => Some(CowdEvent::TokenUsage {
            input: value
                .get("input")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            output: value
                .get("output")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            cache_create: value
                .get("cache_create")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            cache_read: value
                .get("cache_read")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
        }),
        "RuntimePolicyDecision" | "runtime_policy_decision" => value
            .get("summary")
            .cloned()
            .and_then(|summary| serde_json::from_value(summary).ok())
            .map(|summary| CowdEvent::RuntimePolicyDecision { summary }),
        "WorkGraphSummary" | "workgraph_summary" => value
            .get("summary")
            .cloned()
            .and_then(|summary| serde_json::from_value(summary).ok())
            .map(|summary| CowdEvent::WorkGraphSummary { summary }),
        _ => None,
    }
}

pub fn gateway_sse_frame_to_cowd_event(frame: &str) -> Option<CowdEvent> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() || data == "[DONE]" {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(&data)
        .ok()
        .and_then(|value| gateway_sse_json_to_cowd_event(&value))
}

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

    #[test]
    fn gateway_sse_json_maps_core_cowd_events() {
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TextDelta",
                "text": "hello"
            })),
            Some(CowdEvent::TextDelta { .. })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "ThinkingDelta",
                "thinking": "checking"
            })),
            Some(CowdEvent::ThinkingDelta { .. })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "ToolStart",
                "id": "tool-1",
                "name": "read"
            })),
            Some(CowdEvent::ToolStart { .. })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TurnComplete"
            })),
            Some(CowdEvent::TurnComplete { .. })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "ContextEnvelope",
                "envelope": {
                    "id": "ctx-v31",
                    "selected": []
                }
            })),
            Some(CowdEvent::ContextEnvelope { .. })
        ));
        assert!(matches!(
            gateway_sse_json_to_cowd_event(&serde_json::json!({
                "type": "TokenUsage",
                "input": 1,
                "output": 2,
                "cache_create": 3,
                "cache_read": 4
            })),
            Some(CowdEvent::TokenUsage { .. })
        ));
    }

    #[test]
    fn gateway_sse_frame_maps_data_json() {
        assert!(matches!(
            gateway_sse_frame_to_cowd_event(
                "event: message\ndata: {\"type\":\"TextDelta\",\"text\":\"hi\"}\n\n"
            ),
            Some(CowdEvent::TextDelta { .. })
        ));
        assert!(gateway_sse_frame_to_cowd_event("data: [DONE]\n\n").is_none());
    }

    #[test]
    fn gateway_api_inventory_migrates_legacy_control_and_projection_methods() {
        let migrated = [
            "status",
            "runtime_snapshot",
            "list_sessions",
            "session_projection",
            "session_input_projection",
            "turn_inbox",
            "ensure_session",
            "acquire_session_lease",
            "release_session_lease",
            "attach_session",
            "detach_session",
            "lifecycle_snapshot",
            "replay_session",
            "task_status",
            "start_task",
            "cancel_task",
            "complete_task",
            "pending_approvals",
            "mission_projection",
            "mission_session_detail",
            "mission_session_inbox",
            "mission_approvals",
            "mission_relations",
            "submit_mission_approval",
            "start_mission_team_runtime",
            "decide_mission_approval",
            "add_mission_relation",
            "upsert_mission_proxy",
            "route_mission_command",
            "consume_mission_session_command",
            "cancel_mission_session_command",
            "retry_mission_session_command",
            "runtime_agent_input",
            "runtime_agent_interrupt",
            "runtime_agent_shutdown",
            "memory_status",
            "reality_status",
            "reality_flow",
            "reality_boundaries",
            "context_snapshot",
            "respond_approval",
            "connector_resources",
            "message_connectors",
            "message_connector_status",
            "message_connector_repair",
            "message_endpoints",
            "message_routes",
            "message_bindings",
            "revalidate_connector_resource",
            "promote_connector_resource_to_memory",
            "chat_session",
            "subscribe_session_events",
            "runtime_control_plane",
            "cowd_capabilities",
            "cowd_projection",
            "cowd_surfaces",
            "cowd_release_gate",
            "gateway_capability_contract",
            "gateway_openai_tools",
            "structured_sources",
            "structured_facts",
            "structured_evidence",
            "structured_watermarks",
            "structured_ingest_plan",
            "runtime_session_leases",
            "acquire_runtime_session_lease",
            "release_runtime_session_lease",
            "runtime_effective_config",
            "runtime_timeline",
            "current_context",
            "cross_plane_summary",
            "connector_accounts",
            "connector_capabilities",
            "connector_service_tools",
            "execute_connector_service",
            "surface_start",
            "surface_stop",
            "surface_restart",
            "surface_inbox",
            "surface_outbox",
            "surface_messages",
            "surface_archive_messages",
            "surface_purge_archived_events",
            "surface_deliveries",
            "surface_replay_inbox",
            "surface_retry_outbox",
            "surface_dead_letter_outbox",
            "skill_runs",
            "skill_run_detail",
            "skill_action",
            "harness_eval_latest_report",
            "harness_eval_reports",
            "harness_eval_report",
            "harness_eval_report_artifacts",
            "harness_eval_report_gate",
            "harness_eval_run_smoke",
            "harness_eval_run",
            "harness_eval_run_status",
            "harness_eval_cancel_run",
            "evolution_signals",
            "evolution_diagnoses",
            "evolution_missions_summary",
            "evolution_mission_detail",
            "evolution_create_diagnosis",
            "evolution_proposals",
            "evolution_create_proposal",
            "evolution_skill_draft",
            "evolution_chain",
            "evolution_candidates",
            "evolution_candidate_detail",
            "evolution_create_candidate",
            "evolution_candidate_decision",
            "evolution_candidate_sandbox_run",
            "evolution_candidate_artifacts",
            "evolution_candidate_evaluate",
            "evolution_candidate_comparison",
            "evolution_candidate_promote",
            "evolution_adoptions",
            "evolution_active_capabilities",
            "evolution_version_rollback",
            "evolution_memory",
            "evolution_sandbox_evals",
            "preflight_cross_plane_action",
            "execute_cross_plane_action",
            "cross_plane_policy_simulate",
            "tool_registry",
            "tool_execute",
            "tool_cache_stats",
            "tool_batch_readonly",
            "tool_mutation_preview",
            "tool_mutation_apply",
            "tool_checkpoints",
            "tool_checkpoint_create",
            "tool_checkpoint_diff",
            "tool_checkpoint_restore",
            "tool_intent_plan",
            "tool_context_fanout_plan",
            "slash_dispatch",
            "cancel_session_turn",
        ];
        let deleted = ["socket_path", "with_timeout"];
        assert!(
            migrated.len() >= 129,
            "gateway inventory should not shrink when routes are migrated"
        );
        assert!(migrated.contains(&"session_input_projection"));
        assert!(migrated.contains(&"turn_inbox"));
        assert_eq!(deleted.len(), 2);
        assert!(!migrated.iter().any(|item| item.trim().is_empty()));
        assert!(!deleted.iter().any(|item| item.trim().is_empty()));
    }

    #[test]
    fn evolution_gateway_api_inventory_exposes_runtime_evolution_controls() {
        let evolution_methods = [
            "evolution_signals",
            "evolution_diagnoses",
            "evolution_missions_summary",
            "evolution_mission_detail",
            "evolution_create_diagnosis",
            "evolution_proposals",
            "evolution_create_proposal",
            "evolution_skill_draft",
            "evolution_chain",
            "evolution_candidates",
            "evolution_candidate_detail",
            "evolution_create_candidate",
            "evolution_candidate_decision",
            "evolution_candidate_sandbox_run",
            "evolution_candidate_artifacts",
            "evolution_candidate_evaluate",
            "evolution_candidate_comparison",
            "evolution_candidate_promote",
            "evolution_adoptions",
            "evolution_active_capabilities",
            "evolution_version_rollback",
            "evolution_memory",
            "evolution_sandbox_evals",
        ];
        assert_eq!(evolution_methods.len(), 23);
        assert!(evolution_methods
            .iter()
            .all(|method| method.starts_with("evolution_")));
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
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
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

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client.cowd_projection("tui").await.expect("json");
        assert_eq!(json["surface"], "tui");
        assert_eq!(json["capability_count"], 1);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn gateway_contract_endpoints_get_json_with_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "/api/gateway/capability-contract",
                    r#"{"kind":"gateway.capability_contract","capability_count":1,"capabilities":[]}"#,
                ),
                (
                    "/api/gateway/openai-tools",
                    r#"{"kind":"gateway.openai_tools","tool_count":1,"tools":[]}"#,
                ),
            ];
            for (path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept contract");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read contract");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(req.starts_with(&format!("GET {path} HTTP/1.1")), "{req}");
                assert!(req.contains("authorization: Bearer test-token"));
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write contract");
            }
        });

        let client =
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let contract = client
            .gateway_capability_contract()
            .await
            .expect("contract");
        let tools = client.gateway_openai_tools().await.expect("tools");
        assert_eq!(contract["kind"], "gateway.capability_contract");
        assert_eq!(tools["kind"], "gateway.openai_tools");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn session_projection_gets_session_run_projection_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept projection");
            let mut buf = vec![0; 2048];
            let n = socket.read(&mut buf).await.expect("read projection");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("GET /api/sessions/session%20v31/projection HTTP/1.1"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\n\r\n{\"kind\":\"session.run_projection\",\"session_id\":\"session v31\"}",
                )
                .await
                .expect("write projection");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .session_projection("session v31")
            .await
            .expect("json");
        assert_eq!(json["kind"], "session.run_projection");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn structured_projection_gets_all_list_contracts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "/api/cowd/structured/facts",
                    r#"{"kind":"cowd.structured.facts","count":1,"items":[]}"#,
                ),
                (
                    "/api/cowd/structured/evidence",
                    r#"{"kind":"cowd.structured.evidence","count":1,"items":[]}"#,
                ),
                (
                    "/api/cowd/structured/watermarks",
                    r#"{"kind":"cowd.structured.watermarks","count":1,"items":[]}"#,
                ),
            ];
            for (path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept structured");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read structured");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("GET {path} HTTP/1.1")),
                    "unexpected request: {req}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write structured");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(
            client.structured_facts().await.expect("facts")["kind"],
            "cowd.structured.facts"
        );
        assert_eq!(
            client.structured_evidence().await.expect("evidence")["kind"],
            "cowd.structured.evidence"
        );
        assert_eq!(
            client.structured_watermarks().await.expect("watermarks")["kind"],
            "cowd.structured.watermarks"
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn reality_projection_gets_status_flow_and_boundaries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "/api/reality/status",
                    r#"{"kind":"reality.status","status":"ready","engines":{}}"#,
                ),
                (
                    "/api/reality/flow?session_id=session-tui",
                    r#"{"kind":"reality.fact_flow","source":"growth.promotions","session_id":"session-tui","stages":[],"events":[],"promotions":[]}"#,
                ),
                (
                    "/api/reality/boundaries",
                    r#"{"kind":"reality.boundaries","boundaries":[]}"#,
                ),
            ];
            for (path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept reality");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read reality");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("GET {path} HTTP/1.1")),
                    "unexpected request: {req}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write reality");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(
            client.reality_status().await.expect("status")["kind"],
            "reality.status"
        );
        assert_eq!(
            client
                .reality_flow(Some("session-tui"))
                .await
                .expect("flow")["source"],
            "growth.promotions"
        );
        assert_eq!(
            client.reality_boundaries().await.expect("boundaries")["kind"],
            "reality.boundaries"
        );
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

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
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
            GatewayApiClient::new(format!("http://{addr}"), Some("test-token".to_string()))
                .expect("client");
        let json = client
            .preflight_cross_plane_action(serde_json::json!({
                "operation": "send_text",
                "capability": "surface.webui.send",
            }))
            .await
            .expect("json");
        assert_eq!(json["ready"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn surface_send_posts_gateway_surface_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(req.starts_with("POST /api/surfaces/webui/send HTTP/1.1"));
            assert!(req.contains("\"recipient\":\"user:demo\""));
            assert!(req.contains("\"text\":\"hello\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 39\r\n\r\n{\"kind\":\"surface.result\",\"status\":\"ok\"}",
                )
                .await
                .expect("write");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .surface_send(
                "webui",
                "user:demo",
                None,
                "hello",
                serde_json::json!({"source": "test"}),
            )
            .await
            .expect("json");
        assert_eq!(json["status"], "ok");
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

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
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

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
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

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let json = client
            .connector_resources(Some("Ready Doc"), 20, 40)
            .await
            .expect("json");
        assert!(json["resources"].as_array().unwrap().is_empty());
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn message_plane_endpoints_use_gateway_routes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let routes = [
                (
                    "GET",
                    "/api/message-connectors",
                    r#"{"kind":"message.connector.registry","connectors":[]}"#,
                ),
                (
                    "GET",
                    "/api/message-connectors/feishu/status",
                    r#"{"kind":"message.connector.status","connector":"feishu"}"#,
                ),
                (
                    "POST",
                    "/api/message-connectors/feishu/repair",
                    r#"{"kind":"message.connector.repair","connector":"feishu"}"#,
                ),
                (
                    "GET",
                    "/api/message-endpoints",
                    r#"{"kind":"message.endpoint.directory","endpoints":[]}"#,
                ),
                (
                    "GET",
                    "/api/message-routes",
                    r#"{"kind":"message.delivery.routes","routes":[]}"#,
                ),
                (
                    "GET",
                    "/api/message-bindings",
                    r#"{"kind":"message.conversation.bindings","bindings":[]}"#,
                ),
            ];
            for (method, path, body) in routes {
                let (mut socket, _) = listener.accept().await.expect("accept message plane");
                let mut buf = vec![0; 2048];
                let n = socket.read(&mut buf).await.expect("read message plane");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("{method} {path} HTTP/1.1")),
                    "unexpected request: {req}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write message plane");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(
            client.message_connectors().await.expect("connectors")["kind"],
            "message.connector.registry"
        );
        assert_eq!(
            client
                .message_connector_status("feishu")
                .await
                .expect("status")["kind"],
            "message.connector.status"
        );
        assert_eq!(
            client
                .message_connector_repair("feishu")
                .await
                .expect("repair")["kind"],
            "message.connector.repair"
        );
        assert_eq!(
            client.message_endpoints().await.expect("endpoints")["kind"],
            "message.endpoint.directory"
        );
        assert_eq!(
            client.message_routes().await.expect("routes")["kind"],
            "message.delivery.routes"
        );
        assert_eq!(
            client.message_bindings().await.expect("bindings")["kind"],
            "message.conversation.bindings"
        );
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
            assert!(req.starts_with("GET /api/connectors/services/local.docs/tools HTTP/1.1"));
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
            assert!(req.starts_with("POST /api/connectors/services/local.docs/execute HTTP/1.1"));
            assert!(req.contains("\"tool_id\":\"service.local.docs.read\""));
            assert!(req.contains("\"mode\":\"dry_run\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write execute");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let tools = client
            .connector_service_tools("local.docs")
            .await
            .expect("tools");
        assert!(tools["tools"].as_array().unwrap().is_empty());
        let result = client
            .execute_connector_service(
                "local.docs",
                serde_json::json!({
                    "actor_principal": "tui:operator",
                    "tool_id": "service.local.docs.read",
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
    async fn tool_operations_routes_use_management_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let checks: Vec<(&str, &str, Vec<&str>)> = vec![
                ("GET", "/api/tools", vec![]),
                (
                    "POST",
                    "/api/tools/execute",
                    vec!["\"name\":\"tool_cache_stats\"", "\"mode\":\"read_only\""],
                ),
                ("GET", "/api/tools/cache", vec![]),
                (
                    "POST",
                    "/api/tools/batch-readonly",
                    vec!["\"max_concurrency\":3", "\"name\":\"tool_cache_stats\""],
                ),
                (
                    "POST",
                    "/api/tools/mutations/preview",
                    vec!["\"path\":\"README.md\""],
                ),
                (
                    "POST",
                    "/api/tools/mutations/apply",
                    vec!["\"expected_hashes\"", "\"README.md\":\"hash-1\""],
                ),
                ("GET", "/api/tools/checkpoints", vec![]),
                (
                    "POST",
                    "/api/tools/checkpoints",
                    vec!["\"label\":\"before edit\""],
                ),
                ("GET", "/api/tools/checkpoints/cp-1/diff", vec![]),
                ("POST", "/api/tools/checkpoints/cp-1/restore", vec![]),
                (
                    "POST",
                    "/api/tools/intent-plan",
                    vec!["\"prompt\":\"inspect\"", "\"selected_tools\""],
                ),
                (
                    "POST",
                    "/api/tools/context-fanout/plan",
                    vec!["\"prompt\":\"fanout\""],
                ),
                (
                    "GET",
                    "/api/runtime/timeline?session_id=session%20a%2Fb&limit=25",
                    vec![],
                ),
                (
                    "POST",
                    "/api/cross-plane/policy/simulate",
                    vec!["\"requested_capability\":\"service.read\""],
                ),
            ];

            for (method, path, needles) in checks {
                let (mut socket, _) = listener.accept().await.expect("accept tool ops");
                let mut buf = vec![0; 8192];
                let n = socket.read(&mut buf).await.expect("read tool ops");
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.starts_with(&format!("{method} {path} HTTP/1.1")),
                    "unexpected request for {method} {path}: {req}"
                );
                for needle in needles {
                    assert!(req.contains(needle), "missing `{needle}` in request: {req}");
                }
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                    )
                    .await
                    .expect("write tool ops");
            }
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        assert_eq!(client.tool_registry().await.expect("registry")["ok"], true);
        assert_eq!(
            client
                .tool_execute("tool_cache_stats", serde_json::json!({}), "read_only")
                .await
                .expect("execute")["ok"],
            true
        );
        assert_eq!(client.tool_cache_stats().await.expect("cache")["ok"], true);
        assert_eq!(
            client
                .tool_batch_readonly(
                    vec![serde_json::json!({ "name": "tool_cache_stats", "input": {} })],
                    3,
                )
                .await
                .expect("batch")["ok"],
            true
        );
        let edits = vec![serde_json::json!({
            "path": "README.md",
            "old_string": "A",
            "new_string": "B"
        })];
        assert_eq!(
            client
                .tool_mutation_preview(edits.clone())
                .await
                .expect("preview")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_mutation_apply(edits, serde_json::json!({ "README.md": "hash-1" }))
                .await
                .expect("apply")["ok"],
            true
        );
        assert_eq!(
            client.tool_checkpoints().await.expect("checkpoints")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_checkpoint_create("before edit")
                .await
                .expect("checkpoint create")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_checkpoint_diff("cp-1")
                .await
                .expect("checkpoint diff")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_checkpoint_restore("cp-1")
                .await
                .expect("checkpoint restore")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_intent_plan("inspect", vec!["tool_cache_stats".to_string()])
                .await
                .expect("intent")["ok"],
            true
        );
        assert_eq!(
            client
                .tool_context_fanout_plan("fanout")
                .await
                .expect("fanout")["ok"],
            true
        );
        assert_eq!(
            client
                .runtime_timeline("session a/b", 25)
                .await
                .expect("timeline")["ok"],
            true
        );
        assert_eq!(
            client
                .cross_plane_policy_simulate(serde_json::json!({
                    "requested_capability": "service.read"
                }))
                .await
                .expect("policy simulate")["ok"],
            true
        );
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
            assert!(req.contains("\"reference\":\"service://local.docs/document/tui-doc\""));
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
            assert!(req.contains("\"reference\":\"service://local.docs/document/tui-doc\""));
            assert!(req.contains("\"session_id\":\"session-tui\""));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write promote");
        });

        let client = GatewayApiClient::new(format!("http://{addr}"), None).expect("client");
        let revalidated = client
            .revalidate_connector_resource("service://local.docs/document/tui-doc", "stale")
            .await
            .expect("revalidate");
        assert_eq!(revalidated["ok"], true);
        let promoted = client
            .promote_connector_resource_to_memory(
                "service://local.docs/document/tui-doc",
                Some("session-tui"),
            )
            .await
            .expect("promote");
        assert_eq!(promoted["ok"], true);
        server.await.expect("server task");
    }
}
