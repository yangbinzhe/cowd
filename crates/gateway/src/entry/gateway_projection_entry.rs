use crate::SHARED_RT;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GatewayTaskSlashCommand {
    List,
    Start { objective: String, yolo_mode: bool },
    Cancel { id: String },
    Complete { id: String },
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GatewayApprovalSlashCommand {
    List,
    Respond {
        id: String,
        approved: bool,
        persistence: Option<String>,
        reason: Option<String>,
    },
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GatewayContextSlashCommand {
    Current,
    Runtime,
    Config,
    Memory,
    CrossPlane,
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GatewayCrossPlaneSlashCommand {
    Summary,
    Preflight(String),
    Execute(String),
    Help,
}

pub(crate) fn parse_gateway_task_slash_command(
    args: Option<&str>,
) -> Result<GatewayTaskSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "list" | "status") {
        return Ok(GatewayTaskSlashCommand::List);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(GatewayTaskSlashCommand::Help);
    }

    let mut parts = raw.split_whitespace();
    let Some(action) = parts.next() else {
        return Ok(GatewayTaskSlashCommand::List);
    };

    match action {
        "start" => {
            let mut yolo_mode = false;
            let mut objective = Vec::new();
            for part in parts {
                if part == "--yolo" {
                    yolo_mode = true;
                } else {
                    objective.push(part);
                }
            }
            let objective = objective.join(" ").trim().to_string();
            if objective.is_empty() {
                return Err("usage: /tasks start [--yolo] <objective>".to_string());
            }
            Ok(GatewayTaskSlashCommand::Start {
                objective,
                yolo_mode,
            })
        }
        "cancel" => {
            let id = parts.next().unwrap_or_default().trim().to_string();
            if id.is_empty() {
                return Err("usage: /tasks cancel <task-id>".to_string());
            }
            Ok(GatewayTaskSlashCommand::Cancel { id })
        }
        "complete" => {
            let id = parts.next().unwrap_or_default().trim().to_string();
            if id.is_empty() {
                return Err("usage: /tasks complete <task-id>".to_string());
            }
            Ok(GatewayTaskSlashCommand::Complete { id })
        }
        other => Err(format!(
            "unknown /tasks action `{other}`; use /tasks --help"
        )),
    }
}

pub(crate) fn parse_gateway_approval_slash_command(
    args: Option<&str>,
) -> Result<GatewayApprovalSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "list" | "pending" | "status") {
        return Ok(GatewayApprovalSlashCommand::List);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(GatewayApprovalSlashCommand::Help);
    }

    let mut parts = raw.split_whitespace();
    let action = parts.next().unwrap_or_default();
    let approved = match action {
        "approve" | "allow" => true,
        "reject" | "deny" => false,
        other => {
            return Err(format!(
                "unknown /approvals action `{other}`; use /approvals --help"
            ));
        }
    };
    let id = parts.next().unwrap_or_default().trim().to_string();
    if id.is_empty() {
        return Err("usage: /approvals approve|reject <request-id>".to_string());
    }

    let mut persistence = None;
    let mut reason = None;
    let mut rest = parts.peekable();
    while let Some(part) = rest.next() {
        match part {
            "--persist" | "--persistence" => {
                let Some(value) = rest.next() else {
                    return Err("usage: --persist <once|session|forever>".to_string());
                };
                persistence = Some(value.to_string());
            }
            "--reason" => {
                let value = rest.collect::<Vec<_>>().join(" ");
                if !value.trim().is_empty() {
                    reason = Some(value);
                }
                break;
            }
            other => {
                return Err(format!(
                    "unknown /approvals option `{other}`; use /approvals --help"
                ));
            }
        }
    }

    Ok(GatewayApprovalSlashCommand::Respond {
        id,
        approved,
        persistence,
        reason,
    })
}

pub(crate) fn parse_gateway_context_slash_command(
    args: Option<&str>,
) -> Result<GatewayContextSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "current" | "status") {
        return Ok(GatewayContextSlashCommand::Current);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(GatewayContextSlashCommand::Help);
    }
    match raw {
        "runtime" | "control-plane" => Ok(GatewayContextSlashCommand::Runtime),
        "config" | "effective-config" => Ok(GatewayContextSlashCommand::Config),
        "memory" => Ok(GatewayContextSlashCommand::Memory),
        "cross-plane" | "channels" => Ok(GatewayContextSlashCommand::CrossPlane),
        other => Err(format!(
            "unknown /context action `{other}`; use /context --help"
        )),
    }
}

pub(crate) fn parse_gateway_cross_plane_slash_command(
    args: Option<&str>,
) -> Result<GatewayCrossPlaneSlashCommand, String> {
    let raw = args.unwrap_or_default().trim();
    if raw.is_empty() || matches!(raw, "summary" | "status") {
        return Ok(GatewayCrossPlaneSlashCommand::Summary);
    }
    if matches!(raw, "-h" | "--help" | "help") {
        return Ok(GatewayCrossPlaneSlashCommand::Help);
    }

    let Some(split_at) = raw.find(char::is_whitespace) else {
        return Err("usage: /cross-plane preflight|execute <json>".to_string());
    };
    let (action, payload) = raw.split_at(split_at);
    let payload = payload.trim();
    if payload.is_empty() {
        return Err("usage: /cross-plane preflight|execute <json>".to_string());
    }
    match action {
        "preflight" => Ok(GatewayCrossPlaneSlashCommand::Preflight(
            payload.to_string(),
        )),
        "execute" => Ok(GatewayCrossPlaneSlashCommand::Execute(payload.to_string())),
        other => Err(format!(
            "unknown /cross-plane action `{other}`; use /cross-plane --help"
        )),
    }
}

fn gateway_projection_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
}

#[derive(Debug, Clone)]
struct GatewayProjectionClient {
    base_url: String,
    auth_token: Option<String>,
    client: reqwest::Client,
}

impl GatewayProjectionClient {
    fn from_running_gateway(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let base_url = std::env::var("COWD_GATEWAY_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8642".to_string());
        let base_url = normalize_gateway_base_url(base_url)?;
        if !gateway_listener_reachable(&base_url) {
            return Ok(None);
        }
        Ok(Some(Self {
            base_url,
            auth_token,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()?,
        }))
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let request = self.client.get(format!("{}{}", self.base_url, path));
        let request = self.authorize(request);
        let response = request.send().await?;
        Self::parse_response(response).await
    }

    async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let request = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .json(&body);
        let request = self.authorize(request);
        let response = request.send().await?;
        Self::parse_response(response).await
    }

    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.auth_token.as_deref() {
            Some(token) if !token.trim().is_empty() => request.bearer_auth(token.trim()),
            _ => request,
        }
    }

    async fn parse_response(
        response: reqwest::Response,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(format!("Gateway API returned {status}: {text}").into());
        }
        Ok(serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({ "body": text })))
    }

    async fn task_status(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.get_json("/api/tasks").await
    }

    async fn start_task(
        &self,
        objective: &str,
        yolo_mode: bool,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.post_json(
            "/api/tasks/start",
            serde_json::json!({ "objective": objective, "yolo_mode": yolo_mode }),
        )
        .await
    }

    async fn cancel_task(&self, id: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.post_json(
            &format!("/api/tasks/{}/cancel", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    async fn complete_task(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.post_json(
            &format!("/api/tasks/{}/complete", url_encode(id)),
            serde_json::json!({}),
        )
        .await
    }

    async fn pending_approvals(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.get_json("/api/approval/pending").await
    }

    async fn respond_approval(
        &self,
        id: &str,
        approved: bool,
        persistence: Option<&str>,
        reason: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.post_json(
            "/api/approval/respond",
            serde_json::json!({
                "id": id,
                "approved": approved,
                "persistence": persistence,
                "reason": reason,
            }),
        )
        .await
    }

    async fn current_context(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        match session_id {
            Some(id) => {
                self.get_json(&format!(
                    "/api/context/current?session_id={}",
                    url_encode(id)
                ))
                .await
            }
            None => self.get_json("/api/context/current").await,
        }
    }

    async fn runtime_control_plane(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.get_json("/api/runtime/control-plane").await
    }

    async fn runtime_effective_config(
        &self,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.get_json("/api/runtime/config/effective").await
    }

    async fn memory_status(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.get_json("/api/memory/status").await
    }

    async fn cross_plane_summary(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.get_json("/api/cross-plane/summary").await
    }

    async fn preflight_cross_plane_action(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.post_json("/api/cross-plane/action/preflight", request)
            .await
    }

    async fn execute_cross_plane_action(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.post_json("/api/cross-plane/action/execute", request)
            .await
    }
}

fn running_gateway_client() -> Result<GatewayProjectionClient, Box<dyn std::error::Error>> {
    let Some(client) =
        GatewayProjectionClient::from_running_gateway(gateway_projection_auth_token())?
    else {
        return Err("Gateway API is not running; start gateway first".into());
    };
    Ok(client)
}

fn normalize_gateway_base_url(mut base_url: String) -> Result<String, Box<dyn std::error::Error>> {
    base_url = base_url.trim().trim_end_matches('/').to_string();
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return Err(format!(
            "Gateway API base URL must start with http:// or https://: {base_url}"
        )
        .into());
    }
    Ok(base_url)
}

fn gateway_listener_reachable(base_url: &str) -> bool {
    let Some(rest) = base_url
        .strip_prefix("http://")
        .or_else(|| base_url.strip_prefix("https://"))
    else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or_default();
    let mut parts = authority.rsplitn(2, ':');
    let port = parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(80);
    let host = parts.next().unwrap_or(authority);
    std::net::ToSocketAddrs::to_socket_addrs(&(host, port))
        .ok()
        .and_then(|mut addrs| {
            addrs
                .any(|addr| {
                    std::net::TcpStream::connect_timeout(
                        &addr,
                        std::time::Duration::from_millis(100),
                    )
                    .is_ok()
                })
                .then_some(())
        })
        .is_some()
}

fn url_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn print_gateway_task_status(value: &serde_json::Value) {
    println!("## Gateway Tasks");
    let Some(tasks) = value.get("tasks").and_then(serde_json::Value::as_array) else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        );
        return;
    };
    if tasks.is_empty() {
        println!("No active gateway tasks.");
        return;
    }
    for task in tasks {
        let id = task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let status = task
            .get("status")
            .or_else(|| task.get("phase"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let objective = task
            .get("objective")
            .or_else(|| task.get("title"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if objective.is_empty() {
            println!("- {id}: {status}");
        } else {
            println!("- {id}: {status} - {objective}");
        }
    }
}

fn print_gateway_approval_status(value: &serde_json::Value) {
    println!("## Pending Approvals");
    let approvals = value
        .as_array()
        .or_else(|| value.get("approvals").and_then(serde_json::Value::as_array))
        .or_else(|| value.get("pending").and_then(serde_json::Value::as_array));
    let Some(approvals) = approvals else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        );
        return;
    };
    if approvals.is_empty() {
        println!("No pending approvals.");
        return;
    }
    for approval in approvals {
        let id = approval
            .get("id")
            .or_else(|| approval.get("request_id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let capability = approval
            .get("capability")
            .or_else(|| approval.get("operation"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("approval");
        let summary = approval
            .get("summary")
            .or_else(|| approval.get("reason"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if summary.is_empty() {
            println!("- {id}: {capability}");
        } else {
            println!("- {id}: {capability} - {summary}");
        }
    }
}

fn print_gateway_projection_response(title: &str, value: &serde_json::Value) {
    println!("## {title}");
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

pub(crate) fn handle_gateway_tasks_command(
    args: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_gateway_task_slash_command(args)?;
    if command == GatewayTaskSlashCommand::Help {
        println!("## Gateway Tasks");
        println!("/tasks");
        println!("/tasks start [--yolo] <objective>");
        println!("/tasks cancel <task-id>");
        println!("/tasks complete <task-id>");
        return Ok(());
    }

    let client = running_gateway_client()?;
    match command {
        GatewayTaskSlashCommand::List => {
            let value = SHARED_RT.block_on(client.task_status())?;
            print_gateway_task_status(&value);
        }
        GatewayTaskSlashCommand::Start {
            objective,
            yolo_mode,
        } => {
            let value = SHARED_RT.block_on(client.start_task(&objective, yolo_mode))?;
            print_gateway_projection_response("Task Started", &value);
        }
        GatewayTaskSlashCommand::Cancel { id } => {
            let value = SHARED_RT.block_on(client.cancel_task(&id))?;
            print_gateway_projection_response("Task Cancelled", &value);
        }
        GatewayTaskSlashCommand::Complete { id } => {
            let value = SHARED_RT.block_on(client.complete_task(&id))?;
            print_gateway_projection_response("Task Completed", &value);
        }
        GatewayTaskSlashCommand::Help => {}
    }
    Ok(())
}

pub(crate) fn handle_gateway_approvals_command(
    args: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_gateway_approval_slash_command(args)?;
    if command == GatewayApprovalSlashCommand::Help {
        println!("## Gateway Approvals");
        println!("/approvals");
        println!(
            "/approvals approve <request-id> [--persist once|session|forever] [--reason text]"
        );
        println!("/approvals reject <request-id> [--reason text]");
        return Ok(());
    }

    let client = running_gateway_client()?;
    match command {
        GatewayApprovalSlashCommand::List => {
            let value = SHARED_RT.block_on(client.pending_approvals())?;
            print_gateway_approval_status(&value);
        }
        GatewayApprovalSlashCommand::Respond {
            id,
            approved,
            persistence,
            reason,
        } => {
            let value = SHARED_RT.block_on(client.respond_approval(
                &id,
                approved,
                persistence.as_deref(),
                reason.as_deref(),
            ))?;
            print_gateway_projection_response("Approval Responded", &value);
        }
        GatewayApprovalSlashCommand::Help => {}
    }
    Ok(())
}

pub(crate) fn handle_gateway_context_command(
    args: Option<&str>,
    session_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_gateway_context_slash_command(args)?;
    if command == GatewayContextSlashCommand::Help {
        println!("## Gateway Context");
        println!("/context");
        println!("/context runtime");
        println!("/context config");
        println!("/context memory");
        println!("/context cross-plane");
        return Ok(());
    }

    let client = running_gateway_client()?;
    let (title, value) = match command {
        GatewayContextSlashCommand::Current => (
            "Current Context",
            SHARED_RT.block_on(client.current_context(Some(session_id)))?,
        ),
        GatewayContextSlashCommand::Runtime => (
            "Runtime Control Plane",
            SHARED_RT.block_on(client.runtime_control_plane())?,
        ),
        GatewayContextSlashCommand::Config => (
            "Runtime Effective Config",
            SHARED_RT.block_on(client.runtime_effective_config())?,
        ),
        GatewayContextSlashCommand::Memory => {
            ("Memory Status", SHARED_RT.block_on(client.memory_status())?)
        }
        GatewayContextSlashCommand::CrossPlane => (
            "Cross-Plane Summary",
            SHARED_RT.block_on(client.cross_plane_summary())?,
        ),
        GatewayContextSlashCommand::Help => unreachable!("help returned above"),
    };
    print_gateway_projection_response(title, &value);
    Ok(())
}

pub(crate) fn handle_gateway_cross_plane_command(
    args: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_gateway_cross_plane_slash_command(args)?;
    if command == GatewayCrossPlaneSlashCommand::Help {
        println!("## Cross-Plane");
        println!("/cross-plane");
        println!("/cross-plane preflight <json>");
        println!("/cross-plane execute <json>");
        return Ok(());
    }

    let client = running_gateway_client()?;
    match command {
        GatewayCrossPlaneSlashCommand::Summary => {
            let value = SHARED_RT.block_on(client.cross_plane_summary())?;
            print_gateway_projection_response("Cross-Plane Summary", &value);
        }
        GatewayCrossPlaneSlashCommand::Preflight(payload) => {
            let request: serde_json::Value = serde_json::from_str(&payload)?;
            let value = SHARED_RT.block_on(client.preflight_cross_plane_action(request))?;
            print_gateway_projection_response("Cross-Plane Preflight", &value);
        }
        GatewayCrossPlaneSlashCommand::Execute(payload) => {
            let request: serde_json::Value = serde_json::from_str(&payload)?;
            let value = SHARED_RT.block_on(client.execute_cross_plane_action(request))?;
            print_gateway_projection_response("Cross-Plane Execute", &value);
        }
        GatewayCrossPlaneSlashCommand::Help => {}
    }
    Ok(())
}
