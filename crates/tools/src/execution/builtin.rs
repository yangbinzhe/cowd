//! Interactive, MCP, remote, permission, and vision builtin adapters.

use super::*;

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_ask_user_question(input: AskUserQuestionInput) -> Result<String, String> {
    use std::io::{self, BufRead, Write};

    if let Ok(response) = std::env::var("COWD_ASK_USER_RESPONSE") {
        let answer = resolve_ask_user_answer(&response, input.options.as_deref());
        return to_pretty_json(json!({
            "question": input.question,
            "options": input.options,
            "answer": answer,
            "status": "answered",
            "source": "env"
        }));
    }

    if std::env::var_os("COWD_NONINTERACTIVE").is_some() {
        return to_pretty_json(json!({
            "question": input.question,
            "options": input.options,
            "status": "pending_user_input",
            "requires_user_input": true,
            "message": "ask_user_question requires interactive input; set COWD_ASK_USER_RESPONSE to answer non-interactively."
        }));
    }

    // Display the question to the user via stdout
    let stdout = io::stdout();
    let stdin = io::stdin();
    let mut out = stdout.lock();

    writeln!(out, "\n[Question] {}", input.question).map_err(|e| e.to_string())?;

    if let Some(ref options) = input.options {
        for (i, option) in options.iter().enumerate() {
            writeln!(out, "  {}. {}", i + 1, option).map_err(|e| e.to_string())?;
        }
        write!(out, "Enter choice (1-{}): ", options.len()).map_err(|e| e.to_string())?;
    } else {
        write!(out, "Your answer: ").map_err(|e| e.to_string())?;
    }
    out.flush().map_err(|e| e.to_string())?;

    // Read user response from stdin
    let mut response = String::new();
    stdin
        .lock()
        .read_line(&mut response)
        .map_err(|e| e.to_string())?;
    let response = response.trim().to_string();

    // If options were provided, resolve the numeric choice
    let answer = resolve_ask_user_answer(&response, input.options.as_deref());

    to_pretty_json(json!({
        "question": input.question,
        "options": input.options,
        "answer": answer,
        "status": "answered",
        "source": "stdin"
    }))
}

pub(super) fn resolve_ask_user_answer(response: &str, options: Option<&[String]>) -> String {
    let response = response.trim();
    if let Some(options) = options {
        if let Ok(idx) = response.parse::<usize>() {
            if idx >= 1 && idx <= options.len() {
                return options[idx - 1].clone();
            }
        }
    }
    response.to_string()
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_lsp(lease: &ToolHostLease, input: LspInput) -> Result<String, String> {
    let registry = &lease.snapshot().lsp;
    let action = &input.action;
    let path = input.path.as_deref();
    let line = input.line;
    let character = input.character;
    let query = input.query.as_deref();

    match registry.dispatch(action, path, line, character, query) {
        Ok(result) => to_pretty_json(result),
        Err(e) => to_pretty_json(json!({
            "action": action,
            "error": e,
            "status": "error"
        })),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_list_mcp_resources(
    lease: &ToolHostLease,
    input: McpResourceInput,
) -> Result<String, String> {
    let server = input.server.as_deref().unwrap_or("default");
    let Some(service) = lease.snapshot().mcp.as_ref() else {
        return mcp_service_unavailable("list_resources", server);
    };
    match service.list_resources(Some(server)) {
        Ok(resources) => {
            let items: Vec<_> = resources
                .iter()
                .map(|r| {
                    json!({
                        "uri": r.uri,
                        "name": r.name,
                        "mime_type": r.mime_type,
                    })
                })
                .collect();
            to_pretty_json(json!({
                "server": server,
                "resources": items,
                "count": items.len()
            }))
        }
        Err(e) => to_pretty_json(json!({
            "server": server,
            "resources": [],
            "error": e.to_string()
        })),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_read_mcp_resource(
    lease: &ToolHostLease,
    input: McpResourceInput,
) -> Result<String, String> {
    let uri = input.uri.as_deref().unwrap_or("");
    let server = input.server.as_deref().unwrap_or("default");
    let Some(service) = lease.snapshot().mcp.as_ref() else {
        return mcp_service_unavailable("read_resource", server);
    };
    match service.read_resource(server, uri) {
        Ok(resource) => to_pretty_json(json!({
            "server": server,
            "uri": resource.uri,
            "name": resource.name,
            "mime_type": resource.mime_type,
            "content": resource.content
        })),
        Err(e) => to_pretty_json(json!({
            "server": server,
            "uri": uri,
            "error": e.to_string()
        })),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_mcp_auth(lease: &ToolHostLease, input: McpAuthInput) -> Result<String, String> {
    let auth_url_env = format!(
        "COWD_MCP_AUTH_URL_{}",
        input
            .server
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            })
            .collect::<String>()
    );
    let auth_url = std::env::var(auth_url_env).ok();
    let Some(service) = lease.snapshot().mcp.as_ref() else {
        return to_pretty_json(json!({
            "server": input.server,
            "status": "disconnected",
            "auth_required": false,
            "auth_url": auth_url,
            "next_action": "connect_server",
            "message": "MCP service is not configured for this tool runtime."
        }));
    };
    match service.server(&input.server) {
        Ok(state) => {
            let status = state.status;
            to_pretty_json(json!({
            "server": input.server,
            "status": status,
            "auth_required": status == "auth_required",
            "auth_url": auth_url,
            "next_action": if status == "auth_required" { "open_auth_url_or_complete_external_auth" } else { "none" },
            "auth_state": state.auth_state,
            }))
        }
        Err(_) => to_pretty_json(json!({
            "server": input.server,
            "status": "disconnected",
            "auth_required": false,
            "auth_url": auth_url,
            "next_action": "connect_server",
            "message": "Server not registered. Use MCP tool to connect first."
        })),
    }
}

pub(super) fn mcp_service_unavailable(operation: &str, server: &str) -> Result<String, String> {
    to_pretty_json(json!({
        "server": server,
        "operation": operation,
        "status": "service_unavailable",
        "error": "MCP service is not configured for this tool runtime.",
        "next_action": "start_gateway_runtime"
    }))
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_remote_trigger(input: RemoteTriggerInput) -> Result<String, String> {
    let method = input.method.unwrap_or_else(|| "GET".to_string());
    let client = Client::new();

    let mut request = match method.to_uppercase().as_str() {
        "GET" => client.get(&input.url),
        "POST" => client.post(&input.url),
        "PUT" => client.put(&input.url),
        "DELETE" => client.delete(&input.url),
        "PATCH" => client.patch(&input.url),
        "HEAD" => client.head(&input.url),
        other => return Err(format!("unsupported HTTP method: {other}")),
    };

    // Apply custom headers
    if let Some(ref headers) = input.headers {
        if let Some(obj) = headers.as_object() {
            for (key, value) in obj {
                if let Some(val) = value.as_str() {
                    request = request.header(key.as_str(), val);
                }
            }
        }
    }

    // Apply body
    if let Some(ref body) = input.body {
        request = request.body(body.clone());
    }

    // Execute with a 30-second timeout
    let timeout = input.timeout_ms.unwrap_or(30_000).clamp(1, 300_000);
    let request = request.timeout(Duration::from_millis(timeout));

    match request.send() {
        Ok(response) => {
            let status = response.status().as_u16();
            let body = response.text().unwrap_or_default();
            let truncated_body = if body.len() > 8192 {
                format!(
                    "{}\n\n[response truncated — {} bytes total]",
                    &body[..8192],
                    body.len()
                )
            } else {
                body
            };
            to_pretty_json(json!({
                "url": input.url,
                "method": method,
                "status_code": status,
                "body": truncated_body,
                "timeout_ms": timeout,
                "success": (200..300).contains(&status)
            }))
        }
        Err(e) => to_pretty_json(json!({
            "url": input.url,
            "method": method,
            "timeout_ms": timeout,
            "error": e.to_string(),
            "success": false
        })),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_mcp_tool(lease: &ToolHostLease, input: McpToolInput) -> Result<String, String> {
    let args = input.arguments.unwrap_or(serde_json::json!({}));
    let Some(service) = lease.snapshot().mcp.as_ref() else {
        return mcp_service_unavailable("call_tool", &input.server);
    };
    match service.call_tool(mcp::McpToolCallRequest {
        server: input.server.clone(),
        tool: input.tool.clone(),
        input: args,
    }) {
        Ok(receipt) => to_pretty_json(json!({
            "server": input.server,
            "tool": input.tool,
            "result": receipt.output,
            "status": if receipt.ok { "success" } else { "error" }
        })),
        Err(e) => to_pretty_json(json!({
            "server": input.server,
            "tool": input.tool,
            "error": e.to_string(),
            "status": "error"
        })),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn run_testing_permission(input: TestingPermissionInput) -> Result<String, String> {
    to_pretty_json(json!({
        "action": input.action,
        "permitted": true,
        "message": "Testing permission tool stub"
    }))
}

// ── 3B-4: Vision Analyze Tool ──────────────────────────────────────────────────

/// Execute the vision_analyze tool. This tool reads an image file and returns
/// a base64-encoded representation along with metadata, suitable for passing
/// to a multimodal LLM. The actual LLM vision call is handled by the
/// ConversationRuntime, not this tool — this tool prepares the image data.
pub(super) fn run_vision_analyze(lease: &ToolHostLease, input: &Value) -> Result<String, String> {
    let image_path = input
        .get("image_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "vision_analyze requires 'image_path' parameter".to_string())?;

    let prompt = input
        .get("prompt")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "vision_analyze requires 'prompt' parameter".to_string())?;

    let detail = input
        .get("detail")
        .and_then(|v| v.as_str())
        .unwrap_or("auto");

    // Validate the image file exists and is a supported format
    let path = lease
        .path_policy()
        .resolve(image_path)
        .map_err(io_to_string)?;
    if !path.exists() {
        return Err(format!("Image file not found: {}", image_path));
    }

    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    let media_type = match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => {
            return Err(format!(
                "Unsupported image format: '{}'. Supported: PNG, JPG, GIF, WebP",
                extension
            ));
        }
    };

    // Read and base64-encode the image
    let image_data = std::fs::read(&path)
        .map_err(|e| format!("Failed to read image file '{}': {}", image_path, e))?;

    let base64_data = base64::engine::general_purpose::STANDARD.encode(&image_data);

    // Return the prepared vision request payload
    to_pretty_json(json!({
        "tool": "vision_analyze",
        "status": "prepared",
        "image_path": image_path,
        "media_type": media_type,
        "detail": detail,
        "prompt": prompt,
        "image_base64": base64_data,
        "size_bytes": image_data.len(),
        "message": "Image prepared for multimodal LLM analysis. The conversation runtime will include this as a vision content block."
    }))
}
