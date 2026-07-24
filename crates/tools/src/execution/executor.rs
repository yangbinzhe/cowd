use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use base64::Engine;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::bash::{BashCommandInput, BashCommandOutput};
use crate::checkpoint::{
    checkpoint_create_in, checkpoint_diff_in, checkpoint_list_in, checkpoint_restore_in,
    CheckpointCreateInput, CheckpointDiffInput, CheckpointRestoreInput,
};
use crate::file_ops::{
    edit_file, glob_search, grep_search, read_file, write_file, GrepSearchInput,
};
use crate::gates::{GateContext, GateEvaluator};
use crate::lane_events::{LaneEvent, LaneEventName, LaneEventStatus, LaneFailureClass};
use crate::lane_policy::{iso8601_now, LaneContext};
use crate::mutation_plan::{
    apply_mutations, preview_mutations, MutationApplyInput, MutationPreviewInput,
};
use crate::path_policy::WorkspacePathPolicy;
use crate::permissions::{EnforcementResult, PermissionEnforcer, PermissionMode};
use crate::prepared::{
    prepare_readonly_invocations, PreparedReadonlyLeaf, PreparedReadonlyLeafInvocation,
    PreparedToolCall, ToolExecutionContext,
};
use crate::stale_branch::{check_freshness, BranchFreshness};
use crate::ToolHostLease;

/// Check permission before executing a tool. Returns Err with denial reason if blocked.
pub fn enforce_permission_check(
    enforcer: &PermissionEnforcer,
    tool_name: &str,
    input: &Value,
) -> Result<(), String> {
    let input_str = serde_json::to_string(input).unwrap_or_default();
    let result = enforcer.check(tool_name, &input_str);

    match result {
        EnforcementResult::Allowed => Ok(()),
        EnforcementResult::Denied { reason, .. } => Err(reason),
    }
}

#[cfg(test)]
pub fn execute_tool_for_test(name: &str, input: &Value) -> Result<String, String> {
    TEST_TOOL_HOST.with(|host| execute_with_lease(&host.pin_snapshot(), name, input))
}

#[cfg(test)]
thread_local! {
    static TEST_TOOL_HOST: crate::ToolHost = crate::ToolHost::builtin(
        "tools-test",
        std::env::current_dir().unwrap_or_default(),
    );
}

pub(crate) fn execute_with_lease(
    lease: &ToolHostLease,
    name: &str,
    input: &Value,
) -> Result<String, String> {
    execute_tool_with_enforcer(lease, None, None, name, input)
}

/// Execute a tool with optional permission enforcer and gate evaluator checks.
///
/// The gate evaluator is checked FIRST — before the permission enforcer —
/// so that gate failures short-circuit before any enforcement logic runs.
pub(crate) fn execute_tool_with_enforcer(
    lease: &ToolHostLease,
    enforcer: Option<&PermissionEnforcer>,
    gate_evaluator: Option<&GateEvaluator>,
    name: &str,
    input: &Value,
) -> Result<String, String> {
    // Gate evaluator check — runs before permission enforcer to short-circuit
    // on gate failures (e.g., diff size, sensitive data patterns).
    if let Some(ge) = gate_evaluator {
        let input_str = serde_json::to_string(input).unwrap_or_default();
        let context = GateContext {
            repo_path: std::env::current_dir()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            branch: String::new(),
            commit_message: name.to_string(),
            changed_files: Vec::new(),
            diff: input_str,
            author: String::new(),
            author_email: String::new(),
            violations: Vec::new(),
        };
        let (all_passed, results) = ge.evaluate_all(&context);
        if !all_passed {
            let reasons: Vec<String> = results
                .iter()
                .filter(|r| !r.passed)
                .map(|r| {
                    let mut msg = format!("[{}] {}", r.gate_name, r.message);
                    if !r.suggestions.is_empty() {
                        msg.push_str(&format!(" Suggestions: {}", r.suggestions.join(", ")));
                    }
                    msg
                })
                .collect();
            return Err(format!(
                "Gate check failed for tool `{name}`: {}",
                reasons.join("; ")
            ));
        }
    }

    match name {
        "bash" => {
            // Parse input to get the command for permission classification
            let bash_input: BashCommandInput = from_value(input)?;
            let classified_mode = classify_bash_permission(&bash_input.command);
            maybe_enforce_permission_check_with_mode(enforcer, name, input, classified_mode)?;
            run_bash(lease, bash_input)
        }
        "read_file" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<ReadFileInput>(input).and_then(|parsed| run_read_file(lease, parsed))
        }
        "read_many" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<ReadManyInput>(input)
                .and_then(|parsed| run_read_many(lease, enforcer, parsed))
        }
        "write_file" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<WriteFileInput>(input).and_then(|parsed| run_write_file(lease, parsed))
        }
        "edit_file" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<EditFileInput>(input).and_then(|parsed| run_edit_file(lease, parsed))
        }
        "mutation_preview" | "edit_many_preview" | "patch_plan" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<MutationPreviewInput>(input)
                .and_then(|parsed| run_mutation_preview(lease, parsed))
        }
        "apply_patch_transaction" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<MutationApplyInput>(input)
                .and_then(|parsed| run_apply_patch_transaction(lease, parsed))
        }
        "checkpoint_create" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<CheckpointCreateInput>(input)
                .and_then(|parsed| run_checkpoint_create(lease, parsed))
        }
        "checkpoint_list" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            run_checkpoint_list(lease)
        }
        "checkpoint_diff" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<CheckpointDiffInput>(input)
                .and_then(|parsed| run_checkpoint_diff(lease, parsed))
        }
        "checkpoint_restore" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<CheckpointRestoreInput>(input)
                .and_then(|parsed| run_checkpoint_restore(lease, parsed))
        }
        "glob_search" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<GlobSearchInputValue>(input)
                .and_then(|parsed| run_glob_search(lease, parsed))
        }
        "glob_many" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<GlobManyInput>(input)
                .and_then(|parsed| run_glob_many(lease, enforcer, parsed))
        }
        "grep_search" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<GrepSearchInput>(input).and_then(|parsed| run_grep_search(lease, parsed))
        }
        "grep_many" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<GrepManyInput>(input)
                .and_then(|parsed| run_grep_many(lease, enforcer, parsed))
        }
        "workspace_snapshot" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<WorkspaceSnapshotInput>(input)
                .and_then(|parsed| run_workspace_snapshot(lease, parsed))
        }
        "tool_batch_readonly" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            from_value::<ToolBatchReadonlyInput>(input)
                .and_then(|parsed| run_tool_batch_readonly(lease, enforcer, gate_evaluator, parsed))
        }
        "tool_cache_stats" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            to_pretty_json(lease.cache().stats())
        }
        "WebFetch" => from_value::<WebFetchInput>(input).and_then(run_web_fetch),
        "WebSearch" => from_value::<WebSearchInput>(input).and_then(run_web_search),
        "TodoWrite" => from_value::<TodoWriteInput>(input).and_then(run_todo_write),
        "Question" => {
            let q = input.get("question").and_then(|v| v.as_str()).unwrap_or("");
            let opts = input.get("options").and_then(|v| v.as_array()).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            Ok(format!(
                "[QUESTION] {q}{}",
                opts.map(|o| format!("\nOptions: {o}")).unwrap_or_default()
            ))
        }
        "ast_search" => {
            let pattern = input.get("pattern").and_then(|v| v.as_str()).unwrap_or("");
            let lang = input
                .get("language")
                .and_then(|v| v.as_str())
                .unwrap_or("rust");
            Ok(format!(
                "AST search: {pattern} in {lang}\nUse ast_grep_search tool for structured code patterns."
            ))
        }
        "ToolSearch" => {
            from_value::<ToolSearchInput>(input).and_then(|parsed| run_tool_search(lease, parsed))
        }
        "NotebookEdit" => from_value::<NotebookEditInput>(input)
            .and_then(|parsed| run_notebook_edit(lease, parsed)),
        "Sleep" => from_value::<SleepInput>(input).and_then(run_sleep),
        "SendUserMessage" | "Brief" => {
            from_value::<BriefInput>(input).and_then(|parsed| run_brief(lease, parsed))
        }
        "Config" => from_value::<ConfigInput>(input).and_then(run_config),
        "EnterPlanMode" => from_value::<EnterPlanModeInput>(input).and_then(run_enter_plan_mode),
        "ExitPlanMode" => from_value::<ExitPlanModeInput>(input).and_then(run_exit_plan_mode),
        "StructuredOutput" => {
            from_value::<StructuredOutputInput>(input).and_then(run_structured_output)
        }
        "REPL" => from_value::<ReplInput>(input).and_then(run_repl),
        "PowerShell" => {
            // Parse input to get the command for permission classification
            let ps_input: PowerShellInput = from_value(input)?;
            let classified_mode = classify_powershell_permission(&ps_input.command);
            maybe_enforce_permission_check_with_mode(enforcer, name, input, classified_mode)?;
            run_powershell(ps_input)
        }
        "AskUserQuestion" => {
            from_value::<AskUserQuestionInput>(input).and_then(run_ask_user_question)
        }
        "LSP" => from_value::<LspInput>(input).and_then(|parsed| run_lsp(lease, parsed)),
        "ListMcpResources" => from_value::<McpResourceInput>(input)
            .and_then(|parsed| run_list_mcp_resources(lease, parsed)),
        "ReadMcpResource" => from_value::<McpResourceInput>(input)
            .and_then(|parsed| run_read_mcp_resource(lease, parsed)),
        "McpAuth" => {
            from_value::<McpAuthInput>(input).and_then(|parsed| run_mcp_auth(lease, parsed))
        }
        "RemoteTrigger" => from_value::<RemoteTriggerInput>(input).and_then(run_remote_trigger),
        "MCP" => from_value::<McpToolInput>(input).and_then(|parsed| run_mcp_tool(lease, parsed)),
        "TestingPermission" => {
            from_value::<TestingPermissionInput>(input).and_then(run_testing_permission)
        }
        "vision_analyze" => {
            maybe_enforce_permission_check(enforcer, name, input)?;
            run_vision_analyze(lease, input)
        }
        "execute_code" => {
            use crate::sandbox_exec::execute_code;
            let lang = input
                .get("language")
                .and_then(|v| v.as_str())
                .unwrap_or("python");
            let code = input.get("code").and_then(|v| v.as_str()).unwrap_or("");
            let result = execute_code(lang, code);
            Ok(serde_json::to_string_pretty(&serde_json::json!({
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
            }))
            .unwrap_or_default())
        }
        _ => Err(format!("unsupported tool: {name}")),
    }
}

fn maybe_enforce_permission_check(
    enforcer: Option<&PermissionEnforcer>,
    tool_name: &str,
    input: &Value,
) -> Result<(), String> {
    if let Some(enforcer) = enforcer {
        enforce_permission_check(enforcer, tool_name, input)?;
    }
    Ok(())
}

/// Enforce permission check with a dynamically classified permission mode.
/// Used for tools like bash and `PowerShell` where the required permission
/// depends on the actual command being executed.
fn maybe_enforce_permission_check_with_mode(
    enforcer: Option<&PermissionEnforcer>,
    tool_name: &str,
    input: &Value,
    required_mode: PermissionMode,
) -> Result<(), String> {
    if let Some(enforcer) = enforcer {
        let input_str = serde_json::to_string(input).unwrap_or_default();
        let result = enforcer.check_with_required_mode(tool_name, &input_str, required_mode);

        match result {
            EnforcementResult::Allowed => Ok(()),
            EnforcementResult::Denied { reason, .. } => Err(reason),
        }
    } else {
        Ok(())
    }
}

#[allow(clippy::needless_pass_by_value)]
fn run_ask_user_question(input: AskUserQuestionInput) -> Result<String, String> {
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
            "message": "AskUserQuestion requires interactive input; set COWD_ASK_USER_RESPONSE to answer non-interactively."
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

fn resolve_ask_user_answer(response: &str, options: Option<&[String]>) -> String {
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
fn run_lsp(lease: &ToolHostLease, input: LspInput) -> Result<String, String> {
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
fn run_list_mcp_resources(
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
fn run_read_mcp_resource(lease: &ToolHostLease, input: McpResourceInput) -> Result<String, String> {
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
fn run_mcp_auth(lease: &ToolHostLease, input: McpAuthInput) -> Result<String, String> {
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

fn mcp_service_unavailable(operation: &str, server: &str) -> Result<String, String> {
    to_pretty_json(json!({
        "server": server,
        "operation": operation,
        "status": "service_unavailable",
        "error": "MCP service is not configured for this tool runtime.",
        "next_action": "start_gateway_runtime"
    }))
}

#[allow(clippy::needless_pass_by_value)]
fn run_remote_trigger(input: RemoteTriggerInput) -> Result<String, String> {
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
fn run_mcp_tool(lease: &ToolHostLease, input: McpToolInput) -> Result<String, String> {
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
fn run_testing_permission(input: TestingPermissionInput) -> Result<String, String> {
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
fn run_vision_analyze(lease: &ToolHostLease, input: &Value) -> Result<String, String> {
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

fn from_value<T: for<'de> Deserialize<'de>>(input: &Value) -> Result<T, String> {
    serde_json::from_value(input.clone()).map_err(|error| error.to_string())
}

/// Classify bash command permission based on command type and path.
/// ROADMAP #50: Read-only commands targeting CWD paths get `WorkspaceWrite`,
/// all others remain `DangerFullAccess`.
fn classify_bash_permission(command: &str) -> PermissionMode {
    // Read-only commands that are safe when targeting workspace paths
    const READ_ONLY_COMMANDS: &[&str] = &[
        "cat", "head", "tail", "less", "more", "ls", "ll", "dir", "find", "test", "[", "[[",
        "grep", "rg", "awk", "sed", "file", "stat", "readlink", "wc", "sort", "uniq", "cut", "tr",
        "pwd", "echo", "printf",
    ];

    // Get the base command (first word before any args or pipes)
    let base_cmd = command.split_whitespace().next().unwrap_or("");
    let base_cmd = base_cmd.split('|').next().unwrap_or("").trim();
    let base_cmd = base_cmd.split(';').next().unwrap_or("").trim();
    let base_cmd = base_cmd.split('>').next().unwrap_or("").trim();
    let base_cmd = base_cmd.split('<').next().unwrap_or("").trim();

    // Check if it's a read-only command
    let cmd_name = base_cmd.split('/').next_back().unwrap_or(base_cmd);
    let is_read_only = READ_ONLY_COMMANDS.contains(&cmd_name);

    if !is_read_only {
        return PermissionMode::DangerFullAccess;
    }

    // Check if any path argument is outside workspace
    // Simple heuristic: check for absolute paths not starting with CWD
    if has_dangerous_paths(command) {
        return PermissionMode::DangerFullAccess;
    }

    PermissionMode::WorkspaceWrite
}

/// Check if command has dangerous paths (outside workspace).
fn has_dangerous_paths(command: &str) -> bool {
    // Look for absolute paths
    let tokens: Vec<&str> = command.split_whitespace().collect();

    for token in tokens {
        // Skip flags/options
        if token.starts_with('-') {
            continue;
        }

        // Check for absolute paths
        if token.starts_with('/') || token.starts_with("~/") {
            // Check if it's within CWD
            let path =
                PathBuf::from(token.replace('~', &std::env::var("HOME").unwrap_or_default()));
            if let Ok(cwd) = std::env::current_dir() {
                if !path.starts_with(&cwd) {
                    return true; // Path outside workspace
                }
            }
        }

        // Check for parent directory traversal that escapes workspace
        if token.contains("../..") || token.starts_with("../") && !token.starts_with("./") {
            return true;
        }
    }

    false
}

fn run_bash(lease: &ToolHostLease, input: BashCommandInput) -> Result<String, String> {
    if let Some(output) = workspace_test_branch_preflight(&input.command, None) {
        return serde_json::to_string_pretty(&output).map_err(|error| error.to_string());
    }
    serde_json::to_string_pretty(
        &crate::bash::execute_bash_in_workspace(input, lease.workspace_root())
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn workspace_test_branch_preflight(
    command: &str,
    mut lane_ctx: Option<&mut LaneContext>,
) -> Option<BashCommandOutput> {
    if !is_workspace_test_command(command) {
        return None;
    }

    let branch = git_stdout(&["branch", "--show-current"])?;
    let main_ref = resolve_main_ref(&branch)?;
    let freshness = check_freshness(&branch, &main_ref);
    // Also populate lane context for policy evaluation
    if let Some(ref mut ctx) = lane_ctx {
        ctx.stale_branch = Some(freshness.clone());
        ctx.branch_freshness = match &freshness {
            BranchFreshness::Stale { .. } | BranchFreshness::Diverged { .. } => {
                Duration::from_secs(999999)
            }
            BranchFreshness::Fresh => Duration::from_secs(0),
        };
    }
    match freshness {
        BranchFreshness::Fresh => None,
        BranchFreshness::Stale {
            commits_behind,
            missing_fixes,
        } => Some(branch_divergence_output(
            command,
            &branch,
            &main_ref,
            commits_behind,
            None,
            &missing_fixes,
        )),
        BranchFreshness::Diverged {
            ahead,
            behind,
            missing_fixes,
        } => Some(branch_divergence_output(
            command,
            &branch,
            &main_ref,
            behind,
            Some(ahead),
            &missing_fixes,
        )),
    }
}

fn is_workspace_test_command(command: &str) -> bool {
    let normalized = normalize_shell_command(command);
    [
        "cargo test --workspace",
        "cargo test --all",
        "cargo nextest run --workspace",
        "cargo nextest run --all",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn normalize_shell_command(command: &str) -> String {
    command
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn resolve_main_ref(branch: &str) -> Option<String> {
    let has_local_main = git_ref_exists("main");
    let has_remote_main = git_ref_exists("origin/main");

    if branch == "main" && has_remote_main {
        Some("origin/main".to_string())
    } else if has_local_main {
        Some("main".to_string())
    } else if has_remote_main {
        Some("origin/main".to_string())
    } else {
        None
    }
}

fn git_ref_exists(reference: &str) -> bool {
    Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", reference])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn git_stdout(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!stdout.is_empty()).then_some(stdout)
}

fn branch_divergence_output(
    command: &str,
    branch: &str,
    main_ref: &str,
    commits_behind: usize,
    commits_ahead: Option<usize>,
    missing_fixes: &[String],
) -> BashCommandOutput {
    let relation = commits_ahead.map_or_else(
        || format!("is {commits_behind} commit(s) behind"),
        |ahead| format!("has diverged ({ahead} ahead, {commits_behind} behind)"),
    );
    let missing_summary = if missing_fixes.is_empty() {
        "(none surfaced)".to_string()
    } else {
        missing_fixes.join("; ")
    };
    let stderr = format!(
        "branch divergence detected before workspace tests: `{branch}` {relation} `{main_ref}`. Missing commits: {missing_summary}. Merge or rebase `{main_ref}` before re-running `{command}`."
    );

    let lane_event = LaneEvent::new(
        LaneEventName::BranchStaleAgainstMain,
        LaneEventStatus::Blocked,
        iso8601_now(),
    )
    .with_failure_class(LaneFailureClass::BranchDivergence)
    .with_detail(stderr.clone())
    .with_data(json!({
        "branch": branch,
        "mainRef": main_ref,
        "commitsBehind": commits_behind,
        "commitsAhead": commits_ahead,
        "missingCommits": missing_fixes,
        "blockedCommand": command,
        "recommendedAction": format!("merge or rebase {main_ref} before workspace tests")
    }));

    BashCommandOutput {
        stdout: String::new(),
        stderr: stderr.clone(),
        raw_output_path: None,
        interrupted: false,
        is_image: None,
        background_task_id: None,
        backgrounded_by_user: None,
        assistant_auto_backgrounded: None,
        dangerously_disable_sandbox: None,
        return_code_interpretation: Some("preflight_blocked:branch_divergence".to_string()),
        no_output_expected: Some(false),
        structured_content: serde_json::to_value(lane_event)
            .ok()
            .map(|event| vec![event]),
        persisted_output_path: None,
        persisted_output_size: None,
        sandbox_status: None,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn run_read_file(lease: &ToolHostLease, input: ReadFileInput) -> Result<String, String> {
    let resolved = lease
        .path_policy()
        .resolve(&input.path)
        .map_err(io_to_string)?;
    let fingerprint = file_fingerprint(&resolved);
    let scope = file_cache_scope(&resolved);
    cached_json_tool(lease, "read_file", &input, &fingerprint, &scope, || {
        read_file(lease.path_policy(), &input.path, input.offset, input.limit).map_err(io_to_string)
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_read_many(
    lease: &ToolHostLease,
    enforcer: Option<&PermissionEnforcer>,
    input: ReadManyInput,
) -> Result<String, String> {
    let results = run_ordered_batch(input.files, input.max_concurrency, |item| {
        let value = serde_json::to_value(&item).map_err(|error| error.to_string())?;
        maybe_enforce_permission_check(enforcer, "read_file", &value)?;
        let output = run_read_file(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("read_many", results))
}

#[allow(clippy::needless_pass_by_value)]
fn run_write_file(lease: &ToolHostLease, input: WriteFileInput) -> Result<String, String> {
    let scope = file_cache_scope(
        &lease
            .path_policy()
            .resolve(&input.path)
            .map_err(io_to_string)?,
    );
    create_auto_checkpoint(lease, "write_file")?;
    let output = to_pretty_json(
        write_file(lease.path_policy(), &input.path, &input.content).map_err(io_to_string)?,
    );
    if output.is_ok() {
        lease.cache().invalidate_scope(&scope);
    }
    output
}

#[allow(clippy::needless_pass_by_value)]
fn run_edit_file(lease: &ToolHostLease, input: EditFileInput) -> Result<String, String> {
    let scope = file_cache_scope(
        &lease
            .path_policy()
            .resolve(&input.path)
            .map_err(io_to_string)?,
    );
    create_auto_checkpoint(lease, "edit_file")?;
    let output = to_pretty_json(
        edit_file(
            lease.path_policy(),
            &input.path,
            &input.old_string,
            &input.new_string,
            input.replace_all.unwrap_or(false),
        )
        .map_err(io_to_string)?,
    );
    if output.is_ok() {
        lease.cache().invalidate_scope(&scope);
    }
    output
}

#[allow(clippy::needless_pass_by_value)]
fn run_mutation_preview(
    lease: &ToolHostLease,
    input: MutationPreviewInput,
) -> Result<String, String> {
    to_pretty_json(preview_mutations(lease.path_policy(), input).map_err(io_to_string)?)
}

#[allow(clippy::needless_pass_by_value)]
fn run_apply_patch_transaction(
    lease: &ToolHostLease,
    input: MutationApplyInput,
) -> Result<String, String> {
    create_auto_checkpoint(lease, "apply_patch_transaction")?;
    let applied = apply_mutations(lease.path_policy(), input).map_err(io_to_string)?;
    for file in &applied.applied {
        lease.cache().invalidate_scope(&file_cache_scope(
            &lease
                .path_policy()
                .resolve(&file.path)
                .map_err(io_to_string)?,
        ));
    }
    to_pretty_json(applied)
}

#[allow(clippy::needless_pass_by_value)]
fn run_checkpoint_create(
    lease: &ToolHostLease,
    input: CheckpointCreateInput,
) -> Result<String, String> {
    to_pretty_json(checkpoint_create_in(lease.workspace_root(), input).map_err(io_to_string)?)
}

fn create_auto_checkpoint(lease: &ToolHostLease, tool_name: &str) -> Result<(), String> {
    if std::env::var("COWD_AUTO_CHECKPOINT").ok().as_deref() != Some("1") {
        return Ok(());
    }
    checkpoint_create_in(
        lease.workspace_root(),
        CheckpointCreateInput {
            label: Some(format!("auto-before-{tool_name}")),
            paths: Vec::new(),
        },
    )
    .map(|_| ())
    .map_err(io_to_string)
}

fn run_checkpoint_list(lease: &ToolHostLease) -> Result<String, String> {
    to_pretty_json(checkpoint_list_in(lease.workspace_root()).map_err(io_to_string)?)
}

#[allow(clippy::needless_pass_by_value)]
fn run_checkpoint_diff(
    lease: &ToolHostLease,
    input: CheckpointDiffInput,
) -> Result<String, String> {
    to_pretty_json(checkpoint_diff_in(lease.workspace_root(), input).map_err(io_to_string)?)
}

#[allow(clippy::needless_pass_by_value)]
fn run_checkpoint_restore(
    lease: &ToolHostLease,
    input: CheckpointRestoreInput,
) -> Result<String, String> {
    let output =
        to_pretty_json(checkpoint_restore_in(lease.workspace_root(), input).map_err(io_to_string)?);
    if output.is_ok() {
        lease.cache().invalidate_all();
    }
    output
}

#[allow(clippy::needless_pass_by_value)]
fn run_glob_search(lease: &ToolHostLease, input: GlobSearchInputValue) -> Result<String, String> {
    let fingerprint = scope_fingerprint(lease.path_policy(), input.path.as_deref())?;
    let scope = directory_cache_scope(lease.path_policy(), input.path.as_deref())?;
    cached_json_tool(lease, "glob_search", &input, &fingerprint, &scope, || {
        glob_search(lease.path_policy(), &input.pattern, input.path.as_deref())
            .map_err(io_to_string)
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_glob_many(
    lease: &ToolHostLease,
    enforcer: Option<&PermissionEnforcer>,
    input: GlobManyInput,
) -> Result<String, String> {
    let results = run_ordered_batch(input.patterns, input.max_concurrency, |item| {
        let value = serde_json::to_value(&item).map_err(|error| error.to_string())?;
        maybe_enforce_permission_check(enforcer, "glob_search", &value)?;
        let output = run_glob_search(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("glob_many", results))
}

#[allow(clippy::needless_pass_by_value)]
fn run_grep_search(lease: &ToolHostLease, input: GrepSearchInput) -> Result<String, String> {
    let fingerprint = scope_fingerprint(lease.path_policy(), input.path.as_deref())?;
    let scope = directory_cache_scope(lease.path_policy(), input.path.as_deref())?;
    cached_json_tool(lease, "grep_search", &input, &fingerprint, &scope, || {
        grep_search(lease.path_policy(), &input).map_err(io_to_string)
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_grep_many(
    lease: &ToolHostLease,
    enforcer: Option<&PermissionEnforcer>,
    input: GrepManyInput,
) -> Result<String, String> {
    let results = run_ordered_batch(input.searches, input.max_concurrency, |item| {
        let value = serde_json::to_value(&item).map_err(|error| error.to_string())?;
        maybe_enforce_permission_check(enforcer, "grep_search", &value)?;
        let output = run_grep_search(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("grep_many", results))
}

#[allow(clippy::needless_pass_by_value)]
fn run_workspace_snapshot(
    lease: &ToolHostLease,
    input: WorkspaceSnapshotInput,
) -> Result<String, String> {
    let snapshot_input = input.clone();
    let fingerprint = workspace_snapshot_fingerprint(lease.path_policy(), &input)?;
    cached_json_tool(
        lease,
        "workspace_snapshot",
        &input,
        &fingerprint,
        "workspace:.",
        || workspace_snapshot_value(lease, snapshot_input),
    )
}

fn workspace_snapshot_value(
    lease: &ToolHostLease,
    input: WorkspaceSnapshotInput,
) -> Result<Value, String> {
    let include_git = input.include_git.unwrap_or(true);
    let include_files = input.include_files.unwrap_or(true);
    let max_files = input.max_files.unwrap_or(500).clamp(1, 5000);
    let cwd = lease.path_policy().workspace_root().to_path_buf();

    let git = if include_git {
        Some(json!({
            "branch": git_stdout(&["rev-parse", "--abbrev-ref", "HEAD"]),
            "status": git_stdout(&["status", "--short", "--branch"]),
            "head": git_stdout(&["rev-parse", "--short", "HEAD"])
        }))
    } else {
        None
    };

    let files = if include_files {
        let roots = input.roots.unwrap_or_else(|| vec![String::from(".")]);
        let mut files = Vec::new();
        for root in roots {
            if files.len() >= max_files {
                break;
            }
            let root_path = lease.path_policy().resolve(&root).map_err(io_to_string)?;
            collect_snapshot_files(lease.path_policy(), &root_path, max_files, &mut files);
        }
        files.sort();
        files.dedup();
        files.truncate(max_files);
        Some(files)
    } else {
        None
    };

    Ok(json!({
        "type": "workspace_snapshot",
        "cwd": cwd.to_string_lossy(),
        "git": git,
        "files": files,
        "maxFiles": max_files
    }))
}

fn cached_json_tool<T, F, O>(
    lease: &ToolHostLease,
    tool_name: &str,
    input: &T,
    fingerprint: &str,
    scope: &str,
    operation: F,
) -> Result<String, String>
where
    T: Serialize,
    F: FnOnce() -> Result<O, String>,
    O: Serialize,
{
    const UNCACHEABLE_FINGERPRINT: &str = "uncacheable";
    let input_json = serde_json::to_string(input).map_err(|error| error.to_string())?;
    if fingerprint == UNCACHEABLE_FINGERPRINT {
        return to_pretty_json(operation()?);
    }
    let cache_input = format!("{input_json}::fingerprint::{fingerprint}");
    if let Some(cached) = lease.cache().get(
        lease.workspace_id(),
        scope,
        tool_name,
        &cache_input,
        lease.schema_revision(),
    ) {
        return Ok(cached);
    }
    let output = to_pretty_json(operation()?)?;
    lease.cache().put(
        lease.workspace_id(),
        scope,
        tool_name,
        &cache_input,
        lease.schema_revision(),
        &output,
    );
    Ok(output)
}

fn file_cache_scope(path: &Path) -> String {
    format!("file:{}", path.to_string_lossy())
}

fn directory_cache_scope(
    policy: &WorkspacePathPolicy,
    path: Option<&str>,
) -> Result<String, String> {
    match path {
        Some(path) => Ok(format!(
            "directory:{}",
            policy
                .resolve(path)
                .map_err(io_to_string)?
                .to_string_lossy()
        )),
        None => Ok("workspace:.".to_string()),
    }
}

fn file_fingerprint(resolved: &Path) -> String {
    let content_hash = match std::fs::File::open(resolved) {
        Ok(mut file) => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            let mut buffer = [0_u8; 8192];
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => break format!("{:016x}", hasher.finish()),
                    Ok(read) => buffer[..read].hash(&mut hasher),
                    Err(_) => break String::from("unreadable"),
                }
            }
        }
        Err(_) => String::from("missing"),
    };
    let Ok(metadata) = std::fs::metadata(resolved) else {
        return format!("missing:{}", resolved.to_string_lossy());
    };
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!(
        "file:{}:{}:{}",
        resolved.to_string_lossy(),
        metadata.len(),
        modified
    ) + &format!(":{content_hash}")
}

fn scope_fingerprint(policy: &WorkspacePathPolicy, path: Option<&str>) -> Result<String, String> {
    let root = match path {
        Some(path) => policy.resolve(path).map_err(io_to_string)?,
        None => policy.workspace_root().to_path_buf(),
    };
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root.to_string_lossy().hash(&mut hasher);
    let complete = hash_path_scope(&root, &mut hasher, &mut 0usize);
    if !complete {
        return Ok(String::from("uncacheable"));
    }
    Ok(format!("scope:{:016x}", hasher.finish()))
}

fn workspace_snapshot_fingerprint(
    policy: &WorkspacePathPolicy,
    input: &WorkspaceSnapshotInput,
) -> Result<String, String> {
    let mut parts = Vec::new();
    if input.include_git.unwrap_or(true) {
        parts.push(git_stdout(&["rev-parse", "HEAD"]).unwrap_or_default());
        parts.push(git_stdout(&["status", "--short"]).unwrap_or_default());
    }
    if input.include_files.unwrap_or(true) {
        let roots = input
            .roots
            .clone()
            .unwrap_or_else(|| vec![String::from(".")]);
        for root in roots {
            parts.push(scope_fingerprint(policy, Some(&root))?);
        }
    }
    Ok(parts.join("\n"))
}

fn hash_path_scope(
    root: &Path,
    hasher: &mut std::collections::hash_map::DefaultHasher,
    seen: &mut usize,
) -> bool {
    const MAX_FINGERPRINT_FILES: usize = 2048;
    if *seen >= MAX_FINGERPRINT_FILES || should_skip_cache_fingerprint(root) {
        return *seen < MAX_FINGERPRINT_FILES;
    }
    let Ok(metadata) = std::fs::metadata(root) else {
        "missing".hash(hasher);
        root.to_string_lossy().hash(hasher);
        return true;
    };
    root.to_string_lossy().hash(hasher);
    metadata.len().hash(hasher);
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
        .hash(hasher);
    if metadata.is_file() {
        if let Ok(mut file) = std::fs::File::open(root) {
            let mut buffer = [0_u8; 8192];
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => buffer[..read].hash(hasher),
                    Err(_) => {
                        "unreadable".hash(hasher);
                        break;
                    }
                }
            }
        }
        *seen += 1;
        return *seen <= MAX_FINGERPRINT_FILES;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return true;
    };
    let mut paths = entries
        .flatten()
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        if !hash_path_scope(&path, hasher, seen) {
            return false;
        }
        if *seen >= MAX_FINGERPRINT_FILES {
            return false;
        }
    }
    true
}

fn should_skip_cache_fingerprint(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git" | ".cowd" | "target" | "node_modules" | "dist" | "build" | ".cache"
            )
        })
}

#[allow(clippy::needless_pass_by_value)]
fn run_tool_batch_readonly(
    lease: &ToolHostLease,
    enforcer: Option<&PermissionEnforcer>,
    gate_evaluator: Option<&GateEvaluator>,
    input: ToolBatchReadonlyInput,
) -> Result<String, String> {
    for call in &input.calls {
        if !is_allowed_readonly_batch_tool(&call.name) {
            return Err(format!(
                "tool_batch_readonly only accepts approved read-only tools; `{}` is not allowed",
                call.name
            ));
        }
    }

    if gate_evaluator.is_none() {
        let prepared_calls = input
            .calls
            .iter()
            .map(|call| PreparedToolCall {
                name: call.name.clone(),
                input: call.input.clone(),
            })
            .collect::<Vec<_>>();
        let context = ToolExecutionContext::from_current_dir("tool_batch_readonly")?;
        if calls_support_prepared_readonly(&input.calls) {
            let prepared = prepare_readonly_invocations(&context, &prepared_calls)
                .map_err(|error| error.message)?;
            let results = run_ordered_batch(prepared, input.max_concurrency, |prepared| {
                maybe_enforce_permission_check(
                    enforcer,
                    &prepared.normalized_name,
                    prepared_leaf_input(&prepared),
                )?;
                execute_prepared_readonly_leaf(lease, prepared)
            });
            return to_pretty_json(batch_output_with_mode(
                "tool_batch_readonly",
                "prepared_readonly",
                results,
            ));
        }
    }

    let results = run_ordered_batch(input.calls, input.max_concurrency, |call| {
        let output =
            execute_tool_with_enforcer(lease, enforcer, gate_evaluator, &call.name, &call.input)?;
        Ok(serde_json::from_str(&output).unwrap_or(Value::String(output)))
    });
    to_pretty_json(batch_output_with_mode(
        "tool_batch_readonly",
        "compat_recursive",
        results,
    ))
}

fn calls_support_prepared_readonly(calls: &[ToolBatchReadonlyCallInput]) -> bool {
    calls
        .iter()
        .all(|call| is_prepared_readonly_tool(&call.name))
}

fn prepared_leaf_input(prepared: &PreparedReadonlyLeafInvocation) -> &Value {
    match &prepared.leaf {
        PreparedReadonlyLeaf::ReadFile(input)
        | PreparedReadonlyLeaf::GlobSearch(input)
        | PreparedReadonlyLeaf::GrepSearch(input)
        | PreparedReadonlyLeaf::WorkspaceSnapshot(input) => input,
        PreparedReadonlyLeaf::ToolCacheStats => &Value::Null,
    }
}

fn execute_prepared_readonly_leaf(
    lease: &ToolHostLease,
    prepared: PreparedReadonlyLeafInvocation,
) -> Result<Value, String> {
    let output = match prepared.leaf {
        PreparedReadonlyLeaf::ReadFile(input) => {
            from_value::<ReadFileInput>(&input).and_then(|parsed| run_read_file(lease, parsed))?
        }
        PreparedReadonlyLeaf::GlobSearch(input) => from_value::<GlobSearchInputValue>(&input)
            .and_then(|parsed| run_glob_search(lease, parsed))?,
        PreparedReadonlyLeaf::GrepSearch(input) => from_value::<GrepSearchInput>(&input)
            .and_then(|parsed| run_grep_search(lease, parsed))?,
        PreparedReadonlyLeaf::WorkspaceSnapshot(input) => {
            from_value::<WorkspaceSnapshotInput>(&input)
                .and_then(|parsed| run_workspace_snapshot(lease, parsed))?
        }
        PreparedReadonlyLeaf::ToolCacheStats => to_pretty_json(lease.cache().stats())?,
    };
    Ok(serde_json::from_str(&output).unwrap_or(Value::String(output)))
}

fn is_prepared_readonly_tool(name: &str) -> bool {
    matches!(
        name,
        "read_file" | "glob_search" | "grep_search" | "workspace_snapshot" | "tool_cache_stats"
    )
}

fn is_allowed_readonly_batch_tool(name: &str) -> bool {
    matches!(
        name,
        "read_file"
            | "read_many"
            | "glob_search"
            | "glob_many"
            | "grep_search"
            | "grep_many"
            | "workspace_snapshot"
            | "tool_cache_stats"
            | "mutation_preview"
            | "edit_many_preview"
            | "patch_plan"
    )
}

fn collect_snapshot_files(
    policy: &WorkspacePathPolicy,
    root: &Path,
    max_files: usize,
    files: &mut Vec<String>,
) {
    if files.len() >= max_files {
        return;
    }
    let Ok(resolved_root) = policy.ensure_resolved_path(root) else {
        return;
    };
    let Ok(metadata) = std::fs::metadata(&resolved_root) else {
        return;
    };
    if metadata.is_file() {
        files.push(resolved_root.to_string_lossy().into_owned());
        return;
    }
    if !metadata.is_dir() || should_skip_snapshot_dir(&resolved_root) {
        return;
    }
    let Ok(entries) = std::fs::read_dir(&resolved_root) else {
        return;
    };
    for entry in entries.flatten() {
        if files.len() >= max_files {
            break;
        }
        collect_snapshot_files(policy, &entry.path(), max_files, files);
    }
}

fn should_skip_snapshot_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git"
                    | ".cowd"
                    | "target"
                    | "node_modules"
                    | "dist"
                    | "build"
                    | ".cache"
                    | "coverage"
            )
        })
}

#[derive(Debug, Serialize)]
struct BatchToolOutput {
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "executionMode")]
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_mode: Option<String>,
    count: usize,
    #[serde(rename = "successCount")]
    success_count: usize,
    #[serde(rename = "errorCount")]
    error_count: usize,
    #[serde(rename = "partialSuccess")]
    partial_success: bool,
    results: Vec<BatchToolItemOutput>,
}

#[derive(Debug, Serialize)]
struct BatchToolItemOutput {
    index: usize,
    status: String,
    #[serde(rename = "durationMs")]
    duration_ms: u128,
    output: Option<Value>,
    error: Option<String>,
}

fn batch_output(kind: &str, results: Vec<BatchToolItemOutput>) -> BatchToolOutput {
    batch_output_internal(kind, None, results)
}

fn batch_output_with_mode(
    kind: &str,
    execution_mode: &str,
    results: Vec<BatchToolItemOutput>,
) -> BatchToolOutput {
    batch_output_internal(kind, Some(execution_mode.to_string()), results)
}

fn batch_output_internal(
    kind: &str,
    execution_mode: Option<String>,
    results: Vec<BatchToolItemOutput>,
) -> BatchToolOutput {
    let success_count = results
        .iter()
        .filter(|item| item.status == "success")
        .count();
    let error_count = results.len().saturating_sub(success_count);
    BatchToolOutput {
        kind: kind.to_string(),
        execution_mode,
        count: results.len(),
        success_count,
        error_count,
        partial_success: success_count > 0 && error_count > 0,
        results,
    }
}

fn run_ordered_batch<T, F>(
    items: Vec<T>,
    max_concurrency: Option<usize>,
    operation: F,
) -> Vec<BatchToolItemOutput>
where
    T: Clone + Send,
    F: Fn(T) -> Result<Value, String> + Sync,
{
    let concurrency = max_concurrency.unwrap_or(8).clamp(1, 32);
    let mut results: Vec<Option<BatchToolItemOutput>> =
        std::iter::repeat_with(|| None).take(items.len()).collect();

    for chunk_start in (0..items.len()).step_by(concurrency) {
        let chunk_end = chunk_start.saturating_add(concurrency).min(items.len());
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (offset, item) in items[chunk_start..chunk_end].iter().cloned().enumerate() {
                let operation = &operation;
                handles.push(scope.spawn(move || {
                    let started = Instant::now();
                    let result = operation(item);
                    let duration_ms = started.elapsed().as_millis();
                    (offset, duration_ms, result)
                }));
            }
            for handle in handles {
                match handle.join() {
                    Ok((offset, duration_ms, Ok(output))) => {
                        results[chunk_start + offset] = Some(BatchToolItemOutput {
                            index: chunk_start + offset,
                            status: String::from("success"),
                            duration_ms,
                            output: Some(output),
                            error: None,
                        });
                    }
                    Ok((offset, duration_ms, Err(error))) => {
                        results[chunk_start + offset] = Some(BatchToolItemOutput {
                            index: chunk_start + offset,
                            status: String::from("error"),
                            duration_ms,
                            output: None,
                            error: Some(error),
                        });
                    }
                    Err(_) => {
                        let offset = results[chunk_start..chunk_end]
                            .iter()
                            .position(Option::is_none)
                            .unwrap_or(0);
                        results[chunk_start + offset] = Some(BatchToolItemOutput {
                            index: chunk_start + offset,
                            status: String::from("error"),
                            duration_ms: 0,
                            output: None,
                            error: Some(String::from("batch item panicked")),
                        });
                    }
                }
            }
        });
    }

    results
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            item.unwrap_or(BatchToolItemOutput {
                index,
                status: String::from("error"),
                duration_ms: 0,
                output: None,
                error: Some(String::from("batch item did not complete")),
            })
        })
        .collect()
}

#[allow(clippy::needless_pass_by_value)]
fn run_web_fetch(input: WebFetchInput) -> Result<String, String> {
    to_pretty_json(execute_web_fetch(&input)?)
}

#[allow(clippy::needless_pass_by_value)]
fn run_web_search(input: WebSearchInput) -> Result<String, String> {
    to_pretty_json(execute_web_search(&input)?)
}

fn run_todo_write(input: TodoWriteInput) -> Result<String, String> {
    to_pretty_json(execute_todo_write(input)?)
}

fn run_tool_search(lease: &ToolHostLease, input: ToolSearchInput) -> Result<String, String> {
    to_pretty_json(execute_tool_search(lease, input))
}

fn run_notebook_edit(lease: &ToolHostLease, input: NotebookEditInput) -> Result<String, String> {
    to_pretty_json(execute_notebook_edit(lease.path_policy(), input)?)
}

fn run_sleep(input: SleepInput) -> Result<String, String> {
    to_pretty_json(execute_sleep(input)?)
}

fn run_brief(lease: &ToolHostLease, input: BriefInput) -> Result<String, String> {
    to_pretty_json(execute_brief(lease.path_policy(), input)?)
}

fn run_config(input: ConfigInput) -> Result<String, String> {
    to_pretty_json(execute_config(input)?)
}

fn run_enter_plan_mode(input: EnterPlanModeInput) -> Result<String, String> {
    to_pretty_json(execute_enter_plan_mode(input)?)
}

fn run_exit_plan_mode(input: ExitPlanModeInput) -> Result<String, String> {
    to_pretty_json(execute_exit_plan_mode(input)?)
}

fn run_structured_output(input: StructuredOutputInput) -> Result<String, String> {
    to_pretty_json(execute_structured_output(input)?)
}

fn run_repl(input: ReplInput) -> Result<String, String> {
    to_pretty_json(execute_repl(input)?)
}

/// Classify `PowerShell` command permission based on command type and path.
/// ROADMAP #50: Read-only commands targeting CWD paths get `WorkspaceWrite`,
/// all others remain `DangerFullAccess`.
fn classify_powershell_permission(command: &str) -> PermissionMode {
    // Read-only commands that are safe when targeting workspace paths
    const READ_ONLY_COMMANDS: &[&str] = &[
        "Get-Content",
        "Get-ChildItem",
        "Test-Path",
        "Get-Item",
        "Get-ItemProperty",
        "Get-FileHash",
        "Select-String",
    ];

    // Check if command starts with a read-only cmdlet
    let cmd_lower = command.trim().to_lowercase();
    let is_read_only_cmd = READ_ONLY_COMMANDS
        .iter()
        .any(|cmd| cmd_lower.starts_with(&cmd.to_lowercase()));

    if !is_read_only_cmd {
        return PermissionMode::DangerFullAccess;
    }

    // Check if the path is within workspace (CWD or subdirectory)
    // Extract path from command - look for -Path or positional parameter
    let path = extract_powershell_path(command);
    match path {
        Some(p) if is_within_workspace(&p) => PermissionMode::WorkspaceWrite,
        _ => PermissionMode::DangerFullAccess,
    }
}

/// Extract the path argument from a `PowerShell` command.
fn extract_powershell_path(command: &str) -> Option<String> {
    // Look for -Path parameter
    if let Some(idx) = command.to_lowercase().find("-path") {
        let after_path = &command[idx + 5..];
        let path = after_path.split_whitespace().next()?;
        return Some(path.trim_matches('"').trim_matches('\'').to_string());
    }

    // Look for positional path parameter (after command name)
    let parts: Vec<&str> = command.split_whitespace().collect();
    if parts.len() >= 2 {
        // Skip the cmdlet name and take the first argument
        let first_arg = parts[1];
        // Check if it looks like a path (contains \, /, or .)
        if first_arg.contains(['\\', '/', '.']) {
            return Some(first_arg.trim_matches('"').trim_matches('\'').to_string());
        }
    }

    None
}

/// Resolve a path to its canonical form, falling back through multiple strategies.
///
/// 1. Try `canonicalize()` for paths that exist on disk.
/// 2. Fall back to canonicalizing the parent and joining the filename.
/// 3. Last resort: lexically resolve `..` and `.` components.
fn resolve_canonical(path: &str) -> Option<PathBuf> {
    let p = Path::new(path);

    // Strategy 1: full canonicalize
    if let Ok(canonical) = p.canonicalize() {
        return Some(canonical);
    }

    // Strategy 2: canonicalize parent + join filename
    if let Some(parent) = p.parent() {
        if let Ok(canonical_parent) = parent.canonicalize() {
            if let Some(name) = p.file_name() {
                return Some(canonical_parent.join(name));
            }
        }
    }

    // Strategy 3: lexical resolution of .. and .
    let mut resolved = PathBuf::new();
    for component in p.components() {
        match component {
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            other => {
                resolved.push(other.as_os_str());
            }
        }
    }

    Some(resolved)
}

/// Check if a path is within the current workspace using canonical path comparison.
/// This prevents path-traversal attacks like `../../etc/passwd`.
fn is_within_workspace(path: &str) -> bool {
    let resolved_path = resolve_canonical(path);
    let Ok(cwd) = std::env::current_dir() else {
        return false;
    };
    let resolved_cwd = resolve_canonical(cwd.to_str().unwrap_or(""));

    match (resolved_path, resolved_cwd) {
        (Some(path), Some(cwd)) => path.starts_with(&cwd),
        _ => false,
    }
}

fn run_powershell(input: PowerShellInput) -> Result<String, String> {
    to_pretty_json(execute_powershell(input).map_err(|error| error.to_string())?)
}

fn to_pretty_json<T: serde::Serialize>(value: T) -> Result<String, String> {
    serde_json::to_string_pretty(&value).map_err(|error| error.to_string())
}

#[allow(clippy::needless_pass_by_value)]
fn io_to_string(error: std::io::Error) -> String {
    error.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadFileInput {
    path: String,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ReadManyInput {
    files: Vec<ReadFileInput>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct WriteFileInput {
    path: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct EditFileInput {
    path: String,
    old_string: String,
    new_string: String,
    replace_all: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobSearchInputValue {
    pattern: String,
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GlobManyInput {
    patterns: Vec<GlobSearchInputValue>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct GrepManyInput {
    searches: Vec<GrepSearchInput>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceSnapshotInput {
    include_git: Option<bool>,
    include_files: Option<bool>,
    roots: Option<Vec<String>>,
    max_files: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolBatchReadonlyInput {
    calls: Vec<ToolBatchReadonlyCallInput>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolBatchReadonlyCallInput {
    name: String,
    input: Value,
}

#[derive(Debug, Deserialize)]
struct WebFetchInput {
    url: String,
    prompt: String,
}

#[derive(Debug, Deserialize)]
struct WebSearchInput {
    query: String,
    allowed_domains: Option<Vec<String>>,
    blocked_domains: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct TodoWriteInput {
    todos: Vec<TodoItem>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
struct TodoItem {
    content: String,
    #[serde(rename = "activeForm")]
    active_form: String,
    status: TodoStatus,
    #[serde(default)]
    priority: TodoPriority,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum TodoPriority {
    #[default]
    Medium,
    Low,
    High,
    Critical,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TodoStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Deserialize)]
struct ToolSearchInput {
    query: String,
    max_results: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct NotebookEditInput {
    notebook_path: String,
    cell_id: Option<String>,
    new_source: Option<String>,
    cell_type: Option<NotebookCellType>,
    edit_mode: Option<NotebookEditMode>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum NotebookCellType {
    Code,
    Markdown,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum NotebookEditMode {
    Replace,
    Insert,
    Delete,
}

#[derive(Debug, Deserialize)]
struct SleepInput {
    duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct BriefInput {
    message: String,
    attachments: Option<Vec<String>>,
    status: BriefStatus,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BriefStatus {
    Normal,
    Proactive,
}

#[derive(Debug, Deserialize)]
struct ConfigInput {
    setting: String,
    value: Option<ConfigValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EnterPlanModeInput {}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ExitPlanModeInput {}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigValue {
    String(String),
    Bool(bool),
    Number(f64),
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct StructuredOutputInput(BTreeMap<String, Value>);

#[derive(Debug, Deserialize)]
struct ReplInput {
    code: String,
    language: String,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PowerShellInput {
    command: String,
    timeout: Option<u64>,
    description: Option<String>,
    run_in_background: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AskUserQuestionInput {
    question: String,
    #[serde(default)]
    options: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct LspInput {
    action: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    line: Option<u32>,
    #[serde(default)]
    character: Option<u32>,
    #[serde(default)]
    query: Option<String>,
}

#[derive(Debug, Deserialize)]
struct McpResourceInput {
    #[serde(default)]
    server: Option<String>,
    #[serde(default)]
    uri: Option<String>,
}

#[derive(Debug, Deserialize)]
struct McpAuthInput {
    server: String,
}

#[derive(Debug, Deserialize)]
struct RemoteTriggerInput {
    url: String,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    headers: Option<Value>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct McpToolInput {
    server: String,
    tool: String,
    #[serde(default)]
    arguments: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct TestingPermissionInput {
    action: String,
}

#[derive(Debug, Serialize)]
struct WebFetchOutput {
    bytes: usize,
    code: u16,
    #[serde(rename = "codeText")]
    code_text: String,
    result: String,
    #[serde(rename = "durationMs")]
    duration_ms: u128,
    url: String,
}

#[derive(Debug, Serialize)]
struct WebSearchOutput {
    query: String,
    results: Vec<WebSearchResultItem>,
    #[serde(rename = "durationSeconds")]
    duration_seconds: f64,
}

#[derive(Debug, Serialize)]
struct TodoWriteOutput {
    #[serde(rename = "oldTodos")]
    old_todos: Vec<TodoItem>,
    #[serde(rename = "newTodos")]
    new_todos: Vec<TodoItem>,
    #[serde(rename = "verificationNudgeNeeded")]
    verification_nudge_needed: Option<bool>,
}

#[derive(Debug, Serialize)]
struct NotebookEditOutput {
    new_source: String,
    cell_id: Option<String>,
    cell_type: Option<NotebookCellType>,
    language: String,
    edit_mode: String,
    error: Option<String>,
    notebook_path: String,
    original_file: String,
    updated_file: String,
}

#[derive(Debug, Serialize)]
struct SleepOutput {
    duration_ms: u64,
    message: String,
}

#[derive(Debug, Serialize)]
struct BriefOutput {
    message: String,
    attachments: Option<Vec<ResolvedAttachment>>,
    #[serde(rename = "sentAt")]
    sent_at: String,
}

#[derive(Debug, Serialize)]
struct ResolvedAttachment {
    path: String,
    size: u64,
    #[serde(rename = "isImage")]
    is_image: bool,
}

#[derive(Debug, Serialize)]
struct ConfigOutput {
    success: bool,
    operation: Option<String>,
    setting: Option<String>,
    value: Option<Value>,
    #[serde(rename = "previousValue")]
    previous_value: Option<Value>,
    #[serde(rename = "newValue")]
    new_value: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanModeState {
    #[serde(rename = "hadLocalOverride")]
    had_local_override: bool,
    #[serde(rename = "previousLocalMode")]
    previous_local_mode: Option<Value>,
}

#[derive(Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
struct PlanModeOutput {
    success: bool,
    operation: String,
    changed: bool,
    active: bool,
    managed: bool,
    message: String,
    #[serde(rename = "settingsPath")]
    settings_path: String,
    #[serde(rename = "statePath")]
    state_path: String,
    #[serde(rename = "previousLocalMode")]
    previous_local_mode: Option<Value>,
    #[serde(rename = "currentLocalMode")]
    current_local_mode: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchableToolSpec {
    pub(crate) name: String,
    pub(crate) description: String,
}

#[derive(Debug, Serialize)]
struct StructuredOutputResult {
    data: String,
    structured_output: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize)]
struct ReplOutput {
    language: String,
    stdout: String,
    stderr: String,
    #[serde(rename = "exitCode")]
    exit_code: i32,
    #[serde(rename = "durationMs")]
    duration_ms: u128,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum WebSearchResultItem {
    SearchResult {
        tool_use_id: String,
        content: Vec<SearchHit>,
    },
    Commentary(String),
}

#[derive(Debug, Serialize)]
struct SearchHit {
    title: String,
    url: String,
}

fn execute_web_fetch(input: &WebFetchInput) -> Result<WebFetchOutput, String> {
    let started = Instant::now();
    let client = build_http_client()?;
    let request_url = normalize_fetch_url(&input.url)?;
    let response = client
        .get(request_url.clone())
        .send()
        .map_err(|error| error.to_string())?;

    let status = response.status();
    let final_url = response.url().to_string();
    let code = status.as_u16();
    let code_text = status.canonical_reason().unwrap_or("Unknown").to_string();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response.text().map_err(|error| error.to_string())?;
    let bytes = body.len();
    let normalized = normalize_fetched_content(&body, &content_type);
    let result = summarize_web_fetch(&final_url, &input.prompt, &normalized, &body, &content_type);

    Ok(WebFetchOutput {
        bytes,
        code,
        code_text,
        result,
        duration_ms: started.elapsed().as_millis(),
        url: final_url,
    })
}

fn execute_web_search(input: &WebSearchInput) -> Result<WebSearchOutput, String> {
    let started = Instant::now();
    let client = build_http_client()?;
    let search_url = build_search_url(&input.query)?;
    let response = client
        .get(search_url)
        .send()
        .map_err(|error| error.to_string())?;

    let final_url = response.url().clone();
    let html = response.text().map_err(|error| error.to_string())?;
    let mut hits = extract_search_hits(&html);

    if hits.is_empty() && final_url.host_str().is_some() {
        hits = extract_search_hits_from_generic_links(&html);
    }

    if let Some(allowed) = input.allowed_domains.as_ref() {
        hits.retain(|hit| host_matches_list(&hit.url, allowed));
    }
    if let Some(blocked) = input.blocked_domains.as_ref() {
        hits.retain(|hit| !host_matches_list(&hit.url, blocked));
    }

    dedupe_hits(&mut hits);
    hits.truncate(8);

    let summary = if hits.is_empty() {
        format!("No web search results matched the query {:?}.", input.query)
    } else {
        let rendered_hits = hits
            .iter()
            .map(|hit| format!("- [{}]({})", hit.title, hit.url))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "Search results for {:?}. Include a Sources section in the final answer.\n{}",
            input.query, rendered_hits
        )
    };

    Ok(WebSearchOutput {
        query: input.query.clone(),
        results: vec![
            WebSearchResultItem::Commentary(summary),
            WebSearchResultItem::SearchResult {
                tool_use_id: String::from("web_search_1"),
                content: hits,
            },
        ],
        duration_seconds: started.elapsed().as_secs_f64(),
    })
}

fn build_http_client() -> Result<Client, String> {
    Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::limited(10))
        .user_agent("cowd-rust-tools/0.1")
        .build()
        .map_err(|error| error.to_string())
}

fn normalize_fetch_url(url: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(url).map_err(|error| error.to_string())?;
    if parsed.scheme() == "http" {
        let host = parsed.host_str().unwrap_or_default();
        if host != "localhost" && host != "127.0.0.1" && host != "::1" {
            let mut upgraded = parsed;
            upgraded
                .set_scheme("https")
                .map_err(|()| String::from("failed to upgrade URL to https"))?;
            return Ok(upgraded.to_string());
        }
    }
    Ok(parsed.to_string())
}

fn build_search_url(query: &str) -> Result<reqwest::Url, String> {
    if let Ok(base) = std::env::var("COWD_WEB_SEARCH_BASE_URL") {
        let mut url = reqwest::Url::parse(&base).map_err(|error| error.to_string())?;
        url.query_pairs_mut().append_pair("q", query);
        return Ok(url);
    }

    let mut url = reqwest::Url::parse("https://html.duckduckgo.com/html/")
        .map_err(|error| error.to_string())?;
    url.query_pairs_mut().append_pair("q", query);
    Ok(url)
}

fn normalize_fetched_content(body: &str, content_type: &str) -> String {
    if content_type.contains("html") {
        html_to_text(body)
    } else {
        body.trim().to_string()
    }
}

fn summarize_web_fetch(
    url: &str,
    prompt: &str,
    content: &str,
    raw_body: &str,
    content_type: &str,
) -> String {
    let lower_prompt = prompt.to_lowercase();
    let compact = collapse_whitespace(content);

    let detail = if lower_prompt.contains("title") {
        extract_title(content, raw_body, content_type).map_or_else(
            || preview_text(&compact, 600),
            |title| format!("Title: {title}"),
        )
    } else if lower_prompt.contains("summary") || lower_prompt.contains("summarize") {
        preview_text(&compact, 900)
    } else {
        let preview = preview_text(&compact, 900);
        format!("Prompt: {prompt}\nContent preview:\n{preview}")
    };

    format!("Fetched {url}\n{detail}")
}

fn extract_title(content: &str, raw_body: &str, content_type: &str) -> Option<String> {
    if content_type.contains("html") {
        let lowered = raw_body.to_lowercase();
        if let Some(start) = lowered.find("<title>") {
            let after = start + "<title>".len();
            if let Some(end_rel) = lowered[after..].find("</title>") {
                let title =
                    collapse_whitespace(&decode_html_entities(&raw_body[after..after + end_rel]));
                if !title.is_empty() {
                    return Some(title);
                }
            }
        }
    }

    for line in content.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn html_to_text(html: &str) -> String {
    let mut text = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut previous_was_space = false;

    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if in_tag => {}
            '&' => {
                text.push('&');
                previous_was_space = false;
            }
            ch if ch.is_whitespace() => {
                if !previous_was_space {
                    text.push(' ');
                    previous_was_space = true;
                }
            }
            _ => {
                text.push(ch);
                previous_was_space = false;
            }
        }
    }

    collapse_whitespace(&decode_html_entities(&text))
}

fn decode_html_entities(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

fn collapse_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn preview_text(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let shortened = input.chars().take(max_chars).collect::<String>();
    format!("{}…", shortened.trim_end())
}

fn extract_search_hits(html: &str) -> Vec<SearchHit> {
    let mut hits = Vec::new();
    let mut remaining = html;

    while let Some(anchor_start) = remaining.find("result__a") {
        let after_class = &remaining[anchor_start..];
        let Some(href_idx) = after_class.find("href=") else {
            remaining = &after_class[1..];
            continue;
        };
        let href_slice = &after_class[href_idx + 5..];
        let Some((url, rest)) = extract_quoted_value(href_slice) else {
            remaining = &after_class[1..];
            continue;
        };
        let Some(close_tag_idx) = rest.find('>') else {
            remaining = &after_class[1..];
            continue;
        };
        let after_tag = &rest[close_tag_idx + 1..];
        let Some(end_anchor_idx) = after_tag.find("</a>") else {
            remaining = &after_tag[1..];
            continue;
        };
        let title = html_to_text(&after_tag[..end_anchor_idx]);
        if let Some(decoded_url) = decode_duckduckgo_redirect(&url) {
            hits.push(SearchHit {
                title: title.trim().to_string(),
                url: decoded_url,
            });
        }
        remaining = &after_tag[end_anchor_idx + 4..];
    }

    hits
}

fn extract_search_hits_from_generic_links(html: &str) -> Vec<SearchHit> {
    let mut hits = Vec::new();
    let mut remaining = html;

    while let Some(anchor_start) = remaining.find("<a") {
        let after_anchor = &remaining[anchor_start..];
        let Some(href_idx) = after_anchor.find("href=") else {
            remaining = &after_anchor[2..];
            continue;
        };
        let href_slice = &after_anchor[href_idx + 5..];
        let Some((url, rest)) = extract_quoted_value(href_slice) else {
            remaining = &after_anchor[2..];
            continue;
        };
        let Some(close_tag_idx) = rest.find('>') else {
            remaining = &after_anchor[2..];
            continue;
        };
        let after_tag = &rest[close_tag_idx + 1..];
        let Some(end_anchor_idx) = after_tag.find("</a>") else {
            remaining = &after_anchor[2..];
            continue;
        };
        let title = html_to_text(&after_tag[..end_anchor_idx]);
        if title.trim().is_empty() {
            remaining = &after_tag[end_anchor_idx + 4..];
            continue;
        }
        let decoded_url = decode_duckduckgo_redirect(&url).unwrap_or(url);
        if decoded_url.starts_with("http://") || decoded_url.starts_with("https://") {
            hits.push(SearchHit {
                title: title.trim().to_string(),
                url: decoded_url,
            });
        }
        remaining = &after_tag[end_anchor_idx + 4..];
    }

    hits
}

fn extract_quoted_value(input: &str) -> Option<(String, &str)> {
    let quote = input.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let rest = &input[quote.len_utf8()..];
    let end = rest.find(quote)?;
    Some((rest[..end].to_string(), &rest[end + quote.len_utf8()..]))
}

fn decode_duckduckgo_redirect(url: &str) -> Option<String> {
    if url.starts_with("http://") || url.starts_with("https://") {
        return Some(html_entity_decode_url(url));
    }

    let joined = if url.starts_with("//") {
        format!("https:{url}")
    } else if url.starts_with('/') {
        format!("https://duckduckgo.com{url}")
    } else {
        return None;
    };

    let parsed = reqwest::Url::parse(&joined).ok()?;
    if parsed.path() == "/l/" || parsed.path() == "/l" {
        for (key, value) in parsed.query_pairs() {
            if key == "uddg" {
                return Some(html_entity_decode_url(value.as_ref()));
            }
        }
    }
    Some(joined)
}

fn html_entity_decode_url(url: &str) -> String {
    decode_html_entities(url)
}

fn host_matches_list(url: &str, domains: &[String]) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    domains.iter().any(|domain| {
        let normalized = normalize_domain_filter(domain);
        !normalized.is_empty() && (host == normalized || host.ends_with(&format!(".{normalized}")))
    })
}

fn normalize_domain_filter(domain: &str) -> String {
    let trimmed = domain.trim();
    let candidate = reqwest::Url::parse(trimmed)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| trimmed.to_string());
    candidate
        .trim()
        .trim_start_matches('.')
        .trim_end_matches('/')
        .to_ascii_lowercase()
}

fn dedupe_hits(hits: &mut Vec<SearchHit>) {
    let mut seen = BTreeSet::new();
    hits.retain(|hit| seen.insert(hit.url.clone()));
}

fn execute_todo_write(input: TodoWriteInput) -> Result<TodoWriteOutput, String> {
    validate_todos(&input.todos)?;
    let store_path = todo_store_path()?;
    let old_todos = if store_path.exists() {
        serde_json::from_str::<Vec<TodoItem>>(
            &std::fs::read_to_string(&store_path).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?
    } else {
        Vec::new()
    };

    let all_done = input
        .todos
        .iter()
        .all(|todo| matches!(todo.status, TodoStatus::Completed));
    let persisted = if all_done {
        Vec::new()
    } else {
        input.todos.clone()
    };

    if let Some(parent) = store_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(
        &store_path,
        serde_json::to_string_pretty(&persisted).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    let verification_nudge_needed = (all_done
        && input.todos.len() >= 3
        && !input
            .todos
            .iter()
            .any(|todo| todo.content.to_lowercase().contains("verif")))
    .then_some(true);

    Ok(TodoWriteOutput {
        old_todos,
        new_todos: input.todos,
        verification_nudge_needed,
    })
}

fn validate_todos(todos: &[TodoItem]) -> Result<(), String> {
    if todos.is_empty() {
        return Err(String::from("todos must not be empty"));
    }
    // Allow multiple in_progress items for parallel workflows
    if todos.iter().any(|todo| todo.content.trim().is_empty()) {
        return Err(String::from("todo content must not be empty"));
    }
    if todos.iter().any(|todo| todo.active_form.trim().is_empty()) {
        return Err(String::from("todo activeForm must not be empty"));
    }
    Ok(())
}

fn todo_store_path() -> Result<std::path::PathBuf, String> {
    if let Ok(path) = std::env::var("COWD_TODO_STORE") {
        return Ok(std::path::PathBuf::from(path));
    }
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    Ok(cwd.join(".cowd-todos.json"))
}

#[allow(clippy::needless_pass_by_value)]
fn execute_tool_search(
    lease: &ToolHostLease,
    input: ToolSearchInput,
) -> harness_contract::tool::ToolDiscoveryReceipt {
    lease.search(&input.query, input.max_results.unwrap_or(5))
}

pub(crate) fn search_tool_specs(
    query: &str,
    max_results: usize,
    specs: &[SearchableToolSpec],
) -> Vec<String> {
    let lowered = query.to_lowercase();
    if let Some(selection) = lowered.strip_prefix("select:") {
        return selection
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .filter_map(|wanted| {
                let wanted = canonical_tool_token(wanted);
                specs
                    .iter()
                    .find(|spec| canonical_tool_token(&spec.name) == wanted)
                    .map(|spec| spec.name.clone())
            })
            .take(max_results)
            .collect();
    }

    let mut required = Vec::new();
    let mut optional = Vec::new();
    for term in lowered.split_whitespace() {
        if let Some(rest) = term.strip_prefix('+') {
            if !rest.is_empty() {
                required.push(rest);
            }
        } else {
            optional.push(term);
        }
    }
    let terms = if required.is_empty() {
        optional.clone()
    } else {
        required.iter().chain(optional.iter()).copied().collect()
    };

    let mut scored = specs
        .iter()
        .filter_map(|spec| {
            let name = spec.name.to_lowercase();
            let canonical_name = canonical_tool_token(&spec.name);
            let normalized_description = normalize_tool_search_query(&spec.description);
            let haystack = format!(
                "{name} {} {canonical_name}",
                spec.description.to_lowercase()
            );
            let normalized_haystack = format!("{canonical_name} {normalized_description}");
            if required.iter().any(|term| !haystack.contains(term)) {
                return None;
            }

            let mut score = 0_i32;
            for term in &terms {
                let canonical_term = canonical_tool_token(term);
                if haystack.contains(term) {
                    score += 2;
                }
                if name == *term {
                    score += 8;
                }
                if name.contains(term) {
                    score += 4;
                }
                if canonical_name == canonical_term {
                    score += 12;
                }
                if normalized_haystack.contains(&canonical_term) {
                    score += 3;
                }
            }

            if score == 0 && !lowered.is_empty() {
                return None;
            }
            Some((score, spec.name.clone()))
        })
        .collect::<Vec<_>>();

    scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    scored
        .into_iter()
        .map(|(_, name)| name)
        .take(max_results)
        .collect()
}

pub(crate) fn normalize_tool_search_query(query: &str) -> String {
    query
        .trim()
        .split(|ch: char| ch.is_whitespace() || ch == ',')
        .filter(|term| !term.is_empty())
        .map(canonical_tool_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn canonical_tool_token(value: &str) -> String {
    let mut canonical = value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    if let Some(stripped) = canonical.strip_suffix("tool") {
        canonical = stripped.to_string();
    }
    canonical
}

#[allow(clippy::too_many_lines)]
fn execute_notebook_edit(
    policy: &WorkspacePathPolicy,
    input: NotebookEditInput,
) -> Result<NotebookEditOutput, String> {
    let path = policy.resolve(&input.notebook_path).map_err(io_to_string)?;
    if path.extension().and_then(|ext| ext.to_str()) != Some("ipynb") {
        return Err(String::from(
            "File must be a Jupyter notebook (.ipynb file).",
        ));
    }

    let original_file = std::fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let mut notebook: serde_json::Value =
        serde_json::from_str(&original_file).map_err(|error| error.to_string())?;
    let language = notebook
        .get("metadata")
        .and_then(|metadata| metadata.get("kernelspec"))
        .and_then(|kernelspec| kernelspec.get("language"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("python")
        .to_string();
    let cells = notebook
        .get_mut("cells")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| String::from("Notebook cells array not found"))?;

    let edit_mode = input.edit_mode.unwrap_or(NotebookEditMode::Replace);
    let target_index = match input.cell_id.as_deref() {
        Some(cell_id) => Some(resolve_cell_index(cells, Some(cell_id), edit_mode)?),
        None if matches!(
            edit_mode,
            NotebookEditMode::Replace | NotebookEditMode::Delete
        ) =>
        {
            Some(resolve_cell_index(cells, None, edit_mode)?)
        }
        None => None,
    };
    let resolved_cell_type = match edit_mode {
        NotebookEditMode::Delete => None,
        NotebookEditMode::Insert => Some(input.cell_type.unwrap_or(NotebookCellType::Code)),
        NotebookEditMode::Replace => Some(input.cell_type.unwrap_or_else(|| {
            target_index
                .and_then(|index| cells.get(index))
                .and_then(cell_kind)
                .unwrap_or(NotebookCellType::Code)
        })),
    };
    let new_source = require_notebook_source(input.new_source, edit_mode)?;

    let cell_id = match edit_mode {
        NotebookEditMode::Insert => {
            let resolved_cell_type = resolved_cell_type
                .ok_or_else(|| String::from("insert mode requires a cell type"))?;
            let new_id = make_cell_id(cells.len());
            let new_cell = build_notebook_cell(&new_id, resolved_cell_type, &new_source);
            let insert_at = target_index.map_or(cells.len(), |index| index + 1);
            cells.insert(insert_at, new_cell);
            cells
                .get(insert_at)
                .and_then(|cell| cell.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        }
        NotebookEditMode::Delete => {
            let idx = target_index
                .ok_or_else(|| String::from("delete mode requires a target cell index"))?;
            let removed = cells.remove(idx);
            removed
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        }
        NotebookEditMode::Replace => {
            let resolved_cell_type = resolved_cell_type
                .ok_or_else(|| String::from("replace mode requires a cell type"))?;
            let idx = target_index
                .ok_or_else(|| String::from("replace mode requires a target cell index"))?;
            let cell = cells
                .get_mut(idx)
                .ok_or_else(|| String::from("Cell index out of range"))?;
            cell["source"] = serde_json::Value::Array(source_lines(&new_source));
            cell["cell_type"] = serde_json::Value::String(match resolved_cell_type {
                NotebookCellType::Code => String::from("code"),
                NotebookCellType::Markdown => String::from("markdown"),
            });
            match resolved_cell_type {
                NotebookCellType::Code => {
                    if !cell.get("outputs").is_some_and(serde_json::Value::is_array) {
                        cell["outputs"] = json!([]);
                    }
                    if cell.get("execution_count").is_none() {
                        cell["execution_count"] = serde_json::Value::Null;
                    }
                }
                NotebookCellType::Markdown => {
                    if let Some(object) = cell.as_object_mut() {
                        object.remove("outputs");
                        object.remove("execution_count");
                    }
                }
            }
            cell.get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        }
    };

    let updated_file =
        serde_json::to_string_pretty(&notebook).map_err(|error| error.to_string())?;
    std::fs::write(&path, &updated_file).map_err(|error| error.to_string())?;

    Ok(NotebookEditOutput {
        new_source,
        cell_id,
        cell_type: resolved_cell_type,
        language,
        edit_mode: format_notebook_edit_mode(edit_mode),
        error: None,
        notebook_path: path.display().to_string(),
        original_file,
        updated_file,
    })
}

fn require_notebook_source(
    source: Option<String>,
    edit_mode: NotebookEditMode,
) -> Result<String, String> {
    match edit_mode {
        NotebookEditMode::Delete => Ok(source.unwrap_or_default()),
        NotebookEditMode::Insert | NotebookEditMode::Replace => source
            .ok_or_else(|| String::from("new_source is required for insert and replace edits")),
    }
}

fn build_notebook_cell(cell_id: &str, cell_type: NotebookCellType, source: &str) -> Value {
    let mut cell = json!({
        "cell_type": match cell_type {
            NotebookCellType::Code => "code",
            NotebookCellType::Markdown => "markdown",
        },
        "id": cell_id,
        "metadata": {},
        "source": source_lines(source),
    });
    if let Some(object) = cell.as_object_mut() {
        match cell_type {
            NotebookCellType::Code => {
                object.insert(String::from("outputs"), json!([]));
                object.insert(String::from("execution_count"), Value::Null);
            }
            NotebookCellType::Markdown => {}
        }
    }
    cell
}

fn cell_kind(cell: &serde_json::Value) -> Option<NotebookCellType> {
    cell.get("cell_type")
        .and_then(serde_json::Value::as_str)
        .map(|kind| {
            if kind == "markdown" {
                NotebookCellType::Markdown
            } else {
                NotebookCellType::Code
            }
        })
}

const MAX_SLEEP_DURATION_MS: u64 = 300_000;

#[allow(clippy::needless_pass_by_value)]
fn execute_sleep(input: SleepInput) -> Result<SleepOutput, String> {
    if input.duration_ms > MAX_SLEEP_DURATION_MS {
        return Err(format!(
            "duration_ms {} exceeds maximum allowed sleep of {MAX_SLEEP_DURATION_MS}ms",
            input.duration_ms,
        ));
    }
    std::thread::sleep(Duration::from_millis(input.duration_ms));
    Ok(SleepOutput {
        duration_ms: input.duration_ms,
        message: format!("Slept for {}ms", input.duration_ms),
    })
}

fn execute_brief(policy: &WorkspacePathPolicy, input: BriefInput) -> Result<BriefOutput, String> {
    if input.message.trim().is_empty() {
        return Err(String::from("message must not be empty"));
    }

    let attachments = input
        .attachments
        .as_ref()
        .map(|paths| {
            paths
                .iter()
                .map(|path| resolve_attachment(policy, path))
                .collect::<Result<Vec<_>, String>>()
        })
        .transpose()?;

    let message = match input.status {
        BriefStatus::Normal | BriefStatus::Proactive => input.message,
    };

    Ok(BriefOutput {
        message,
        attachments,
        sent_at: iso8601_timestamp(),
    })
}

fn resolve_attachment(
    policy: &WorkspacePathPolicy,
    path: &str,
) -> Result<ResolvedAttachment, String> {
    let resolved = policy.resolve(path).map_err(io_to_string)?;
    let metadata = std::fs::metadata(&resolved).map_err(|error| error.to_string())?;
    Ok(ResolvedAttachment {
        path: resolved.display().to_string(),
        size: metadata.len(),
        is_image: is_image_path(&resolved),
    })
}

fn is_image_path(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" | "svg")
    )
}

fn execute_config(input: ConfigInput) -> Result<ConfigOutput, String> {
    let setting = input.setting.trim();
    if setting.is_empty() {
        return Err(String::from("setting must not be empty"));
    }
    let Some(spec) = supported_config_setting(setting) else {
        return Ok(ConfigOutput {
            success: false,
            operation: None,
            setting: None,
            value: None,
            previous_value: None,
            new_value: None,
            error: Some(format!("Unknown setting: \"{setting}\"")),
        });
    };

    let path = config_file_for_scope(spec.scope)?;
    let mut document = read_json_object(&path)?;

    if let Some(value) = input.value {
        let normalized = normalize_config_value(spec, value)?;
        let previous_value = get_nested_value(&document, spec.path).cloned();
        set_nested_value(&mut document, spec.path, normalized.clone());
        write_json_object(&path, &document)?;
        Ok(ConfigOutput {
            success: true,
            operation: Some(String::from("set")),
            setting: Some(setting.to_string()),
            value: Some(normalized.clone()),
            previous_value,
            new_value: Some(normalized),
            error: None,
        })
    } else {
        Ok(ConfigOutput {
            success: true,
            operation: Some(String::from("get")),
            setting: Some(setting.to_string()),
            value: get_nested_value(&document, spec.path).cloned(),
            previous_value: None,
            new_value: None,
            error: None,
        })
    }
}

const PERMISSION_DEFAULT_MODE_PATH: &[&str] = &["permissions", "defaultMode"];

fn execute_enter_plan_mode(_input: EnterPlanModeInput) -> Result<PlanModeOutput, String> {
    let settings_path = config_file_for_scope(ConfigScope::Settings)?;
    let state_path = plan_mode_state_file()?;
    let mut document = read_json_object(&settings_path)?;
    let current_local_mode = get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned();
    let current_is_plan =
        matches!(current_local_mode.as_ref(), Some(Value::String(value)) if value == "plan");

    if let Some(state) = read_plan_mode_state(&state_path)? {
        if current_is_plan {
            return Ok(PlanModeOutput {
                success: true,
                operation: String::from("enter"),
                changed: false,
                active: true,
                managed: true,
                message: String::from("Plan mode override is already active for this worktree."),
                settings_path: settings_path.display().to_string(),
                state_path: state_path.display().to_string(),
                previous_local_mode: state.previous_local_mode,
                current_local_mode,
            });
        }
        clear_plan_mode_state(&state_path)?;
    }

    if current_is_plan {
        return Ok(PlanModeOutput {
            success: true,
            operation: String::from("enter"),
            changed: false,
            active: true,
            managed: false,
            message: String::from(
                "Worktree-local plan mode is already enabled outside EnterPlanMode; leaving it unchanged.",
            ),
            settings_path: settings_path.display().to_string(),
            state_path: state_path.display().to_string(),
            previous_local_mode: None,
            current_local_mode,
        });
    }

    let state = PlanModeState {
        had_local_override: current_local_mode.is_some(),
        previous_local_mode: current_local_mode.clone(),
    };
    write_plan_mode_state(&state_path, &state)?;
    set_nested_value(
        &mut document,
        PERMISSION_DEFAULT_MODE_PATH,
        Value::String(String::from("plan")),
    );
    write_json_object(&settings_path, &document)?;

    Ok(PlanModeOutput {
        success: true,
        operation: String::from("enter"),
        changed: true,
        active: true,
        managed: true,
        message: String::from("Enabled worktree-local plan mode override."),
        settings_path: settings_path.display().to_string(),
        state_path: state_path.display().to_string(),
        previous_local_mode: state.previous_local_mode,
        current_local_mode: get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned(),
    })
}

fn execute_exit_plan_mode(_input: ExitPlanModeInput) -> Result<PlanModeOutput, String> {
    let settings_path = config_file_for_scope(ConfigScope::Settings)?;
    let state_path = plan_mode_state_file()?;
    let mut document = read_json_object(&settings_path)?;
    let current_local_mode = get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned();
    let current_is_plan =
        matches!(current_local_mode.as_ref(), Some(Value::String(value)) if value == "plan");

    let Some(state) = read_plan_mode_state(&state_path)? else {
        return Ok(PlanModeOutput {
            success: true,
            operation: String::from("exit"),
            changed: false,
            active: current_is_plan,
            managed: false,
            message: String::from("No EnterPlanMode override is active for this worktree."),
            settings_path: settings_path.display().to_string(),
            state_path: state_path.display().to_string(),
            previous_local_mode: None,
            current_local_mode,
        });
    };

    if !current_is_plan {
        clear_plan_mode_state(&state_path)?;
        return Ok(PlanModeOutput {
            success: true,
            operation: String::from("exit"),
            changed: false,
            active: false,
            managed: false,
            message: String::from(
                "Cleared stale EnterPlanMode state because plan mode was already changed outside the tool.",
            ),
            settings_path: settings_path.display().to_string(),
            state_path: state_path.display().to_string(),
            previous_local_mode: state.previous_local_mode,
            current_local_mode,
        });
    }

    if state.had_local_override {
        if let Some(previous_local_mode) = state.previous_local_mode.clone() {
            set_nested_value(
                &mut document,
                PERMISSION_DEFAULT_MODE_PATH,
                previous_local_mode,
            );
        } else {
            remove_nested_value(&mut document, PERMISSION_DEFAULT_MODE_PATH);
        }
    } else {
        remove_nested_value(&mut document, PERMISSION_DEFAULT_MODE_PATH);
    }
    write_json_object(&settings_path, &document)?;
    clear_plan_mode_state(&state_path)?;

    Ok(PlanModeOutput {
        success: true,
        operation: String::from("exit"),
        changed: true,
        active: false,
        managed: false,
        message: String::from("Restored the prior worktree-local plan mode setting."),
        settings_path: settings_path.display().to_string(),
        state_path: state_path.display().to_string(),
        previous_local_mode: state.previous_local_mode,
        current_local_mode: get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned(),
    })
}

fn execute_structured_output(
    input: StructuredOutputInput,
) -> Result<StructuredOutputResult, String> {
    if input.0.is_empty() {
        return Err(String::from("structured output payload must not be empty"));
    }
    Ok(StructuredOutputResult {
        data: String::from("Structured output provided successfully"),
        structured_output: input.0,
    })
}

fn execute_repl(input: ReplInput) -> Result<ReplOutput, String> {
    if input.code.trim().is_empty() {
        return Err(String::from("code must not be empty"));
    }
    let language = input.language.trim().to_ascii_lowercase();
    if !matches!(
        language.as_str(),
        "python" | "py" | "javascript" | "js" | "node" | "bash" | "sh" | "shell"
    ) {
        return Err(format!("unsupported REPL language: {}", input.language));
    }
    let started = Instant::now();
    let output = crate::sandbox_exec::execute_code_with_timeout(
        &input.language,
        &input.code,
        input.timeout_ms,
    );
    if output.exit_code == 124 {
        return Err(format!(
            "REPL execution exceeded timeout of {} ms",
            input.timeout_ms.unwrap_or_default()
        ));
    }
    if output.exit_code != 0 {
        return Err(format!(
            "REPL execution failed with exit code {}: {}",
            output.exit_code,
            output.stderr.trim()
        ));
    }

    Ok(ReplOutput {
        language: input.language,
        stdout: output.stdout,
        stderr: output.stderr,
        exit_code: output.exit_code,
        duration_ms: started.elapsed().as_millis(),
    })
}

#[derive(Clone, Copy)]
enum ConfigScope {
    Global,
    Settings,
}

#[derive(Clone, Copy)]
struct ConfigSettingSpec {
    scope: ConfigScope,
    kind: ConfigKind,
    path: &'static [&'static str],
    options: Option<&'static [&'static str]>,
}

#[derive(Clone, Copy)]
enum ConfigKind {
    Boolean,
    String,
}

fn supported_config_setting(setting: &str) -> Option<ConfigSettingSpec> {
    Some(match setting {
        "theme" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["theme"],
            options: None,
        },
        "editorMode" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["editorMode"],
            options: Some(&["default", "vim", "emacs"]),
        },
        "verbose" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["verbose"],
            options: None,
        },
        "preferredNotifChannel" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["preferredNotifChannel"],
            options: None,
        },
        "autoCompactEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["autoCompactEnabled"],
            options: None,
        },
        "autoMemoryEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::Boolean,
            path: &["autoMemoryEnabled"],
            options: None,
        },
        "autoDreamEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::Boolean,
            path: &["autoDreamEnabled"],
            options: None,
        },
        "fileCheckpointingEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["fileCheckpointingEnabled"],
            options: None,
        },
        "showTurnDuration" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["showTurnDuration"],
            options: None,
        },
        "terminalProgressBarEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["terminalProgressBarEnabled"],
            options: None,
        },
        "todoFeatureEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["todoFeatureEnabled"],
            options: None,
        },
        "model" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::String,
            path: &["model"],
            options: None,
        },
        "alwaysThinkingEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::Boolean,
            path: &["alwaysThinkingEnabled"],
            options: None,
        },
        "permissions.defaultMode" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::String,
            path: &["permissions", "defaultMode"],
            options: Some(&["default", "plan", "acceptEdits", "dontAsk", "auto"]),
        },
        "language" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::String,
            path: &["language"],
            options: None,
        },
        "teammateMode" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["teammateMode"],
            options: Some(&["tmux", "in-process", "auto"]),
        },
        _ => return None,
    })
}

fn normalize_config_value(spec: ConfigSettingSpec, value: ConfigValue) -> Result<Value, String> {
    let normalized = match (spec.kind, value) {
        (ConfigKind::Boolean, ConfigValue::Bool(value)) => Value::Bool(value),
        (ConfigKind::Boolean, ConfigValue::String(value)) => {
            match value.trim().to_ascii_lowercase().as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => return Err(String::from("setting requires true or false")),
            }
        }
        (ConfigKind::Boolean, ConfigValue::Number(_)) => {
            return Err(String::from("setting requires true or false"));
        }
        (ConfigKind::String, ConfigValue::String(value)) => Value::String(value),
        (ConfigKind::String, ConfigValue::Bool(value)) => Value::String(value.to_string()),
        (ConfigKind::String, ConfigValue::Number(value)) => json!(value),
    };

    if let Some(options) = spec.options {
        let Some(as_str) = normalized.as_str() else {
            return Err(String::from("setting requires a string value"));
        };
        if !options.iter().any(|option| option == &as_str) {
            return Err(format!(
                "Invalid value \"{as_str}\". Options: {}",
                options.join(", ")
            ));
        }
    }

    Ok(normalized)
}

fn config_file_for_scope(scope: ConfigScope) -> Result<PathBuf, String> {
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    Ok(match scope {
        ConfigScope::Global => config_home_dir()?.join("config.yaml"),
        ConfigScope::Settings => cwd.join(".cowd").join("config.local.yaml"),
    })
}

fn config_home_dir() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("COWD_CONFIG_HOME") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map_err(|_| {
            String::from(
                "HOME is not set (on Windows, set USERPROFILE or HOME, \
                 or use CC_CONFIG_HOME to point directly at the config directory)",
            )
        })?;
    Ok(PathBuf::from(home).join(".cowd"))
}

fn read_json_object(path: &Path) -> Result<serde_json::Map<String, Value>, String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            if contents.trim().is_empty() {
                return Ok(serde_json::Map::new());
            }
            // Try JSON first (fast path), fall back to YAML parser
            // (YAML is a superset of JSON so both work)
            let val = serde_json::from_str::<Value>(&contents)
                .or_else(|_| serde_yaml::from_str::<Value>(&contents))
                .map_err(|error| {
                    format!("failed to parse config (tried JSON then YAML): {error}")
                })?;
            val.as_object()
                .cloned()
                .ok_or_else(|| String::from("config file must contain a JSON/YAML object"))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(serde_json::Map::new()),
        Err(error) => Err(error.to_string()),
    }
}

fn write_json_object(path: &Path, value: &serde_json::Map<String, Value>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    // Write as well-formatted JSON (which is valid YAML)
    std::fs::write(
        path,
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn get_nested_value<'a>(
    value: &'a serde_json::Map<String, Value>,
    path: &[&str],
) -> Option<&'a Value> {
    let (first, rest) = path.split_first()?;
    let mut current = value.get(*first)?;
    for key in rest {
        current = current.as_object()?.get(*key)?;
    }
    Some(current)
}

fn set_nested_value(root: &mut serde_json::Map<String, Value>, path: &[&str], new_value: Value) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };
    if rest.is_empty() {
        root.insert((*first).to_string(), new_value);
        return;
    }

    let entry = root
        .entry((*first).to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if !entry.is_object() {
        *entry = Value::Object(serde_json::Map::new());
    }
    if let Some(map) = entry.as_object_mut() {
        set_nested_value(map, rest, new_value);
    }
}

fn remove_nested_value(root: &mut serde_json::Map<String, Value>, path: &[&str]) -> bool {
    let Some((first, rest)) = path.split_first() else {
        return false;
    };
    if rest.is_empty() {
        return root.remove(*first).is_some();
    }

    let mut should_remove_parent = false;
    let removed = root.get_mut(*first).is_some_and(|entry| {
        entry.as_object_mut().is_some_and(|map| {
            let removed = remove_nested_value(map, rest);
            should_remove_parent = removed && map.is_empty();
            removed
        })
    });

    if should_remove_parent {
        root.remove(*first);
    }

    removed
}

fn plan_mode_state_file() -> Result<PathBuf, String> {
    Ok(config_file_for_scope(ConfigScope::Settings)?
        .parent()
        .ok_or_else(|| String::from("config.local.yaml has no parent directory"))?
        .join("tool-state")
        .join("plan-mode.json"))
}

fn read_plan_mode_state(path: &Path) -> Result<Option<PlanModeState>, String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            if contents.trim().is_empty() {
                return Ok(None);
            }
            serde_json::from_str(&contents)
                .map(Some)
                .map_err(|error| error.to_string())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn write_plan_mode_state(path: &Path, state: &PlanModeState) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(
        path,
        serde_json::to_string_pretty(state).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn clear_plan_mode_state(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn iso8601_timestamp() -> String {
    if let Ok(output) = Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
    {
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }
    iso8601_now()
}

#[allow(clippy::needless_pass_by_value)]
fn execute_powershell(input: PowerShellInput) -> std::io::Result<BashCommandOutput> {
    if let Some(output) = workspace_test_branch_preflight(&input.command, None) {
        return Ok(output);
    }
    let shell = detect_powershell_shell()?;
    crate::bash::execute_bash(BashCommandInput {
        command: format!(
            "exec {} -NoProfile -NonInteractive -Command {}",
            shell_quote(&shell.display().to_string()),
            shell_quote(&input.command)
        ),
        cwd: None,
        timeout: input.timeout,
        description: input.description,
        run_in_background: input.run_in_background,
        dangerously_disable_sandbox: Some(false),
        namespace_restrictions: Some(true),
        isolate_network: None,
        filesystem_mode: None,
        allowed_mounts: None,
    })
}

fn detect_powershell_shell() -> std::io::Result<PathBuf> {
    if let Some(path) = find_command("pwsh") {
        Ok(path)
    } else if let Some(path) = find_command("powershell") {
        Ok(path)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "PowerShell executable not found (expected `pwsh` or `powershell` in PATH)",
        ))
    }
}

fn find_command(name: &str) -> Option<PathBuf> {
    // Safety: validate input to prevent path traversal and injection
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return None;
    }
    // Search PATH directories for the executable
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let full_path = dir.join(name);
            if !full_path.is_file() {
                return None;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::metadata(&full_path)
                    .map(|m| m.permissions().mode() & 0o111 != 0)
                    .unwrap_or(false)
                    .then(|| full_path.canonicalize().unwrap_or(full_path))
            }
            #[cfg(not(unix))]
            {
                Some(full_path.canonicalize().unwrap_or(full_path))
            }
        })
    })
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn resolve_cell_index(
    cells: &[serde_json::Value],
    cell_id: Option<&str>,
    edit_mode: NotebookEditMode,
) -> Result<usize, String> {
    if cells.is_empty()
        && matches!(
            edit_mode,
            NotebookEditMode::Replace | NotebookEditMode::Delete
        )
    {
        return Err(String::from("Notebook has no cells to edit"));
    }
    if let Some(cell_id) = cell_id {
        cells
            .iter()
            .position(|cell| cell.get("id").and_then(serde_json::Value::as_str) == Some(cell_id))
            .ok_or_else(|| format!("Cell id not found: {cell_id}"))
    } else {
        Ok(cells.len().saturating_sub(1))
    }
}

fn source_lines(source: &str) -> Vec<serde_json::Value> {
    if source.is_empty() {
        return vec![serde_json::Value::String(String::new())];
    }
    source
        .split_inclusive('\n')
        .map(|line| serde_json::Value::String(line.to_string()))
        .collect()
}

fn format_notebook_edit_mode(mode: NotebookEditMode) -> String {
    match mode {
        NotebookEditMode::Replace => String::from("replace"),
        NotebookEditMode::Insert => String::from("insert"),
        NotebookEditMode::Delete => String::from("delete"),
    }
}

fn make_cell_id(index: usize) -> String {
    format!("cell-{}", index + 1)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use super::execute_tool_for_test as execute_tool;
    use crate::lane_events::LaneEventName;
    use crate::permissions::{PermissionEnforcer, PermissionMode, PermissionPolicy};
    use crate::{mvp_tool_specs, permission_mode_from_plugin, ToolCatalog};
    use serde_json::json;

    fn env_lock() -> &'static Mutex<()> {
        crate::test_process_environment_lock()
    }

    fn temp_path(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("cowd-tools-{unique}-{name}"))
    }

    fn execute_in_workspace(
        root: &Path,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<String, String> {
        let host = crate::ToolHost::builtin("tools-test-workspace", root);
        super::execute_with_lease(&host.pin_snapshot(), name, input)
    }

    fn run_git(cwd: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .unwrap_or_else(|error| panic!("git {} failed: {error}", args.join(" ")));
        assert!(
            status.success(),
            "git {} exited with {status}",
            args.join(" ")
        );
    }

    fn init_git_repo(path: &Path) {
        std::fs::create_dir_all(path).expect("create repo");
        run_git(path, &["init", "--quiet", "-b", "main"]);
        run_git(path, &["config", "user.email", "tests@example.com"]);
        run_git(path, &["config", "user.name", "Tools Tests"]);
        std::fs::write(path.join("README.md"), "initial\n").expect("write readme");
        run_git(path, &["add", "README.md"]);
        run_git(path, &["commit", "-m", "initial commit", "--quiet"]);
    }

    fn commit_file(path: &Path, file: &str, contents: &str, message: &str) {
        std::fs::write(path.join(file), contents).expect("write file");
        run_git(path, &["add", file]);
        run_git(path, &["commit", "-m", message, "--quiet"]);
    }

    fn permission_policy_for_mode(mode: PermissionMode) -> PermissionPolicy {
        mvp_tool_specs()
            .into_iter()
            .fold(PermissionPolicy::new(mode), |policy, spec| {
                policy.with_tool_requirement(spec.name, spec.required_permission)
            })
    }

    struct HttpResponse {
        status: u16,
        reason: &'static str,
        content_type: &'static str,
        body: String,
    }

    impl HttpResponse {
        fn html(status: u16, reason: &'static str, body: impl Into<String>) -> Self {
            Self {
                status,
                reason,
                content_type: "text/html; charset=utf-8",
                body: body.into(),
            }
        }

        fn text(status: u16, reason: &'static str, body: impl Into<String>) -> Self {
            Self {
                status,
                reason,
                content_type: "text/plain; charset=utf-8",
                body: body.into(),
            }
        }
    }

    struct TestServer {
        addr: SocketAddr,
    }

    impl TestServer {
        fn spawn(handler: Arc<dyn Fn(&str) -> HttpResponse + Send + Sync + 'static>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
            let addr = listener.local_addr().expect("test server addr");
            thread::spawn(move || {
                for stream in listener.incoming().take(8) {
                    let Ok(mut stream) = stream else {
                        continue;
                    };
                    let mut buffer = [0_u8; 4096];
                    let Ok(read) = stream.read(&mut buffer) else {
                        continue;
                    };
                    let request = String::from_utf8_lossy(&buffer[..read]);
                    let request_line = request.lines().next().unwrap_or_default();
                    let response = handler(request_line);
                    let payload = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response.status,
                        response.reason,
                        response.content_type,
                        response.body.len(),
                        response.body
                    );
                    let _ = stream.write_all(payload.as_bytes());
                }
            });
            Self { addr }
        }

        fn addr(&self) -> SocketAddr {
            self.addr
        }
    }

    #[test]
    fn exposes_mvp_tools() {
        let names = mvp_tool_specs()
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"bash"));
        assert!(names.contains(&"read_file"));
        assert!(names.contains(&"WebFetch"));
        assert!(names.contains(&"WebSearch"));
        assert!(names.contains(&"TodoWrite"));
        assert!(names.contains(&"ToolSearch"));
        assert!(names.contains(&"NotebookEdit"));
        assert!(names.contains(&"Sleep"));
        assert!(names.contains(&"SendUserMessage"));
        assert!(names.contains(&"Config"));
        assert!(names.contains(&"EnterPlanMode"));
        assert!(names.contains(&"ExitPlanMode"));
        assert!(names.contains(&"StructuredOutput"));
        assert!(names.contains(&"REPL"));
        assert!(names.contains(&"PowerShell"));
        for removed in [
            "Agent",
            "TaskCreate",
            "RunTaskPacket",
            "TaskGet",
            "TaskList",
            "TaskStop",
            "TaskUpdate",
            "TaskOutput",
            "WorkerCreate",
            "WorkerObserve",
            "WorkerAwaitReady",
            "WorkerSendPrompt",
            "WorkerRestart",
            "WorkerTerminate",
            "TeamCreate",
            "TeamDelete",
            "CronCreate",
            "CronDelete",
            "CronList",
        ] {
            assert!(
                !names.contains(&removed),
                "control-plane tool {removed} must not be exposed by tools"
            );
        }
    }

    #[test]
    fn rejects_unknown_tool_names() {
        let error = execute_tool("nope", &json!({})).expect_err("tool should be rejected");
        assert!(error.contains("unsupported tool"));
    }

    #[test]
    fn test_catalog_denies_blocked_tool_before_dispatch() {
        // given
        let policy = permission_policy_for_mode(PermissionMode::ReadOnly);
        let registry = ToolCatalog::builtin().with_enforcer(PermissionEnforcer::new(policy));

        // when
        let error = registry
            .execute(
                "write_file",
                &json!({
                    "path": "blocked.txt",
                    "content": "blocked"
                }),
            )
            .expect_err("write tool should be denied before dispatch");

        // then
        assert!(error.contains("requires workspace-write permission"));
    }

    #[test]
    fn permission_mode_from_plugin_rejects_invalid_inputs() {
        let unknown_permission = permission_mode_from_plugin("admin")
            .expect_err("unknown plugin permission should fail");
        assert!(unknown_permission.contains("unsupported plugin permission: admin"));

        let empty_permission =
            permission_mode_from_plugin("").expect_err("empty plugin permission should fail");
        assert!(empty_permission.contains("unsupported plugin permission: "));
    }

    #[test]
    fn runtime_tools_extend_registry_definitions_permissions_and_search() {
        let registry = Arc::new(
            ToolCatalog::builtin()
                .with_runtime_tools(vec![crate::RuntimeToolDefinition {
                    name: "mcp__demo__echo".to_string(),
                    description: Some("Echo text from the demo MCP server".to_string()),
                    input_schema: json!({
                        "type": "object",
                        "properties": { "text": { "type": "string" } },
                        "additionalProperties": false
                    }),
                    required_permission: PermissionMode::ReadOnly,
                }])
                .expect("runtime tools should register"),
        );

        let allowed = registry
            .normalize_allowed_tools(&["mcp__demo__echo".to_string()])
            .expect("runtime tool should be allow-listable")
            .expect("allow-list should be populated");
        assert!(allowed.contains("mcp__demo__echo"));

        let definitions = registry.definitions(Some(&allowed));
        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].name, "mcp__demo__echo");

        let permissions = registry
            .permission_specs(Some(&allowed))
            .expect("runtime tool permissions should resolve");
        assert_eq!(
            permissions,
            vec![("mcp__demo__echo".to_string(), PermissionMode::ReadOnly)]
        );

        let host = crate::ToolHost::new(
            "test-workspace",
            std::env::current_dir().unwrap(),
            crate::ToolHostSnapshot::new(
                Arc::clone(&registry),
                Arc::new(crate::lsp_client::LspRegistry::new()),
                None,
            ),
        );
        let search = host.pin_snapshot().search("demo echo", 5);
        let output = serde_json::to_value(search).expect("search output should serialize");
        assert_eq!(output["activation_candidates"][0], "mcp__demo__echo");
        assert_eq!(output["descriptors"][0]["source"], "runtime");
        assert_eq!(output["catalog_revision"], 1);
    }

    #[test]
    fn web_fetch_returns_prompt_aware_summary() {
        let server = TestServer::spawn(Arc::new(|request_line: &str| {
            assert!(request_line.starts_with("GET /page "));
            HttpResponse::html(
                200,
                "OK",
                "<html><head><title>Ignored</title></head><body><h1>Test Page</h1><p>Hello <b>world</b> from local server.</p></body></html>",
            )
        }));

        let result = execute_tool(
            "WebFetch",
            &json!({
                "url": format!("http://{}/page", server.addr()),
                "prompt": "Summarize this page"
            }),
        )
        .expect("WebFetch should succeed");

        let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
        assert_eq!(output["code"], 200);
        let summary = output["result"].as_str().expect("result string");
        assert!(summary.contains("Fetched"));
        assert!(summary.contains("Test Page"));
        assert!(summary.contains("Hello world from local server"));

        let titled = execute_tool(
            "WebFetch",
            &json!({
                "url": format!("http://{}/page", server.addr()),
                "prompt": "What is the page title?"
            }),
        )
        .expect("WebFetch title query should succeed");
        let titled_output: serde_json::Value = serde_json::from_str(&titled).expect("valid json");
        let titled_summary = titled_output["result"].as_str().expect("result string");
        assert!(titled_summary.contains("Title: Ignored"));
    }

    #[test]
    fn web_fetch_supports_plain_text_and_rejects_invalid_url() {
        let server = TestServer::spawn(Arc::new(|request_line: &str| {
            assert!(request_line.starts_with("GET /plain "));
            HttpResponse::text(200, "OK", "plain text response")
        }));

        let result = execute_tool(
            "WebFetch",
            &json!({
                "url": format!("http://{}/plain", server.addr()),
                "prompt": "Show me the content"
            }),
        )
        .expect("WebFetch should succeed for text content");

        let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
        assert_eq!(output["url"], format!("http://{}/plain", server.addr()));
        assert!(output["result"]
            .as_str()
            .expect("result")
            .contains("plain text response"));

        let error = execute_tool(
            "WebFetch",
            &json!({
                "url": "not a url",
                "prompt": "Summarize"
            }),
        )
        .expect_err("invalid URL should fail");
        assert!(error.contains("relative URL without a base") || error.contains("invalid"));
    }

    #[test]
    fn web_search_extracts_and_filters_results() {
        // Serialize env-var mutation so this test cannot race with the sibling
        // web_search_handles_generic_links_and_invalid_base_url test that also
        // sets COWD_WEB_SEARCH_BASE_URL. Without the lock, parallel test
        // runners can interleave the set/remove calls and cause assertion
        // failures on the wrong port.
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let server = TestServer::spawn(Arc::new(|request_line: &str| {
            assert!(request_line.contains("GET /search?q=rust+web+search "));
            HttpResponse::html(
                200,
                "OK",
                r#"
                <html><body>
                  <a class="result__a" href="https://docs.rs/reqwest">Reqwest docs</a>
                  <a class="result__a" href="https://example.com/blocked">Blocked result</a>
                </body></html>
                "#,
            )
        }));

        std::env::set_var(
            "COWD_WEB_SEARCH_BASE_URL",
            format!("http://{}/search", server.addr()),
        );
        let result = execute_tool(
            "WebSearch",
            &json!({
                "query": "rust web search",
                "allowed_domains": ["https://DOCS.rs/"],
                "blocked_domains": ["HTTPS://EXAMPLE.COM"]
            }),
        )
        .expect("WebSearch should succeed");
        std::env::remove_var("COWD_WEB_SEARCH_BASE_URL");

        let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
        assert_eq!(output["query"], "rust web search");
        let results = output["results"].as_array().expect("results array");
        let search_result = results
            .iter()
            .find(|item| item.get("content").is_some())
            .expect("search result block present");
        let content = search_result["content"].as_array().expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["title"], "Reqwest docs");
        assert_eq!(content[0]["url"], "https://docs.rs/reqwest");
    }

    #[test]
    fn web_search_handles_generic_links_and_invalid_base_url() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let server = TestServer::spawn(Arc::new(|request_line: &str| {
            assert!(request_line.contains("GET /fallback?q=generic+links "));
            HttpResponse::html(
                200,
                "OK",
                r#"
                <html><body>
                  <a href="https://example.com/one">Example One</a>
                  <a href="https://example.com/one">Duplicate Example One</a>
                  <a href="https://docs.rs/tokio">Tokio Docs</a>
                </body></html>
                "#,
            )
        }));

        std::env::set_var(
            "COWD_WEB_SEARCH_BASE_URL",
            format!("http://{}/fallback", server.addr()),
        );
        let result = execute_tool(
            "WebSearch",
            &json!({
                "query": "generic links"
            }),
        )
        .expect("WebSearch fallback parsing should succeed");
        std::env::remove_var("COWD_WEB_SEARCH_BASE_URL");

        let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
        let results = output["results"].as_array().expect("results array");
        let search_result = results
            .iter()
            .find(|item| item.get("content").is_some())
            .expect("search result block present");
        let content = search_result["content"].as_array().expect("content array");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["url"], "https://example.com/one");
        assert_eq!(content[1]["url"], "https://docs.rs/tokio");

        std::env::set_var("COWD_WEB_SEARCH_BASE_URL", "://bad-base-url");
        let error = execute_tool("WebSearch", &json!({ "query": "generic links" }))
            .expect_err("invalid base URL should fail");
        std::env::remove_var("COWD_WEB_SEARCH_BASE_URL");
        assert!(error.contains("relative URL without a base") || error.contains("empty host"));
    }

    #[test]
    fn todo_write_persists_and_returns_previous_state() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = temp_path("todos.json");
        std::env::set_var("COWD_TODO_STORE", &path);

        let first = execute_tool(
            "TodoWrite",
            &json!({
                "todos": [
                    {"content": "Add tool", "activeForm": "Adding tool", "status": "in_progress"},
                    {"content": "Run tests", "activeForm": "Running tests", "status": "pending"}
                ]
            }),
        )
        .expect("TodoWrite should succeed");
        let first_output: serde_json::Value = serde_json::from_str(&first).expect("valid json");
        assert_eq!(first_output["oldTodos"].as_array().expect("array").len(), 0);

        let second = execute_tool(
            "TodoWrite",
            &json!({
                "todos": [
                    {"content": "Add tool", "activeForm": "Adding tool", "status": "completed"},
                    {"content": "Run tests", "activeForm": "Running tests", "status": "completed"},
                    {"content": "Verify", "activeForm": "Verifying", "status": "completed"}
                ]
            }),
        )
        .expect("TodoWrite should succeed");
        std::env::remove_var("COWD_TODO_STORE");
        let _ = std::fs::remove_file(path);

        let second_output: serde_json::Value = serde_json::from_str(&second).expect("valid json");
        assert_eq!(
            second_output["oldTodos"].as_array().expect("array").len(),
            2
        );
        assert_eq!(
            second_output["newTodos"].as_array().expect("array").len(),
            3
        );
        assert!(second_output["verificationNudgeNeeded"].is_null());
    }

    #[test]
    fn todo_write_rejects_invalid_payloads_and_sets_verification_nudge() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = temp_path("todos-errors.json");
        std::env::set_var("COWD_TODO_STORE", &path);

        let empty = execute_tool("TodoWrite", &json!({ "todos": [] }))
            .expect_err("empty todos should fail");
        assert!(empty.contains("todos must not be empty"));

        // Multiple in_progress items are now allowed for parallel workflows
        let _multi_active = execute_tool(
            "TodoWrite",
            &json!({
                "todos": [
                    {"content": "One", "activeForm": "Doing one", "status": "in_progress"},
                    {"content": "Two", "activeForm": "Doing two", "status": "in_progress"}
                ]
            }),
        )
        .expect("multiple in-progress todos should succeed");

        let blank_content = execute_tool(
            "TodoWrite",
            &json!({
                "todos": [
                    {"content": "   ", "activeForm": "Doing it", "status": "pending"}
                ]
            }),
        )
        .expect_err("blank content should fail");
        assert!(blank_content.contains("todo content must not be empty"));

        let nudge = execute_tool(
            "TodoWrite",
            &json!({
                "todos": [
                    {"content": "Write tests", "activeForm": "Writing tests", "status": "completed"},
                    {"content": "Fix errors", "activeForm": "Fixing errors", "status": "completed"},
                    {"content": "Ship branch", "activeForm": "Shipping branch", "status": "completed"}
                ]
            }),
        )
        .expect("completed todos should succeed");
        std::env::remove_var("COWD_TODO_STORE");
        let _ = fs::remove_file(path);

        let output: serde_json::Value = serde_json::from_str(&nudge).expect("valid json");
        assert_eq!(output["verificationNudgeNeeded"], true);
    }

    #[test]
    fn tool_search_supports_keyword_and_select_queries() {
        let keyword = execute_tool(
            "ToolSearch",
            &json!({"query": "web current", "max_results": 3}),
        )
        .expect("ToolSearch should succeed");
        let keyword_output: serde_json::Value = serde_json::from_str(&keyword).expect("valid json");
        let matches = keyword_output["activation_candidates"]
            .as_array()
            .expect("activation candidates");
        assert!(matches.iter().any(|value| value == "WebSearch"));

        let selected = execute_tool(
            "ToolSearch",
            &json!({"query": "select:WebSearch,ToolSearch"}),
        )
        .expect("ToolSearch should succeed");
        let selected_output: serde_json::Value =
            serde_json::from_str(&selected).expect("valid json");
        let selected_matches = selected_output["activation_candidates"]
            .as_array()
            .expect("activation candidates");
        assert_eq!(selected_matches.len(), 2);
        assert!(selected_matches.iter().any(|value| value == "WebSearch"));
        assert!(selected_matches.iter().any(|value| value == "ToolSearch"));

        let source_search = execute_tool(
            "ToolSearch",
            &json!({"query": "select:grep_search,grep_many,read_file"}),
        )
        .expect("ToolSearch should expose executable source tools");
        let source_output: serde_json::Value =
            serde_json::from_str(&source_search).expect("valid json");
        assert_eq!(
            source_output["activation_candidates"],
            json!(["grep_search", "grep_many", "read_file"])
        );

        let exact_grep = execute_tool(
            "ToolSearch",
            &json!({"query": "grep_search", "max_results": 1}),
        )
        .expect("focused grep discovery should succeed");
        let exact_grep_output: serde_json::Value =
            serde_json::from_str(&exact_grep).expect("valid json");
        assert_eq!(
            exact_grep_output["activation_candidates"],
            json!(["grep_search"])
        );

        let removed = execute_tool("ToolSearch", &json!({"query": "select:Agent,WorkerCreate"}))
            .expect("ToolSearch should ignore removed control-plane tools");
        let removed_output: serde_json::Value = serde_json::from_str(&removed).expect("valid json");
        assert!(
            removed_output["activation_candidates"]
                .as_array()
                .expect("activation candidates")
                .is_empty(),
            "removed control-plane tools must not be searchable"
        );
    }

    #[test]
    fn lane_event_schema_serializes_to_canonical_names() {
        let cases = [
            (LaneEventName::Started, "lane.started"),
            (LaneEventName::Ready, "lane.ready"),
            (LaneEventName::PromptMisdelivery, "lane.prompt_misdelivery"),
            (LaneEventName::Blocked, "lane.blocked"),
            (LaneEventName::Red, "lane.red"),
            (LaneEventName::Green, "lane.green"),
            (LaneEventName::CommitCreated, "lane.commit.created"),
            (LaneEventName::PrOpened, "lane.pr.opened"),
            (LaneEventName::MergeReady, "lane.merge.ready"),
            (LaneEventName::Finished, "lane.finished"),
            (LaneEventName::Failed, "lane.failed"),
            (
                LaneEventName::BranchStaleAgainstMain,
                "branch.stale_against_main",
            ),
            (
                LaneEventName::BranchWorkspaceMismatch,
                "branch.workspace_mismatch",
            ),
        ];

        for (event, expected) in cases {
            assert_eq!(
                serde_json::to_value(event).expect("serialize lane event"),
                json!(expected)
            );
        }
    }

    #[test]
    fn tools_executor_does_not_own_agent_lifecycle() {
        let source_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/execution/executor.rs");
        let source =
            std::fs::read_to_string(source_path).expect("executor source should be readable");
        for forbidden in [
            ["fn ", "spawn_agent_job"].concat(),
            ["fn ", "run_agent_job"].concat(),
            ["fn ", "write_agent_manifest"].concat(),
            ["fn ", "persist_agent_terminal_state"].concat(),
            ["std::thread::", "Builder::new()"].concat(),
        ] {
            assert!(
                !source.contains(&forbidden),
                "tools executor must not own Agent lifecycle primitive `{forbidden}`"
            );
        }
        assert!(
            !source.contains(&["runtime", "::", "spawn_provider_agent"].concat()),
            "tools executor must not spawn agent providers directly"
        );
    }

    #[test]
    fn tools_crate_does_not_own_runtime_control_plane_registries() {
        let lib_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
        let executor_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/execution/executor.rs");
        let lib = std::fs::read_to_string(lib_path).expect("lib source should be readable");
        let executor =
            std::fs::read_to_string(executor_path).expect("executor source should be readable");

        for forbidden in [
            ["fn ", "global_worker_registry"].concat(),
            ["fn ", "global_team_registry"].concat(),
            ["fn ", "global_cron_registry"].concat(),
            ["fn ", "global_task_registry"].concat(),
            ["WorkerRegistry", "::new"].concat(),
            ["TeamRegistry", "::new"].concat(),
            ["CronRegistry", "::new"].concat(),
            ["TaskRegistry", "::new"].concat(),
        ] {
            assert!(
                !lib.contains(&forbidden) && !executor.contains(&forbidden),
                "tools must not own runtime control-plane registry `{forbidden}`"
            );
        }
        assert!(
            !executor.contains(&["runtime", "::", "global_runtime_control_plane()"].concat()),
            "tools executor must not call runtime control-plane directly"
        );
        assert!(
            !executor.contains(&["runtime", "::", "global_task_registry()"].concat()),
            "tools executor must not call task registry directly"
        );
    }

    #[test]
    fn agent_control_plane_tool_is_not_executable_from_tools() {
        let error = execute_tool(
            "Agent",
            &json!({
                "description": "Inspect branch",
                "prompt": "Inspect"
            }),
        )
        .expect_err("control-plane Agent tool should not be executable from tools");
        assert!(error.contains("unsupported tool"));
    }

    #[test]
    fn notebook_edit_replaces_inserts_and_deletes_cells() {
        let path = temp_path("notebook.ipynb");
        let root = path.parent().expect("notebook parent");
        std::fs::write(
            &path,
            r#"{
  "cells": [
    {"cell_type": "code", "id": "cell-a", "metadata": {}, "source": ["print(1)\n"], "outputs": [], "execution_count": null}
  ],
  "metadata": {"kernelspec": {"language": "python"}},
  "nbformat": 4,
  "nbformat_minor": 5
}"#,
        )
        .expect("write notebook");

        let replaced = execute_in_workspace(
            root,
            "NotebookEdit",
            &json!({
                "notebook_path": path.display().to_string(),
                "cell_id": "cell-a",
                "new_source": "print(2)\n",
                "edit_mode": "replace"
            }),
        )
        .expect("NotebookEdit replace should succeed");
        let replaced_output: serde_json::Value = serde_json::from_str(&replaced).expect("json");
        assert_eq!(replaced_output["cell_id"], "cell-a");
        assert_eq!(replaced_output["cell_type"], "code");

        let inserted = execute_in_workspace(
            root,
            "NotebookEdit",
            &json!({
                "notebook_path": path.display().to_string(),
                "cell_id": "cell-a",
                "new_source": "# heading\n",
                "cell_type": "markdown",
                "edit_mode": "insert"
            }),
        )
        .expect("NotebookEdit insert should succeed");
        let inserted_output: serde_json::Value = serde_json::from_str(&inserted).expect("json");
        assert_eq!(inserted_output["cell_type"], "markdown");
        let appended = execute_in_workspace(
            root,
            "NotebookEdit",
            &json!({
                "notebook_path": path.display().to_string(),
                "new_source": "print(3)\n",
                "edit_mode": "insert"
            }),
        )
        .expect("NotebookEdit append should succeed");
        let appended_output: serde_json::Value = serde_json::from_str(&appended).expect("json");
        assert_eq!(appended_output["cell_type"], "code");

        let deleted = execute_in_workspace(
            root,
            "NotebookEdit",
            &json!({
                "notebook_path": path.display().to_string(),
                "cell_id": "cell-a",
                "edit_mode": "delete"
            }),
        )
        .expect("NotebookEdit delete should succeed without new_source");
        let deleted_output: serde_json::Value = serde_json::from_str(&deleted).expect("json");
        assert!(deleted_output["cell_type"].is_null());
        assert_eq!(deleted_output["new_source"], "");

        let final_notebook: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read notebook"))
                .expect("valid notebook json");
        let cells = final_notebook["cells"].as_array().expect("cells array");
        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0]["cell_type"], "markdown");
        assert!(cells[0].get("outputs").is_none());
        assert_eq!(cells[1]["cell_type"], "code");
        assert_eq!(cells[1]["source"][0], "print(3)\n");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn notebook_edit_rejects_invalid_inputs() {
        let text_path = temp_path("notebook.txt");
        let root = text_path.parent().expect("notebook parent");
        fs::write(&text_path, "not a notebook").expect("write text file");
        let wrong_extension = execute_in_workspace(
            root,
            "NotebookEdit",
            &json!({
                "notebook_path": text_path.display().to_string(),
                "new_source": "print(1)\n"
            }),
        )
        .expect_err("non-ipynb file should fail");
        assert!(wrong_extension.contains("Jupyter notebook"));
        let _ = fs::remove_file(&text_path);

        let empty_notebook = temp_path("empty.ipynb");
        fs::write(
            &empty_notebook,
            r#"{"cells":[],"metadata":{"kernelspec":{"language":"python"}},"nbformat":4,"nbformat_minor":5}"#,
        )
        .expect("write empty notebook");

        let missing_source = execute_in_workspace(
            root,
            "NotebookEdit",
            &json!({
                "notebook_path": empty_notebook.display().to_string(),
                "edit_mode": "insert"
            }),
        )
        .expect_err("insert without source should fail");
        assert!(missing_source.contains("new_source is required"));

        let missing_cell = execute_in_workspace(
            root,
            "NotebookEdit",
            &json!({
                "notebook_path": empty_notebook.display().to_string(),
                "edit_mode": "delete"
            }),
        )
        .expect_err("delete on empty notebook should fail");
        assert!(missing_cell.contains("Notebook has no cells to edit"));
        let _ = fs::remove_file(empty_notebook);
    }

    #[test]
    fn bash_tool_reports_success_exit_failure_timeout_and_background() {
        let root = temp_path("bash-tool-cwd");
        fs::create_dir_all(&root).expect("bash cwd should exist");
        let cwd = root.to_string_lossy().to_string();

        let success = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "printf 'hello'", "cwd": cwd, "dangerouslyDisableSandbox": true }),
        )
        .expect("bash should succeed");
        let success_output: serde_json::Value = serde_json::from_str(&success).expect("json");
        assert_eq!(success_output["stdout"], "hello");
        assert_eq!(success_output["interrupted"], false);

        let failure = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "printf 'oops' >&2; exit 7", "cwd": cwd, "dangerouslyDisableSandbox": true }),
        )
        .expect("bash failure should still return structured output");
        let failure_output: serde_json::Value = serde_json::from_str(&failure).expect("json");
        assert_eq!(failure_output["returnCodeInterpretation"], "exit_code:7");
        assert!(failure_output["stderr"]
            .as_str()
            .expect("stderr")
            .contains("oops"));

        let timeout = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "sleep 1", "cwd": cwd, "timeout": 10, "dangerouslyDisableSandbox": true }),
        )
        .expect("bash timeout should return output");
        let timeout_output: serde_json::Value = serde_json::from_str(&timeout).expect("json");
        assert_eq!(timeout_output["interrupted"], true);
        assert_eq!(timeout_output["returnCodeInterpretation"], "timeout");
        assert!(timeout_output["stderr"]
            .as_str()
            .expect("stderr")
            .contains("Command exceeded timeout"));

        let background = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "sleep 1", "cwd": cwd, "run_in_background": true, "dangerouslyDisableSandbox": true }),
        )
        .expect("bash background should succeed");
        let background_output: serde_json::Value = serde_json::from_str(&background).expect("json");
        assert!(background_output["backgroundTaskId"].as_str().is_some());
        assert_eq!(background_output["noOutputExpected"], true);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bash_workspace_tests_are_blocked_when_branch_is_behind_main() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("workspace-test-preflight");
        let original_dir = std::env::current_dir().expect("cwd");
        init_git_repo(&root);
        run_git(&root, &["checkout", "-b", "feature/stale-tests"]);
        run_git(&root, &["checkout", "main"]);
        commit_file(
            &root,
            "hotfix.txt",
            "fix from main\n",
            "fix: unblock workspace tests",
        );
        run_git(&root, &["checkout", "feature/stale-tests"]);
        std::env::set_current_dir(&root).expect("set cwd");

        let output = execute_tool(
            "bash",
            &json!({ "command": "cargo test --workspace --all-targets" }),
        )
        .expect("preflight should return structured output");
        let output_json: serde_json::Value = serde_json::from_str(&output).expect("json");
        assert_eq!(
            output_json["returnCodeInterpretation"],
            "preflight_blocked:branch_divergence"
        );
        assert!(output_json["stderr"]
            .as_str()
            .expect("stderr")
            .contains("branch divergence detected before workspace tests"));
        assert_eq!(
            output_json["structuredContent"][0]["event"],
            "branch.stale_against_main"
        );
        assert_eq!(
            output_json["structuredContent"][0]["failureClass"],
            "branch_divergence"
        );
        assert_eq!(
            output_json["structuredContent"][0]["data"]["missingCommits"][0],
            "fix: unblock workspace tests"
        );

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn bash_targeted_tests_skip_branch_preflight() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("targeted-test-no-preflight");
        let original_dir = std::env::current_dir().expect("cwd");
        init_git_repo(&root);
        run_git(&root, &["checkout", "-b", "feature/targeted-tests"]);
        run_git(&root, &["checkout", "main"]);
        commit_file(
            &root,
            "hotfix.txt",
            "fix from main\n",
            "fix: only broad tests should block",
        );
        run_git(&root, &["checkout", "feature/targeted-tests"]);
        std::env::set_current_dir(&root).expect("set cwd");

        let output = execute_tool(
            "bash",
            &json!({ "command": "printf 'targeted ok'; cargo test -p runtime stale_branch" }),
        )
        .expect("targeted commands should still execute");
        let output_json: serde_json::Value = serde_json::from_str(&output).expect("json");
        assert_ne!(
            output_json["returnCodeInterpretation"],
            "preflight_blocked:branch_divergence"
        );

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn file_tools_cover_read_write_and_edit_behaviors() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("fs-suite");
        fs::create_dir_all(&root).expect("create root");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        let write_create = execute_tool(
            "write_file",
            &json!({ "path": "nested/demo.txt", "content": "alpha\nbeta\nalpha\n" }),
        )
        .expect("write create should succeed");
        let write_create_output: serde_json::Value =
            serde_json::from_str(&write_create).expect("json");
        assert_eq!(write_create_output["type"], "create");
        assert!(root.join("nested/demo.txt").exists());

        let write_update = execute_tool(
            "write_file",
            &json!({ "path": "nested/demo.txt", "content": "alpha\nbeta\ngamma\n" }),
        )
        .expect("write update should succeed");
        let write_update_output: serde_json::Value =
            serde_json::from_str(&write_update).expect("json");
        assert_eq!(write_update_output["type"], "update");
        assert_eq!(write_update_output["originalFile"], "alpha\nbeta\nalpha\n");

        let read_full = execute_tool("read_file", &json!({ "path": "nested/demo.txt" }))
            .expect("read full should succeed");
        let read_full_output: serde_json::Value = serde_json::from_str(&read_full).expect("json");
        assert_eq!(read_full_output["file"]["content"], "alpha\nbeta\ngamma");
        assert_eq!(read_full_output["file"]["startLine"], 1);
        assert_eq!(read_full_output["file"]["byteLength"], 17);
        assert_eq!(read_full_output["file"]["endsWithNewline"], true);
        assert_eq!(
            read_full_output["file"]["sha256"],
            "4fdbc441ea7b546100e086ac1e4fc5ae6749b7314311c99db05be450eca12996"
        );

        let read_slice = execute_tool(
            "read_file",
            &json!({ "path": "nested/demo.txt", "offset": 1, "limit": 1 }),
        )
        .expect("read slice should succeed");
        let read_slice_output: serde_json::Value = serde_json::from_str(&read_slice).expect("json");
        assert_eq!(read_slice_output["file"]["content"], "beta");
        assert_eq!(read_slice_output["file"]["startLine"], 2);

        let read_past_end = execute_tool(
            "read_file",
            &json!({ "path": "nested/demo.txt", "offset": 50 }),
        )
        .expect("read past EOF should succeed");
        let read_past_end_output: serde_json::Value =
            serde_json::from_str(&read_past_end).expect("json");
        assert_eq!(read_past_end_output["file"]["content"], "");
        assert_eq!(read_past_end_output["file"]["startLine"], 4);

        let read_error = execute_tool("read_file", &json!({ "path": "missing.txt" }))
            .expect_err("missing file should fail");
        assert!(!read_error.is_empty());

        let edit_once = execute_tool(
            "edit_file",
            &json!({ "path": "nested/demo.txt", "old_string": "alpha", "new_string": "omega" }),
        )
        .expect("single edit should succeed");
        let edit_once_output: serde_json::Value = serde_json::from_str(&edit_once).expect("json");
        assert_eq!(edit_once_output["replaceAll"], false);
        assert_eq!(
            fs::read_to_string(root.join("nested/demo.txt")).expect("read file"),
            "omega\nbeta\ngamma\n"
        );

        execute_tool(
            "write_file",
            &json!({ "path": "nested/demo.txt", "content": "alpha\nbeta\nalpha\n" }),
        )
        .expect("reset file");
        let edit_all = execute_tool(
            "edit_file",
            &json!({
                "path": "nested/demo.txt",
                "old_string": "alpha",
                "new_string": "omega",
                "replace_all": true
            }),
        )
        .expect("replace all should succeed");
        let edit_all_output: serde_json::Value = serde_json::from_str(&edit_all).expect("json");
        assert_eq!(edit_all_output["replaceAll"], true);
        assert_eq!(
            fs::read_to_string(root.join("nested/demo.txt")).expect("read file"),
            "omega\nbeta\nomega\n"
        );

        let edit_same = execute_tool(
            "edit_file",
            &json!({ "path": "nested/demo.txt", "old_string": "omega", "new_string": "omega" }),
        )
        .expect_err("identical old/new should fail");
        assert!(edit_same.contains("must differ"));

        let edit_missing = execute_tool(
            "edit_file",
            &json!({ "path": "nested/demo.txt", "old_string": "missing", "new_string": "omega" }),
        )
        .expect_err("missing substring should fail");
        assert!(edit_missing.contains("old_string not found"));

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn glob_and_grep_tools_cover_success_and_errors() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("search-suite");
        fs::create_dir_all(root.join("nested")).expect("create root");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        fs::write(
            root.join("nested/lib.rs"),
            "fn main() {}\nlet alpha = 1;\nlet alpha = 2;\n",
        )
        .expect("write rust file");
        fs::write(root.join("nested/notes.txt"), "alpha\nbeta\n").expect("write txt file");

        let globbed = execute_tool("glob_search", &json!({ "pattern": "nested/*.rs" }))
            .expect("glob should succeed");
        let globbed_output: serde_json::Value = serde_json::from_str(&globbed).expect("json");
        assert_eq!(globbed_output["numFiles"], 1);
        assert!(globbed_output["filenames"][0]
            .as_str()
            .expect("filename")
            .ends_with("nested/lib.rs"));

        let glob_error = execute_tool("glob_search", &json!({ "pattern": "[" }))
            .expect_err("invalid glob should fail");
        assert!(!glob_error.is_empty());

        let grep_content = execute_tool(
            "grep_search",
            &json!({
                "pattern": "alpha",
                "path": "nested",
                "glob": "*.rs",
                "output_mode": "content",
                "-n": true,
                "head_limit": 1,
                "offset": 1
            }),
        )
        .expect("grep content should succeed");
        let grep_content_output: serde_json::Value =
            serde_json::from_str(&grep_content).expect("json");
        assert_eq!(grep_content_output["numFiles"], 0);
        assert!(grep_content_output["appliedLimit"].is_null());
        assert_eq!(grep_content_output["appliedOffset"], 1);
        assert!(grep_content_output["content"]
            .as_str()
            .expect("content")
            .contains("let alpha = 2;"));

        let grep_count = execute_tool(
            "grep_search",
            &json!({ "pattern": "alpha", "path": "nested", "output_mode": "count" }),
        )
        .expect("grep count should succeed");
        let grep_count_output: serde_json::Value = serde_json::from_str(&grep_count).expect("json");
        assert_eq!(grep_count_output["numMatches"], 3);

        let grep_error = execute_tool(
            "grep_search",
            &json!({ "pattern": "(alpha", "path": "nested" }),
        )
        .expect_err("invalid regex should fail");
        assert!(!grep_error.is_empty());

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_many_preserves_order_and_reports_partial_failures() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("read-many-suite");
        fs::create_dir_all(root.join("nested")).expect("create root");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        fs::write(root.join("nested/a.txt"), "alpha\nbeta\n").expect("write a");
        fs::write(root.join("nested/b.txt"), "gamma\n").expect("write b");

        let output = execute_tool(
            "read_many",
            &json!({
                "files": [
                    { "path": "nested/a.txt", "offset": 1, "limit": 1 },
                    { "path": "missing.txt" },
                    { "path": "nested/b.txt" }
                ],
                "max_concurrency": 2
            }),
        )
        .expect("read_many should return structured batch output");
        let value: serde_json::Value = serde_json::from_str(&output).expect("json");

        assert_eq!(value["type"], "read_many");
        assert_eq!(value["count"], 3);
        assert_eq!(value["successCount"], 2);
        assert_eq!(value["errorCount"], 1);
        assert_eq!(value["partialSuccess"], true);
        assert_eq!(value["results"][0]["index"], 0);
        assert_eq!(value["results"][0]["status"], "success");
        assert_eq!(value["results"][0]["output"]["file"]["content"], "beta");
        assert_eq!(value["results"][1]["index"], 1);
        assert_eq!(value["results"][1]["status"], "error");
        assert_eq!(value["results"][2]["index"], 2);
        assert_eq!(value["results"][2]["output"]["file"]["content"], "gamma");

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_tool_cache_hits_and_invalidates_after_write() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("tool-cache-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        let file = root.join("src/lib.rs");
        fs::write(&file, "alpha\n").expect("write file");
        let host = crate::ToolHost::builtin("tool-cache-suite", &root);
        let lease = host.pin_snapshot();

        super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
            .expect("first read");
        super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
            .expect("second read");
        let stats =
            super::execute_with_lease(&lease, "tool_cache_stats", &json!({})).expect("stats");
        let stats_value: serde_json::Value = serde_json::from_str(&stats).expect("json");
        assert_eq!(stats_value["hits"], 1);
        assert_eq!(stats_value["entries"], 1);

        super::execute_with_lease(
            &lease,
            "write_file",
            &json!({ "path": "src/lib.rs", "content": "omega\n" }),
        )
        .expect("write invalidates cache");
        let stats = super::execute_with_lease(&lease, "tool_cache_stats", &json!({}))
            .expect("stats after write");
        let stats_value: serde_json::Value = serde_json::from_str(&stats).expect("json");
        assert_eq!(stats_value["invalidations"], 1);
        assert_eq!(stats_value["scopeEpochs"], 1);
        let reread =
            super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
                .expect("reread should not use stale cache");
        assert!(reread.contains("omega"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_tool_cache_misses_after_external_file_change() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("tool-cache-external-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        let file = root.join("src/lib.rs");
        fs::write(&file, "alpha\n").expect("write file");
        let host = crate::ToolHost::builtin("tool-cache-external-suite", &root);
        let lease = host.pin_snapshot();

        let first =
            super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
                .expect("first");
        assert!(first.contains("alpha"));
        fs::write(&file, "omega\n").expect("external write");
        let second =
            super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
                .expect("second");
        assert!(second.contains("omega"));
        let stats =
            super::execute_with_lease(&lease, "tool_cache_stats", &json!({})).expect("stats");
        let stats_value: serde_json::Value = serde_json::from_str(&stats).expect("json");
        assert_eq!(stats_value["hits"], 0);
        assert_eq!(stats_value["misses"], 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn auto_checkpoint_can_guard_mutations_when_enabled() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("auto-checkpoint-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        fs::write(root.join("src/lib.rs"), "alpha\n").expect("write file");
        let original_dir = std::env::current_dir().expect("cwd");
        let original_auto_checkpoint = std::env::var("COWD_AUTO_CHECKPOINT").ok();
        std::env::set_current_dir(&root).expect("set cwd");
        std::env::set_var("COWD_AUTO_CHECKPOINT", "1");

        execute_tool(
            "write_file",
            &json!({ "path": "src/lib.rs", "content": "omega\n" }),
        )
        .expect("write should create checkpoint first");
        let checkpoints = execute_tool("checkpoint_list", &json!({})).expect("list checkpoints");
        let value: serde_json::Value = serde_json::from_str(&checkpoints).expect("json");
        let labels = value["checkpoints"]
            .as_array()
            .expect("checkpoints")
            .iter()
            .filter_map(|checkpoint| checkpoint["label"].as_str())
            .collect::<Vec<_>>();
        assert!(labels.contains(&"auto-before-write_file"));

        match original_auto_checkpoint {
            Some(value) => std::env::set_var("COWD_AUTO_CHECKPOINT", value),
            None => std::env::remove_var("COWD_AUTO_CHECKPOINT"),
        }
        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn mutation_preview_and_apply_patch_transaction_cover_conflict_and_success() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("mutation-transaction-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        let file = root.join("src/lib.rs");
        fs::write(&file, "alpha\nbeta\n").expect("write file");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        let preview = execute_tool(
            "mutation_preview",
            &json!({
                "edits": [
                    { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
                ]
            }),
        )
        .expect("mutation preview should succeed");
        let preview_value: serde_json::Value = serde_json::from_str(&preview).expect("json");
        assert_eq!(preview_value["type"], "mutation_preview");
        assert_eq!(preview_value["conflictCount"], 0);
        let expected_hash = preview_value["files"][0]["expectedHash"]
            .as_str()
            .expect("expected hash")
            .to_string();

        let applied = execute_tool(
            "apply_patch_transaction",
            &json!({
                "edits": [
                    { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
                ],
                "expected_hashes": {
                    "src/lib.rs": expected_hash
                }
            }),
        )
        .expect("apply should succeed");
        let applied_value: serde_json::Value = serde_json::from_str(&applied).expect("json");
        assert_eq!(applied_value["type"], "mutation_apply");
        assert_eq!(
            fs::read_to_string(&file).expect("read file"),
            "omega\nbeta\n"
        );

        fs::write(&file, "alpha\nalpha\n").expect("reset file");
        let conflict = execute_tool(
            "patch_plan",
            &json!({
                "edits": [
                    { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
                ]
            }),
        )
        .expect("patch plan should return conflict report");
        let conflict_value: serde_json::Value = serde_json::from_str(&conflict).expect("json");
        assert_eq!(conflict_value["conflictCount"], 1);

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_patch_transaction_rejects_stale_expected_hash() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("mutation-stale-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        let file = root.join("src/lib.rs");
        fs::write(&file, "alpha\n").expect("write file");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        let err = execute_tool(
            "apply_patch_transaction",
            &json!({
                "edits": [
                    { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
                ],
                "expected_hashes": {
                    "src/lib.rs": "stale"
                }
            }),
        )
        .expect_err("stale hash should fail");
        assert!(err.contains("changed before apply"));
        assert_eq!(fs::read_to_string(&file).expect("read file"), "alpha\n");

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn checkpoint_tools_create_diff_and_restore_workspace_files() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("checkpoint-suite");
        let unrelated_cwd = temp_path("checkpoint-unrelated-cwd");
        fs::create_dir_all(root.join("src")).expect("create root");
        fs::create_dir_all(&unrelated_cwd).expect("create unrelated cwd");
        let file = root.join("src/lib.rs");
        fs::write(&file, "alpha\n").expect("write file");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&unrelated_cwd).expect("set unrelated cwd");

        let created = execute_in_workspace(
            &root,
            "checkpoint_create",
            &json!({ "label": "before edit" }),
        )
        .expect("checkpoint create should succeed");
        let created_value: serde_json::Value = serde_json::from_str(&created).expect("json");
        let checkpoint_id = created_value["id"]
            .as_str()
            .expect("checkpoint id")
            .to_string();
        assert!(root.join(".cowd/checkpoints").is_dir());
        assert!(
            !unrelated_cwd.join(".cowd/checkpoints").exists(),
            "checkpoint state must remain in the leased workspace rather than process cwd"
        );

        fs::write(&file, "omega\n").expect("mutate file");
        fs::write(root.join("src/new.rs"), "new\n").expect("add file");
        fs::remove_file(&file).expect("delete file");
        let diff = execute_in_workspace(&root, "checkpoint_diff", &json!({ "id": checkpoint_id }))
            .expect("checkpoint diff should succeed");
        let diff_value: serde_json::Value = serde_json::from_str(&diff).expect("json");
        assert_eq!(diff_value["type"], "checkpoint_diff");
        assert!(diff_value["deletedFiles"]
            .as_array()
            .expect("deleted files")
            .iter()
            .any(|file| file.as_str() == Some("src/lib.rs")));
        assert!(diff_value["addedFiles"]
            .as_array()
            .expect("added files")
            .iter()
            .any(|file| file.as_str() == Some("src/new.rs")));

        let checkpoint_id = created_value["id"].as_str().expect("checkpoint id");
        execute_in_workspace(&root, "checkpoint_restore", &json!({ "id": checkpoint_id }))
            .expect("checkpoint restore should succeed");
        assert_eq!(fs::read_to_string(&file).expect("read restored"), "alpha\n");
        assert!(!root.join("src/new.rs").exists());

        let listed =
            execute_in_workspace(&root, "checkpoint_list", &json!({})).expect("checkpoint list");
        let listed_value: serde_json::Value = serde_json::from_str(&listed).expect("json");
        assert_eq!(listed_value["type"], "checkpoint_list");
        assert!(!listed_value["checkpoints"]
            .as_array()
            .expect("checkpoints")
            .is_empty());

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(unrelated_cwd);
    }

    #[test]
    fn glob_many_and_grep_many_preserve_order() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("search-many-suite");
        fs::create_dir_all(root.join("nested")).expect("create root");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        fs::write(root.join("nested/lib.rs"), "let alpha = 1;\n").expect("write rs");
        fs::write(root.join("nested/notes.md"), "alpha\nbeta\n").expect("write md");

        let globbed = execute_tool(
            "glob_many",
            &json!({
                "patterns": [
                    { "pattern": "nested/*.rs" },
                    { "pattern": "[" },
                    { "pattern": "nested/*.md" }
                ],
                "max_concurrency": 2
            }),
        )
        .expect("glob_many should return structured batch output");
        let globbed_value: serde_json::Value = serde_json::from_str(&globbed).expect("json");
        assert_eq!(globbed_value["successCount"], 2);
        assert_eq!(globbed_value["errorCount"], 1);
        assert_eq!(globbed_value["results"][0]["index"], 0);
        assert_eq!(globbed_value["results"][1]["status"], "error");
        assert_eq!(globbed_value["results"][2]["index"], 2);

        let grepped = execute_tool(
            "grep_many",
            &json!({
                "searches": [
                    { "pattern": "alpha", "path": "nested", "glob": "*.rs" },
                    { "pattern": "(alpha", "path": "nested" },
                    { "pattern": "beta", "path": "nested", "output_mode": "content" }
                ],
                "max_concurrency": 2
            }),
        )
        .expect("grep_many should return structured batch output");
        let grepped_value: serde_json::Value = serde_json::from_str(&grepped).expect("json");
        assert_eq!(grepped_value["successCount"], 2);
        assert_eq!(grepped_value["errorCount"], 1);
        assert_eq!(grepped_value["results"][0]["index"], 0);
        assert_eq!(grepped_value["results"][1]["status"], "error");
        assert_eq!(grepped_value["results"][2]["index"], 2);
        assert!(grepped_value["results"][2]["output"]["content"]
            .as_str()
            .expect("content")
            .contains("beta"));

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn workspace_snapshot_reports_compact_read_only_state() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("workspace-snapshot-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        fs::write(root.join("src/main.rs"), "fn main() {}\n").expect("write file");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        let output = execute_tool(
            "workspace_snapshot",
            &json!({
                "include_git": false,
                "include_files": true,
                "roots": ["src"],
                "max_files": 10
            }),
        )
        .expect("workspace_snapshot should succeed");
        let value: serde_json::Value = serde_json::from_str(&output).expect("json");
        assert_eq!(value["type"], "workspace_snapshot");
        assert!(value["git"].is_null());
        assert!(value["files"]
            .as_array()
            .expect("files")
            .iter()
            .any(|file| file
                .as_str()
                .is_some_and(|path| path.ends_with("src/main.rs"))));

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tool_batch_readonly_runs_allowed_calls_in_order() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("tool-batch-readonly-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        fs::write(root.join("src/lib.rs"), "pub fn alpha() {}\n").expect("write rs");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        let output = execute_tool(
            "tool_batch_readonly",
            &json!({
                "calls": [
                    { "name": "read_file", "input": { "path": "src/lib.rs" } },
                    { "name": "grep_search", "input": { "pattern": "alpha", "path": "src" } },
                    { "name": "glob_search", "input": { "pattern": "src/*.rs" } }
                ],
                "max_concurrency": 3
            }),
        )
        .expect("tool_batch_readonly should succeed");
        let value: serde_json::Value = serde_json::from_str(&output).expect("json");
        assert_eq!(value["type"], "tool_batch_readonly");
        assert_eq!(value["executionMode"], "prepared_readonly");
        assert_eq!(value["successCount"], 3);
        assert_eq!(value["errorCount"], 0);
        assert_eq!(value["results"][0]["index"], 0);
        assert_eq!(
            value["results"][0]["output"]["file"]["content"],
            "pub fn alpha() {}"
        );
        assert_eq!(value["results"][1]["index"], 1);
        assert_eq!(value["results"][2]["index"], 2);

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tool_batch_readonly_falls_back_for_readonly_aggregate_tools() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("tool-batch-readonly-compat-suite");
        fs::create_dir_all(root.join("src")).expect("create root");
        fs::write(root.join("src/a.rs"), "alpha\n").expect("write a");
        fs::write(root.join("src/b.rs"), "beta\n").expect("write b");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        let output = execute_tool(
            "tool_batch_readonly",
            &json!({
                "calls": [
                    {
                        "name": "read_many",
                        "input": {
                            "files": [
                                { "path": "src/a.rs" },
                                { "path": "src/b.rs" }
                            ],
                            "max_concurrency": 2
                        }
                    }
                ]
            }),
        )
        .expect("tool_batch_readonly should keep aggregate compatibility");
        let value: serde_json::Value = serde_json::from_str(&output).expect("json");
        assert_eq!(value["executionMode"], "compat_recursive");
        assert_eq!(value["successCount"], 1);
        assert_eq!(value["results"][0]["output"]["type"], "read_many");

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tool_batch_readonly_rejects_non_readonly_tools_before_execution() {
        let output = execute_tool(
            "tool_batch_readonly",
            &json!({
                "calls": [
                    { "name": "read_file", "input": { "path": "Cargo.toml" } },
                    { "name": "write_file", "input": { "path": "should-not-exist.txt", "content": "no" } }
                ]
            }),
        )
        .expect_err("write_file must be rejected");
        assert!(output.contains("write_file"));
        assert!(output.contains("not allowed"));
    }

    #[test]
    fn sleep_waits_and_reports_duration() {
        let started = std::time::Instant::now();
        let result =
            execute_tool("Sleep", &json!({"duration_ms": 20})).expect("Sleep should succeed");
        let elapsed = started.elapsed();
        let output: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(output["duration_ms"], 20);
        assert!(output["message"]
            .as_str()
            .expect("message")
            .contains("Slept for 20ms"));
        assert!(elapsed >= Duration::from_millis(15));
    }

    #[test]
    fn given_excessive_duration_when_sleep_then_rejects_with_error() {
        let result = execute_tool("Sleep", &json!({"duration_ms": 999_999_999_u64}));
        let error = result.expect_err("excessive sleep should fail");
        assert!(error.contains("exceeds maximum allowed sleep"));
    }

    #[test]
    fn given_zero_duration_when_sleep_then_succeeds() {
        let result =
            execute_tool("Sleep", &json!({"duration_ms": 0})).expect("0ms sleep should succeed");
        let output: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(output["duration_ms"], 0);
    }

    #[test]
    fn brief_returns_sent_message_and_attachment_metadata() {
        let attachment = std::env::temp_dir().join(format!(
            "cowd-brief-{}.png",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::write(&attachment, b"png-data").expect("write attachment");
        let root = attachment.parent().expect("attachment parent");

        let result = execute_in_workspace(
            root,
            "SendUserMessage",
            &json!({
                "message": "hello user",
                "attachments": [attachment.display().to_string()],
                "status": "normal"
            }),
        )
        .expect("SendUserMessage should succeed");

        let output: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(output["message"], "hello user");
        assert!(output["sentAt"].as_str().is_some());
        assert_eq!(output["attachments"][0]["isImage"], true);
        let _ = std::fs::remove_file(attachment);
    }

    #[test]
    fn config_reads_and_writes_supported_values() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = std::env::temp_dir().join(format!(
            "cowd-config-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let home = root.join("home");
        let cwd = root.join("cwd");
        std::fs::create_dir_all(home.join(".cowd")).expect("home dir");
        std::fs::create_dir_all(cwd.join(".cowd")).expect("cwd dir");
        std::fs::write(
            home.join(".cowd").join("config.yaml"),
            r#"{"verbose":false}"#,
        )
        .expect("write global config");

        let original_home = std::env::var("HOME").ok();
        let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_var("HOME", &home);
        std::env::remove_var("COWD_CONFIG_HOME");
        std::env::set_current_dir(&cwd).expect("set cwd");

        let get = execute_tool("Config", &json!({"setting": "verbose"})).expect("get config");
        let get_output: serde_json::Value = serde_json::from_str(&get).expect("json");
        assert_eq!(get_output["value"], false);

        let set = execute_tool(
            "Config",
            &json!({"setting": "permissions.defaultMode", "value": "plan"}),
        )
        .expect("set config");
        let set_output: serde_json::Value = serde_json::from_str(&set).expect("json");
        assert_eq!(set_output["operation"], "set");
        assert_eq!(set_output["newValue"], "plan");

        let invalid = execute_tool(
            "Config",
            &json!({"setting": "permissions.defaultMode", "value": "bogus"}),
        )
        .expect_err("invalid config value should error");
        assert!(invalid.contains("Invalid value"));

        let unknown =
            execute_tool("Config", &json!({"setting": "nope"})).expect("unknown setting result");
        let unknown_output: serde_json::Value = serde_json::from_str(&unknown).expect("json");
        assert_eq!(unknown_output["success"], false);

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        match original_home {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
        match original_config_home {
            Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
            None => std::env::remove_var("COWD_CONFIG_HOME"),
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn enter_and_exit_plan_mode_round_trip_existing_local_override() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = std::env::temp_dir().join(format!(
            "cowd-plan-mode-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let home = root.join("home");
        let cwd = root.join("cwd");
        std::fs::create_dir_all(home.join(".cowd")).expect("home dir");
        std::fs::create_dir_all(cwd.join(".cowd")).expect("cwd dir");
        std::fs::write(
            cwd.join(".cowd").join("config.local.yaml"),
            r#"{"permissions":{"defaultMode":"acceptEdits"}}"#,
        )
        .expect("write local config");

        let original_home = std::env::var("HOME").ok();
        let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_var("HOME", &home);
        std::env::remove_var("COWD_CONFIG_HOME");
        std::env::set_current_dir(&cwd).expect("set cwd");

        let enter = execute_tool("EnterPlanMode", &json!({})).expect("enter plan mode");
        let enter_output: serde_json::Value = serde_json::from_str(&enter).expect("json");
        assert_eq!(enter_output["changed"], true);
        assert_eq!(enter_output["managed"], true);
        assert_eq!(enter_output["previousLocalMode"], "acceptEdits");
        assert_eq!(enter_output["currentLocalMode"], "plan");

        let local_settings = std::fs::read_to_string(cwd.join(".cowd").join("config.local.yaml"))
            .expect("local config after enter");
        assert!(local_settings.contains(r#""defaultMode": "plan""#));
        let state =
            std::fs::read_to_string(cwd.join(".cowd").join("tool-state").join("plan-mode.json"))
                .expect("plan mode state");
        assert!(state.contains(r#""hadLocalOverride": true"#));
        assert!(state.contains(r#""previousLocalMode": "acceptEdits""#));

        let exit = execute_tool("ExitPlanMode", &json!({})).expect("exit plan mode");
        let exit_output: serde_json::Value = serde_json::from_str(&exit).expect("json");
        assert_eq!(exit_output["changed"], true);
        assert_eq!(exit_output["managed"], false);
        assert_eq!(exit_output["previousLocalMode"], "acceptEdits");
        assert_eq!(exit_output["currentLocalMode"], "acceptEdits");

        let local_settings = std::fs::read_to_string(cwd.join(".cowd").join("config.local.yaml"))
            .expect("local settings after exit");
        assert!(local_settings.contains(r#""defaultMode": "acceptEdits""#));
        assert!(!cwd
            .join(".cowd")
            .join("tool-state")
            .join("plan-mode.json")
            .exists());

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        match original_home {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
        match original_config_home {
            Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
            None => std::env::remove_var("COWD_CONFIG_HOME"),
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn exit_plan_mode_clears_override_when_enter_created_it_from_empty_local_state() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = std::env::temp_dir().join(format!(
            "cowd-plan-mode-empty-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let home = root.join("home");
        let cwd = root.join("cwd");
        std::fs::create_dir_all(home.join(".cowd")).expect("home dir");
        std::fs::create_dir_all(cwd.join(".cowd")).expect("cwd dir");

        let original_home = std::env::var("HOME").ok();
        let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_var("HOME", &home);
        std::env::remove_var("COWD_CONFIG_HOME");
        std::env::set_current_dir(&cwd).expect("set cwd");

        let enter = execute_tool("EnterPlanMode", &json!({})).expect("enter plan mode");
        let enter_output: serde_json::Value = serde_json::from_str(&enter).expect("json");
        assert_eq!(enter_output["previousLocalMode"], serde_json::Value::Null);
        assert_eq!(enter_output["currentLocalMode"], "plan");

        let exit = execute_tool("ExitPlanMode", &json!({})).expect("exit plan mode");
        let exit_output: serde_json::Value = serde_json::from_str(&exit).expect("json");
        assert_eq!(exit_output["changed"], true);
        assert_eq!(exit_output["currentLocalMode"], serde_json::Value::Null);

        let local_settings = std::fs::read_to_string(cwd.join(".cowd").join("config.local.yaml"))
            .expect("local config after exit");
        let local_settings_json: serde_json::Value =
            serde_json::from_str(&local_settings).expect("valid config json");
        assert_eq!(
            local_settings_json.get("permissions"),
            None,
            "permissions override should be removed on exit"
        );
        assert!(!cwd
            .join(".cowd")
            .join("tool-state")
            .join("plan-mode.json")
            .exists());

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        match original_home {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
        match original_config_home {
            Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
            None => std::env::remove_var("COWD_CONFIG_HOME"),
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn structured_output_echoes_input_payload() {
        let result = execute_tool("StructuredOutput", &json!({"ok": true, "items": [1, 2, 3]}))
            .expect("StructuredOutput should succeed");
        let output: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(output["data"], "Structured output provided successfully");
        assert_eq!(output["structured_output"]["ok"], true);
        assert_eq!(output["structured_output"]["items"][1], 2);
    }

    #[test]
    fn given_empty_payload_when_structured_output_then_rejects_with_error() {
        let result = execute_tool("StructuredOutput", &json!({}));
        let error = result.expect_err("empty payload should fail");
        assert!(error.contains("must not be empty"));
    }

    #[test]
    fn repl_executes_python_code() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_tool(
            "REPL",
            &json!({"language": "python", "code": "print(1 + 1)", "timeout_ms": 500}),
        )
        .expect("REPL should succeed");
        let output: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(output["language"], "python");
        assert_eq!(output["exitCode"], 0);
        assert!(output["stdout"].as_str().expect("stdout").contains('2'));
    }

    #[test]
    fn given_empty_code_when_repl_then_rejects_with_error() {
        let result = execute_tool("REPL", &json!({"language": "python", "code": "   "}));

        let error = result.expect_err("empty REPL code should fail");
        assert!(error.contains("code must not be empty"));
    }

    #[test]
    fn given_unsupported_language_when_repl_then_rejects_with_error() {
        let result = execute_tool("REPL", &json!({"language": "ruby", "code": "puts 1"}));

        let error = result.expect_err("unsupported REPL language should fail");
        assert!(error.contains("unsupported REPL language: ruby"));
    }

    #[test]
    fn given_timeout_ms_when_repl_blocks_then_returns_timeout_error() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_tool(
            "REPL",
            &json!({
                "language": "python",
                "code": "import time\ntime.sleep(1)",
                "timeout_ms": 10
            }),
        );

        let error = result.expect_err("timed out REPL execution should fail");
        assert!(error.contains("REPL execution exceeded timeout of 10 ms"));
    }

    #[test]
    fn powershell_runs_via_stub_shell() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!(
                "cowd-pwsh-bin-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("time")
                    .as_nanos()
            ));
        std::fs::create_dir_all(&dir).expect("create dir");
        let script = dir.join("pwsh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
while [ "$1" != "-Command" ] && [ $# -gt 0 ]; do shift; done
shift
printf 'pwsh:%s' "$1"
"#,
        )
        .expect("write script");
        std::process::Command::new("/bin/chmod")
            .arg("+x")
            .arg(&script)
            .status()
            .expect("chmod");
        let original_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", dir.display(), original_path));

        let result = execute_tool(
            "PowerShell",
            &json!({"command": "Write-Output hello", "timeout": 1000}),
        )
        .expect("PowerShell should succeed");

        let background = execute_tool(
            "PowerShell",
            &json!({"command": "Write-Output hello", "run_in_background": true}),
        )
        .expect("PowerShell background should succeed");

        std::env::set_var("PATH", original_path);
        let _ = std::fs::remove_dir_all(dir);

        let output: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(output["stdout"], "pwsh:Write-Output hello");
        assert!(output["stderr"].as_str().expect("stderr").is_empty());

        let background_output: serde_json::Value = serde_json::from_str(&background).expect("json");
        assert!(background_output["backgroundTaskId"].as_str().is_some());
        assert_eq!(background_output["backgroundedByUser"], true);
        assert_eq!(background_output["assistantAutoBackgrounded"], false);
    }

    #[test]
    fn powershell_errors_when_shell_is_missing() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let original_path = std::env::var("PATH").unwrap_or_default();
        let empty_dir = std::env::temp_dir().join(format!(
            "cowd-empty-bin-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&empty_dir).expect("create empty dir");
        std::env::set_var("PATH", empty_dir.display().to_string());

        let err = execute_tool("PowerShell", &json!({"command": "Write-Output hello"}))
            .expect_err("PowerShell should fail when shell is missing");

        std::env::set_var("PATH", original_path);
        let _ = std::fs::remove_dir_all(empty_dir);

        assert!(err.contains("PowerShell executable not found"));
    }

    fn read_only_registry() -> ToolCatalog {
        let policy = mvp_tool_specs().into_iter().fold(
            PermissionPolicy::new(PermissionMode::ReadOnly),
            |policy, spec| policy.with_tool_requirement(spec.name, spec.required_permission),
        );
        let mut registry = ToolCatalog::builtin();
        registry.set_enforcer(PermissionEnforcer::new(policy));
        registry
    }

    #[test]
    fn given_read_only_enforcer_when_bash_then_denied() {
        let registry = read_only_registry();
        // Use a command that requires DangerFullAccess (rm) to ensure it's blocked in read-only mode
        let err = registry
            .execute("bash", &json!({ "command": "rm -rf /" }))
            .expect_err("bash should be denied in read-only mode");
        assert!(
            err.contains("current mode is 'read-only'"),
            "should cite active mode: {err}"
        );
    }

    #[test]
    fn given_read_only_enforcer_when_write_file_then_denied() {
        let registry = read_only_registry();
        let err = registry
            .execute(
                "write_file",
                &json!({ "path": "/tmp/x.txt", "content": "x" }),
            )
            .expect_err("write_file should be denied in read-only mode");
        assert!(
            err.contains("current mode is read-only"),
            "should cite active mode: {err}"
        );
    }

    #[test]
    fn given_read_only_enforcer_when_edit_file_then_denied() {
        let registry = read_only_registry();
        let err = registry
            .execute(
                "edit_file",
                &json!({ "path": "/tmp/x.txt", "old_string": "a", "new_string": "b" }),
            )
            .expect_err("edit_file should be denied in read-only mode");
        assert!(
            err.contains("current mode is read-only"),
            "should cite active mode: {err}"
        );
    }

    #[test]
    fn given_read_only_enforcer_when_read_file_then_not_permission_denied() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = temp_path("perm-read");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("readable.txt"), "content\n").expect("write test file");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("set cwd");

        let registry = read_only_registry();
        let result = registry.execute("read_file", &json!({ "path": "readable.txt" }));
        assert!(result.is_ok(), "read_file should be allowed: {result:?}");

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn given_read_only_enforcer_when_glob_search_then_not_permission_denied() {
        let registry = read_only_registry();
        let result = registry.execute("glob_search", &json!({ "pattern": "*.rs" }));
        assert!(
            result.is_ok(),
            "glob_search should be allowed in read-only mode: {result:?}"
        );
    }

    #[test]
    fn given_no_enforcer_when_bash_then_executes_normally() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let registry = ToolCatalog::builtin();
        let result = registry
            .execute("bash", &json!({ "command": "printf 'ok'" }))
            .expect("bash should succeed without enforcer");
        let output: serde_json::Value = serde_json::from_str(&result).expect("json");
        assert_eq!(output["stdout"], "ok");
    }

    #[test]
    fn tools_crate_does_not_depend_on_provider_directly() {
        let manifest = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
        )
        .expect("tools manifest should be readable");
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with("provider =")),
            "tools must route provider access through runtime, not a direct provider dependency"
        );
    }
}
