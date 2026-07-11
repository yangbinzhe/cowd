use crate::{CliOutputFormat, VERSION};
use runtime::{McpServer, McpServerSpec, McpTool};
use tools::mvp_tool_specs;
/// Starts a minimal Model Context Protocol server that exposes cowd's
/// built-in tools over stdio.
///
/// Tool descriptors come from [`tools::mvp_tool_specs`] and calls are
/// dispatched through [`tools::execute_tool`], so this server exposes exactly
/// Read `.cowd/worker-state.json` from the current working directory and print it.
/// This is the file-based worker observability surface: `push_event()` in `worker_boot.rs`
/// atomically writes state transitions here so external observers (cowd-orchestrator, orchestrators)
/// can poll current `WorkerStatus` without needing an HTTP route on the opencode binary.
pub(crate) fn run_worker_state(
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let state_path = runtime::cowd_dirs::worker_state_path();
    if !state_path.exists() {
        // Emit a structured error, then return Err so the process exits 1.
        // Callers (scripts, CI) need a non-zero exit to detect "no state" without
        // parsing prose output.
        // Let the error propagate to main() which will format it correctly
        // (prose for text mode, JSON envelope for --output-format json).
        return Err(format!(
            "no worker state file found at {} — run a worker first",
            state_path.display()
        )
        .into());
    }
    let raw = std::fs::read_to_string(&state_path)?;
    match output_format {
        CliOutputFormat::Text => println!("{raw}"),
        CliOutputFormat::Json => {
            // Validate it parses as JSON before re-emitting
            let _: serde_json::Value = serde_json::from_str(&raw)?;
            println!("{raw}");
        }
    }
    Ok(())
}

/// the same surface the in-process agent loop uses.
pub(crate) fn run_mcp_serve() -> Result<(), Box<dyn std::error::Error>> {
    let tools = mvp_tool_specs()
        .into_iter()
        .map(|spec| McpTool {
            name: spec.name.to_string(),
            description: Some(spec.description.to_string()),
            input_schema: Some(spec.input_schema),
            annotations: None,
            meta: None,
        })
        .collect();

    let workspace_root = std::env::current_dir()?;
    let tool_host = std::sync::Arc::new(tools::ToolHost::builtin("mcp-stdio", workspace_root));
    let spec = McpServerSpec {
        server_name: "cowd".to_string(),
        server_version: VERSION.to_string(),
        tools,
        tool_handler: Box::new(move |name, input| {
            let lease = tool_host.pin_snapshot();
            let effect = lease.describe_effect(name, input);
            let decision = runtime::ToolPolicy
                .authorize(
                    &effect,
                    format!("mcp-stdio:{name}"),
                    runtime::PermissionMode::DangerFullAccess,
                    300,
                )
                .map_err(|error| error.to_string())?;
            lease
                .execute(&decision.authorization, name, input)
                .map_err(|error| error.to_string())
        }),
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let mut server = McpServer::new(spec);
        server.run().await
    })?;
    Ok(())
}
