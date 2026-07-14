use crate::VERSION;
use runtime::{McpServer, McpServerSpec, McpTool};
use tools::mvp_tool_specs;
/// Starts a minimal Model Context Protocol server that exposes cowd's
/// built-in tools over stdio.
///
/// Tool descriptors come from [`tools::mvp_tool_specs`] and calls are
/// dispatched through [`tools::execute_tool`], so this server exposes exactly
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
