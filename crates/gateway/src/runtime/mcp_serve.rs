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
    let authorization_negotiator = runtime::AuthorizationNegotiator::new();
    let permission_policy = runtime::PermissionPolicy::new(runtime::PermissionMode::ReadOnly);
    let spec = McpServerSpec {
        server_name: "cowd".to_string(),
        server_version: VERSION.to_string(),
        tools,
        tool_handler: Box::new(move |name, input| {
            let lease = tool_host.pin_snapshot();
            let effect = lease.describe_effect(name, input);
            let request_id = format!("mcp-stdio:{name}");
            let assessment = authorization_negotiator.assess(
                &permission_policy,
                &runtime::AuthorizationRequest {
                    principal_id: "mcp:stdio".to_string(),
                    capability: effect.tool_id.clone(),
                    input: input.to_string(),
                    idempotency_key: request_id.clone(),
                    effect: effect.clone(),
                    parent_ceiling: runtime::PermissionMode::ReadOnly,
                    parent_lease_id: Some("mcp:stdio".to_string()),
                    approval_satisfied: false,
                    recovery_scope: request_id.clone(),
                    context: runtime::PermissionContext::default(),
                    safe_alternatives: Vec::new(),
                },
            );
            let authorization_lease = assessment
                .lease
                .clone()
                .ok_or_else(|| serde_json::to_string(&assessment).unwrap_or_default())?;
            let decision = runtime::ToolPolicy
                .authorize(&effect, request_id, authorization_lease, 300)
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
