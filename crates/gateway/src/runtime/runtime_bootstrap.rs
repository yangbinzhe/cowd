use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use plugins::{PluginHooks, PluginManager, PluginManagerConfig, PluginRegistry};
use runtime::{ConfigLoader, McpServerManager, ToolError};
use serde_json::json;
use tools::{permissions::PermissionMode as ToolPermissionMode, RuntimeToolDefinition};

pub(crate) type GatewayToolRegistry = tools::GlobalToolRegistry;
pub(crate) type RuntimePluginStateBuildOutput = (
    Option<Arc<Mutex<RuntimeMcpState>>>,
    Vec<RuntimeToolDefinition>,
);

pub(crate) struct RuntimeBootstrapState {
    pub(crate) feature_config: runtime::RuntimeFeatureConfig,
    pub(crate) tool_registry: GatewayToolRegistry,
    pub(crate) plugin_registry: PluginRegistry,
    pub(crate) mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
}

pub(crate) struct RuntimeMcpState {
    runtime: tokio::runtime::Runtime,
    manager: McpServerManager,
    pending_servers: Vec<String>,
    degraded_report: Option<runtime::McpDegradedReport>,
}

pub(crate) fn assemble_runtime_state() -> Result<RuntimeBootstrapState, Box<dyn std::error::Error>>
{
    let cwd = std::env::current_dir()?;
    let loader = ConfigLoader::default_for(&cwd);
    let runtime_config = loader.load()?;
    assemble_runtime_state_with_loader(&cwd, &loader, &runtime_config)
}

pub(crate) fn assemble_runtime_state_with_loader(
    cwd: &Path,
    loader: &ConfigLoader,
    runtime_config: &runtime::RuntimeConfig,
) -> Result<RuntimeBootstrapState, Box<dyn std::error::Error>> {
    let plugin_manager = build_plugin_manager(cwd, loader, runtime_config);
    let plugin_registry = plugin_manager.plugin_registry()?;
    let plugin_hook_config =
        runtime_hook_config_from_plugin_hooks(plugin_registry.aggregated_hooks()?);
    let feature_config = runtime_config
        .feature_config()
        .clone()
        .with_hooks(runtime_config.hooks().merged(&plugin_hook_config));
    let (mcp_state, runtime_tools) = assemble_mcp_tool_state(runtime_config)?;
    let tool_registry =
        GatewayToolRegistry::with_plugin_tools(plugin_registry.aggregated_tools()?)?
            .with_runtime_tools(runtime_tools)?;
    Ok(RuntimeBootstrapState {
        feature_config,
        tool_registry,
        plugin_registry,
        mcp_state,
    })
}

pub(crate) fn load_tool_registry_for_current_dir() -> Result<GatewayToolRegistry, String> {
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let loader = ConfigLoader::default_for(&cwd);
    let runtime_config = loader.load().map_err(|error| error.to_string())?;
    let state = assemble_runtime_state_with_loader(&cwd, &loader, &runtime_config)
        .map_err(|error| error.to_string())?;
    let registry = state.tool_registry.clone();
    if let Some(mcp_state) = state.mcp_state {
        mcp_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .shutdown()
            .map_err(|error| error.to_string())?;
    }
    Ok(registry)
}

pub(crate) fn build_plugin_manager(
    cwd: &Path,
    loader: &ConfigLoader,
    runtime_config: &runtime::RuntimeConfig,
) -> PluginManager {
    let plugin_settings = runtime_config.plugins();
    let mut plugin_config = PluginManagerConfig::new(loader.config_home().to_path_buf());
    plugin_config.enabled_plugins = plugin_settings.enabled_plugins().clone();
    let state_path = runtime::cowd_dirs::config_home_dir().join("plugin-state.json");
    if let Ok(content) = std::fs::read_to_string(&state_path) {
        if !content.trim().is_empty() {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(map) = val.get("enabledPlugins").and_then(|v| v.as_object()) {
                    for (key, value) in map {
                        if let Some(enabled) = value.as_bool() {
                            plugin_config.enabled_plugins.insert(key.clone(), enabled);
                        }
                    }
                }
            }
        }
    }
    plugin_config.external_dirs = plugin_settings
        .external_directories()
        .iter()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path))
        .collect();
    plugin_config.install_root = plugin_settings
        .install_root()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    plugin_config.registry_path = plugin_settings
        .registry_path()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    plugin_config.bundled_root = plugin_settings
        .bundled_root()
        .map(|path| resolve_plugin_path(cwd, loader.config_home(), path));
    PluginManager::new(plugin_config)
}

fn resolve_plugin_path(cwd: &Path, config_home: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else if value.starts_with('.') {
        cwd.join(path)
    } else {
        config_home.join(path)
    }
}

impl RuntimeMcpState {
    fn new(
        runtime_config: &runtime::RuntimeConfig,
    ) -> Result<Option<(Self, runtime::McpToolDiscoveryReport)>, Box<dyn std::error::Error>> {
        let mut manager = McpServerManager::from_runtime_config(runtime_config);
        if manager.server_names().is_empty() && manager.unsupported_servers().is_empty() {
            return Ok(None);
        }

        if tokio::runtime::Handle::try_current().is_ok() {
            return Ok(None);
        }
        let runtime = tokio::runtime::Runtime::new()?;
        let discovery = runtime.block_on(manager.discover_tools_best_effort());
        let pending_servers = discovery
            .failed_servers
            .iter()
            .map(|failure| failure.server_name.clone())
            .chain(
                discovery
                    .unsupported_servers
                    .iter()
                    .map(|server| server.server_name.clone()),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let available_tools = discovery
            .tools
            .iter()
            .map(|tool| tool.qualified_name.clone())
            .collect::<Vec<_>>();
        let failed_server_names = pending_servers.iter().cloned().collect::<BTreeSet<_>>();
        let working_servers = manager
            .server_names()
            .into_iter()
            .filter(|server_name| !failed_server_names.contains(server_name))
            .collect::<Vec<_>>();
        let failed_servers =
            discovery
                .failed_servers
                .iter()
                .map(|failure| runtime::McpFailedServer {
                    server_name: failure.server_name.clone(),
                    phase: runtime::McpLifecyclePhase::ToolDiscovery,
                    error: runtime::McpErrorSurface::new(
                        runtime::McpLifecyclePhase::ToolDiscovery,
                        Some(failure.server_name.clone()),
                        failure.error.clone(),
                        std::collections::BTreeMap::new(),
                        true,
                    ),
                })
                .chain(discovery.unsupported_servers.iter().map(|server| {
                    runtime::McpFailedServer {
                        server_name: server.server_name.clone(),
                        phase: runtime::McpLifecyclePhase::ServerRegistration,
                        error: runtime::McpErrorSurface::new(
                            runtime::McpLifecyclePhase::ServerRegistration,
                            Some(server.server_name.clone()),
                            server.reason.clone(),
                            std::collections::BTreeMap::from([(
                                "transport".to_string(),
                                format!("{:?}", server.transport).to_ascii_lowercase(),
                            )]),
                            false,
                        ),
                    }
                }))
                .collect::<Vec<_>>();
        let degraded_report = (!failed_servers.is_empty()).then(|| {
            runtime::McpDegradedReport::new(
                working_servers,
                failed_servers,
                available_tools.clone(),
                available_tools,
            )
        });

        Ok(Some((
            Self {
                runtime,
                manager,
                pending_servers,
                degraded_report,
            },
            discovery,
        )))
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.runtime.block_on(self.manager.shutdown())?;
        Ok(())
    }

    pub(crate) fn pending_servers(&self) -> Option<Vec<String>> {
        (!self.pending_servers.is_empty()).then(|| self.pending_servers.clone())
    }

    pub(crate) fn degraded_report(&self) -> Option<runtime::McpDegradedReport> {
        self.degraded_report.clone()
    }

    pub(crate) fn server_names(&self) -> Vec<String> {
        self.manager.server_names()
    }

    pub(crate) fn call_tool(
        &mut self,
        qualified_tool_name: &str,
        arguments: Option<serde_json::Value>,
    ) -> Result<String, ToolError> {
        let response = self
            .runtime
            .block_on(self.manager.call_tool(qualified_tool_name, arguments))
            .map_err(|error| ToolError::new(error.to_string()))?;
        if let Some(error) = response.error {
            return Err(ToolError::new(format!(
                "MCP tool `{qualified_tool_name}` returned JSON-RPC error: {} ({})",
                error.message, error.code
            )));
        }

        let result = response.result.ok_or_else(|| {
            ToolError::new(format!(
                "MCP tool `{qualified_tool_name}` returned no result payload"
            ))
        })?;
        serde_json::to_string_pretty(&result).map_err(|error| ToolError::new(error.to_string()))
    }

    pub(crate) fn list_resources_for_server(
        &mut self,
        server_name: &str,
    ) -> Result<String, ToolError> {
        let result = self
            .runtime
            .block_on(self.manager.list_resources(server_name))
            .map_err(|error| ToolError::new(error.to_string()))?;
        serde_json::to_string_pretty(&json!({
            "server": server_name,
            "resources": result.resources,
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }

    pub(crate) fn list_resources_for_all_servers(&mut self) -> Result<String, ToolError> {
        let mut resources = Vec::new();
        let mut failures = Vec::new();

        for server_name in self.server_names() {
            match self
                .runtime
                .block_on(self.manager.list_resources(&server_name))
            {
                Ok(result) => resources.push(json!({
                    "server": server_name,
                    "resources": result.resources,
                })),
                Err(error) => failures.push(json!({
                    "server": server_name,
                    "error": error.to_string(),
                })),
            }
        }

        if resources.is_empty() && !failures.is_empty() {
            let message = failures
                .iter()
                .filter_map(|failure| failure.get("error").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ToolError::new(message));
        }

        serde_json::to_string_pretty(&json!({
            "resources": resources,
            "failures": failures,
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }

    pub(crate) fn read_resource(
        &mut self,
        server_name: &str,
        uri: &str,
    ) -> Result<String, ToolError> {
        let result = self
            .runtime
            .block_on(self.manager.read_resource(server_name, uri))
            .map_err(|error| ToolError::new(error.to_string()))?;
        serde_json::to_string_pretty(&json!({
            "server": server_name,
            "contents": result.contents,
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }
}

fn assemble_mcp_tool_state(
    runtime_config: &runtime::RuntimeConfig,
) -> Result<RuntimePluginStateBuildOutput, Box<dyn std::error::Error>> {
    let mut runtime_tools = runtime_capability_tool_definitions();
    let Some((mcp_state, discovery)) = RuntimeMcpState::new(runtime_config)? else {
        return Ok((None, runtime_tools));
    };

    runtime_tools.extend(
        discovery
            .tools
            .iter()
            .map(mcp_runtime_tool_definition)
            .collect::<Vec<_>>(),
    );
    if !mcp_state.server_names().is_empty() {
        runtime_tools.extend(mcp_wrapper_tool_definitions());
    }

    Ok((Some(Arc::new(Mutex::new(mcp_state))), runtime_tools))
}

fn runtime_capability_tool_definitions() -> Vec<RuntimeToolDefinition> {
    vec![
        RuntimeToolDefinition {
            name: "runtime_capabilities".to_string(),
            description: Some(
                "Return Cowd runtime capability guidance, execution patterns, evidence planning, batch/parallel tool advice, and orchestration suggestions for the current task.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "intent": { "type": "string" },
                    "surface": { "type": "string" },
                    "profile": { "type": "string" },
                    "detail": {
                        "type": "string",
                        "enum": ["summary", "execution_patterns", "team_templates", "agent_catalog", "orchestration_options", "runtime_action_contract", "capability_catalog", "action_selection", "budget_controls", "policy_gates"]
                    }
                },
                "required": ["intent"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
        },
        RuntimeToolDefinition {
            name: "runtime_orchestrate".to_string(),
            description: Some(
                "Submit a controlled stateful runtime orchestration request. Executable lifecycle actions create runtime-owned team/subagent/verification/background/session receipts; deliberation/reflexion return strategy packets; risk_gate returns an approval packet. Use runtime_capabilities for read-only planning first.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "intent": { "type": "string" },
                    "session_id": {
                        "type": "string",
                        "description": "Optional in gateway/API sessions because Cowd auto-binds the active session_id. Required only for detached/offline runtime_orchestrate calls."
                    },
                    "action": {
                        "type": "string",
                        "description": "executable_lifecycle: request_team, request_subagent, request_verification, request_background_review, request_session_link; executable_tool_dag: request_parallel_tools, request_rewoo_evidence; strategy_packet: request_deliberation, request_reflexion_retry; approval_packet: request_risk_gate",
                        "enum": [
                            "plan_only",
                            "request_team",
                            "request_subagent",
                            "request_verification",
                            "request_parallel_tools",
                            "request_rewoo_evidence",
                            "request_deliberation",
                            "request_reflexion_retry",
                            "request_background_review",
                            "request_risk_gate",
                            "request_session_link"
                        ]
                    },
                    "reason": { "type": "string" },
                    "template_hint": { "type": "string" },
                    "capabilities": { "type": "array", "items": { "type": "string" } },
                    "evidence_refs": { "type": "array", "items": { "type": "string" } },
                    "surface": { "type": "string" },
                    "constraints": {
                        "type": "object",
                        "properties": {
                            "max_parallel_agents": { "type": "integer", "minimum": 1 },
                            "risk": { "type": "string", "enum": ["low", "medium", "high", "critical"] },
                            "requires_write": { "type": "boolean" },
                            "surface_latency_sensitive": { "type": "boolean" }
                        },
                        "additionalProperties": false
                    }
                },
                "required": ["intent"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::WorkspaceWrite,
        },
    ]
}

fn mcp_runtime_tool_definition(tool: &runtime::ManagedMcpTool) -> RuntimeToolDefinition {
    RuntimeToolDefinition {
        name: tool.qualified_name.clone(),
        description: Some(
            tool.tool
                .description
                .clone()
                .unwrap_or_else(|| format!("Invoke MCP tool `{}`.", tool.qualified_name)),
        ),
        input_schema: tool
            .tool
            .input_schema
            .clone()
            .unwrap_or_else(|| json!({ "type": "object", "additionalProperties": true })),
        required_permission: permission_mode_for_mcp_tool(&tool.tool),
    }
}

fn mcp_wrapper_tool_definitions() -> Vec<RuntimeToolDefinition> {
    vec![
        RuntimeToolDefinition {
            name: "MCPTool".to_string(),
            description: Some(
                "Call a configured MCP tool by its qualified name and JSON arguments.".to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "qualifiedName": { "type": "string" },
                    "arguments": {}
                },
                "required": ["qualifiedName"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::DangerFullAccess,
        },
        RuntimeToolDefinition {
            name: "ListMcpResourcesTool".to_string(),
            description: Some(
                "List MCP resources from one configured server or from every connected server."
                    .to_string(),
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" }
                },
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
        },
        RuntimeToolDefinition {
            name: "ReadMcpResourceTool".to_string(),
            description: Some("Read a specific MCP resource from a configured server.".to_string()),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" },
                    "uri": { "type": "string" }
                },
                "required": ["server", "uri"],
                "additionalProperties": false
            }),
            required_permission: ToolPermissionMode::ReadOnly,
        },
    ]
}

fn permission_mode_for_mcp_tool(tool: &runtime::McpTool) -> ToolPermissionMode {
    let read_only = mcp_annotation_flag(tool, "readOnlyHint");
    let destructive = mcp_annotation_flag(tool, "destructiveHint");
    let open_world = mcp_annotation_flag(tool, "openWorldHint");

    if read_only && !destructive && !open_world {
        ToolPermissionMode::ReadOnly
    } else if destructive || open_world {
        ToolPermissionMode::DangerFullAccess
    } else {
        ToolPermissionMode::WorkspaceWrite
    }
}

fn mcp_annotation_flag(tool: &runtime::McpTool, key: &str) -> bool {
    tool.annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn runtime_hook_config_from_plugin_hooks(hooks: PluginHooks) -> runtime::RuntimeHookConfig {
    runtime::RuntimeHookConfig::new(
        hooks.pre_tool_use,
        hooks.post_tool_use,
        hooks.post_tool_use_failure,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_capability_tool_is_always_registered_as_readonly() {
        let tools = runtime_capability_tool_definitions();

        let capability_tool = tools
            .iter()
            .find(|tool| tool.name == "runtime_capabilities")
            .expect("runtime capability tool");
        assert_eq!(
            capability_tool.required_permission,
            ToolPermissionMode::ReadOnly
        );
        assert_eq!(capability_tool.input_schema["required"][0], "intent");
        assert!(capability_tool.input_schema["properties"]["detail"]["enum"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "budget_controls")));
    }
}
