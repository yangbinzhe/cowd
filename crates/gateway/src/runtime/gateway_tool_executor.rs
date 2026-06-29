use std::sync::{Arc, Mutex};

use runtime::{ToolError, ToolExecutor};
use serde::Deserialize;

use crate::runtime_bootstrap::{GatewayToolRegistry, RuntimeMcpState};
use crate::{format_tool_result, AllowedToolSet};

#[derive(Debug, Deserialize)]
struct ToolSearchRequest {
    query: String,
    max_results: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct McpToolRequest {
    #[serde(rename = "qualifiedName")]
    qualified_name: Option<String>,
    tool: Option<String>,
    arguments: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ListMcpResourcesRequest {
    server: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadMcpResourceRequest {
    server: String,
    uri: String,
}

#[derive(Debug, Deserialize)]
struct RuntimeCapabilitiesRequest {
    intent: String,
    surface: Option<String>,
    profile: Option<String>,
    detail: Option<String>,
}

pub(crate) struct GatewayToolExecutor {
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    tool_registry: GatewayToolRegistry,
    mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    runtime_session_id: Option<String>,
}

impl GatewayToolExecutor {
    pub(crate) fn new(
        allowed_tools: Option<AllowedToolSet>,
        emit_output: bool,
        tool_registry: GatewayToolRegistry,
        mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    ) -> Self {
        Self {
            emit_output,
            allowed_tools,
            tool_registry,
            mcp_state,
            runtime_session_id: None,
        }
    }

    #[must_use]
    pub(crate) fn with_runtime_session_id(mut self, session_id: impl Into<String>) -> Self {
        let session_id = session_id.into();
        if !session_id.is_empty() {
            self.runtime_session_id = Some(session_id);
        }
        self
    }

    fn execute_search_tool(&self, value: serde_json::Value) -> Result<String, ToolError> {
        let input: ToolSearchRequest = serde_json::from_value(value)
            .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
        let (pending_mcp_servers, mcp_degraded) =
            self.mcp_state.as_ref().map_or((None, None), |state| {
                let state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    state.pending_servers(),
                    state
                        .degraded_report()
                        .and_then(|report| serde_json::to_value(report).ok()),
                )
            });
        serde_json::to_string_pretty(&self.tool_registry.search(
            &input.query,
            input.max_results.unwrap_or(5),
            pending_mcp_servers,
            mcp_degraded,
        ))
        .map_err(|error| ToolError::new(error.to_string()))
    }

    fn execute_runtime_tool(
        &self,
        tool_name: &str,
        value: serde_json::Value,
    ) -> Result<String, ToolError> {
        if tool_name == "runtime_capabilities" {
            let input: RuntimeCapabilitiesRequest = serde_json::from_value(value)
                .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
            return serde_json::to_string_pretty(
                &runtime::runtime_capabilities_response_with_detail(
                    &input.intent,
                    input.surface.as_deref(),
                    input.profile.as_deref(),
                    input.detail.as_deref(),
                ),
            )
            .map_err(|error| ToolError::new(error.to_string()));
        }
        if tool_name == "runtime_orchestrate" {
            let mut value = value;
            if let Some(session_id) = &self.runtime_session_id {
                if let Some(object) = value.as_object_mut() {
                    let missing_session = object
                        .get("session_id")
                        .and_then(serde_json::Value::as_str)
                        .is_none_or(str::is_empty);
                    if missing_session {
                        object.insert(
                            "session_id".to_string(),
                            serde_json::Value::String(session_id.clone()),
                        );
                    }
                }
            }
            return serde_json::to_string_pretty(&runtime::runtime_orchestration_response(value))
                .map_err(|error| ToolError::new(error.to_string()));
        }

        let Some(mcp_state) = &self.mcp_state else {
            return Err(ToolError::new(format!(
                "runtime tool `{tool_name}` is unavailable without configured MCP servers"
            )));
        };
        let mut mcp_state = mcp_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        match tool_name {
            "MCPTool" => {
                let input: McpToolRequest = serde_json::from_value(value)
                    .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
                let qualified_name = input
                    .qualified_name
                    .or(input.tool)
                    .ok_or_else(|| ToolError::new("missing required field `qualifiedName`"))?;
                mcp_state.call_tool(&qualified_name, input.arguments)
            }
            "ListMcpResourcesTool" => {
                let input: ListMcpResourcesRequest = serde_json::from_value(value)
                    .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
                match input.server {
                    Some(server_name) => mcp_state.list_resources_for_server(&server_name),
                    None => mcp_state.list_resources_for_all_servers(),
                }
            }
            "ReadMcpResourceTool" => {
                let input: ReadMcpResourceRequest = serde_json::from_value(value)
                    .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
                mcp_state.read_resource(&input.server, &input.uri)
            }
            _ => mcp_state.call_tool(tool_name, Some(value)),
        }
    }
}

impl ToolExecutor for GatewayToolExecutor {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        if self
            .allowed_tools
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(tool_name))
        {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` is not enabled by the current --allowedTools setting"
            )));
        }
        let value = serde_json::from_str(input)
            .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
        let result = if tool_name == "ToolSearch" {
            self.execute_search_tool(value)
        } else if self.tool_registry.has_runtime_tool(tool_name) {
            self.execute_runtime_tool(tool_name, value)
        } else {
            self.tool_registry
                .execute(tool_name, &value)
                .map_err(ToolError::new)
        };
        match result {
            Ok(output) => {
                if self.emit_output {
                    let markdown = format_tool_result(tool_name, &output, false);
                    print!("{markdown}");
                }
                Ok(output)
            }
            Err(error) => {
                if self.emit_output {
                    let markdown = format_tool_result(tool_name, &error.to_string(), true);
                    print!("{markdown}");
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tools::permissions::PermissionMode as ToolPermissionMode;
    use tools::RuntimeToolDefinition;

    #[test]
    fn runtime_capabilities_executes_without_mcp_state() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_capabilities".to_string(),
                description: Some("capability guidance".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "intent": { "type": "string" }
                    },
                    "required": ["intent"],
                    "additionalProperties": false
                }),
                required_permission: ToolPermissionMode::ReadOnly,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None);

        let output = executor
            .execute(
                "runtime_capabilities",
                r#"{"intent":"检查 README 是否反映最新架构"}"#,
            )
            .expect("runtime capabilities should execute without MCP");

        assert!(output.contains("runtime_capabilities"));
        assert!(output.contains("evidence_plan"));
        assert!(output.contains("tool_batch_readonly"));
        assert!(output.contains("runtime_orchestrate"));
    }

    #[test]
    fn runtime_orchestrate_executes_without_mcp_state() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_orchestrate".to_string(),
                description: Some("runtime orchestration".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "intent": { "type": "string" },
                        "action": { "type": "string" }
                    },
                    "required": ["intent"],
                    "additionalProperties": true
                }),
                required_permission: ToolPermissionMode::ReadOnly,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None);

        let output = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"检查 README 是否反映最新架构","action":"plan_only"}"#,
            )
            .expect("runtime orchestrate should execute without MCP");

        assert!(output.contains("runtime-orch-"));
        assert!(output.contains("plan_only"));
    }

    #[test]
    fn runtime_orchestrate_auto_binds_gateway_session_for_team_requests() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_orchestrate".to_string(),
                description: Some("runtime orchestration".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "intent": { "type": "string" },
                        "action": { "type": "string" }
                    },
                    "required": ["intent"],
                    "additionalProperties": true
                }),
                required_permission: ToolPermissionMode::ReadOnly,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None)
            .with_runtime_session_id("gateway-session-v26");

        let output = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"需要多 Agent 协同审查架构","action":"request_team"}"#,
            )
            .expect("gateway-bound runtime orchestrate should execute without explicit session_id");

        assert!(output.contains("\"status\": \"running\""), "{output}");
        assert!(output.contains("\"type\": \"team_runtime\""), "{output}");
        assert!(output.contains("gateway-session-v26"), "{output}");
        assert!(!output.contains("missing_session_id_for_team_runtime"));
    }
}
