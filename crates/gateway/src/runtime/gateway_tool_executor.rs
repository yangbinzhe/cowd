use std::sync::{Arc, Mutex, OnceLock};

use runtime::{ToolError, ToolExecutor};
use serde::Deserialize;
use tools::permissions::PermissionMode as ToolPermissionMode;
use tools::{ToolHost, ToolHostSnapshot};

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

fn is_gateway_runtime_control_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "runtime_capabilities"
            | "runtime_orchestrate"
            | "MCPTool"
            | "ListMcpResourcesTool"
            | "ReadMcpResourceTool"
    )
}

pub(crate) struct GatewayToolExecutor {
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    tool_host: Arc<ToolHost>,
    mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    runtime_session_id: Option<String>,
    runtime_execution_decision: Arc<Mutex<Option<runtime::RuntimeExecutionDecision>>>,
    runtime_services: Arc<OnceLock<Arc<runtime::RuntimeServices>>>,
}

impl GatewayToolExecutor {
    pub(crate) fn new(
        allowed_tools: Option<AllowedToolSet>,
        emit_output: bool,
        tool_registry: GatewayToolRegistry,
        mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    ) -> Self {
        let workspace_root =
            std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let tool_host = Arc::new(ToolHost::new(
            "gateway",
            workspace_root,
            ToolHostSnapshot::new(
                Arc::new(tool_registry),
                Arc::new(tools::lsp_client::LspRegistry::new()),
                None,
            ),
        ));
        Self {
            emit_output,
            allowed_tools,
            tool_host,
            mcp_state,
            runtime_session_id: None,
            runtime_execution_decision: Arc::new(Mutex::new(None)),
            runtime_services: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn from_tool_host(
        allowed_tools: Option<AllowedToolSet>,
        emit_output: bool,
        tool_host: Arc<ToolHost>,
        mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    ) -> Self {
        Self {
            emit_output,
            allowed_tools,
            tool_host,
            mcp_state,
            runtime_session_id: None,
            runtime_execution_decision: Arc::new(Mutex::new(None)),
            runtime_services: Arc::new(OnceLock::new()),
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

    pub(crate) fn bind_runtime_services(
        &self,
        services: Arc<runtime::RuntimeServices>,
    ) -> Result<(), String> {
        self.runtime_services
            .set(services)
            .map_err(|_| "runtime services already bound to gateway tool executor".to_string())
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
        let receipt = self
            .tool_host
            .pin_snapshot()
            .search(&input.query, input.max_results.unwrap_or(5));
        let mut value =
            serde_json::to_value(receipt).map_err(|error| ToolError::new(error.to_string()))?;
        if let Some(object) = value.as_object_mut() {
            let matches = object
                .get("activation_candidates")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]));
            object.insert("matches".to_string(), matches);
            object.insert(
                "pending_mcp_servers".to_string(),
                pending_mcp_servers.map_or(serde_json::Value::Null, |servers| {
                    serde_json::json!(servers)
                }),
            );
            object.insert(
                "mcp_degraded".to_string(),
                mcp_degraded.unwrap_or(serde_json::Value::Null),
            );
        }
        serde_json::to_string_pretty(&value).map_err(|error| ToolError::new(error.to_string()))
    }

    fn execute_runtime_tool(
        &self,
        tool_name: &str,
        value: serde_json::Value,
    ) -> Result<String, ToolError> {
        if tool_name == "runtime_capabilities" {
            let input: RuntimeCapabilitiesRequest = serde_json::from_value(value)
                .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
            let active_evolution = crate::current_active_evolution_capability_overlay();
            let leased_decision = self
                .runtime_execution_decision
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            return serde_json::to_string_pretty(
                &runtime::runtime_capabilities_response_with_leased_decision_and_tools(
                    &input.intent,
                    input.surface.as_deref(),
                    input.profile.as_deref(),
                    input.detail.as_deref(),
                    &active_evolution,
                    leased_decision.as_ref(),
                    &self.available_tool_names(),
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
            let leased_decision = self
                .runtime_execution_decision
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let request = serde_json::from_value::<runtime::RuntimeOrchestrationRequest>(value)
                .map_err(|error| {
                    ToolError::new(format!("invalid runtime_orchestrate input: {error}"))
                })?;
            let services = self.runtime_services.get().cloned().ok_or_else(|| {
                ToolError::new("runtime_orchestrate requires the workspace RuntimeServices Runner")
            })?;
            let decision = leased_decision;
            let result = std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let runtime_handle = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|error| ToolError::new(error.to_string()))?;
                        Ok(
                            runtime_handle.block_on(runtime::submit_runtime_orchestration_request(
                                request,
                                decision.as_ref(),
                                services.as_ref(),
                            )),
                        )
                    })
                    .join()
                    .map_err(|_| ToolError::new("runtime orchestration worker panicked"))?
            })?;
            return serde_json::to_string_pretty(&result)
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

    fn available_tool_names(&self) -> Vec<String> {
        self.tool_host
            .pin_snapshot()
            .snapshot()
            .catalog
            .definitions(self.allowed_tools.as_ref())
            .into_iter()
            .map(|definition| definition.name)
            .collect()
    }

    fn tool_permission_mode(&self, tool_name: &str) -> Option<ToolPermissionMode> {
        self.tool_host
            .pin_snapshot()
            .snapshot()
            .catalog
            .permission_specs(self.allowed_tools.as_ref())
            .ok()?
            .into_iter()
            .find_map(|(name, permission)| (name == tool_name).then_some(permission))
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
        } else if is_gateway_runtime_control_tool(tool_name) {
            self.execute_runtime_tool(tool_name, value)
        } else {
            let lease = self.tool_host.pin_snapshot();
            let effect = lease.describe_effect(tool_name, &value);
            runtime::ToolPolicy
                .authorize(
                    &effect,
                    format!(
                        "gateway:{tool_name}:{}",
                        chrono::Utc::now().timestamp_micros()
                    ),
                    runtime::PermissionMode::DangerFullAccess,
                    300,
                )
                .map_err(|error| ToolError::new(error.to_string()))
                .and_then(|decision| {
                    let output = lease
                        .execute(&decision.authorization, tool_name, &value)
                        .map_err(|error| ToolError::new(error.to_string()))?;
                    if tool_name.starts_with("mcp__") {
                        let receipt: serde_json::Value = serde_json::from_str(&output)
                            .map_err(|error| ToolError::new(error.to_string()))?;
                        return serde_json::to_string_pretty(&receipt["output"])
                            .map_err(|error| ToolError::new(error.to_string()));
                    }
                    Ok(output)
                })
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

    fn has_registered_tools(&self) -> bool {
        !self.available_tool_names().is_empty()
    }

    fn describe_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        Some(
            self.tool_host
                .pin_snapshot()
                .describe_effect(tool_name, input),
        )
    }

    fn execute_authorized(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<String, ToolError> {
        let value = serde_json::from_str(input)
            .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
        if tool_name == "ToolSearch" || is_gateway_runtime_control_tool(tool_name) {
            return self.execute(tool_name, input);
        }
        self.tool_host
            .pin_snapshot()
            .execute(authorization, tool_name, &value)
            .map_err(|error| ToolError::new(error.to_string()))
    }

    fn available_tool_names(&self) -> Vec<String> {
        GatewayToolExecutor::available_tool_names(self)
    }

    fn classify_tool_safety(
        &self,
        tool_name: &str,
        _input: &str,
    ) -> Option<runtime::ToolSafetyCategory> {
        self.tool_permission_mode(tool_name)
            .map(|permission| match permission {
                ToolPermissionMode::ReadOnly => runtime::ToolSafetyCategory::ReadOnly,
                ToolPermissionMode::WorkspaceWrite => runtime::ToolSafetyCategory::WriteLocal,
                ToolPermissionMode::DangerFullAccess
                | ToolPermissionMode::Prompt
                | ToolPermissionMode::Allow => runtime::ToolSafetyCategory::Destructive,
            })
    }

    fn collaboration_runtime_available(&self) -> bool {
        self.has_tool("runtime_orchestrate")
    }

    fn mission_runtime_available(&self) -> bool {
        self.has_tool("runtime_orchestrate")
    }

    fn bind_execution_decision(&self, decision: runtime::RuntimeExecutionDecision) {
        *self
            .runtime_execution_decision
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(decision);
    }
}

impl runtime::RuntimeExecutionHost for GatewayToolExecutor {
    fn execute_runtime_tool(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> runtime::RuntimeToolExecutionOutcome {
        let evidence_ref = format!("gateway-tool:{}", request.tool_use_id);
        match <Self as ToolExecutor>::execute(self, &request.tool_name, &request.input) {
            Ok(output) => runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(output),
                error: None,
                evidence_ref,
            },
            Err(error) => runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::Failed,
                category: request.category,
                output: None,
                error: Some(error.to_string()),
                evidence_ref,
            },
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
        let response: serde_json::Value = serde_json::from_str(&output).expect("capability json");
        assert_eq!(response["runtime_orchestrate"]["available"], false);
        assert!(response["strategy"]["model_callable_tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().all(|tool| tool != "runtime_orchestrate")));
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
                required_permission: ToolPermissionMode::WorkspaceWrite,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None);
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .unwrap();

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
    fn runtime_tool_permission_metadata_drives_safety_classification() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "company_catalog_lookup".to_string(),
                description: Some("read company catalog".to_string()),
                input_schema: json!({"type":"object"}),
                required_permission: ToolPermissionMode::ReadOnly,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None);

        assert_eq!(
            executor.classify_tool_safety("company_catalog_lookup", "{}"),
            Some(runtime::ToolSafetyCategory::ReadOnly)
        );
    }

    #[test]
    fn capabilities_and_orchestration_reuse_the_bound_turn_strategy_lease() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![
                RuntimeToolDefinition {
                    name: "runtime_capabilities".to_string(),
                    description: Some("capability guidance".to_string()),
                    input_schema: json!({"type":"object","properties":{"intent":{"type":"string"}},"required":["intent"]}),
                    required_permission: ToolPermissionMode::ReadOnly,
                },
                RuntimeToolDefinition {
                    name: "runtime_orchestrate".to_string(),
                    description: Some("runtime orchestration".to_string()),
                    input_schema: json!({"type":"object","properties":{"intent":{"type":"string"},"action":{"type":"string"}},"required":["intent"]}),
                    required_permission: ToolPermissionMode::WorkspaceWrite,
                },
            ])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None);
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .unwrap();
        let decision = runtime::build_runtime_execution_decision(
            "并行调研 runtime gateway memory 的当前实现",
            None,
        );
        let lease_id = decision.lease.lease_id.clone();
        executor.bind_execution_decision(decision);

        let capabilities: serde_json::Value = serde_json::from_str(
            &executor
                .execute(
                    "runtime_capabilities",
                    r#"{"intent":"换一个描述也必须复用当前 turn 决策"}"#,
                )
                .expect("capabilities"),
        )
        .expect("capability json");
        let orchestration: serde_json::Value = serde_json::from_str(
            &executor
                .execute(
                    "runtime_orchestrate",
                    r#"{"intent":"换一个描述也必须复用当前 turn 决策","action":"plan_only"}"#,
                )
                .expect("orchestration"),
        )
        .expect("orchestration json");

        assert_eq!(
            capabilities["execution_decision"]["lease"]["lease_id"],
            lease_id
        );
        assert_eq!(
            orchestration["evidence"]["strategy_lease"]["lease_id"],
            lease_id
        );
    }

    #[test]
    fn runtime_orchestrate_auto_binds_gateway_session_for_team_requests() {
        let _env_guard = crate::test_process_env_lock();
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
                required_permission: ToolPermissionMode::WorkspaceWrite,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None)
            .with_runtime_session_id("gateway-session-v26");
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .unwrap();

        let output = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"需要多 Agent 协同审查架构","action":"request_team"}"#,
            )
            .expect("gateway-bound runtime orchestrate should execute without explicit session_id");

        let response: serde_json::Value = serde_json::from_str(&output).expect("typed response");
        assert_eq!(response["status"], "unavailable");
        assert_eq!(response["execution"]["type"], "orchestration_not_submitted");
        assert_eq!(response["evidence"]["accepted"], false);
        assert!(response["decision"]["validation_findings"]
            .as_array()
            .expect("validation findings")
            .iter()
            .any(|finding| finding
                .as_str()
                .is_some_and(|value| value.contains("execution_capability_unavailable"))));
        assert!(!output.contains("missing_session_id_for_team_runtime"));
    }

    #[test]
    fn runtime_orchestrate_parallel_tools_injects_gateway_tool_host() {
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
                required_permission: ToolPermissionMode::WorkspaceWrite,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None)
            .with_runtime_session_id("gateway-session-v6-tool-host");
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .unwrap();

        let output = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"检查 README 是否反映最新架构","action":"request_parallel_tools"}"#,
            )
            .expect("gateway-bound runtime orchestrate should inject a tool host");

        let response: serde_json::Value = serde_json::from_str(&output).expect("typed response");
        assert_eq!(response["status"], "compiled");
        assert_eq!(response["execution"]["type"], "execution_graph_compilation");
        assert_eq!(response["evidence"]["compiled"], true);
        assert_eq!(response["evidence"]["accepted"], false);
        assert!(response["execution"].get("receipt").is_none());
    }
}
