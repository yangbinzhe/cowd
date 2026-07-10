use std::sync::{Arc, Mutex};

use runtime::{ToolError, ToolExecutor};
use serde::Deserialize;
use tools::permissions::PermissionMode as ToolPermissionMode;

use crate::runtime_bootstrap::{GatewayToolRegistry, RuntimeMcpState};
use crate::services::{start_team_runtime_with_spawner_decision, MissionTeamExecutionMode};
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
    runtime_execution_decision: Arc<Mutex<Option<runtime::RuntimeExecutionDecision>>>,
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
            runtime_execution_decision: Arc::new(Mutex::new(None)),
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
            return serde_json::to_string_pretty(
                &runtime::runtime_orchestration_response_with_host_and_decision(
                    value,
                    Some(self),
                    leased_decision.as_ref(),
                ),
            )
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
        self.tool_registry
            .definitions(self.allowed_tools.as_ref())
            .into_iter()
            .map(|definition| definition.name)
            .collect()
    }

    fn tool_permission_mode(&self, tool_name: &str) -> Option<ToolPermissionMode> {
        self.tool_registry
            .permission_specs(self.allowed_tools.as_ref())
            .ok()?
            .into_iter()
            .find_map(|(name, permission)| (name == tool_name).then_some(permission))
    }
}

fn ensure_gateway_mission_session(session_id: &str, intent: &str) -> Result<(), ToolError> {
    if runtime::global_mission_runtime()
        .get_session(session_id)
        .is_some()
    {
        return Ok(());
    }
    runtime::global_mission_runtime()
        .start_session(runtime::StartMissionSessionRequest {
            title: format!(
                "Gateway runtime session: {}",
                intent.chars().take(80).collect::<String>()
            ),
            session_id: Some(session_id.to_string()),
        })
        .map(|_| ())
        .map_err(ToolError::new)
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

    fn has_registered_tools(&self) -> bool {
        !self.available_tool_names().is_empty()
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

    fn start_runtime_team(
        &self,
        request: &runtime::RuntimeOrchestrationRequest,
        decision: &runtime::CollaborationDecision,
    ) -> Option<Result<serde_json::Value, String>> {
        let result = (|| {
            let session_id = request
                .session_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "request_team requires session_id".to_string())?;
            ensure_gateway_mission_session(session_id, &request.intent)
                .map_err(|error| error.to_string())?;
            let team = start_team_runtime_with_spawner_decision(
                session_id,
                request.intent.clone(),
                None,
                MissionTeamExecutionMode::ProviderInProcess,
                decision.clone(),
            )?;
            let workgraph = runtime::TeamExecutionLoop::plan(&team.team_id)
                .ok()
                .map(|plan| {
                    serde_json::json!({
                        "workgraph_id": plan.workgraph.id,
                        "ready_node_ids": plan.ready_node_ids,
                        "blocked_node_ids": plan.blocked_node_ids,
                        "quality": plan.workgraph_quality,
                    })
                });
            Ok(serde_json::json!({
                "type": "team_runtime",
                "status": "running",
                "execution_fidelity": "runtime_owned_gateway_adapter",
                "team": team,
                "workgraph": workgraph,
                "mission": runtime::global_mission_runtime().projection(),
                "control_actions": ["inspect", "tick_ready", "synthesis", "handoff", "cancel", "pause"],
            }))
        })();
        Some(result)
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

        let output = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"检查 README 是否反映最新架构","action":"request_parallel_tools"}"#,
            )
            .expect("gateway-bound runtime orchestrate should inject a tool host");

        assert!(output.contains("\"status\": \"executed\""), "{output}");
        assert!(output.contains("runtime.tool_dag.executed"), "{output}");
        assert!(output.contains("gateway-tool:"), "{output}");
        assert!(!output.contains("blocked_missing_executor"), "{output}");
    }
}
