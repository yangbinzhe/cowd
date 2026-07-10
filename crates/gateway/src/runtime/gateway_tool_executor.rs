use std::sync::{Arc, Mutex};

use runtime::{ToolError, ToolExecutor};
use serde::{Deserialize, Serialize};

use crate::runtime_bootstrap::{GatewayToolRegistry, RuntimeMcpState};
use crate::services::{start_team_runtime_with_spawner, MissionTeamExecutionMode};
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

#[derive(Debug, Deserialize, Serialize)]
struct RuntimeOrchestrateGatewayRequest {
    intent: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    action: runtime::RuntimeOrchestrationAction,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    template_hint: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    evidence_refs: Vec<String>,
    #[serde(default)]
    constraints: runtime::RuntimeOrchestrationConstraints,
    #[serde(default)]
    surface: Option<String>,
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
            let active_evolution = crate::current_active_evolution_capability_overlay();
            return serde_json::to_string_pretty(
                &runtime::runtime_capabilities_response_with_detail_and_overlay(
                    &input.intent,
                    input.surface.as_deref(),
                    input.profile.as_deref(),
                    input.detail.as_deref(),
                    &active_evolution,
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
            if let Some(output) = self.try_execute_gateway_runtime_orchestration(&value)? {
                return Ok(output);
            }
            return serde_json::to_string_pretty(
                &runtime::runtime_orchestration_response_with_host(value, Some(self)),
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

    fn try_execute_gateway_runtime_orchestration(
        &self,
        value: &serde_json::Value,
    ) -> Result<Option<String>, ToolError> {
        let request: RuntimeOrchestrateGatewayRequest = match serde_json::from_value(value.clone())
        {
            Ok(request) => request,
            Err(_) => return Ok(None),
        };
        match request.action {
            runtime::RuntimeOrchestrationAction::RequestTeam => {
                let Some(session_id) = request
                    .session_id
                    .as_deref()
                    .filter(|session_id| !session_id.trim().is_empty())
                else {
                    return Ok(None);
                };
                ensure_gateway_mission_session(session_id, &request.intent)?;
                let team = start_team_runtime_with_spawner(
                    session_id,
                    request.intent.clone(),
                    None,
                    MissionTeamExecutionMode::ProviderInProcess,
                )
                .map_err(ToolError::new)?;
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
                let action_selection =
                    runtime::build_runtime_action_selection_report(&request.intent, None);
                let selected_template = action_selection
                    .recommended_template
                    .map(|template| template.as_str().to_string());
                serde_json::to_string_pretty(&serde_json::json!({
                    "type": "runtime_orchestration_result",
                    "request_id": format!("runtime-orch-{}", uuid::Uuid::new_v4()),
                    "status": "running",
                    "decision": {
                        "selected_pattern": action_selection.recommended_pattern,
                        "selected_template": selected_template,
                        "reason": request.reason.unwrap_or_else(|| action_selection.reason.clone()),
                        "policy_gates": ["gateway_session_auto_bound", "mission_session_ensured", "team_spawner_provider_in_process"],
                        "budget": {},
                        "permission": {"mode": "workspace_write", "team_execution_mode": "provider_in_process"},
                        "status": "accepted"
                    },
                    "execution": {
                        "type": "team_runtime",
                        "status": "running",
                        "execution_fidelity": "gateway_mission_team_spawner",
                        "team": team,
                        "workgraph": workgraph,
                        "mission": runtime::global_mission_runtime().projection(),
                        "control_actions": ["inspect", "tick_ready", "synthesis", "handoff", "cancel", "pause"],
                        "note": "Gateway-bound runtime_orchestrate used MissionService spawner semantics and starts provider-backed lifecycle agents for real team execution."
                    },
                    "evidence": {
                        "type": "runtime_orchestration_evidence",
                        "runtime_action": "use_team_template",
                        "tool_action": "request_team",
                        "runtime_owner": "runtime.orchestration",
                        "gateway_adapter": "mission_team_spawner",
                        "session_id": session_id,
                    },
                    "action_selection_report": action_selection,
                    "next_model_guidance": "Inspect mission projection and tick/dispatch the team when the task should continue; use runtime_capabilities for read-only planning before additional stateful orchestration."
                }))
                .map(Some)
                .map_err(|error| ToolError::new(error.to_string()))
            }
            runtime::RuntimeOrchestrationAction::RequestSessionLink => {
                let Some(session_id) = request
                    .session_id
                    .as_deref()
                    .filter(|session_id| !session_id.trim().is_empty())
                else {
                    return Ok(None);
                };
                ensure_gateway_mission_session(session_id, &request.intent)?;
                let receipt =
                    runtime::SessionExecutionPlane::bridge(runtime::CrossSessionMessage {
                        from_session_id: session_id.to_string(),
                        target_ref: request
                            .template_hint
                            .clone()
                            .unwrap_or_else(|| format!("@{session_id}")),
                        command: request.intent.clone(),
                        actor: Some("runtime_orchestrate".to_string()),
                        evidence_refs: request.evidence_refs.clone(),
                    });
                serde_json::to_string_pretty(&serde_json::json!({
                    "type": "runtime_orchestration_result",
                    "request_id": format!("runtime-orch-{}", uuid::Uuid::new_v4()),
                    "status": receipt.status,
                    "execution": {
                        "type": "session_link",
                        "execution_fidelity": "gateway_session_bridge",
                        "receipt": receipt,
                        "mission": runtime::global_mission_runtime().projection(),
                    },
                    "evidence": {
                        "type": "runtime_orchestration_evidence",
                        "runtime_action": "dispatch_session",
                        "tool_action": "request_session_link",
                        "gateway_adapter": "session_execution_bridge",
                        "session_id": session_id,
                    },
                    "action_selection_report": runtime::build_runtime_action_selection_report(&request.intent, None),
                    "next_model_guidance": "Use the returned session command or route receipt as the traceable cross-session handoff."
                }))
                .map(Some)
                .map_err(|error| ToolError::new(error.to_string()))
            }
            _ => Ok(None),
        }
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
}

impl runtime::RuntimeToolExecutionHost for GatewayToolExecutor {
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
