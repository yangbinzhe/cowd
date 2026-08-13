use std::sync::{Arc, Mutex, OnceLock};

use runtime::{ConfigLoader, ToolError, ToolExecutor};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tools::permissions::PermissionMode as ToolPermissionMode;
use tools::ToolHost;
#[cfg(test)]
use tools::ToolHostSnapshot;

use crate::lark_cli_tool::{execute_lark_cli_tool, LarkCliToolMode, LarkCliToolRequest};
#[cfg(test)]
use crate::runtime_bootstrap::GatewayToolRegistry;
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

fn parse_qualified_mcp_name(qualified: &str) -> Result<(String, String), ToolError> {
    let Some((server, tool)) = qualified
        .strip_prefix("mcp__")
        .and_then(|value| value.split_once("__"))
    else {
        return Err(ToolError::new(format!(
            "invalid MCP tool name `{qualified}`; expected `mcp__server__tool`"
        )));
    };
    if server.is_empty() || tool.is_empty() {
        return Err(ToolError::new(format!(
            "invalid MCP tool name `{qualified}`; server and tool are required"
        )));
    }
    Ok((server.to_string(), tool.to_string()))
}

#[derive(Debug, Deserialize)]
struct RuntimeCapabilitiesRequest {
    intent: String,
    surface: Option<String>,
    profile: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RuntimeConfigViewRequest {
    detail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContextRemainingRequest {
    #[serde(default)]
    detail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeResourceCapabilitiesRequest {
    resource_kind: String,
    mime: Option<String>,
    intent: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TeamBoardToolRequest {
    operation: String,
    expected_revision: Option<u64>,
    kind: Option<runtime::TeamWorkingStateKind>,
    summary: Option<String>,
    #[serde(default)]
    refs: Vec<String>,
    #[serde(default)]
    artifact_refs: Vec<String>,
    #[serde(default)]
    visibility: runtime::TeamWorkingStateVisibility,
    after_revision: Option<u64>,
    exact_revision: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceRetrieveToolRequest {
    evidence_ref: String,
    #[serde(default)]
    selector: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ContextRetrieveSource {
    Memory,
    SessionCatalog,
    SessionHistory,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ContextRetrieveScope {
    Current,
    RelatedSessions,
    WorkspaceSessions,
    ExplicitSession,
}

#[derive(Debug, Deserialize)]
struct ContextRetrieveRequest {
    source: ContextRetrieveSource,
    #[serde(default)]
    query: Option<String>,
    memory_id: Option<String>,
    scope: Option<ContextRetrieveScope>,
    session_id: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    before_sequence: Option<usize>,
    message_id: Option<String>,
    sequence: Option<usize>,
    block_cursor: Option<usize>,
    block_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeToolExecutionBinding<'a> {
    session_id: Option<&'a str>,
    authorized_scopes: &'a [String],
    memory_context: Option<&'a memory::MemoryTurnContext>,
    model_lease: Option<&'a str>,
    parent_execution: Option<&'a harness_contract::execution_graph::ExecutionParentBinding>,
    execution_decision: Option<&'a runtime::RuntimeExecutionDecision>,
    permission_ceiling: harness_contract::policy::PermissionMode,
}

fn is_gateway_runtime_control_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "runtime_config_view"
            | "runtime_resource_capabilities"
            | "runtime_capabilities"
            | "runtime_orchestrate"
            | "mcp_tool"
            | "list_mcp_resources_tool"
            | "read_mcp_resource_tool"
            | "lark_cli_read"
            | "lark_cli_write"
            | "team_board"
            | "evidence_retrieve"
            | "get_context_remaining"
    )
}

fn is_gateway_context_tool(tool_name: &str) -> bool {
    tool_name == "context_retrieve"
}

fn effective_runtime_execution_decision(
    request_decision: Option<&runtime::RuntimeExecutionDecision>,
    shared_fallback: Option<runtime::RuntimeExecutionDecision>,
) -> Option<runtime::RuntimeExecutionDecision> {
    request_decision.cloned().or(shared_fallback)
}

pub(crate) struct GatewayToolExecutor {
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    tool_host: Arc<ToolHost>,
    runtime_session_id: Option<String>,
    runtime_memory_context: Option<memory::MemoryTurnContext>,
    runtime_model_lease: Option<String>,
    runtime_permission_ceiling: harness_contract::policy::PermissionMode,
    runtime_execution_decision: Arc<Mutex<Option<runtime::RuntimeExecutionDecision>>>,
    runtime_services: Arc<OnceLock<Arc<runtime::RuntimeServices>>>,
}

impl GatewayToolExecutor {
    fn input_contract_error(&self, tool_name: &str, error: impl std::fmt::Display) -> ToolError {
        let lease = self.tool_host.pin_snapshot();
        let definition = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .find(|definition| definition.name == tool_name);
        let allowed_fields = definition
            .as_ref()
            .and_then(|definition| definition.input_schema.get("properties"))
            .and_then(serde_json::Value::as_object)
            .map(|properties| properties.keys().cloned().collect())
            .unwrap_or_default();
        let schema_hash = lease
            .catalog_receipt()
            .descriptors
            .into_iter()
            .find(|descriptor| descriptor.canonical_id == tool_name)
            .map(|descriptor| descriptor.schema_hash);
        ToolError::from_failure(
            harness_contract::tool::ToolExecutionFailure::input_contract(
                tool_name,
                error.to_string(),
                schema_hash,
                allowed_fields,
            ),
        )
    }

    #[cfg(test)]
    pub(crate) fn new(
        allowed_tools: Option<AllowedToolSet>,
        emit_output: bool,
        tool_registry: GatewayToolRegistry,
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
            runtime_session_id: None,
            runtime_memory_context: None,
            runtime_model_lease: None,
            runtime_permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
            runtime_execution_decision: Arc::new(Mutex::new(None)),
            runtime_services: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn from_tool_host(
        allowed_tools: Option<AllowedToolSet>,
        emit_output: bool,
        tool_host: Arc<ToolHost>,
    ) -> Self {
        Self {
            emit_output,
            allowed_tools,
            tool_host,
            runtime_session_id: None,
            runtime_memory_context: None,
            runtime_model_lease: None,
            runtime_permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
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

    /// Bind active Memory retrieval to the same exact lease used by passive
    /// context assembly for this ConversationRuntime.
    #[must_use]
    pub(crate) fn with_runtime_memory_context(
        mut self,
        context: memory::MemoryTurnContext,
    ) -> Self {
        self.runtime_memory_context = Some(context);
        self
    }

    /// Bind orchestration spawned from this conversation to the exact model
    /// selected for the parent runtime. Agent graphs may not silently fall
    /// back to a fictional `default` model lease.
    #[must_use]
    pub(crate) fn with_runtime_model_lease(mut self, model: impl Into<String>) -> Self {
        let model = model.into();
        if !model.trim().is_empty() {
            self.runtime_model_lease = Some(model);
        }
        self
    }

    #[must_use]
    pub(crate) fn with_runtime_permission_ceiling(
        mut self,
        permission_ceiling: harness_contract::policy::PermissionMode,
    ) -> Self {
        self.runtime_permission_ceiling = permission_ceiling;
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

    #[cfg(test)]
    pub(crate) async fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        <Self as ToolExecutor>::execute_output(self, tool_name, input)
            .await
            .map(|output| output.model_text().to_string())
    }

    #[cfg(test)]
    pub(crate) async fn execute_authorized(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<String, ToolError> {
        <Self as ToolExecutor>::execute_authorized_output(self, authorization, tool_name, input)
            .await
            .map(|output| output.model_text().to_string())
    }

    fn execute_search_tool(&self, value: serde_json::Value) -> Result<String, ToolError> {
        let input: ToolSearchRequest = serde_json::from_value(value)
            .map_err(|error| self.input_contract_error("tool_search", error))?;
        let mcp_health = self
            .tool_host
            .pin_snapshot()
            .snapshot()
            .mcp
            .as_ref()
            .and_then(|service| service.health().ok());
        let pending_mcp_servers = mcp_health
            .as_ref()
            .and_then(|health| health.get("pending_servers"))
            .and_then(serde_json::Value::as_array)
            .map(|servers| {
                servers
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|servers| !servers.is_empty());
        let mcp_degraded = mcp_health
            .as_ref()
            .and_then(|health| health.get("degraded"))
            .filter(|degraded| !degraded.is_null())
            .cloned();
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
            let matches_count = matches.as_array().map(|items| items.len()).unwrap_or(0);
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
            object.insert(
                "ordering".to_string(),
                serde_json::json!({
                    "strategy": "relevance",
                    "ranked": true,
                    "total_candidates": matches_count,
                    "note": "descriptors and activation_candidates are returned in descending relevance; model selection should prefer the first candidates.",
                }),
            );
        }
        serde_json::to_string_pretty(&value).map_err(|error| ToolError::new(error.to_string()))
    }

    async fn execute_runtime_tool(
        &self,
        tool_name: &str,
        value: serde_json::Value,
    ) -> Result<String, ToolError> {
        self.execute_runtime_tool_with_binding(
            tool_name,
            value,
            RuntimeToolExecutionBinding {
                session_id: self.runtime_session_id.as_deref(),
                authorized_scopes: &[],
                memory_context: self.runtime_memory_context.as_ref(),
                model_lease: self.runtime_model_lease.as_deref(),
                parent_execution: None,
                execution_decision: None,
                permission_ceiling: self.runtime_permission_ceiling,
            },
        )
        .await
    }

    async fn execute_runtime_tool_with_binding(
        &self,
        tool_name: &str,
        value: serde_json::Value,
        binding: RuntimeToolExecutionBinding<'_>,
    ) -> Result<String, ToolError> {
        if matches!(tool_name, "lark_cli_read" | "lark_cli_write") {
            let input: LarkCliToolRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            let workspace_root = self.tool_host.workspace_root();
            let config = ConfigLoader::default_for(workspace_root)
                .load()
                .map_err(|error| {
                    ToolError::new(format!("load active runtime configuration: {error}"))
                })?;
            let mode = if tool_name == "lark_cli_read" {
                LarkCliToolMode::Read
            } else {
                LarkCliToolMode::Write
            };
            return execute_lark_cli_tool(config.gateway(), input, mode).map_err(ToolError::new);
        }
        if tool_name == "runtime_config_view" {
            let input: RuntimeConfigViewRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            return self.execute_runtime_config_view(input);
        }
        if tool_name == "runtime_resource_capabilities" {
            let input: RuntimeResourceCapabilitiesRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            return self.execute_runtime_resource_capabilities(input);
        }
        if tool_name == "get_context_remaining" {
            let input: ContextRemainingRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            return self
                .execute_get_context_remaining(input, binding.session_id)
                .await;
        }
        if tool_name == "context_retrieve" {
            let input: ContextRetrieveRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            return self.execute_context_retrieve(input, binding).await;
        }
        if tool_name == "team_board" {
            let input: TeamBoardToolRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            let parent = match binding.parent_execution {
                Some(parent) => parent,
                None => {
                    if matches!(input.operation.as_str(), "read_after" | "read_exact") {
                        return serde_json::to_string_pretty(&serde_json::json!({
                            "available": false,
                            "revisions": {},
                            "hint": "team_board read requires an active Team Agent execution binding; use runtime_capabilities or runtime_orchestrate inspect to view team_board_revisions",
                        }))
                        .map_err(|error| ToolError::new(error.to_string()));
                    }
                    return Err(ToolError::new(
                        "team_board publish requires an immutable Team Agent execution binding",
                    ));
                }
            };
            let services = self.runtime_services.get().cloned().ok_or_else(|| {
                ToolError::new("team_board requires the workspace RuntimeServices")
            })?;
            let state = match input.operation.as_str() {
                "publish" => {
                    services
                        .team_runtime()
                        .publish_working_state(runtime::TeamWorkingStatePublishRequest {
                            graph_id: parent.execution_id.clone(),
                            node_id: parent.node_id.clone(),
                            expected_revision: input.expected_revision.ok_or_else(|| {
                                ToolError::new("team_board publish requires expected_revision")
                            })?,
                            kind: input.kind.ok_or_else(|| {
                                ToolError::new("team_board publish requires kind")
                            })?,
                            summary: input.summary.ok_or_else(|| {
                                ToolError::new("team_board publish requires summary")
                            })?,
                            refs: input.refs,
                            artifact_refs: input.artifact_refs,
                            visibility: input.visibility,
                        })
                        .await
                }
                "read_after" | "read_exact" => services.team_runtime().read_working_state(
                    runtime::TeamWorkingStateReadRequest {
                        graph_id: parent.execution_id.clone(),
                        node_id: parent.node_id.clone(),
                        after_revision: input.after_revision,
                        exact_revision: input.exact_revision,
                    },
                ),
                _ => Err(
                    "team_board operation must be publish, read_after, or read_exact".to_string(),
                ),
            }
            .map_err(ToolError::new)?;
            return serde_json::to_string_pretty(&state)
                .map_err(|error| ToolError::new(error.to_string()));
        }
        if tool_name == "evidence_retrieve" {
            let input: EvidenceRetrieveToolRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            return self
                .execute_evidence_retrieve(input, binding.session_id, binding.authorized_scopes)
                .await;
        }
        if tool_name == "runtime_capabilities" {
            let input: RuntimeCapabilitiesRequest = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
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
                    leased_decision.as_ref(),
                    &self.available_tool_names(),
                ),
            )
            .map_err(|error| ToolError::new(error.to_string()));
        }
        if tool_name == harness_contract::orchestration::RUNTIME_ORCHESTRATE_TOOL_ID {
            let input = serde_json::from_value::<
                harness_contract::orchestration::ModelRuntimeOrchestrationInput,
            >(value)
            .map_err(|error| self.input_contract_error(tool_name, error))?;
            if input.operation
                == harness_contract::orchestration::RuntimeOrchestrationOperation::RouteInput
            {
                return Err(ToolError::new(
                    "runtime_orchestrate route_input is unsupported (fail-closed); available operations: inspect, propose, revise, control",
                ));
            }
            let leased_decision = self
                .runtime_execution_decision
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let services = self.runtime_services.get().cloned().ok_or_else(|| {
                ToolError::new("runtime_orchestrate requires the workspace RuntimeServices Runner")
            })?;
            let lineage = binding.parent_execution.and_then(|parent| {
                services
                    .graph_state_store()
                    .load(&parent.execution_id)
                    .ok()
                    .and_then(|graph| graph.lineage)
            });
            let mission_id = lineage.as_ref().and_then(|lineage| {
                services
                    .task_aggregate_service()
                    .get(&lineage.root_task_id)
                    .ok()
                    .flatten()
                    .map(|task| task.mission_id)
            });
            let mut request = runtime::RuntimeOrchestrationCommand::from_model(
                input,
                runtime::RuntimeOrchestrationBinding {
                    model_lease: binding.model_lease.map(str::to_string),
                    session_id: binding.session_id.map(str::to_string),
                    lineage,
                    mission_id,
                    selection_mode: None,
                    strategy_binding: None,
                    capabilities: Vec::new(),
                    surface: None,
                    permission_ceiling: binding.permission_ceiling,
                },
            );
            self.bind_delegated_capabilities(&mut request);
            let decision =
                effective_runtime_execution_decision(binding.execution_decision, leased_decision);
            let result = runtime::submit_runtime_orchestration_request(
                request,
                decision.as_ref(),
                services.as_ref(),
                binding.parent_execution.cloned(),
            )
            .await;
            tracing::info!(
                status = %result.status,
                selected_pattern = %result.decision.selected_pattern.as_str(),
                findings = ?result.decision.validation_findings,
                "runtime orchestration request completed"
            );
            if matches!(
                result.status.as_str(),
                "rejected" | "unavailable" | "blocked" | "failed"
            ) {
                let execution = serde_json::to_string(&result.execution)
                    .unwrap_or_else(|_| "{\"type\":\"unserializable_execution\"}".to_string());
                return Err(ToolError::new(format!(
                    "runtime orchestration {}: {}; execution={execution}",
                    result.status,
                    result.decision.validation_findings.join(", ")
                )));
            }
            // The full orchestration graph is retained as durable raw tool
            // evidence by ConversationRuntime. The parent model receives the
            // typed terminal receipt only; otherwise a completed team graph
            // recursively injects every child projection into the next turn.
            return serde_json::to_string_pretty(&result.model_receipt())
                .map_err(|error| ToolError::new(error.to_string()));
        }

        if tool_name == "read_mcp_resource_tool" {
            if let Some(uri) = value
                .get("uri")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
            {
                if uri.starts_with("session://") || uri.starts_with("memory:") {
                    return Err(ToolError::new(
                        "Session and Memory evidence references are audit locators, not MCP resources. Use `context_retrieve` and its returned `read_request` or `next_request` to read authorized content.",
                    ));
                }
            }
        }
        let service = self
            .tool_host
            .pin_snapshot()
            .snapshot()
            .mcp
            .clone()
            .ok_or_else(|| {
                ToolError::new(format!(
                    "runtime tool `{tool_name}` is unavailable without configured MCP servers"
                ))
            })?;
        match tool_name {
            "mcp_tool" => {
                serde_json::from_value::<McpToolRequest>(value.clone())
                    .map_err(|error| self.input_contract_error(tool_name, error))?;
            }
            "list_mcp_resources_tool" => {
                serde_json::from_value::<ListMcpResourcesRequest>(value.clone())
                    .map_err(|error| self.input_contract_error(tool_name, error))?;
            }
            "read_mcp_resource_tool" => {
                serde_json::from_value::<ReadMcpResourceRequest>(value.clone())
                    .map_err(|error| self.input_contract_error(tool_name, error))?;
            }
            _ => {}
        }
        let tool_name = tool_name.to_string();
        runtime::ToolExecutionPlane::adapt_blocking(move || {
            let output = match tool_name.as_str() {
                "mcp_tool" => {
                    let input: McpToolRequest = serde_json::from_value(value).map_err(|error| {
                        ToolError::new(format!("invalid tool input JSON: {error}"))
                    })?;
                    let qualified_name = input
                        .qualified_name
                        .or(input.tool)
                        .ok_or_else(|| ToolError::new("missing required field `qualifiedName`"))?;
                    let (server, tool) = parse_qualified_mcp_name(&qualified_name)?;
                    serde_json::to_value(
                        service
                            .call_tool(mcp::McpToolCallRequest {
                                server,
                                tool,
                                input: input.arguments.unwrap_or_else(|| serde_json::json!({})),
                            })
                            .map_err(|error| ToolError::new(error.to_string()))?,
                    )
                }
                "list_mcp_resources_tool" => {
                    let input: ListMcpResourcesRequest =
                        serde_json::from_value(value).map_err(|error| {
                            ToolError::new(format!("invalid tool input JSON: {error}"))
                        })?;
                    serde_json::to_value(
                        service
                            .list_resources(input.server.as_deref())
                            .map_err(|error| ToolError::new(error.to_string()))?,
                    )
                }
                "read_mcp_resource_tool" => {
                    let input: ReadMcpResourceRequest =
                        serde_json::from_value(value).map_err(|error| {
                            ToolError::new(format!("invalid tool input JSON: {error}"))
                        })?;
                    serde_json::to_value(
                        service
                            .read_resource(&input.server, &input.uri)
                            .map_err(|error| ToolError::new(error.to_string()))?,
                    )
                }
                _ => {
                    let (server, tool) = parse_qualified_mcp_name(&tool_name)?;
                    serde_json::to_value(
                        service
                            .call_tool(mcp::McpToolCallRequest {
                                server,
                                tool,
                                input: value,
                            })
                            .map_err(|error| ToolError::new(error.to_string()))?,
                    )
                }
            }
            .map_err(|error| ToolError::new(error.to_string()))?;
            serde_json::to_string_pretty(&output).map_err(|error| ToolError::new(error.to_string()))
        })
        .await
        .map_err(|error| ToolError::new(error.to_string()))?
    }

    async fn execute_context_retrieve(
        &self,
        input: ContextRetrieveRequest,
        binding: RuntimeToolExecutionBinding<'_>,
    ) -> Result<String, ToolError> {
        let query = input
            .query
            .as_deref()
            .map(str::trim)
            .filter(|query| !query.is_empty())
            .map(str::to_string);
        let session_id = binding
            .session_id
            .filter(|session_id| !session_id.trim().is_empty())
            .ok_or_else(|| {
                ToolError::new("context_retrieve requires a Runtime-bound session identity")
            })?
            .to_string();
        let services = self.runtime_services.get().cloned().ok_or_else(|| {
            ToolError::new("context_retrieve requires the workspace RuntimeServices")
        })?;
        let limit = input.limit.unwrap_or(8).clamp(1, 16);
        if input.memory_id.is_some() && input.source != ContextRetrieveSource::Memory {
            return Err(ToolError::new("memory_id is valid only with source=memory"));
        }
        if input.source == ContextRetrieveSource::Memory
            && input.memory_id.is_some()
            && query.is_some()
        {
            return Err(ToolError::new(
                "memory retrieval accepts either query or memory_id, not both",
            ));
        }

        let value = match input.source {
            ContextRetrieveSource::Memory => {
                if input
                    .scope
                    .is_some_and(|scope| scope != ContextRetrieveScope::Current)
                {
                    return Err(ToolError::new(
                        "memory retrieval uses only the current Runtime Binding",
                    ));
                }
                let Some(manager) = services.memory_manager() else {
                    return serde_json::to_string_pretty(&serde_json::json!({
                        "kind": "runtime.context_retrieval",
                        "source": "memory",
                        "scope": "current_binding",
                        "status": "degraded",
                        "reason": "memory manager is not configured",
                        "selected": [],
                    }))
                    .map_err(|error| ToolError::new(error.to_string()));
                };
                let Some(context) = binding.memory_context.cloned() else {
                    return serde_json::to_string_pretty(&serde_json::json!({
                        "kind": "runtime.context_retrieval",
                        "source": "memory",
                        "scope": "current_binding",
                        "status": "degraded",
                        "reason": "Runtime did not supply an exact Memory Binding",
                        "selected": [],
                    }))
                    .map_err(|error| ToolError::new(error.to_string()));
                };
                let kernel = memory::MemoryKernel::new(manager);
                if let Some(memory_id) = input
                    .memory_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|memory_id| !memory_id.is_empty())
                {
                    let memory_id = uuid::Uuid::try_parse(memory_id)
                        .map_err(|_| ToolError::new("memory_id must be a valid Memory UUID"))?;
                    let entry = kernel
                        .retrieve_visible_entry(&context, memory_id)
                        .await
                        .map_err(|error| ToolError::new(error.to_string()))?;
                    let selected = entry
                        .map(|entry| {
                            let (content, truncated) = bounded_context_text(&entry.content, 8_192);
                            serde_json::json!({
                                "memory_id": entry.id,
                                "layer": format!("{:?}", entry.layer),
                                "category": format!("{:?}", entry.category),
                                "title": entry.title,
                                "content": content,
                                "content_truncated": truncated,
                                "scope": entry.scope.to_string(),
                                "updated_at": entry.updated_at,
                                "evidence_ref": format!("memory:{}", entry.id),
                            })
                        })
                        .into_iter()
                        .collect::<Vec<_>>();
                    serde_json::json!({
                        "kind": "runtime.context_retrieval",
                        "source": "memory",
                        "scope": "current_binding",
                        "status": "completed",
                        "memory_id": memory_id,
                        "selected_count": selected.len(),
                        "selected": selected,
                        "authorization": "exact Runtime Memory Binding",
                        "reference_contract": context_reference_contract(),
                    })
                } else {
                    let query = query.as_deref().ok_or_else(|| {
                        ToolError::new("memory retrieval requires query or memory_id")
                    })?;
                    let packet = kernel
                        .retrieve_packet_preview(&context, query, limit, 8_192)
                        .await
                        .map_err(|error| ToolError::new(error.to_string()))?;
                    serde_json::json!({
                        "kind": "runtime.context_retrieval",
                        "source": "memory",
                        "scope": "current_binding",
                        "status": "completed",
                        "query": query,
                        "selected": packet.selected.iter().map(|item| serde_json::json!({
                            "memory_id": item.atom.id,
                            "layer": format!("{:?}", item.atom.layer),
                            "role": format!("{:?}", item.role),
                            "title": item.atom.title,
                            "preview": item.content_preview,
                            "reason": item.reason,
                            "evidence_ref": item.atom.evidence_pointer,
                            "read_request": {
                                "source": "memory",
                                "scope": "current",
                                "memory_id": item.atom.id,
                            },
                        })).collect::<Vec<_>>(),
                        "selected_count": packet.selected.len(),
                        "omitted_count": packet.omitted.len(),
                        "truncated": packet.truncated,
                        "token_estimate": packet.token_estimate,
                        "reference_contract": context_reference_contract(),
                    })
                }
            }
            ContextRetrieveSource::SessionCatalog => {
                let query = query.as_deref().ok_or_else(|| {
                    ToolError::new("session_catalog retrieval requires a focused query")
                })?;
                if input
                    .scope
                    .is_some_and(|scope| scope != ContextRetrieveScope::WorkspaceSessions)
                {
                    return Err(ToolError::new(
                        "session_catalog supports only workspace_sessions scope",
                    ));
                }
                let Some(history) = services.session_history_reader() else {
                    return serde_json::to_string_pretty(&serde_json::json!({
                        "kind": "runtime.context_retrieval",
                        "source": "session_catalog",
                        "scope": "workspace_sessions",
                        "status": "degraded",
                        "reason": "session history reader is not configured",
                        "selected": [],
                    }))
                    .map_err(|error| ToolError::new(error.to_string()));
                };
                let offset = input.offset.unwrap_or(0);
                let page = history
                    .discover_browsable_sessions(&session_id, Some(query), limit, offset)
                    .await
                    .map_err(|error| ToolError::new(error.to_string()))?;
                serde_json::json!({
                    "kind": "runtime.context_retrieval",
                    "source": "session_catalog",
                    "scope": "workspace_sessions",
                    "status": "completed",
                    "query": query,
                    "selected": page.records.iter().map(|record| serde_json::json!({
                        "session_id": record.session_id,
                        "title": session_record_title(record),
                        "platform": record.platform,
                        "status": record.status,
                        "last_activity": record.last_activity,
                        "message_count": record.message_count,
                        "evidence_ref": format!("session://{}", record.session_id),
                        "read_request": {
                            "source": "session_history",
                            "scope": "explicit_session",
                            "session_id": record.session_id,
                            "limit": limit,
                        },
                    })).collect::<Vec<_>>(),
                    "selected_count": page.records.len(),
                    "total": page.total,
                    "offset": offset,
                    "next_offset": (offset + page.records.len() < page.total)
                        .then_some(offset + page.records.len()),
                    "next_request": (offset + page.records.len() < page.total)
                        .then(|| serde_json::json!({
                            "source": "session_catalog",
                            "scope": "workspace_sessions",
                            "query": query,
                            "limit": limit,
                            "offset": offset + page.records.len(),
                        })),
                    "truncated": offset + page.records.len() < page.total,
                    "authorization": "same durable workspace and actor identity",
                    "reference_contract": context_reference_contract(),
                })
            }
            ContextRetrieveSource::SessionHistory => {
                let Some(history) = services.session_history_reader() else {
                    return serde_json::to_string_pretty(&serde_json::json!({
                        "kind": "runtime.context_retrieval",
                        "source": "session_history",
                        "scope": "current",
                        "status": "degraded",
                        "reason": "session history reader is not configured",
                        "selected": [],
                    }))
                    .map_err(|error| ToolError::new(error.to_string()));
                };
                let retrieval_scope = input.scope.unwrap_or(ContextRetrieveScope::Current);
                let mut authorized_sessions =
                    std::collections::BTreeSet::from([session_id.clone()]);
                for relation in services.session_relations().relations_for(&session_id) {
                    if relation.from_session_id == session_id {
                        authorized_sessions.insert(relation.to_session_id);
                    } else if relation.to_session_id == session_id {
                        authorized_sessions.insert(relation.from_session_id);
                    }
                }
                let target_session_id = match retrieval_scope {
                    ContextRetrieveScope::Current => session_id.clone(),
                    ContextRetrieveScope::ExplicitSession => input
                        .session_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            ToolError::new(
                                "explicit_session retrieval requires a target session_id",
                            )
                        })?
                        .to_string(),
                    ContextRetrieveScope::RelatedSessions => session_id.clone(),
                    ContextRetrieveScope::WorkspaceSessions => session_id.clone(),
                };
                let explicitly_authorized = authorized_sessions.contains(&target_session_id);
                let workspace_authorized = if retrieval_scope
                    == ContextRetrieveScope::ExplicitSession
                    && !explicitly_authorized
                {
                    history
                        .can_read_session(&session_id, &target_session_id)
                        .await
                        .map_err(|error| ToolError::new(error.to_string()))?
                } else {
                    false
                };
                if retrieval_scope == ContextRetrieveScope::ExplicitSession
                    && !explicitly_authorized
                    && !workspace_authorized
                {
                    return Err(ToolError::new(format!(
                        "target Session `{target_session_id}` is outside the current Session's durable workspace/actor boundary and has no explicit relation"
                    )));
                }
                if input.message_id.is_some() && input.sequence.is_some() {
                    return Err(ToolError::new(
                        "exact Session retrieval accepts message_id or sequence, not both",
                    ));
                }
                if input.message_id.is_some() || input.sequence.is_some() {
                    if query.is_some()
                        || retrieval_scope == ContextRetrieveScope::RelatedSessions
                        || retrieval_scope == ContextRetrieveScope::WorkspaceSessions
                        || input.before_sequence.is_some()
                    {
                        return Err(ToolError::new(
                            "exact Session retrieval cannot be combined with query, related_sessions, or before_sequence",
                        ));
                    }
                    let message = if let Some(message_id) = input
                        .message_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                    {
                        history
                            .message_by_stable_id(&target_session_id, message_id)
                            .await
                    } else {
                        history
                            .message_by_sequence(
                                &target_session_id,
                                input.sequence.expect("exact selector was checked"),
                            )
                            .await
                    }
                    .map_err(|error| ToolError::new(error.to_string()))?;
                    let Some(message) = message else {
                        return Err(ToolError::new("authorized Session message does not exist"));
                    };
                    let block_cursor = input.block_cursor.unwrap_or(0);
                    let block_limit = input.block_limit.unwrap_or(16).clamp(1, 128);
                    let exact = exact_session_message_page(
                        &message,
                        block_cursor,
                        block_limit,
                        retrieval_scope,
                    )?;
                    return serde_json::to_string_pretty(&exact)
                        .map_err(|error| ToolError::new(error.to_string()));
                }
                if retrieval_scope == ContextRetrieveScope::WorkspaceSessions {
                    let query = query.as_deref().ok_or_else(|| {
                        ToolError::new("workspace_sessions retrieval requires a focused query")
                    })?;
                    let mut offset = 0usize;
                    loop {
                        let page = history
                            .discover_browsable_sessions(&session_id, None, 24, offset)
                            .await
                            .map_err(|error| ToolError::new(error.to_string()))?;
                        let page_len = page.records.len();
                        authorized_sessions
                            .extend(page.records.into_iter().map(|record| record.session_id));
                        offset = offset.saturating_add(page_len);
                        if page_len == 0 || offset >= page.total || offset >= 512 {
                            break;
                        }
                    }
                    if query.trim().is_empty() {
                        return Err(ToolError::new(
                            "workspace_sessions retrieval requires a focused query",
                        ));
                    }
                }
                let authorized_sessions = authorized_sessions.into_iter().collect::<Vec<_>>();
                let (messages, next_before_sequence) = match retrieval_scope {
                    ContextRetrieveScope::Current | ContextRetrieveScope::ExplicitSession => {
                        if let Some(query) = query.as_deref() {
                            (
                                history
                                    .search_messages(query, &target_session_id, limit)
                                    .await,
                                None,
                            )
                        } else {
                            let count = history
                                .message_count(&target_session_id)
                                .await
                                .map_err(|error| ToolError::new(error.to_string()))?;
                            let end = input.before_sequence.unwrap_or(count).min(count);
                            let start = end.saturating_sub(limit);
                            (
                                history
                                    .messages(
                                        &target_session_id,
                                        start,
                                        end.saturating_sub(start).max(1),
                                    )
                                    .await,
                                (start > 0).then_some(start),
                            )
                        }
                    }
                    ContextRetrieveScope::RelatedSessions => {
                        let query = query.as_deref().ok_or_else(|| {
                            ToolError::new("related_sessions retrieval requires a focused query")
                        })?;
                        (
                            history
                                .search_messages_in_sessions(query, &authorized_sessions, limit)
                                .await,
                            None,
                        )
                    }
                    ContextRetrieveScope::WorkspaceSessions => {
                        let query = query.as_deref().ok_or_else(|| {
                            ToolError::new("workspace_sessions retrieval requires a focused query")
                        })?;
                        (
                            history
                                .search_messages_in_sessions(query, &authorized_sessions, limit)
                                .await,
                            None,
                        )
                    }
                };
                let messages = messages.map_err(|error| ToolError::new(error.to_string()))?;
                let authorization_basis = if target_session_id == session_id {
                    "current_session"
                } else if explicitly_authorized {
                    "durable_session_relation"
                } else {
                    "same_workspace_and_actor"
                };
                let truncated = if query.is_some() {
                    messages.len() == limit
                } else {
                    next_before_sequence.is_some()
                };
                serde_json::json!({
                    "kind": "runtime.context_retrieval",
                    "source": "session_history",
                    "scope": match retrieval_scope {
                        ContextRetrieveScope::Current => "current",
                        ContextRetrieveScope::RelatedSessions => "related_sessions",
                        ContextRetrieveScope::ExplicitSession => "explicit_session",
                        ContextRetrieveScope::WorkspaceSessions => "workspace_sessions",
                    },
                    "status": "completed",
                    "query": query,
                    "target_session_id": (retrieval_scope != ContextRetrieveScope::RelatedSessions)
                        .then_some(target_session_id.clone()),
                    "authorized_session_count": authorized_sessions.len(),
                    "selected": messages.iter().map(|message| serde_json::json!({
                        "message_id": message.stable_message_id,
                        "session_id": message.session_id,
                        "sequence": message.sequence,
                        "role": message.role,
                        "created_at_ms": message.created_at_ms,
                        "preview": session_message_preview(&message.content_json, 800),
                        "evidence_ref": format!(
                            "session://{}/messages/{}",
                            message.session_id, message.sequence
                        ),
                    })).collect::<Vec<_>>(),
                    "selected_count": messages.len(),
                    "next_before_sequence": next_before_sequence,
                    "next_request": next_before_sequence.map(|before_sequence| serde_json::json!({
                        "source": "session_history",
                        "scope": match retrieval_scope {
                            ContextRetrieveScope::Current => "current",
                            ContextRetrieveScope::ExplicitSession => "explicit_session",
                            ContextRetrieveScope::RelatedSessions => "related_sessions",
                            ContextRetrieveScope::WorkspaceSessions => "workspace_sessions",
                        },
                        "session_id": (retrieval_scope == ContextRetrieveScope::ExplicitSession)
                            .then_some(target_session_id.clone()),
                        "limit": limit,
                        "before_sequence": before_sequence,
                    })),
                    "truncated": truncated,
                    "authorization_basis": authorization_basis,
                    "reference_contract": context_reference_contract(),
                })
            }
        };
        serde_json::to_string_pretty(&value).map_err(|error| ToolError::new(error.to_string()))
    }

    fn execute_runtime_config_view(
        &self,
        input: RuntimeConfigViewRequest,
    ) -> Result<String, ToolError> {
        let config = ConfigLoader::default_for(self.tool_host.workspace_root())
            .load()
            .map_err(|error| {
                ToolError::new(format!("load active runtime configuration: {error}"))
            })?;
        let active_model = self
            .runtime_model_lease
            .clone()
            .or_else(|| config.resolved_model())
            .unwrap_or_else(|| "unresolved".to_string());
        let provider = config.providers().resolve_full(&active_model);
        let effective_protocol = provider
            .and_then(|provider| {
                model_protocol::provider_config::ProviderProtocol::effective_for_provider(provider)
                    .ok()
            })
            .map(|protocol| protocol.as_str().to_string())
            .unwrap_or_else(|| "locally inferred when request is built".to_string());
        let context_window = runtime::model_context_window_with_overrides(
            &active_model,
            Some(config.model_context_windows()),
        );
        let detail = input.detail.as_deref().unwrap_or("summary");
        let provider_projection = config
            .providers()
            .providers
            .values()
            .map(|provider| {
                serde_json::json!({
                    "name": provider.name,
                    "models": provider.models,
                    "protocol": model_protocol::provider_config::ProviderProtocol::effective_for_provider(provider)
                        .map(|protocol| protocol.as_str())
                        .unwrap_or("invalid"),
                })
            })
            .collect::<Vec<_>>();
        let mcp = self
            .tool_host
            .pin_snapshot()
            .snapshot()
            .mcp
            .as_ref()
            .map_or_else(
                || serde_json::json!({"configured": false, "servers": []}),
                |service| {
                    let servers = service.list_servers().unwrap_or_default();
                    serde_json::json!({
                        "configured": true,
                        "servers": servers.iter().map(|server| server.name.clone()).collect::<Vec<_>>(),
                        "pending_servers": servers.iter().filter(|server| server.status == "error").map(|server| server.name.clone()).collect::<Vec<_>>(),
                    })
                },
            );
        let response = match detail {
            "providers" => serde_json::json!({
                "kind": "runtime.config_view",
                "detail": "providers",
                "active_model": active_model,
                "effective_protocol": effective_protocol,
                "context_window": context_window,
                "providers": provider_projection,
                "fallback_models": config.fallbacks(),
            }),
            "policy" => serde_json::json!({
                "kind": "runtime.config_view",
                "detail": "policy",
                "permission_mode": format!("{:?}", config.permission_mode()),
                "approval": config.approval(),
                "runtime_control_enabled": config.runtime_control().policy.enabled,
                "memory_enabled": config.memory().enabled,
                "compression": config.compression().session,
                "mcp": mcp,
            }),
            "summary" => serde_json::json!({
                "kind": "runtime.config_view",
                "detail": "summary",
                "active_model": active_model,
                "effective_protocol": effective_protocol,
                "context_window": context_window,
                "permission_mode": format!("{:?}", config.permission_mode()),
                "fallback_models": config.fallbacks(),
                "mcp": mcp,
                "redaction": "credentials, headers, environment values, and config paths are intentionally unavailable",
            }),
            other => {
                return Err(ToolError::new(format!(
                    "unsupported runtime_config_view detail `{other}`; expected summary, providers, or policy"
                )));
            }
        };
        serde_json::to_string_pretty(&response).map_err(|error| ToolError::new(error.to_string()))
    }

    fn execute_runtime_resource_capabilities(
        &self,
        input: RuntimeResourceCapabilitiesRequest,
    ) -> Result<String, ToolError> {
        let kind = input.resource_kind.trim().to_ascii_lowercase();
        let desired_tools = match kind.as_str() {
            "image" => ["vision_analyze", "read_file", "read_many"].as_slice(),
            "audio" | "video" => ["bash", "execute_code", "read_file"].as_slice(),
            "pdf" | "document" | "archive" => ["bash", "read_file", "read_many"].as_slice(),
            "csv" => ["execute_code", "read_file", "read_many"].as_slice(),
            "text" | "markdown" | "code" => ["read_file", "read_many", "grep_many"].as_slice(),
            _ => ["read_file", "bash", "execute_code"].as_slice(),
        };
        let available = self.available_tool_names();
        let candidate_tools = desired_tools
            .iter()
            .filter(|tool| available.iter().any(|name| name == **tool))
            .map(|tool| (*tool).to_string())
            .collect::<Vec<_>>();
        // This is an explicit model tool call, so a bounded environment scan is
        // allowed. Registration/rendering never performs this discovery.
        let snapshot = runtime::ResourceCapabilitySnapshot::discover_environment();
        let keywords = resource_capability_keywords(&kind, input.mime.as_deref(), &input.intent);
        let filter_candidates = |values: Vec<String>, limit: usize| {
            values
                .into_iter()
                .filter(|value| capability_name_matches(value, &keywords))
                .take(limit)
                .collect::<Vec<_>>()
        };
        let response = serde_json::json!({
            "kind": "runtime.resource_capabilities",
            "resource_kind": kind,
            "mime": input.mime,
            "intent": input.intent,
            "candidate_tools": candidate_tools,
            "installed_skills": filter_candidates(snapshot.skills, 4),
            "installed_plugins": filter_candidates(snapshot.plugins, 4),
            "local_commands": filter_candidates(snapshot.local_commands, 4),
            "mcp_resource_actions": snapshot.mcp_resources.into_iter().take(2).collect::<Vec<_>>(),
            "discovery_boundary": "Candidates only. Invoke an exposed tool or approved installation path and verify output before claiming resource content.",
        });
        serde_json::to_string_pretty(&response).map_err(|error| ToolError::new(error.to_string()))
    }

    async fn execute_get_context_remaining(
        &self,
        input: ContextRemainingRequest,
        session_id: Option<&str>,
    ) -> Result<String, ToolError> {
        let config = ConfigLoader::default_for(self.tool_host.workspace_root())
            .load()
            .map_err(|error| {
                ToolError::new(format!("load active runtime configuration: {error}"))
            })?;
        let active_model = self
            .runtime_model_lease
            .clone()
            .or_else(|| config.resolved_model())
            .unwrap_or_else(|| "unresolved".to_string());
        let window = runtime::model_context_window_with_overrides(
            &active_model,
            Some(config.model_context_windows()),
        );
        let services = self.runtime_services.get().cloned();
        let usage = session_id.and_then(|sid| {
            services.as_ref().and_then(|services| {
                services
                    .session_execution_index(sid)
                    .latest_execution_id
                    .and_then(|execution_id| services.execution_live(&execution_id))
                    .and_then(|live| live.context_usage)
            })
        });
        let detail = input.detail.as_deref().unwrap_or("summary");
        let response = serde_json::json!({
            "kind": "get_context_remaining",
            "status": if usage.is_some() { "measured" } else { "window_only" },
            "detail": detail,
            "active_model": active_model,
            "context_window_tokens": usage
                .as_ref()
                .and_then(|usage| usage.window_tokens)
                .unwrap_or(u64::from(window)),
            "input_tokens": usage.as_ref().and_then(|usage| usage.input_tokens),
            "remaining_tokens": usage.as_ref().and_then(|usage| usage.remaining_tokens),
            "usage_percent_bp": usage.as_ref().and_then(|usage| usage.usage_percent_bp),
            "components": usage.map(|usage| usage.components).unwrap_or_default(),
            "hint": "window_only means no active execution ledger was found for this session; re-invoke while a turn is running for measured utilization.",
        });
        serde_json::to_string_pretty(&response).map_err(|error| ToolError::new(error.to_string()))
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
        let lease = self.tool_host.pin_snapshot();
        let tool_name = lease.snapshot().catalog.canonical_name(tool_name)?;
        lease
            .snapshot()
            .catalog
            .permission_specs(self.allowed_tools.as_ref())
            .ok()?
            .into_iter()
            .find_map(|(name, permission)| (name == tool_name).then_some(permission))
    }

    /// A team request must carry explicit least-privilege capabilities into
    /// its protocol packets. Models are not required to enumerate the local
    /// read-only catalog in tool JSON, and forwarding arbitrary names would
    /// let a prompt define its own delegation boundary. Gateway therefore
    /// intersects caller hints with its active catalog and adds the active
    /// read-only evidence tools. Lifecycle controls never propagate to leaf
    /// agents.
    fn bind_delegated_capabilities(&self, request: &mut runtime::RuntimeOrchestrationCommand) {
        let mut allowed_tools = self
            .available_tool_names()
            .into_iter()
            .filter(|name| !is_gateway_runtime_control_tool(name))
            .filter(|name| self.tool_permission_mode(name) == Some(ToolPermissionMode::ReadOnly))
            .collect::<Vec<_>>();
        allowed_tools.sort();
        allowed_tools.dedup();
        let allowed = allowed_tools
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();

        request.capabilities.retain(|capability| {
            capability
                .strip_prefix("tool:")
                .is_none_or(|tool| allowed.contains(tool))
        });
        request
            .capabilities
            .extend(allowed_tools.into_iter().map(|tool| format!("tool:{tool}")));
        request.capabilities.sort();
        request.capabilities.dedup();
    }
}

fn resource_capability_keywords(kind: &str, mime: Option<&str>, intent: &str) -> Vec<String> {
    let mut keywords = vec![kind.to_string()];
    // Installed command names describe implementation details rather than the
    // resource's MIME type. Keep the mapping here, beside the explicit
    // capability query, so normal attachment ingestion never probes PATH or
    // leaks an installation inventory into every model request.
    keywords.extend(
        match kind {
            "image" => ["vision", "image", "ocr"].as_slice(),
            "audio" => ["audio", "ffmpeg", "ffprobe", "transcribe"].as_slice(),
            "video" => ["video", "ffmpeg", "ffprobe", "transcribe"].as_slice(),
            "pdf" => ["pdf", "pdftotext", "pdfinfo", "document"].as_slice(),
            "document" => ["document", "pandoc", "unzip", "office"].as_slice(),
            "archive" => ["archive", "unzip", "tar"].as_slice(),
            "csv" => ["csv", "python", "dataframe"].as_slice(),
            "text" | "markdown" | "code" => ["text", "code", "grep"].as_slice(),
            _ => [].as_slice(),
        }
        .iter()
        .map(|value| (*value).to_string()),
    );
    if let Some(mime) = mime {
        keywords.extend(
            mime.split(|character: char| !character.is_ascii_alphanumeric())
                .filter(|part| part.len() >= 3)
                .map(str::to_ascii_lowercase),
        );
    }
    keywords.extend(
        intent
            .split(|character: char| !character.is_alphanumeric())
            .filter(|part| part.len() >= 4)
            .take(4)
            .map(str::to_ascii_lowercase),
    );
    keywords.sort();
    keywords.dedup();
    keywords
}

fn capability_name_matches(value: &str, keywords: &[String]) -> bool {
    let normalized = value.to_ascii_lowercase();
    keywords.iter().any(|keyword| normalized.contains(keyword))
}

fn context_reference_contract() -> serde_json::Value {
    serde_json::json!({
        "evidence_refs": "audit locators retained with the result; they are not MCP resources",
        "drill_down_tool": "context_retrieve",
        "instruction": "Evidence locators are not MCP resources. Use a selected item's read_request or the response next_request; do not pass session:// or memory: references to read_mcp_resource_tool.",
    })
}

fn bounded_context_text(content: &str, max_chars: usize) -> (String, bool) {
    let mut chars = content.chars();
    let bounded = chars.by_ref().take(max_chars).collect::<String>();
    let truncated = chars.next().is_some();
    (bounded, truncated)
}

fn session_message_preview(content_json: &str, max_chars: usize) -> String {
    let value = serde_json::from_str::<serde_json::Value>(content_json).unwrap_or_default();
    let blocks = value.as_array().map_or_else(Vec::new, Clone::clone);
    let mut parts = Vec::new();
    for block in blocks {
        let kind = block
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let text = match kind {
            "text" | "reasoning_summary" => block
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            "tool_use" => block
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(|name| format!("[tool:{name}]")),
            "tool_result" => block
                .get("output")
                .and_then(serde_json::Value::as_str)
                .map(|output| format!("[tool_result] {output}")),
            "image" => Some("[image]".to_string()),
            _ => None,
        };
        if let Some(text) = text.filter(|text| !text.trim().is_empty()) {
            parts.push(text);
        }
    }
    let joined = parts.join("\n");
    let mut preview = joined.chars().take(max_chars).collect::<String>();
    if joined.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

fn exact_session_message_page(
    message: &session::SessionMessage,
    block_cursor: usize,
    block_limit: usize,
    scope: ContextRetrieveScope,
) -> Result<serde_json::Value, ToolError> {
    let blocks = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
        .map_err(|error| ToolError::new(format!("stored Session message is malformed: {error}")))?;
    let start = block_cursor.min(blocks.len());
    let end = start.saturating_add(block_limit).min(blocks.len());
    let selected = blocks[start..end]
        .iter()
        .enumerate()
        .map(|(relative_index, block)| {
            let encoded = serde_json::to_vec(block).unwrap_or_default();
            serde_json::json!({
                "index": start + relative_index,
                "digest": format!("{:x}", Sha256::digest(&encoded)),
                "content": block,
            })
        })
        .collect::<Vec<_>>();
    let next_cursor = (end < blocks.len()).then_some(end);
    let scope_name = match scope {
        ContextRetrieveScope::Current => "current",
        ContextRetrieveScope::ExplicitSession => "explicit_session",
        ContextRetrieveScope::RelatedSessions => "related_sessions",
        ContextRetrieveScope::WorkspaceSessions => "workspace_sessions",
    };
    Ok(serde_json::json!({
        "kind": "runtime.context_retrieval",
        "source": "session_history",
        "scope": scope_name,
        "status": "completed",
        "target_session_id": message.session_id,
        "message_id": message.stable_message_id,
        "sequence": message.sequence,
        "role": message.role,
        "created_at_ms": message.created_at_ms,
        "message_digest": format!("{:x}", Sha256::digest(message.content_json.as_bytes())),
        "block_cursor": start,
        "block_count": blocks.len(),
        "selected_count": selected.len(),
        "selected": selected,
        "next_request": next_cursor.map(|cursor| serde_json::json!({
            "source": "session_history",
            "scope": scope_name,
            "session_id": (scope == ContextRetrieveScope::ExplicitSession)
                .then_some(message.session_id.clone()),
            "message_id": message.stable_message_id,
            "block_cursor": cursor,
            "block_limit": block_limit,
        })),
        "truncated": next_cursor.is_some(),
        "authorization_basis": if scope == ContextRetrieveScope::Current {
            "current_session"
        } else {
            "explicit_authorized_session"
        },
        "reference_contract": context_reference_contract(),
    }))
}

impl GatewayToolExecutor {
    async fn execute_authorized_output_with_progress(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
        progress: Option<&std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let tool_name = <Self as ToolExecutor>::resolve_tool_name(self, tool_name)
            .ok_or_else(|| ToolError::new(format!("tool `{tool_name}` is not registered")))?;
        let mut value: serde_json::Value = serde_json::from_str(input)
            .map_err(|error| self.input_contract_error(&tool_name, error))?;
        if tool_name == "bash" {
            if let Some(object) = value.as_object_mut() {
                match authorization.authorization_lease.ceiling {
                    harness_contract::policy::PermissionMode::DangerFullAccess => {
                        object.insert(
                            "dangerouslyDisableSandbox".to_string(),
                            serde_json::json!(true),
                        );
                        object.insert("isolateNetwork".to_string(), serde_json::json!(false));
                    }
                    harness_contract::policy::PermissionMode::WorkspaceWrite => {
                        object.insert(
                            "dangerouslyDisableSandbox".to_string(),
                            serde_json::json!(false),
                        );
                        object.insert("isolateNetwork".to_string(), serde_json::json!(false));
                    }
                    harness_contract::policy::PermissionMode::ReadOnly => {
                        object.insert(
                            "dangerouslyDisableSandbox".to_string(),
                            serde_json::json!(false),
                        );
                        object.insert("isolateNetwork".to_string(), serde_json::json!(true));
                    }
                }
            }
        }
        if tool_name == "tool_search"
            || is_gateway_runtime_control_tool(&tool_name)
            || is_gateway_context_tool(&tool_name)
        {
            return self.execute_output(&tool_name, input).await;
        }
        let tool_host = Arc::clone(&self.tool_host);
        let authorization = authorization.clone();
        let output = if tool_name == "bash" {
            let progress = progress.cloned();
            tool_host
                .pin_snapshot()
                .execute_async_with_progress(
                    &authorization,
                    &tool_name,
                    &value,
                    progress.map(|callback| {
                        let callback: std::sync::Arc<
                            dyn Fn(tools::bash::BashProgressSample) + Send + Sync,
                        > = std::sync::Arc::new(move |sample| {
                            callback(&format!(
                                "stdout_bytes={} stderr_bytes={} at_ms={}",
                                sample.stdout_bytes, sample.stderr_bytes, sample.at_ms
                            ));
                        });
                        callback
                    }),
                )
                .await
                .map_err(|error| ToolError::new(error.to_string()))?
        } else {
            runtime::ToolExecutionPlane::adapt_blocking(move || {
                tool_host
                    .pin_snapshot()
                    .execute(&authorization, &tool_name, &value)
                    .map_err(|error| ToolError::new(error.to_string()))
            })
            .await
            .map_err(|error| ToolError::new(error.to_string()))??
        };
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            output,
        ))
    }

    async fn execute_evidence_retrieve(
        &self,
        input: EvidenceRetrieveToolRequest,
        session_id: Option<&str>,
        authorized_scopes: &[String],
    ) -> Result<String, ToolError> {
        let services = self.runtime_services.get().cloned().ok_or_else(|| {
            ToolError::new("evidence_retrieve requires the workspace RuntimeServices")
        })?;
        let selector = input
            .selector
            .clone()
            .unwrap_or_else(|| input.evidence_ref.clone());
        if !selector.starts_with("tool://") {
            return serde_json::to_string_pretty(&serde_json::json!({
                "kind": "evidence_retrieve",
                "evidence_ref": input.evidence_ref,
                "available": false,
                "reason": "unsupported_ref",
                "hint": "Only durable tool:// raw-output references are resolvable from the Runtime ArtifactStore; memory:/session:// refs must be read through context_retrieve",
            }))
            .map_err(|error| ToolError::new(error.to_string()));
        }
        let store = services.artifact_store();
        let artifact = store.resolve(&selector).map_err(|error| {
            ToolError::new(format!("evidence_retrieve resolve failed: {error}"))
        })?;
        let fallback_scopes = session_id
            .map(|session| vec![format!("session:{session}")])
            .unwrap_or_default();
        let effective_scopes: &[String] = if authorized_scopes.is_empty() {
            &fallback_scopes
        } else {
            authorized_scopes
        };
        if !evidence_scope_allowed(effective_scopes, &artifact.visibility_scope) {
            return serde_json::to_string_pretty(&serde_json::json!({
                "kind": "evidence_retrieve",
                "evidence_ref": input.evidence_ref,
                "available": false,
                "reason": "not_authorized_scope",
                "hint": "This evidence reference is outside the current session/team authorized scopes",
            }))
            .map_err(|error| ToolError::new(error.to_string()));
        }
        let bytes: Vec<u8> = store
            .read(&artifact, &artifact.visibility_scope, None)
            .await
            .map_err(|error| ToolError::new(format!("evidence_retrieve read failed: {error}")))?;
        let content = String::from_utf8_lossy(&bytes);
        let bounded = content.chars().take(12_000).collect::<String>();
        serde_json::to_string_pretty(&serde_json::json!({
            "kind": "evidence_retrieve",
            "evidence_ref": input.evidence_ref,
            "available": true,
            "bytes": bytes.len(),
            "media_type": artifact.media_type,
            "content": bounded,
            "truncated": bytes.len() > 12_000,
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }
}

fn evidence_scope_allowed(authorized_scopes: &[String], visibility_scope: &str) -> bool {
    authorized_scopes
        .iter()
        .any(|scope| scope == visibility_scope)
}

fn session_record_title(record: &session::SessionRecord) -> String {
    record
        .metadata_json
        .as_deref()
        .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
        .and_then(|metadata| {
            metadata
                .get("title")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            (!record.chat_id.trim().is_empty())
                .then(|| record.chat_id.clone())
                .unwrap_or_else(|| record.session_id.clone())
        })
}

#[async_trait::async_trait]
impl ToolExecutor for GatewayToolExecutor {
    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        let lease = self.tool_host.pin_snapshot();
        let mut receipt = lease.catalog_receipt();
        if let Some(allowed) = self.allowed_tools.as_ref() {
            receipt
                .descriptors
                .retain(|descriptor| allowed.contains(&descriptor.canonical_id));
            receipt.activation_candidates = receipt
                .descriptors
                .iter()
                .map(|descriptor| descriptor.canonical_id.clone())
                .collect();
        }
        receipt
    }

    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let canonical_name = <Self as ToolExecutor>::resolve_tool_name(self, tool_name)
            .ok_or_else(|| ToolError::new(format!("tool `{tool_name}` is not registered")))?;
        let tool_name = canonical_name.as_str();
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
            .map_err(|error| self.input_contract_error(tool_name, error))?;
        let result = if tool_name == "tool_search" {
            self.execute_search_tool(value)
        } else if is_gateway_runtime_control_tool(tool_name) || is_gateway_context_tool(tool_name) {
            self.execute_runtime_tool(tool_name, value).await
        } else {
            Err(ToolError::new(format!(
                "ordinary tool `{tool_name}` requires Runtime authorization"
            )))
        };
        match result {
            Ok(output) => {
                if self.emit_output {
                    let markdown = format_tool_result(tool_name, &output, false);
                    print!("{markdown}");
                }
                Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
                    output,
                ))
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

    fn validate_tool_input(&self, tool_name: &str, input: &str) -> Result<(), ToolError> {
        let canonical_name = <Self as ToolExecutor>::resolve_tool_name(self, tool_name)
            .ok_or_else(|| ToolError::new(format!("tool `{tool_name}` is not registered")))?;
        if self
            .allowed_tools
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&canonical_name))
        {
            return Err(ToolError::new(format!(
                "tool `{canonical_name}` is not enabled by the current --allowedTools setting"
            )));
        }
        let value = serde_json::from_str::<serde_json::Value>(input)
            .map_err(|error| self.input_contract_error(&canonical_name, error))?;
        self.tool_host
            .pin_snapshot()
            .validate_input(&canonical_name, &value)
            .map_err(|error| self.input_contract_error(&canonical_name, error))
    }

    fn has_registered_tools(&self) -> bool {
        !self.available_tool_names().is_empty()
    }

    fn registered_tool_effect(
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

    fn prepare_governed_invocations(
        &self,
        requests: &[runtime::tool_dispatch::ToolRequest],
    ) -> Vec<harness_contract::tool::GovernedToolInvocation> {
        let lease = self.tool_host.pin_snapshot();
        requests
            .iter()
            .map(|request| {
                let input = serde_json::from_str::<serde_json::Value>(&request.input)
                    .unwrap_or(serde_json::Value::Null);
                lease.prepare_governed_invocation(
                    request.tool_use_id.clone(),
                    &request.tool_name,
                    &input,
                    &request.depends_on,
                )
            })
            .collect()
    }

    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        self.execute_authorized_output_with_progress(authorization, tool_name, input, None)
            .await
    }

    fn available_tool_names(&self) -> Vec<String> {
        GatewayToolExecutor::available_tool_names(self)
    }

    fn resolve_tool_name(&self, requested: &str) -> Option<String> {
        let canonical = self
            .tool_host
            .pin_snapshot()
            .snapshot()
            .catalog
            .canonical_name(requested)?;
        self.allowed_tools
            .as_ref()
            .is_none_or(|allowed| allowed.contains(&canonical))
            .then_some(canonical)
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
                ToolPermissionMode::DangerFullAccess => runtime::ToolSafetyCategory::Destructive,
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

#[async_trait::async_trait]
impl runtime::RuntimeExecutionHost for GatewayToolExecutor {
    async fn execute_runtime_tool(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> runtime::RuntimeToolExecutionOutcome {
        let evidence_ref = format!(
            "gateway-tool:{}:{}:{}",
            request.governed_plan_id, request.governed_plan_revision, request.tool_use_id
        );
        let Some(canonical_tool_name) =
            <Self as ToolExecutor>::resolve_tool_name(self, &request.tool_name)
        else {
            return runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::Failed,
                category: request.category,
                output: None,
                error: Some(format!("tool `{}` is not registered", request.tool_name)),
                evidence_ref,
            };
        };
        let normalized_request;
        let request = if canonical_tool_name == request.tool_name {
            request
        } else {
            normalized_request = runtime::RuntimeToolExecutionRequest {
                tool_name: canonical_tool_name,
                ..request.clone()
            };
            &normalized_request
        };
        if request.evaluation_isolated && request.category != runtime::ToolSafetyCategory::ReadOnly
        {
            return runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some(
                    "paired evaluation permits only read-only tools; use a dedicated sandboxed evaluation executor for mutations"
                        .to_string(),
                ),
                evidence_ref,
            };
        }
        if request.managed_invocation.is_some() && request.tool_name == "runtime_orchestrate" {
            // `runtime_orchestrate` would create a child graph whose task
            // packets do not belong to this invocation's effect outbox. A
            // Managed Agent must select a Managed Team target instead, so all
            // child roles inherit the same durable invocation fence.
            return runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some(
                    "managed Agent cannot invoke runtime_orchestrate; use a Managed Team definition so every child role inherits the invocation fence"
                        .to_string(),
                ),
                evidence_ref,
            };
        }
        let value: serde_json::Value = match serde_json::from_str(&request.input) {
            Ok(value) => value,
            Err(error) => {
                return runtime::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: runtime::RuntimeToolExecutionStatus::Failed,
                    category: request.category,
                    output: None,
                    error: Some(format!("invalid tool input JSON: {error}")),
                    evidence_ref,
                };
            }
        };
        let managed_effect = if request.category != runtime::ToolSafetyCategory::ReadOnly {
            if let Some(fence) = request.managed_invocation.as_ref() {
                let Some(services) = self.runtime_services.get().cloned() else {
                    return runtime::RuntimeToolExecutionOutcome {
                        tool_use_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                        category: request.category,
                        output: None,
                        error: Some(
                            "managed Agent side effect is blocked because Gateway has no Runtime effect-fence service"
                                .to_string(),
                        ),
                        evidence_ref,
                    };
                };
                let effect_id = format!("tool:{}:{}", request.tool_name, request.tool_use_id);
                match services.begin_managed_agent_effect(
                    fence,
                    &effect_id,
                    format!("runtime_tool:{:?}", request.category).to_ascii_lowercase(),
                    request.idempotency_key.clone(),
                    format!(
                        "runtime-tool:{}:{}",
                        request.tool_name, request.idempotency_key
                    ),
                ) {
                    Ok(runtime::ManagedAgentEffectPermit::Execute { .. }) => {
                        Some((fence.clone(), effect_id, services))
                    }
                    Ok(runtime::ManagedAgentEffectPermit::AlreadyCompleted { record }) => {
                        return runtime::RuntimeToolExecutionOutcome {
                            tool_use_id: request.tool_use_id.clone(),
                            tool_name: request.tool_name.clone(),
                            status: runtime::RuntimeToolExecutionStatus::Executed,
                            category: request.category,
                            output: Some(format!(
                                "managed effect was already completed; receipt={}",
                                record.receipt_ref.unwrap_or_else(|| "unknown".to_string())
                            )),
                            error: None,
                            evidence_ref,
                        };
                    }
                    Err(error) => {
                        return runtime::RuntimeToolExecutionOutcome {
                            tool_use_id: request.tool_use_id.clone(),
                            tool_name: request.tool_name.clone(),
                            status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                            category: request.category,
                            output: None,
                            error: Some(format!(
                                "managed Agent side effect failed Runtime fencing: {error}"
                            )),
                            evidence_ref,
                        };
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        let result = if request.tool_name == "tool_search" {
            self.execute_search_tool(value)
        } else if is_gateway_runtime_control_tool(&request.tool_name)
            || is_gateway_context_tool(&request.tool_name)
        {
            self.execute_runtime_tool_with_binding(
                &request.tool_name,
                value,
                RuntimeToolExecutionBinding {
                    session_id: request.session_id.as_deref(),
                    authorized_scopes: &request.authorized_scopes,
                    memory_context: request.memory_context.as_ref(),
                    model_lease: request.model_lease.as_deref(),
                    parent_execution: request.parent_execution.as_ref(),
                    execution_decision: request.execution_decision.as_ref(),
                    permission_ceiling: request
                        .authorization
                        .as_ref()
                        .map_or(self.runtime_permission_ceiling, |authorization| {
                            authorization.authorization_lease.ceiling
                        }),
                },
            )
            .await
        } else if let Some(authorization) = request.authorization.as_ref() {
            self.execute_authorized_output_with_progress(
                authorization,
                &request.tool_name,
                &request.input,
                request.tool_progress.0.as_ref(),
            )
            .await
            .map(|output| output.model_text().to_string())
        } else {
            Err(ToolError::new(format!(
                "ordinary tool `{}` requires Runtime authorization",
                request.tool_name
            )))
        };
        match result {
            Ok(output) => {
                if let Some((fence, effect_id, services)) = managed_effect {
                    if let Err(error) = services.complete_managed_agent_effect(
                        &fence,
                        &effect_id,
                        evidence_ref.clone(),
                    ) {
                        let _ = services.reconcile_managed_agent_effect(
                            &fence,
                            &effect_id,
                            format!(
                                "tool returned success but effect receipt commit failed: {error}"
                            ),
                        );
                        return runtime::RuntimeToolExecutionOutcome {
                            tool_use_id: request.tool_use_id.clone(),
                            tool_name: request.tool_name.clone(),
                            status: runtime::RuntimeToolExecutionStatus::Failed,
                            category: request.category,
                            output: None,
                            error: Some(format!(
                                "managed Agent effect may have completed but its receipt requires reconciliation: {error}"
                            )),
                            evidence_ref,
                        };
                    }
                }
                runtime::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: runtime::RuntimeToolExecutionStatus::Executed,
                    category: request.category,
                    output: Some(output),
                    error: None,
                    evidence_ref,
                }
            }
            Err(error) => {
                if let Some((fence, effect_id, services)) = managed_effect {
                    let _ = services.reconcile_managed_agent_effect(
                        &fence,
                        &effect_id,
                        format!("tool adapter returned error: {error}"),
                    );
                }
                runtime::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: runtime::RuntimeToolExecutionStatus::Failed,
                    category: request.category,
                    output: None,
                    error: Some(error.to_string()),
                    evidence_ref,
                }
            }
        }
    }

    fn delegated_tool_definitions(
        &self,
        tool_names: &[String],
    ) -> Vec<runtime::ProviderToolDefinition> {
        let allowed = tool_names
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        self.tool_host
            .pin_snapshot()
            .snapshot()
            .catalog
            .definitions(Some(&allowed))
            .into_iter()
            .map(|definition| runtime::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect()
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        self.has_tool(tool_name).then(|| {
            self.tool_host
                .pin_snapshot()
                .describe_effect(tool_name, input)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tools::permissions::PermissionMode as ToolPermissionMode;
    use tools::RuntimeToolDefinition;

    #[test]
    fn request_execution_decision_precedes_process_shared_fallback() {
        let mut request = runtime::build_runtime_execution_decision("request-bound turn", None);
        request.turn_ref = Some("turn-request".to_string());
        let request_id = request.decision_id.clone();
        let mut shared =
            runtime::build_runtime_execution_decision("unrelated concurrent turn", None);
        shared.turn_ref = Some("turn-shared".to_string());

        let selected = effective_runtime_execution_decision(Some(&request), Some(shared))
            .expect("request decision");

        assert_eq!(selected.decision_id, request_id);
        assert_eq!(selected.turn_ref.as_deref(), Some("turn-request"));
    }
    #[test]
    fn evidence_scope_allowed_requires_exact_membership() {
        assert!(evidence_scope_allowed(
            &["session:s1".to_string()],
            "session:s1"
        ));
        assert!(!evidence_scope_allowed(
            &["session:s1".to_string()],
            "session:s2"
        ));
        assert!(!evidence_scope_allowed(
            &["session:s1".to_string()],
            "public"
        ));
        assert!(!evidence_scope_allowed(&[], "session:s1"));
    }

    #[test]
    fn governed_web_search_receives_a_runtime_authorization_under_workspace_write() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        let requests = [runtime::tool_dispatch::ToolRequest {
            tool_use_id: "web-search-1".to_string(),
            tool_name: "web_search".to_string(),
            input: r#"{"query":"rust stable"}"#.to_string(),
            depends_on: Vec::new(),
        }];
        let prepared = executor.prepare_governed_invocations(&requests);
        let invocation = prepared.first().expect("governed invocation");
        assert_eq!(
            invocation.effect.required_permission,
            harness_contract::tool::ToolPermissionMode::ReadOnly
        );
        let negotiator = runtime::AuthorizationNegotiator::new();
        let policy = runtime::PermissionPolicy::new(runtime::PermissionMode::WorkspaceWrite);
        let evaluated = negotiator.assess_effective(
            &policy,
            &runtime::AuthorizationRequest {
                principal_id: "test:web-search".to_string(),
                capability: invocation.effect.tool_id.clone(),
                input: invocation.intent.normalized_input.to_string(),
                idempotency_key: "web-search-request".to_string(),
                effect: invocation.effect.clone(),
                parent_ceiling: runtime::PermissionMode::WorkspaceWrite,
                parent_lease_id: None,
                approval_satisfied: false,
                recovery_scope: "web-search-request".to_string(),
                context: runtime::PermissionContext::default(),
                safe_alternatives: Vec::new(),
            },
        );
        let assessment = evaluated.assessment;
        let decision = runtime::ToolPolicy
            .authorize(
                &evaluated.effective,
                &assessment,
                "web-search-request",
                assessment
                    .lease
                    .clone()
                    .expect("read-only web search lease"),
                60,
            )
            .expect("read-only web search must receive a Runtime authorization");
        assert_eq!(decision.authorization.tool_id, "web_search");
    }

    #[test]
    fn production_executor_rejects_invalid_model_input_before_governance() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());

        let error = executor
            .validate_tool_input("bash", "{}")
            .expect_err("missing bash command must be rejected");
        assert!(error
            .to_string()
            .contains("missing required field `command`"));
        executor
            .validate_tool_input("bash", r#"{"command":"pwd"}"#)
            .expect("valid bash input");
        executor
            .validate_tool_input("enter_plan_mode", "{}")
            .expect("valid no-argument tool");
    }

    #[tokio::test]
    async fn runtime_capabilities_executes_without_mcp_state() {
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
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        let output = executor
            .execute(
                "runtime_capabilities",
                r#"{"intent":"检查 README 是否反映最新架构"}"#,
            )
            .await
            .expect("runtime capabilities should execute without MCP");

        assert!(output.contains("runtime_capabilities"));
        assert!(output.contains("evidence_plan"));
        assert!(output.contains("tool_batch_readonly"));
        let response: serde_json::Value = serde_json::from_str(&output).expect("capability json");
        assert_eq!(response["runtime_orchestrate"]["available"], false);
        assert_eq!(response["context_retrieval"]["available"], false);
        assert!(response["strategy"]["model_callable_tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().all(|tool| tool != "runtime_orchestrate")));
    }

    #[tokio::test]
    async fn context_retrieve_is_runtime_bound_and_degrades_explicitly() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "context_retrieve".to_string(),
                description: Some("bounded context retrieval".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "query": { "type": "string" }
                    },
                    "required": ["source", "query"]
                }),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor =
            GatewayToolExecutor::new(None, false, registry).with_runtime_session_id("s1");
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .expect("bind services");

        let output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"memory","query":"session decisions"}"#,
            )
            .await
            .expect("unconfigured memory returns a degraded receipt");
        let value: serde_json::Value = serde_json::from_str(&output).expect("context receipt");

        assert_eq!(value["kind"], "runtime.context_retrieval");
        assert_eq!(value["status"], "degraded");
        assert_eq!(value["source"], "memory");
    }

    #[tokio::test]
    async fn context_evidence_references_do_not_fall_through_to_mcp() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());

        let error = executor
            .execute_runtime_tool(
                "read_mcp_resource_tool",
                json!({
                    "server": "runtime",
                    "uri": "session://session-a/messages/4",
                }),
            )
            .await
            .expect_err("Session evidence is not an MCP resource");

        assert!(error.to_string().contains("context_retrieve"));
        assert!(error.to_string().contains("not MCP resources"));
    }

    #[tokio::test]
    async fn context_retrieve_reads_the_exact_runtime_memory_binding() {
        let temp = tempfile::tempdir().expect("context retrieval root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let manager = Arc::new(
            memory::CognitiveContextManager::new(memory::MemoryConfig {
                store: memory::config::StoreConfig {
                    sqlite_path: temp.path().join("memory.sqlite"),
                    blob_dir: temp.path().join("blobs"),
                    enable_vector_index: false,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("memory manager"),
        );
        let context = memory::MemoryTurnContext::new("session-exact", "agent-exact")
            .with_definition_lineage_id(Some("definition-exact".to_string()))
            .with_project_id(Some(runtime::memory_project_id_for_workspace(&workspace)))
            .with_task_id(Some("task-exact".to_string()))
            .with_cognitive_read_scopes(vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Project,
            ]);
        let now = chrono::Utc::now();
        let exact_memory_id = uuid::Uuid::new_v4();
        manager
            .remember_for_turn(
                &context,
                memory::MemoryEntry {
                    id: exact_memory_id,
                    layer: memory::MemoryLayer::L2,
                    category: memory::MemoryCategory::Decision,
                    priority: memory::Priority::High,
                    source: memory::MemorySource::UserExplicit,
                    title: "Context isolation decision".to_string(),
                    content: "Use exact runtime binding for active memory retrieval.".to_string(),
                    embedding: None,
                    tags: vec!["context-isolation".to_string()],
                    relations: Vec::new(),
                    confidence: 0.98,
                    access_count: 0,
                    staleness: 0.0,
                    created_at: now,
                    updated_at: now,
                    last_accessed_at: None,
                    scope: memory::MemoryScope::default(),
                    session_id: None,
                    source_agent: None,
                    visibility: memory::AgentVisibility::Private,
                },
            )
            .await
            .expect("remember exact binding");
        let other_context = memory::MemoryTurnContext::new("session-other", "agent-other")
            .with_project_id(Some("other-project".to_string()))
            .with_task_id(Some("task-other".to_string()))
            .with_cognitive_read_scopes(vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Project,
            ]);
        manager
            .remember_for_turn(
                &other_context,
                memory::MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: memory::MemoryLayer::L2,
                    category: memory::MemoryCategory::Decision,
                    priority: memory::Priority::Critical,
                    source: memory::MemorySource::UserExplicit,
                    title: "Other project context isolation decision".to_string(),
                    content:
                        "Other project also mentions exact runtime binding and must remain hidden."
                            .to_string(),
                    embedding: None,
                    tags: vec!["context-isolation".to_string()],
                    relations: Vec::new(),
                    confidence: 1.0,
                    access_count: 0,
                    staleness: 0.0,
                    created_at: now,
                    updated_at: now,
                    last_accessed_at: None,
                    scope: memory::MemoryScope::default(),
                    session_id: None,
                    source_agent: None,
                    visibility: memory::AgentVisibility::Private,
                },
            )
            .await
            .expect("remember out-of-binding memory");
        assert_eq!(
            manager
                .list_all_entries()
                .await
                .expect("list remembered entries")
                .len(),
            2
        );
        assert_eq!(
            manager
                .search_memories(memory::SearchMemoriesRequest {
                    query: "exact runtime binding".to_string(),
                    limit: 8,
                    ..Default::default()
                })
                .await
                .expect("search remembered entries")
                .entries
                .len(),
            2
        );
        let services = runtime::RuntimeServices::builder(temp.path().join("home"), &workspace)
            .memory_manager(Arc::clone(&manager))
            .build()
            .expect("runtime services");
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "context_retrieve".to_string(),
                description: Some("bounded context retrieval".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "query": { "type": "string" }
                    },
                    "required": ["source", "query"]
                }),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry)
            .with_runtime_session_id("session-exact")
            .with_runtime_memory_context(context);
        executor
            .bind_runtime_services(services)
            .expect("bind services");

        let output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"memory","query":"exact runtime binding","limit":8}"#,
            )
            .await
            .expect("active memory retrieval");
        let value: serde_json::Value = serde_json::from_str(&output).expect("context receipt");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["selected_count"], 1, "{output}");
        assert_eq!(value["selected"][0]["title"], "Context isolation decision");
        assert_eq!(
            value["selected"][0]["read_request"]["memory_id"],
            exact_memory_id.to_string()
        );
        assert!(value["reference_contract"]["instruction"]
            .as_str()
            .is_some_and(|instruction| instruction.contains("not MCP resources")));
        assert!(!output.contains("Other project"));

        let exact_output = executor
            .execute(
                "context_retrieve",
                &serde_json::json!({
                    "source": "memory",
                    "memory_id": exact_memory_id,
                })
                .to_string(),
            )
            .await
            .expect("exact authorized memory retrieval");
        let exact: serde_json::Value =
            serde_json::from_str(&exact_output).expect("exact memory receipt");
        assert_eq!(exact["selected_count"], 1);
        assert_eq!(
            exact["selected"][0]["content"],
            "Use exact runtime binding for active memory retrieval."
        );
    }

    #[tokio::test]
    async fn context_retrieve_follows_only_durable_session_relations() {
        let temp = tempfile::tempdir().expect("session retrieval root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let store =
            Arc::new(session::UnifiedSessionStore::open_in_memory().expect("session store"));
        let now = chrono::Utc::now().to_rfc3339();
        for session_id in [
            "session-current",
            "session-related",
            "session-workspace-peer",
            "session-unrelated",
        ] {
            let owner = if matches!(session_id, "session-current" | "session-workspace-peer") {
                "local-human"
            } else {
                "other-human"
            };
            store
                .create_session(&session::SessionRecord {
                    session_id: session_id.to_string(),
                    platform: "test".to_string(),
                    chat_id: session_id.to_string(),
                    user_id: None,
                    model: None,
                    created_at: now.clone(),
                    last_activity: now.clone(),
                    message_count: 0,
                    reset_policy: "manual".to_string(),
                    metadata_json: Some(
                        serde_json::json!({
                            "title": format!("Context {session_id}"),
                            "workspace_root": workspace.display().to_string(),
                            "owner_principal_id": owner,
                        })
                        .to_string(),
                    ),
                    input_tokens: 0,
                    output_tokens: 0,
                    estimated_cost_usd: 0.0,
                    status: "active".to_string(),
                })
                .await
                .expect("create session");
            store
                .insert_message(&session::SessionMessage {
                    stable_message_id: format!("{session_id}-message"),
                    session_id: session_id.to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    content_json: serde_json::json!([{
                        "type": "text",
                        "text": format!("shared gateway relation marker from {session_id}")
                    }])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 1,
                })
                .await
                .expect("insert session message");
        }
        let event_bus = crate::event_bus::SessionProjectionHub::new();
        let repository = Arc::new(
            crate::services::session_service::repository::SessionRepository::new(
                Arc::new(crate::gateway::HotSessionPool::new()),
                Some(Arc::clone(&store)),
                event_bus,
            ),
        );
        let presence = Arc::new(
            crate::services::session_service::presence::SessionPresenceLedger::with_store(
                Arc::clone(&store),
            ),
        );
        let session_port =
            crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
                repository, presence,
            );
        let services = runtime::RuntimeServices::builder(temp.path().join("home"), &workspace)
            .build()
            .expect("runtime services");
        services
            .install_session_ports(
                session_port.clone(),
                session_port.clone(),
                session_port.clone(),
                session_port,
            )
            .expect("install session ports");
        services
            .session_relations()
            .add_relation(
                "session-current",
                "session-related",
                runtime::SessionRelationKind::References,
                "current session explicitly references the related session",
                Vec::new(),
            )
            .expect("durable session relation");
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "context_retrieve".to_string(),
                description: Some("bounded context retrieval".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "query": { "type": "string" },
                        "scope": { "type": "string" }
                    },
                    "required": ["source", "query"]
                }),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry)
            .with_runtime_session_id("session-current");
        executor
            .bind_runtime_services(services)
            .expect("bind services");

        let output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"related_sessions","query":"gateway relation marker","limit":8}"#,
            )
            .await
            .expect("active related-session retrieval");
        let value: serde_json::Value = serde_json::from_str(&output).expect("context receipt");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["scope"], "related_sessions");
        assert_eq!(value["authorized_session_count"], 2);
        assert_eq!(value["selected_count"], 2, "{output}");
        assert!(output.contains("session-current"));
        assert!(output.contains("session-related"));
        assert!(!output.contains("session-unrelated"));

        let catalog_output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_catalog","query":"gateway relation marker","limit":8}"#,
            )
            .await
            .expect("discover same-actor workspace Sessions");
        let catalog: serde_json::Value =
            serde_json::from_str(&catalog_output).expect("session catalog receipt");
        assert_eq!(catalog["scope"], "workspace_sessions");
        assert_eq!(catalog["selected_count"], 2, "{catalog_output}");
        assert!(catalog_output.contains("session-current"));
        assert!(catalog_output.contains("session-workspace-peer"));
        assert!(!catalog_output.contains("session-related"));
        assert_eq!(
            catalog["selected"][0]["read_request"]["source"],
            "session_history"
        );
        assert!(catalog["reference_contract"]["instruction"]
            .as_str()
            .is_some_and(|instruction| instruction.contains("not MCP resources")));
        assert!(!catalog_output.contains("session-unrelated"));

        let workspace_output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"workspace_sessions","query":"gateway relation marker","limit":8}"#,
            )
            .await
            .expect("one-hop workspace Session search");
        let workspace: serde_json::Value =
            serde_json::from_str(&workspace_output).expect("workspace search receipt");
        assert_eq!(workspace["scope"], "workspace_sessions");
        assert!(workspace_output.contains("session-workspace-peer"));
        assert!(!workspace_output.contains("session-unrelated"));

        let explicit_output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"explicit_session","session_id":"session-workspace-peer","limit":8}"#,
            )
            .await
            .expect("read explicit same-actor Session");
        let explicit: serde_json::Value =
            serde_json::from_str(&explicit_output).expect("explicit session receipt");
        assert_eq!(explicit["scope"], "explicit_session");
        assert_eq!(explicit["selected_count"], 1, "{explicit_output}");
        assert!(explicit_output.contains("session-workspace-peer"));

        let denied = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"explicit_session","session_id":"session-unrelated","limit":8}"#,
            )
            .await
            .expect_err("other actor Session must remain hidden");
        assert!(denied.to_string().contains("outside"));
    }

    #[test]
    fn session_message_preview_is_bounded_and_structured() {
        let preview = session_message_preview(
            r#"[{"type":"text","text":"hello context"},{"type":"tool_use","name":"read_file"}]"#,
            20,
        );

        assert!(preview.starts_with("hello context"));
        assert!(preview.contains("[tool"));
        assert!(preview.chars().count() <= 23);
    }

    #[test]
    fn exact_session_message_pages_restore_every_block_with_stable_digest() {
        let message = session::SessionMessage {
            stable_message_id: "message-stable".to_string(),
            session_id: "session-current".to_string(),
            sequence: 42,
            role: "assistant".to_string(),
            content_json: serde_json::json!([
                {"type":"text","text":"first"},
                {"type":"tool_use","name":"read_file","input":{"path":"README.md"}},
                {"type":"text","text":"last"}
            ])
            .to_string(),
            blocks_count: 3,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 7,
        };
        let first = exact_session_message_page(&message, 0, 2, ContextRetrieveScope::Current)
            .expect("first page");
        let second = exact_session_message_page(
            &message,
            first["next_request"]["block_cursor"]
                .as_u64()
                .expect("cursor") as usize,
            2,
            ContextRetrieveScope::Current,
        )
        .expect("second page");

        assert_eq!(first["selected_count"], 2);
        assert_eq!(second["selected_count"], 1);
        assert_eq!(first["message_digest"], second["message_digest"]);
        assert!(second["next_request"].is_null());
        assert_eq!(second["selected"][0]["content"]["text"], "last");
    }

    #[tokio::test]
    async fn runtime_config_view_is_read_only_and_never_returns_credentials() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_config_view".to_string(),
                description: Some("safe config view".to_string()),
                input_schema: json!({"type":"object","additionalProperties":false}),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        let output = executor
            .execute("runtime_config_view", r#"{"detail":"summary"}"#)
            .await
            .expect("safe configuration view");
        let value: serde_json::Value = serde_json::from_str(&output).expect("config view json");
        assert_eq!(value["kind"], "runtime.config_view");
        assert!(value.get("config_path").is_none());
        assert!(value.get("headers").is_none());
        assert!(value.get("env").is_none());
    }

    #[tokio::test]
    async fn resource_capability_query_is_explicit_and_bounded() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_resource_capabilities".to_string(),
                description: Some("resource capability query".to_string()),
                input_schema: json!({"type":"object","additionalProperties":true}),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        let output = executor
            .execute(
                "runtime_resource_capabilities",
                r#"{"resource_kind":"pdf","mime":"application/pdf","intent":"extract document text"}"#,
            )
            .await
            .expect("resource capability query");
        let value: serde_json::Value = serde_json::from_str(&output).expect("resource json");
        assert_eq!(value["kind"], "runtime.resource_capabilities");
        assert!(value["candidate_tools"]
            .as_array()
            .is_some_and(|tools| tools.len() <= 3));
        assert!(value["installed_skills"]
            .as_array()
            .is_some_and(|items| items.len() <= 4));
    }

    #[test]
    fn resource_capability_keywords_include_kind_specific_parsers() {
        let pdf = resource_capability_keywords("pdf", Some("application/pdf"), "extract text");
        assert!(pdf.contains(&"pdftotext".to_string()));
        assert!(pdf.contains(&"pdfinfo".to_string()));

        let audio = resource_capability_keywords("audio", Some("audio/mpeg"), "inspect");
        assert!(audio.contains(&"ffprobe".to_string()));
    }

    #[tokio::test]
    async fn runtime_orchestrate_executes_without_mcp_state() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_orchestrate".to_string(),
                description: Some("runtime orchestration".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "intent": { "type": "string" },
                        "operation": { "type": "string" }
                    },
                    "required": ["intent"],
                    "additionalProperties": true
                }),
                required_permission: ToolPermissionMode::WorkspaceWrite,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.state_write",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .unwrap();

        let output = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"检查 Runtime 状态","operation":"inspect"}"#,
            )
            .await
            .expect("runtime orchestrate should execute without MCP");

        assert!(output.contains("runtime-orch-"));
        assert!(output.contains("inspected"));
    }

    #[tokio::test]
    async fn runtime_orchestrate_reports_repairable_typed_input_contract_errors() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_orchestrate".to_string(),
                description: Some("runtime orchestration".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "intent": { "type": "string" },
                        "operation": { "type": "string" },
                        "proposal": { "type": "object" }
                    },
                    "required": ["intent"],
                    "additionalProperties": false
                }),
                required_permission: ToolPermissionMode::WorkspaceWrite,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.state_write",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        let error = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"review","operation":"propose","input_refs":["wrong-level"]}"#,
            )
            .await
            .expect_err("unknown top-level field must be rejected before execution");
        let failure = error.failure().expect("typed failure");
        assert_eq!(
            failure.class,
            harness_contract::tool::ToolExecutionFailureClass::InputContract
        );
        assert_eq!(failure.tool_name, "runtime_orchestrate");
        assert!(!failure.side_effect_committed);
        assert!(failure.schema_hash.is_some());
        assert!(failure.allowed_fields.contains(&"proposal".to_string()));
        assert!(!failure.allowed_fields.contains(&"input_refs".to_string()));
        assert!(error.model_text().contains("repair_arguments_once"));
    }

    #[test]
    fn runtime_tool_permission_metadata_drives_safety_classification() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "company_catalog_lookup".to_string(),
                description: Some("read company catalog".to_string()),
                input_schema: json!({"type":"object"}),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        assert_eq!(
            executor.classify_tool_safety("company_catalog_lookup", "{}"),
            Some(runtime::ToolSafetyCategory::ReadOnly)
        );
    }

    #[tokio::test]
    async fn capabilities_and_orchestration_reuse_the_bound_turn_strategy_lease() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![
                RuntimeToolDefinition {
                    name: "runtime_capabilities".to_string(),
                    description: Some("capability guidance".to_string()),
                    input_schema: json!({"type":"object","properties":{"intent":{"type":"string"}},"required":["intent"]}),
                    required_permission: ToolPermissionMode::ReadOnly,
                    effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                        "runtime.readonly",
                    ),
                },
                RuntimeToolDefinition {
                    name: "runtime_orchestrate".to_string(),
                    description: Some("runtime orchestration".to_string()),
                    input_schema: json!({"type":"object","properties":{"intent":{"type":"string"},"operation":{"type":"string"}},"required":["intent"]}),
                    required_permission: ToolPermissionMode::WorkspaceWrite,
                    effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                        "runtime.state_write",
                    ),
                },
            ])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);
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
                .await
                .expect("capabilities"),
        )
        .expect("capability json");
        let orchestration: serde_json::Value = serde_json::from_str(
            &executor
                .execute(
                    "runtime_orchestrate",
                    r#"{"intent":"换一个描述也必须复用当前 turn 决策","operation":"inspect"}"#,
                )
                .await
                .expect("orchestration"),
        )
        .expect("orchestration json");

        assert_eq!(
            capabilities["execution_decision"]["lease"]["lease_id"],
            lease_id
        );
        assert_eq!(orchestration["evidence"]["strategy_lease_id"], lease_id);
    }

    #[tokio::test]
    async fn runtime_orchestrate_auto_binds_gateway_session_for_team_requests() {
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
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.state_write",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry)
            .with_runtime_session_id("gateway-session-v26");
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .unwrap();

        let error = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"需要多 Agent 协同审查架构","operation":"propose","proposal":{"mutation_id":"missing-runtime-team","reason":"test","nodes":[{"node_id":"team","recipe":"team","objective":"审查架构"}]}}"#,
            )
            .await
            .expect_err("unavailable team orchestration must not be reported as a successful tool");

        assert!(
            error.to_string().contains("runtime orchestration"),
            "{error}"
        );
        assert!(!error
            .to_string()
            .contains("missing_session_id_for_team_runtime"));
    }

    #[tokio::test]
    async fn runtime_orchestrate_inspect_uses_gateway_runtime_services() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_orchestrate".to_string(),
                description: Some("runtime orchestration".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "intent": { "type": "string" },
                        "operation": { "type": "string" }
                    },
                    "required": ["intent"],
                    "additionalProperties": true
                }),
                required_permission: ToolPermissionMode::WorkspaceWrite,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.state_write",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry)
            .with_runtime_session_id("gateway-session-v6-tool-host");
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .unwrap();

        let output = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"检查 Runtime 状态","operation":"inspect"}"#,
            )
            .await
            .expect("gateway-bound runtime orchestrate should inject a tool host");

        let response: serde_json::Value = serde_json::from_str(&output).expect("typed response");
        assert_eq!(response["status"], "inspected");
        assert_eq!(
            response["runtime_snapshot"]["capability_recipes"][0],
            "direct"
        );
        assert_eq!(response["evidence"]["operation"], "inspect");
    }

    #[test]
    fn delegated_capabilities_are_catalog_bound_read_only_and_non_recursive() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        let mut request = runtime::RuntimeOrchestrationCommand {
            intent: "review the workspace with a delegated team".to_string(),
            model_lease: None,
            session_id: Some("session".to_string()),
            lineage: None,
            mission_id: None,
            operation: runtime::RuntimeOrchestrationOperation::Inspect,
            inspect_execution_id: None,
            proposal: None,
            control: None,
            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: vec![
                "tool:runtime_orchestrate".to_string(),
                "tool:unknown_tool".to_string(),
                "tool:write_file".to_string(),
                "backend:process_jsonl".to_string(),
            ],
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        };

        executor.bind_delegated_capabilities(&mut request);
        let delegated = request
            .capabilities
            .iter()
            .filter_map(|capability| capability.strip_prefix("tool:"))
            .collect::<Vec<_>>();

        assert!(!delegated.is_empty());
        assert!(!delegated.contains(&"runtime_orchestrate"));
        assert!(!delegated.contains(&"runtime_capabilities"));
        assert!(!delegated.contains(&"unknown_tool"));
        assert!(delegated.iter().all(|tool| {
            executor.tool_permission_mode(tool) == Some(ToolPermissionMode::ReadOnly)
        }));
        assert!(request
            .capabilities
            .contains(&"backend:process_jsonl".to_string()));
    }

    #[test]
    fn model_orchestration_cannot_supply_resource_authority() {
        let parsed = serde_json::from_value::<
            harness_contract::orchestration::ModelRuntimeOrchestrationInput,
        >(json!({
            "intent": "parallel architecture review",
            "operation": "propose",
            "proposal": {
                "mutation_id": "model-authority",
                "reason": "test",
                "nodes": [{
                    "node_id": "team",
                    "recipe": "team",
                    "objective": "review",
                    "resource_scopes": ["write:secret"],
                    "focuses": [{
                        "focus_id": "runtime",
                        "role_id": "researcher",
                        "objective": "runtime",
                        "resource_scopes": ["write:secret"]
                    }]
                }]
            }
        }));
        assert!(parsed.is_err());
    }
}
