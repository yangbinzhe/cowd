use std::sync::{Arc, Mutex, OnceLock};

use runtime::{ConfigLoader, ToolError, ToolExecutor};
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

#[derive(Debug, Deserialize, Default)]
struct RuntimeConfigViewRequest {
    detail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeResourceCapabilitiesRequest {
    resource_kind: String,
    mime: Option<String>,
    intent: String,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeToolExecutionBinding<'a> {
    session_id: Option<&'a str>,
    model_lease: Option<&'a str>,
    parent_execution: Option<&'a harness_contract::execution_graph::ExecutionParentBinding>,
}

fn is_gateway_runtime_control_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "runtime_config_view"
            | "runtime_resource_capabilities"
            | "runtime_capabilities"
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
    runtime_model_lease: Option<String>,
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
            runtime_model_lease: None,
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
            runtime_model_lease: None,
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
        self.execute_runtime_tool_with_binding(
            tool_name,
            value,
            RuntimeToolExecutionBinding {
                session_id: self.runtime_session_id.as_deref(),
                model_lease: self.runtime_model_lease.as_deref(),
                parent_execution: None,
            },
        )
    }

    fn execute_runtime_tool_with_binding(
        &self,
        tool_name: &str,
        value: serde_json::Value,
        binding: RuntimeToolExecutionBinding<'_>,
    ) -> Result<String, ToolError> {
        if tool_name == "runtime_config_view" {
            let input: RuntimeConfigViewRequest = serde_json::from_value(value)
                .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
            return self.execute_runtime_config_view(input);
        }
        if tool_name == "runtime_resource_capabilities" {
            let input: RuntimeResourceCapabilitiesRequest = serde_json::from_value(value)
                .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
            return self.execute_runtime_resource_capabilities(input);
        }
        if tool_name == "runtime_capabilities" {
            let input: RuntimeCapabilitiesRequest = serde_json::from_value(value)
                .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))?;
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
        if tool_name == "runtime_orchestrate" {
            let mut value = value;
            if let Some(object) = value.as_object_mut() {
                // The gateway owns session/model binding. A model can request
                // a target session only through the typed dispatch fields;
                // it cannot redirect a graph or its child agents by forging
                // the parent identity in tool JSON.
                if let Some(session_id) = binding
                    .session_id
                    .filter(|session_id| !session_id.trim().is_empty())
                {
                    object.insert(
                        "session_id".to_string(),
                        serde_json::Value::String(session_id.to_string()),
                    );
                }
                if let Some(model) = binding.model_lease.filter(|model| !model.trim().is_empty()) {
                    object.insert(
                        "model_lease".to_string(),
                        serde_json::Value::String(model.to_string()),
                    );
                }
            }
            let leased_decision = self
                .runtime_execution_decision
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let mut request = serde_json::from_value::<runtime::RuntimeOrchestrationRequest>(value)
                .map_err(|error| {
                    ToolError::new(format!("invalid runtime_orchestrate input: {error}"))
                })?;
            sanitize_model_orchestration_request(&mut request);
            self.bind_delegated_capabilities(&mut request);
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
                                binding.parent_execution.cloned(),
                            )),
                        )
                    })
                    .join()
                    .map_err(|_| ToolError::new("runtime orchestration worker panicked"))?
            })?;
            tracing::info!(
                status = %result.status,
                selected_pattern = %result.decision.selected_pattern.as_str(),
                findings = ?result.decision.validation_findings,
                "runtime orchestration request completed"
            );
            if matches!(
                result.status.as_str(),
                "rejected" | "unavailable" | "blocked"
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

    fn execute_runtime_config_view(
        &self,
        input: RuntimeConfigViewRequest,
    ) -> Result<String, ToolError> {
        let workspace_root = std::env::current_dir().map_err(|error| {
            ToolError::new(format!("resolve workspace for config view: {error}"))
        })?;
        let config = ConfigLoader::default_for(&workspace_root)
            .load()
            .map_err(|error| {
                ToolError::new(format!("load active runtime configuration: {error}"))
            })?;
        let active_model = self
            .runtime_model_lease
            .clone()
            .or_else(|| config.model().map(str::to_string))
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
        let mcp = self.mcp_state.as_ref().map_or_else(
            || serde_json::json!({"configured": false, "servers": []}),
            |state| {
                let state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                serde_json::json!({
                    "configured": true,
                    "servers": state.server_names(),
                    "pending_servers": state.pending_servers(),
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

    /// A team request must carry explicit least-privilege capabilities into
    /// its protocol packets. Models are not required to enumerate the local
    /// read-only catalog in tool JSON, and forwarding arbitrary names would
    /// let a prompt define its own delegation boundary. Gateway therefore
    /// intersects caller hints with its active catalog and adds the active
    /// read-only evidence tools. Lifecycle controls never propagate to leaf
    /// agents.
    fn bind_delegated_capabilities(&self, request: &mut runtime::RuntimeOrchestrationRequest) {
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
                .map_or(true, |tool| allowed.contains(tool))
        });
        request
            .capabilities
            .extend(allowed_tools.into_iter().map(|tool| format!("tool:{tool}")));
        request.capabilities.sort();
        request.capabilities.dedup();
    }
}

/// A provider can select an objective, a published Team template, and safe
/// ceilings. It cannot construct runtime topology. Focus plans remain a
/// deliberate human/API authoring capability, while model-originated team
/// requests always let Runtime resolve the template's versioned role contract.
fn sanitize_model_orchestration_request(request: &mut runtime::RuntimeOrchestrationRequest) {
    if !request.focus_partition_plans.is_empty() {
        tracing::info!(
            discarded_focus_plan_count = request.focus_partition_plans.len(),
            action = %request.action.as_str(),
            "discarded model-supplied Team focus partitions; Runtime owns template topology"
        );
        request.focus_partition_plans.clear();
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
        let value = match serde_json::from_str(&request.input) {
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
        let result = if request.tool_name == "ToolSearch" {
            self.execute_search_tool(value)
        } else if is_gateway_runtime_control_tool(&request.tool_name) {
            self.execute_runtime_tool_with_binding(
                &request.tool_name,
                value,
                RuntimeToolExecutionBinding {
                    session_id: request.session_id.as_deref(),
                    model_lease: request.model_lease.as_deref(),
                    parent_execution: request.parent_execution.as_ref(),
                },
            )
        } else if let Some(authorization) = request.authorization.as_ref() {
            <Self as ToolExecutor>::execute_authorized(
                self,
                authorization,
                &request.tool_name,
                &request.input,
            )
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
    fn runtime_config_view_is_read_only_and_never_returns_credentials() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_config_view".to_string(),
                description: Some("safe config view".to_string()),
                input_schema: json!({"type":"object","additionalProperties":false}),
                required_permission: ToolPermissionMode::ReadOnly,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None);

        let output = executor
            .execute("runtime_config_view", r#"{"detail":"summary"}"#)
            .expect("safe configuration view");
        let value: serde_json::Value = serde_json::from_str(&output).expect("config view json");
        assert_eq!(value["kind"], "runtime.config_view");
        assert!(value.get("config_path").is_none());
        assert!(value.get("headers").is_none());
        assert!(value.get("env").is_none());
    }

    #[test]
    fn resource_capability_query_is_explicit_and_bounded() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_resource_capabilities".to_string(),
                description: Some("resource capability query".to_string()),
                input_schema: json!({"type":"object","additionalProperties":true}),
                required_permission: ToolPermissionMode::ReadOnly,
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry, None);

        let output = executor
            .execute(
                "runtime_resource_capabilities",
                r#"{"resource_kind":"pdf","mime":"application/pdf","intent":"extract document text"}"#,
            )
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
        assert_eq!(orchestration["evidence"]["strategy_lease_id"], lease_id);
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

        let error = executor
            .execute(
                "runtime_orchestrate",
                r#"{"intent":"需要多 Agent 协同审查架构","action":"request_team"}"#,
            )
            .expect_err("unavailable team orchestration must not be reported as a successful tool");

        assert!(error.to_string().contains("runtime orchestration blocked"));
        assert!(!error
            .to_string()
            .contains("missing_session_id_for_team_runtime"));
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

    #[test]
    fn delegated_capabilities_are_catalog_bound_read_only_and_non_recursive() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin(), None);
        let mut request = runtime::RuntimeOrchestrationRequest {
            intent: "review the workspace with a delegated team".to_string(),
            model_lease: None,
            session_id: Some("session".to_string()),
            target_session_id: None,
            action: runtime::RuntimeOrchestrationAction::RequestTeam,
            reason: None,
            template_hint: None,
            focus_partition_plans: Vec::new(),
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
    fn model_orchestration_cannot_supply_template_role_topology() {
        let mut request: runtime::RuntimeOrchestrationRequest = serde_json::from_value(json!({
            "intent": "parallel architecture review",
            "action": "request_team",
            "focus_partition_plans": [{
                "role_id": "invented_role",
                "slots": [{
                    "focus_id": "runtime",
                    "boundary": "runtime only",
                    "evidence_responsibility": "source evidence"
                }]
            }]
        }))
        .expect("model request parses before Gateway normalization");

        sanitize_model_orchestration_request(&mut request);

        assert!(request.focus_partition_plans.is_empty());
    }
}
