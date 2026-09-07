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
        let tool_host = Arc::new(
            ToolHost::new(
                "gateway",
                workspace_root,
                ToolHostSnapshot::new(
                    Arc::new(tool_registry),
                    Arc::new(tools::lsp_client::LspRegistry::new()),
                    None,
                ),
            )
            .with_authorization_lease_verifier(Arc::new(
                runtime::AuthorizationNegotiator::verify_lease_signature,
            )),
        );
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
                action_id: None,
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
        mut value: serde_json::Value,
        binding: RuntimeToolExecutionBinding<'_>,
    ) -> Result<String, ToolError> {
        if is_agent_action_tool(tool_name) {
            let services = self.runtime_services.get().cloned().ok_or_else(|| {
                ToolError::new("Agent action requires the workspace RuntimeServices")
            })?;
            let actor = match binding.parent_execution {
                Some(parent) => {
                    let trusted_root = binding.execution_decision.and_then(|decision| {
                        (decision.execution_graph_ref.as_deref()
                            == Some(parent.execution_id.as_str())
                            && decision.session_ref.as_deref() == binding.session_id
                            && decision
                                .turn_ref
                                .as_deref()
                                .is_some_and(|turn| !turn.is_empty()))
                        .then(|| root_agent_action_actor(binding))
                    });
                    services
                        .resolve_agent_action_actor(parent, trusted_root)
                        .await
                        .map_err(ToolError::new)?
                }
                None => root_agent_action_actor(binding),
            };
            let expected_revision = match value
                .as_object_mut()
                .and_then(|object| object.remove("expected_revision"))
            {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::Number(number)) => {
                    Some(number.as_u64().ok_or_else(|| {
                        self.input_contract_error(
                            tool_name,
                            "expected_revision must be a non-negative integer".to_string(),
                        )
                    })?)
                }
                Some(_) => {
                    return Err(self.input_contract_error(
                        tool_name,
                        "expected_revision must be a non-negative integer".to_string(),
                    ));
                }
            };
            let action = parse_agent_action(tool_name, value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            let resolved_content_ref = match &action {
                harness_contract::agent_action::AgentAction::ArtifactCommit(input) => {
                    Some(input.content_ref.clone())
                }
                _ => None,
            };
            let envelope = harness_contract::agent_action::AgentActionEnvelope {
                action_id: binding
                    .action_id
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| ToolError::new(
                        "Agent action requires a durable Runtime invocation id; payload hashes are not action identities",
                    ))?
                    .to_string(),
                actor,
                expected_revision,
                action,
            };
            let mut observation = services
                .submit_agent_action(&envelope)
                .await
                .map_err(ToolError::new)?;
            // `objective_complete_request` only commits the model-authored
            // request here. The durable Program projection lane is the sole
            // live reconciliation trigger: it asks ObjectiveSupervisor for a
            // verdict and wakes the root graph. Running the same reconciliation
            // synchronously in this foreground tool path raced that lane on the
            // Goal stream, producing a false failed tool receipt even when the
            // Objective terminal had already committed successfully.
            let should_dispatch = observation.status
                == harness_contract::agent_action::AgentActionStatus::Applied
                && !matches!(
                    envelope.action,
                    harness_contract::agent_action::AgentAction::StateInspect(_)
                );
            // Reconcile duplicate mutations too. The semantic write may have
            // survived a crash while its physical follow-up graph did not;
            // dispatch is deterministic and therefore safe to replay.
            if should_dispatch {
                match services
                    .dispatch_agentic_followups(
                        &envelope,
                        runtime::AgenticDispatchContext {
                            session_id: envelope.actor.session_id.clone(),
                            turn_id: envelope.actor.turn_id.clone(),
                            model_lease: binding.model_lease.unwrap_or("default").to_string(),
                            permission_ceiling: binding.permission_ceiling,
                            resource_scopes: envelope.actor.resource_scopes.clone(),
                        },
                    )
                    .await
                {
                    Ok(dispatches) if !dispatches.is_empty() => {
                        observation.actionable.push(format!(
                            "Runtime admitted {} real Agent execution graph(s)",
                            dispatches.len()
                        ));
                    }
                    Ok(_) => {}
                    Err(error) => {
                        // The Agent Action is already durable. Reporting the
                        // entire tool call as failed lies to the model and may
                        // cause duplicate semantic work. Preserve the applied
                        // receipt and expose physical dispatch as a retryable,
                        // independently observable follow-up state.
                        tracing::warn!(
                            action_id = %envelope.action_id,
                            program_id = %envelope.actor.program_id,
                            %error,
                            "Agent action committed while physical dispatch was deferred"
                        );
                        observation.actionable.push(format!(
                            "Semantic action committed; physical Agent dispatch is deferred and remains recoverable: {error}"
                        ));
                    }
                }
            }
            return serialize_agent_action_receipt(&observation, resolved_content_ref.as_deref())
                .map_err(|error| ToolError::new(error.to_string()));
        }
        if tool_name == "runtime_capabilities" {
            tracing::debug!(
                tool = %tool_name,
                session = ?binding.session_id,
                fallback_ceiling = ?binding.permission_ceiling,
                "runtime control tool binding ceiling"
            );
        }
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
            let agent_catalog = self
                .runtime_services
                .get()
                .and_then(|services| services.definition_registry().runnable_agent_catalog().ok());
            return serde_json::to_string_pretty(
                &runtime::runtime_capabilities_response_with_leased_decision_and_tools(
                    &input.intent,
                    input.surface.as_deref(),
                    input.profile.as_deref(),
                    input.detail.as_deref(),
                    leased_decision.as_ref(),
                    &self.available_tool_names(),
                    agent_catalog.as_deref(),
                ),
            )
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
                        let sequence = input.sequence.ok_or_else(|| {
                            ToolError::new("exact Session retrieval requires a sequence")
                        })?;
                        history
                            .message_by_sequence(&target_session_id, sequence)
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

}
