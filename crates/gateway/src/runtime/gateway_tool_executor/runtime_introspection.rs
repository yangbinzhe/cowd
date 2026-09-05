impl GatewayToolExecutor {
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
}

