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
            let effect = self
                .tool_host
                .pin_snapshot()
                .describe_effect(tool_name, &value);
            if effect.required_permission != harness_contract::policy::PermissionMode::ReadOnly {
                Err(ToolError::new(format!(
                    "control tool `{tool_name}` mutation requires Runtime authorization"
                )))
            } else {
                self.execute_runtime_tool(tool_name, value).await
            }
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
        harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .iter()
            .all(|tool| self.has_tool(tool))
    }

    fn mission_runtime_available(&self) -> bool {
        self.collaboration_runtime_available()
    }

    fn bind_execution_decision(&self, decision: runtime::RuntimeExecutionDecision) {
        *self
            .runtime_execution_decision
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(decision);
    }
}
