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
        self.reject_orchestrated_tools_outside_allow_list(tool_name, &value)?;
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

    fn validate_tool_input(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
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
        let mut value = serde_json::from_str::<serde_json::Value>(input)
            .map_err(|error| self.input_contract_error(&canonical_name, error))?;
        // The repair belongs here, at the boundary that already holds the
        // pinned schema, not in the dispatch path. It used to live only inside
        // `execute_runtime_tool_with_binding`, which is downstream of this
        // check - so the one corruption it exists to fix, a field the schema
        // types as array/object arriving as a JSON-encoded string, was refused
        // here before the repair could ever run. One live run lost a whole
        // objective to exactly that: two identical frames refused, the single
        // frame-repair budget spent, three Teams and zero Tasks, verdict
        // `partial`.
        let repaired_fields = self.coerce_string_typed_structured_fields(&canonical_name, &mut value);
        if !repaired_fields.is_empty() {
            tracing::info!(
                tool_name,
                ?repaired_fields,
                "coerced string-typed structured tool input fields at the schema boundary"
            );
        }
        self.tool_host
            .pin_snapshot()
            .validate_input(&canonical_name, &value)
            .map_err(|error| self.input_contract_error(&canonical_name, error))
        // Hand back the bytes Runtime will actually dispatch so the
        // transcript, the governed plan and the execution cannot disagree.
        // `Value` re-serializes deterministically, so a model that repeats the
        // same frame still hashes the same and the repetition guard still sees
        // it.
        .map(|()| serde_json::to_string(&value).unwrap_or_else(|_| input.to_string()))
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
        input: &str,
    ) -> Option<runtime::ToolSafetyCategory> {
        // Derive from the input-sensitive effect descriptor so a composable tool
        // (e.g. `tool_orchestrate`) escalates with its sub-calls, matching the
        // delegated-agent executor instead of the static catalog mode.
        let canonical = self.resolve_tool_name(tool_name)?;
        let value: serde_json::Value =
            serde_json::from_str(input).unwrap_or(serde_json::Value::Null);
        let effect = self
            .tool_host
            .pin_snapshot()
            .describe_effect(&canonical, &value);
        Some(runtime::ToolSafetyCategory::from_effect(&effect))
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

impl GatewayToolExecutor {
    /// A composite orchestration tool dispatches sub-calls from inside the
    /// tools crate, bypassing `execute_output`'s top-level allow-list check.
    /// Validate every requested sub-call before delegation so the call fails
    /// closed and no sub-call runs. A missing allow-list means unrestricted,
    /// matching the executor's existing semantics.
    fn reject_orchestrated_tools_outside_allow_list(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Result<(), ToolError> {
        match tools::requested_sub_call_tools(tool_name, input) {
            tools::SubCallAuthorization::NotOrchestration => Ok(()),
            tools::SubCallAuthorization::Invalid(reason) => Err(ToolError::new(reason)),
            tools::SubCallAuthorization::Requires(names) => {
                let Some(allowed) = self.allowed_tools.as_ref() else {
                    return Ok(());
                };
                for name in names {
                    if !allowed.contains(&name) {
                        return Err(ToolError::new(format!(
                            "orchestrated tool `{name}` is not enabled by the current --allowedTools setting"
                        )));
                    }
                }
                Ok(())
            }
        }
    }
}
