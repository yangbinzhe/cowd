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
                observed_evidence: Vec::new(),
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
                observed_evidence: Vec::new(),
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
                    observed_evidence: Vec::new(),
                };
            }
        };
        if request.policy_revision == 0 {
            return runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some("production tool request is missing policy_revision".to_string()),
                evidence_ref,
                observed_evidence: Vec::new(),
            };
        }
        let current_policy = request.session_id.as_deref().and_then(|session_id| {
            self.runtime_services
                .get()
                .and_then(|services| services.session_execution_policy(session_id))
        });
        if current_policy.as_ref().map(|policy| policy.revision) != Some(request.policy_revision) {
            return runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some(format!(
                    "tool request policy revision {} is stale or has no active Session policy",
                    request.policy_revision
                )),
                evidence_ref,
                observed_evidence: Vec::new(),
            };
        }
        if current_policy.as_ref().map(|policy| policy.sandbox_posture)
            != Some(request.sandbox_posture)
        {
            return runtime::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some("production tool request sandbox_posture does not match the exact live Session policy revision".to_string()),
                evidence_ref,
                observed_evidence: Vec::new(),
            };
        }
        if let Some(authorization) = request.authorization.as_ref() {
            if authorization.policy_revision != request.policy_revision {
                return runtime::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                    category: request.category,
                    output: None,
                    error: Some(format!(
                        "tool authorization policy revision {} does not match request revision {}",
                        authorization.policy_revision, request.policy_revision
                    )),
                    evidence_ref,
                    observed_evidence: Vec::new(),
                };
            }
        }
        if is_gateway_runtime_control_tool(&request.tool_name)
            || is_gateway_context_tool(&request.tool_name)
        {
            let host_lease = self.tool_host.pin_snapshot();
            let effect = host_lease.describe_effect(&request.tool_name, &value);
            match request.authorization.as_ref() {
                Some(authorization) => {
                    if let Err(error) =
                        host_lease.validate_authorization(authorization, &request.tool_name, &value)
                    {
                        return runtime::RuntimeToolExecutionOutcome {
                            tool_use_id: request.tool_use_id.clone(),
                            tool_name: request.tool_name.clone(),
                            status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                            category: request.category,
                            output: None,
                            error: Some(error.to_string()),
                            evidence_ref,
                            observed_evidence: Vec::new(),
                        };
                    }
                }
                None if effect.required_permission
                    != harness_contract::policy::PermissionMode::ReadOnly =>
                {
                    return runtime::RuntimeToolExecutionOutcome {
                        tool_use_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                        category: request.category,
                        output: None,
                        error: Some(format!(
                            "control tool `{}` mutation requires signed Runtime authorization",
                            request.tool_name
                        )),
                        evidence_ref,
                        observed_evidence: Vec::new(),
                    };
                }
                None => {}
            }
        }
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
                        observed_evidence: Vec::new(),
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
                            observed_evidence: Vec::new(),
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
                            observed_evidence: Vec::new(),
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
                    action_id: Some(request.idempotency_key.as_str()),
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
            // Runtime-derived SandboxPosture is the single authoritative
            // source for bash execution boundaries. Admission above already
            // proved exact equality with the live Session policy revision.
            let effective_input = if request.tool_name == "bash" {
                runtime::autonomy_profile::apply_bash_sandbox_posture(
                    &request.input,
                    request.sandbox_posture,
                )
            } else {
                request.input.clone()
            };
            self.execute_authorized_output_with_progress(
                authorization,
                &request.tool_name,
                &effective_input,
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
                            observed_evidence: Vec::new(),
                        };
                    }
                }
                let observed_evidence =
                    gateway_observed_evidence(self, request, &output, &evidence_ref);
                runtime::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: runtime::RuntimeToolExecutionStatus::Executed,
                    category: request.category,
                    output: Some(output),
                    error: None,
                    evidence_ref,
                    observed_evidence,
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
                    observed_evidence: Vec::new(),
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
