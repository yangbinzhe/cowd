#[async_trait::async_trait]
impl runtime::RuntimeExecutionHost for GatewayToolExecutor {
    fn bounded_process_authority(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> Result<runtime::tool_host::BoundedProcessAuthority, ToolError> {
        self.validated_bounded_process_lease(request)?;
        runtime::tool_host::BoundedProcessAuthority::bind(
            self.upgrade_runtime_services()
                .ok_or_else(|| ToolError::new("bounded process Runtime unavailable"))?,
            request.clone(),
        )
    }
    fn bounded_process_lease(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, ToolError> {
        self.validated_bounded_process_lease(request)
    }

    fn bounded_process_plane(&self) -> Option<Arc<runtime::ToolExecutionPlane>> {
        self.upgrade_runtime_services()
            .map(|services| Arc::clone(services.tool_execution_plane()))
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        Some(self.tool_host.pin_snapshot())
    }

    async fn execute_runtime_tool(&self, request: &runtime::RuntimeToolExecutionRequest) -> runtime::RuntimeToolExecutionOutcome {
        self.execute_runtime_tool_carried(request, None).await
    }

    async fn execute_runtime_tool_with_action_invocation(&self, request: &runtime::RuntimeToolExecutionRequest, capability: &runtime::agentic::action_invocation::VerifiedActionInvocation) -> runtime::RuntimeToolExecutionOutcome {
        if let Err(error) = capability.validate_request(request) {
            return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(), tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::BlockedPermission, category: request.category,
                output: None, error: Some(error.to_string()), evidence_ref: String::new(), observed_evidence: vec![],
            };
        }
        self.execute_runtime_tool_carried(request, Some(capability)).await
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

impl GatewayToolExecutor {
    async fn execute_runtime_tool_carried(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
        action_invocation: Option<&runtime::agentic::action_invocation::VerifiedActionInvocation>,
    ) -> runtime::RuntimeToolExecutionOutcome {
        let runtime_services = self.upgrade_runtime_services();
        let evidence_ref = format!(
            "gateway-tool:{}:{}:{}",
            request.governed_plan_id, request.governed_plan_revision, request.tool_use_id
        );
        let step_lease = match request
            .step_binding
            .as_ref()
            .ok_or_else(|| "production_tool_origin_required".to_string())
            .and_then(|binding| {
                let step = binding
                    .resolve(Some(self.tool_host.pin_snapshot()))
                    .map_err(|error| error.to_string())?;
                let session_id = request
                    .session_id
                    .as_deref()
                    .ok_or_else(|| "step call requires session identity".to_string())?;
                step.validate_call(
                    &binding.origin,
                    session_id,
                    &request.tool_use_id,
                    &request.tool_name,
                    &request.input,
                    chrono::Utc::now().timestamp_millis(),
                )
                .map_err(|error| error.to_string())?;
                if matches!(
                    binding.origin.source,
                    harness_contract::step::CallSource::SystemDerived { .. }
                ) {
                    runtime_services
                        .as_ref()
                        .ok_or_else(|| "system call requires Runtime parent authority".to_string())?
                        .commit_service()
                        .validate_system_tool_parent(request)
                        .map_err(|error| error.to_string())?;
                }
                Ok::<_, String>(step.lease().clone())
            }) {
            Ok(lease) => lease,
            Err(error) => {
                return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                    category: request.category,
                    output: None,
                    error: Some(error),
                    evidence_ref,
                    observed_evidence: Vec::new(),
                }
            }
        };
        let Some(canonical_tool_name) = step_lease
            .snapshot()
            .catalog
            .canonical_name(&request.tool_name)
        else {
            return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
        if request.evaluation_isolated
            && request.category != runtime::ToolSafetyCategory::ReadOnly
            && !request
                .authorized_scopes
                .iter()
                .any(|scope| scope.starts_with("write:.cowd/evaluation/"))
        {
            return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: runtime::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some(
                    "paired evaluation mutation is missing its Runtime-issued isolated output lease"
                        .to_string(),
                ),
                evidence_ref,
                observed_evidence: Vec::new(),
            };
        }
        let value: serde_json::Value = match serde_json::from_str(&request.input) {
            Ok(value) => value,
            Err(error) => {
                return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
            return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
            runtime_services
                .as_ref()
                .and_then(|services| services.session_execution_policy(session_id))
        });
        if current_policy.as_ref().map(|policy| policy.revision) != Some(request.policy_revision) {
            return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
            return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
                return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
            let host_lease = step_lease.clone();
            let effect = host_lease.describe_effect(&request.tool_name, &value);
            match request.authorization.as_ref() {
                Some(authorization) => {
                    if let Err(error) =
                        host_lease.validate_authorization(authorization, &request.tool_name, &value)
                    {
                        return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
                    return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
                let Some(services) = runtime_services.as_ref().map(Arc::clone) else {
                    return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
                        return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
                        return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: None,
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
        let mut typed_observations = Vec::new();
        let result = if request.tool_name == "tool_search" {
            self.execute_search_tool(value)
        } else if is_gateway_runtime_control_tool(&request.tool_name)
            || is_gateway_context_tool(&request.tool_name)
        {
            let binding = RuntimeToolExecutionBinding { action_invocation,
                    tool_lease: Some(&step_lease),
                    action_id: Some(request.idempotency_key.as_str()),
                    session_id: request.session_id.as_deref(),
                    authorized_scopes: &request.authorized_scopes,
                    memory_context: request.memory_context.as_ref(),
                    reality_context: request.reality_context.as_ref(),
                    model_lease: request.model_lease.as_deref(),
                    parent_execution: request.parent_execution.as_ref(),
                    parent_execution_attempt: request.parent_execution_attempt,
                    execution_decision: request.execution_decision.as_ref(),
                    permission_ceiling: request
                        .authorization
                        .as_ref()
                        .map_or(self.runtime_permission_ceiling, |authorization| {
                            authorization.authorization_lease.ceiling
                        }),
                };
            if request.tool_name == "agent_action_batch" {
                self.execute_agent_action_batch(value, binding, request).await
            } else {
                self.execute_runtime_tool_with_binding(&request.tool_name, value, binding).await
            }
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
            if request.compiled_invocation.as_ref().is_some_and(|invocation| {
                invocation.effect.uses_network && !invocation.effect.spawns_process
            }) {
                async {
                    let input = harness_contract::step::parse_strict_json(&effective_input)
                        .map_err(|error| ToolError::new(error.to_string()))?;
                    self.reject_orchestrated_tools_outside_allow_list(&request.tool_name, &input)?;
                    let binding = runtime::ExecutionCommitService::local_network_binding(request)
                        .map_err(|error| ToolError::new(error.to_string()))?;
                    let (output, fact) = step_lease.execute_joined_local_network(
                        authorization, &request.tool_name, &effective_input, binding,
                        request.session_id.as_deref().ok_or_else(|| ToolError::new("local network Session missing"))?,
                        &request.step_binding.as_ref().ok_or_else(|| ToolError::new("local network Step missing"))?.origin,
                    ).await.map_err(|error| ToolError::new(error.to_string()))?;
                    typed_observations.push(fact.into_observation(
                        request.tool_name.clone(), request.observation_wave_sequence,
                    ));
                    Ok(output)
                }.await
            } else {
                self.execute_authorized_output_with_progress(
                    authorization,
                    &request.tool_name,
                    &effective_input,
                    request.tool_progress.callback(),
                    Some(step_lease),
                )
                .await
                .and_then(|output| {
                    typed_observations = output.observations().to_vec();
                    if let harness_contract::context::ToolOutputDraft::InProcess { evidence, .. } = &output {
                        if let Some(reads) = evidence.downcast_ref::<Vec<tools::OrchestratedRead>>() {
                            request.tool_progress.record_child_reads(reads.clone())
                                .map_err(ToolError::new)?;
                        }
                    }
                    Ok(output.model_text().to_string())
                })
            }
        } else {
            Err(ToolError::new(format!(
                "ordinary tool `{}` requires Runtime authorization",
                request.tool_name
            )))
        };
        let original_members = request.tool_progress.original_members();
        let pending_agent_batch = (!original_members.is_empty() || request.tool_progress.has_original_member_stop()).then(||
            runtime::PendingAgentBatch::new(runtime::PendingAgentBatchValue::RecordedSink(request.tool_progress.clone())));
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
                        return runtime::RuntimeToolExecutionOutcome { pending_agent_batch: pending_agent_batch.clone(),
                            tool_use_id: request.tool_use_id.clone(),
                            tool_name: request.tool_name.clone(),
                            status: if pending_agent_batch.is_some() { runtime::RuntimeToolExecutionStatus::Unknown } else { runtime::RuntimeToolExecutionStatus::Failed },
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
                let mut observed_evidence = typed_observations;
                observed_evidence.extend(gateway_observed_evidence(
                    self,
                    request,
                    &output,
                    &evidence_ref,
                ));
                runtime::RuntimeToolExecutionOutcome { pending_agent_batch: pending_agent_batch.clone(),
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
                let unknown = error.is_unknown() || pending_agent_batch.is_some();
                if let Some((fence, effect_id, services)) = managed_effect.filter(|_| !unknown) {
                    let _ = services.reconcile_managed_agent_effect(
                        &fence,
                        &effect_id,
                        format!("tool adapter returned error: {error}"),
                    );
                }
                // Keep the actual same-call sink alive on an unresolved outcome,
                // including a single action whose checked result was already stored.
                let original_sink = unknown.then(|| runtime::PendingAgentBatch::new(
                    runtime::PendingAgentBatchValue::RecordedSink(request.tool_progress.clone())));
                let pending_agent_batch = match (pending_agent_batch, error.pending_agent_batch().cloned()) {
                    (Some(primary), cause) => Some(runtime::PendingAgentBatch::with_cause(primary, cause)),
                    (None, cause) => cause,
                };
                let pending_agent_batch = match (pending_agent_batch, original_sink) {
                    (Some(primary), cause) => Some(runtime::PendingAgentBatch::with_cause(primary, cause)),
                    (None, cause) => cause,
                };
                runtime::RuntimeToolExecutionOutcome { pending_agent_batch,
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: if unknown { runtime::RuntimeToolExecutionStatus::Unknown } else { runtime::RuntimeToolExecutionStatus::Failed },
                    category: request.category,
                    output: None,
                    error: Some(error.to_string()),
                    evidence_ref,
                    observed_evidence: Vec::new(),
                }
            }
        }
    }

}
