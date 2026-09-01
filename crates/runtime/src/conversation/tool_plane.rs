//! Governed tool-wave execution, evidence preparation, and result publication.

use super::*;

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    /// Execute one graph-owned tool wave. All tool side effects in a normal
    /// conversation turn enter through this method.
    pub(crate) async fn execute_tool_batch_step(
        &self,
        calls: &[ModelToolCall],
        prompter: &crate::permissions::SharedPrompter,
        iteration: usize,
    ) -> Result<ToolBatchStepResult, RuntimeError>
    where
        C: Sync,
    {
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new("turn cancelled before tool execution"));
        }
        use crate::tool_dispatch::ToolRequest;

        let mut requests = calls
            .iter()
            .map(|call| ToolRequest {
                tool_use_id: call.id.clone(),
                tool_name: call.name.clone(),
                input: call.input.clone(),
                depends_on: call.depends_on.clone(),
            })
            .collect::<Vec<_>>();
        let _ = crate::intent_planner::infer_tool_dependencies(&mut requests);
        let pending = calls
            .iter()
            .map(|call| (call.id.clone(), call.name.clone(), call.input.clone()))
            .collect::<Vec<_>>();
        let prepared = self.tool_executor.prepare_governed_invocations(&requests);
        let workspace_root = self.governed_workspace_root()?;
        let compilation =
            GovernedToolCompiler.compile_partial(&workspace_root, &requests, |name, input| {
                prepared
                    .iter()
                    .find(|invocation| {
                        invocation.intent.tool_name == name
                            && invocation.intent.normalized_input == *input
                    })
                    .map(|invocation| {
                        (
                            invocation.effect.clone(),
                            invocation.catalog_revision,
                            invocation.descriptor_set_hash.clone(),
                        )
                    })
            });
        let compilation = match compilation {
            Ok(compilation) => compilation,
            Err(error) => {
                let reason = format!("governed tool DAG rejected before execution: {error}");
                self.append_execution_runtime_event(
                    RuntimeEventScope::Tool,
                    "tool.plan.rejected",
                    Some("rejected".to_string()),
                    calls
                        .iter()
                        .map(|call| RuntimeEventRef {
                            kind: "tool_call".to_string(),
                            id: call.id.clone(),
                        })
                        .collect(),
                    serde_json::json!({
                        "reason": error.to_string(),
                        "tool_count": calls.len(),
                    }),
                );
                let mut messages = Vec::with_capacity(calls.len());
                for call in calls {
                    let message = ConversationMessage::tool_result(
                        call.id.clone(),
                        call.name.clone(),
                        reason.clone(),
                        true,
                    );
                    self.session
                        .write()
                        .await
                        .push_message(message.clone())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    let sequence = self.session_head().await.message_count.wrapping_sub(1);
                    self.record_message_event(&message, sequence);
                    self.remember_tool_trace_from_message(&message);
                    messages.push(message);
                }
                return Ok(ToolBatchStepResult {
                    failed: messages.len(),
                    messages,
                    max_concurrency_observed: 0,
                    parallel_batches: 0,
                });
            }
        };
        let mut preflight_messages = Vec::with_capacity(compilation.rejected.len());
        for rejected in &compilation.rejected {
            let message = ConversationMessage::tool_result(
                rejected.tool_call_id.clone(),
                rejected.tool_name.clone(),
                format!(
                    "governed tool node rejected before execution: {}",
                    rejected.reason
                ),
                true,
            );
            self.session
                .write()
                .await
                .push_message(message.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let sequence = self.session_head().await.message_count.wrapping_sub(1);
            self.record_message_event(&message, sequence);
            self.remember_tool_trace_from_message(&message);
            preflight_messages.push(message);
        }
        if !compilation.rejected.is_empty() {
            self.append_execution_runtime_event(
                RuntimeEventScope::Tool,
                "tool.plan.partially_rejected",
                Some("partial".to_string()),
                compilation
                    .rejected
                    .iter()
                    .map(|rejected| RuntimeEventRef {
                        kind: "tool_call".to_string(),
                        id: rejected.tool_call_id.clone(),
                    })
                    .collect(),
                serde_json::json!({
                    "rejected": compilation.rejected,
                    "accepted_count": compilation.plan.as_ref().map_or(0, |plan| plan.task_count),
                }),
            );
        }
        let Some(plan) = compilation.plan else {
            return Ok(ToolBatchStepResult {
                failed: preflight_messages.len(),
                messages: preflight_messages,
                max_concurrency_observed: 0,
                parallel_batches: 0,
            });
        };
        self.record_governed_tool_plan(&plan, self.session_head().await.message_count);
        let decision = self.retarget_active_turn_strategy_for_governed_plan(&plan, calls)?;
        self.tool_executor.bind_execution_decision(decision.clone());
        let mut validation = plan.validate_against_execution_decision(&decision);
        if validation.allowed {
            self.satisfy_tool_strategy_gates(&decision, &mut validation, prompter)
                .await;
        }
        self.record_tool_strategy_validation(&validation, self.session_head().await.message_count);
        let mut max_concurrency_observed = 0;
        let mut parallel_batches = 0;
        let mut messages = preflight_messages;
        if validation.allowed {
            self.record_tool_schedule(&plan, &requests, self.session_head().await.message_count);
            let context = ConversationGovernedToolContext {
                runtime: self,
                pending_tool_uses: &pending,
                prompter,
                iterations: iteration,
                plan_id: &plan.plan_id,
                plan_revision: plan.revision,
            };
            let report = GovernedToolExecutor.execute(&plan, &context).await;
            max_concurrency_observed = report.max_active;
            parallel_batches = usize::from(report.max_active > 1);
            for outcome in report.outcomes {
                let Some((message, _)) = outcome.receipt else {
                    return Err(RuntimeError::new(format!(
                        "governed tool task `{}` reached terminal state without a durable result receipt",
                        outcome.task_id
                    )));
                };
                self.remember_tool_trace_from_message(&message);
                messages.push(message);
            }
        } else {
            let reason = format!(
                "runtime strategy lease `{}` denied tool batch: {}",
                validation.lease_id,
                validation.findings.join(", ")
            );
            for call in calls {
                let message = ConversationMessage::tool_result(
                    call.id.clone(),
                    call.name.clone(),
                    reason.clone(),
                    true,
                );
                self.session
                    .write()
                    .await
                    .push_message(message.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&message, sequence);
                self.remember_tool_trace_from_message(&message);
                messages.push(message);
            }
        }
        let failed = count_failed_tool_results(&messages);
        Ok(ToolBatchStepResult {
            messages,
            failed,
            max_concurrency_observed,
            parallel_batches,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn finalize_graph_turn(
        &mut self,
        user_input: &str,
        final_answer: String,
        assistant_messages: Vec<ConversationMessage>,
        tool_results: Vec<ConversationMessage>,
        iterations: usize,
        model: Option<String>,
        models_used: Vec<String>,
        first_token_latency_ms: Option<u64>,
        active_stream_duration_ms: u64,
        input_tokens: u64,
        output_tokens: u64,
        wall_duration_ms: u64,
        duplicate_tool_calls: u64,
        write_attempt_paths: Vec<String>,
        max_tool_concurrency_observed: usize,
        parallel_tool_batches: usize,
        terminal_completion: harness_contract::goal::GoalCompletion,
        defer_post_turn_memory_maintenance: bool,
    ) -> Result<TurnSummary, RuntimeError> {
        let finalize_started = Instant::now();
        if final_answer.trim().is_empty() {
            return Err(RuntimeError::new("model produced an empty final answer"));
        }
        if self.active_turn_strategy().is_none() {
            return Err(RuntimeError::new(
                "turn finalization requires the Host-admitted turn strategy owner",
            ));
        }
        let decision = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("turn finalization has no strategy owner"))?;
        let mut kernel = RuntimeAiKernel::begin_turn_with_execution_decision(
            self.session_id().to_string(),
            user_input.to_string(),
            self.context_profile(),
            &self.system_prompt,
            decision,
        );
        if let Ok(plans) = self.turn_governed_tool_plans.lock() {
            for plan in plans.iter().cloned() {
                kernel.record_governed_tool_plan(plan);
            }
        }
        if !matches!(
            terminal_completion,
            harness_contract::goal::GoalCompletion::Satisfied
        ) {
            kernel.record_terminal_blocked(
                "the execution graph reached a non-satisfied terminal completion",
            );
        }
        let failed_tools = count_failed_tool_results(&tool_results);
        let ai_kernel_trace = kernel.finalize(
            &final_answer,
            tool_results.len().saturating_sub(failed_tools),
            failed_tools,
        );
        // Request-preflight owns compaction. Finalization never rewrites a
        // healthy transcript merely because an aggregate token estimate grew.
        let auto_compaction = self
            .turn_preflight_compaction
            .lock()
            .ok()
            .and_then(|mut receipt| receipt.take());
        let compaction_elapsed = Duration::ZERO;
        let memory_started = Instant::now();
        if defer_post_turn_memory_maintenance {
            self.schedule_memory_post_turn(user_input).await;
        } else {
            let _ = self.run_memory_post_turn(user_input).await;
        }
        self.memory_context_revision.fetch_add(1, Ordering::AcqRel);
        let memory_elapsed = memory_started.elapsed();
        let usage = self.usage_tracker.cumulative_usage();
        let telemetry = crate::cowd_event::RunModelTelemetry {
            model: model.clone(),
            models_used,
            first_token_latency_ms,
            active_stream_duration_ms: Some(active_stream_duration_ms.max(1)),
            wall_duration_ms: wall_duration_ms.max(1),
            output_chars: final_answer.chars().count() as u64,
            output_chunks: iterations as u64,
            input_tokens,
            output_tokens,
            cache_create_tokens: u64::from(usage.cache_creation_input_tokens),
            cache_read_tokens: u64::from(usage.cache_read_input_tokens),
            total_tokens: input_tokens.saturating_add(output_tokens),
            usage_source: "provider".to_string(),
            wall_chars_per_second: rate_per_second(
                final_answer.chars().count() as u64,
                wall_duration_ms.max(1),
            ),
            wall_tokens_per_second: rate_per_second(output_tokens, wall_duration_ms.max(1)),
            active_chars_per_second: None,
            active_tokens_per_second: None,
            chars_per_second: rate_per_second(
                final_answer.chars().count() as u64,
                wall_duration_ms.max(1),
            ),
            tokens_per_second: rate_per_second(output_tokens, wall_duration_ms.max(1)),
        };
        let context_turn_report = self.build_context_turn_report(
            &ai_kernel_trace.harness_receipt.id,
            usage,
            auto_compaction.clone(),
        );
        self.remember_context_turn_report(context_turn_report.clone())
            .await?;
        let mut assistant_messages = assistant_messages;
        if !matches!(
            terminal_completion,
            harness_contract::goal::GoalCompletion::Satisfied
        ) {
            assistant_messages.push(ConversationMessage::assistant(vec![ContentBlock::Text {
                text: final_answer.clone(),
            }]));
        }
        let summary = TurnSummary {
            final_answer,
            terminal_completion,
            assistant_messages,
            tool_results,
            iterations,
            usage,
            model_telemetry: telemetry,
            auto_compaction,
            ai_kernel_trace,
            context_turn_report,
            model_observations: self.turn_model_observations(),
            duplicate_tool_calls,
            write_attempt_paths,
            max_tool_concurrency_observed,
            parallel_tool_batches,
        };
        self.record_turn_completed(&summary);
        self.record_ai_kernel_trace_event(
            &summary.ai_kernel_trace,
            self.session_head().await.message_count,
        );
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::WriteAttemptsObserved {
                paths: summary.write_attempt_paths.clone(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::RunModelTelemetry {
                telemetry: summary.model_telemetry.clone(),
            });
        }
        let commit_ms = finalize_started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        crate::execution_core::performance::record_turn_latency_trace(
            crate::execution_core::performance::TurnLatencyTrace {
                trace_id: summary.ai_kernel_trace.harness_receipt.id.clone(),
                session_id: self.session_id().to_string(),
                turn_id: None,
                activation_ms: None,
                context_ms: None,
                provider_ms: Some(wall_duration_ms),
                tool_ms: None,
                commit_ms: Some(commit_ms),
                total_ms: wall_duration_ms.saturating_add(commit_ms),
                recorded_at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_millis() as u64),
            },
        );
        tracing::debug!(
            total_ms = finalize_started.elapsed().as_millis(),
            compaction_ms = compaction_elapsed.as_millis(),
            memory_post_turn_ms = memory_elapsed.as_millis(),
            post_turn_memory_deferred = defer_post_turn_memory_maintenance,
            "graph turn finalization completed"
        );
        Ok(summary)
    }

    pub(super) async fn satisfy_tool_strategy_gates(
        &self,
        execution_decision: &crate::execution_core::RuntimeExecutionDecision,
        validation: &mut GovernedToolPolicyValidationReport,
        prompter: &crate::permissions::SharedPrompter,
    ) {
        // `requires_approval` is a scheduling signal only. Authorization is
        // deliberately resolved per concrete Tool effect below; a batch-level
        // boolean can never authorize sibling calls with different hashes.

        if !validation.requires_checkpoint {
            return;
        }
        if !self.tool_executor.has_tool("checkpoint_create") {
            validation.allowed = false;
            validation
                .findings
                .push("checkpoint_create_tool_unavailable".to_string());
            return;
        }

        let checkpoint_input = serde_json::json!({
            "label": format!(
                "runtime strategy lease {} before high-risk mutation",
                execution_decision.lease.lease_id
            )
        })
        .to_string();
        let executor = Arc::clone(&self.tool_executor);
        let checkpoint_value = match serde_json::from_str::<serde_json::Value>(&checkpoint_input) {
            Ok(value) => value,
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_input_invalid:{error}"));
                return;
            }
        };
        let Some(descriptor) =
            executor.registered_tool_effect("checkpoint_create", &checkpoint_value)
        else {
            validation.allowed = false;
            validation
                .findings
                .push("checkpoint_create_missing_effect_descriptor".to_string());
            return;
        };
        let timeout = self.tool_timeout.unwrap_or_else(|| {
            Duration::from_secs(
                crate::ToolSafetyCategory::from_effect(&descriptor).default_timeout_secs(),
            )
        });
        let authorization = match self
            .negotiate_tool_authorization(
                &descriptor,
                &checkpoint_input,
                format!(
                    "{}:checkpoint:{}",
                    self.session_id().to_string(),
                    execution_decision.lease.lease_id
                ),
                PermissionContext::default(),
                timeout.as_secs(),
                prompter,
            )
            .await
        {
            Ok(ToolAuthorizationDecision::Authorized(decision)) => decision,
            Ok(ToolAuthorizationDecision::Gap { assessment, .. }) => {
                validation.allowed = false;
                validation.findings.push(format!(
                    "checkpoint_authorization_denied:{}",
                    assessment
                        .gap
                        .as_ref()
                        .map_or("unknown capability gap", |gap| gap.reason.as_str())
                ));
                return;
            }
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_authorization_denied:{error}"));
                return;
            }
        };
        let checkpoint_demand = crate::governed_tool_plan::resource_demand_from_effect(&descriptor);
        let result = self
            .tool_execution_plane
            .execute_async_classified_retained(
                &checkpoint_demand,
                Some(timeout),
                self.execution_service_class,
                Some(self.execution_service_class),
                Some(self.session_id()),
                async move {
                    executor
                        .execute_authorized_output(
                            &authorization.authorization,
                            "checkpoint_create",
                            &checkpoint_input,
                        )
                        .await
                },
            )
            .await;
        let result = result.0;
        match result {
            Ok(Ok(output)) => {
                validation.checkpoint_created = true;
                tracing::info!(
                    strategy_lease_id = %execution_decision.lease.lease_id,
                    checkpoint = %preview_chars(output.model_text(), 240),
                    "strategy checkpoint created before mutation"
                );
            }
            Ok(Err(error)) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_creation_failed:{error}"));
            }
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_execution_failed:{error}"));
            }
        }
    }

    /// Extract the per-tool execution logic from run_turn for reuse.
    pub(super) async fn execute_single_tool(
        &self,
        task: &crate::governed_tool_plan::GovernedToolPlanTask,
        plan_id: &str,
        plan_revision: u64,
        input: &str,
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        retained_admission: &mut Option<crate::ToolExecutionAdmission>,
    ) -> Result<ConversationMessage, RuntimeError> {
        let tool_use_id = task.tool_call_id.as_str();
        let tool_name = task.tool_name.as_str();
        let pre_hook_result = self.run_pre_tool_use_hook(tool_name, input);
        let effective_input = pre_hook_result
            .updated_input()
            .map_or_else(|| input.to_string(), ToOwned::to_owned);
        let model_delivery_requirement = self
            .tool_executor
            .model_delivery_requirement(tool_name, &effective_input);
        let mut permission_context = PermissionContext::new(
            pre_hook_result.permission_override(),
            pre_hook_result.permission_reason().map(ToOwned::to_owned),
        );
        if pre_hook_result.is_cancelled() {
            permission_context = PermissionContext::new(
                Some(crate::permissions::PermissionOverride::Deny),
                Some(format!("PreToolUse hook cancelled tool `{tool_name}`")),
            );
        } else if pre_hook_result.is_failed() {
            let hook_msgs = pre_hook_result.messages().join("; ");
            permission_context = PermissionContext::new(
                Some(crate::permissions::PermissionOverride::Deny),
                Some(if hook_msgs.is_empty() {
                    format!("PreToolUse hook failed for tool `{tool_name}`")
                } else {
                    format!("PreToolUse hook failed for tool `{tool_name}`: {hook_msgs}")
                }),
            );
        } else if pre_hook_result.is_denied() {
            permission_context = PermissionContext::new(
                Some(crate::permissions::PermissionOverride::Deny),
                Some(format!("PreToolUse hook denied tool `{tool_name}`")),
            );
        }
        let profile_timeout = Duration::from_secs(task.safety_category.default_timeout_secs());
        let tool_timeout = self
            .tool_timeout
            .map_or(profile_timeout, |timeout| timeout.min(profile_timeout));
        let authorization_id = format!(
            "{}:{plan_id}:{plan_revision}:{tool_use_id}:{iterations}",
            self.session_id()
        );
        let authorization_decision = self
            .negotiate_tool_authorization(
                &task.effect,
                &effective_input,
                authorization_id,
                permission_context,
                tool_timeout.as_secs(),
                prompter,
            )
            .await?;

        match authorization_decision {
            ToolAuthorizationDecision::Authorized(authorization) => {
                let execution_policy = self.permission_policy.execution_policy_control().snapshot();
                if execution_policy.revision != authorization.authorization.policy_revision {
                    return Err(RuntimeError::new(format!(
                        "session_policy_revision_stale: authorization rev {} current rev {}; replan before tool admission",
                        authorization.authorization.policy_revision, execution_policy.revision
                    )));
                }
                let invocation_record = self
                    .start_tool_invocation_record(
                        tool_use_id,
                        tool_name,
                        &effective_input,
                        iterations,
                    )
                    .with_governed_plan(plan_id, plan_revision);
                self.verify_session_execution_fence(
                    crate::SessionExecutionFencePhase::ToolExecution,
                )
                .await?;
                self.record_tool_invocation_event(
                    &invocation_record,
                    "tool.invocation.started",
                    self.session_head().await.message_count,
                );
                self.record_tool_started(iterations, tool_name);
                if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
                    metrics.observe_invocation(tool_name);
                }
                if let Some(callback) = &self.tool_callback {
                    let preview: String = effective_input.chars().take(200).collect();
                    callback.on_tool_start(tool_use_id, tool_name, &preview);
                }

                let start = Instant::now();
                let tname = tool_name.to_string();
                let tname_for_err = tname.clone();
                let tinput = effective_input.clone();
                let provider_invocation_id = tool_use_id.to_string();
                let tool_exec = Arc::clone(&self.tool_executor);
                let evidence_sandbox = self.tool_output_sandbox.clone();
                let is_evidence_retrieve = tool_name == "evidence_retrieve";
                let demand = task.resource_demand.clone();
                let plane = Arc::clone(&self.tool_execution_plane);
                let executor_owns_durable_effect =
                    self.tool_executor.owns_durable_tool_effect(tool_name);
                let effect_request = crate::RuntimeToolExecutionRequest {
                    governed_plan_id: plan_id.to_string(),
                    governed_plan_revision: plan_revision,
                    observation_wave_sequence: u64::try_from(iterations).unwrap_or(u64::MAX).max(1),
                    idempotency_key: format!(
                        "{}:{plan_id}:{plan_revision}:{tool_use_id}:{iterations}",
                        self.session_id()
                    ),
                    tool_use_id: tool_use_id.to_string(),
                    tool_name: tool_name.to_string(),
                    input: effective_input.clone(),
                    category: task.safety_category,
                    authorization: Some(authorization.authorization.clone()),
                    session_id: Some(self.session_id().to_string()),
                    sandbox_posture: execution_policy.sandbox_posture,
                    policy_revision: authorization.authorization.policy_revision,
                    authorized_scopes: vec![format!("session:{}", self.session_id())],
                    memory_context: Some(self.memory_turn_context()),
                    model_lease: None,
                    parent_execution: None,
                    parent_execution_attempt: None,
                    execution_decision: None,
                    evaluation_isolated: false,
                    managed_invocation: None,
                    tool_progress: crate::ToolProgressSink(self.cowd_bus.as_ref().map(|bus| {
                        let bus = bus.clone();
                        let id = tool_use_id.to_string();
                        let name = tool_name.to_string();
                        let callback: std::sync::Arc<dyn Fn(&str) + Send + Sync> =
                            std::sync::Arc::new(move |progress| {
                                bus.emit_tool_progress(&id, &name, progress);
                            });
                        callback
                    })),
                };
                let effect_commit = (!executor_owns_durable_effect)
                    .then(|| {
                        self.runtime_event_store.as_ref().map(|store| {
                            crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(
                                store,
                            ))
                        })
                    })
                    .flatten();
                let effect_state = match (executor_owns_durable_effect, effect_commit.as_ref()) {
                    (true, _) => crate::execution_core::graph::ToolEffectState::NotRequired,
                    (false, Some(commit)) => commit
                        .begin_tool_effect(&effect_request, &task.effect)
                        .map_err(|error| RuntimeError::new(error.to_string()))?,
                    (false, None)
                        if task.effect.effect_kind
                            == harness_contract::tool::ToolEffectKind::Read =>
                    {
                        crate::execution_core::graph::ToolEffectState::NotRequired
                    }
                    (false, None) => {
                        return Err(RuntimeError::new(
                            "mutation tool execution requires the durable Runtime effect ledger",
                        ));
                    }
                };
                let execute_fresh = matches!(
                    effect_state,
                    crate::execution_core::graph::ToolEffectState::Fresh
                        | crate::execution_core::graph::ToolEffectState::NotRequired
                );
                let execution = match effect_state {
                    crate::execution_core::graph::ToolEffectState::Completed(outcome) => {
                        if outcome.status == crate::RuntimeToolExecutionStatus::Executed {
                            Ok(Ok(
                                harness_contract::context::ToolOutputDraft::bounded_inline(
                                    outcome.output.unwrap_or_default(),
                                ),
                            ))
                        } else {
                            Ok(Err(ToolError::new(
                                outcome
                                    .error
                                    .or(outcome.output)
                                    .unwrap_or_else(|| "durable tool effect failed".to_string()),
                            )))
                        }
                    }
                    crate::execution_core::graph::ToolEffectState::Uncertain => {
                        return Err(RuntimeError::new(format!(
                            "tool effect `{}` is uncertain; non-idempotent execution was not replayed",
                            effect_request.idempotency_key
                        )));
                    }
                    crate::execution_core::graph::ToolEffectState::Fresh
                    | crate::execution_core::graph::ToolEffectState::NotRequired => {
                        let (execution, admission) = plane
                            .execute_async_classified_retained(
                                &demand,
                                Some(tool_timeout),
                                self.execution_service_class,
                                Some(self.execution_service_class),
                                Some(self.session_id()),
                                async move {
                                    if is_evidence_retrieve {
                                        return retrieve_tool_evidence_from_sandbox(
                                            evidence_sandbox.as_ref(),
                                            &tinput,
                                        )
                                        .map(harness_contract::context::ToolOutputDraft::bounded_inline)
                                        .map_err(ToolError::new);
                                    }
                                    if matches!(
                                        tname.as_str(),
                                        "tool_search" | "runtime_capabilities" | "team_board"
                                    ) {
                                        tool_exec
                                            .execute_invocation_output(
                                                &provider_invocation_id,
                                                &tname,
                                                &tinput,
                                            )
                                            .await
                                    } else {
                                        tool_exec
                                            .execute_authorized_invocation_output(
                                                &provider_invocation_id,
                                                &authorization.authorization,
                                                &tname,
                                                &tinput,
                                            )
                                            .await
                                    }
                                },
                            )
                            .await;
                        *retained_admission = admission;
                        execution
                    }
                };
                let (output_draft, mut is_error, mut failure_kind) = match execution {
                    Ok(Ok(output)) => (output, false, None),
                    Ok(Err(error)) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            error.to_string(),
                        ),
                        true,
                        Some(ToolFailureKind::ExecutionError),
                    ),
                    Err(crate::ToolExecutionPlaneError::TimedOut(_)) => {
                        tracing::warn!(tool = %tname_for_err, timeout_secs = tool_timeout.as_secs(), "tool execution waiter timed out; started operation remains fenced");
                        (
                            harness_contract::context::ToolOutputDraft::bounded_inline(format!(
                                "tool `{tname_for_err}` timed out after {tool_timeout:?}"
                            )),
                            true,
                            Some(ToolFailureKind::Timeout),
                        )
                    }
                    Err(crate::ToolExecutionPlaneError::Panicked) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            "tool execution panicked",
                        ),
                        true,
                        Some(ToolFailureKind::Panic),
                    ),
                    Err(error) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            error.to_string(),
                        ),
                        true,
                        Some(ToolFailureKind::ExecutionError),
                    ),
                };
                let output = output_draft.model_text().to_string();
                if execute_fresh && !executor_owns_durable_effect {
                    self.verify_session_execution_fence(
                        crate::SessionExecutionFencePhase::ToolCommit,
                    )
                    .await?;
                    if let Some(commit) = effect_commit.as_ref() {
                        commit
                            .commit_tool_effect(
                                &effect_request,
                                &task.effect,
                                &crate::RuntimeToolExecutionOutcome {
                                    tool_use_id: tool_use_id.to_string(),
                                    tool_name: tool_name.to_string(),
                                    status: if is_error {
                                        crate::RuntimeToolExecutionStatus::Failed
                                    } else {
                                        crate::RuntimeToolExecutionStatus::Executed
                                    },
                                    category: task.safety_category,
                                    output: (!is_error).then(|| output.clone()),
                                    error: is_error.then(|| output.clone()),
                                    evidence_ref: format!(
                                        "tool-effect:{}",
                                        effect_request.idempotency_key
                                    ),
                                    observed_evidence: Vec::new(),
                                },
                            )
                            .map_err(|error| {
                                RuntimeError::new(format!(
                                    "tool effect completed but durable receipt failed: {error}"
                                ))
                            })?;
                    }
                }
                let elapsed_ms = start.elapsed().as_millis() as u64;
                self.hook_runner
                    .fire_post_tool(tool_name, &output, is_error, elapsed_ms);

                if let Some(callback) = &self.tool_callback {
                    let summary: String = output.chars().take(500).collect();
                    let exit_code = if is_error { Some(1) } else { Some(0) };
                    callback.on_tool_complete(tool_use_id, tool_name, &summary, exit_code);
                }

                let post_hook_result = if is_error {
                    self.run_post_tool_use_failure_hook(tool_name, &effective_input, &output)
                } else {
                    self.run_post_tool_use_hook(tool_name, &effective_input, &output, false)
                };
                if post_hook_result.is_denied()
                    || post_hook_result.is_failed()
                    || post_hook_result.is_cancelled()
                {
                    is_error = true;
                    if failure_kind.is_none() {
                        failure_kind = Some(ToolFailureKind::HookDenied);
                    }
                }

                let elapsed_ms = start.elapsed().as_millis() as u64;
                if let Some(cowd) = self.cowd_bus() {
                    cowd.emit(crate::cowd_event::CowdEvent::ToolExecuted {
                        name: tool_name.to_string(),
                        duration_ms: elapsed_ms,
                    });
                }

                // T36: Truncate oversized tool results before storing.
                // Append hook feedback messages to the tool output.
                let tool_search_activated = tool_name == "tool_search"
                    && !is_error
                    && self
                        .activate_tool_discovery(&output)
                        .is_some_and(|receipt| receipt.activated_ids().next().is_some());
                let mut combined = if tool_name == "runtime_capabilities" && !is_error {
                    self.project_runtime_capabilities_for_model(&output)
                } else {
                    output
                };
                if tool_search_activated {
                    combined.push_str(
                        "\n\nThe discovered tools are active on the immediately following \
                         automatic model request in this same turn. Continue the current task \
                         and invoke the relevant activated tool directly; do not ask the user \
                         to resend solely because activation just completed.",
                    );
                }
                for msg in pre_hook_result.messages() {
                    combined.push('\n');
                    combined.push_str(msg);
                }
                for msg in post_hook_result.messages() {
                    combined.push('\n');
                    combined.push_str(msg);
                }
                let completed_record = if is_error {
                    invocation_record.failed_with_output_policy(
                        failure_kind.unwrap_or(ToolFailureKind::Unknown),
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                } else {
                    invocation_record.completed_with_output_policy(
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                };
                let prepared_vision = prepared_vision_payload(tool_name, &combined, is_error);
                let indexable_output = prepared_vision
                    .as_ref()
                    .map(vision_index_summary)
                    .unwrap_or_else(|| combined.clone());
                let (raw_ref, raw_access) = self
                    .record_tool_output_evidence(
                        tool_use_id,
                        tool_name,
                        &completed_record.input_hash,
                        &output_draft,
                        &combined,
                        is_error,
                        elapsed_ms,
                        None,
                    )
                    .await?;
                self.maybe_index_tool_output(
                    raw_ref.id(),
                    tool_name,
                    &indexable_output,
                    Some(&raw_access),
                );
                let completed_record =
                    completed_record.with_full_output_ref(format!("tool://{}", raw_ref.id()));
                let mut model_receipt = self.tool_model_receipt(
                    tool_name,
                    &combined,
                    is_error,
                    &raw_ref,
                    Some(&raw_access),
                    &model_delivery_requirement,
                );
                if let Some(payload) = prepared_vision.as_ref() {
                    model_receipt.summary = vision_tool_model_receipt(payload, &raw_ref);
                    model_receipt.receipt_tokens =
                        crate::context_ledger::estimate_text_tokens(&model_receipt.summary);
                    model_receipt.omitted_tokens = model_receipt
                        .raw_tokens
                        .saturating_sub(model_receipt.receipt_tokens);
                    model_receipt.truncated =
                        model_receipt.receipt_tokens < model_receipt.raw_tokens;
                }
                self.record_generated_model_receipt(
                    tool_use_id,
                    tool_name,
                    &model_delivery_requirement,
                    &raw_ref,
                    &model_receipt,
                    is_error,
                )?;
                let audit_projection =
                    crate::context_evidence::audit_projection(&model_receipt, Some(&raw_access));
                self.push_turn_evidence_audit(audit_projection);
                let model_summary = model_receipt.summary;
                let output_envelope = harness_contract::context::ToolOutputEnvelope {
                    artifact_ref: Some(harness_contract::context::ArtifactRef::durable(
                        raw_access.retrieval_selector.clone(),
                        raw_access.sha256.clone(),
                        raw_access.bytes,
                        raw_access.media_type.clone(),
                        raw_access.visibility_scope.clone(),
                    )),
                    evidence_ref: Some(raw_access),
                    receipt: completed_record
                        .full_output_ref
                        .clone()
                        .unwrap_or_else(|| format!("tool://{}", raw_ref.id())),
                };
                self.push_turn_tool_observation(
                    ToolObservation::new(
                        tool_name.to_string(),
                        completed_record.invocation_id.clone(),
                        raw_ref,
                        model_summary.clone(),
                    )
                    .with_output_envelope(output_envelope),
                );
                let result = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    model_summary,
                    is_error,
                );
                self.session
                    .write()
                    .await
                    .push_message(result.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&result, sequence);
                if let Some(payload) = prepared_vision {
                    let image_message = vision_user_message(&payload);
                    self.session
                        .write()
                        .await
                        .push_message(image_message.clone())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    self.record_message_event(
                        &image_message,
                        self.session_head().await.message_count.wrapping_sub(1),
                    );
                }
                self.record_tool_invocation_event(
                    &completed_record,
                    if is_error {
                        "tool.invocation.failed"
                    } else {
                        "tool.invocation.completed"
                    },
                    self.session_head().await.message_count.wrapping_sub(1),
                );
                self.record_tool_finished(iterations, &result);
                Ok(result)
            }
            ToolAuthorizationDecision::Gap { assessment, .. } => {
                let gap = assessment.gap.as_ref();
                let reason = gap.map_or_else(
                    || "capability authorization was not granted".to_string(),
                    |gap| gap.reason.clone(),
                );
                let failure_kind = if reason.starts_with("PreToolUse hook") {
                    ToolFailureKind::HookDenied
                } else {
                    ToolFailureKind::PermissionDenied
                };
                self.record_tool_invocation_denied(
                    tool_use_id,
                    tool_name,
                    &effective_input,
                    iterations,
                    failure_kind,
                    &reason,
                );
                let first_recovery = gap.is_some_and(|gap| gap.recoverable);
                let payload = serde_json::json!({
                    "kind": "capability_gap",
                    "assessment_id": assessment.assessment_id,
                    "path": assessment.path,
                    "gap": assessment.gap,
                    "controlled_recovery_available": first_recovery,
                    "instruction": if first_recovery {
                        "Use one listed safe alternative or revise the plan with existing capabilities. Do not repeat the same denied action without new evidence or approval."
                    } else {
                        "The same capability gap is closed for this turn. Preserve evidence and report the limitation without retrying the denied action."
                    },
                })
                .to_string();
                let denied = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    payload,
                    !first_recovery,
                );
                self.session
                    .write()
                    .await
                    .push_message(denied.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&denied, sequence);
                Ok(denied)
            }
        }
    }

    pub(super) fn collect_tool_result_message(
        &self,
        result_msg: ConversationMessage,
    ) -> (String, (ConversationMessage, Option<String>)) {
        let (msg_id, tool_name) = extract_tool_info(&result_msg);
        let inject = self.turn_callback.as_ref().and_then(|callback| {
            let output = result_msg
                .blocks
                .first()
                .and_then(|block| match block {
                    ContentBlock::ToolResult { output, .. } => Some(output.as_str()),
                    _ => None,
                })
                .unwrap_or("");
            (callback.on_tool_result)(&tool_name, output)
        });
        (msg_id, (result_msg, inject))
    }

    /// Ingest an outcome already executed by the graph-owned tool host.
    /// The graph remains responsible for publication; this method persists
    /// raw evidence, updates context governance, and applies Runtime-owned
    /// capability discovery before the next model request.
    #[cfg(test)]
    pub(crate) async fn prepare_governed_tool_result(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> Result<ConversationMessage, RuntimeError> {
        self.prepare_governed_tool_result_with_invocation(
            tool_use_id,
            tool_name,
            input,
            output,
            is_error,
            None,
        )
        .await
    }

    pub(crate) async fn prepare_governed_tool_result_with_invocation(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
        invocation: Option<ToolInvocationRecord>,
    ) -> Result<ConversationMessage, RuntimeError> {
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_invocation(tool_name);
        }
        let tool_search_activated = tool_name == "tool_search"
            && !is_error
            && self
                .activate_tool_discovery(output)
                .is_some_and(|receipt| receipt.activated_ids().next().is_some());
        let input_hash = format!(
            "{:016x}",
            model_protocol::fingerprint::stable_hash_bytes(input.as_bytes())
        );
        let source_evidence_ref = format!("runtime-tool:{tool_use_id}");
        let (raw_ref, raw_access) = self
            .record_tool_raw_evidence(
                tool_use_id,
                tool_name,
                &input_hash,
                output,
                is_error,
                0,
                Some(&source_evidence_ref),
            )
            .await?;
        if let Some(mut terminal) = invocation {
            let sequence = self.session_head_blocking().message_count;
            terminal.session_id = self.session_id().to_string();
            terminal.turn_index = sequence;
            terminal = terminal.with_full_output_ref(format!("tool://{}", raw_ref.id()));
            self.record_tool_invocation_event(
                &terminal.started_fact(),
                "tool.invocation.started",
                sequence,
            );
            let terminal_kind = match terminal.status.as_str() {
                "completed" => "tool.invocation.completed",
                "denied" => "tool.invocation.denied",
                _ => "tool.invocation.failed",
            };
            self.record_tool_invocation_event(&terminal, terminal_kind, sequence);
        }
        self.maybe_index_tool_output(raw_ref.id(), tool_name, output, Some(&raw_access));
        let delivery_requirement = self
            .tool_executor
            .model_delivery_requirement(tool_name, input);
        let receipt = self.tool_model_receipt(
            tool_name,
            output,
            is_error,
            &raw_ref,
            Some(&raw_access),
            &delivery_requirement,
        );
        self.record_generated_model_receipt(
            tool_use_id,
            tool_name,
            &delivery_requirement,
            &raw_ref,
            &receipt,
            is_error,
        )?;
        self.push_turn_evidence_audit(crate::context_evidence::audit_projection(
            &receipt,
            Some(&raw_access),
        ));
        let mut summary = receipt.summary;
        if tool_search_activated {
            summary.push_str(
                "\n\nThe discovered tools are active on the immediately following automatic \
                 model request in this same turn. Continue the current task and invoke the \
                 relevant activated tool directly; do not ask the user to resend solely because \
                 activation just completed.",
            );
        }
        let output_envelope = harness_contract::context::ToolOutputEnvelope {
            artifact_ref: Some(harness_contract::context::ArtifactRef::durable(
                raw_access.retrieval_selector.clone(),
                raw_access.sha256.clone(),
                raw_access.bytes,
                raw_access.media_type.clone(),
                raw_access.visibility_scope.clone(),
            )),
            evidence_ref: Some(raw_access),
            receipt: format!("tool://{}", raw_ref.id()),
        };
        self.push_turn_tool_observation(
            ToolObservation::new(
                tool_name.to_string(),
                tool_use_id.to_string(),
                raw_ref,
                summary.clone(),
            )
            .with_output_envelope(output_envelope),
        );
        Ok(ConversationMessage::tool_result(
            tool_use_id.to_string(),
            tool_name.to_string(),
            summary,
            is_error,
        ))
    }

    pub(super) fn tool_model_receipt(
        &self,
        tool_name: &str,
        output: &str,
        is_error: bool,
        raw_ref: &EvidenceRef,
        access: Option<&EvidenceAccessRef>,
        delivery_requirement: &ToolModelDeliveryRequirement,
    ) -> crate::context_evidence::ModelReceipt {
        let raw_tokens = crate::context_ledger::estimate_text_tokens(output);
        let plan = if delivery_requirement.is_exact() && !is_error {
            Self::apply_exact_evidence_delivery_budget(self.runtime_budget_plan())
        } else {
            self.runtime_budget_plan()
        };
        let per_tool_limit = plan.tool_result_budget.per_tool_max_tokens as u64;
        if delivery_requirement.is_exact() && !is_error {
            if let Ok(mut ledger) = self.turn_context_ledger.lock() {
                ledger.expand_tool_result_limit(plan.tool_result_budget.max_total_tokens as u64);
            }
        }
        // `build_tool_receipt` spends part of the granted budget on its
        // evidence URI and structured summary prefix. Reserving only the raw
        // body size made even a tiny exact `read_file` JSON lose its `content`
        // field to head-tail truncation. Keep bounded headroom for the receipt
        // envelope while preserving the existing per-tool hard ceiling.
        let requested = raw_tokens.saturating_add(96).min(per_tool_limit).max(1);
        let granted = self
            .turn_context_ledger
            .lock()
            .map(|mut ledger| ledger.reserve_tool_result(requested))
            .unwrap_or(requested);
        let mut receipt = crate::context_evidence::build_tool_receipt(
            tool_name,
            output,
            is_error,
            raw_ref.clone(),
            granted.max(24),
        );
        // Runtime collaboration commands already return a deliberately bounded model
        // receipt. Preserve a completed terminal summary as valid JSON so the
        // parent graph can consume it directly even on embedded/legacy hosts;
        // generic head-tail evidence compaction can otherwise split the JSON
        // and force an unnecessary parent model round.
        if !is_error
            && (tool_name.eq_ignore_ascii_case("runtime_orchestrate")
                || tool_name.eq_ignore_ascii_case(
                    harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
                ))
            && output.len() <= 24_000
            && serde_json::from_str::<serde_json::Value>(output)
                .ok()
                .is_some_and(|value| {
                    value.get("status").and_then(serde_json::Value::as_str) == Some("completed")
                        && value
                            .get("terminal_summary")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|summary| !summary.trim().is_empty())
                })
        {
            receipt.summary = output.to_string();
            receipt.receipt_tokens = raw_tokens;
            receipt.omitted_tokens = 0;
            receipt.truncated = false;
        }
        if access.is_none() {
            if receipt.summary.starts_with("Tool `") {
                receipt.summary = receipt.summary.replacen(
                    "Evidence: tool://",
                    "Ephemeral evidence (active runtime only): tool://",
                    1,
                );
            }
            receipt.receipt_tokens = crate::context_ledger::estimate_text_tokens(&receipt.summary);
            receipt.omitted_tokens = raw_tokens.saturating_sub(receipt.receipt_tokens);
            receipt.truncated = receipt.receipt_tokens < raw_tokens;
        }
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolResult,
            receipt.receipt_tokens,
            access.map(|_| format!("tool://{}", raw_ref.id())),
            self.session_head_blocking().message_count,
        );
        receipt
    }

    #[allow(dead_code)]
    pub(super) fn start_tool_invocation_record(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
    ) -> ToolInvocationRecord {
        let session_id = self.session_id().to_string();
        let safety_category = serde_json::from_str::<serde_json::Value>(input)
            .ok()
            .and_then(|input| self.tool_executor.registered_tool_effect(tool_name, &input))
            .map_or(crate::ToolSafetyCategory::Destructive, |effect| {
                crate::ToolSafetyCategory::from_effect(&effect)
            });
        ToolInvocationRecord::started(
            session_id,
            iterations,
            tool_use_id.to_string(),
            tool_name.to_string(),
            input,
            safety_category,
            now_ms(),
        )
    }

    pub(super) fn record_tool_invocation_denied(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
        failure_kind: ToolFailureKind,
        reason: &str,
    ) {
        let record = self
            .start_tool_invocation_record(tool_use_id, tool_name, input, iterations)
            .failed(failure_kind, reason, now_ms());
        self.record_tool_invocation_event(
            &record,
            "tool.invocation.denied",
            self.session_head_blocking().message_count,
        );
    }

    pub(super) fn record_tool_invocation_event(
        &self,
        record: &ToolInvocationRecord,
        kind: &'static str,
        _sequence: usize,
    ) {
        let mut refs = vec![
            RuntimeEventRef {
                kind: "tool_invocation".to_string(),
                id: record.invocation_id.clone(),
            },
            RuntimeEventRef {
                kind: "tool_call".to_string(),
                id: record.tool_call_id.clone(),
            },
        ];
        if let Some(plan_id) = &record.governed_plan_id {
            refs.push(RuntimeEventRef {
                kind: "governed_tool_plan".to_string(),
                id: plan_id.clone(),
            });
        }
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            kind,
            Some(record.status.as_str().to_string()),
            refs,
            serde_json::to_value(record).unwrap_or_else(
                |error| serde_json::json!({ "serialization_error": error.to_string() }),
            ),
        );
    }

    pub(super) fn record_governed_tool_plan(&self, plan: &GovernedToolPlan, _sequence: usize) {
        if let Ok(mut plans) = self.turn_governed_tool_plans.lock() {
            plans.push(plan.projection());
        }
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.execution_plan.created",
            Some("planned".to_string()),
            vec![RuntimeEventRef {
                kind: "governed_tool_plan".to_string(),
                id: plan.plan_id.clone(),
            }],
            serde_json::to_value(plan).unwrap_or_else(
                |error| serde_json::json!({ "serialization_error": error.to_string() }),
            ),
        );
    }

    pub(super) fn record_tool_strategy_validation(
        &self,
        report: &GovernedToolPolicyValidationReport,
        _sequence: usize,
    ) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.strategy_validation.completed",
            Some(if report.allowed { "allowed" } else { "denied" }.to_string()),
            vec![RuntimeEventRef {
                kind: "strategy_lease".to_string(),
                id: report.lease_id.clone(),
            }],
            serde_json::to_value(report).unwrap_or_else(|_| {
                serde_json::json!({
                    "allowed": false,
                    "findings": ["strategy_validation_serialization_failed"],
                    "lease_id": report.lease_id,
                })
            }),
        );
    }

    pub(super) fn record_tool_schedule(
        &self,
        plan: &GovernedToolPlan,
        requests: &[crate::tool_dispatch::ToolRequest],
        _sequence: usize,
    ) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Schedule,
            "tool.schedule.created",
            Some("planned".to_string()),
            requests
                .iter()
                .map(|request| RuntimeEventRef {
                    kind: "tool_call".to_string(),
                    id: request.tool_use_id.clone(),
                })
                .collect(),
            serde_json::json!({
                "plan_id": plan.plan_id,
                "plan_revision": plan.revision,
                "topology_hash": plan.topology_hash,
                "topological_order": plan.topological_order,
                "task_count": plan.task_count,
                "tool_count": requests.len(),
            }),
        );
    }

    pub(super) fn record_ai_kernel_trace_event(
        &self,
        trace: &RuntimeAiKernelTrace,
        sequence: usize,
    ) {
        if self.runtime_event_store.is_none() {
            return;
        }
        let payload = serde_json::json!({
            "strategy": {
                "pattern": trace.execution_decision.strategy.pattern.as_str(),
                "confidence": trace.execution_decision.strategy.confidence,
                "policy_version": trace.execution_decision.strategy.policy_version,
                "reasons": trace.execution_decision.strategy.reasons,
                "required_capabilities": trace.execution_decision.strategy.required_capabilities.iter().map(|item| format!("{item:?}")).collect::<Vec<_>>(),
                "complexity": format!("{:?}", trace.execution_decision.strategy.understanding.complexity),
                "risk": format!("{:?}", trace.execution_decision.strategy.understanding.risk),
                "modifiers": trace.execution_decision.strategy.modifiers.iter().map(|item| item.as_str()).collect::<Vec<_>>(),
            },
            "collaboration": {
                "template_id": trace.collaboration_decision.template_id.as_str(),
                "rationale": trace.collaboration_decision.rationale,
            },
            "context": {
                "epoch_id": trace.context_epoch.epoch_id,
                "envelope_id": trace.context_envelope_id,
                "token_total": trace.context_epoch.token_total,
                "selected_count": trace.context_epoch.selected.len(),
                "omitted_count": trace.context_epoch.omitted.len(),
                "alignment": trace.context_alignment,
            },
            "verification": {
                "can_finalize": trace.verification_report.can_finalize,
                "verification_blocked": trace.verification_blocked,
                "severity": format!("{:?}", trace.verification_report.severity),
                "blocking_reasons": trace.verification_report.blocking_reasons,
                "claim_count": trace.verification_report.claim_count,
                "evidence_count": trace.verification_report.evidence_count,
                "unsupported_required_count": trace.verification_report.unsupported_required_claims.len(),
                "not_run_count": trace.verification_report.not_run_claims.len(),
                "matrix_missing_evidence": matrix_missing_evidence(trace),
            },
            "governed_tool_plans": trace.governed_tool_plans.iter().map(|plan| serde_json::json!({
                "id": plan.plan_id,
                "revision": plan.revision,
                "catalog_revision": plan.catalog_revision,
                "invocation_count": plan.invocations.len(),
                "dependency_count": plan.dependencies.len(),
            })).collect::<Vec<_>>(),
            "harness": {
                "receipt_id": trace.harness_receipt.id,
                "harness_id": trace.harness_receipt.harness_id,
                "agent_spec_id": trace.harness_receipt.agent_spec_id,
                "strategy_pattern": trace.harness_receipt.strategy_pattern,
                "context_epoch_id": trace.harness_receipt.context_epoch_id,
                "governed_tool_plan_ids": trace.harness_receipt.governed_tool_plan_ids,
                "verification_can_finalize": trace.harness_receipt.verification_can_finalize,
                "policy_receipts": trace.harness_receipt.policy_receipts,
                "output_summary": trace.harness_receipt.output_summary,
            },
            "policy_receipts": trace.policy_receipts.iter().map(|receipt| serde_json::json!({
                "id": receipt.id,
                "scope": format!("{:?}", receipt.scope),
                "decision": format!("{:?}", receipt.decision),
                "reasons": receipt.reasons,
                "evidence_refs": receipt.evidence_refs,
                "source_policy": receipt.source_policy,
                "created_at": receipt.created_at,
            })).collect::<Vec<_>>(),
            "behavior_policy": {
                "necessity": trace.behavior_policy.necessity,
                "reuse_opportunities": trace.behavior_policy.reuse_opportunities,
                "overengineering_risks": trace.behavior_policy.overengineering_risks,
                "safety_exceptions": trace.behavior_policy.safety_exceptions,
                "recommended_scope": format!("{:?}", trace.behavior_policy.recommended_scope),
                "enforcement": {
                    "allow_execution": trace.behavior_policy.enforcement.allow_execution,
                    "requires_scope_downgrade": trace.behavior_policy.enforcement.requires_scope_downgrade,
                    "requires_human_review": trace.behavior_policy.enforcement.requires_human_review,
                },
                "eval_checks": trace.behavior_policy.eval_checks,
            },
            "execution_graph": trace.execution_graph.as_ref().map(|graph| serde_json::json!({
                "id": graph.id,
                "node_count": graph.nodes.len(),
                "edge_count": graph.edges.len(),
            })),
            "execution_graph_quality": trace.execution_graph_quality.as_ref().map(|quality| serde_json::json!({
                "node_count": quality.node_count,
                "edge_count": quality.edge_count,
                "ready_count": quality.ready_count,
                "blocked_count": quality.blocked_count,
                "failed_count": quality.failed_count,
                "has_verify_node": quality.has_verify_node,
                "has_synthesize_node": quality.has_synthesize_node,
                "is_dag": quality.is_dag,
                "warnings": quality.warnings,
            })),
            "bench": {
                "passed": trace.bench_result.passed,
                "score": trace.bench_result.score,
                "case_id": trace.bench_result.case_id,
                "reasons": trace.bench_result.reasons,
            },
            "regression_gate": {
                "allowed": trace.regression_gate.allowed,
                "average_score": trace.regression_gate.average_score,
                "failed": trace.regression_gate.failed,
                "reasons": trace.regression_gate.reasons,
            },
            "growth": {
                "record_id": trace.learning_record.id,
                "event_id": trace.growth_event.id,
                "policy": trace.learning_record.policy,
                "has_blocker": trace.learning_record.has_blocker(),
                "signals": trace.learning_record.signals.iter().map(|signal| serde_json::json!({
                    "kind": format!("{:?}", signal.kind),
                    "severity": format!("{:?}", signal.severity),
                    "summary": signal.summary,
                })).collect::<Vec<_>>(),
                "next_strategy_hints": trace.learning_record.next_strategy_hints,
            },
            "growth_event_schema_version": 1,
            "growth_event": trace.growth_event,
            "strategy_experience": strategy_experience_projection(trace),
            "maintenance_candidates": growth_maintenance_candidates(trace),
            "matrix_evidence_signal": {
                "source": "ai_kernel_trace",
                "growth_event_id": trace.growth_event.id,
                "packet_contract": {
                    "problem_statement": "AI harness execution quality",
                    "trace_ref": format!("runtime:event:{sequence}"),
                    "strategy_pattern": trace.execution_decision.strategy.pattern.as_str(),
                    "verification_can_finalize": trace.verification_report.can_finalize,
                    "regression_allowed": trace.regression_gate.allowed,
                    "harness_receipt_id": trace.harness_receipt.id,
                },
                "evidence_refs": trace.growth_event.evidence_refs,
                "signals": trace.growth_event.matrix_signals,
                "missing_evidence": matrix_missing_evidence(trace),
            },
        });
        self.append_execution_runtime_event(
            RuntimeEventScope::Task,
            "runtime.harness_contract.trace",
            Some(if trace.verification_report.can_finalize {
                "completed".to_string()
            } else {
                "degraded".to_string()
            }),
            vec![RuntimeEventRef {
                kind: "harness_receipt".to_string(),
                id: trace.harness_receipt.id.clone(),
            }],
            payload,
        );
    }
}
