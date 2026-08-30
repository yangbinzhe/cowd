//! Governed tool context, synthesis, terminal evidence, and presentation policy.

use super::*;

impl crate::GovernedToolExecutionContext for HostGovernedToolContext<'_> {
    type Output = crate::RuntimeToolExecutionOutcome;
    type Admission = Option<crate::ToolExecutionAdmission>;
    type Receipt = crate::RuntimeToolExecutionOutcome;

    fn local_ceiling(&self) -> usize {
        crate::governed_tool_plan::default_parallel_tool_concurrency()
    }

    fn try_admit<'a>(
        &'a self,
        _task: &'a crate::GovernedToolPlanTask,
    ) -> crate::GovernedToolFuture<'a, crate::GovernedToolAdmission<Self::Admission>> {
        Box::pin(async { crate::GovernedToolAdmission::Granted(None) })
    }

    fn execute<'a>(
        &'a self,
        task: &'a crate::GovernedToolPlanTask,
        admission: &'a mut Self::Admission,
    ) -> crate::GovernedToolFuture<'a, Result<Self::Output, String>> {
        Box::pin(async move {
            let call = self.calls.get(task.original_call_index).ok_or_else(|| {
                format!(
                    "governed tool task `{}` references missing original call index {}",
                    task.tool_call_id, task.original_call_index
                )
            })?;
            if let Some(assessment) = self.capability_gaps.get(&call.id) {
                return Ok(capability_gap_outcome(
                    call,
                    task.safety_category,
                    assessment,
                ));
            }
            let host = Arc::clone(&self.host);
            let authorization = self.tool_authorizations.get(&call.id).cloned();
            let effect = self
                .prepared_invocations
                .get(&call.id)
                .map(|invocation| invocation.effect.clone());
            let commit_service = self.commit_service.clone();
            let request = bound_runtime_tool_request(
                call,
                task,
                self.plan_id,
                self.plan_revision,
                self.observation_wave_sequence,
                self.session_id,
                self.sandbox_posture,
                self.policy_revision,
                self.memory_context,
                self.model_lease,
                self.ticket,
                self.execution_decision,
                authorization,
                self.idempotency_keys
                    .and_then(|keys| keys.get(task.tool_call_id.as_str())),
            );
            let (execution, retained_admission) = self
                .execution_plane
                .execute_async_classified_retained(
                    &task.resource_demand,
                    Some(std::time::Duration::from_secs(
                        task.safety_category.default_timeout_secs(),
                    )),
                    self.ticket.service_class,
                    Some(self.ticket.service_class),
                    Some(self.session_id),
                    async move {
                        execute_fenced_runtime_tool(
                            host.as_ref(),
                            &commit_service,
                            &request,
                            effect.as_ref(),
                        )
                        .await
                    },
                )
                .await;
            *admission = retained_admission;
            execution.map_err(|error| error.to_string())
        })
    }

    fn classify_output(&self, output: &Self::Output) -> Result<(), String> {
        if output.status == crate::RuntimeToolExecutionStatus::Executed {
            Ok(())
        } else {
            Err(output.error.clone().unwrap_or_else(|| {
                format!("tool `{}` did not complete successfully", output.tool_name)
            }))
        }
    }

    fn precompleted(
        &self,
        task: &crate::GovernedToolPlanTask,
    ) -> Option<(crate::GovernedToolTaskTerminal<Self::Output>, Self::Receipt)> {
        let receipt = self.precompleted?.get(&task.tool_call_id)?;
        // The early dispatcher already emitted transient start/terminal events,
        // but its invocation map is intentionally task-local. Rehydrate the
        // terminal fact here so the finalized DAG can persist the same tool
        // lifecycle beside its durable message/result without re-executing it.
        let input = self
            .calls
            .get(task.original_call_index)
            .map_or("", |call| call.input.as_str());
        let summary = receipt
            .outcome
            .output
            .as_deref()
            .or(receipt.outcome.error.as_deref())
            .unwrap_or("early tool completed without output");
        let started = ToolInvocationRecord::started(
            self.session_id,
            0,
            task.tool_call_id.clone(),
            task.tool_name.clone(),
            input,
            task.safety_category,
            receipt.started_at_ms,
        )
        .with_governed_plan(self.plan_id, self.plan_revision);
        let record = match receipt.outcome.status {
            crate::RuntimeToolExecutionStatus::Executed => {
                started.completed(summary, receipt.completed_at_ms)
            }
            crate::RuntimeToolExecutionStatus::BlockedPermission => started.failed(
                ToolFailureKind::PermissionDenied,
                summary,
                receipt.completed_at_ms,
            ),
            _ => started.failed(
                ToolFailureKind::ExecutionError,
                summary,
                receipt.completed_at_ms,
            ),
        };
        self.invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(task.tool_call_id.clone(), record);
        let terminal = if receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed {
            crate::GovernedToolTaskTerminal::Succeeded(receipt.outcome.clone())
        } else {
            crate::GovernedToolTaskTerminal::FailedOutput {
                output: receipt.outcome.clone(),
                error: receipt
                    .outcome
                    .error
                    .clone()
                    .unwrap_or_else(|| "early read did not complete successfully".to_string()),
            }
        };
        Some((terminal, receipt.outcome.clone()))
    }

    fn commit_terminal<'a>(
        &'a self,
        task: &'a crate::GovernedToolPlanTask,
        terminal: &'a crate::GovernedToolTaskTerminal<Self::Output>,
    ) -> crate::GovernedToolFuture<'a, Result<Self::Receipt, String>> {
        Box::pin(async move {
            let call = self.calls.get(task.original_call_index).ok_or_else(|| {
                format!(
                    "governed tool task `{}` references missing original call index {}",
                    task.tool_call_id, task.original_call_index
                )
            })?;
            let outcome = match terminal {
                crate::GovernedToolTaskTerminal::Succeeded(outcome)
                | crate::GovernedToolTaskTerminal::FailedOutput {
                    output: outcome, ..
                } => outcome.clone(),
                _ => failed_governed_tool_outcome(
                    call,
                    task.safety_category,
                    host_tool_terminal_reason(terminal),
                ),
            };
            if self
                .prepared_invocations
                .get(&call.id)
                .is_some_and(|invocation| {
                    invocation.effect.effect_kind == harness_contract::tool::ToolEffectKind::Read
                })
            {
                let request = bound_runtime_tool_request(
                    call,
                    task,
                    self.plan_id,
                    self.plan_revision,
                    self.observation_wave_sequence,
                    self.session_id,
                    self.sandbox_posture,
                    self.policy_revision,
                    self.memory_context,
                    self.model_lease,
                    self.ticket,
                    self.execution_decision,
                    self.tool_authorizations.get(&call.id).cloned(),
                    self.idempotency_keys
                        .and_then(|keys| keys.get(call.id.as_str())),
                );
                self.commit_service
                    .commit_readonly_tool_receipts(&[(request, outcome.clone())])
                    .map_err(|error| error.to_string())?;
            }
            Ok(outcome)
        })
    }

    fn on_task_started(&self, task: &crate::GovernedToolPlanTask) {
        let input = self
            .calls
            .get(task.original_call_index)
            .map_or("", |call| call.input.as_str());
        let record = ToolInvocationRecord::started(
            self.session_id,
            0,
            task.tool_call_id.clone(),
            task.tool_name.clone(),
            input,
            task.safety_category,
            crate::tool_invocation::now_ms(),
        )
        .with_governed_plan(self.plan_id, self.plan_revision);
        self.invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(task.tool_call_id.clone(), record);
        if let Some(bus) = &self.event_bus {
            bus.emit_tool_started_with_dependencies(
                &task.tool_call_id,
                &task.tool_name,
                &host_event_preview(input, 200),
                &task.depends_on,
            );
        }
    }

    fn on_task_terminal(
        &self,
        task: &crate::GovernedToolPlanTask,
        terminal: &crate::GovernedToolTaskTerminal<Self::Output>,
        receipt: Option<&Self::Receipt>,
    ) {
        let (summary, exit_code) = receipt.map_or_else(
            || (host_tool_terminal_reason(terminal), Some(1)),
            |outcome| {
                let failed = outcome.status != crate::RuntimeToolExecutionStatus::Executed;
                (
                    outcome
                        .output
                        .as_deref()
                        .or(outcome.error.as_deref())
                        .unwrap_or("tool completed without output")
                        .to_string(),
                    Some(i32::from(failed)),
                )
            },
        );
        let mut invocations = self
            .invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let started = invocations.remove(&task.tool_call_id).unwrap_or_else(|| {
            let input = self
                .calls
                .get(task.original_call_index)
                .map_or("", |call| call.input.as_str());
            ToolInvocationRecord::started(
                self.session_id,
                0,
                task.tool_call_id.clone(),
                task.tool_name.clone(),
                input,
                task.safety_category,
                crate::tool_invocation::now_ms(),
            )
            .with_governed_plan(self.plan_id, self.plan_revision)
        });
        let ended_at_ms = crate::tool_invocation::now_ms();
        let record = match receipt.map(|outcome| &outcome.status) {
            Some(crate::RuntimeToolExecutionStatus::Executed) => {
                started.completed(&summary, ended_at_ms)
            }
            Some(crate::RuntimeToolExecutionStatus::BlockedPermission) => {
                started.failed(ToolFailureKind::PermissionDenied, &summary, ended_at_ms)
            }
            _ => started.failed(
                host_tool_failure_kind(terminal, &summary),
                &summary,
                ended_at_ms,
            ),
        };
        invocations.insert(task.tool_call_id.clone(), record);
        drop(invocations);
        if let Some(bus) = &self.event_bus {
            bus.emit_tool_completed_with_dependencies(
                &task.tool_call_id,
                &task.tool_name,
                &host_event_preview(&summary, 500),
                exit_code,
                &task.depends_on,
            );
        }
    }
}

pub(super) async fn execute_governed_runtime_tool_batch(
    host: Arc<dyn crate::RuntimeExecutionHost>,
    event_bus: Option<crate::CowdEventBus>,
    calls: &[ModelToolCall],
    session_id: &str,
    sandbox_posture: harness_contract::policy::SandboxPosture,
    policy_revision: u64,
    memory_context: Option<&memory::MemoryTurnContext>,
    model_lease: Option<&str>,
    ticket: &NodeExecutionTicket,
    observation_wave_sequence: u64,
    tool_authorizations: &std::collections::HashMap<
        String,
        harness_contract::tool::ToolExecutionAuthorization,
    >,
    capability_gaps: &std::collections::HashMap<
        String,
        harness_contract::policy::CapabilityAssessment,
    >,
    prepared_invocations: &std::collections::HashMap<
        String,
        harness_contract::tool::GovernedToolInvocation,
    >,
    compilation: Result<crate::GovernedToolCompilation, crate::GovernedToolCompileError>,
    decision: &crate::execution_core::RuntimeExecutionDecision,
    execution_plane: &Arc<crate::ToolExecutionPlane>,
    commit_service: &crate::execution_core::graph::ExecutionCommitService,
    precompleted: &BTreeMap<String, crate::conversation::EarlyToolExecutionReceipt>,
) -> GovernedToolBatchResult {
    let compilation = match compilation {
        Ok(compilation) => compilation,
        Err(error) => {
            let reason = format!("governed tool DAG rejected before execution: {error}");
            return GovernedToolBatchResult {
                messages: calls
                    .iter()
                    .map(|call| {
                        tool_outcome_message(failed_governed_tool_outcome(
                            call,
                            crate::ToolSafetyCategory::Destructive,
                            reason.clone(),
                        ))
                    })
                    .collect(),
                invocations: rejected_tool_invocations(
                    calls,
                    crate::ToolSafetyCategory::Destructive,
                    None,
                    0,
                    &reason,
                ),
                observed_evidence: Vec::new(),
                max_concurrency_observed: 0,
                parallel_batches: 0,
            };
        }
    };
    let mut messages_by_call = std::collections::HashMap::new();
    let mut invocation_records = HashMap::new();
    for rejection in compilation.rejected {
        if let Some(call) = calls.iter().find(|call| call.id == rejection.tool_call_id) {
            let reason = format!(
                "governed tool node rejected before execution: {}",
                rejection.reason
            );
            messages_by_call.insert(
                call.id.clone(),
                tool_outcome_message(failed_governed_tool_outcome(
                    call,
                    crate::ToolSafetyCategory::Destructive,
                    reason.clone(),
                )),
            );
            invocation_records.extend(rejected_tool_invocations(
                std::slice::from_ref(call),
                crate::ToolSafetyCategory::Destructive,
                None,
                0,
                &reason,
            ));
        }
    }
    let Some(plan) = compilation.plan else {
        return GovernedToolBatchResult {
            messages: calls
                .iter()
                .filter_map(|call| messages_by_call.remove(&call.id))
                .collect(),
            invocations: invocation_records,
            observed_evidence: Vec::new(),
            max_concurrency_observed: 0,
            parallel_batches: 0,
        };
    };
    let validation = plan.validate_against_execution_decision(decision);
    if !validation.allowed {
        let reason = format!(
            "runtime strategy lease `{}` denied tool batch: {}",
            validation.lease_id,
            validation.findings.join(", ")
        );
        for task in &plan.tasks {
            let call = &calls[task.original_call_index];
            messages_by_call.insert(
                call.id.clone(),
                tool_outcome_message(failed_governed_tool_outcome(
                    call,
                    task.safety_category,
                    reason.clone(),
                )),
            );
            invocation_records.extend(rejected_tool_invocations(
                std::slice::from_ref(call),
                task.safety_category,
                Some(plan.plan_id.as_str()),
                plan.revision,
                &reason,
            ));
        }
        return GovernedToolBatchResult {
            messages: calls
                .iter()
                .filter_map(|call| messages_by_call.remove(&call.id))
                .collect(),
            invocations: invocation_records,
            observed_evidence: Vec::new(),
            max_concurrency_observed: 0,
            parallel_batches: 0,
        };
    }
    let invocations = Arc::new(Mutex::new(invocation_records));
    let context = HostGovernedToolContext {
        host,
        event_bus,
        calls,
        session_id,
        sandbox_posture,
        policy_revision,
        memory_context,
        model_lease,
        ticket,
        execution_decision: Some(decision),
        tool_authorizations,
        capability_gaps,
        prepared_invocations,
        plan_id: &plan.plan_id,
        plan_revision: plan.revision,
        observation_wave_sequence,
        execution_plane,
        commit_service,
        precompleted: Some(precompleted),
        idempotency_keys: None,
        invocations: Arc::clone(&invocations),
    };
    let report = crate::GovernedToolExecutor.execute(&plan, &context).await;
    let max_concurrency_observed = report.max_active;
    let parallel_batches = usize::from(report.max_active > 1);
    let mut observed_evidence = Vec::new();
    for (index, outcome) in report.outcomes.into_iter().enumerate() {
        let task = &plan.tasks[index];
        let call = &calls[task.original_call_index];
        let receipt = outcome.receipt.unwrap_or_else(|| {
            failed_governed_tool_outcome(
                call,
                task.safety_category,
                "tool reached terminal state without a durable receipt".to_string(),
            )
        });
        observed_evidence.extend(receipt.observed_evidence.iter().cloned());
        messages_by_call.insert(call.id.clone(), tool_outcome_message(receipt));
    }
    let messages = calls
        .iter()
        .map(|call| {
            messages_by_call.remove(&call.id).unwrap_or_else(|| {
                tool_outcome_message(failed_governed_tool_outcome(
                    call,
                    crate::ToolSafetyCategory::Destructive,
                    "tool reached terminal state without a governed outcome".to_string(),
                ))
            })
        })
        .collect();
    let invocations = Arc::try_unwrap(invocations)
        .map(Mutex::into_inner)
        .unwrap_or_else(|shared| {
            Ok(shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        })
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    GovernedToolBatchResult {
        messages,
        invocations,
        observed_evidence,
        max_concurrency_observed,
        parallel_batches,
    }
}

impl<C, T> TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    /// Produce the user-facing explanation for a partial/blocked terminal.
    /// Uses one bounded zero-tool provider request so the model can explain
    /// what happened, why it failed, and what to do next; falls back to the
    /// structured blocked answer whenever the provider step cannot complete.
    /// Generated at most once per turn and cached in TurnGraphState.
    async fn terminal_narrated_answer(
        &self,
        raw: &str,
        objective: &str,
        envelope: &harness_contract::outcome::DeliveryEnvelope,
        presentation_id: &str,
        attempt_id: &str,
    ) -> Result<(String, Option<String>, Vec<String>, String), String> {
        let language = crate::conversation::user_reply_language(objective);
        let (cached, protocol_recovery_exhausted) = {
            let state = self.state.lock().await;
            (
                state.terminal_failure_narration.clone(),
                state.provider_protocol_recovery_attempts > PROVIDER_PROTOCOL_RECOVERY_BUDGET,
            )
        };
        if let Some(cached) = cached {
            return Ok(match cached {
                TerminalFailureNarration::Local(answer) => {
                    (answer, None, Vec::new(), attempt_id.to_string())
                }
                TerminalFailureNarration::Provider { answer, attempt_id } => {
                    (answer, None, Vec::new(), attempt_id)
                }
            });
        }
        // The same provider already received its single governed protocol
        // recovery. Calling it once more merely to narrate that exhaustion
        // adds cost and reopens the protocol boundary with no path to success.
        if protocol_recovery_exhausted {
            return Err(user_facing_blocked_answer(raw, language));
        }
        let visible_facts = serde_json::to_string(envelope)
            .map_err(|error| format!("encode terminal delivery envelope: {error}"))?;
        // Keep the Runtime guard out of the `match` scrutinee. Rust extends a
        // scrutinee temporary through the match arms; taking the same mutex in
        // the error arm (to inspect cancellation) would otherwise deadlock
        // precisely when a delegated budget rejects the narrator before
        // Provider dispatch.
        let cancellation = {
            let runtime = self.runtime.lock().await;
            runtime.cancellation_token()
        };
        let narration = {
            let mut runtime = self.runtime.lock().await;
            runtime
                .synthesize_failure_explanation(
                    objective,
                    raw,
                    &visible_facts,
                    language,
                    presentation_id,
                    attempt_id,
                    &envelope.envelope_id,
                    envelope.revision,
                )
                .await
        };
        match narration {
            Ok((explanation, model, models_used, provider_attempt_id)) => {
                let mut state = self.state.lock().await;
                state.terminal_failure_narration = Some(TerminalFailureNarration::Provider {
                    answer: explanation.clone(),
                    attempt_id: provider_attempt_id.clone(),
                });
                Ok((explanation, model, models_used, provider_attempt_id))
            }
            Err(error) => {
                if cancellation.is_cancelled() {
                    return Err("terminal_presentation_cancelled".to_string());
                }
                tracing::warn!(
                    error = %error,
                    "failure explanation synthesis failed; falling back to structured blocked answer"
                );
                Err(user_facing_blocked_answer(raw, language))
            }
        }
    }

    /// Convert a verified Program evidence carrier into one coherent root
    /// answer. A deterministic quality gate may request one repair draft; raw
    /// Team bundles are never exposed as a successful fallback.
    async fn terminal_collaboration_answer(
        &self,
        carrier: &str,
        objective: &str,
        envelope: &harness_contract::outcome::DeliveryEnvelope,
        presentation_id: &str,
        attempt_id: &str,
    ) -> Result<(String, Option<String>, Vec<String>, String), String> {
        const HIERARCHICAL_PACKET_TARGET_CHARS: usize = 64_000;
        const HIERARCHICAL_PACKET_TRIGGER_CHARS: usize = 96_000;
        const MAX_HIERARCHICAL_LEVELS: usize = 4;
        let language = crate::conversation::user_reply_language(objective);
        let mut synthesis_carrier = carrier.to_string();
        let mut all_models_used = Vec::new();
        if carrier.chars().count() > HIERARCHICAL_PACKET_TRIGGER_CHARS {
            let mut layer_results = collaboration_carrier_results(carrier)
                .filter(|results| !results.is_empty())
                .ok_or_else(|| "collaboration evidence carrier has no Team results".to_string())?;
            for level in 1..=MAX_HIERARCHICAL_LEVELS {
                let partitions = partition_complete_collaboration_results(
                    layer_results,
                    HIERARCHICAL_PACKET_TARGET_CHARS,
                );
                if partitions.len() <= 1 {
                    synthesis_carrier = collaboration_synthesis_layer(
                        partitions.into_iter().next().unwrap_or_default(),
                        level,
                    );
                    break;
                }
                let mut next_layer = Vec::with_capacity(partitions.len());
                for (partition_index, partition) in partitions.into_iter().enumerate() {
                    let source = collaboration_synthesis_layer(partition, level);
                    let mut feedback = Vec::new();
                    let mut accepted = None;
                    for repair_attempt in 0..=1_u8 {
                        let narration = {
                            let mut runtime = self.runtime.lock().await;
                            runtime
                                .synthesize_collaboration_answer(
                                    objective,
                                    &source,
                                    language,
                                    &feedback,
                                    true,
                                    presentation_id,
                                    &format!(
                                        "{attempt_id}:layer:{level}:partition:{partition_index}:quality:{repair_attempt}"
                                    ),
                                    &envelope.envelope_id,
                                    envelope.revision,
                                )
                                .await
                        };
                        let (answer, _model, models_used, _provider_attempt_id) =
                            narration.map_err(|error| error.to_string())?;
                        for model in models_used {
                            if !all_models_used.contains(&model) {
                                all_models_used.push(model);
                            }
                        }
                        feedback = collaboration_intermediate_quality_findings(&answer, &source);
                        if feedback.is_empty() {
                            accepted = Some(answer);
                            break;
                        }
                    }
                    let answer = accepted.ok_or_else(|| {
                        format!(
                            "hierarchical collaboration synthesis level {level} partition {partition_index} failed quality gate: {}",
                            feedback.join("; ")
                        )
                    })?;
                    next_layer.push(format!(
                        "Evidence-preserving synthesis layer {level}, partition {}:\n{answer}",
                        partition_index + 1
                    ));
                }
                synthesis_carrier = collaboration_synthesis_layer(next_layer.clone(), level);
                if synthesis_carrier.chars().count() <= HIERARCHICAL_PACKET_TRIGGER_CHARS {
                    break;
                }
                layer_results = next_layer;
                if level == MAX_HIERARCHICAL_LEVELS {
                    return Err(
                        "hierarchical collaboration synthesis did not converge within four complete-result layers"
                            .to_string(),
                    );
                }
            }
        }
        let mut validation_feedback = Vec::new();
        let mut last_error = None;
        for repair_attempt in 0..=1_u8 {
            let narration = {
                let mut runtime = self.runtime.lock().await;
                runtime
                    .synthesize_collaboration_answer(
                        objective,
                        &synthesis_carrier,
                        language,
                        &validation_feedback,
                        false,
                        presentation_id,
                        &format!("{attempt_id}:quality:{repair_attempt}"),
                        &envelope.envelope_id,
                        envelope.revision,
                    )
                    .await
            };
            match narration {
                Ok((answer, model, models_used, provider_attempt_id)) => {
                    for attempted in models_used {
                        if !all_models_used.contains(&attempted) {
                            all_models_used.push(attempted);
                        }
                    }
                    validation_feedback = collaboration_answer_quality_findings(&answer, objective);
                    if validation_feedback.is_empty() {
                        return Ok((answer, model, all_models_used, provider_attempt_id));
                    }
                    if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                        bus.emit(CowdEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                                presentation_id: presentation_id.to_string(),
                                attempt_id: provider_attempt_id,
                                reason: "collaboration_answer_quality_gate_rejected".to_string(),
                            },
                        });
                    }
                    last_error = Some(format!(
                        "collaboration answer failed quality gate: {}",
                        validation_feedback.join("; ")
                    ));
                }
                Err(error) => {
                    if self
                        .runtime
                        .lock()
                        .await
                        .cancellation_token()
                        .is_cancelled()
                    {
                        return Err("terminal_presentation_cancelled".to_string());
                    }
                    last_error = Some(error.to_string());
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            "collaboration answer quality gate rejected every draft".to_string()
        }))
    }
}

#[async_trait]
impl<C, T> crate::execution_core::graph::executors::SynthesizeBackend
    for TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
            bus.emit(CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
                detail: Some("synthesizing terminal".to_string()),
            });
        }
        let pending_inputs = self
            .runtime
            .lock()
            .await
            .consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::BeforeFinalAnswer);
        if !pending_inputs.is_empty() {
            let discard_latest_assistant = {
                let mut state = self.state.lock().await;
                state.terminal_override = None;
                state.clean_terminal_synthesis_next = false;
                state.pending_focus_terminal_candidate = None;
                state.assistant_messages.pop().is_some()
            };
            if discard_latest_assistant {
                let mut runtime = self.runtime.lock().await;
                let message_count = runtime.session_head().await.message_count;
                if message_count > 0 {
                    runtime
                        .session_mut_async()
                        .await
                        .truncate_messages(message_count.saturating_sub(1));
                }
            }
            let next = dynamic_node(
                ticket,
                self.state.lock().await.iterations,
                "input-cursor-replan-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            );
            let evidence_refs = pending_inputs
                .iter()
                .map(|record| format!("session_input:{}", record.envelope.input_id))
                .collect::<Vec<_>>();
            let mut outcome = NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:terminal-superseded", ticket.graph_id)),
                ExecutionUsage::default(),
            ))
            .with_replan(ExecutionGraphReplan {
                nodes: vec![next.clone()],
                edges: dynamic_edges(&ticket.node_id, &[next]),
                reason:
                    "new durable Session input crossed the final-answer barrier; terminal candidate was superseded"
                        .to_string(),
            });
            let observation_identity = {
                let state = self.state.lock().await;
                runtime_observation_identity(&self.services, &state, ticket)
            };
            let mut observation = runtime_observation(
                observation_identity,
                RuntimeObservationKind::UserInput,
                "runtime.before_final_answer",
                u64::from(ticket.attempt),
                format!(
                    "{} newer Session input(s) superseded the terminal candidate",
                    pending_inputs.len()
                ),
                format!(
                    "terminal-input-cursor:{}",
                    sha256_digest(&evidence_refs.join("\n"))
                ),
                ObservationResultClass::Informational,
            );
            observation.evidence_refs = evidence_refs;
            outcome.domain_events.push(
                self.services
                    .goal_store()
                    .observation_event(
                        &observation,
                        format!("{}:terminal-input-observation", ticket.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?,
            );
            return Ok(outcome);
        }
        let projection = self
            .services
            .execution_supervisor()
            .projection(&ticket.graph_id)
            .await
            .map_err(|error| error.to_string())?;
        let (
            ingress,
            goal_id,
            terminal_override,
            objective,
            terminal_model,
            input_tokens,
            output_tokens,
            turn_transcript_start,
            session_id,
            turn_id,
            execution_role,
        ) = {
            let state = self.state.lock().await;
            (
                state.ingress.clone(),
                state.goal_id.clone(),
                state.terminal_override.clone(),
                state.content.clone(),
                state.model.clone(),
                state.input_tokens,
                state.output_tokens,
                state.turn_transcript_start,
                state.session_id.clone(),
                state.turn_id.clone(),
                state.execution_role,
            )
        };
        let mut completion = terminal_override
            .as_ref()
            .map(|(completion, _)| *completion)
            .unwrap_or(GoalCompletion::Satisfied);
        let mut envelope = projection.delivery_envelope.clone().unwrap_or_else(|| {
            terminal_delivery_envelope(
                &projection,
                &goal_id,
                completion,
                &objective,
                &ticket.node_id,
            )
        });
        if terminal_override.is_none() {
            completion = match envelope.delivery_status {
                harness_contract::outcome::DeliveryStatus::Satisfied => GoalCompletion::Satisfied,
                harness_contract::outcome::DeliveryStatus::Partial
                | harness_contract::outcome::DeliveryStatus::Denied
                | harness_contract::outcome::DeliveryStatus::Unavailable => GoalCompletion::Partial,
            };
        }
        let presentation_id = format!("presentation:{}:{}", ticket.graph_id, envelope.revision);
        let attempt_id = format!("{}:attempt:{}", presentation_id, ticket.attempt);
        let delegated_leaf = execution_role.is_delegated_leaf();
        let direct_answer = match terminal_override.as_ref() {
            Some((GoalCompletion::Satisfied, answer)) if !answer.trim().is_empty() => {
                Some(answer.clone())
            }
            None => committed_terminal_answer(&projection, &ticket.graph_id)
                .ok()
                .filter(|answer| !answer.starts_with("<synthesized_terminal")),
            _ => None,
        }
        .filter(|answer| {
            if delegated_leaf {
                !answer.trim().is_empty() && !answer.starts_with("<synthesized_terminal")
            } else {
                qualified_root_answer(answer, &envelope)
            }
        });
        let reusable_origin = projection
            .terminal_presentation
            .as_ref()
            .filter(|candidate| {
                candidate.envelope_id == envelope.envelope_id
                    && candidate.envelope_revision == envelope.revision
                    && candidate.answer_origin
                        == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
            })
            .map_or(
                harness_contract::outcome::AnswerOrigin::ModelDirect,
                |candidate| candidate.answer_origin,
            );
        let (
            final_answer,
            answer_origin,
            fallback_reason,
            narrator_model,
            attempted_models,
            committed_attempt_id,
        ) = if delegated_leaf {
            let answer = terminal_override
                .as_ref()
                .map(|(_, answer)| answer.clone())
                .or(direct_answer)
                .unwrap_or_default();
            (
                answer,
                harness_contract::outcome::AnswerOrigin::TerminalDelegate,
                None,
                terminal_model.clone(),
                Vec::new(),
                attempt_id.clone(),
            )
        } else if let Some(answer) = direct_answer {
            let visible_answer = visible_final_answer(&answer);
            if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                bus.emit_synthetic_text_item("terminal-presentation", &visible_answer);
                bus.emit(CowdEvent::TerminalDelivery {
                    delivery:
                        harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
                            presentation_id: presentation_id.clone(),
                            attempt_id: attempt_id.clone(),
                            envelope_id: envelope.envelope_id.clone(),
                            envelope_revision: envelope.revision,
                            objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
                        },
                });
                bus.emit(CowdEvent::TerminalDelivery {
                    delivery: harness_contract::live::TerminalDeliveryEvent::TextDelta {
                        presentation_id: presentation_id.clone(),
                        attempt_id: attempt_id.clone(),
                        byte_start: 0,
                        byte_end: u64::try_from(visible_answer.len()).unwrap_or(u64::MAX),
                        delta: visible_answer.clone(),
                    },
                });
            }
            (
                visible_answer,
                reusable_origin,
                None,
                None,
                Vec::new(),
                attempt_id.clone(),
            )
        } else {
            let raw = terminal_override
                .as_ref()
                .map(|(_, answer)| answer.as_str())
                .unwrap_or("Execution ended without a qualified root answer candidate.");
            let collaboration_carrier = is_collaboration_evidence_carrier(raw);
            let narrated = if collaboration_carrier {
                self.terminal_collaboration_answer(
                    raw,
                    &objective,
                    &envelope,
                    &presentation_id,
                    &attempt_id,
                )
                .await
            } else {
                self.terminal_narrated_answer(
                    raw,
                    &objective,
                    &envelope,
                    &presentation_id,
                    &attempt_id,
                )
                .await
            };
            match narrated {
                Ok((answer, model, models_used, provider_attempt_id)) => (
                    answer,
                    harness_contract::outcome::AnswerOrigin::TerminalNarrator,
                    None,
                    model,
                    models_used,
                    provider_attempt_id,
                ),
                Err(cancelled) if cancelled == "terminal_presentation_cancelled" => {
                    return Err(cancelled);
                }
                Err(fallback) => {
                    let fallback = if collaboration_carrier {
                        completion = GoalCompletion::Partial;
                        envelope.delivery_status =
                            harness_contract::outcome::DeliveryStatus::Partial;
                        envelope
                            .unresolved
                            .push(harness_contract::outcome::DeliveryUnresolved {
                                unresolved_id: format!(
                                    "terminal-presentation:{}",
                                    envelope.revision
                                ),
                                kind: "collaboration_answer_quality".to_string(),
                                summary: fallback,
                                source_execution_id: Some(ticket.graph_id.clone()),
                                obligation_id: None,
                            });
                        "协作执行及完整证据均已保留，但根综合答案未通过完整性与清晰度质量门，框架已按部分完成关闭，未将原始 Team 证据包冒充最终答案。".to_string()
                    } else {
                        fallback
                    };
                    let fallback_attempt_id = format!("{attempt_id}:fallback");
                    if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                        bus.emit(CowdEvent::TerminalDelivery {
                                delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
                                    presentation_id: presentation_id.clone(),
                                    attempt_id: fallback_attempt_id.clone(),
                                    envelope_id: envelope.envelope_id.clone(),
                                    envelope_revision: envelope.revision,
                                    objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
                                },
                            });
                        bus.emit(CowdEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TextDelta {
                                presentation_id: presentation_id.clone(),
                                attempt_id: fallback_attempt_id.clone(),
                                byte_start: 0,
                                byte_end: u64::try_from(fallback.len()).unwrap_or(u64::MAX),
                                delta: fallback.clone(),
                            },
                        });
                    }
                    (
                        fallback,
                        harness_contract::outcome::AnswerOrigin::ProgrammaticFallback,
                        Some("all configured terminal narrator candidates failed".to_string()),
                        None,
                        Vec::new(),
                        fallback_attempt_id,
                    )
                }
            }
        };
        if self
            .runtime
            .lock()
            .await
            .cancellation_token()
            .is_cancelled()
        {
            if !delegated_leaf {
                if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                    bus.emit(CowdEvent::TerminalDelivery {
                    delivery:
                        harness_contract::live::TerminalDeliveryEvent::TerminalPresentationAborted {
                            presentation_id: presentation_id.clone(),
                            attempt_id: committed_attempt_id,
                            reason: "user_cancelled".to_string(),
                        },
                });
                }
            }
            return Err("terminal_presentation_cancelled".to_string());
        }
        let late_inputs = self
            .runtime
            .lock()
            .await
            .consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::BeforeFinalAnswer);
        if !late_inputs.is_empty() {
            if !delegated_leaf {
                if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                    bus.emit(CowdEvent::TerminalDelivery {
                    delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                        presentation_id: presentation_id.clone(),
                        attempt_id: committed_attempt_id,
                        reason: "new_durable_session_input".to_string(),
                    },
                });
                }
            }
            {
                let mut state = self.state.lock().await;
                state.terminal_override = None;
                state.terminal_failure_narration = None;
                state.clean_terminal_synthesis_next = false;
            }
            let next = dynamic_node(
                ticket,
                self.state.lock().await.iterations,
                "post-narrator-input-replan-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            );
            return Ok(NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:terminal-superseded-after-narration", ticket.graph_id)),
                ExecutionUsage::default(),
            ))
            .with_replan(ExecutionGraphReplan {
                nodes: vec![next.clone()],
                edges: dynamic_edges(&ticket.node_id, &[next]),
                reason: format!(
                    "{} newer durable Session input(s) superseded the terminal presentation before commit",
                    late_inputs.len()
                ),
            }));
        }
        let now_ms = crate::tool_invocation::now_ms();
        let presentation = execution_role.owns_root_presentation().then(|| {
            harness_contract::outcome::TerminalPresentation {
                presentation_id,
                attempt_id: committed_attempt_id,
                envelope_id: envelope.envelope_id.clone(),
                envelope_revision: envelope.revision,
                state: harness_contract::outcome::TerminalPresentationState::Committed,
                answer_origin,
                source_execution_id: Some(ticket.graph_id.clone()),
                narrator_model: if answer_origin
                    == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
                {
                    projection
                        .terminal_presentation
                        .as_ref()
                        .and_then(|candidate| candidate.narrator_model.clone())
                } else if answer_origin == harness_contract::outcome::AnswerOrigin::TerminalNarrator
                {
                    narrator_model.or(terminal_model)
                } else {
                    None
                },
                narrator_provider: projection
                    .terminal_presentation
                    .as_ref()
                    .filter(|_| {
                        answer_origin == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
                    })
                    .and_then(|candidate| candidate.narrator_provider.clone()),
                models_attempted: if answer_origin
                    == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
                {
                    projection
                        .terminal_presentation
                        .as_ref()
                        .map(|candidate| candidate.models_attempted.clone())
                        .unwrap_or_default()
                } else {
                    attempted_models
                        .into_iter()
                        .map(
                            |model| harness_contract::outcome::PresentationModelAttempt {
                                provider: "configured".to_string(),
                                model,
                                failure: None,
                            },
                        )
                        .collect()
                },
                validation: harness_contract::outcome::AnswerValidation {
                    status: if answer_origin
                        == harness_contract::outcome::AnswerOrigin::ProgrammaticFallback
                    {
                        harness_contract::outcome::AnswerValidationStatus::Invalid
                    } else {
                        harness_contract::outcome::AnswerValidationStatus::Valid
                    },
                    findings: fallback_reason.clone().into_iter().collect(),
                    envelope_revision: Some(envelope.revision),
                },
                fallback_reason,
                generated_at_ms: now_ms,
                committed_at_ms: Some(now_ms),
            }
        });
        {
            let mut state = self.state.lock().await;
            state.delivery_envelope = Some(envelope.clone());
            state.terminal_presentation = presentation.clone();
            state.terminal_commit_owner = Some((ticket.node_id.clone(), ticket.attempt));
            state.committed_terminal_answer = Some(final_answer.clone());
            state.committed_terminal_completion = Some(completion);
        }
        let digest = format!("{:x}", Sha256::digest(final_answer.as_bytes()));
        let mut outcome = NodeExecutionOutcome::new(completed_result(
            Some(format!("turn-result:{}:{digest}", ticket.graph_id)),
            ExecutionUsage::default(),
        ));
        outcome.delivery_envelope = Some(envelope.clone());
        outcome.terminal_presentation = presentation.clone();
        outcome.domain_events.push(
            self.services
                .goal_store()
                .terminal_event(
                    &goal_id,
                    completion,
                    vec![format!("execution_graph:{}", ticket.graph_id)],
                    "terminal_synthesis".to_string(),
                    format!("{}:goal-complete", ticket.idempotency_key),
                )
                .map_err(|error| format!("goal completion cannot commit: {error}"))?,
        );
        let recovery_scope = format!("turn:{turn_id}");
        let controlled_recovery_claim_fingerprints = self
            .runtime
            .lock()
            .await
            .authorization_negotiator()
            .controlled_recovery_claims_for_scope(&recovery_scope);
        outcome.domain_events.push(
            crate::authorization_negotiator::controlled_recovery_terminal_event(
                &crate::authorization_negotiator::ControlledRecoveryTerminalRecord {
                    recovery_scope,
                    session_id: session_id.clone(),
                    turn_id: turn_id.clone(),
                    execution_id: ticket.graph_id.clone(),
                    fingerprints: controlled_recovery_claim_fingerprints.clone(),
                },
            )?,
        );
        self.state
            .lock()
            .await
            .pending_controlled_recovery_claim_fingerprints =
            controlled_recovery_claim_fingerprints.clone();
        if let Some(ingress) = ingress {
            let presentation = presentation.as_ref().ok_or_else(|| {
                "root Session ingress terminal is missing its presentation".to_string()
            })?;
            let (terminal_fence, consumed_input_sequence) = {
                let runtime = self.runtime.lock().await;
                let terminal_fence = runtime
                    .capture_session_execution_fence(
                        crate::SessionExecutionFencePhase::TerminalCommit,
                    )
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| {
                        "Session terminal requires a durable execution fence snapshot".to_string()
                    })?;
                let consumed_input_sequence = runtime
                    .consumed_session_input_cursor()
                    .filter(|cursor| cursor.generation == ingress.session_generation)
                    .map_or(ingress.input_sequence, |cursor| cursor.sequence)
                    .max(ingress.input_sequence);
                (terminal_fence, consumed_input_sequence)
            };
            let mut transcript = {
                let runtime = self.runtime.lock().await;
                let session = runtime.session_snapshot().await;
                session
                    .messages_page(
                        turn_transcript_start,
                        session
                            .message_count()
                            .saturating_sub(turn_transcript_start),
                    )
                    .materialize()
            };
            // The source ingress row and its Runtime request are committed in
            // one Gateway transaction before execution begins. Persisting it
            // again here would create a duplicate user turn.
            if transcript
                .first()
                .is_some_and(|message| message.role.role_str() == "user")
            {
                transcript.remove(0);
            }
            let terminal_is_last = transcript.last().is_some_and(|message| {
                message.role.role_str() == "assistant"
                    && message.blocks.iter().any(
                        |block| matches!(block, ContentBlock::Text { text } if text == &final_answer),
                    )
            });
            if !terminal_is_last {
                transcript.push(ConversationMessage::assistant(vec![ContentBlock::Text {
                    text: final_answer.clone(),
                }]));
            }
            let transcript = transcript
                .iter()
                .map(|message| {
                    let persisted = message
                        .to_persisted_json()
                        .map_err(|error| format!("seal terminal Provider transcript: {error}"))?;
                    serde_json::from_str::<serde_json::Value>(&persisted.render())
                        .map_err(|error| format!("encode terminal transcript: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (terminal_input_tokens, terminal_output_tokens) = terminal_aggregate_usage(
                input_tokens,
                output_tokens,
                self.services.execution_live(&ticket.graph_id).as_ref(),
            );
            let terminal_id = format!("turn-terminal:{}", ingress.request_id);
            let collaboration_evidence = terminal_override
                .as_ref()
                .map(|(_, value)| value.as_str())
                .filter(|value| is_collaboration_evidence_carrier(value));
            let terminal_payload = serde_json::to_vec(&serde_json::json!({
                "schema_version": crate::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION,
                "text": final_answer,
                // The user-facing synthesis and its complete source carrier
                // are committed atomically. Presentation limits can therefore
                // never erase Team semantics or make the concise answer the
                // only surviving copy of a complex Program result.
                "collaboration_evidence": collaboration_evidence,
                "delivery_envelope": envelope.clone(),
                "terminal_presentation": presentation.clone(),
                "answer_origin": presentation.answer_origin,
                "envelope_id": presentation.envelope_id,
                "envelope_revision": presentation.envelope_revision,
                "narrator_provider": presentation.narrator_provider,
                "narrator_model": presentation.narrator_model,
                "fallback_reason": presentation.fallback_reason,
                "goal_completion": completion,
                "ingress_message_id": ingress.message_id,
                "consumed_input_sequence": consumed_input_sequence,
                "transcript": transcript,
                "token_usage": {
                    "input_tokens": terminal_input_tokens,
                    "output_tokens": terminal_output_tokens,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                }
            }))
            .map_err(|error| format!("encode terminal artifact: {error}"))?;
            let terminal_artifact = self
                .services
                .artifact_store()
                .write_bytes(
                    harness_contract::context::ArtifactWriteDescriptor {
                        media_type: "application/vnd.cowd.session-terminal+json".to_string(),
                        visibility_scope: format!("session:{}", ingress.session_id),
                        expected_bytes: Some(terminal_payload.len() as u64),
                        original_name: Some(format!("{}.json", terminal_id.replace(':', "-"))),
                    },
                    &terminal_payload,
                )
                .await
                .map_err(|error| format!("persist terminal artifact: {error}"))?;
            let staging_owner = format!("staging:{terminal_id}");
            self.services
                .artifact_store()
                .pin(
                    &terminal_artifact,
                    &staging_owner,
                    crate::tool_invocation_now_ms()
                        .saturating_add(crate::ARTIFACT_STAGING_PIN_TTL_MS),
                )
                .map_err(|error| format!("pin terminal artifact: {error}"))?;
            let payload_ref = crate::encode_session_terminal_artifact_ref(&terminal_artifact)?;
            self.state.lock().await.pending_terminal_artifact = Some(PendingTerminalArtifact {
                artifact: terminal_artifact,
                staging_owner,
                durable_owner: terminal_id.clone(),
            });
            let terminal = crate::runtime_event_store::SessionTerminalInput {
                terminal_id,
                message_id: format!("assistant:{}", ingress.message_id),
                session_id: ingress.session_id.clone(),
                execution_id: Some(ticket.graph_id.clone()),
                turn_id: Some(ingress.turn_id.clone()),
                request_id: Some(terminal_fence.request_id),
                session_generation: Some(terminal_fence.session_generation),
                input_sequence: Some(ingress.input_sequence),
                input_claim_owner: Some(terminal_fence.claim_owner),
                input_claim_token: Some(terminal_fence.claim_token),
                // Runtime terminal storage predates the immutable fence epoch
                // name. Its `input_claim_revision` column carries that epoch,
                // never the renewable outbox row revision.
                input_claim_revision: Some(terminal_fence.claim_fence_epoch),
                controlled_recovery_claim_fingerprints,
                payload_ref,
            };
            outcome
                .domain_events
                .push(crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("session-terminal:{}", ingress.request_id),
                        scope: crate::RuntimeEventScope::SessionInput,
                        kind: "runtime.session.terminal_requested".to_string(),
                        status: Some("pending_delivery".to_string()),
                        actor: Some("SynthesizeNodeExecutor".to_string()),
                        refs: vec![
                            crate::RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: ticket.graph_id.clone(),
                            },
                            crate::RuntimeEventRef {
                                kind: "session".to_string(),
                                id: ingress.session_id.clone(),
                            },
                        ],
                        payload: serde_json::to_value(&terminal).unwrap_or_default(),
                    },
                    idempotency_key: Some(ticket.idempotency_key.clone()),
                    schema_version: 1,
                });
        }
        Ok(outcome)
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), String> {
        let owns_terminal_commit = {
            let state = self.state.lock().await;
            terminal_commit_owned_by(
                state.terminal_commit_owner.as_ref(),
                &ticket.node_id,
                ticket.attempt,
            )
        };
        if !owns_terminal_commit {
            // Synthesize also owns the final-input barrier. When new durable
            // input wins that barrier it commits a replan, not a terminal
            // presentation. Runner still invokes `after_commit` for the
            // committed node transition, so this hook must be a no-op.
            return Ok(());
        }
        let committed_recovery_claims = std::mem::take(
            &mut self
                .state
                .lock()
                .await
                .pending_controlled_recovery_claim_fingerprints,
        );
        if !committed_recovery_claims.is_empty() {
            let acknowledged = self
                .runtime
                .lock()
                .await
                .authorization_negotiator()
                .acknowledge_controlled_recovery_terminals(&committed_recovery_claims);
            if acknowledged != committed_recovery_claims.len() {
                tracing::warn!(
                    acknowledged,
                    expected = committed_recovery_claims.len(),
                    "durable turn terminal found missing controlled recovery hot claims"
                );
            }
        }
        let pending_terminal_artifact = self.state.lock().await.pending_terminal_artifact.take();
        if let Some(pending) = pending_terminal_artifact {
            if let Err(error) = self.services.artifact_store().pin(
                &pending.artifact,
                &pending.durable_owner,
                crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            ) {
                // The graph event is already committed. Preserve the staged
                // object permanently rather than allowing a referenced
                // terminal artifact to expire before reconciliation.
                let _ = self.services.artifact_store().pin(
                    &pending.artifact,
                    &pending.staging_owner,
                    crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                );
                return Err(format!("promote committed terminal artifact pin: {error}"));
            }
            if let Err(error) = self
                .services
                .artifact_store()
                .unpin(&pending.artifact, &pending.staging_owner)
            {
                tracing::warn!(
                    error = %error,
                    artifact = %pending.artifact.selector,
                    "committed terminal artifact retained an extra staging pin"
                );
            }
        }
        let (
            terminal_override,
            committed_terminal_answer,
            committed_terminal_completion,
            defer_post_turn_memory_maintenance,
        ) = {
            let state = self.state.lock().await;
            (
                state.terminal_override.clone(),
                state.committed_terminal_answer.clone(),
                state.committed_terminal_completion,
                state.ingress.is_some(),
            )
        };
        let terminal_completion = committed_terminal_completion.unwrap_or_else(|| {
            terminal_override
                .as_ref()
                .map(|(completion, _)| *completion)
                .unwrap_or(GoalCompletion::Satisfied)
        });
        let final_answer = committed_terminal_answer.ok_or_else(|| {
            "terminal presentation committed without a cached user-visible answer".to_string()
        })?;
        // `after_commit` has committed only the Runtime graph transition. The
        // Session transcript/outbox transaction has not committed yet, so it
        // must not publish a terminal-delivery committed fact. Gateway's
        // durable outbox bridge is the single authority for that event.
        let (
            content,
            assistant_messages,
            tool_results,
            iterations,
            model,
            models_used,
            first_token_latency_ms,
            active_stream_duration_ms,
            input_tokens,
            output_tokens,
            wall_duration_ms,
            duplicate_tool_calls,
            write_attempt_paths,
            max_tool_concurrency_observed,
            parallel_tool_batches,
        ) = {
            let mut state = self.state.lock().await;
            (
                state.content.clone(),
                std::mem::take(&mut state.assistant_messages),
                std::mem::take(&mut state.tool_results),
                state.iterations,
                state.model.clone(),
                state.models_used.clone(),
                state.first_token_latency_ms,
                state.active_stream_duration_ms,
                state.input_tokens,
                state.output_tokens,
                state.wall_duration_ms,
                state.duplicate_tool_calls,
                std::mem::take(&mut state.write_attempt_paths),
                state.max_tool_concurrency_observed,
                state.parallel_tool_batches,
            )
        };
        let summary = self
            .runtime
            .lock()
            .await
            .finalize_graph_turn(
                &content,
                final_answer,
                assistant_messages,
                tool_results,
                iterations,
                model,
                models_used,
                first_token_latency_ms,
                active_stream_duration_ms,
                input_tokens,
                output_tokens,
                wall_duration_ms,
                duplicate_tool_calls,
                write_attempt_paths,
                max_tool_concurrency_observed,
                parallel_tool_batches,
                terminal_completion,
                defer_post_turn_memory_maintenance,
            )
            .await
            .map_err(|error| error.to_string())?;
        {
            let mut state = self.state.lock().await;
            state.summary = Some(summary);
            state.terminal_commit_owner = None;
            state.committed_terminal_answer = None;
            state.committed_terminal_completion = None;
        }
        Ok(())
    }

    async fn after_abort(&self, ticket: &NodeExecutionTicket, reason: &str) -> Result<(), String> {
        let presentation = {
            let mut state = self.state.lock().await;
            let owns_terminal_commit = terminal_commit_owned_by(
                state.terminal_commit_owner.as_ref(),
                &ticket.node_id,
                ticket.attempt,
            );
            let presentation = if owns_terminal_commit {
                state.terminal_presentation.take()
            } else {
                None
            };
            if owns_terminal_commit {
                state.terminal_commit_owner = None;
                state.committed_terminal_answer = None;
                state.committed_terminal_completion = None;
                // A streamed narrator attempt that did not reach the graph
                // commit boundary is no longer reusable. Do not let an abort
                // from a sibling Synthesize node erase this owner's cache.
                state.terminal_failure_narration = None;
            }
            presentation
        };
        if let (Some(bus), Some(presentation)) =
            (self.runtime.lock().await.cowd_bus().cloned(), presentation)
        {
            bus.emit(CowdEvent::TerminalDelivery {
                delivery:
                    harness_contract::live::TerminalDeliveryEvent::TerminalPresentationAborted {
                        presentation_id: presentation.presentation_id,
                        attempt_id: presentation.attempt_id,
                        reason: reason.to_string(),
                    },
            });
        }
        Ok(())
    }
}

pub(super) fn terminal_commit_owned_by(
    owner: Option<&(String, u32)>,
    node_id: &str,
    attempt: u32,
) -> bool {
    owner.is_some_and(|(owner_node_id, owner_attempt)| {
        owner_node_id == node_id && *owner_attempt == attempt
    })
}

pub(super) fn user_facing_blocked_answer(raw: &str, language: &str) -> String {
    let normalized = raw.to_ascii_lowercase();
    let zh = language.eq_ignore_ascii_case("zh");
    let (category, hint, preserved, reason_label) = if normalized
        .contains("explicit team acceptance is incomplete")
    {
        if zh {
            (
                "团队协作未完成",
                "部分团队没有产出可验收结果，已保留的证据仍然有效。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Team collaboration incomplete",
                "Some team roles did not produce acceptable results; preserved evidence remains valid.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if normalized.contains("execution blocked safely")
        || normalized.contains("safety fuse")
        || normalized.contains("safety-fuse")
    {
        if zh {
            (
                "安全上限",
                "已达到安全步骤上限，未执行超出边界的操作。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Safety limit reached",
                "The safety step limit was reached; no out-of-bound operations were executed.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if normalized.contains("provider")
        && (normalized.contains("repeated") || normalized.contains("failed"))
    {
        if zh {
            (
                "模型服务异常",
                "模型服务多次失败或不可用，已保留已完成的内容和证据。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Model service issue",
                "The model service failed repeatedly or is unavailable; completed content and evidence were retained.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if normalized.contains("approval") {
        if zh {
            (
                "审批未通过",
                "有操作未获得授权，因此未执行；无冲突的其它工作可以继续，也可以重新授权后继续。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Approval not granted",
                "An operation was not authorized, so it was not executed; other non-conflicting work may continue, or you may re-authorize and continue.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if zh {
        (
            "任务未完成",
            "任务没有达到预期终态，但已保留已完成的工作和证据。",
            "已保留的已完成工作和证据，详见下方结果树/活动详情。",
            "失败原因",
        )
    } else {
        (
            "Task incomplete",
            "The task did not reach the intended terminal state, but completed work and evidence were retained.",
            "Completed work and evidence were retained; see the result tree / activity detail below.",
            "Failure reason",
        )
    };
    let mut markdown = if zh {
        format!("**{category}**：{hint}\n\n{preserved}")
    } else {
        format!("**{category}**: {hint}\n\n{preserved}")
    };
    let reason = raw.trim();
    if !reason.is_empty() {
        let capped = if reason.chars().count() > 1500 {
            let tail: String = reason.chars().take(1500).collect();
            if zh {
                format!("{tail}…（已截断，完整原因见活动详情）")
            } else {
                format!("{tail}… (truncated; full reason in activity detail)")
            }
        } else {
            reason.to_string()
        };
        markdown.push_str(&format!("\n\n**{reason_label}**\n\n```text\n"));
        markdown.push_str(&capped);
        markdown.push_str("\n```\n");
    }
    markdown
}

/// Convert a structured JSON contract answer into user-visible Markdown without
/// corrupting the raw `assistant_json:` result_ref consumed by validators.
pub(super) fn visible_final_answer(text: &str) -> String {
    let trimmed = text.trim();
    if let Ok(serde_json::Value::Object(object)) =
        serde_json::from_str::<serde_json::Value>(trimmed)
    {
        for field in ["answer", "summary", "final_answer", "content"] {
            if let Some(serde_json::Value::String(value)) = object.get(field) {
                if !value.trim().is_empty() {
                    return value.clone();
                }
            }
        }
        let mut markdown = String::from("已完成：\n");
        for (key, value) in object {
            if key == "unresolved" || key == "risks" {
                continue;
            }
            let rendered = match value {
                serde_json::Value::String(value) => value,
                other => other.to_string(),
            };
            markdown.push_str(&format!("- {key}: {rendered}\n"));
        }
        return markdown;
    }
    text.to_string()
}

pub(super) fn sha256_digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

pub(super) fn terminal_aggregate_usage(
    own_input_tokens: u64,
    own_output_tokens: u64,
    live: Option<&harness_contract::projection::ExecutionLiveState>,
) -> (u64, u64) {
    live.map_or((own_input_tokens, own_output_tokens), |live| {
        (
            own_input_tokens.max(live.metrics.input_tokens),
            own_output_tokens.max(live.metrics.output_tokens),
        )
    })
}

pub(super) fn runtime_observation_identity(
    services: &crate::RuntimeServices,
    state: &TurnGraphState,
    ticket: &NodeExecutionTicket,
) -> RuntimeObservationIdentity {
    RuntimeObservationIdentity {
        workspace_id: services.workspace_key().to_string(),
        session_id: state.session_id.clone(),
        turn_id: state
            .ingress
            .as_ref()
            .map(|ingress| ingress.turn_id.clone()),
        task_id: None,
        graph_id: ticket.graph_id.clone(),
        goal_id: state.goal_id.clone(),
        node_id: Some(ticket.node_id.clone()),
    }
}

pub(super) fn observation_freshness(observed_at_ms: u64) -> ObservationFreshness {
    ObservationFreshness {
        observed_at_ms,
        valid_until_ms: None,
        policy_revision: "goal-observation-v2".to_string(),
    }
}

pub(super) fn runtime_observation(
    identity: RuntimeObservationIdentity,
    kind: RuntimeObservationKind,
    source: &str,
    source_revision: u64,
    summary: String,
    fingerprint: String,
    result_class: ObservationResultClass,
) -> RuntimeObservation {
    RuntimeObservation {
        identity,
        kind,
        source: source.to_string(),
        source_revision: source_revision.max(1),
        freshness: observation_freshness(crate::tool_invocation::now_ms()),
        summary,
        fingerprint,
        evidence_refs: Vec::new(),
        observed_evidence: Vec::new(),
        criterion_deltas: Vec::new(),
        evidence_delta: EvidenceDelta::default(),
        effect_deltas: Vec::new(),
        conflict_deltas: Vec::new(),
        unknown_deltas: Vec::new(),
        cost_delta: CostDelta::default(),
        information_gain: InformationGain::default(),
        context_delta: ContextDelta::default(),
        parallelism_delta: ParallelismDelta::default(),
        result_class,
        failure_class: None,
    }
}

pub(super) fn predecessor_goal_observations(
    graph: &harness_contract::execution_graph::ExecutionGraph,
    ticket: &NodeExecutionTicket,
    current_identity: &RuntimeObservationIdentity,
) -> Vec<RuntimeObservation> {
    graph
        .edges
        .iter()
        .filter(|edge| edge.to == ticket.node_id)
        .filter_map(|edge| {
            let node = graph.nodes.iter().find(|node| node.id == edge.from)?;
            if !matches!(
                node.kind,
                ExecutionNodeKind::Approval
                    | ExecutionNodeKind::AgentTask
                    | ExecutionNodeKind::Verify
            ) {
                return None;
            }
            let result = graph.node_results.get(&node.id)?;
            if !result.status.is_terminal() {
                return None;
            }
            let source = match node.kind {
                ExecutionNodeKind::Approval => "runtime.approval_result",
                ExecutionNodeKind::Verify => "runtime.verification_result",
                ExecutionNodeKind::AgentTask => {
                    if serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(
                        &node.payload_ref,
                    )
                    .ok()
                    .is_some_and(|packet| packet.team_id().is_some())
                    {
                        "runtime.team_agent_result"
                    } else {
                        "runtime.agent_result"
                    }
                }
                _ => return None,
            };
            let result_class = match result.status {
                ExecutionNodeStatus::Completed => ObservationResultClass::Succeeded,
                ExecutionNodeStatus::Blocked
                | ExecutionNodeStatus::Failed
                | ExecutionNodeStatus::Cancelled => ObservationResultClass::Failed,
                _ => ObservationResultClass::Informational,
            };
            let mut identity = current_identity.clone();
            identity.node_id = Some(node.id.clone());
            let mut observation = runtime_observation(
                identity,
                RuntimeObservationKind::GraphProgress,
                source,
                result.finished_at_ms.max(1),
                result
                    .summary
                    .clone()
                    .unwrap_or_else(|| format!("{:?} node {} completed", node.kind, node.id)),
                sha256_digest(&format!(
                    "{}:{:?}:{}",
                    node.id,
                    result.status,
                    result.result_ref.as_deref().unwrap_or_default()
                )),
                result_class,
            );
            let durable_result_ref = result
                .result_ref
                .as_ref()
                .map(|reference| format!("execution_result:{reference}"));
            let materialized_evidence = result
                .evidence_refs
                .iter()
                .map(|reference| reference.evidence_ref.id.clone())
                .filter(|reference| !reference.trim().is_empty())
                .collect::<BTreeSet<_>>();
            observation.evidence_refs = materialized_evidence
                .iter()
                .cloned()
                .chain(durable_result_ref.iter().cloned())
                .collect();
            if result.status == ExecutionNodeStatus::Completed {
                observation.evidence_delta.added = observation.evidence_refs.clone();
                observation.information_gain = InformationGain {
                    distinguishing_evidence_refs: materialized_evidence.into_iter().collect(),
                    resolved_unknown_refs: Vec::new(),
                    provenance: if result.evidence_refs.is_empty() {
                        MeasureProvenance::Unknown
                    } else {
                        MeasureProvenance::Observed
                    },
                };
            }
            observation.effect_deltas.push(EffectDelta {
                effect_id: format!("execution-node:{}", node.id),
                terminal_class: match result.status {
                    ExecutionNodeStatus::Completed => EffectTerminalClass::Completed,
                    ExecutionNodeStatus::Cancelled => EffectTerminalClass::Cancelled,
                    ExecutionNodeStatus::Blocked | ExecutionNodeStatus::Failed => {
                        EffectTerminalClass::Failed
                    }
                    _ => EffectTerminalClass::Uncertain,
                },
                idempotency_ref: node.idempotency_key.clone(),
            });
            observation.cost_delta = CostDelta {
                model_steps: u64::from(result.usage.model.is_some()),
                tool_calls: result.usage.tool_calls,
                duration_ms: result.usage.duration_ms,
                input_tokens: result.usage.input_tokens,
                output_tokens: result.usage.output_tokens,
                cached_tokens: result.usage.cached_tokens,
            };
            observation.failure_class = (result.status != ExecutionNodeStatus::Completed)
                .then_some(match node.kind {
                    ExecutionNodeKind::Approval => ObservationFailureClass::Approval,
                    ExecutionNodeKind::Verify => ObservationFailureClass::Verification,
                    ExecutionNodeKind::AgentTask => ObservationFailureClass::Unknown,
                    _ => ObservationFailureClass::Unknown,
                });
            Some(observation)
        })
        .collect()
}

pub(super) fn propose_intervention_after_observation(
    services: &crate::RuntimeServices,
    goal_id: &str,
    observation: RuntimeObservation,
) -> Result<RuntimeIntervention, String> {
    let projection = services
        .goal_store()
        .projection(goal_id)?
        .ok_or_else(|| format!("goal {goal_id} disappeared before intervention"))?;
    let mut progress = projection.progress;
    crate::execution_core::GoalProgressReducer::apply(&mut progress, &observation)?;
    let mut observations = projection.observations;
    observations.push(observation);
    crate::execution_core::InterventionPolicy.propose(&projection.goal, &progress, &observations)
}

pub(super) fn dynamic_node(
    ticket: &NodeExecutionTicket,
    iteration: usize,
    suffix: &str,
    kind: ExecutionNodeKind,
    executor_prefix: &str,
    _scoped_model_kind: &str,
) -> ExecutionNodeSpec {
    let id = format!("{}:{iteration}:{suffix}", ticket.graph_id);
    ExecutionNodeSpec {
        id: id.clone(),
        kind,
        payload_ref: ticket.payload_ref.clone(),
        executor_kind: executor_prefix.to_string(),
        idempotency_key: format!("{id}:attempt"),
        lease_ref: None,
        acceptance: Default::default(),
        retry_policy: Default::default(),
        resource_scopes: Vec::new(),
        work: Some(
            harness_contract::execution_graph::ExecutionWorkContract::new(match kind {
                ExecutionNodeKind::ToolBatch => {
                    harness_contract::execution_graph::ExecutionWorkRole::Tool
                }
                ExecutionNodeKind::Verify | ExecutionNodeKind::Approval => {
                    harness_contract::execution_graph::ExecutionWorkRole::Verify
                }
                ExecutionNodeKind::Synthesize => {
                    harness_contract::execution_graph::ExecutionWorkRole::Synthesize
                }
                ExecutionNodeKind::AgentTask | ExecutionNodeKind::Subgraph => {
                    harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze
                }
                ExecutionNodeKind::InlineModel => {
                    harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze
                }
                ExecutionNodeKind::SessionDispatch | ExecutionNodeKind::Timer => {
                    harness_contract::execution_graph::ExecutionWorkRole::Plan
                }
            }),
        ),
    }
}

pub(super) fn model_intent_summary(intent: &ModelStepIntent) -> String {
    match intent {
        ModelStepIntent::FinalAnswer { .. } => "model produced a terminal answer".to_string(),
        ModelStepIntent::ToolCalls { calls } => {
            format!("model requested {} tool call(s)", calls.len())
        }
        ModelStepIntent::Replan { reason } => format!("model requested replan: {reason}"),
    }
}

pub(super) fn provider_protocol_intervention_kind(attempt: u8) -> RuntimeInterventionKind {
    if attempt <= PROVIDER_PROTOCOL_RECOVERY_BUDGET {
        RuntimeInterventionKind::Replan
    } else {
        RuntimeInterventionKind::Block
    }
}

pub(super) fn provider_failure_intervention_kind_after_receipt(
    evidence_synthesis_attempted: bool,
) -> RuntimeInterventionKind {
    if evidence_synthesis_attempted {
        RuntimeInterventionKind::Block
    } else {
        RuntimeInterventionKind::Synthesize
    }
}

pub(super) fn runtime_replan_context_item(
    node_id: &str,
    intervention: Option<&RuntimeIntervention>,
) -> Option<ContextItem> {
    let intervention =
        intervention.filter(|value| value.kind == RuntimeInterventionKind::Replan)?;
    let mut item = ContextItem::new(
        format!("runtime-replan-guidance:{node_id}"),
        ContextSourceKind::Task,
        ContextRole::Instruction,
        format!(
            "Runtime replan guidance (mandatory): {}",
            intervention.reason
        ),
    );
    item.authority = ContextAuthority::System;
    item.visibility = ContextVisibility::Private;
    item.evidence = intervention.evidence_refs.clone();
    Some(item)
}

pub(super) fn final_answer_recovery_reason(
    text: &str,
    _workspace_root: &std::path::Path,
) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Some("empty final answer".to_string());
    }
    let normalized = trimmed.to_ascii_lowercase();
    if [
        "<tool_call",
        "<function=",
        "<parameter=",
        "</tool_call>",
        "<｜｜dsml｜｜tool_calls>",
        "<｜｜dsml｜｜invoke",
        "```tool_use",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return Some("simulated tool-call markup in a final answer".to_string());
    }
    if looks_like_unfinished_work_preamble(trimmed) {
        return Some("unfinished work preamble was presented as a final answer".to_string());
    }
    None
}

pub(super) fn final_answer_recovery_reason_for_objective(
    text: &str,
    workspace_root: &std::path::Path,
    objective: &str,
) -> Option<String> {
    let _ = objective;
    final_answer_recovery_reason(text, workspace_root)
}

pub(super) fn final_answer_recovery_reason_for_execution_scope(
    text: &str,
    workspace_root: &std::path::Path,
    objective: &str,
    bounded_evidence_role: bool,
) -> Option<String> {
    if bounded_evidence_role {
        // A delegated role owns only its typed Focus/output contract. Aggregate
        // requirements from the parent objective (for example, a minimum
        // number of source paths across all lanes) belong to the parent
        // synthesizer and must not reject an otherwise complete child result.
        final_answer_recovery_reason(text, workspace_root)
    } else {
        final_answer_recovery_reason_for_objective(text, workspace_root, objective)
    }
}

pub(super) fn looks_like_unfinished_work_preamble(text: &str) -> bool {
    if text.chars().count() > 420 || text.lines().count() > 3 {
        return false;
    }
    let normalized = text.trim().to_ascii_lowercase();
    let promises_more_work = [
        "let me try",
        "let me read",
        "let me inspect",
        "let me get",
        "i will now read",
        "i will now inspect",
        "i'll read",
        "i'll inspect",
        "i need to read",
        "let me continue",
        "让我再",
        "让我尝试",
        "让我使用",
        "让我获取",
        "让我读取",
        "让我搜索",
        "我再读取",
        "我来执行",
        "我将继续",
        "接下来我会",
        "继续读取",
        "继续检查",
        "用 glob",
        "用 grep",
        "用 read",
        "现在读取",
        "先读取",
        "需要查看",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix));
    let explicit_continuation = [
        "let me try",
        "let me read",
        "let me inspect",
        "let me get",
        "let me continue",
        "i will now read",
        "i will now inspect",
        "i'll read",
        "i'll inspect",
        "i need to read",
        "让我继续",
        "让我尝试",
        "让我使用",
        "让我获取",
        "让我读取",
        "让我搜索",
        "同时查看可用的工具",
        "继续收集完整证据",
        "需要小段读取",
        "需要分块读取",
        "同时搜索",
        "先收集证据",
        "先获取",
        "让我直接搜索",
        "let me continue",
        "continue collecting evidence",
    ]
    .iter()
    .any(|fragment| normalized.contains(fragment));
    explicit_continuation
        || promises_more_work
            && (normalized.ends_with(':')
                || normalized.ends_with('：')
                || normalized.contains("once more")
                || normalized.contains("再试一次"))
}

pub(super) fn normalize_terminal_answer_with_evidence(
    text: &str,
    _tool_results: &[ConversationMessage],
    _workspace_root: &std::path::Path,
    _objective: &str,
) -> String {
    let trimmed = text.trim();
    if serde_json::from_str::<serde_json::Value>(trimmed).is_ok_and(|value| value.is_object()) {
        // Delegated roles and evaluation Judges own exact machine-readable
        // output contracts. Appending prose evidence to a valid JSON object
        // silently corrupts that contract and makes completed Agent work fail
        // at the parent validation boundary.
        return trimmed.to_string();
    }
    if looks_like_unfinished_work_preamble(text) {
        return text.trim().to_string();
    }
    text.trim().to_string()
}

pub(super) fn retained_orchestration_terminal_candidate(
    messages: &[ConversationMessage],
    workspace_root: &std::path::Path,
    objective: &str,
) -> Option<String> {
    let mut candidates = messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_name,
                output,
                is_error: false,
                ..
            } if tool_name.eq_ignore_ascii_case("runtime_orchestrate")
                || tool_name.eq_ignore_ascii_case(
                    harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
                ) =>
            {
                orchestration_receipt_json(output)
            }
            _ => None,
        })
        .filter_map(|receipt| verified_team_terminal_summary(&receipt))
        .map(|summary| summary.trim().to_string())
        .filter(|summary| !summary.is_empty() && !looks_like_unfinished_work_preamble(summary))
        .collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.chars().count()));
    candidates.into_iter().find_map(|candidate| {
        let normalized = normalize_terminal_answer_with_evidence(
            &candidate,
            messages,
            workspace_root,
            objective,
        );
        final_answer_recovery_reason_for_objective(&normalized, workspace_root, objective)
            .is_none()
            .then_some(normalized)
    })
}

pub(super) fn replace_latest_assistant_text(
    assistant_messages: &mut [ConversationMessage],
    pending_transcript: &mut BTreeMap<String, Vec<ConversationMessage>>,
    node_id: &str,
    text: &str,
) {
    let replace = |message: &mut ConversationMessage| {
        if let Some(ContentBlock::Text { text: current }) = message
            .blocks
            .iter_mut()
            .find(|block| matches!(block, ContentBlock::Text { .. }))
        {
            *current = text.to_string();
        }
    };
    if let Some(message) = assistant_messages.last_mut() {
        replace(message);
    }
    if let Some(message) = pending_transcript
        .get_mut(node_id)
        .and_then(|messages| messages.last_mut())
    {
        replace(message);
    }
}

pub(super) fn early_tool_receipt_message(
    receipt: &crate::conversation::EarlyToolExecutionReceipt,
) -> ConversationMessage {
    let is_error = receipt.outcome.status != crate::RuntimeToolExecutionStatus::Executed;
    let output = receipt
        .outcome
        .output
        .clone()
        .or_else(|| receipt.outcome.error.clone())
        .unwrap_or_else(|| {
            format!(
                "Early tool receipt recorded as {}",
                receipt.outcome.evidence_ref
            )
        });
    ConversationMessage::tool_result(
        receipt.call.id.clone(),
        receipt.call.name.clone(),
        output,
        is_error,
    )
}

pub(super) fn terminal_evidence_digest(messages: &[ConversationMessage]) -> String {
    let receipts = messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_name,
                output,
                is_error,
                ..
            } => Some((tool_name.as_str(), output.as_str(), *is_error)),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut seen = BTreeSet::new();
    let mut rendered = String::new();
    for (index, (tool_name, output, is_error)) in receipts.into_iter().enumerate() {
        let fingerprint = sha256_digest(output);
        if !seen.insert(fingerprint) {
            continue;
        }
        let receipt = format!(
            "\n\n### Receipt {} · {} · {}\n{}",
            index + 1,
            tool_name,
            if is_error { "failed" } else { "completed" },
            output,
        );
        rendered.push_str(&receipt);
    }
    rendered.trim().to_string()
}

pub(super) fn focus_synthesis_evidence_context_item(
    node_id: &str,
    calls: &[ModelToolCall],
    messages: &[ConversationMessage],
    required_fields: &[String],
) -> Option<ContextItem> {
    let evidence = terminal_evidence_digest(messages);
    if evidence.is_empty() {
        return None;
    }
    let required_fields = required_fields.join(", ");
    let mut item = ContextItem::new(
        format!("runtime-focus-synthesis-evidence:{node_id}"),
        ContextSourceKind::ToolTrace,
        ContextRole::Evidence,
        format!(
            "## Runtime-verified Focus evidence\n\
             The Focus acceptance scopes for this delegated role are complete. \
             The receipts below are the actual committed, role-local tool results. \
             Use their source paths and content to cover every required terminal presentation field \
             [{required_fields}]. Native structured output, JSON, Markdown headings, and `Field: value` labels are accepted. \
             Do not claim that source evidence is unavailable, do not invoke more tools, and do not \
             substitute prose for the committed receipt facts.\n\n{evidence}"
        ),
    );
    item.authority = ContextAuthority::System;
    item.visibility = ContextVisibility::Private;
    item.evidence = calls
        .iter()
        .map(|call| format!("tool_call:{}", call.id))
        .collect();
    Some(item)
}

/// Bounds no-tool final-answer recovery using the same Runtime lease that
/// governs the turn. This is intentionally not a global retry constant:
/// complex work with already-committed evidence deserves more chances to
/// convert a malformed provider response into a useful synthesis, while a
/// pressured or explicitly constrained turn stops promptly.
pub(super) fn terminal_recovery_retry_budget(
    lease: &crate::execution_core::ExecutionBudgetLease,
) -> u8 {
    use harness_contract::core::TaskComplexity;

    let mut retries: u8 = match lease.complexity {
        TaskComplexity::Trivial | TaskComplexity::Simple => 1,
        TaskComplexity::Moderate => 2,
        TaskComplexity::Complex | TaskComplexity::Strategic => 3,
    };
    if lease.explicit_user_limit.is_some_and(|limit| limit <= 2) {
        retries = retries.min(1);
    }
    retries
}

/// A provider may emit a normal final answer and then append direct XML-like
/// tool markup in the same text stream. The adapter can only execute a *pure*,
/// declared XML response; executing a mixed prose block would turn generated
/// text into a command channel. Preserve the already complete answer and drop
/// only a suffix that begins with a tool marker after visible prose. A response
/// that starts with markup remains invalid and follows governed recovery.
pub(super) fn strip_trailing_simulated_tool_markup(text: String) -> String {
    let normalized = text.to_ascii_lowercase();
    let start = [
        "<tool_call",
        "<function=",
        "<parameter=",
        "<｜｜dsml｜｜tool_calls>",
        "<｜｜dsml｜｜invoke",
        "```tool_use",
    ]
    .iter()
    .filter_map(|marker| normalized.find(marker))
    .min();
    let Some(start) = start else {
        return text;
    };
    let suffix = &text[start..];
    let lower_suffix = suffix.to_ascii_lowercase();
    let is_direct_markup = lower_suffix.starts_with("<tool_call")
        || lower_suffix.starts_with("<function=")
        || lower_suffix.starts_with("<parameter=")
        || lower_suffix.starts_with("<｜｜dsml｜｜tool_calls>")
        || lower_suffix.starts_with("<｜｜dsml｜｜invoke")
        || lower_suffix.starts_with("```tool_use");
    if start > 0 && is_direct_markup {
        text[..start].trim_end().to_string()
    } else {
        text
    }
}

pub(super) fn model_intent_kind(intent: &ModelStepIntent) -> &'static str {
    match intent {
        ModelStepIntent::FinalAnswer { .. } => "final_answer",
        ModelStepIntent::ToolCalls { .. } => "tool_calls",
        ModelStepIntent::Replan { .. } => "replan",
    }
}

pub(super) fn independent_tool_call_count(intent: &ModelStepIntent) -> usize {
    match intent {
        ModelStepIntent::ToolCalls { calls } => calls
            .iter()
            .filter(|call| call.depends_on.is_empty())
            .count(),
        _ => 0,
    }
}

pub(super) fn context_pressure_basis_points(input_tokens: u64, context_window: u32) -> i64 {
    let window = u64::from(context_window.max(1));
    i64::try_from(input_tokens.saturating_mul(10_000) / window).unwrap_or(i64::MAX)
}

pub(super) fn failed_tool_names(messages: &[ConversationMessage]) -> Vec<String> {
    let mut names = messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_name,
                is_error: true,
                ..
            } => Some(tool_name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

/// Return the one bounded, model-repairable semantic admission diagnostic from
/// a failed receipt. Gateway deliberately exposes this compact receipt instead
/// of an executable graph, so this parser uses typed recovery hints rather
/// than inferring state from provider prose.
pub(super) fn retryable_collaboration_compile_diagnostic(
    messages: &[ConversationMessage],
) -> Option<String> {
    messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .find_map(|block| {
            let ContentBlock::ToolResult {
                tool_name,
                output,
                is_error: true,
                ..
            } = block
            else {
                return None;
            };
            if !tool_name.eq_ignore_ascii_case(
                harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
            ) {
                return None;
            }
            orchestration_receipt_json(output).and_then(|receipt| {
                receipt
                    .get("recovery_hints")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|hints| {
                        hints.iter().find_map(|hint| {
                            (hint.get("retryable").and_then(serde_json::Value::as_bool)
                                == Some(true))
                            .then(|| hint.get("code").and_then(serde_json::Value::as_str))
                            .flatten()
                            .filter(|code| code.starts_with("collaboration_compile_"))
                            .map(str::to_string)
                        })
                    })
            })
        })
}

/// A governed action is identified by its tool name and canonical input, not
/// by the provider-generated call id. The id changes on every retry, so using
/// it would hide repeated work from Goal/Intervention policy.
pub(super) fn tool_batch_fingerprint(calls: &[ModelToolCall]) -> String {
    let mut actions = calls
        .iter()
        .map(|call| {
            let input = canonical_tool_input_for_governance(call);
            format!("{}:{input}", call.name)
        })
        .collect::<Vec<_>>();
    actions.sort();
    format!("tool_action:{}", sha256_digest(&actions.join("\n")))
}

/// Capability discovery is a control-plane lookup, not new evidence. Provider
/// wording in `intent` is intentionally excluded so paraphrased repeated
/// queries are visible to the InterventionPolicy instead of resetting its
/// novelty/progress accounting. Detail, surface, and profile remain because
/// they change the returned capability view.
pub(super) fn canonical_tool_input_for_governance(call: &ModelToolCall) -> String {
    let parsed = serde_json::from_str::<serde_json::Value>(&call.input).ok();
    if call.name.eq_ignore_ascii_case("runtime_capabilities") {
        let object = parsed.as_ref().and_then(serde_json::Value::as_object);
        let detail = object
            .and_then(|value| value.get("detail"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("summary");
        let surface = object
            .and_then(|value| value.get("surface"))
            .and_then(serde_json::Value::as_str);
        let profile = object
            .and_then(|value| value.get("profile"))
            .and_then(serde_json::Value::as_str);
        return serde_json::json!({
            "detail": detail,
            "surface": surface,
            "profile": profile,
        })
        .to_string();
    }
    parsed.map_or_else(|| call.input.clone(), |value| value.to_string())
}

/// Derive stable evidence coverage keys without treating provider-generated
/// call identifiers or superficial query variations as new investigation.
/// Direct file reads retain file-level detail; broad discovery tools collapse
/// to their workspace/crate zone so repeatedly globbing or grepping the same
/// area becomes visible to the Goal policy as low-novelty work.
pub(super) fn tool_batch_coverage_keys(calls: &[ModelToolCall]) -> BTreeSet<String> {
    calls
        .iter()
        .flat_map(tool_call_coverage_keys)
        .collect::<BTreeSet<_>>()
}

/// Delegated evidence roles have a deliberately tighter contract. Main turns
/// get one additional no-progress batch so a multi-chunk file read is not cut
/// off prematurely, but must still converge before repeatedly rebuilding an
/// ever-larger provider context from unchanged evidence.
pub(super) const fn evidence_saturation_limit(bounded_evidence_role: bool) -> usize {
    if bounded_evidence_role {
        2
    } else {
        3
    }
}

pub(super) fn focus_acceptance_is_met(
    bounded_evidence_role: bool,
    required_scopes: &[String],
    pending_scopes: &[String],
) -> bool {
    bounded_evidence_role && !required_scopes.is_empty() && pending_scopes.is_empty()
}

pub(super) fn should_force_focus_synthesis(
    focus_acceptance_met: bool,
    required_scopes: &[String],
    _repeated_evidence_saturation: bool,
    has_retained_terminal_candidate: bool,
) -> bool {
    if !focus_acceptance_met {
        return false;
    }
    // Runtime retained this candidate specifically while it completed the
    // missing deterministic read. Once the typed receipt closes the exact
    // obligation, another provider answer creates a stale second-final race;
    // the retained candidate owns the text and the receipt owns the evidence.
    if has_retained_terminal_candidate {
        return true;
    }
    // A bounded read-only role has no additional acceptance obligation once
    // its exact scope is proven.  Leaving native tools enabled here invites
    // the provider to keep rediscovering the same source and, worse, lets a
    // later malformed/exhausted request turn a successful receipt into a
    // blocked Team.  Move directly to the text-only terminal presentation;
    // the synthesis still has every retained receipt in context.
    //
    // This is about the role's immutable evidence contract, not its display
    // name, template, or any catalog-specific behavior.
    !required_scopes.is_empty()
}

pub(super) fn should_recover_missing_required_write(
    required_write_for_completion: bool,
    bounded_evidence_role: bool,
    repeated_evidence_saturation: bool,
    write_attempt_paths: &[String],
    successful_write_observed: bool,
    required_write_replans: u8,
) -> bool {
    required_write_for_completion
        && !bounded_evidence_role
        && repeated_evidence_saturation
        && write_attempt_paths.is_empty()
        && !successful_write_observed
        && required_write_replans == 0
}

pub(super) fn required_write_for_turn(
    strategy_requires_write: bool,
    bounded_evidence_role: bool,
    focus_acceptance_scopes: &[String],
) -> bool {
    if bounded_evidence_role {
        return focus_acceptance_scopes
            .iter()
            .any(|scope| scope.starts_with("write:"));
    }
    strategy_requires_write
}

/// Responsibility-zone coverage is intentionally coarser than file coverage.
/// It is consulted only for a bounded delegated role, where reading another
/// file in an already-investigated component is not by itself a reason to
/// defer a supported conclusion.
pub(super) fn tool_batch_scope_keys(calls: &[ModelToolCall]) -> BTreeSet<String> {
    calls
        .iter()
        .flat_map(|call| {
            let value = serde_json::from_str::<serde_json::Value>(&call.input).ok();
            let paths = value.as_ref().map(coverage_paths).unwrap_or_default();
            if paths.is_empty() {
                vec![format!("tool:{}", call.name.to_ascii_lowercase())]
            } else {
                paths.iter().map(|path| coverage_zone(path)).collect()
            }
        })
        .collect()
}

pub(super) fn tool_call_coverage_keys(call: &ModelToolCall) -> Vec<String> {
    let value = serde_json::from_str::<serde_json::Value>(&call.input).ok();
    let name = call.name.to_ascii_lowercase();
    let paths = value.as_ref().map(coverage_paths).unwrap_or_default();
    let is_discovery = matches!(
        name.as_str(),
        "workspace_snapshot"
            | "glob_search"
            | "glob_many"
            | "grep_search"
            | "grep_many"
            | "lsp"
            | "toolsearch"
            | "tool_search"
            | "runtime_capabilities"
            | "tool_batch_readonly"
    );
    if is_discovery {
        let zones = if paths.is_empty() {
            vec!["workspace".to_string()]
        } else {
            paths.iter().map(|path| coverage_zone(path)).collect()
        };
        return zones
            .into_iter()
            .map(|zone| format!("discovery:{zone}"))
            .collect();
    }
    if !paths.is_empty() {
        return paths
            .iter()
            .map(|path| format!("evidence:{name}:{}", normalized_coverage_path(path)))
            .collect();
    }
    vec![format!("tool:{name}")]
}

pub(super) fn coverage_paths(value: &serde_json::Value) -> Vec<String> {
    const PATH_FIELDS: &[&str] = &[
        "path",
        "file_path",
        "file",
        "files",
        "paths",
        "pattern",
        "patterns",
        "searches",
        "uri",
        "evidence_ref",
    ];
    let mut values = Vec::new();
    if let Some(object) = value.as_object() {
        for field in PATH_FIELDS {
            if let Some(field_value) = object.get(*field) {
                collect_coverage_strings(field_value, &mut values);
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

pub(super) fn collect_coverage_strings(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => {
            if let Ok(nested) = serde_json::from_str::<serde_json::Value>(value) {
                collect_coverage_strings(&nested, output);
            } else if !value.trim().is_empty() {
                output.push(value.trim().to_string());
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_coverage_strings(value, output);
            }
        }
        serde_json::Value::Object(values) => {
            for field in ["path", "file_path", "file", "glob", "pattern", "uri"] {
                if let Some(value) = values.get(field) {
                    collect_coverage_strings(value, output);
                }
            }
        }
        _ => {}
    }
}

pub(super) fn normalized_coverage_path(path: &str) -> String {
    let path = path.replace('\\', "/");
    path.find("crates/")
        .map_or(path.clone(), |index| path[index..].to_string())
}

pub(super) fn coverage_zone(path: &str) -> String {
    let normalized = normalized_coverage_path(path);
    let parts = normalized.split('/').collect::<Vec<_>>();
    if parts.first() == Some(&"crates") && parts.len() >= 2 {
        format!("crates/{}", parts[1])
    } else if parts.first() == Some(&"docs") {
        "docs".to_string()
    } else {
        "workspace".to_string()
    }
}

pub(super) fn explicit_model_step_limit(content: &str) -> Option<usize> {
    ["max_steps=", "max model steps=", "最大模型步骤="]
        .into_iter()
        .find_map(|marker| {
            let start = content.to_ascii_lowercase().find(marker)? + marker.len();
            let digits = content[start..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            digits.parse::<usize>().ok().filter(|value| *value > 0)
        })
}

/// The legacy conversation engine appends provider/tool messages during the
/// effect call. They are deliberately removed until Runner commits the node;
/// `after_commit` is the only publisher to the parent transcript.
pub(super) fn resource_scopes_for_tool_calls(calls: &[ModelToolCall]) -> Vec<String> {
    // These scopes are descriptive graph/evaluation metadata, not execution
    // leases. ToolBatch container nodes deliberately skip ScopeLockManager in
    // GraphRunner; each leaf acquires its authoritative descriptor-derived
    // ResourceDemand through ToolExecutionPlane.
    let mut paths = std::collections::BTreeMap::<String, bool>::new();
    let mut other = Vec::new();
    for call in calls {
        let Ok(input) = serde_json::from_str::<serde_json::Value>(&call.input) else {
            continue;
        };
        let Some(effect) = graph_metadata_effect(&call.name, &input) else {
            continue;
        };
        let access = effect.effect_kind != harness_contract::tool::ToolEffectKind::Read;
        for scope in effect.scopes {
            let Some(target) = scope.target else {
                continue;
            };
            match scope.resource {
                harness_contract::policy::PermissionResource::File
                | harness_contract::policy::PermissionResource::Tool => {
                    paths
                        .entry(target)
                        .and_modify(|write| *write |= access)
                        .or_insert(access);
                }
                harness_contract::policy::PermissionResource::Network => {
                    other.push("network:*".to_string());
                }
                _ => {}
            }
        }
    }
    other.extend(
        paths
            .into_iter()
            .map(|(path, write)| format!("{}:{path}", if write { "write" } else { "read" })),
    );
    other.sort();
    other.dedup();
    other
}

pub(super) fn graph_metadata_effect(
    tool_name: &str,
    input: &serde_json::Value,
) -> Option<harness_contract::tool::ToolEffectDescriptor> {
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use harness_contract::tool::{
        ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
        ToolPermissionMode,
    };

    // This bridge only materializes model-declared paths before the registered
    // ToolHost is entered. It is not an execution safety fallback: unknown or
    // pathless tools emit no graph scope and remain an Unknown barrier in the
    // governed compiler.
    let normalized = tool_name.trim().replace('-', "_").to_ascii_lowercase();
    let effect_kind = match normalized.as_str() {
        "read_file" | "read_many" | "grep_search" | "grep_many" | "glob_search" | "glob_many"
        | "workspace_snapshot" => ToolEffectKind::Read,
        "write_file" | "edit_file" | "apply_patch_transaction" | "notebook_edit" => {
            ToolEffectKind::Write
        }
        _ => return None,
    };
    let mut targets = Vec::new();
    collect_graph_resource_targets(input, &mut targets);
    targets.sort();
    targets.dedup();
    if targets.is_empty() {
        return None;
    }
    let operation = if effect_kind == ToolEffectKind::Read {
        PermissionOperation::Read
    } else {
        PermissionOperation::Write
    };
    Some(ToolEffectDescriptor {
        tool_id: tool_name.to_string(),
        descriptor_hash: "graph-metadata-only".to_string(),
        effect_kind,
        idempotency: ToolIdempotency::Unknown,
        scopes: targets
            .into_iter()
            .map(|target| PermissionScope {
                resource: PermissionResource::File,
                operation: operation.clone(),
                target: Some(target),
            })
            .collect(),
        required_permission: if effect_kind == ToolEffectKind::Read {
            ToolPermissionMode::ReadOnly
        } else {
            ToolPermissionMode::WorkspaceWrite
        },
        approval_class: ToolApprovalClass::None,
        uses_network: false,
        spawns_process: false,
        mutates_packages: false,
        mutates_system: false,
        assessment: harness_contract::policy::EffectAssessment {
            reversibility: if effect_kind == ToolEffectKind::Read {
                harness_contract::policy::EffectReversibility::Reversible
            } else {
                harness_contract::policy::EffectReversibility::Compensatable
            },
            externality: if effect_kind == ToolEffectKind::Read {
                harness_contract::policy::EffectExternality::Internal
            } else {
                harness_contract::policy::EffectExternality::Workspace
            },
            data_sensitivity: harness_contract::policy::DataClassification::Internal,
            novelty: harness_contract::policy::EffectNovelty::Routine,
            blast_radius: if effect_kind == ToolEffectKind::Read {
                harness_contract::policy::EffectBlastRadius::Item
            } else {
                harness_contract::policy::EffectBlastRadius::Workspace
            },
        },
    })
}

pub(super) fn collect_graph_resource_targets(value: &serde_json::Value, targets: &mut Vec<String>) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_graph_resource_targets(value, targets);
            }
        }
        serde_json::Value::Object(values) => {
            for field in ["path", "file_path", "file", "notebook_path"] {
                if let Some(path) = values.get(field).and_then(serde_json::Value::as_str) {
                    targets.push(path.to_string());
                }
            }
            for field in ["files", "edits", "calls", "searches"] {
                if let Some(value) = values.get(field) {
                    collect_graph_resource_targets(value, targets);
                }
            }
        }
        _ => {}
    }
}

/// Compile model-provided paths into graph lock scopes without turning a bad
/// path into a terminal graph failure.  These scopes only coordinate concurrent
/// work; the governed tool host remains the authority that permits or rejects
/// the actual filesystem operation.
///
/// A path outside the workspace (or containing a parent traversal) is therefore
/// represented by a conservative workspace-wide lock.  The tool still receives
/// the original path and returns its normal security error to the model, which
/// lets the next model step correct a typo instead of leaving the turn without a
/// terminal result.
pub(super) fn graph_resource_scopes_for_tool_calls(
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) -> Vec<String> {
    let mut scopes = resource_scopes_for_tool_calls(calls);
    let mut invalid_read = false;
    let mut invalid_write = false;
    scopes.retain_mut(|scope| {
        let Some((mode, path)) = scope
            .split_once(':')
            .map(|(mode, path)| (mode.to_string(), path.trim().to_string()))
        else {
            return true;
        };
        if !matches!(mode.as_str(), "read" | "write") {
            return true;
        }
        let requested = std::path::Path::new(&path);
        let valid = if requested.is_absolute() {
            if let Ok(relative) = requested.strip_prefix(workspace_root) {
                let relative = if relative.as_os_str().is_empty() {
                    ".".to_string()
                } else {
                    relative.to_string_lossy().replace('\\', "/")
                };
                *scope = format!("{mode}:{relative}");
                true
            } else {
                false
            }
        } else {
            !requested.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        };
        if valid {
            true
        } else {
            invalid_write |= mode == "write";
            invalid_read |= mode == "read";
            false
        }
    });

    if invalid_write || (invalid_read && scopes.iter().any(|scope| scope.starts_with("write:"))) {
        scopes.retain(|scope| !scope.starts_with("read:") && !scope.starts_with("write:"));
        scopes.push("write:.".to_string());
    } else if invalid_read {
        scopes.retain(|scope| !scope.starts_with("read:"));
        scopes.push("read:.".to_string());
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

pub(super) fn successful_tool_call_ids(messages: &[ConversationMessage]) -> BTreeSet<String> {
    messages
        .iter()
        .flat_map(|message| &message.blocks)
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                is_error: false,
                ..
            } => Some(tool_use_id.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn normalize_workspace_scope(scope: &str) -> Option<(&str, String)> {
    let (mode, path) = scope.split_once(':')?;
    if !matches!(mode, "read" | "write" | "workspace") {
        return None;
    }
    let path = path.trim().replace('\\', "/");
    if path.starts_with('/') {
        return None;
    }
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => return None,
            value if value.contains(':') => return None,
            value => components.push(value),
        }
    }
    if components.is_empty() {
        return (path == "." || path == "./").then(|| (mode, ".".to_string()));
    }
    Some((mode, components.join("/")))
}

pub(super) fn evaluation_scope_authorizes(allowed: &str, requested: &str) -> bool {
    let (Some((allowed_mode, allowed_path)), Some((requested_mode, requested_path))) = (
        normalize_workspace_scope(allowed),
        normalize_workspace_scope(requested),
    ) else {
        return allowed == requested;
    };
    let mode_authorized = match allowed_mode {
        "write" | "workspace" => matches!(requested_mode, "read" | "write" | "workspace"),
        "read" => requested_mode == "read",
        _ => false,
    };
    mode_authorized
        && (allowed_path == "."
            || requested_path == allowed_path
            || requested_path
                .strip_prefix(&allowed_path)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

pub(super) fn evaluation_scope_violation(
    allowed: &[String],
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) -> Option<String> {
    if allowed.is_empty() {
        return None;
    }
    // Provider tool calls commonly use an absolute path even though the
    // evaluation contract is workspace-relative. Reuse the production graph
    // canonicalizer so an in-workspace absolute path is checked against the
    // same relative scope instead of being rejected and replanned forever.
    graph_resource_scopes_for_tool_calls(calls, workspace_root)
        .into_iter()
        .find(|requested| {
            !allowed
                .iter()
                .any(|scope| evaluation_scope_authorizes(scope, requested))
        })
}

pub(super) fn pending_focus_write_action_violation(
    pending_scopes: &[String],
    observed_scopes: &BTreeSet<String>,
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) -> Option<Vec<String>> {
    let pending_writes = pending_scopes
        .iter()
        .filter(|scope| scope.starts_with("write:"))
        .cloned()
        .collect::<Vec<_>>();
    if pending_writes.is_empty()
        || !pending_writes.iter().all(|write_scope| {
            write_scope
                .strip_prefix("write:")
                .is_some_and(|path| observed_scopes.contains(&format!("read:{path}")))
        })
    {
        return None;
    }
    let requested = graph_resource_scopes_for_tool_calls(calls, workspace_root);
    (!requested.iter().any(|scope| {
        scope.starts_with("write:")
            && pending_writes
                .iter()
                .any(|required| evaluation_scope_authorizes(required, scope))
    }))
    .then_some(pending_writes)
}

pub(super) fn focus_action_rejection_outcome(
    ticket: &NodeExecutionTicket,
    state: &mut TurnGraphState,
    pending_writes: &[String],
) -> (RuntimeIntervention, Vec<ExecutionNodeSpec>) {
    state.focus_action_rejections = state.focus_action_rejections.saturating_add(1);
    let pending = pending_writes.join(", ");
    if state.focus_action_rejections <= 2 {
        state.force_tool_allowlist_next_model = Some(required_mutation_tool_allowlist());
        let reason = format!(
            "the delegated mutation role already has its required pre-write read receipt; the next accepted action must invoke an authorized write tool for [{pending}]. Do not reread, search, glob, synthesize, or claim the change in prose before the committed write receipt exists"
        );
        return (
            RuntimeIntervention {
                goal_id: state.goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason,
                evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                expected_graph_revision: None,
            },
            vec![dynamic_node(
                ticket,
                state.iterations,
                "focus-required-write-replan-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            )],
        );
    }

    let terminal_reason = format!(
        "Execution blocked after the delegated mutation role repeatedly ignored the required write action [{pending}]. No unverified replacement action was executed."
    );
    state.terminal_override = Some((GoalCompletion::Partial, terminal_reason.clone()));
    let mut node = dynamic_node(
        ticket,
        state.iterations,
        "focus-required-write-block-synthesize",
        ExecutionNodeKind::Synthesize,
        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
        "inline_model",
    );
    node.executor_kind =
        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
    (
        RuntimeIntervention {
            goal_id: state.goal_id.clone(),
            kind: RuntimeInterventionKind::Block,
            reason: terminal_reason,
            evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
            expected_graph_revision: None,
        },
        vec![node],
    )
}

pub(super) fn evaluation_scope_rejection_outcome(
    ticket: &NodeExecutionTicket,
    state: &mut TurnGraphState,
    violation: &str,
    workspace_root: &std::path::Path,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    replan_node_suffix: &str,
    reason: String,
) -> (RuntimeIntervention, Vec<ExecutionNodeSpec>) {
    state.evaluation_scope_rejections = state.evaluation_scope_rejections.saturating_add(1);
    if state.evaluation_scope_rejections == 1 {
        return (
            RuntimeIntervention {
                goal_id: state.goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason,
                evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                expected_graph_revision: None,
            },
            vec![dynamic_node(
                ticket,
                state.iterations,
                replan_node_suffix,
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            )],
        );
    }

    // A provider may ignore the exact-path correction and repeat a broad
    // `read:.` request. The registered evaluation contract already contains
    // the exact paths, so compile those independent reads into one governed
    // ToolBatch instead of spending another provider turn or blocking useful
    // work. This recovery is deliberately unavailable to writes, malformed
    // paths, unbounded scope sets, and any third violation.
    if state.evaluation_scope_rejections == 2 && violation == "read:." {
        if let Some(calls) = evaluation_scope_recovery_tool_calls(
            &state.evaluation_resource_scopes,
            state.iterations,
        ) {
            if let Ok(nodes) = tool_nodes_for_calls(
                ticket,
                state.iterations,
                &state.session_id,
                calls,
                workspace_root,
            ) {
                return (
                    RuntimeIntervention {
                        goal_id: state.goal_id.clone(),
                        kind: RuntimeInterventionKind::Replan,
                        reason: "replaced a repeated broad read request with bounded, exact-path reads from the pre-registered evaluation contract".to_string(),
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                        expected_graph_revision: None,
                    },
                    nodes,
                );
            }
        }
    }

    // After exact-path evidence recovery, a mutation objective may still
    // repeat the same broad read instead of attempting its authorized write.
    // Grant one final model step with the existing resource ceiling intact;
    // never synthesize a write on the model's behalf and never loop.
    if required_write_final_replan_allowed(
        state.evaluation_scope_rejections,
        violation,
        state.required_write_for_completion,
        &state.write_attempt_paths,
    ) {
        state.force_tool_allowlist_next_model = Some(required_mutation_tool_allowlist());
        let reason = "exact-path evidence is already retained, but the mutation objective has not attempted its authorized write. Do not read the workspace broadly again. Invoke the smallest authorized exact-path write now, or return an honest blocked result if the retained evidence is insufficient".to_string();
        return (
            RuntimeIntervention {
                goal_id: state.goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason,
                evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                expected_graph_revision: None,
            },
            vec![dynamic_node(
                ticket,
                state.iterations,
                "eval-required-write-final-replan-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            )],
        );
    }

    // If the broad read is requested after a successful write receipt, it
    // represents verification intent rather than another pre-write
    // exploration. Compile one final bounded exact-read batch from the existing
    // evaluation lease;
    // no model-authored path, extra write, or scope expansion is admitted.
    if post_write_exact_read_recovery_allowed(
        state.evaluation_scope_rejections,
        violation,
        state.required_write_for_completion,
        write_obligation_satisfied(
            state.required_write_for_completion,
            &state.required_workspace_write_scopes,
            &state.committed_workspace_observed_evidence,
            state.committed_workspace_write_observed || state.collaboration_committed_write,
            path_identity_resolver,
        ),
    ) {
        if let Some(calls) = evaluation_scope_recovery_tool_calls(
            &state.evaluation_resource_scopes,
            state.iterations,
        ) {
            if let Ok(nodes) = tool_nodes_for_calls(
                ticket,
                state.iterations,
                &state.session_id,
                calls,
                workspace_root,
            ) {
                return (
                    RuntimeIntervention {
                        goal_id: state.goal_id.clone(),
                        kind: RuntimeInterventionKind::Replan,
                        reason: "replaced a post-write broad read with bounded, exact-path verification reads from the pre-registered evaluation contract".to_string(),
                        evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                        expected_graph_revision: None,
                    },
                    nodes,
                );
            }
        }
    }

    let terminal_reason = format!(
        "Execution blocked after repeated evaluation scope violations: `{violation}`. No out-of-scope effect was executed; narrow the requested action or explicitly expand the authorized scope."
    );
    state.terminal_override = Some((GoalCompletion::Partial, terminal_reason.clone()));
    let mut node = dynamic_node(
        ticket,
        state.iterations,
        "eval-resource-ceiling-block-synthesize",
        ExecutionNodeKind::Synthesize,
        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
        "inline_model",
    );
    node.executor_kind =
        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
    (
        RuntimeIntervention {
            goal_id: state.goal_id.clone(),
            kind: RuntimeInterventionKind::Block,
            reason: terminal_reason,
            evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
            expected_graph_revision: None,
        },
        vec![node],
    )
}

pub(super) fn required_write_final_replan_allowed(
    rejection_count: u8,
    violation: &str,
    required_write_for_completion: bool,
    write_attempt_paths: &[String],
) -> bool {
    rejection_count == 3
        && violation == "read:."
        && required_write_for_completion
        && write_attempt_paths.is_empty()
}

pub(super) fn post_write_exact_read_recovery_allowed(
    rejection_count: u8,
    violation: &str,
    required_write_for_completion: bool,
    successful_write_observed: bool,
) -> bool {
    rejection_count == 3
        && violation == "read:."
        && required_write_for_completion
        && successful_write_observed
}

pub(super) fn required_mutation_tool_allowlist() -> BTreeSet<String> {
    BTreeSet::from(["edit_file".to_string(), "write_file".to_string()])
}

/// Compile a repeated broad evaluation read into bounded exact-path calls.
///
/// The returned calls are dependency-free so the existing ToolBatch scheduler
/// can execute them concurrently. Write scopes authorize verification reads,
/// but never become write calls in this recovery path.
pub(super) fn evaluation_scope_recovery_tool_calls(
    scopes: &[String],
    iteration: usize,
) -> Option<Vec<ModelToolCall>> {
    let mut paths = scopes
        .iter()
        .filter_map(|scope| {
            let (mode, path) = normalize_workspace_scope(scope)?;
            matches!(mode, "read" | "write" | "workspace")
                .then_some(path)
                .filter(|path| path != ".")
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    if paths.is_empty() || paths.len() > 8 {
        return None;
    }
    Some(
        paths
            .into_iter()
            .enumerate()
            .map(|(index, path)| ModelToolCall {
                id: format!("runtime-eval-exact-read-{iteration}-{index}"),
                name: "read_file".to_string(),
                input: serde_json::json!({"path": path}).to_string(),
                depends_on: Vec::new(),
            })
            .collect(),
    )
}

pub(super) fn record_write_attempt_paths(
    paths: &mut Vec<String>,
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) {
    paths.extend(
        graph_resource_scopes_for_tool_calls(calls, workspace_root)
            .into_iter()
            .filter_map(|scope| scope.strip_prefix("write:").map(str::to_string)),
    );
    paths.sort();
    paths.dedup();
}

/// Compile deterministic post-write verification into governed read calls.
///
/// Focus acceptance scopes are Runtime-authored and already bounded by the
/// delegated role contract. Executing their exact reads here avoids spending
/// another provider round trip merely to ask the model to repeat a mechanical
/// action. Mixed or malformed scopes retain the normal model-driven recovery.
pub(super) fn focus_verification_tool_calls(
    pending_scopes: &[String],
    iteration: usize,
    workspace_root: &Path,
) -> Option<Vec<ModelToolCall>> {
    if pending_scopes.is_empty() {
        return None;
    }
    pending_scopes
        .iter()
        .enumerate()
        .map(|(index, scope)| {
            let path = scope
                .strip_prefix("read:")
                .or_else(|| scope.strip_prefix("verify_after_write:"))
                .or_else(|| scope.strip_prefix("verify_upstream_change:"))?;
            let (_, path) = normalize_workspace_scope(&format!("read:{path}"))?;
            let path = focus_verification_read_path(&path, workspace_root)?;
            (path != ".").then(|| ModelToolCall {
                id: format!("runtime-focus-verify-{iteration}-{index}"),
                name: "read_file".to_string(),
                // ExactContent is a whole-object evidence contract. The
                // default read window is useful for exploration but cannot
                // close this obligation, even though it reports a full-file
                // digest. Runtime therefore makes EOF intent explicit.
                input: serde_json::json!({"path": path, "complete": true}).to_string(),
                depends_on: Vec::new(),
            })
        })
        .collect()
}

/// Return only Focus acceptance scopes proven by a successful,
/// Runtime-authored exact `read_file` call in the current ToolBatch.
///
/// This is a receipt bridge, not an authorization bypass: arbitrary
/// model-generated reads never enter it, and a scope is returned only when it
/// already belongs to the immutable role contract and the matching tool
/// result is successful.
pub(super) fn successful_runtime_focus_scope_keys(
    calls: &[ModelToolCall],
    successful_call_ids: &BTreeSet<String>,
    required_scopes: &[String],
) -> BTreeSet<String> {
    calls
        .iter()
        .filter(|call| {
            call.id.starts_with("runtime-focus-verify-")
                && call.name == "read_file"
                && successful_call_ids.contains(&call.id)
        })
        .filter_map(|call| {
            serde_json::from_str::<serde_json::Value>(&call.input)
                .ok()
                .and_then(|input| {
                    (input.get("complete").and_then(serde_json::Value::as_bool) == Some(true))
                        .then(|| {
                            input
                                .get("path")
                                .and_then(|path| path.as_str())
                                .map(str::to_string)
                        })
                        .flatten()
                })
        })
        .flat_map(|path| {
            required_scopes.iter().filter_map(move |scope| {
                let scoped_path = scope
                    .strip_prefix("read:")
                    .or_else(|| scope.strip_prefix("verify_after_write:"))
                    .or_else(|| scope.strip_prefix("verify_upstream_change:"));
                (scoped_path == Some(path.as_str())).then(|| scope.clone())
            })
        })
        .collect()
}

/// A `read:` Focus may authorize either a file or a directory. `read_file`
/// cannot satisfy the latter when passed the directory itself, so choose one
/// deterministic regular descendant for the Runtime-authored verification.
/// The governed ToolHost still performs the actual read and creates the
/// receipt; this lookup only selects a safe, already-authorized target.
pub(super) fn focus_verification_read_path(path: &str, workspace_root: &Path) -> Option<String> {
    if path == "." {
        return None;
    }
    let candidate = workspace_root.join(path);
    let metadata = std::fs::symlink_metadata(&candidate).ok();
    if metadata.as_ref().is_none_or(|metadata| !metadata.is_dir()) {
        return Some(path.to_string());
    }
    let root = workspace_root.canonicalize().ok()?;
    let file = first_regular_workspace_file(&candidate, &root)?;
    file.strip_prefix(&root)
        .ok()?
        .to_str()
        .map(|relative| relative.replace('\\', "/"))
}

pub(super) fn first_regular_workspace_file(
    directory: &Path,
    workspace_root: &Path,
) -> Option<PathBuf> {
    let mut directories = vec![directory.to_path_buf()];
    while let Some(directory) = directories.pop() {
        let mut entries = std::fs::read_dir(directory)
            .ok()?
            .flatten()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries.into_iter().rev() {
            let file_type = entry.file_type().ok()?;
            if file_type.is_symlink() {
                continue;
            }
            let path = entry.path();
            if file_type.is_file() {
                let canonical = path.canonicalize().ok()?;
                if canonical.starts_with(workspace_root) {
                    return Some(canonical);
                }
            } else if file_type.is_dir() {
                directories.push(path);
            }
        }
    }
    None
}

pub(super) fn concrete_focus_verification_scopes(
    pending_scopes: &[String],
    observed_evidence: &[harness_contract::context::ObservedEvidence],
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> Vec<String> {
    let mut concrete = pending_scopes
        .iter()
        .flat_map(|pending| {
            let Some(authorized_path) = pending.strip_prefix("verify_after_write:") else {
                return vec![pending.clone()];
            };
            let required_write = format!("write:{authorized_path}");
            let required = resolver.compile_obligation_or_unresolved(&required_write);
            let matched = observed_evidence
                .iter()
                .filter(|observed| {
                    crate::path_identity::observed_evidence_satisfies(&required, observed)
                })
                .filter_map(|observed| match &observed.target {
                    harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                        if scope.access_mode
                            == harness_contract::context::WorkspaceAccessMode::Write =>
                    {
                        Some(scope.path.workspace_relative_path.as_str())
                    }
                    _ => None,
                })
                .filter(|path| *path != ".")
                .map(|path| format!("verify_after_write:{path}"))
                .collect::<Vec<_>>();
            if matched.is_empty() {
                vec![pending.clone()]
            } else {
                matched
            }
        })
        .collect::<Vec<_>>();
    concrete.sort();
    concrete.dedup();
    concrete
}

pub(super) fn should_prefetch_focus_verification(
    first_model_step: bool,
    bounded_evidence_role: bool,
    already_prefetched: bool,
    pending_scopes: &[String],
) -> bool {
    first_model_step
        && bounded_evidence_role
        && !already_prefetched
        && !pending_scopes.is_empty()
        && pending_scopes
            .iter()
            .all(|scope| scope.starts_with("read:") || scope.starts_with("verify_upstream_change:"))
}

pub(super) fn typed_satisfied_focus_acceptance_scope_keys(
    required_scopes: &[String],
    current: &[harness_contract::context::ObservedEvidence],
    prior: &[harness_contract::context::ObservedEvidence],
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> BTreeSet<String> {
    let mut all = prior.to_vec();
    all.extend_from_slice(current);
    required_scopes
        .iter()
        .filter(|raw| {
            let evidence = if raw.starts_with("verify_upstream_change:") {
                current.to_vec()
            } else {
                all.clone()
            };
            // A directory-scoped write authorization is intentionally broad,
            // while post-write truth is exact-path. Resolve every committed
            // descendant write into its concrete verification obligation and
            // require all of those reads before closing the parent scope.
            concrete_focus_verification_scopes(&[(*raw).clone()], &all, resolver)
                .into_iter()
                .all(|concrete| {
                    // The whole-workspace read alias (`read:.`) is only minted
                    // under a full-trust lease. Compile it with root-alias
                    // tolerance so any Runtime-attested descendant exact read
                    // satisfies it; the strict compiler would reject the root
                    // alias and keep the obligation unsatisfiable forever.
                    let root_alias = matches!(
                        concrete.trim(),
                        "read:." | "read:./" | "write:." | "write:./"
                    );
                    let required = if root_alias {
                        resolver.compile_required_acceptance_with_root_alias(&[], &[concrete], true)
                    } else {
                        resolver.compile_required_acceptance(&[], &[concrete])
                    };
                    // Host is only deciding whether it must schedule another
                    // deterministic verification read. It still asks the
                    // canonical evaluator for the verdict rather than
                    // matching obligations locally; terminal producers and
                    // Team reduction consume that same envelope unchanged.
                    let (_, evaluation) =
                        crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_terminal(
                            &required,
                            Vec::new(),
                            evidence.clone(),
                        );
                    evaluation.verdict == harness_contract::acceptance::AcceptanceVerdict::Satisfied
                })
        })
        .cloned()
        .collect()
}

/// Explain a completed Runtime-owned reviewer prefetch to the synthesis step.
///
/// The instruction is deliberately limited to upstream-review scopes. A
/// post-write verification has different ownership semantics and ordinary
/// reads must never be promoted into an independent review claim.
pub(super) fn upstream_verification_completion_instruction(
    verified_scopes: &BTreeSet<String>,
) -> Option<String> {
    let paths = verified_scopes
        .iter()
        .filter_map(|scope| scope.strip_prefix("verify_upstream_change:"))
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return None;
    }
    Some(format!(
        "Runtime reviewer evidence (authoritative): before this synthesis, the governed tool DAG performed this role's independent exact-path read for [{}]. The retained read receipt, exact content, byteLength, sha256, endsWithNewline and tool:// reference are role-local evidence, not an upstream self-report. Tools are now disabled because acquisition is complete, not because verification was unavailable. Return one concise JSON object under 800 output tokens: cite exact byte metadata and upstream read/write receipts (including protected-path evidence), distinguish verified state from genuine risk, and do not claim that content, trailing-newline, or unchanged-scope verification was impossible when the receipts prove it.",
        paths.join(", ")
    ))
}

#[derive(Debug, Clone)]
pub(super) struct ParsedRouteInput {
    pub(super) batch: harness_contract::input_disposition::ModelInputDispositionBatch,
    pub(super) constraints: harness_contract::orchestration::ModelRuntimeOrchestrationConstraints,
    pub(super) remaining_calls: Vec<ModelToolCall>,
    pub(super) route_call_id: String,
}

#[derive(Debug, Clone)]
pub(super) enum RouteInputResolution {
    NotRequired,
    Valid(ParsedRouteInput),
    Invalid(String),
}

pub(super) fn parse_route_input_intent(
    intent: &ModelStepIntent,
    slot_count: usize,
) -> RouteInputResolution {
    let ModelStepIntent::ToolCalls { calls } = intent else {
        return RouteInputResolution::Invalid(
            "pending running-Turn inputs require a route_input tool call before terminal output"
                .to_string(),
        );
    };
    let route_calls = calls
        .iter()
        .filter(|call| {
            call.name
                .eq_ignore_ascii_case(harness_contract::orchestration::RUNTIME_ORCHESTRATE_TOOL_ID)
        })
        .filter_map(|call| {
            serde_json::from_str::<harness_contract::orchestration::ModelRuntimeOrchestrationInput>(
                &call.input,
            )
            .ok()
            .filter(|input| {
                input.operation
                    == harness_contract::orchestration::RuntimeOrchestrationOperation::RouteInput
            })
            .map(|input| (call, input))
        })
        .collect::<Vec<_>>();
    if route_calls.len() != 1 {
        return RouteInputResolution::Invalid(format!(
            "expected exactly one runtime_orchestrate(route_input) call, received {}",
            route_calls.len()
        ));
    }
    let (route_call, mut input) = route_calls[0].clone();
    if input.inspect_execution_id.is_some() || input.proposal.is_some() || input.control.is_some() {
        return RouteInputResolution::Invalid(
            "route_input must contain only semantic input_disposition decisions".to_string(),
        );
    }
    let Some(batch) = input.input_disposition.take() else {
        return RouteInputResolution::Invalid(
            "route_input is missing input_disposition".to_string(),
        );
    };
    if let Err(error) = batch.validate_slots(slot_count) {
        return RouteInputResolution::Invalid(error);
    }
    let route_call_id = route_call.id.as_str();
    let mut remaining_calls = calls
        .iter()
        .filter(|call| call.id != route_call_id)
        .cloned()
        .collect::<Vec<_>>();
    for call in &mut remaining_calls {
        call.depends_on
            .retain(|dependency| dependency != route_call_id);
    }
    RouteInputResolution::Valid(ParsedRouteInput {
        batch,
        constraints: input.constraints,
        remaining_calls,
        route_call_id: route_call.id.clone(),
    })
}

pub(super) fn remove_tool_call_from_latest_assistant(
    assistant_messages: &mut [ConversationMessage],
    pending_transcript: &mut BTreeMap<String, Vec<ConversationMessage>>,
    node_id: &str,
    tool_call_id: &str,
) {
    let remove = |message: &mut ConversationMessage| {
        message.blocks.retain(
            |block| !matches!(block, ContentBlock::ToolUse { id, .. } if id == tool_call_id),
        );
    };
    if let Some(message) = assistant_messages.last_mut() {
        remove(message);
    }
    if let Some(message) = pending_transcript
        .get_mut(node_id)
        .and_then(|messages| messages.last_mut())
    {
        remove(message);
    }
}

/// Keep stateful runtime orchestration outside a workspace-tool batch.
///
/// A `runtime_orchestrate(propose:team)` call may synchronously drive a child
/// graph whose agents read or write the workspace. If it shares one parent
/// ToolBatch with a file mutation, the graph-level lease would be retained
/// across the entire child execution. We compile two ordered durable batches:
/// normal tools retain their exact scopes; runtime control is governed by its
/// own contract and does not claim filesystem ownership. Cross-batch
/// dependencies are represented by this order and removed from the inner
/// batch scheduler.
pub(super) fn tool_batches_for_turn(
    calls: &[ModelToolCall],
) -> Result<Vec<Vec<ModelToolCall>>, String> {
    // An escalation is guarded by a Runtime receipt, rather than a model
    // assertion that it has already inspected source.  Providers commonly
    // emit a first read/glob and the required escalation in the same frame;
    // executing that frame as one parallel ToolBatch races the receipt guard
    // and consumes the Agent's only requested escalation.  Persist the
    // source batch first and make the escalation its own successor node.
    //
    // This is deliberately a scheduling constraint, not an inferred model
    // dependency: the delegated tool itself retains the safe-checkpoint
    // validation and still rejects an escalation when there is no actual
    // prior evidence receipt.
    let (managed_escalation, other): (Vec<_>, Vec<_>) = calls.iter().cloned().partition(|call| {
        call.name
            .eq_ignore_ascii_case("request_collaboration_escalation")
    });
    if !managed_escalation.is_empty() && !other.is_empty() {
        let mut batches = tool_batches_for_turn(&other)?;
        let escalation_ids = managed_escalation
            .iter()
            .map(|call| call.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let mut escalation = managed_escalation;
        for call in &mut escalation {
            // Dependencies on the completed evidence batch are represented by
            // the durable graph edge between batches; retain only dependencies
            // between same-batch escalation calls for the leaf scheduler.
            call.depends_on
                .retain(|dependency| escalation_ids.contains(dependency));
        }
        batches.push(escalation);
        return Ok(batches);
    }

    let (runtime_control, regular): (Vec<_>, Vec<_>) = calls.iter().cloned().partition(|call| {
        call.name
            .eq_ignore_ascii_case(harness_contract::orchestration::RUNTIME_ORCHESTRATE_TOOL_ID)
    });
    if runtime_control.is_empty() || regular.is_empty() {
        return Ok(vec![calls.to_vec()]);
    }

    let runtime_ids = runtime_control
        .iter()
        .map(|call| call.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let regular_ids = regular
        .iter()
        .map(|call| call.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let regular_after_runtime = regular.iter().any(|call| {
        call.depends_on
            .iter()
            .any(|dependency| runtime_ids.contains(dependency.as_str()))
    });
    let runtime_after_regular = runtime_control.iter().any(|call| {
        call.depends_on
            .iter()
            .any(|dependency| regular_ids.contains(dependency.as_str()))
    });
    if regular_after_runtime && runtime_after_regular {
        return Err(
            "runtime_orchestrate and workspace tools contain a cross-batch dependency cycle"
                .to_string(),
        );
    }

    let mut ordered = if regular_after_runtime {
        vec![runtime_control, regular]
    } else {
        // No explicit cross-batch dependency, or runtime control depends on
        // evidence from regular tools: release workspace leases first.
        vec![regular, runtime_control]
    };
    for batch in &mut ordered {
        let ids = batch
            .iter()
            .map(|call| call.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for call in batch {
            call.depends_on
                .retain(|dependency| ids.contains(dependency));
        }
    }
    Ok(ordered)
}

pub(super) fn dynamic_edges(from: &str, nodes: &[ExecutionNodeSpec]) -> Vec<ExecutionEdge> {
    let mut previous = from.to_string();
    nodes
        .iter()
        .map(|node| {
            let edge = ExecutionEdge {
                from: previous.clone(),
                to: node.id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            };
            previous.clone_from(&node.id);
            edge
        })
        .collect()
}

pub(super) fn structured_field_is_materialized(value: Option<&serde_json::Value>) -> bool {
    match value {
        Some(serde_json::Value::String(value)) => !value.trim().is_empty(),
        Some(serde_json::Value::Array(values)) => !values.is_empty(),
        Some(serde_json::Value::Object(values)) => !values.is_empty(),
        Some(serde_json::Value::Bool(_) | serde_json::Value::Number(_)) => true,
        Some(serde_json::Value::Null) | None => false,
    }
}

pub(super) fn missing_required_structured_fields(
    candidate: &str,
    required: &[String],
) -> Vec<String> {
    let output =
        crate::agent_in_process_worker::structured_agent_output_for_fields(candidate, required);
    required
        .iter()
        .filter(|field| {
            let value = output
                .as_ref()
                .and_then(|object| object.get(field.as_str()));
            !crate::agent_in_process_worker::structured_contract_field_materialized(field, value)
        })
        .cloned()
        .collect()
}

pub(super) fn normalized_team_terminal_candidate(
    candidate: &str,
    required: &[String],
) -> Option<String> {
    let body = candidate.trim();
    if body.is_empty()
        || body.starts_with("<synthesized_terminal")
        || body.contains("<tool_call>")
        || body.contains("```tool_use")
        || body.contains("<function=")
    {
        return None;
    }
    let mut object =
        crate::agent_in_process_worker::structured_agent_output_for_fields(body, required)
            .unwrap_or_default();
    for required_field in required {
        if !missing_required_structured_field_from_object(&object, required_field) {
            continue;
        }
        // A bounded research/direct role may use the two user-facing labels
        // `summary` and `findings` interchangeably. This is transport
        // normalization only; Focus receipt verification has already happened
        // before this helper runs.
        let alias = match required_field.as_str() {
            "findings" => Some("summary"),
            "summary" => Some("findings"),
            _ => None,
        };
        if let Some(value) = alias
            .and_then(|field| object.get(field))
            .filter(|value| structured_field_is_materialized(Some(value)))
            .cloned()
        {
            object.insert(required_field.clone(), value);
            continue;
        }
        if narrative_terminal_field_is_safe(required_field) {
            object.insert(
                required_field.clone(),
                serde_json::Value::String(body.to_string()),
            );
            continue;
        }
        // Risks, unresolved work, and legacy acceptance claims are material
        // disclosures. Generic prose must never manufacture them.
        return None;
    }
    serde_json::to_string(&object).ok()
}

/// After one isolated, zero-tool presentation recovery, tolerate only missing
/// Runtime-declared custom artifact wrappers. The provider must already have
/// materialized every fixed field (especially evidence, risks and unresolved
/// work); Runtime merely copies that recovered terminal wording under the
/// declared transport key. Agent validation still requires fresh tool
/// receipts or authenticated upstream evidence for the custom artifact.
pub(super) fn normalized_declared_custom_terminal_after_recovery(
    candidate: &str,
    required: &[String],
) -> Option<String> {
    normalized_terminal_after_bounded_recovery(candidate, required)
}

/// Close a presentation-only contract after a bounded provider recovery and
/// after Runtime has already verified all evidence scopes.
///
/// This never creates evidence or semantic findings. Narrative fields and
/// declared custom wrappers retain the provider's own terminal wording. A
/// missing disclosure is represented as an explicit Runtime-observed gap,
/// never as an empty list (which would falsely claim that the provider found
/// no risk or unresolved work).
pub(super) fn normalized_terminal_after_bounded_recovery(
    candidate: &str,
    required: &[String],
) -> Option<String> {
    let body = candidate.trim();
    if body.is_empty()
        || body.starts_with("<synthesized_terminal")
        || body.contains("<tool_call>")
        || body.contains("```tool_use")
        || body.contains("<function=")
    {
        return None;
    }
    let mut object =
        crate::agent_in_process_worker::structured_agent_output_for_fields(body, required)
            .unwrap_or_default();
    for field in required {
        if !missing_required_structured_field_from_object(&object, field) {
            continue;
        }
        let value = if narrative_terminal_field_is_safe(field) {
            match field.as_str() {
                "findings" => object
                    .get("summary")
                    .filter(|value| structured_field_is_materialized(Some(value)))
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
                "summary" => object
                    .get("findings")
                    .filter(|value| structured_field_is_materialized(Some(value)))
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
                _ => serde_json::Value::String(body.to_string()),
            }
        } else if matches!(
            field.as_str(),
            "risks" | "unresolved" | "unresolved_or_risks"
        ) {
            serde_json::json!([{
                "kind": "runtime_presentation_gap",
                "status": "provider_omitted_after_bounded_recovery",
                "field": field,
                "no_empty_state_inferred": true,
            }])
        } else if fixed_team_terminal_field(field) {
            // Evidence, decisions, and other fixed semantic declarations
            // remain provider/receipt owned and cannot be manufactured here.
            return None;
        } else {
            // Runtime-declared custom artifacts already accept an exact
            // provider-authored summary wrapper in the ordinary recovery
            // path. Preserve that authority while closing sibling fields.
            object
                .get("summary")
                .or_else(|| object.get("findings"))
                .filter(|value| structured_field_is_materialized(Some(value)))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string()))
        };
        object.insert(field.clone(), value);
    }
    required
        .iter()
        .all(|field| !missing_required_structured_field_from_object(&object, field))
        .then(|| serde_json::to_string(&object).ok())
        .flatten()
}

pub(super) fn fixed_team_terminal_field(field: &str) -> bool {
    matches!(
        field,
        "evidence"
            | "summary"
            | "findings"
            | "plan"
            | "implementation"
            | "source_verification"
            | "review"
            | "risks"
            | "unresolved"
            | "key_decisions"
            | "unresolved_or_risks"
            | "proposal"
            | "critique"
            | "mitigation"
            | "checkpoint"
    )
}

pub(super) fn missing_required_structured_field_from_object(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> bool {
    !crate::agent_in_process_worker::structured_contract_field_materialized(
        field,
        object.get(field),
    )
}

pub(super) fn narrative_terminal_field_is_safe(field: &str) -> bool {
    matches!(
        field,
        "summary"
            | "findings"
            | "plan"
            | "implementation"
            | "source_verification"
            | "review"
            | "proposal"
            | "critique"
            | "mitigation"
            | "checkpoint"
    )
}

/// Materialize the implementer's mechanical hand-off directly from committed
/// Runtime receipts. The independent reviewer still performs the semantic
/// comparison against the user objective; this only avoids paying for a model
/// to restate an already verified write + post-write read as JSON.
pub(super) fn runtime_verified_implementation_terminal_candidate(
    required: &[String],
    observed_scopes: &BTreeSet<String>,
    write_attempt_paths: &[String],
    tool_results: &[ConversationMessage],
    workspace_root: &std::path::Path,
) -> Option<String> {
    let fields = required.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if fields != BTreeSet::from(["implementation", "source_verification"]) {
        return None;
    }
    let mut write_paths = observed_scopes
        .iter()
        .filter_map(|scope| scope.strip_prefix("write:"))
        .filter(|path| *path != ".")
        .map(str::to_string)
        .collect::<Vec<_>>();
    write_paths.sort();
    write_paths.dedup();
    if write_paths.is_empty()
        || !write_paths.iter().all(|path| {
            write_attempt_paths.contains(path)
                && observed_scopes.contains(&format!("verify_after_write:{path}"))
        })
    {
        return None;
    }
    let receipts = runtime_tool_receipt_evidence(tool_results, workspace_root);
    if receipts.is_empty() {
        return None;
    }
    let post_write_evidence_ref = receipts.iter().rev().find_map(|receipt| {
        (receipt.get("tool").and_then(serde_json::Value::as_str) == Some("read_file"))
            .then(|| receipt.get("evidence_ref").cloned())
            .flatten()
    })?;
    serde_json::to_string(&serde_json::json!({
        "implementation": {
            "status": "committed",
            "write_paths": write_paths.clone(),
            "runtime_receipt_count": receipts.len(),
            "receipts": receipts.clone(),
        },
        "source_verification": {
            "status": "verified_after_commit",
            "paths": write_paths,
            "post_write_evidence_ref": post_write_evidence_ref,
        },
        "risks": "No effect-level risk detected by Runtime; the independent reviewer remains responsible for semantic acceptance against the objective.",
    }))
    .ok()
}

pub(super) fn runtime_tool_receipt_evidence(
    messages: &[ConversationMessage],
    workspace_root: &std::path::Path,
) -> Vec<serde_json::Value> {
    messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .enumerate()
        .filter_map(|(index, block)| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error: false,
            } => {
                let evidence_ref = output
                    .split_once("tool://")
                    .map(|(_, tail)| tail)
                    .and_then(|tail| tail.split_whitespace().next())
                    .map(|value| {
                        format!(
                            "tool://{}",
                            value.trim_end_matches(['.', ',', ';', ')', ']', '}'])
                        )
                    })?;
                Some(serde_json::json!({
                    "sequence": index.saturating_add(1),
                    "tool_call_id": tool_use_id,
                    "tool": tool_name,
                    "evidence_ref": evidence_ref,
                    "paths": tool_receipt_workspace_paths(output, workspace_root),
                }))
            }
            _ => None,
        })
        .collect()
}

pub(super) fn tool_receipt_workspace_paths(
    output: &str,
    workspace_root: &std::path::Path,
) -> Vec<String> {
    let Some(object_start) = output.find('{') else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&output[object_start..]) else {
        return Vec::new();
    };
    let Some(raw) = value
        .pointer("/file/filePath")
        .or_else(|| value.get("filePath"))
        .and_then(serde_json::Value::as_str)
    else {
        return Vec::new();
    };
    let path = std::path::Path::new(raw);
    let relative = if path.is_absolute() {
        path.strip_prefix(workspace_root).ok()
    } else {
        Some(path)
    };
    relative
        .filter(|path| {
            !path.as_os_str().is_empty()
                && !path.components().any(|component| {
                    matches!(
                        component,
                        std::path::Component::ParentDir
                            | std::path::Component::RootDir
                            | std::path::Component::Prefix(_)
                    )
                })
        })
        .map(|path| vec![path.to_string_lossy().replace('\\', "/")])
        .unwrap_or_default()
}

pub(super) fn completed_result(
    result_ref: Option<String>,
    usage: ExecutionUsage,
) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref,
        summary: None,
        evidence_refs: Vec::new(),
        failure: None,
        usage,
        finished_at_ms: crate::tool_invocation::now_ms(),
    }
}
