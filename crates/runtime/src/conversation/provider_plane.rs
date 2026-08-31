//! Provider admission, request execution, and governed terminal synthesis.

use super::*;

#[derive(Debug, Default)]
pub(super) struct TurnProviderState {
    pub(super) tool_exposure: TurnToolExposureMetrics,
    pub(super) unavailable_accounts: BTreeSet<String>,
}

impl std::ops::Deref for TurnProviderState {
    type Target = TurnToolExposureMetrics;

    fn deref(&self) -> &Self::Target {
        &self.tool_exposure
    }
}

impl std::ops::DerefMut for TurnProviderState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tool_exposure
    }
}

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    fn provider_account_key_for_model(&self, model: &str) -> Option<String> {
        self.api_client.provider_name_for_model(model).map(|name| {
            self.provider_resource_config
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .account_for(&name, model)
        })
    }

    fn provider_account_unavailable(&self, account: Option<&String>) -> bool {
        account.is_some_and(|account| {
            self.turn_tool_exposure_metrics
                .lock()
                .map(|state| state.unavailable_accounts.contains(account))
                .unwrap_or(false)
        })
    }

    fn mark_provider_account_unavailable(&self, error: &RuntimeError) {
        if error.provider_failure_scope()
            != model_protocol::provider_failure::ProviderFailureScope::Account
        {
            return;
        }
        if let (Some(account), Ok(mut state)) = (
            error.provider_account_key(),
            self.turn_tool_exposure_metrics.lock(),
        ) {
            state.unavailable_accounts.insert(account.to_string());
        }
    }

    fn next_available_provider_candidate(
        &self,
        candidates: &mut VecDeque<String>,
    ) -> Option<(String, Option<String>)> {
        while let Some(model) = candidates.pop_front() {
            let account = self.provider_account_key_for_model(&model);
            if !self.provider_account_unavailable(account.as_ref()) {
                return Some((model, account));
            }
        }
        None
    }

    fn discover_tools_with_metrics(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        let started = Instant::now();
        let discovery = self.tool_executor.tool_discovery_receipt();
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_catalog_lookup(started.elapsed());
        }
        discovery
    }

    fn configure_terminal_tool_exposure(&mut self, reason: &str) {
        let revision = self
            .tool_exposure_revision
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let discovery = self.discover_tools_with_metrics();
        self.api_client.configure_tool_exposure(
            ToolExposureState {
                catalog_revision: discovery.catalog_revision,
                bootstrap: Default::default(),
                active: Default::default(),
                deferred: discovery
                    .descriptors
                    .iter()
                    .map(|descriptor| descriptor.canonical_id.clone())
                    .collect(),
                reason: reason.to_string(),
                revision,
                fallback_full: false,
            }
            .projection(0),
        );
    }

    fn pack_timed_provider_attempt(
        &self,
        prompt: &PromptAssembly,
        messages: &HistoryView,
        model: &str,
        inventory: ProviderContextInventory,
    ) -> Result<ApiRequest, RuntimeError> {
        let started = Instant::now();
        let request = self.pack_provider_attempt(prompt, messages, model, inventory);
        crate::execution_core::performance::observe_duration(
            "request_materialize_ms",
            started.elapsed(),
        );
        request
    }

    fn observe_provider_downstream_overload(
        result: crate::execution_core::graph::ResourceResultClass,
    ) {
        if result == crate::execution_core::graph::ResourceResultClass::DownstreamOverload {
            crate::execution_core::performance::observe_count(
                "provider_downstream_overload_total",
                1,
            );
        }
    }

    /// Require one text-only provider response after a governed evidence
    /// checkpoint. The normal dynamic tool exposure is restored afterwards.
    pub(crate) fn require_next_model_final_response(&self) {
        self.next_model_text_only.store(true, Ordering::SeqCst);
    }

    /// Restrict exactly one provider request to an existing subset of tools.
    /// Tool discovery, authorization and resource ceilings remain authoritative;
    /// unknown names are omitted rather than activated.
    pub(crate) fn require_next_model_tools(&self, tool_ids: impl IntoIterator<Item = String>) {
        if let Ok(mut allowlist) = self.next_model_tool_allowlist.lock() {
            *allowlist = Some(tool_ids.into_iter().collect());
        }
    }

    /// Restrict the next provider request to a governed tool subset and make
    /// one real call mandatory. Ordinary autonomous planning continues to use
    /// automatic tool choice; this is reserved for a missing committed action.
    pub(crate) fn require_next_model_tool_action(
        &self,
        tool_ids: impl IntoIterator<Item = String>,
    ) {
        self.require_next_model_tools(tool_ids);
        self.next_model_tool_required.store(true, Ordering::SeqCst);
    }

    /// Require one exact already-governed native tool on the next provider
    /// request. This is used only for a control-plane continuation after the
    /// model has already inspected the capability catalog.
    pub(crate) fn require_next_model_named_tool_action(&self, tool_id: impl Into<String>) {
        let tool_id = tool_id.into();
        self.require_next_model_tool_action([tool_id.clone()]);
        if let Ok(mut required_tool_name) = self.next_model_required_tool_name.lock() {
            *required_tool_name = Some(tool_id);
        }
    }

    /// Require the root model to take one native control-plane action before
    /// a user-required collaboration may admit any Team. The bounded pair
    /// deliberately includes a read-only capability inspection as well as a
    /// proposal: a model must be able to discover current template role ids
    /// before it can make a valid typed proposal. Ordinary workspace and
    /// discovery tools remain unavailable, so this is still an admission
    /// barrier rather than a hardcoded Team topology.
    #[cfg(test)]
    pub(crate) fn require_next_model_orchestration_only(&self) {
        self.require_next_model_tool_action([
            "runtime_capabilities".to_string(),
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID.to_string(),
        ]);
        // Qwen hybrid endpoints reject `tool_choice=required` while their
        // thinking mode is enabled.  The proposal step is intentionally tiny
        // and fully typed, so disable thinking for this one wire request only;
        // every admitted Team and Agent keeps its normal model policy.
        self.require_next_model_reasoning_effort("none");
    }

    /// Override reasoning effort for exactly one provider request. Provider
    /// adapters ignore this when the selected model has no compatible control.
    pub(crate) fn require_next_model_reasoning_effort(&self, effort: impl Into<String>) {
        if let Ok(mut next) = self.next_model_reasoning_effort.lock() {
            *next = Some(effort.into());
        }
    }

    /// Run one clean, zero-tool synthesis request from the original objective
    /// and already-committed evidence receipts. Unlike the normal continuation
    /// path, this request carries no exploratory assistant/tool-call history,
    /// so a provider that became stuck repeating its prior tool protocol gets
    /// one bounded opportunity to convert evidence into a deliverable.
    pub(crate) async fn execute_clean_terminal_synthesis(
        &mut self,
        objective: &str,
        evidence: &str,
    ) -> Result<ModelStepResult, RuntimeError> {
        // This owner creates an isolated reduction request directly rather
        // than passing through `execute_model_step`, so it must apply the
        // request-local reasoning overlay itself. Otherwise reasoning-capable
        // providers may spend the terminal budget on private reasoning and
        // produce no user-visible answer after evidence has been committed.
        self.require_next_model_reasoning_effort("none");
        let evidence = if evidence.trim().is_empty() {
            "No checked tool receipt was available; give an honest bounded answer and name the missing evidence."
        } else {
            evidence
        };
        let messages: HistoryView = vec![ConversationMessage::user_text(format!(
            "Original objective:\n{objective}\n\nChecked evidence receipts:\n{evidence}\n\nReturn the final answer now."
        ))]
        .into();
        self.execute_terminal_provider_step(
            objective,
            "## Clean terminal synthesis\n\
             Produce the final user-facing answer for the supplied objective from the checked \
             evidence receipts only. This request has no tools and no continuation work. Do not \
             emit function calls, simulated tool markup, plans to inspect more data, or promises \
             to continue. Give the best supported conclusion now and state unresolved facts \
             explicitly.",
            messages,
            "clean terminal synthesis exposes no executable tools",
            None,
        )
        .await
        .map(|(step, _)| step)
    }

    /// Run one bounded zero-tool provider request whose prompt is owned by
    /// Runtime (clean terminal synthesis or failure explanation). The request
    /// carries no exploratory assistant/tool-call history, so a provider that
    /// became stuck repeating its prior tool protocol gets one bounded
    /// opportunity to answer the terminal prompt.
    pub(super) async fn execute_terminal_provider_step(
        &mut self,
        objective: &str,
        system_section: &str,
        messages: HistoryView,
        exposure_reason: &str,
        presentation: Option<(&str, &str, &str, u64)>,
    ) -> Result<(ModelStepResult, Option<String>), RuntimeError> {
        let started_at = Instant::now();
        self.configure_terminal_tool_exposure(exposure_reason);

        let mut prompt = PromptAssembly::new(self.system_prompt.clone());
        prompt.push_trusted_system(crate::prompt::runtime_clock_section());
        prompt.push_trusted_system(system_section);
        let inventory = self.api_client.context_inventory();
        let mut last_error = None;
        let mut models_tried = Vec::new();
        let mut presentation_attempt_sequence = 0_u32;
        let mut provider_attempt_sequence = 0_u32;
        let one_shot_reasoning_effort = self
            .next_model_reasoning_effort
            .lock()
            .ok()
            .and_then(|mut effort| effort.take());

        let mut candidates = VecDeque::from(self.model_candidates_for_turn(objective));
        while let Some((model, provider_account_key)) =
            self.next_available_provider_candidate(&mut candidates)
        {
            'candidate_attempt: loop {
                let mut request =
                    match self.pack_provider_attempt(&prompt, &messages, &model, inventory) {
                        Ok(request) => request,
                        Err(error) => {
                            tracing::warn!(
                                model,
                                error = %error,
                                "provider request preflight rejected clean terminal synthesis"
                            );
                            last_error = Some(error);
                            break 'candidate_attempt;
                        }
                    };
                request.reasoning_effort_override = one_shot_reasoning_effort.clone();
                let mut token_reservations = match ProviderTokenReservationSet::acquire(
                    self.evaluation_provider_token_lease.as_ref(),
                    self.delegated_provider_budget.as_ref(),
                    &model,
                    &mut request,
                ) {
                    Ok(reservations) => reservations,
                    Err(error) => {
                        last_error = Some(error);
                        break 'candidate_attempt;
                    }
                };
                if !models_tried.contains(&model) {
                    models_tried.push(model.clone());
                }
                if let Some(cowd) = &self.cowd_bus {
                    cowd.emit(crate::cowd_event::CowdEvent::ProviderAttempt {
                        model: model.clone(),
                        models_tried: models_tried.clone(),
                        context_window_tokens: request.budget.context_window_tokens,
                        context_window_source: request.budget.context_window_source.clone(),
                        packed_input_tokens: request
                            .budget
                            .fixed_input_tokens
                            .saturating_add(request.budget.dynamic_input_tokens)
                            .saturating_add(request.budget.protocol_overhead_tokens),
                    });
                }
                let request_sequence = self.session_head().await.message_count;
                provider_attempt_sequence = provider_attempt_sequence.saturating_add(1);
                request.provider_evidence_context = Some(crate::ProviderRequestEvidenceContext {
                    session_id: self.session_id().to_string(),
                    request_sequence,
                    request_compiler_cache_hit: request.request_compiler_cache_hit,
                    budget: request.budget.clone(),
                    attempt: provider_attempt_sequence,
                });
                self.record_provider_context_request(
                    &request,
                    request_sequence,
                    inventory,
                    self.api_client.tool_schema_cache_stats(),
                );
                let transport_policy = provider_transport_policy(
                    request
                        .budget
                        .context_window_tokens
                        .min(u64::from(u32::MAX)) as u32,
                    &request,
                );
                let (provider_lease, provider_queue_wait) =
                    self.acquire_provider_capacity(&model, &request).await?;
                let cancellation = self.cancellation_token.clone();
                let stream_started = Instant::now();
                let terminal_attempt_id = presentation.map(|(
                    presentation_id,
                    base_attempt_id,
                    envelope_id,
                    envelope_revision,
                )| {
                    presentation_attempt_sequence = presentation_attempt_sequence.saturating_add(1);
                    let attempt_id = format!(
                        "{base_attempt_id}:provider:{}",
                        presentation_attempt_sequence
                    );
                    if let Some(bus) = &self.cowd_bus {
                        bus.emit(crate::cowd_event::CowdEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
                                presentation_id: presentation_id.to_string(),
                                attempt_id: attempt_id.clone(),
                                envelope_id: envelope_id.to_string(),
                                envelope_revision,
                                objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
                            },
                        });
                    }
                    attempt_id
                });
                let mut reducer = ModelStreamReducer::new(
                    self.cowd_bus.clone(),
                    self.runtime_event_store.clone(),
                    self.session_id().to_string(),
                );
                if let (Some((presentation_id, _, _, _)), Some(attempt_id)) =
                    (presentation, terminal_attempt_id.as_deref())
                {
                    reducer = reducer.with_terminal_presentation(presentation_id, attempt_id);
                }
                token_reservations.mark_dispatched();
                let ApiClientStream {
                    events,
                    transport_activity,
                } = self.api_client.stream_with_transport_activity(request);
                let stream_run = consume_provider_stream_with_activity(
                    events,
                    cancellation,
                    Some(ProviderStreamTimeoutPolicy {
                        idle: transport_policy.idle_timeout,
                        heartbeat_grace: transport_policy.heartbeat_grace,
                    }),
                    reducer,
                    None,
                    transport_activity,
                )
                .await;
                self.record_provider_resource_outcome(
                    provider_lease.as_ref(),
                    provider_queue_wait,
                    stream_started.elapsed(),
                    stream_run.resource_result_class,
                );
                drop(provider_lease);
                let CollectedProviderStream {
                    text,
                    public_reasoning,
                    private_reasoning,
                    signature,
                    calls,
                    usage,
                    effective_provider_identity,
                    first_event_at,
                    first_text_at: _,
                    early_tool_receipts: _,
                    early_tool_deferrals: _,
                    response_completed_at_ms,
                } = stream_run.collected;
                let effective_model = effective_provider_identity
                    .as_ref()
                    .map(|identity| identity.model.clone());
                if let Some(identity) = effective_provider_identity {
                    *self
                        .active_provider_identity
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(identity);
                }
                token_reservations.reconcile(usage)?;
                self.reconcile_provider_context_usage(usage);
                if let Some(error) = stream_run.failure {
                    let error = error.with_provider_account_key(provider_account_key.clone());
                    if self.cancellation_token.is_cancelled()
                        || error.to_string().to_ascii_lowercase().contains("cancelled")
                    {
                        if let (Some((presentation_id, _, _, _)), Some(attempt_id), Some(bus)) = (
                            presentation,
                            terminal_attempt_id.as_deref(),
                            self.cowd_bus.as_ref(),
                        ) {
                            bus.emit(crate::cowd_event::CowdEvent::TerminalDelivery {
                                delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationAborted {
                                    presentation_id: presentation_id.to_string(),
                                    attempt_id: attempt_id.to_string(),
                                    reason: "user_cancelled".to_string(),
                                },
                            });
                        }
                        return Err(error);
                    }
                    if let (Some((presentation_id, _, _, _)), Some(attempt_id), Some(bus)) = (
                        presentation,
                        terminal_attempt_id.as_deref(),
                        self.cowd_bus.as_ref(),
                    ) {
                        bus.emit(crate::cowd_event::CowdEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                                presentation_id: presentation_id.to_string(),
                                attempt_id: attempt_id.to_string(),
                                reason: "terminal_provider_attempt_failed".to_string(),
                            },
                        });
                    }
                    if error.provider_failure_scope()
                        == model_protocol::provider_failure::ProviderFailureScope::Account
                    {
                        self.mark_provider_account_unavailable(&error);
                        last_error = Some(error);
                        break 'candidate_attempt;
                    }
                    return Err(error);
                }

                if !calls.is_empty() || text.trim().is_empty() {
                    if let (Some((presentation_id, _, _, _)), Some(attempt_id), Some(bus)) = (
                        presentation,
                        terminal_attempt_id.as_deref(),
                        self.cowd_bus.as_ref(),
                    ) {
                        bus.emit(crate::cowd_event::CowdEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                                presentation_id: presentation_id.to_string(),
                                attempt_id: attempt_id.to_string(),
                                reason: if calls.is_empty() {
                                    "terminal_provider_returned_empty_text".to_string()
                                } else {
                                    "terminal_provider_returned_tool_protocol".to_string()
                                },
                            },
                        });
                    }
                    let error = RuntimeError::new(if calls.is_empty() {
                        "terminal provider returned no user-visible text"
                    } else {
                        "terminal provider returned tool protocol in a zero-tool presentation step"
                    });
                    return Err(error);
                }

                let mut blocks = Vec::new();
                if !public_reasoning.is_empty() {
                    blocks.push(ContentBlock::ReasoningSummary {
                        text: public_reasoning,
                    });
                }
                if !private_reasoning.is_empty() || !signature.is_empty() {
                    blocks.push(ContentBlock::Thinking {
                        thinking: private_reasoning,
                        signature: (!signature.is_empty()).then_some(signature),
                    });
                }
                blocks.push(ContentBlock::Text { text: text.clone() });
                for call in &calls {
                    blocks.push(ContentBlock::ToolUse {
                        id: call.id.clone(),
                        name: call.name.clone(),
                        input: call.input.clone(),
                    });
                }
                let effective_model = effective_model.or(Some(model));
                if let Some(model) = effective_model.as_ref() {
                    if !models_tried.contains(model) {
                        models_tried.push(model.clone());
                    }
                }
                return Ok((
                    ModelStepResult {
                        intent: classify_model_step_intent(text, calls),
                        assistant_message: ConversationMessage {
                            role: crate::session::MessageRole::Assistant,
                            blocks,
                            usage: Some(usage),
                        },
                        usage,
                        model: effective_model,
                        models_used: models_tried.clone(),
                        first_token_latency_ms: first_event_at.map(|first| {
                            u64::try_from(
                                first.saturating_duration_since(stream_started).as_millis(),
                            )
                            .unwrap_or(u64::MAX)
                        }),
                        active_stream_duration_ms: first_event_at
                            .map(|first| millis_since(first).max(1)),
                        wall_duration_ms: millis_since(started_at).max(1),
                        early_tool_receipts: Vec::new(),
                        early_tool_deferrals: Vec::new(),
                        response_completed_at_ms,
                        text_only_response: true,
                    },
                    terminal_attempt_id,
                ));
            }
        }

        Err(last_error.unwrap_or_else(|| {
            RuntimeError::new("terminal provider step exhausted all provider candidates")
        }))
    }

    /// Ask the provider to explain a partial/blocked terminal in user-facing
    /// Markdown: what happened, why it failed, and what to do next, written in
    /// the same language as the user's original message. Bounded to one
    /// zero-tool provider step; the caller falls back to the raw structured
    /// reason whenever this provider step cannot complete.
    pub(crate) async fn synthesize_failure_explanation(
        &mut self,
        objective: &str,
        raw_reason: &str,
        findings: &str,
        language: &str,
        presentation_id: &str,
        attempt_id: &str,
        envelope_id: &str,
        envelope_revision: u64,
    ) -> Result<(String, Option<String>, Vec<String>, String), RuntimeError> {
        let raw_reason = raw_reason.trim();
        let findings = findings.trim();
        let findings_block = if findings.is_empty() {
            String::new()
        } else {
            format!("Framework check findings:\n{findings}\n\n")
        };
        let messages: HistoryView = vec![ConversationMessage::user_text(format!(
            "Original task objective:\n{objective}\n\nRun result / failure information:\n{raw_reason}\n\n{findings_block}Give the user-facing explanation now."
        ))]
        .into();
        let (step, terminal_attempt_id) = self
            .execute_terminal_provider_step(
                objective,
                &format!(
                    "## Failure explanation\n\
                     You are the summarizer for the execution framework. The user's task did not \
                     reach its intended terminal state. Write a concise, user-facing explanation in \
                     the SAME LANGUAGE as the user's original message (detected language: {language}). \
                     Adapt the structure, detail and tone to the user's request. Make completed work, \
                     unresolved facts and the most useful next action easy to understand, without \
                     forcing fixed headings. Do not output JSON unless the user explicitly requested \
                     it, do not simulate tool calls, and do not dump the raw error stack. If the \
                     failure information is incomplete, honestly state what can currently be confirmed.",
                ),
                messages,
                "failure explanation exposes no executable tools",
                Some((
                    presentation_id,
                    attempt_id,
                    envelope_id,
                    envelope_revision,
                )),
            )
            .await?;
        let text = step
            .assistant_message
            .blocks
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        if text.trim().is_empty() {
            return Err(RuntimeError::new(
                "failure explanation provider returned no text",
            ));
        }
        Ok((
            text,
            step.model,
            step.models_used,
            terminal_attempt_id.unwrap_or_else(|| attempt_id.to_string()),
        ))
    }

    /// Synthesize one root answer from the complete, Runtime-verified results
    /// of a collaboration Program. Team carriers are evidence inputs, never a
    /// user-facing answer: the root model must reconcile them, distinguish
    /// fact from inference, and close the original objective without exposing
    /// Runtime transport syntax.
    pub(crate) async fn synthesize_collaboration_answer(
        &mut self,
        objective: &str,
        evidence_carrier: &str,
        language: &str,
        validation_feedback: &[String],
        intermediate: bool,
        presentation_id: &str,
        attempt_id: &str,
        envelope_id: &str,
        envelope_revision: u64,
    ) -> Result<(String, Option<String>, Vec<String>, String), RuntimeError> {
        let feedback = if validation_feedback.is_empty() {
            String::new()
        } else {
            format!(
                "\n\nThe previous draft was rejected by the deterministic quality gate. Repair every finding:\n- {}",
                validation_feedback.join("\n- ")
            )
        };
        let stage_instruction = if intermediate {
            "Produce a lossless intermediate synthesis for a later root merge. Preserve every material finding, source path, disagreement, risk, unresolved item and scope limitation from these complete inputs. Do not claim to be the overall final answer."
        } else {
            "Produce the final answer now."
        };
        let messages: HistoryView = vec![ConversationMessage::user_text(format!(
            "Original task objective:\n{objective}\n\nComplete verified collaboration evidence carrier:\n{evidence_carrier}{feedback}\n\n{stage_instruction}"
        ))]
        .into();
        let (step, terminal_attempt_id) = self
            .execute_terminal_provider_step(
                objective,
                &format!(
                    "## Root collaboration synthesis\n\
                     You are the root synthesis authority for a completed multi-Team Program. Write a \
                     high-quality, comprehensive final answer in the SAME LANGUAGE as the original \
                     request (detected language: {language}). Treat every Team result as evidence, \
                     not as prose to concatenate. Reconcile overlaps and disagreements; organize the \
                     answer around the user's requested decisions and deliverables; explicitly separate \
                     verified facts, source-grounded inference, and work or simulations that were not \
                     actually executed. Preserve concrete source paths and material risks. Explain \
                     concurrency waves, dependencies, bottlenecks, failure modes, capacity boundaries, \
                     and the scale recommendation when the objective requests them. Never emit Runtime \
                     Team ids, evidence-bundle headers, delivery counters, JSON transport wrappers, \
                     `[truncated]`, tool calls, or promises to continue. Do not claim that unresolved \
                     work is empty when any Team reports unresolved items. A verified \
                     `root_runtime_attestation` is authoritative only for aggregate execution and receipt \
                     satisfaction: use it to resolve role-local visibility gaps, but never let it erase \
                     semantic risks or unresolved items in Team evidence. Preserve every phrase that the \
                     original objective explicitly requires verbatim. End with a complete conclusion; \
                     never stop mid-sentence. Do not shorten or omit required content merely to satisfy \
                     an arbitrary character target. When this is an intermediate synthesis layer, \
                     preserve all material evidence for the next layer instead of pretending to \
                     conclude the whole Program.",
                ),
                messages,
                "root collaboration synthesis exposes no executable tools",
                (!intermediate).then_some((
                    presentation_id,
                    attempt_id,
                    envelope_id,
                    envelope_revision,
                )),
            )
            .await?;
        let text = step
            .assistant_message
            .blocks
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        if text.trim().is_empty() {
            return Err(RuntimeError::new(
                "collaboration synthesizer returned no text",
            ));
        }
        Ok((
            text,
            step.model,
            step.models_used,
            terminal_attempt_id.unwrap_or_else(|| attempt_id.to_string()),
        ))
    }

    pub(super) async fn acquire_provider_capacity(
        &self,
        model: &str,
        request: &ApiRequest,
    ) -> Result<
        (
            Option<crate::execution_core::graph::ExecutionResourceLease>,
            Duration,
        ),
        RuntimeError,
    > {
        let started = Instant::now();
        let lease = if let Some(manager) = &self.provider_admission {
            let estimated_tokens = request
                .budget
                .fixed_input_tokens
                .saturating_add(request.budget.dynamic_input_tokens)
                .saturating_add(request.budget.protocol_overhead_tokens)
                .saturating_add(request.budget.requested_output_tokens);
            let demands = self.api_client.provider_name_for_model(model).map_or_else(
                || {
                    vec![(
                        crate::execution_core::graph::ExecutionResourceKind::Provider,
                        1,
                    )]
                },
                |provider_name| {
                    self.provider_resource_config
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .admission_demands(&provider_name, model, estimated_tokens)
                },
            );
            let admission = crate::execution_core::graph::ResourceAdmissionRequest::new(
                self.execution_service_class,
                demands,
            )
            .with_parent_class_ceiling(self.execution_service_class)
            .with_deadline_at_ms(now_ms().saturating_add(30_000))
            .with_fairness_key(format!("session:{}", self.session_id()));
            let acquire = manager.admit(admission);
            let lease = tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    return Err(RuntimeError::new(
                        "turn cancelled while waiting for provider capacity",
                    ));
                }
                decision = acquire => {
                    match decision.map_err(|error| RuntimeError::new(format!(
                        "provider capacity admission failed: {error}"
                    )))? {
                        crate::execution_core::graph::ResourceAdmissionDecision::Granted { lease, .. } => lease,
                        crate::execution_core::graph::ResourceAdmissionDecision::Deferred { wait_reason, .. }
                        | crate::execution_core::graph::ResourceAdmissionDecision::Overloaded { wait_reason, .. } => {
                            return Err(RuntimeError::new(format!(
                                "provider capacity admission did not grant: {wait_reason:?}"
                            )));
                        }
                    }
                },
            };
            Some(lease)
        } else {
            None
        };
        let queue_wait = started.elapsed();
        crate::execution_core::performance::observe_duration(
            "provider_admission_queue_ms",
            queue_wait,
        );
        self.verify_session_execution_fence(crate::SessionExecutionFencePhase::ProviderRequest)
            .await?;
        Ok((lease, queue_wait))
    }

    pub(super) fn record_provider_resource_outcome(
        &self,
        lease: Option<&crate::execution_core::graph::ExecutionResourceLease>,
        queue_wait: Duration,
        service_time: Duration,
        result_class: crate::execution_core::graph::ResourceResultClass,
    ) {
        let (Some(manager), Some(lease)) = (&self.provider_admission, lease) else {
            return;
        };
        let observation = crate::execution_core::graph::ResourceObservation::terminal(
            queue_wait,
            service_time,
            result_class,
        );
        for (kind, _) in lease.demands() {
            let _ = manager.record_observation(kind, observation);
        }
    }

    /// Run a session health probe to verify the runtime is functional after compaction.
    /// Returns Ok(()) if healthy, Err if the session appears broken.
    /// Execute exactly one provider request and translate its response into a
    /// typed graph intent.
    #[cfg(test)]
    pub(crate) async fn execute_model_step(
        &mut self,
        user_input: &str,
        first_step: bool,
    ) -> Result<ModelStepResult, RuntimeError> {
        self.execute_model_step_with_early_dispatch(user_input, first_step, None, false)
            .await
    }

    /// Execute one Provider step while optionally dispatching completed,
    /// descriptor-proven read-only tool items through the graph-owned early
    /// lane. The dispatcher is supplied by the graph Host; this method never
    /// creates a second tool executor or policy owner.
    pub(crate) async fn execute_model_step_with_early_dispatch(
        &mut self,
        user_input: &str,
        first_step: bool,
        early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
        provider_retry_fenced: bool,
    ) -> Result<ModelStepResult, RuntimeError> {
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new(
                "turn cancelled before provider execution",
            ));
        }
        let started_at = Instant::now();
        if first_step {
            self.record_turn_started(user_input);
            self.record_context_event("user_input", "user", &preview_chars(user_input, 200), 8);
            self.session
                .write()
                .await
                .push_user_text(user_input.to_string())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            self.record_message_event(
                &ConversationMessage::user_text(user_input.to_string()),
                self.session_head().await.message_count.wrapping_sub(1),
            );
            self.activate_skills_for_turn(user_input).await?;
        }

        if self.active_turn_strategy().is_none() {
            return Err(RuntimeError::new(
                "model execution requires the Host-admitted turn strategy owner",
            ));
        }
        let decision = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("turn strategy was not admitted"))?;
        if !decision.executable {
            return Err(RuntimeError::new(format!(
                "runtime strategy is not executable: {}",
                decision.blocked_reasons.join("; ")
            )));
        }

        let text_only_response = self.next_model_text_only.swap(false, Ordering::SeqCst);
        let one_shot_tool_allowlist = self
            .next_model_tool_allowlist
            .lock()
            .ok()
            .and_then(|mut allowlist| allowlist.take());
        let one_shot_tool_required = self.next_model_tool_required.swap(false, Ordering::SeqCst);
        let one_shot_required_tool_name = self
            .next_model_required_tool_name
            .lock()
            .ok()
            .and_then(|mut tool_name| tool_name.take());
        let tool_activation_ceiling = one_shot_tool_allowlist.clone();
        let one_shot_reasoning_effort = self
            .next_model_reasoning_effort
            .lock()
            .ok()
            .and_then(|mut effort| effort.take());
        let explicitly_forbids_tool_use =
            harness_contract::strategy::prompt_explicitly_forbids_tool_use(user_input);
        let discovery_activation_notice = if text_only_response || explicitly_forbids_tool_use {
            None
        } else {
            self.next_model_tool_activation_notice
                .lock()
                .ok()
                .and_then(|notice| notice.clone())
        };
        let discovery = self.discover_tools_with_metrics();
        let available_tools = discovery
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect::<Vec<_>>();
        let mut exposure = if first_step {
            tool_exposure_for_catalog(
                &discovery,
                contract_permission_mode(self.permission_policy.active_mode()),
            )
        } else {
            self.tool_exposure_state
                .lock()
                .ok()
                .and_then(|state| state.clone())
                .filter(|state| state.catalog_revision == discovery.catalog_revision)
                .unwrap_or_else(|| {
                    tool_exposure_for_catalog(
                        &discovery,
                        contract_permission_mode(self.permission_policy.active_mode()),
                    )
                })
        };
        if first_step {
            self.seed_recent_session_tools(&mut exposure, &discovery)
                .await;
        }
        let active_skill_tool_refs = self
            .active_skill_tool_refs
            .lock()
            .map(|tool_refs| tool_refs.clone())
            .unwrap_or_default();
        if !active_skill_tool_refs.is_empty() {
            let mut skill_discovery = discovery.clone();
            skill_discovery.activation_candidates = active_skill_tool_refs.into_iter().collect();
            let allowed_ids = exposure
                .bootstrap
                .iter()
                .chain(exposure.active.iter())
                .chain(exposure.deferred.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            let policy = ToolExposurePolicy {
                allowed_ids,
                maximum_permission: contract_permission_mode(self.permission_policy.active_mode()),
                supports_dynamic_exposure: true,
            };
            let activation = ToolExposurePlanner.activate(&mut exposure, &skill_discovery, &policy);
            tracing::info!(
                activated = ?activation.activated_ids().collect::<Vec<_>>(),
                "runtime Skill tool references applied to the current provider request"
            );
        }
        let collaboration_obligation = self
            .active_turn_strategy()
            .and_then(|state| state.decision.collaboration_obligation);
        if let Some(obligation) = collaboration_obligation {
            for tool in [
                "runtime_capabilities",
                harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
            ] {
                exposure.active.insert(tool.to_string());
                exposure.deferred.remove(tool);
            }
            exposure.reason =
                "collaboration execution obligation forces orchestration tools active".to_string();
            tracing::info!(
                team_required = true,
                obligation_source = ?obligation.source,
                minimum_team_count = obligation.minimum_team_count,
                active = ?exposure.active,
                "collaboration execution obligation forced orchestration exposure"
            );
        }
        let one_shot_tool_overlay =
            one_shot_tool_allowlist.is_some() || discovery_activation_notice.is_some();
        let mut exposure = if text_only_response || explicitly_forbids_tool_use {
            ToolExposureState {
                catalog_revision: exposure.catalog_revision,
                bootstrap: Default::default(),
                active: Default::default(),
                deferred: available_tools.iter().cloned().collect(),
                reason: if text_only_response {
                    "governed low-novelty checkpoint requires a text-only conclusion".to_string()
                } else {
                    "user explicitly prohibited tool calls for this request".to_string()
                },
                revision: exposure.revision.saturating_add(1),
                fallback_full: false,
            }
        } else if let Some(allowlist) = one_shot_tool_allowlist {
            let eligible_tools = exposure
                .bootstrap
                .iter()
                .chain(exposure.active.iter())
                .chain(exposure.deferred.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            let active = eligible_tools
                .iter()
                .filter(|tool_id| allowlist.contains(*tool_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            let deferred = available_tools
                .iter()
                .filter(|tool_id| !active.contains(*tool_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            ToolExposureState {
                catalog_revision: exposure.catalog_revision,
                bootstrap: Default::default(),
                active,
                deferred,
                reason:
                    "governed focus checkpoint restricts the next action to required mutation tools"
                        .to_string(),
                revision: exposure.revision.saturating_add(1),
                fallback_full: false,
            }
        } else if discovery_activation_notice.is_some() {
            exposure.bootstrap.remove("tool_search");
            exposure.active.remove("tool_search");
            exposure.deferred.insert("tool_search".to_string());
            exposure.reason =
                "post-discovery execution handoff; tool_search is paused for one request"
                    .to_string();
            exposure.revision = exposure.revision.saturating_add(1);
            exposure
        } else {
            exposure
        };
        exposure.revision = self
            .tool_exposure_revision
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        // A text-only checkpoint is a one-request overlay. Keep the normal
        // catalog state for discovery/projection, while still sending an
        // explicit empty schema set for this provider request.
        if !text_only_response && !explicitly_forbids_tool_use && !one_shot_tool_overlay {
            if let Ok(mut state) = self.tool_exposure_state.lock() {
                *state = Some(exposure.clone());
            }
        }
        let exposure_projection = exposure.projection(0);
        let exposed_tool_ids = exposure_projection
            .bootstrap_ids
            .iter()
            .chain(exposure_projection.active_ids.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        self.api_client.configure_tool_exposure(exposure_projection);
        self.api_client
            .configure_tool_choice(one_shot_tool_required, one_shot_required_tool_name);

        // Tool schemas are part of the request budget. Read their inventory
        // only after Runtime has made the exposure decision.
        let inventory = self.api_client.context_inventory();
        let model_candidates = self.model_candidates_for_turn(user_input);
        let collection_budget = model_candidates
            .iter()
            .map(|model| {
                let window = u64::from(self.context_window_for_model(model));
                let output = u64::from(provider_output_budget_hint(
                    model,
                    window as u32,
                    self.provider_max_output_override,
                ));
                let protocol = 128u64
                    .saturating_add(u64::from(inventory.tool_count as u32).saturating_mul(12));
                let safety = (window / 100).clamp(128, 2_048);
                window
                    .saturating_sub(output)
                    .saturating_sub(protocol)
                    .saturating_sub(safety)
            })
            .max()
            .unwrap_or_else(|| self.context_budget_tokens());
        // Collect memory/knowledge/fact/matrix data once against the largest
        // physically usable input window. The per-attempt packer below still
        // applies each model's hard cap, schema and history. If preflight
        // compacts the transcript, this snapshot is rebuilt before dispatch.
        let mut one_shot_context_items = self.take_next_model_context_items();
        let context_select_started = Instant::now();
        let mut prompt = self
            .prepare_reality_context_with_budget_and_items(
                user_input,
                collection_budget,
                one_shot_context_items.clone(),
            )
            .await;
        crate::execution_core::performance::observe_duration(
            "context_select_ms",
            context_select_started.elapsed(),
        );
        let evidence = crate::evidence_planner::plan_evidence_with_understanding(
            user_input,
            &decision.strategy.understanding,
        );
        let apply_runtime_controls = |prompt: &mut PromptAssembly| {
            prompt.push_trusted_system(crate::evidence_planner::evidence_plan_prompt(&evidence));
            prompt.push_trusted_system(
                crate::execution_core::runtime_execution_guidance_prompt_with_tool_exposure(
                    &decision,
                    Some(&exposure.projection(0)),
                ),
            );
            if let Some(activated_ids) = discovery_activation_notice.as_ref() {
                prompt.push_trusted_system(format!(
                    "## Tool discovery handoff\nThis is the immediate automatic continuation of the same user turn. tool_search already completed successfully and is intentionally unavailable for this request. Newly activated native function schemas: [{}]. Continue the original task now by invoking the relevant activated schema directly when evidence or action is still required. Do not ask the user to resend the request and do not claim that a new user turn is needed.",
                    activated_ids.iter().cloned().collect::<Vec<_>>().join(", ")
                ));
            }
            if text_only_response {
                prompt.push_trusted_system(
                    "## Terminal response boundary\nThis request is a text-only terminal checkpoint. The executable tool set for this request is empty, regardless of any earlier capability inventory or historical tool receipts in the context. Do not emit native function calls, simulated tool markup, JSON commands, new plans, or more work. Use only retained evidence receipts to produce the best final answer now. State unresolved facts explicitly instead of performing another search.".to_string(),
                );
            }
            if explicitly_forbids_tool_use {
                prompt.push_trusted_system(
                    "## User-selected execution boundary\nThe user explicitly prohibited tool calls for this request. The executable tool set is empty. Answer from the supplied prompt and retained conversation evidence only; do not emit native function calls, simulated tool markup, or JSON commands.".to_string(),
                );
            }
        };
        apply_runtime_controls(&mut prompt);
        self.record_runtime_policy_decision(&decision, self.session_head().await.message_count)
            .await;
        self.record_context_event(
            "evidence_plan",
            "runtime",
            &format!("{:?}: {}", evidence.mode, evidence.reason),
            7,
        );
        self.record_context_event(
            "execution_decision",
            "runtime",
            &format!(
                "{}: {:?}",
                decision.pattern().as_str(),
                decision.recommended_actions
            ),
            8,
        );
        let request_clone_started = Instant::now();
        let mut request_messages = self.session.read().await.messages_view();
        crate::execution_core::performance::observe_duration(
            "request_history_clone_ms",
            request_clone_started.elapsed(),
        );
        crate::execution_core::performance::observe_bytes(
            "clone_bytes",
            request_messages.weight().bytes,
        );

        // Compression is a request-preflight recovery path, never a fixed
        // transcript-ratio timer. Optional packets have already been allowed
        // to compete for hard capacity; compact only when no configured
        // candidate can carry the fixed history plus required continuity.
        let no_candidate_can_fit = model_candidates.iter().all(|model| {
            self.pack_provider_attempt(&prompt, &request_messages, model, inventory)
                .is_err()
        });
        if no_candidate_can_fit {
            if let Some(turn_id) = self.session_input_stream.active_turn_id() {
                let consumed = self
                    .consume_runtime_input_records(&turn_id, TurnInputCheckpoint::BeforeCompaction);
                one_shot_context_items.extend(crate::turn_inbox::checkpoint_context_items(
                    TurnInputCheckpoint::BeforeCompaction,
                    &consumed,
                ));
            }
            let compaction = self
                .compact_session_with_checkpoint(self.compaction_config_for_session(1))
                .await?;
            if compaction.is_none() {
                return Err(RuntimeError::new(
                    "all provider candidates reject the required request context and no semantic compaction boundary is available",
                ));
            }
            request_messages = self.session.read().await.messages_view();
            prompt = self
                .prepare_reality_context_with_budget_and_items(
                    user_input,
                    collection_budget,
                    one_shot_context_items,
                )
                .await;
            apply_runtime_controls(&mut prompt);
            self.record_context_event(
                "context_preflight_compaction",
                "runtime",
                "all provider candidates required semantic compaction before request dispatch",
                9,
            );
            if let Ok(mut preflight_compaction) = self.turn_preflight_compaction.lock() {
                *preflight_compaction = compaction;
            }
        }
        if let Some(turn_id) = self.session_input_stream.active_turn_id() {
            self.consume_runtime_inputs_at_checkpoint(
                &turn_id,
                TurnInputCheckpoint::BeforeProviderRequest,
                &mut prompt,
            );
        }
        if knowledge_hard_gate_active(&prompt.trusted_system) {
            return Err(RuntimeError::new(
                "knowledge compliance hard gate blocked turn",
            ));
        }

        let mut last_error = None;
        let mut candidates = VecDeque::from(model_candidates);
        let mut models_tried = Vec::new();
        // One retry per model is sufficient: calibration only accepts a
        // smaller explicit provider limit, so the second request is strictly
        // smaller. Repeating beyond that would mask malformed provider errors.
        let mut calibration_retries = BTreeSet::new();
        let mut provider_retries = BTreeMap::<String, u8>::new();
        let mut provider_attempt_sequence = 0_u32;
        while let Some((model, provider_account_key)) =
            self.next_available_provider_candidate(&mut candidates)
        {
            let materialized =
                self.pack_timed_provider_attempt(&prompt, &request_messages, &model, inventory);
            let mut request = match materialized {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!(
                        model,
                        error = %error,
                        "provider request preflight rejected model candidate"
                    );
                    last_error = Some(error);
                    continue;
                }
            };
            request.reasoning_effort_override = one_shot_reasoning_effort.clone();
            let mut token_reservations = match ProviderTokenReservationSet::acquire(
                self.evaluation_provider_token_lease.as_ref(),
                self.delegated_provider_budget.as_ref(),
                &model,
                &mut request,
            ) {
                Ok(reservations) => reservations,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            if !models_tried.contains(&model) {
                models_tried.push(model.clone());
            }
            if let Some(cowd) = &self.cowd_bus {
                cowd.emit(crate::cowd_event::CowdEvent::ProviderAttempt {
                    model: model.clone(),
                    models_tried: models_tried.clone(),
                    context_window_tokens: request.budget.context_window_tokens,
                    context_window_source: request.budget.context_window_source.clone(),
                    packed_input_tokens: request
                        .budget
                        .fixed_input_tokens
                        .saturating_add(request.budget.dynamic_input_tokens)
                        .saturating_add(request.budget.protocol_overhead_tokens),
                });
            }
            let request_sequence = self.session_head().await.message_count;
            provider_attempt_sequence = provider_attempt_sequence.saturating_add(1);
            request.provider_evidence_context = Some(crate::ProviderRequestEvidenceContext {
                session_id: self.session_id().to_string(),
                request_sequence,
                request_compiler_cache_hit: request.request_compiler_cache_hit,
                budget: request.budget.clone(),
                attempt: provider_attempt_sequence,
            });
            // Inspect the immutable, fully packed request immediately before
            // transport dispatch. Generation alone is not observation; these
            // candidates are promoted only after a valid response is committed.
            let packed_model_observations = self.packed_model_observation_candidates(
                &request,
                request_sequence,
                provider_attempt_sequence,
            )?;
            self.record_provider_context_request(
                &request,
                request_sequence,
                inventory,
                self.api_client.tool_schema_cache_stats(),
            );
            let attempt_budget = self.runtime_budget_plan_for_candidates(&[model.clone()]);
            let transport_policy = provider_transport_policy(
                attempt_budget.model_context_window.min(u64::from(u32::MAX)) as u32,
                &request,
            );
            let idle_timeout = transport_policy.idle_timeout;
            let heartbeat_grace = transport_policy.heartbeat_grace;
            let cancellation = self.cancellation_token.clone();
            let (provider_lease, provider_queue_wait) =
                self.acquire_provider_capacity(&model, &request).await?;
            let provider_started = Instant::now();
            let stream_started = Instant::now();
            let reducer = ModelStreamReducer::new(
                self.cowd_bus.clone(),
                self.runtime_event_store.clone(),
                self.session_id().to_string(),
            );
            token_reservations.mark_dispatched();
            let ApiClientStream {
                events,
                transport_activity,
            } = self.api_client.stream_with_transport_activity(request);
            let stream_run = consume_provider_stream_with_activity(
                events,
                cancellation,
                Some(ProviderStreamTimeoutPolicy {
                    idle: idle_timeout,
                    heartbeat_grace,
                }),
                reducer,
                early_dispatcher.clone(),
                transport_activity,
            )
            .await;
            let resource_result_class = stream_run.resource_result_class;
            let CollectedProviderStream {
                text,
                public_reasoning,
                private_reasoning,
                signature,
                mut calls,
                usage,
                effective_provider_identity,
                first_event_at,
                first_text_at,
                early_tool_receipts,
                early_tool_deferrals,
                response_completed_at_ms,
            } = stream_run.collected;
            if let Some(first_text_at) = first_text_at {
                crate::execution_core::performance::observe_duration(
                    "actual_first_delta_ms",
                    first_text_at.saturating_duration_since(provider_started),
                );
            }
            let effective_model = effective_provider_identity
                .as_ref()
                .map(|identity| identity.model.clone());
            if let Some(identity) = effective_provider_identity {
                *self
                    .active_provider_identity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(identity);
            }
            let signature = (!signature.is_empty()).then_some(signature);
            crate::execution_core::performance::observe_duration(
                "provider_stream_ms",
                stream_started.elapsed(),
            );
            crate::execution_core::performance::observe_duration(
                "provider_service_ms",
                provider_started.elapsed(),
            );
            crate::execution_core::performance::observe_count(
                "provider_input_tokens",
                u64::from(usage.input_tokens),
            );
            crate::execution_core::performance::observe_count(
                "provider_output_tokens",
                u64::from(usage.output_tokens),
            );
            let service_ms = provider_started.elapsed().as_millis().max(1) as u64;
            crate::execution_core::performance::observe_count(
                "provider_output_tokens_per_second",
                u64::from(usage.output_tokens).saturating_mul(1_000) / service_ms,
            );
            Self::observe_provider_downstream_overload(resource_result_class);
            self.record_provider_resource_outcome(
                provider_lease.as_ref(),
                provider_queue_wait,
                provider_started.elapsed(),
                resource_result_class,
            );
            drop(provider_lease);
            token_reservations
                .reconcile(usage)
                .map_err(|error| error.with_effect_receipts(early_tool_receipts.clone()))?;
            if let Some(error) = stream_run.failure {
                let error = error.with_provider_account_key(provider_account_key.clone());
                if provider_retry_is_fenced(provider_retry_fenced, early_tool_receipts.len()) {
                    return Err(if early_tool_receipts.is_empty() {
                        error
                    } else {
                        error.with_effect_receipts(early_tool_receipts)
                    });
                }
                if error.is_provider_tool_protocol_failure() {
                    return Err(error);
                }
                if error.provider_failure_scope()
                    == model_protocol::provider_failure::ProviderFailureScope::Account
                {
                    self.mark_provider_account_unavailable(&error);
                    last_error = Some(error);
                    continue;
                }
                if let Some(observed_limit) = error.provider_context_window_limit() {
                    if calibration_retries.insert(model.clone())
                        && self.calibrate_model_context_window(&model, observed_limit)
                    {
                        tracing::info!(
                            model,
                            observed_limit,
                            "provider context window calibrated; retrying candidate once"
                        );
                        candidates.push_front(model);
                        continue;
                    }
                }
                let retries = provider_retries.entry(model.clone()).or_default();
                if error.provider_retryable() && *retries < MAX_RUNTIME_PROVIDER_RETRIES_PER_MODEL {
                    *retries = retries.saturating_add(1);
                    let retry_after = error
                        .provider_retry_after()
                        .unwrap_or(DEFAULT_RUNTIME_PROVIDER_RETRY_DELAY);
                    crate::execution_core::performance::observe_duration(
                        "provider_retry_after_ms",
                        retry_after,
                    );
                    tokio::select! {
                        () = self.cancellation_token.cancelled() => {
                            return Err(RuntimeError::new(
                                "turn cancelled during provider retry-after delay",
                            ));
                        }
                        () = tokio::time::sleep(retry_after) => {}
                    }
                    candidates.push_front(model);
                    continue;
                }
                last_error = Some(error);
                continue;
            }

            canonicalize_model_tool_names(&mut calls, self.tool_executor.as_ref());
            let requested_tool_call_count = calls.len();
            let unexposed_tool_names = unexposed_model_tool_names(&calls, &exposed_tool_ids);
            if !unexposed_tool_names.is_empty() {
                let activation_candidates = unexposed_tool_names
                    .iter()
                    .filter(|name| {
                        tool_activation_ceiling
                            .as_ref()
                            .is_none_or(|allowlist| allowlist.contains(*name))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let denied_by_overlay = unexposed_tool_names
                    .iter()
                    .filter(|name| {
                        tool_activation_ceiling
                            .as_ref()
                            .is_some_and(|allowlist| !allowlist.contains(*name))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let activated =
                    self.activate_deferred_tool_calls(&activation_candidates, &discovery);
                // Provider transports validate framing, while Runtime owns
                // this request's exposure lease. When every requested tool is
                // a known healthy deferred tool, Runtime has already parsed
                // the current frame and can execute it under the just-bound
                // lease. Dropping that frame for a model retry causes managed
                // Agents to exhaust protocol recovery after their first
                // source receipt, before they can meet a required escalation.
                // Unknown, unhealthy, or overlay-denied names still fail
                // closed before any assistant transcript is published.
                self.reconcile_provider_context_usage(usage);
                self.usage_tracker.record(usage);
                if let Some(callback) = &self.tool_callback {
                    callback.on_usage(&usage);
                }
                if denied_by_overlay.is_empty() && activated.len() == unexposed_tool_names.len() {
                    // Fall through and execute the parsed calls. Activation
                    // remains durable for subsequent provider requests.
                } else {
                    return Err(
                        RuntimeError::with_provider_failure_metadata(
                            format!(
                                "tool_protocol_violation: provider requested unknown, unavailable, or unauthorized tool names outside this request's exposure lease: [{}]{}",
                                unexposed_tool_names.join(", "),
                                (!denied_by_overlay.is_empty()).then(|| format!(
                                    "; governed one-request allowlist rejected [{}]",
                                    denied_by_overlay.join(", ")
                                )).unwrap_or_default()
                            ),
                            None,
                            true,
                            crate::execution_core::graph::ResourceResultClass::Failed,
                        )
                        .with_provider_usage(usage)
                        .with_effect_receipts(early_tool_receipts),
                    );
                }
            }
            if let Some((call, error)) = calls.iter().find_map(|call| {
                self.tool_executor
                    .validate_tool_input(&call.name, &call.input)
                    .err()
                    .map(|error| (call, error))
            }) {
                // Tool arguments are provider protocol, not an executable
                // request. Reject them before transcript publication and
                // permission negotiation so malformed calls cannot create an
                // invisible approval wait. Host applies the existing bounded
                // single recovery and then fails closed on repetition.
                self.reconcile_provider_context_usage(usage);
                self.usage_tracker.record(usage);
                if let Some(callback) = &self.tool_callback {
                    callback.on_usage(&usage);
                }
                return Err(
                    RuntimeError::with_provider_failure_metadata(
                        format!(
                            "tool_protocol_violation: provider supplied invalid arguments for exposed tool `{}`: {}",
                            call.name, error
                        ),
                        None,
                        true,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    )
                    .with_provider_usage(usage)
                    .with_effect_receipts(early_tool_receipts),
                );
            }
            if discovery_activation_notice.is_some() {
                if let Ok(mut notice) = self.next_model_tool_activation_notice.lock() {
                    *notice = None;
                }
            }
            let mut blocks = Vec::new();
            if !public_reasoning.is_empty() {
                blocks.push(ContentBlock::ReasoningSummary {
                    text: public_reasoning,
                });
            }
            if !private_reasoning.is_empty() || signature.is_some() {
                blocks.push(ContentBlock::Thinking {
                    thinking: private_reasoning,
                    signature,
                });
            }
            blocks.push(ContentBlock::Text { text: text.clone() });
            for call in &calls {
                blocks.push(ContentBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: call.input.clone(),
                });
            }
            let assistant_message = ConversationMessage {
                role: crate::session::MessageRole::Assistant,
                blocks,
                usage: Some(usage),
            };
            self.session
                .write()
                .await
                .push_message(assistant_message.clone())
                .map_err(|error| {
                    RuntimeError::new(error.to_string())
                        .with_effect_receipts(early_tool_receipts.clone())
                })?;
            self.record_message_event(
                &assistant_message,
                self.session_head().await.message_count.wrapping_sub(1),
            );
            self.reconcile_provider_context_usage(usage);
            self.usage_tracker.record(usage);
            if let Some(callback) = &self.tool_callback {
                callback.on_usage(&usage);
            }
            self.record_assistant_iteration(
                self.session_head().await.message_count,
                &assistant_message,
                requested_tool_call_count,
            );
            let classified = classify_model_step_intent(text, calls);
            let intent = apply_explicit_team_requirement(
                self.explicit_team_escalation,
                user_input,
                first_step,
                &decision,
                classified,
            );
            let effective_model = effective_model.or(Some(model));
            if let Some(model) = effective_model.as_ref() {
                if !models_tried.contains(model) {
                    models_tried.push(model.clone());
                }
                self.confirm_model_observations(packed_model_observations, model)?;
            }
            self.consume_active_runtime_inputs_for_next_step(
                TurnInputCheckpoint::AfterProviderResponse,
            );
            return Ok(ModelStepResult {
                intent,
                assistant_message,
                usage,
                // Preserve the model that actually produced the provider stream,
                // not merely Runtime's preferred candidate.
                model: effective_model,
                models_used: models_tried.clone(),
                first_token_latency_ms: first_event_at.map(|first| {
                    u64::try_from(first.saturating_duration_since(stream_started).as_millis())
                        .unwrap_or(u64::MAX)
                }),
                active_stream_duration_ms: first_event_at.map(|first| millis_since(first).max(1)),
                wall_duration_ms: millis_since(started_at).max(1),
                early_tool_receipts,
                early_tool_deferrals,
                response_completed_at_ms,
                text_only_response,
            });
        }
        Err(last_error.unwrap_or_else(|| RuntimeError::new("all provider fallbacks exhausted")))
    }
}
