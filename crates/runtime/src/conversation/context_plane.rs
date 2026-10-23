//! Context assembly, activation, memory projection, and budget governance.

use super::*;

fn native_root_continuation_matches(
    source: &harness_contract::projection::RuntimeActivityBinding,
    consumer: &harness_contract::projection::RuntimeActivityBinding,
    session_id: &str,
    execution_id: &str,
    turn_id: &str,
) -> bool {
    source.validate().is_ok()
        && consumer.validate().is_ok()
        && source.agent_run_id.is_none()
        && consumer.agent_run_id.is_none()
        && source.root_execution_id == execution_id
        && consumer.root_execution_id == execution_id
        && source.session_id == session_id
        && consumer.session_id == session_id
        && source.turn_id == turn_id
        && consumer.turn_id == turn_id
        && source.root_task_id == consumer.root_task_id
        && source.task_id == consumer.task_id
        && source.generation == consumer.generation
}

fn native_root_activity_is_original(
    store: &crate::RuntimeEventStore,
    binding: &harness_contract::projection::RuntimeActivityBinding,
) -> Result<bool, RuntimeError> {
    let Some(node_id) = binding
        .node_id
        .as_deref()
        .filter(|node_id| !node_id.is_empty())
    else {
        return Ok(false);
    };
    let stream = format!("{}:node:{node_id}", binding.root_execution_id);
    let key = format!(
        "{}:{node_id}:{}",
        binding.root_execution_id, binding.revision
    );
    let Some(original) = store
        .event_by_idempotency_key(&stream, &key)
        .map_err(|error| RuntimeError::new(error.to_string()))?
    else {
        return Ok(false);
    };
    Ok(original.activity_binding().as_ref() == Some(binding)
        && original.stream_id == stream
        && original.kind == "execution_node.transitioned"
        && original.scope == RuntimeEventScope::ExecutionNode
        && original.actor.as_deref() == Some("execution_commit_service")
        && original.status.as_deref() == Some("running")
        && original.schema_version == 1
        && original.payload["to"] == "running")
}

#[cfg(test)]
tokio::task_local! {
    static NATIVE_DELIVERY_CONFIRMATION_FAILURE: bool;
    static NATIVE_DELIVERY_CONFIRMATION_SOURCE_RACE: Arc<dyn Fn() + Send + Sync>;
}

#[cfg(test)]
pub(crate) async fn with_native_delivery_confirmation_failure<Output>(
    operation: impl std::future::Future<Output = Output>,
) -> Output {
    NATIVE_DELIVERY_CONFIRMATION_FAILURE
        .scope(true, operation)
        .await
}

#[cfg(test)]
pub(crate) async fn with_native_delivery_confirmation_source_race<Output>(
    after_cut: Arc<dyn Fn() + Send + Sync>,
    operation: impl std::future::Future<Output = Output>,
) -> Output {
    NATIVE_DELIVERY_CONFIRMATION_SOURCE_RACE
        .scope(after_cut, operation)
        .await
}

/// Placeholder the envelope carries where the per-request clock section belongs.
///
/// A sentinel rather than the clock text itself: the envelope is durable, and
/// embedding a wall-clock string in it would make a stored envelope disagree with
/// the request it was built for. The rendering site substitutes the live section.
pub(crate) const VOLATILE_CLOCK_SENTINEL: &str = "\u{1}cowd-volatile-clock\u{1}";

/// Identity of the turn-position packet Runtime re-renders on every request.
///
/// `#GoalState` carries `elapsed_ms` and `rounds`, both of which change on every
/// provider request by construction (see `agentic_turn_framing`). As a normal
/// context packet it sat among content that is byte-stable across requests, so it
/// forked the prefix before every stable segment behind it could be reused.
///
/// Routing on the packet's own identity, not its prose: the text is free to be
/// reworded without silently changing where it lands.
pub(crate) const GOAL_STATE_PACKET_PREFIX: &str = "goal-state:";

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    /// Return a human-readable description of memory subsystem health.
    /// `None` when healthy; `Some(msg)` when degraded or unavailable.
    pub fn memory_status(&self) -> Option<&str> {
        self.memory_status.as_deref()
    }

    /// Return the current project lifecycle phase.
    pub fn phase(&self) -> &str {
        &self.project_phase
    }

    /// Return the latest context envelope assembled for an actual model turn.
    pub fn last_context_envelope(&self) -> Option<ContextEnvelope> {
        self.last_context_envelope
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Return the latest context governance report emitted by a completed turn.
    pub fn last_context_turn_report(&self) -> Option<ContextTurnReport> {
        self.last_context_turn_report
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Return the active context profile used for the next envelope.
    pub fn context_profile(&self) -> ContextProfile {
        self.context_profile
            .lock()
            .map(|guard| *guard)
            .unwrap_or(ContextProfile::MainTurn)
    }

    /// Set the active context profile used for subsequent envelope assembly.
    pub fn set_context_profile(&self, profile: ContextProfile) {
        if let Ok(mut guard) = self.context_profile.lock() {
            *guard = profile;
        }
    }

    pub(crate) fn set_immutable_user_prefix(&self, messages: Vec<String>) {
        if let Ok(mut guard) = self.immutable_user_prefix.lock() {
            *guard = messages
                .into_iter()
                .filter(|message| !message.trim().is_empty())
                .collect();
        }
    }

    fn immutable_user_prefix(&self) -> Vec<String> {
        self.immutable_user_prefix
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_cache_cohort_user_prefix(&self, messages: Vec<String>) {
        if let Ok(mut guard) = self.cache_cohort_user_prefix.lock() {
            *guard = messages
                .into_iter()
                .filter(|message| !message.trim().is_empty())
                .collect();
        }
    }

    fn cache_cohort_user_prefix(&self) -> Vec<String> {
        self.cache_cohort_user_prefix
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Bind an absolute model-step ceiling supplied by the Runtime owner.
    /// This is not model-visible prompt text and cannot be raised by a
    /// delegated provider response.
    pub fn set_model_step_limit_override(&self, limit: usize) {
        self.model_step_limit_override
            .store(limit.max(1), Ordering::SeqCst);
    }

    /// Return the Runtime-issued model-step ceiling, if one is active.
    #[must_use]
    pub fn model_step_limit_override(&self) -> Option<usize> {
        match self.model_step_limit_override.load(Ordering::SeqCst) {
            0 => None,
            limit => Some(limit),
        }
    }

    /// Bind the validated Focus acceptance/novelty policy for a delegated
    /// child. The values are Runtime-owned and cannot be changed by provider
    /// output or model-visible prompt text.
    pub fn set_delegated_focus_policy(
        &self,
        novelty_target_bp: u16,
        acceptance_scopes: Vec<String>,
        required_output_fields: Vec<String>,
    ) {
        self.delegated_focus_novelty_target_bp
            .store(u64::from(novelty_target_bp.min(10_000)), Ordering::SeqCst);
        if let Ok(mut guard) = self.delegated_focus_acceptance_scopes.lock() {
            *guard = acceptance_scopes;
        }
        if let Ok(mut guard) = self.delegated_focus_required_output_fields.lock() {
            *guard = required_output_fields;
        }
    }

    #[must_use]
    pub fn delegated_focus_policy(&self) -> (u16, Vec<String>, Vec<String>) {
        (
            u16::try_from(
                self.delegated_focus_novelty_target_bp
                    .load(Ordering::SeqCst),
            )
            .unwrap_or(10_000),
            self.delegated_focus_acceptance_scopes
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default(),
            self.delegated_focus_required_output_fields
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default(),
        )
    }

    /// Replace runtime-owned context supplied by orchestration layers.
    pub fn set_external_context_items(&self, items: Vec<ContextItem>) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            *guard = items;
        }
    }

    /// Add one runtime-owned context item supplied by orchestration layers.
    pub fn push_external_context_item(&self, item: ContextItem) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            guard.push(item);
        }
    }

    /// Add a checkpoint-owned instruction to the next provider request only.
    /// This is intentionally distinct from persistent external context: graph
    /// recovery may steer one request without accumulating hidden prompt
    /// state across the remainder of a session.
    pub(crate) fn push_next_model_context_item(&self, item: ContextItem) {
        if let Ok(mut guard) = self.next_model_context_items.lock() {
            guard.push(item);
        }
    }

    pub(super) fn take_next_model_context_items(&self) -> Vec<ContextItem> {
        self.next_model_context_items
            .lock()
            .map(|mut guard| std::mem::take(&mut *guard))
            .unwrap_or_default()
    }

    pub(crate) fn bind_skill_task_objective(&mut self, objective: String) {
        self.skill_task_objective = Some(objective);
    }

    pub(super) async fn activate_skills_for_turn(
        &self,
        user_input: &str,
    ) -> Result<(), RuntimeError> {
        if let Ok(mut tool_refs) = self.active_skill_tool_refs.lock() {
            tool_refs.clear();
        }
        self.skill_instruction_pins
            .lock()
            .map_err(|_| RuntimeError::new("Skill pin provenance is poisoned"))?
            .clear();
        let profiles = match self.skill_instruction_source.as_ref() {
            Some(source) if !self.skill_binding_pins.is_empty() => source
                .profiles_for_pins(&self.skill_binding_pins)
                .map_err(RuntimeError::new)?,
            Some(source) => source
                .approved_profiles()
                .map_err(RuntimeError::new)?
                .unwrap_or_else(|| self.skill_profiles.clone()),
            None => self.skill_profiles.clone(),
        };
        if profiles.is_empty() {
            self.skill_instruction_pins
                .lock()
                .map_err(|_| RuntimeError::new("Skill pin provenance is poisoned"))?
                .clear();
            return Ok(());
        }

        let turn_index = self.session_head().await.message_count;
        let decision = SkillActivationEngine::activate(SkillActivationInput {
            session_id: self.session_id().to_string(),
            turn_index,
            query: self.skill_task_objective.as_ref().map_or_else(
                || user_input.to_string(),
                |objective| format!("{objective}\n\n{user_input}"),
            ),
            capability_refs: Vec::new(),
            available_profiles: profiles,
            agent_profile: self.agent_skill_profile.clone(),
        });

        // Prepare the complete requested set before publishing any prompt or
        // tool exposure. A missing later Skill must not leave ghost input for
        // the next Provider attempt.
        let mut prepared_items = Vec::new();
        let mut prepared_pins = BTreeMap::new();
        let mut prepared_tools = Vec::new();
        for activation in &decision.activations {
            if let Some(invocation) = activation.selected_invocation.as_ref() {
                let strategy = self.active_turn_strategy().ok_or_else(|| {
                    RuntimeError::new(
                        "Skill invocation requires the Host-admitted turn strategy owner",
                    )
                })?;
                let evaluation_isolated =
                    strategy.resource_snapshot.sample_source.contains("corpus=");
                let config_revision = if evaluation_isolated {
                    format!(
                        "{}:evaluation:{:016x}",
                        self.runtime_config_revision,
                        model_protocol::fingerprint::stable_hash_bytes(
                            strategy.resource_snapshot.sample_source.as_bytes(),
                        )
                    )
                } else {
                    self.runtime_config_revision.clone()
                };
                let usage_context = crate::RuntimeSkillUsageContext {
                    workspace_identity: self.checkpoint_workspace_id.clone(),
                    workload_fingerprint: StrategyWorkloadFingerprint::from_understanding(
                        &strategy.decision.strategy.understanding,
                        strategy.decision.strategy.understanding.requires_write,
                    )
                    .digest(),
                    config_revision,
                    evaluation_environment: if evaluation_isolated {
                        "harness_evaluation".to_string()
                    } else {
                        "production".to_string()
                    },
                    execution_id: format!("turn:{}", strategy.decision_id),
                    session_id: strategy.session_ref.clone(),
                    turn_id: strategy.turn_ref.clone(),
                    observed_at_ms: now_ms(),
                };
                let asset = match self.skill_instruction_source.as_ref() {
                    Some(source) => {
                        let loaded = if self.skill_binding_pins.is_empty() {
                            source.load_instruction(invocation, &usage_context).await
                        } else {
                            let pin = self
                                .skill_binding_pins
                                .iter()
                                .find(|pin| {
                                    pin.installation.skill_id == invocation.skill_id
                                        && pin.instruction_version == invocation.skill_version
                                })
                                .ok_or_else(|| {
                                    RuntimeError::new("selected Skill has no original Binding pin")
                                })?;
                            source
                                .load_pinned_instruction(pin, invocation, &usage_context)
                                .await
                        };
                        loaded.map_err(|error| {
                            RuntimeError::new(format!(
                                "runtime skill `{}` instruction page-in failed: {error}",
                                invocation.skill_id
                            ))
                        })?
                    }
                    None => {
                        return Err(RuntimeError::new(
                            "Skill instruction requires the original approval source",
                        ))
                    }
                };
                if asset.is_none()
                    && invocation.adapter == harness_contract::skill::SkillAdapterKind::PromptOnly
                {
                    return Err(RuntimeError::new(format!(
                        "runtime skill `{}` has no instruction asset for version {:?}",
                        invocation.skill_id, invocation.skill_version
                    )));
                }
                if let Some(asset) = asset {
                    if asset.skill_id != invocation.skill_id
                        || asset.version != invocation.skill_version
                    {
                        return Err(RuntimeError::new(format!(
                            "runtime skill `{}` instruction identity/version mismatch",
                            invocation.skill_id
                        )));
                    }
                    prepared_tools.extend(asset.tool_refs.iter().cloned());
                    let mut item = ContextItem::new(
                        format!(
                            "runtime-skill:{}:{}",
                            asset.skill_id, activation.activation.turn_index
                        ),
                        ContextSourceKind::Task,
                        ContextRole::Instruction,
                        format!(
                            "# Activated skill: {}\nversion: {}\nsource: {}\n\n{}",
                            asset.skill_id,
                            asset.version.as_deref().unwrap_or("unversioned"),
                            asset.source_ref,
                            asset.content
                        ),
                    );
                    item.authority = ContextAuthority::Project;
                    item.source_id = Some(format!("skill:{}", asset.skill_id));
                    item.source_version = asset.version.clone();
                    item.source_reason = Some("runtime selected prompt-only skill".to_string());
                    item.evidence = vec![asset.source_ref.clone()];
                    use sha2::{Digest, Sha256};
                    if asset.pin.instruction_sha256
                        != format!("sha256:{:x}", Sha256::digest(asset.content.as_bytes()))
                        || asset.pin.installation.skill_id != asset.skill_id
                        || asset.pin.instruction_version != asset.version
                        || asset.pin.tool_refs != asset.tool_refs
                    {
                        return Err(RuntimeError::new(
                            "Skill instruction does not match its original pin",
                        ));
                    }
                    self.skill_instruction_source
                        .as_ref()
                        .ok_or_else(|| RuntimeError::new("Skill approval source is unavailable"))?
                        .validate_pins(std::slice::from_ref(&asset.pin))
                        .map_err(RuntimeError::new)?;
                    prepared_pins.insert(
                        format!("skill:{}", asset.skill_id),
                        (
                            format!("sha256:{:x}", Sha256::digest(item.content.as_bytes())),
                            asset.pin,
                        ),
                    );
                    prepared_items.push(item);
                }
            }
        }

        for activation in &decision.activations {
            if activation.activation.selected.is_some() {
                self.append_execution_runtime_event(
                    RuntimeEventScope::Skill,
                    "skill.activation.selected",
                    Some("completed".to_string()),
                    activation
                        .activation
                        .selected
                        .iter()
                        .map(|skill_id| RuntimeEventRef {
                            kind: "skill".to_string(),
                            id: skill_id.clone(),
                        })
                        .collect(),
                    serde_json::to_value(&activation.activation).unwrap_or_else(
                        |error| serde_json::json!({ "serialization_error": error.to_string() }),
                    ),
                );
            }

            let Some(port) = self.session_journal_port.as_ref() else {
                continue;
            };
            let activation_event = activation.activation.to_runtime_session_event(0);
            port.append_event(&activation_event)
                .await
                .map_err(|error| {
                    RuntimeError::new(format!(
                        "runtime skill activation persistence failed for session {}: {error}",
                        activation.activation.session_id
                    ))
                })?;
            if let Some(candidate) = memory_candidate_from_skill_activation(
                &activation.activation,
                &SkillMemoryPolicy::default(),
            ) {
                if let Some(event) =
                    skill_memory_candidate_session_event(&activation.activation, &candidate, 0)
                {
                    port.append_event(&event).await.map_err(|error| {
                        RuntimeError::new(format!(
                        "runtime skill memory bridge persistence failed for session {}: {error}",
                        activation.activation.session_id
                    ))
                    })?;
                }
            }
        }
        if let Ok(mut tool_refs) = self.active_skill_tool_refs.lock() {
            tool_refs.extend(prepared_tools);
        }
        *self
            .skill_instruction_pins
            .lock()
            .map_err(|_| RuntimeError::new("Skill pin provenance is poisoned"))? = prepared_pins;
        for item in prepared_items {
            self.push_next_model_context_item(item);
        }
        Ok(())
    }

    /// Remove runtime-owned context items from a given source.
    pub fn clear_external_context_source(&self, source: ContextSourceKind) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            guard.retain(|item| item.source != source);
        }
    }

    /// Inject resume/handoff state into the next runtime context envelope.
    pub fn inject_resume_context(&self, packet: ResumeContextPacket) {
        let item = ContextRuntimeKernel::resume_item(&packet);
        self.clear_external_context_source(item.source);
        self.push_external_context_item(item);
    }

    pub(super) fn external_context_items(&self) -> Vec<ContextItem> {
        self.external_context_items
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    pub(super) fn tool_trace_context_items(&self) -> Vec<ContextItem> {
        self.tool_trace_context_items
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Start the runtime-owned state epoch for one top-level conversation turn.
    ///
    /// The Host must call this exactly once at turn admission, before any
    /// graph-planned Runtime tool prefetch. A Provider model node is not a turn
    /// boundary: receipts and governed plans created before the first Provider
    /// request must remain live so the packed request can attest their actual
    /// delivery.
    pub(crate) async fn begin_turn_runtime_epoch(&self) {
        let epoch = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_execution_context())
            .and_then(|context| {
                super::ProviderAttemptEpoch::new(context.execution_id, context.turn_id)
            });
        let mut recovered = std::collections::BTreeMap::new();
        let mut recovery_authoritative = true;
        if let (Some(reader), Some(epoch)) = (self.session_history_reader.as_ref(), epoch.as_ref())
        {
            for kind in [
                "context.provider_request_packed",
                "context.provider_attempt_outcome",
            ] {
                let events = match reader
                    .domain_events_for_epoch(
                        self.session_id(),
                        kind,
                        &epoch.execution_id,
                        &epoch.turn_id,
                    )
                    .await
                {
                    Ok(events) => events,
                    Err(error) => {
                        recovery_authoritative = false;
                        tracing::warn!(
                            session_id = self.session_id(),
                            execution_id = %epoch.execution_id,
                            turn_id = %epoch.turn_id,
                            %error,
                            "provider attempt epoch recovery failed closed"
                        );
                        break;
                    }
                };
                for event in events {
                    debug_assert!(epoch.matches_payload(&event.payload));
                    super::TurnCacheDimensionsObservation::apply_durable_payload(
                        &mut recovered,
                        &event.kind,
                        &event.payload,
                    );
                }
            }
        }
        self.cache_dimensions_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .begin_epoch_with_recovery_authority(epoch, recovered, recovery_authoritative);
        if let Ok(mut guard) = self.turn_tool_observations.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_evidence_audits.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_generated_model_receipts.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_native_tool_sources.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_native_root_owners.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.native_wire_receipts.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_model_observations.lock() {
            guard.clear();
        }
        if let Ok(mut provider_state) = self.turn_tool_exposure_metrics.lock() {
            provider_state
                .tool_exposure
                .reset(self.api_client.tool_schema_cache_stats());
            provider_state.unavailable_accounts.clear();
        }
        if let Ok(mut metrics) = self.turn_stable_prefix_metrics.lock() {
            metrics.reset();
        }
        if let Ok(mut plans) = self.turn_governed_tool_plans.lock() {
            plans.clear();
        }
        if let Ok(mut preflight_compaction) = self.turn_preflight_compaction.lock() {
            *preflight_compaction = None;
        }
        let budget = self.runtime_budget_plan();
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            ledger.reset(
                budget.subsystem_budget_tokens,
                budget.tool_result_budget.max_total_tokens as u64,
            );
        }
    }

    pub(crate) fn turn_cache_dimensions_known(&self) -> bool {
        self.cache_dimensions_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .all_known()
    }

    pub(crate) fn turn_recovered_provider_usage(&self) -> super::RecoveredProviderUsage {
        self.cache_dimensions_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovered_usage()
    }

    pub(crate) fn turn_recovered_provider_attempt_count(&self) -> usize {
        self.cache_dimensions_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .current_attempt_count()
    }

    pub(super) fn push_turn_tool_observation(&self, observation: ToolObservation) {
        if let Ok(mut guard) = self.turn_tool_observations.lock() {
            guard.push(observation);
        }
    }

    pub(super) fn turn_tool_observations(&self) -> Vec<ToolObservation> {
        self.turn_tool_observations
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    pub(super) fn tool_exposure_metrics(&self) -> ToolExposureMetrics {
        self.turn_tool_exposure_metrics
            .lock()
            .map(|state| state.tool_exposure.projection())
            .unwrap_or_default()
    }

    pub(super) fn stable_prefix_metrics(&self) -> StablePrefixMetrics {
        self.turn_stable_prefix_metrics
            .lock()
            .map(|metrics| metrics.projection.clone())
            .unwrap_or_default()
    }

    pub(super) fn push_turn_evidence_audit(&self, projection: EvidenceAuditProjection) {
        if let Ok(mut guard) = self.turn_evidence_audits.lock() {
            if let Some(existing) = guard
                .iter_mut()
                .find(|existing| existing.evidence_ref == projection.evidence_ref)
            {
                *existing = projection;
            } else {
                guard.push(projection);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn turn_evidence_audit_count_for_test(&self) -> usize {
        self.turn_evidence_audits
            .lock()
            .expect("turn evidence audit lock poisoned")
            .len()
    }

    pub(super) fn turn_evidence_audits(&self) -> Vec<EvidenceAuditProjection> {
        self.turn_evidence_audits
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    pub(super) fn record_generated_model_receipt(
        &self,
        provider_invocation_id: &str,
        tool_name: &str,
        requirement: &ToolModelDeliveryRequirement,
        raw_ref: &EvidenceRef,
        receipt: &crate::context_evidence::ModelReceipt,
        is_error: bool,
        output: &str,
    ) -> Result<(), RuntimeError> {
        let read_body = crate::context_evidence::read_body_seal(tool_name, output);
        if is_error || (!requirement.is_exact() && read_body.is_none()) {
            return Ok(());
        }
        let source = self
            .turn_native_tool_sources
            .lock()
            .map_err(|_| RuntimeError::new("Native tool source projection poisoned"))?
            .get(provider_invocation_id)
            .filter(|source| source.call.name == tool_name)
            .cloned();
        let generated = GeneratedModelReceipt {
            provider_invocation_id: provider_invocation_id.to_string(),
            tool_name: tool_name.to_string(),
            obligation_ids: requirement.obligation_ids().to_vec(),
            raw_ref: raw_ref.clone(),
            model_receipt_sha256: format!(
                "sha256:{:x}",
                Sha256::digest(receipt.summary.as_bytes())
            ),
            raw_tokens: receipt.raw_tokens,
            receipt_tokens: receipt.receipt_tokens,
            omitted_tokens: receipt.omitted_tokens,
            complete: !receipt.truncated && receipt.omitted_tokens == 0,
            presentation_bytes: receipt.summary.len() as u64,
            source,
            read_body,
        };
        let mut receipts = self
            .turn_generated_model_receipts
            .lock()
            .map_err(|_| RuntimeError::new("generated model receipt ledger is poisoned"))?;
        if let Some(existing) = receipts
            .iter()
            .find(|existing| existing.provider_invocation_id == provider_invocation_id)
        {
            if existing == &generated {
                return Ok(());
            }
            return Err(RuntimeError::new(format!(
                "provider invocation `{provider_invocation_id}` produced conflicting exact model receipts"
            )));
        }
        receipts.push(generated);
        Ok(())
    }

    pub(crate) fn remember_native_tool_calls(
        &self,
        calls: &[ModelToolCall],
    ) -> Result<(), RuntimeError> {
        let Some(bus) = self.cowd_bus.as_ref() else {
            return Ok(());
        };
        let Some(execution) = bus.current_execution_context() else {
            return Ok(());
        };
        let mut sources = self
            .turn_native_tool_sources
            .lock()
            .map_err(|_| RuntimeError::new("Native tool source projection poisoned"))?;
        for call in calls {
            if !matches!(
                call.name.as_str(),
                "read_file" | "read_ref" | "evidence_retrieve"
            ) {
                continue;
            }
            let Some(binding) = call.binding.as_ref() else {
                continue;
            };
            if binding.snapshot.identity.session_id != self.session_id()
                || binding.snapshot.identity.turn_id != execution.turn_id
                || binding.snapshot.identity.execution_id.as_deref()
                    != Some(execution.execution_id.as_str())
                || binding.origin.origin_step_id != binding.snapshot.identity.step_id
                || binding.origin.call_id != call.id
                || binding.origin.catalog_digest != binding.snapshot.catalog_digest
                || !matches!(&binding.origin.source, harness_contract::step::CallSource::Provider { attempt_id }
                    if attempt_id == &binding.snapshot.provider_attempt.attempt_id)
            {
                return Err(RuntimeError::new(
                    "Native tool source does not match the admitted provider Step",
                ));
            }
            if let Some(existing) = sources.get(&call.id) {
                if existing.call.binding != call.binding
                    || existing.call.name != call.name
                    || existing.call.input != call.input
                    || existing.execution != execution
                {
                    return Err(RuntimeError::new(
                        "Native tool invocation has conflicting source Step or input",
                    ));
                }
                continue;
            }
            sources.insert(
                call.id.clone(),
                NativeToolSource {
                    call: call.clone(),
                    execution: execution.clone(),
                    activity_owner: bus.current_activity_binding(),
                    tool_owner: bus.current_tool_activity_binding(&call.id, &call.name),
                },
            );
        }
        Ok(())
    }

    pub(super) fn capture_native_root_owner(
        &self,
        record: &ToolInvocationRecord,
        writer_payload: &serde_json::Value,
        mut after_sequence: u64,
    ) -> Result<(), RuntimeError> {
        let Some(store) = self.runtime_event_store.as_ref() else {
            return Ok(());
        };
        let source = self
            .turn_native_tool_sources
            .lock()
            .map_err(|_| RuntimeError::new("Native tool source projection poisoned"))?
            .get(&record.tool_call_id)
            .cloned();
        let Some(source) = source else {
            return Ok(());
        };
        let (Some(binding), Some(tool_owner)) =
            (source.call.binding.as_ref(), source.tool_owner.as_ref())
        else {
            return Ok(());
        };
        if source.call.name != record.tool_name
            || tool_owner.root_execution_id != source.execution.execution_id
            || record.session_id != self.session_id()
            || record.status.as_str() != "completed"
            || record.input_hash != crate::tool_invocation::stable_hash(&source.call.input)
        {
            return Ok(());
        }
        let mut expected = writer_payload.clone();
        expected["_runtime_activity_binding"] = serde_json::to_value(tool_owner)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let stream_id = format!("session:{}", self.session_id());
        let through_sequence = store
            .stream_revision(&stream_id)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        while after_sequence < through_sequence {
            let events = store
                .list_stream_after(
                    &stream_id,
                    after_sequence,
                    through_sequence,
                    64,
                    4 * 1024 * 1024,
                )
                .map_err(RuntimeError::new)?;
            if events.is_empty() {
                break;
            }
            for event in events {
                after_sequence = event.sequence;
                if event.kind != "tool.invocation.completed"
                    || event.scope != RuntimeEventScope::Tool
                    || event.actor.as_deref() != Some("conversation_runtime")
                    || event.status.as_deref() != Some("completed")
                    || event.payload != expected
                    || event.activity_binding().as_ref() != Some(tool_owner)
                {
                    continue;
                }
                let owner = crate::context_evidence::RootReadOwner {
                    stream_id: event.stream_id,
                    sequence: event.sequence,
                    event_id: event.event_id,
                    payload_digest: crate::context_evidence::read_payload_digest(&event.payload),
                    binding: tool_owner.clone(),
                };
                self.turn_native_root_owners
                    .lock()
                    .map_err(|_| RuntimeError::new("Native Root owner projection poisoned"))?
                    .entry(binding.effect_idempotency_key())
                    .or_insert(owner);
            }
        }
        Ok(())
    }

    pub(crate) fn native_tool_activity_owner(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        input_hash: &str,
    ) -> Result<Option<harness_contract::projection::RuntimeActivityBinding>, RuntimeError> {
        let source = self
            .turn_native_tool_sources
            .lock()
            .map_err(|_| RuntimeError::new("Native tool source projection poisoned"))?
            .get(tool_call_id)
            .cloned();
        let Some(source) = source else {
            return Ok(None);
        };
        let owner = source
            .activity_owner
            .as_ref()
            .ok_or_else(|| RuntimeError::new("Native tool source has no original activity"))?;
        if owner.agent_run_id.is_some() {
            return Ok(None);
        }
        let context = self
            .cowd_bus()
            .and_then(crate::CowdEventBus::current_execution_context)
            .ok_or_else(|| RuntimeError::new("Native tool writer has no original root"))?;
        let tool = source
            .tool_owner
            .as_ref()
            .ok_or_else(|| RuntimeError::new("Native tool source has no original tool activity"))?;
        let live = self
            .cowd_bus()
            .and_then(crate::CowdEventBus::current_activity_binding)
            .ok_or_else(|| RuntimeError::new("Native tool writer has no live activity"))?;
        let store = self
            .runtime_event_store
            .as_ref()
            .ok_or_else(|| RuntimeError::new("Native tool writer has no original source store"))?;
        if source.execution != context
            || !native_root_continuation_matches(
                owner,
                &live,
                &context.session_id,
                &context.execution_id,
                &context.turn_id,
            )
            || source.call.id != tool_call_id
            || source.call.name != tool_name
            || crate::tool_invocation::stable_hash(&source.call.input) != input_hash
            || owner.root_execution_id != context.execution_id
            || owner.session_id != context.session_id
            || owner.turn_id != context.turn_id
            || !native_root_activity_is_original(store, owner)?
        {
            return Err(RuntimeError::new(
                "Native tool writer source/context/input mismatch",
            ));
        }
        let mut expected = owner.clone();
        expected.activity_id = tool.activity_id.clone();
        expected.parent_activity_id = Some(owner.activity_id.clone());
        expected.initiator_activity_id = Some(owner.activity_id.clone());
        expected.node_id = None;
        expected.skill_id = None;
        expected.skill_revision = None;
        expected.skill_activation_id = None;
        expected.tool_contract_id = Some(tool_name.to_string());
        expected.tool_call_id = Some(tool_call_id.to_string());
        expected.approval_id = None;
        if *tool != expected || tool.validate().is_err() {
            return Err(RuntimeError::new(
                "Native tool writer original tool activity mismatch",
            ));
        }
        Ok(Some(tool.clone()))
    }

    fn native_read_recipient(
        &self,
        source: &NativeToolSource,
        acquisition: &NativeReadAcquisition,
        raw_ref: &EvidenceRef,
        context: &crate::ProviderRequestEvidenceContext,
    ) -> Result<Option<crate::context_evidence::ReadDeliveryRecipient>, RuntimeError> {
        use crate::context_evidence::ReadDeliveryRecipient;
        if source.execution.session_id != context.session_id
            || source.execution.turn_id != context.turn_id
            || source.execution.execution_id != context.execution_id
        {
            return Ok(None);
        }
        let live_owner = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_activity_binding());
        let Some(source_owner) = source.activity_owner.as_ref() else {
            return Ok(None);
        };
        if let Some(identity) = self.execution_identity.as_ref().filter(|identity| {
            identity.kind() == harness_contract::execution::ExecutionIdentityKind::AgentNode
        }) {
            if source.activity_owner != live_owner
                || identity.graph_id() != Some(acquisition.seal.graph_id.as_str())
                || identity.node_id() != Some(acquisition.seal.node_id.as_str())
                || identity.agent_run_id().is_none()
                || identity.agent_run_id() != acquisition.seal.step.run_ref.as_deref()
            {
                return Ok(None);
            }
            return Ok(Some(ReadDeliveryRecipient::AgentTask {
                identity: identity.clone(),
            }));
        }
        if self.execution_identity.as_ref().is_some_and(|identity| {
            !matches!(
                identity.kind(),
                harness_contract::execution::ExecutionIdentityKind::TaskGraph
                    | harness_contract::execution::ExecutionIdentityKind::SessionTurn
            )
        }) {
            return Ok(None);
        }
        let Some(binding) = source.call.binding.as_ref() else {
            return Ok(None);
        };
        let Some(expected) = source.tool_owner.as_ref() else {
            return Ok(None);
        };
        let Some(live) = live_owner.as_ref() else {
            return Ok(None);
        };
        if !native_root_continuation_matches(
            source_owner,
            live,
            &context.session_id,
            &context.execution_id,
            &context.turn_id,
        ) {
            return Ok(None);
        }
        if expected.root_execution_id != context.execution_id
            || expected.session_id != context.session_id
            || expected.turn_id != context.turn_id
            || expected.root_execution_id != source_owner.root_execution_id
            || expected.session_id != source_owner.session_id
            || expected.turn_id != source_owner.turn_id
            || expected.task_id != source_owner.task_id
            || expected.root_task_id != source_owner.root_task_id
            || expected.revision != source_owner.revision
            || expected.fence != source_owner.fence
            || expected.generation != source_owner.generation
            || expected.parent_activity_id.as_deref() != Some(source_owner.activity_id.as_str())
            || expected.initiator_activity_id.as_deref() != Some(source_owner.activity_id.as_str())
        {
            return Ok(None);
        }
        let owner = self
            .turn_native_root_owners
            .lock()
            .map_err(|_| RuntimeError::new("Native Root owner projection poisoned"))?
            .get(&binding.effect_idempotency_key())
            .cloned();
        let Some(owner) = owner else {
            return Ok(None);
        };
        let Some(store) = self.runtime_event_store.as_ref() else {
            return Ok(None);
        };
        if !native_root_activity_is_original(store, source_owner)?
            || !native_root_activity_is_original(store, live)?
        {
            return Ok(None);
        }
        let events = store
            .list_stream_after(
                &owner.stream_id,
                owner.sequence.saturating_sub(1),
                owner.sequence,
                1,
                4 * 1024 * 1024,
            )
            .map_err(RuntimeError::new)?;
        let Some(event) = events.first() else {
            return Ok(None);
        };
        if &owner.binding != expected
            || !crate::context_evidence::root_read_owner_matches(&owner, event, &acquisition.seal)
            || event.commit_cursor <= acquisition.cursor
            || event.payload["full_output_ref"] != format!("tool://{}", raw_ref.id())
            || event.payload["input_hash"]
                != crate::tool_invocation::stable_hash(&source.call.input)
        {
            return Ok(None);
        }
        Ok(Some(ReadDeliveryRecipient::Root {
            owner,
            identity: self.execution_identity.clone(),
        }))
    }

    fn native_read_acquisition(
        &self,
        source: &NativeToolSource,
        body: &crate::context_evidence::ReadBodySeal,
    ) -> Result<Option<NativeReadAcquisition>, RuntimeError> {
        let Some(store) = self.runtime_event_store.as_ref() else {
            return Ok(None);
        };
        let Some(binding) = source.call.binding.as_ref() else {
            return Ok(None);
        };
        let key = binding.effect_idempotency_key();
        let primary_stream = format!("execution-effect:{key}");
        let primary_key = format!("{key}:read-receipt");
        let Some(primary) = store
            .event_by_idempotency_key(&primary_stream, &primary_key)
            .map_err(|error| RuntimeError::new(error.to_string()))?
        else {
            return Ok(None);
        };
        let Some(value) = primary
            .payload
            .get(crate::context_evidence::READ_ACQUISITION_FIELD)
        else {
            return Ok(None);
        };
        let seal =
            crate::context_evidence::decode_read_acquisition(value).map_err(RuntimeError::new)?;
        crate::context_evidence::native_acquisition_descriptor(&primary, &seal)
            .map_err(RuntimeError::new)?;
        let origin = serde_json::to_value(&binding.origin)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if primary.kind != "execution.read.receipt"
            || primary.actor.as_deref() != Some("governed_tool")
            || primary.scope != RuntimeEventScope::ExecutionNode
            || primary.status.as_deref() != Some("completed")
            || seal.primary_stream != primary_stream
            || seal.primary_key != primary_key
            || seal.index_stream
                != format!(
                    "execution-agent-receipts:{}:{}:{}",
                    seal.graph_id, seal.node_id, seal.attempt
                )
            || seal.index_key != format!("agent-tool-receipt:{key}")
            || seal.invocation_id != source.call.id
            || seal.tool_name != source.call.name
            || &seal.body != body
            || seal.session_id != self.session_id()
            || seal.step != binding.snapshot.identity
            || seal.origin != origin
            || primary.payload["input_sha256"] != seal.input_sha256
            || primary.payload["replay"]["origin"] != origin
            || primary.payload["replay"]["invocation"]["origin"] != origin
            || primary.payload["replay"]["invocation"]["invocation_id"] != source.call.id
            || primary.payload["replay"]["invocation"]["intent"]["tool_name"] != source.call.name
            || primary.payload["replay"]["call_id"] != source.call.id
        {
            return Ok(None);
        }
        {
            let input = harness_contract::step::parse_strict_json(&source.call.input)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let digest = harness_contract::step::canonical_digest("tool-effect-payload", &input)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            if primary.payload["replay"]["payload_digest"] != digest {
                return Ok(None);
            }
        }
        let Some(index) = store
            .event_by_idempotency_key(&seal.index_stream, &seal.index_key)
            .map_err(|error| RuntimeError::new(error.to_string()))?
        else {
            return Ok(None);
        };
        if index.kind != "execution.agent_tool.receipt"
            || index.actor.as_deref() != Some("governed_tool")
            || index.payload["effect_kind"] != "read"
            || index.scope != RuntimeEventScope::ExecutionNode
            || index.status.as_deref() != Some("completed")
            || index.transaction_id != primary.transaction_id
            || index.commit_cursor != primary.commit_cursor
            || index.transaction_index <= primary.transaction_index
            || index
                .payload
                .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                != Some(value)
            || index.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
            || index.payload["effect"] != primary.payload["effect"]
            || index.payload["outcome"] != primary.payload["outcome"]
            || !index.refs.iter().any(|reference| {
                reference.kind == "agent_attempt" && reference.id == seal.attempt.to_string()
            })
            || !primary.refs.iter().any(|reference| {
                reference.kind == "agent_attempt" && reference.id == seal.attempt.to_string()
            })
            || !primary.refs.iter().any(|reference| {
                reference.kind == "execution_graph" && reference.id == seal.graph_id
            })
            || !primary
                .refs
                .iter()
                .any(|reference| reference.kind == "execution_node" && reference.id == seal.node_id)
        {
            return Ok(None);
        }
        if primary.payload["replay"]["parent_execution"].is_null() {
            let intent = store
                .event_by_idempotency_key(&primary_stream, &format!("{key}:read-intent"))
                .map_err(|error| RuntimeError::new(error.to_string()))?
                .ok_or_else(|| {
                    RuntimeError::new("Native root acquisition has no original read intent")
                })?;
            let owner = crate::context_evidence::authenticate_root_read_intent(
                &intent, &primary, &index, &seal,
            )
            .map_err(RuntimeError::new)?;
            let admitted_graph = store
                .transaction_stream_revision_exact(&intent.transaction_id, &seal.graph_id)
                .map_err(|error| RuntimeError::new(error.to_string()))?
                .ok_or_else(|| {
                    RuntimeError::new("Native root intent has no atomic graph authority source")
                })?;
            if Some(admitted_graph.expected_revision)
                != intent.payload["root_effect_owner"]["graph_revision"].as_u64()
                || admitted_graph.committed_revision != admitted_graph.expected_revision
            {
                return Err(RuntimeError::new(
                    "Native root intent graph revision disagrees with admission transaction",
                ));
            }
            if owner.root_execution_id != source.execution.execution_id
                || owner.session_id != source.execution.session_id
                || owner.turn_id != source.execution.turn_id
                || source.activity_owner.as_ref().is_none_or(|source_owner| {
                    source_owner.root_execution_id != owner.root_execution_id
                        || source_owner.session_id != owner.session_id
                        || source_owner.turn_id != owner.turn_id
                        || source_owner.root_task_id != owner.root_task_id
                        || source_owner.task_id != owner.task_id
                        || source_owner.generation != owner.generation
                        || source_owner.agent_run_id.is_some()
                })
            {
                return Err(RuntimeError::new(
                    "Native root acquisition original activity differs from raw source",
                ));
            }
            let original_stream = format!("{}:node:{}", seal.graph_id, seal.node_id);
            let mut through = store
                .stream_revision(&original_stream)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let mut original = None;
            while through > 0 {
                let page = store
                    .list_stream_before(&original_stream, through, 64, 4 * 1024 * 1024)
                    .map_err(RuntimeError::new)?;
                if page.is_empty() {
                    return Err(RuntimeError::new(
                        "Native root node activity history is incomplete",
                    ));
                }
                for event in &page {
                    if event.commit_cursor <= intent.commit_cursor {
                        original = Some(event.clone());
                        break;
                    }
                }
                if original.is_some() {
                    break;
                }
                through = page.last().unwrap().sequence.saturating_sub(1);
            }
            if original.as_ref().is_none_or(|event| {
                event.activity_binding().as_ref() != Some(&owner)
                    || event.payload["to"] != "running"
                    || event.kind != "execution_node.transitioned"
                    || event.actor.as_deref() != Some("execution_commit_service")
                    || event.scope != RuntimeEventScope::ExecutionNode
                    || event.status.as_deref() != Some("running")
            }) {
                return Err(RuntimeError::new(
                    "Native root admission has no original running node activity",
                ));
            }
        } else if primary.payload["replay"]["parent_execution"]["execution_id"] != seal.graph_id
            || primary.payload["replay"]["parent_execution"]["node_id"] != seal.node_id
        {
            return Ok(None);
        }
        let Ok(outcome) = serde_json::from_value::<crate::RuntimeToolExecutionOutcome>(
            primary.payload["outcome"].clone(),
        ) else {
            return Ok(None);
        };
        if outcome.status != crate::RuntimeToolExecutionStatus::Executed
            || outcome.error.is_some()
            || outcome.tool_name != seal.tool_name
            || outcome.tool_use_id != seal.invocation_id
        {
            return Ok(None);
        }
        if primary.payload["outcome_truncated"] == false
            && outcome.output.as_ref().is_none_or(|output| {
                crate::context_evidence::read_body_seal(&seal.tool_name, output).as_ref()
                    != Some(&seal.body)
            })
        {
            return Err(RuntimeError::new(
                "Native acquisition body differs from its original outcome",
            ));
        }
        Ok(Some(NativeReadAcquisition {
            seal,
            event_id: primary.event_id,
            cursor: primary.commit_cursor,
            index_event_id: index.event_id,
        }))
    }

    pub(super) fn packed_model_observation_candidates(
        &self,
        request: &ApiRequest,
        request_sequence: usize,
        provider_attempt: u32,
    ) -> Result<Vec<harness_contract::context::ProviderModelObservationAttestation>, RuntimeError>
    {
        let generated = self
            .turn_generated_model_receipts
            .lock()
            .map_err(|_| RuntimeError::new("generated model receipt ledger is poisoned"))?
            .clone();
        if generated.is_empty() {
            return Ok(Vec::new());
        }

        let mut packed_results = BTreeMap::<String, (String, String, bool)>::new();
        for message in request.messages.iter() {
            for block in &message.blocks {
                let ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    output,
                    is_error,
                } = block
                else {
                    continue;
                };
                let packed = (tool_name.clone(), output.clone(), *is_error);
                if let Some(existing) = packed_results.get(tool_use_id) {
                    if existing != &packed {
                        return Err(RuntimeError::new(format!(
                            "packed provider request contains conflicting ToolResult blocks for invocation `{tool_use_id}`"
                        )));
                    }
                } else {
                    packed_results.insert(tool_use_id.clone(), packed);
                }
            }
        }

        let mut candidates = Vec::new();
        for receipt in generated {
            let Some((tool_name, output, is_error)) =
                packed_results.get(&receipt.provider_invocation_id)
            else {
                // Compaction or request selection may omit an old result. An
                // omitted result is simply not observed by this attempt.
                continue;
            };
            let packed_digest = format!("sha256:{:x}", Sha256::digest(output.as_bytes()));
            if tool_name != &receipt.tool_name
                || *is_error
                || packed_digest != receipt.model_receipt_sha256
            {
                return Err(RuntimeError::new(format!(
                    "packed ToolResult for exact invocation `{}` no longer matches its generated receipt",
                    receipt.provider_invocation_id
                )));
            }
            candidates.push(
                harness_contract::context::ProviderModelObservationAttestation {
                    provider_invocation_id: receipt.provider_invocation_id,
                    obligation_ids: receipt.obligation_ids,
                    raw_ref: receipt.raw_ref,
                    model_receipt_sha256: receipt.model_receipt_sha256,
                    raw_tokens: receipt.raw_tokens,
                    receipt_tokens: receipt.receipt_tokens,
                    omitted_tokens: receipt.omitted_tokens,
                    complete: receipt.complete,
                    provider_request_sequence: u64::try_from(request_sequence).unwrap_or(u64::MAX),
                    provider_attempt,
                    model: request.model.clone(),
                },
            );
        }
        Ok(candidates)
    }

    /// Settle the input observations one provider attempt actually delivered.
    ///
    /// An input the model read is a durable fact about what it saw, and it is
    /// independent of whether the tool calls the model emitted afterwards were
    /// legal. A refused output frame does not un-read an input that was
    /// delivered, so the fact is settled here, in the owner of the observation
    /// ledgers, and not at the end of the response handling that a refusal can
    /// skip.
    ///
    /// The settlement needs the accepted wire of the attempt that carried the
    /// observation. The transport persists that outcome before it publishes
    /// the terminal, so the consumer sees an accepted wire at this boundary
    /// deterministically. An attempt that failed, was cancelled, or was never
    /// confirmed has no accepted wire, and then the confirmation gates write
    /// nothing: that is the honest answer, not a lost observation.
    pub(super) fn settle_observed_inputs(
        &self,
        candidates: Vec<harness_contract::context::ProviderModelObservationAttestation>,
        effective_model: &str,
        context: &crate::ProviderRequestEvidenceContext,
        accepted_step: Option<&harness_contract::step::StepSnapshot>,
    ) -> Result<(), RuntimeError> {
        self.confirm_model_observations(candidates, effective_model, context, accepted_step)
    }

    pub(super) fn confirm_model_observations(
        &self,
        mut candidates: Vec<harness_contract::context::ProviderModelObservationAttestation>,
        effective_model: &str,
        context: &crate::ProviderRequestEvidenceContext,
        accepted_step: Option<&harness_contract::step::StepSnapshot>,
    ) -> Result<(), RuntimeError> {
        if candidates.is_empty()
            || self.cancellation_token.is_cancelled()
            || context.execution_id.is_empty()
            || context.turn_id.is_empty()
        {
            return Ok(());
        }
        let Some(accepted_step) = accepted_step else {
            return Ok(());
        };
        if context.session_id != self.session_id()
            || accepted_step.identity.session_id != context.session_id
            || accepted_step.identity.turn_id != context.turn_id
            || accepted_step.identity.execution_id.as_deref() != Some(context.execution_id.as_str())
        {
            return Ok(());
        }
        let Some(store) = &self.runtime_event_store else {
            return Ok(());
        };
        let wires = self
            .native_wire_receipts
            .lock()
            .map_err(|_| RuntimeError::new("native wire receipt projection poisoned"))?
            .iter()
            .filter(|wire| {
                wire.context == *context
                    && wire.outcome_sequence.is_some()
                    && wire.step_render.as_ref().is_some_and(|render| {
                        render.step_id == accepted_step.identity.step_id
                            && render.attempt_id == accepted_step.provider_attempt.attempt_id
                            && render.catalog_digest == accepted_step.catalog_digest
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        if wires.len() != 1 {
            return Ok(());
        }
        let wire = &wires[0];
        let generated = self
            .turn_generated_model_receipts
            .lock()
            .map_err(|_| RuntimeError::new("generated model receipt ledger is poisoned"))?
            .clone();
        let mut confirmed = Vec::new();
        for candidate in &mut candidates {
            let Some(receipt) = generated
                .iter()
                .find(|receipt| receipt.provider_invocation_id == candidate.provider_invocation_id)
            else {
                continue;
            };
            let (Some(source), Some(body)) = (&receipt.source, &receipt.read_body) else {
                continue;
            };
            let Some(acquisition) = self.native_read_acquisition(source, body)? else {
                continue;
            };
            let stream_id = format!("session:{}", self.session_id());
            let confirmation_revision = store
                .stream_revision(&stream_id)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let Some(recipient) =
                self.native_read_recipient(source, &acquisition, &candidate.raw_ref, context)?
            else {
                continue;
            };
            if !wire.results.iter().any(|result| {
                result.invocation_id == candidate.provider_invocation_id
                    && !result.is_error
                    && result.presentation_sha256 == candidate.model_receipt_sha256
                    && result.presentation_bytes == receipt.presentation_bytes
            }) {
                continue;
            }
            let material = crate::context_evidence::ReadDeliveryMaterial {
                schema_version: 1,
                grade: crate::context_evidence::ReadDeliveryGrade::ProviderModelObserved,
                acquisition: acquisition.seal.clone(),
                acquisition_event_id: acquisition.event_id.clone(),
                acquisition_cursor: acquisition.cursor,
                index_event_id: acquisition.index_event_id.clone(),
                session_id: context.session_id.clone(),
                execution_id: context.execution_id.clone(),
                turn_id: context.turn_id.clone(),
                tool_invocation_id: candidate.provider_invocation_id.clone(),
                recipient,
                presentation_sha256: candidate.model_receipt_sha256.clone(),
                presentation_bytes: receipt.presentation_bytes,
                body_omitted: !candidate.complete,
                delivered_extent: candidate
                    .complete
                    .then(|| acquisition.seal.body.extent.clone()),
                transport:
                    crate::context_evidence::ReadDeliveryTransport::NativeProviderContinuation {
                        request_id: wire.request_id.clone(),
                        request_sequence: context.request_sequence,
                        provider_attempt: context.attempt,
                        step_render: wire.step_render.clone().expect("matched step render"),
                        wire_body_sha256: wire.wire_sha256.clone(),
                        wire_artifact: wire.wire_artifact.clone(),
                        packed_sequence: wire.packed_sequence,
                        outcome_sequence: wire
                            .outcome_sequence
                            .expect("matched successful outcome"),
                        model: effective_model.to_string(),
                        raw_ref: candidate.raw_ref.clone(),
                    },
            };
            let payload = serde_json::to_value(&material)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            crate::context_evidence::decode_read_delivery(&payload).map_err(RuntimeError::new)?;
            let key = format!(
                "native-read:{}:{}",
                wire.request_id, candidate.provider_invocation_id
            );
            if let Some(existing) = store
                .event_by_idempotency_key(&stream_id, &key)
                .map_err(|error| RuntimeError::new(error.to_string()))?
            {
                if existing.kind != crate::context_evidence::NATIVE_DELIVERY_KIND
                    || existing.payload != payload
                    || existing.actor.as_deref() != Some("conversation_runtime")
                    || existing.status.as_deref() != Some("confirmed")
                    || existing.schema_version != 1
                    || existing.scope != RuntimeEventScope::Session
                {
                    return Err(RuntimeError::new(
                        "conflicting Native read delivery material",
                    ));
                }
            } else {
                #[cfg(test)]
                if NATIVE_DELIVERY_CONFIRMATION_FAILURE
                    .try_with(|enabled| *enabled)
                    .unwrap_or(false)
                {
                    return Err(RuntimeError::new(
                        "injected Native delivery Runtime confirmation append failure",
                    ));
                }
                let mut refs = vec![
                    RuntimeEventRef {
                        kind: "read_acquisition".to_string(),
                        id: acquisition.event_id.clone(),
                    },
                    RuntimeEventRef {
                        kind: "read_acquisition_index".to_string(),
                        id: acquisition.index_event_id.clone(),
                    },
                    RuntimeEventRef {
                        kind: "tool_invocation".to_string(),
                        id: candidate.provider_invocation_id.clone(),
                    },
                    RuntimeEventRef {
                        kind: "step".to_string(),
                        id: acquisition.seal.step.step_id.clone(),
                    },
                ];
                let mut expected_streams =
                    vec![crate::runtime_event_store::ExpectedStreamRevision {
                        stream_id: stream_id.clone(),
                        expected_revision: confirmation_revision,
                    }];
                for source_stream in [
                    &acquisition.seal.primary_stream,
                    &acquisition.seal.index_stream,
                ] {
                    expected_streams.push(crate::runtime_event_store::ExpectedStreamRevision {
                        stream_id: source_stream.clone(),
                        expected_revision: store
                            .stream_revision(source_stream)
                            .map_err(|error| RuntimeError::new(error.to_string()))?,
                    });
                }
                let effect_key = acquisition
                    .seal
                    .primary_key
                    .strip_suffix(":read-receipt")
                    .ok_or_else(|| {
                        RuntimeError::new(
                            "Native acquisition primary key has no original effect identity",
                        )
                    })?;
                if let Some(intent) = store
                    .event_by_idempotency_key(
                        &acquisition.seal.primary_stream,
                        &format!("{effect_key}:read-intent"),
                    )
                    .map_err(|error| RuntimeError::new(error.to_string()))?
                {
                    refs.push(RuntimeEventRef {
                        kind: "read_intent".into(),
                        id: intent.event_id,
                    });
                    refs.push(RuntimeEventRef {
                        kind: "execution_graph".into(),
                        id: acquisition.seal.graph_id.clone(),
                    });
                    refs.push(RuntimeEventRef {
                        kind: "execution_node".into(),
                        id: acquisition.seal.node_id.clone(),
                    });
                }
                if let crate::context_evidence::ReadDeliveryRecipient::Root { owner, .. } =
                    &material.recipient
                {
                    refs.push(RuntimeEventRef {
                        kind: "root_read_owner".to_string(),
                        id: owner.event_id.clone(),
                    });
                }
                #[cfg(test)]
                let _ = NATIVE_DELIVERY_CONFIRMATION_SOURCE_RACE.try_with(|after_cut| after_cut());
                store
                    .append_transaction(crate::runtime_event_store::AppendTransactionRequest {
                        transaction_id: format!(
                            "native-read:{}",
                            crate::context_evidence::read_sha256(key.as_bytes())
                        ),
                        expected_streams,
                        events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                            event: crate::runtime_event_store::RuntimeEventInput {
                                stream_id,
                                scope: RuntimeEventScope::Session,
                                kind: crate::context_evidence::NATIVE_DELIVERY_KIND.to_string(),
                                status: Some("confirmed".to_string()),
                                actor: Some("conversation_runtime".to_string()),
                                refs,
                                payload,
                            },
                            idempotency_key: Some(key),
                            schema_version: 1,
                        }],
                    })
                    .map_err(|error| {
                        RuntimeError::new(format!("Native delivery confirmation unknown: {error}"))
                    })?;
            }
            candidate.model = effective_model.to_string();
            confirmed.push(candidate.clone());
        }
        let mut observations = self
            .turn_model_observations
            .lock()
            .map_err(|_| RuntimeError::new("model observation ledger is poisoned"))?;
        for candidate in confirmed {
            if let Some(existing) = observations.iter().find(|existing| {
                existing.provider_invocation_id == candidate.provider_invocation_id
            }) {
                if existing.model_receipt_sha256 != candidate.model_receipt_sha256
                    || existing.raw_ref != candidate.raw_ref
                    || existing.obligation_ids != candidate.obligation_ids
                {
                    return Err(RuntimeError::new(format!(
                        "provider invocation `{}` has conflicting model observation attestations",
                        candidate.provider_invocation_id
                    )));
                }
                continue;
            }
            observations.push(candidate);
        }
        Ok(())
    }

    pub(super) fn turn_model_observations(
        &self,
    ) -> Vec<harness_contract::context::ProviderModelObservationAttestation> {
        self.turn_model_observations
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    pub(super) fn existing_evidence_access(
        &self,
        evidence_ref: &EvidenceRef,
    ) -> Option<EvidenceAccessRef> {
        self.turn_evidence_audits.lock().ok().and_then(|guard| {
            guard
                .iter()
                .find(|projection| &projection.evidence_ref == evidence_ref)
                .and_then(|projection| projection.access.clone())
        })
    }

    pub(super) fn current_tool_exposure_projection(
        &self,
    ) -> Option<harness_contract::tool::ToolExposureProjection> {
        let schema_tokens = self.api_client.context_inventory().tool_schema_tokens;
        self.tool_exposure_state
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|state| state.projection(schema_tokens)))
    }

    /// Overlay Gateway's catalog-level capability result with the Runtime-owned
    /// provider schema projection for this exact request. Gateway can describe
    /// every registered backend tool, but only Conversation knows which schemas
    /// were actually sent to the model after discovery and permission filtering.
    pub(super) fn project_runtime_capabilities_for_model(&self, output: &str) -> String {
        let Ok(mut response) = serde_json::from_str::<serde_json::Value>(output) else {
            tracing::warn!("runtime_capabilities returned non-JSON output");
            return output.to_string();
        };
        let Some(object) = response.as_object_mut() else {
            tracing::warn!("runtime_capabilities returned a non-object JSON value");
            return output.to_string();
        };
        let Some(exposure) = self.current_tool_exposure_projection() else {
            return output.to_string();
        };

        let active_function_schemas = exposure.active_ids.clone();
        let tool_search_active = active_function_schemas
            .iter()
            .any(|name| name == "tool_search");
        let active_actions = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .iter()
            .filter(|action| {
                active_function_schemas
                    .iter()
                    .any(|active| active == **action)
            })
            .copied()
            .collect::<Vec<_>>();
        let actions_available = !active_actions.is_empty();

        object.insert("actions".to_string(), serde_json::json!(active_actions));
        object.insert(
            "available".to_string(),
            serde_json::Value::Bool(actions_available),
        );
        object.insert(
            "tool_visibility".to_string(),
            serde_json::json!({
                "active_function_schemas": active_function_schemas,
                "deferred_catalog_tools": exposure.deferred_ids,
                "catalog_revision": exposure.catalog_revision,
                "exposure_revision": exposure.exposure_revision,
                "activation_protocol": if tool_search_active {
                    "Call tool_search once with a focused query. Accepted candidates become callable native function schemas on the immediately following automatic provider request inside this same user turn."
                } else {
                    "No discovery schema is active on this request; do not simulate a deferred catalog tool."
                }
            }),
        );

        serde_json::to_string(&response).unwrap_or_else(|error| {
            tracing::warn!(%error, "failed to serialize projected runtime capabilities");
            output.to_string()
        })
    }

    pub(super) fn activate_tool_discovery(
        &self,
        output: &str,
    ) -> Option<harness_contract::tool::ToolActivationReceipt> {
        let Ok(discovery) =
            serde_json::from_str::<harness_contract::tool::ToolDiscoveryReceipt>(output)
        else {
            tracing::warn!("tool_search returned a non-canonical discovery receipt");
            if let Ok(mut state) = self.turn_tool_exposure_metrics.lock() {
                state.tool_exposure.observe_invalid_search();
            }
            return None;
        };
        self.activate_tool_candidates(&discovery, true)
    }

    pub(super) fn activate_tool_candidates(
        &self,
        discovery: &harness_contract::tool::ToolDiscoveryReceipt,
        count_as_search: bool,
    ) -> Option<harness_contract::tool::ToolActivationReceipt> {
        let Ok(mut guard) = self.tool_exposure_state.lock() else {
            tracing::warn!("tool exposure state lock poisoned");
            return None;
        };
        let Some(state) = guard.as_mut() else {
            tracing::warn!("tool_search completed before tool exposure was initialized");
            return None;
        };
        let allowed_ids = state
            .bootstrap
            .iter()
            .chain(state.active.iter())
            .chain(state.deferred.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let policy = ToolExposurePolicy {
            allowed_ids,
            maximum_permission: contract_permission_mode(self.permission_policy.active_mode()),
            supports_dynamic_exposure: true,
        };
        let activation = ToolExposurePlanner.activate(state, &discovery, &policy);
        let activated_ids = activation
            .activated_ids()
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        tracing::info!(
            catalog_revision = activation.catalog_revision,
            previous_exposure_revision = activation.previous_exposure_revision,
            exposure_revision = activation.exposure_revision,
            activated = ?activated_ids,
            "tool_search activation applied to the next provider request"
        );
        if !activated_ids.is_empty() {
            if let Ok(mut notice) = self.next_model_tool_activation_notice.lock() {
                notice.get_or_insert_default().extend(activated_ids);
            }
        }
        if let Ok(mut state) = self.turn_tool_exposure_metrics.lock() {
            if count_as_search {
                state.tool_exposure.observe_search(&activation);
            } else {
                state.tool_exposure.observe_activation(&activation);
            }
        }
        Some(activation)
    }

    pub(super) fn activate_deferred_tool_calls(
        &self,
        requested: &[String],
        catalog: &harness_contract::tool::ToolDiscoveryReceipt,
    ) -> BTreeSet<String> {
        let known = catalog
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.as_str())
            .collect::<BTreeSet<_>>();
        let activation_candidates = requested
            .iter()
            .filter(|name| known.contains(name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if activation_candidates.is_empty() {
            return BTreeSet::new();
        }
        let mut activation = catalog.clone();
        activation.query = "provider-deferred-tool-call".to_string();
        activation.activation_candidates = activation_candidates;
        self.activate_tool_candidates(&activation, false)
            .map(|receipt| receipt.activated_ids().map(str::to_string).collect())
            .unwrap_or_default()
    }

    pub(super) async fn seed_recent_session_tools(
        &self,
        exposure: &mut ToolExposureState,
        catalog: &harness_contract::tool::ToolDiscoveryReceipt,
    ) {
        const MAX_RECENT_SESSION_TOOLS: usize = 8;
        let session = self.session.read().await;
        let mut recent = BTreeSet::new();
        'messages: for message in session.messages().rev().take(64) {
            for block in message.blocks.iter().rev() {
                let ContentBlock::ToolResult {
                    tool_name,
                    is_error: false,
                    ..
                } = block
                else {
                    continue;
                };
                if let Some(canonical) = self.tool_executor.resolve_tool_name(tool_name) {
                    recent.insert(canonical);
                    if recent.len() >= MAX_RECENT_SESSION_TOOLS {
                        break 'messages;
                    }
                }
            }
        }
        drop(session);
        if recent.is_empty() {
            return;
        }
        let mut discovery = catalog.clone();
        discovery.query = "recent-session-tool-rehydration".to_string();
        discovery.activation_candidates = recent.into_iter().collect();
        let allowed_ids = exposure
            .bootstrap
            .iter()
            .chain(exposure.active.iter())
            .chain(exposure.deferred.iter())
            .cloned()
            .collect();
        let policy = ToolExposurePolicy {
            allowed_ids,
            maximum_permission: contract_permission_mode(self.permission_policy.active_mode()),
            supports_dynamic_exposure: true,
        };
        let activation = ToolExposurePlanner.activate(exposure, &discovery, &policy);
        if activation.activated_ids().next().is_some() {
            exposure.reason =
                "bootstrap plus recently successful session tools rehydrated".to_string();
            if let Ok(mut state) = self.turn_tool_exposure_metrics.lock() {
                state.tool_exposure.observe_activation(&activation);
            }
        }
    }

    pub(super) fn remember_tool_trace_from_message(&self, message: &ConversationMessage) {
        let Some(ContentBlock::ToolResult {
            tool_use_id,
            tool_name,
            output,
            is_error,
        }) = message.blocks.first()
        else {
            return;
        };
        let summary = output
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(600)
            .collect::<String>();
        // A reconstructed message does not carry ArtifactStore internals.
        // Reuse only the canonical durable reference that the Runtime itself
        // placed in the model receipt; never synthesize a plausible-looking
        // URI from the provider call id. Such aliases cannot be authenticated
        // after a checkpoint and previously caused submit/review retry loops.
        let evidence_ids = output
            .split_once("Evidence: tool://")
            .and_then(|(_, suffix)| suffix.split_whitespace().next())
            .map(|id| id.trim_end_matches(['.', ',', ';']).to_string())
            .filter(|id| !id.is_empty())
            .into_iter()
            .collect();
        let packet = ToolTracePacket {
            tool_name: tool_name.clone(),
            invocation_id: tool_use_id.clone(),
            status: if *is_error {
                ToolTraceStatus::Failed
            } else {
                ToolTraceStatus::Succeeded
            },
            summary,
            changed_files: Vec::new(),
            evidence_ids,
            token_estimate: (output.len() as u64).div_ceil(4).min(256).max(1),
        };
        let mut item = ContextRuntimeKernel::tool_trace_item(&packet);
        item.score = if *is_error { 0.9 } else { 0.65 };
        if let Ok(mut guard) = self.tool_trace_context_items.lock() {
            guard.retain(|existing| existing.id != item.id);
            guard.push(item);
            let overflow = guard.len().saturating_sub(8);
            if overflow > 0 {
                guard.drain(0..overflow);
            }
        }
    }

    pub(super) async fn remember_context_envelope(&self, envelope: ContextEnvelope) {
        if let Ok(mut guard) = self.last_context_envelope.lock() {
            *guard = Some(envelope.clone());
        }
        self.persist_context_envelope(envelope.clone()).await;
        if let Some(cowd) = self.cowd_bus() {
            cowd.emit(crate::cowd_event::CowdEvent::ContextEnvelope { envelope });
        }
    }

    pub(super) async fn persist_context_envelope(&self, envelope: ContextEnvelope) {
        let Some(port) = self.session_journal_port.as_ref() else {
            return;
        };
        let session_id = envelope.identity.session_id.clone();
        let envelope_id = envelope.id.clone();
        let persisted = PersistedContextEnvelope::from(&envelope);
        let Ok(persisted_bytes) = serde_json::to_vec(&persisted) else {
            tracing::warn!(
                session_id,
                envelope_id,
                "context envelope serialization failed"
            );
            return;
        };
        let mut artifact_receipt = None;
        let envelope_value = if let Some(artifacts) = self
            .artifact_store
            .as_ref()
            .filter(|store| persisted_bytes.len() as u64 > store.config().compact_threshold_bytes)
        {
            let visibility_scope = format!("session:{session_id}");
            let descriptor = ArtifactWriteDescriptor {
                media_type: "application/vnd.cowd.context-envelope+json".to_string(),
                visibility_scope: visibility_scope.clone(),
                expected_bytes: Some(persisted_bytes.len() as u64),
                original_name: Some(format!("context-envelope-{envelope_id}.json")),
            };
            match artifacts.write_bytes(descriptor, &persisted_bytes).await {
                Ok(artifact) => {
                    let staging_owner = format!("staging:context-envelope:{envelope_id}");
                    match artifacts.pin(
                        &artifact,
                        &staging_owner,
                        now_ms().saturating_add(crate::ARTIFACT_STAGING_PIN_TTL_MS),
                    ) {
                        Ok(()) => {
                            artifact_receipt = Some((
                                Arc::clone(artifacts),
                                artifact,
                                visibility_scope,
                                staging_owner,
                            ));
                            serde_json::json!({
                                "id": persisted.id,
                                "epoch_id": persisted.epoch_id,
                                "identity": persisted.identity,
                                "profile": persisted.profile,
                                "intent": persisted.intent,
                                "budget": persisted.budget,
                                "diagnostics": persisted.diagnostics,
                                "created_at": persisted.created_at,
                                "selected_count": persisted.selected.len(),
                                "omitted_count": persisted.omitted.len(),
                                "artifact_backed": true,
                            })
                        }
                        Err(error) => {
                            let _ = artifacts.delete(&artifact, &visibility_scope);
                            tracing::warn!(
                                %error,
                                session_id,
                                envelope_id,
                                "context envelope artifact pin failed; retaining inline evidence"
                            );
                            serde_json::to_value(&persisted).unwrap_or_default()
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        session_id,
                        envelope_id,
                        "context envelope artifact write failed; retaining inline evidence"
                    );
                    serde_json::to_value(&persisted).unwrap_or_default()
                }
            }
        } else {
            serde_json::to_value(&persisted).unwrap_or_default()
        };
        let context_artifact = artifact_receipt
            .as_ref()
            .map(|(_, artifact, _, _)| artifact.clone());
        let payload = serde_json::json!({
            "type": "ContextEnvelope",
            "schema_version": PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION,
            "envelope_id": envelope_id,
            "formatter_version": CONTEXT_RENDER_FORMATTER_VERSION,
            "envelope": envelope_value,
            "context_artifact": context_artifact,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let record = crate::RuntimeContextEnvelopeRecord {
            session_id: session_id.clone(),
            payload,
            created_at_ms,
        };
        match port.append_context_envelope_if_absent(&record).await {
            Ok(Some(_)) => {
                if let Some((artifacts, artifact, _, staging_owner)) = artifact_receipt {
                    let durable_owner = format!("context-envelope:{envelope_id}");
                    if let Err(error) = artifacts.pin(
                        &artifact,
                        &durable_owner,
                        crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                    ) {
                        let _ = artifacts.pin(
                            &artifact,
                            &staging_owner,
                            crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                        );
                        tracing::warn!(
                            %error,
                            session_id,
                            envelope_id,
                            "context envelope artifact retained by staging owner"
                        );
                        return;
                    }
                    if let Err(error) = artifacts.unpin(&artifact, &staging_owner) {
                        tracing::warn!(
                            %error,
                            session_id,
                            envelope_id,
                            "context envelope artifact retained an extra staging pin"
                        );
                    }
                }
            }
            Ok(None) => {
                if let Some((artifacts, artifact, visibility_scope, staging_owner)) =
                    artifact_receipt
                {
                    let _ = artifacts.unpin(&artifact, &staging_owner);
                    let _ = artifacts.delete(&artifact, &visibility_scope);
                }
                tracing::debug!(session_id, "context envelope event already persisted");
            }
            Err(error) => {
                if let Some((artifacts, artifact, visibility_scope, staging_owner)) =
                    artifact_receipt
                {
                    let _ = artifacts.unpin(&artifact, &staging_owner);
                    let _ = artifacts.delete(&artifact, &visibility_scope);
                }
                tracing::warn!(%error, session_id, "context envelope event append failed");
            }
        }
    }

    pub(super) async fn remember_context_turn_report(
        &self,
        report: ContextTurnReport,
    ) -> Result<(), RuntimeError> {
        self.persist_context_turn_report(&report).await?;
        if let Ok(mut guard) = self.last_context_turn_report.lock() {
            *guard = Some(report);
        }
        Ok(())
    }

    pub(super) async fn persist_context_turn_report(
        &self,
        report: &ContextTurnReport,
    ) -> Result<(), RuntimeError> {
        let Some(port) = self.session_journal_port.as_ref() else {
            // Embedding callers may intentionally run without a durable
            // session carrier. They receive the in-memory report but cannot
            // claim restart/audit durability.
            return Ok(());
        };
        let session_id = self.session_id().to_string();
        let payload = serde_json::json!({
            "type": "ContextTurnReport",
            "report": report,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            0,
            crate::RuntimeSessionEventKind::ContextTurnReport,
            payload,
            created_at_ms,
        );
        port.append_event(&event).await.map_err(|error| {
            RuntimeError::new(format!(
                "context governance persistence failed for session `{session_id}`: {error}"
            ))
        })?;
        Ok(())
    }

    pub(super) async fn finalize_context_prompt(
        &self,
        user_input: &str,
        envelope: ContextEnvelope,
        knowledge: Option<KnowledgeTurnReport>,
        clock_stamp: &crate::prompt::RuntimeClockStamp,
    ) -> Result<PromptAssembly, RuntimeError> {
        let fact_decision = self
            .runtime_fact_decision_for_context(user_input, &envelope)
            .await;
        let report = ContextRuntimeKernel::governance_report(
            &envelope,
            knowledge.as_ref(),
            fact_decision,
            None,
        );
        self.remember_context_governance_report(report).await;
        let mut prompt = Self::provider_prompt_from_envelope(&envelope, clock_stamp);
        prompt.set_cache_cohort_user_prefix(self.cache_cohort_user_prefix());
        prompt.set_immutable_user_prefix(self.immutable_user_prefix());
        self.remember_context_envelope(envelope).await;
        let pins = self
            .skill_instruction_pins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prompt.with_skill_instruction_pins(&pins)
    }

    pub(super) async fn remember_context_governance_report(
        &self,
        report: RuntimeContextGovernanceReport,
    ) {
        self.persist_context_governance_report(report).await;
    }

    pub(super) async fn persist_context_governance_report(
        &self,
        report: RuntimeContextGovernanceReport,
    ) {
        let Some(port) = self.session_journal_port.as_ref() else {
            return;
        };
        let session_id = report.session_id.clone();
        let envelope_id = report.envelope_id.clone();
        let context_epoch = report.context_epoch.clone();
        let payload = serde_json::json!({
            "type": "RuntimeContextGovernanceReport",
            "report": report,
        });
        let created_at_ms = now_ms();
        let mut event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            0,
            crate::RuntimeSessionEventKind::ContextGovernanceReport,
            payload,
            created_at_ms,
        );
        event.status = Some("recorded".to_string());
        event.refs.extend([
            crate::RuntimeSessionEventRef {
                ref_type: "context_envelope".to_string(),
                id: envelope_id,
                label: None,
            },
            crate::RuntimeSessionEventRef {
                ref_type: "context_epoch".to_string(),
                id: context_epoch,
                label: None,
            },
        ]);
        if let Err(error) = port.append_event(&event).await {
            tracing::warn!(%error, session_id, "context governance domain event append failed");
        }
    }

    pub(super) async fn runtime_fact_decision_for_context(
        &self,
        user_input: &str,
        envelope: &ContextEnvelope,
    ) -> Option<RuntimeContextFactDecision> {
        let trigger = fact_extraction_trigger_for_turn(user_input, envelope.profile)?;
        let policy = RuntimeFactExtractionPolicy {
            provider_available: false,
            ..RuntimeFactExtractionPolicy::default()
        };
        let scheduler = RuntimeFactExtractionScheduler::new(policy);
        let decision = scheduler.decide(trigger);
        let evidence_refs = envelope
            .source_registry
            .iter()
            .map(|source| source.source_id.clone())
            .take(32)
            .collect::<Vec<_>>();
        let input = RuntimeFactExtractionInput::new(trigger, user_input)
            .with_session_id(Some(envelope.identity.session_id.clone()))
            .with_project_id(envelope.identity.project_id.clone())
            .with_task_id(envelope.identity.task_id.clone())
            .with_team_id(envelope.identity.team_id.clone())
            .with_agent_id(Some(envelope.identity.agent_id.clone()))
            .with_evidence_refs(evidence_refs)
            .with_token_budget(Some(envelope.budget.total_tokens));
        let extractor = RuleFactExtractor;
        let batch = extractor.extract(&input);
        let event = FactExtractionRuntimeEvent::from_decision(
            &decision,
            extractor.extractor_version(),
            batch.candidates.len(),
            batch.source_evidence.len(),
            batch.token_usage,
        );
        if let Some(port) = self.session_journal_port.as_ref() {
            let mut domain_event = crate::RuntimeSessionEvent::new(
                envelope.identity.session_id.clone(),
                0,
                crate::RuntimeSessionEventKind::ContextFactCandidateReview,
                serde_json::json!({
                    "event": event,
                    "batch_id": batch.batch_id.as_str(),
                    "candidate_count": batch.candidates.len(),
                    "candidates": batch.candidates,
                    "promotion": "review_required",
                }),
                now_ms(),
            );
            domain_event.status = Some("reviewable".to_string());
            domain_event.refs.push(crate::RuntimeSessionEventRef {
                ref_type: "context_envelope".to_string(),
                id: envelope.id.clone(),
                label: None,
            });
            let session_id = envelope.identity.session_id.clone();
            if let Err(error) = port.append_event(&domain_event).await {
                tracing::warn!(%error, session_id, "fact candidate domain event append failed");
            }
        }
        Some(RuntimeContextFactDecision {
            trigger: format!("{:?}", decision.trigger),
            mode: decision.mode.as_str().to_string(),
            degraded: decision.degraded,
            reason: decision.reason,
            candidate_count: batch.candidates.len(),
            review_required: true,
        })
    }

    pub(super) fn context_budget_tokens(&self) -> u64 {
        self.runtime_budget_plan().subsystem_budget_tokens
    }

    pub(super) fn runtime_budget_plan(&self) -> RuntimeBudgetPlan {
        let model_max_output = self
            .model
            .as_deref()
            .filter(|model| !model.is_empty())
            .map_or(0, |model| {
                provider_output_budget_hint(
                    model,
                    self.context_window_for_model(model),
                    self.provider_max_output_override,
                )
            });
        RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: self.model_context_window,
            model_max_output_tokens: model_max_output,
            subsystem_budget_ratio_bp: self.subsystem_budget_ratio_bp,
            profile: self.context_profile(),
            autonomy_mode: None,
            expected_parallel_branches: 1,
            expected_verification_passes: 0,
        })
    }

    /// A fallback route is only safe when the prepared context fits every
    /// candidate that may receive it. Use the narrowest configured candidate
    /// window and output reservation before context selection, rather than
    /// constructing a large primary-only packet and hoping fallback accepts it.
    pub(super) fn runtime_budget_plan_for_candidates(
        &self,
        candidates: &[String],
    ) -> RuntimeBudgetPlan {
        let mut windows = candidates
            .iter()
            .filter(|model| !model.trim().is_empty())
            .map(|model| self.context_window_for_model(model));
        let model_context_window = windows.next().map_or(self.model_context_window, |first| {
            windows.fold(first, u32::min)
        });
        let mut outputs = candidates
            .iter()
            .filter(|model| !model.trim().is_empty())
            .map(|model| {
                provider_output_budget_hint(
                    model,
                    self.context_window_for_model(model),
                    self.provider_max_output_override,
                )
            });
        let model_max_output_tokens = outputs
            .next()
            .map_or(0, |first| outputs.fold(first, u32::min));
        RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window,
            model_max_output_tokens,
            subsystem_budget_ratio_bp: self.subsystem_budget_ratio_bp,
            profile: self.context_profile(),
            autonomy_mode: None,
            expected_parallel_branches: 1,
            expected_verification_passes: 0,
        })
    }

    pub(super) fn apply_exact_evidence_delivery_budget(
        mut plan: RuntimeBudgetPlan,
    ) -> RuntimeBudgetPlan {
        // Exact evidence is a required Provider input, not an ordinary tool
        // preview. Keep the subsystem ceiling, output reservation, and a
        // request-safety reserve intact while allowing the remaining context
        // to carry complete file bodies. Provider preflight still rejects an
        // attempt whose fixed inputs plus exact evidence cannot fit.
        let request_safety_reserve = (plan.model_context_window / 20).max(16_000);
        let exact_total = plan
            .subsystem_budget_tokens
            .saturating_sub(plan.max_output_tokens)
            .saturating_sub(request_safety_reserve);
        let exact_total = usize::try_from(exact_total).unwrap_or(usize::MAX);
        if exact_total > plan.tool_result_budget.max_total_tokens {
            // The exact-evidence path carries whole file bodies, so the turn's
            // TOTAL lease may grow to the subsystem ceiling. A single result must
            // still not dominate the prompt: one whole source file in this repo
            // is 5k-10k tokens, so a hard per-result ceiling keeps a whole file
            // plus the receipt envelope while refusing the "tens of thousands in
            // one result" case that makes every later round re-send it.
            const EXACT_EVIDENCE_PER_RESULT_CEILING: usize = 16_000;
            plan.tool_result_budget.max_total_tokens = exact_total;
            plan.tool_result_budget.per_tool_max_tokens =
                exact_total.min(EXACT_EVIDENCE_PER_RESULT_CEILING).max(
                    plan.tool_result_budget
                        .per_tool_max_tokens
                        .min(EXACT_EVIDENCE_PER_RESULT_CEILING),
                );
        }
        plan
    }

    #[cfg(test)]
    fn exact_evidence_per_result_ceiling_tokens() -> usize {
        16_000
    }

    pub(super) fn context_window_resolution_for_model(
        &self,
        model: &str,
    ) -> provider::ModelContextWindowResolution {
        let mut resolution = if self.model.as_deref() == Some(model) {
            provider::ModelContextWindowResolution {
                tokens: self.model_context_window,
                source: self.model_context_window_source,
            }
        } else {
            provider::model_context_window_resolution(model, Some(&self.model_context_windows))
        };
        if let Some(tokens) = self.api_client.configured_context_window(model) {
            resolution.tokens =
                if resolution.source == provider::ModelContextWindowSource::Configured {
                    resolution.tokens.min(tokens)
                } else {
                    tokens
                };
            resolution.source = provider::ModelContextWindowSource::Configured;
        }
        if let Ok(calibrated) = self.calibrated_model_context_windows.lock() {
            if let Some(&tokens) = calibrated
                .get(model)
                .filter(|tokens| **tokens < resolution.tokens)
            {
                resolution.tokens = tokens;
                resolution.source = provider::ModelContextWindowSource::Calibrated;
            }
        }
        resolution
    }

    pub(super) fn context_window_for_model(&self, model: &str) -> u32 {
        self.context_window_resolution_for_model(model).tokens
    }

    pub(super) fn calibrate_model_context_window(&self, model: &str, observed_tokens: u32) -> bool {
        if observed_tokens < 1_024 {
            return false;
        }
        let current = self.context_window_for_model(model);
        if observed_tokens >= current {
            return false;
        }
        let Ok(mut calibrated) = self.calibrated_model_context_windows.lock() else {
            return false;
        };
        let next = calibrated
            .get(model)
            .copied()
            .map_or(observed_tokens, |existing| existing.min(observed_tokens));
        calibrated.insert(model.to_string(), next);
        true
    }

    pub(crate) fn memory_turn_context(&self) -> MemoryTurnContext {
        let turn = self.active_turn_strategy();
        let source_readset = turn.as_ref().and_then(|turn| {
            self.memory_source_uses
                .lock()
                .ok()
                .and_then(|uses| (uses.0 == turn.turn_ref).then(|| uses.1.clone()).flatten())
        });
        let project_id = self.with_session_read_blocking(memory_project_id_for_session);
        let task_id = Some(format!("session-task-{}", self.session_id()));
        MemoryTurnContext::new(self.session_id().to_string(), self.memory_agent_id.clone())
            .with_definition_lineage_id(self.memory_definition_lineage_id.clone())
            .with_project_id(project_id)
            .with_task_id(task_id)
            .with_team_id(self.memory_team_id.clone())
            .with_cognitive_read_scopes(self.memory_read_scopes.clone())
            .with_turn_ref(turn.map(|turn| turn.turn_ref))
            .with_source_readset(source_readset)
    }

    pub(super) fn build_context_turn_report(
        &self,
        turn_id: &str,
        usage: TokenUsage,
        auto_compaction: Option<AutoCompactionEvent>,
    ) -> ContextTurnReport {
        let used_tokens = self.with_session_read_blocking(estimate_session_tokens) as u64;
        let pressure = ContextPressureState::new(
            format!("{:?}", self.context_profile()),
            self.context_budget_tokens(),
            used_tokens,
        )
        .with_reserved_tokens(u64::from(usage.output_tokens));
        let mut decision = ContextGovernanceDecision::new(
            pressure.clone(),
            if pressure.compaction_recommended {
                "context pressure exceeded governance threshold"
            } else {
                "context pressure within governance budget"
            },
        );
        let compaction_receipt = auto_compaction
            .as_ref()
            .and_then(|compaction| compaction.compaction_receipt.clone());
        if let Some(compaction) = auto_compaction.as_ref() {
            decision.compact = true;
            decision.estimated_tokens_to_reclaim = compaction.removed_message_count as u64;
        }
        let mut report = ContextTurnReport::new(turn_id.to_string(), pressure)
            .with_output_token_estimate(u64::from(usage.output_tokens))
            .with_governance_decision(decision)
            .with_tool_exposure_metrics(self.tool_exposure_metrics())
            .with_stable_prefix_metrics(self.stable_prefix_metrics());
        if let Ok(ledger) = self.turn_context_ledger.lock() {
            report = report.with_ledger(ledger.projection());
        }
        if let Some(receipt) = compaction_receipt {
            report = report.with_compaction_receipt(receipt);
        }
        for observation in self.turn_tool_observations() {
            report = report.with_observation(observation);
        }
        if let Some(exposure) = self.current_tool_exposure_projection() {
            report = report.with_tool_exposure(exposure);
        }
        for projection in self.turn_evidence_audits() {
            report = report.with_audit_projection(projection);
        }
        if let Some(knowledge) = self.take_turn_knowledge_report() {
            report = report.with_knowledge(knowledge);
        }
        report
    }

    pub(super) fn set_turn_knowledge_report(
        &self,
        report: harness_contract::knowledge::KnowledgeTurnReport,
    ) {
        if let Ok(mut guard) = self.turn_knowledge_report.lock() {
            *guard = Some(report);
        }
    }

    pub(super) fn take_turn_knowledge_report(
        &self,
    ) -> Option<harness_contract::knowledge::KnowledgeTurnReport> {
        self.turn_knowledge_report
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }

    pub(super) fn build_context_envelope(
        &self,
        user_input: &str,
        dynamic_items: Vec<ContextItem>,
        omitted: Vec<ContextOmission>,
        degraded_sources: Vec<ContextSourceKind>,
        total_budget_tokens: u64,
        clock_stamp: &crate::prompt::RuntimeClockStamp,
    ) -> ContextEnvelope {
        let session_id = self.session_id().to_string();
        let workspace_root = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf));
        let profile = self.context_profile();
        let mut identity = ContextIdentity::main(session_id.clone());
        identity.mode = ContextRuntimeKernel::mode_for_profile(profile);
        let canonical_prompt = PromptAssembly::new(self.system_prompt.clone());
        let mut runtime_header = canonical_prompt.runtime_system_segments().to_vec();
        runtime_header.extend(ContextRuntimeKernel::runtime_header(&identity, profile));
        // The clock is NOT part of the attested header. It is per-request data and
        // is emitted as the last section of the context instead, so a segment that
        // changes every request cannot sit in front of segments that do not.
        runtime_header.push(VOLATILE_CLOCK_SENTINEL.to_string());
        // The governance report id deliberately stays out of the model-visible
        // prompt. It is bookkeeping for `RealityRecallDecision`, which reads the
        // same value from the structured report (`context_runtime.rs`), so the
        // model gains nothing from it - and because it is a hash of THIS turn's
        // user input, it changes every turn and sits in front of the stable
        // context. Its bytes were pure prefix churn.
        let mut selected_items = self.external_context_items();
        if let Some(cwd) = workspace_root {
            selected_items.extend(crate::prompt::discover_project_context_items_for_profile(
                &cwd, profile,
            ));
        }
        selected_items.extend(self.tool_trace_context_items());
        selected_items.extend(dynamic_items);
        let (selected_items, binding_omissions) =
            revalidate_context_binding(&session_id, selected_items);
        let mut omitted = omitted;
        omitted.extend(binding_omissions);
        let cache_cohort_segment_count = canonical_prompt.cache_cohort_segment_count();
        let mut envelope = ContextRuntimeKernel::build_envelope_with_cache_cohort(
            ContextEnvelopeRequest {
                profile,
                runtime_header,
                identity,
                intent: user_input.to_string(),
                stable_head: canonical_prompt.stable_system_segments().to_vec(),
                dynamic_items: selected_items,
                omitted,
                total_budget_tokens,
            },
            cache_cohort_segment_count,
        );
        envelope.diagnostics.degraded_sources = degraded_sources;
        // Sources that failed while this request's items were being gathered
        // (Fact/Matrix recall degradation, session page-in failure) also deserve
        // the model-visible marker.
        for source in self.take_degraded_context_sources() {
            if !envelope.diagnostics.degraded_sources.contains(&source) {
                envelope.diagnostics.degraded_sources.push(source);
            }
        }
        envelope.diagnostics.cache_hit =
            self.current_context_cache_hit.swap(false, Ordering::AcqRel);
        if let Ok(mut latency) = self.current_context_source_latency_ms.lock() {
            envelope.diagnostics.source_latency_ms = std::mem::take(&mut *latency);
        }
        envelope
    }

    /// Mark a context source as degraded for this request so
    /// `provider_prompt_from_envelope` can tell the model it is missing.
    pub(super) fn degrade_context_source(&self, source: crate::context_runtime::ContextSourceKind) {
        if let Ok(mut degraded) = self.degraded_context_sources.lock() {
            if !degraded.contains(&source) {
                degraded.push(source);
            }
        }
    }

    /// Drain the degraded sources recorded for this request.
    pub(super) fn take_degraded_context_sources(
        &self,
    ) -> Vec<crate::context_runtime::ContextSourceKind> {
        std::mem::take(
            &mut *self
                .degraded_context_sources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_iter()
        .collect()
    }

    pub(super) fn record_context_source_latency(&self, source: &str, elapsed: Duration) {
        if let Ok(mut latency) = self.current_context_source_latency_ms.lock() {
            latency.insert(
                source.to_string(),
                elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            );
        }
    }

    pub(super) fn provider_prompt_from_envelope(
        envelope: &ContextEnvelope,
        clock_stamp: &crate::prompt::RuntimeClockStamp,
    ) -> PromptAssembly {
        let mut prompt = PromptAssembly::from_stable_system_with_cache_cohort(
            envelope.assembled.stable_head.clone(),
            envelope.assembled.cache_cohort_segment_count,
        );
        for header in &envelope.assembled.runtime_header {
            // The sentinel marks the one per-request segment the envelope carries.
            // Routing on it is explicit: the alternative - matching the clock's
            // prose - would break the day the clock text is reworded.
            if header == VOLATILE_CLOCK_SENTINEL {
                // The stamp is handed in, not minted here: a caller that rebuilds a
                // prompt twice from one prepared envelope must render the same
                // clock both times, or the two requests disagree on when the turn
                // happened. A genuinely new request gets a genuinely new stamp from
                // its caller.
                prompt.push_volatile_tail(crate::prompt::runtime_clock_section_at(clock_stamp));
            } else {
                prompt.push_runtime_context(header.clone());
            }
        }
        // Degraded sources must be visible to the model. Without this marker a
        // silent drop is indistinguishable from "there is no evidence": the model
        // reasons over a partial context and cannot disclose what it lacks.
        // The marker is a constant per source, so it is byte-stable and cannot
        // itself fork the provider prefix.
        for source in &envelope.diagnostics.degraded_sources {
            prompt.push_runtime_context(omitted_source_marker(*source));
        }
        for item in &envelope.selected {
            // Per-request turn state goes to the tail. Everything else stays a
            // packet: packets are the only content the hard-cap packer can
            // evict, and moving an evictable item out of that path would take it
            // out of admission control.
            if item.id.starts_with(GOAL_STATE_PACKET_PREFIX) {
                let rendered =
                    crate::PromptContextPacket::from_item(item).render_for_user_context();
                prompt.push_volatile_tail(rendered);
            } else {
                prompt.push_context_item(item);
            }
        }
        prompt
    }

    /// Pack a previously collected context snapshot for one concrete provider
    /// attempt. This is deliberately pure: a fallback never re-reads memory
    /// or mutates the session, it only applies the narrower candidate budget.
    pub(super) fn pack_provider_attempt(
        &self,
        prompt: &PromptAssembly,
        messages: &HistoryView,
        model: &str,
        inventory: ProviderContextInventory,
    ) -> Result<ApiRequest, RuntimeError> {
        let policy_revision = self
            .permission_policy
            .execution_policy_control()
            .snapshot()
            .revision;
        if prompt
            .context_policy_revision
            .is_some_and(|revision| revision != policy_revision)
        {
            return Err(RuntimeError::new(
                "context_policy_changed_during_compilation",
            ));
        }
        let window_resolution = self.context_window_resolution_for_model(model);
        let context_window_tokens = u64::from(window_resolution.tokens);
        // Protocol framing is deliberately explicit and conservative. Schema
        // payload itself is accounted separately from fixed wire framing.
        let protocol_overhead_tokens =
            128u64.saturating_add(u64::from(inventory.tool_count as u32).saturating_mul(12));
        // Serialized provider envelopes (message wrappers, tool schemas and
        // choice fields) are proportionally expensive on small windows and
        // are not represented by the text-only estimator. Reserve an
        // adaptive envelope margin so the provider preflight cannot reject a
        // continuation after Runtime has admitted it.
        let safety_margin_tokens = if context_window_tokens <= 32_768 {
            (context_window_tokens / 5).clamp(2_048, 4_096)
        } else {
            (context_window_tokens / 100).clamp(128, 2_048)
        };
        let prepared = self.request_compiler.prepare(
            prompt,
            messages,
            inventory,
            model_protocol::fingerprint::hash_serializable(&(
                self.permission_fingerprint,
                policy_revision,
            )),
            model,
        );
        let fixed_input_tokens = prepared.fixed_input_tokens;
        let required_input_tokens = prompt.required_packet_token_estimate();
        let max_output =
            provider::model_max_output_resolution(model, self.provider_max_output_override);
        let output_budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
            context_window_tokens,
            max_output_tokens: u64::from(max_output.tokens),
            fixed_input_tokens,
            required_input_tokens,
            protocol_overhead_tokens,
            safety_margin_tokens,
        });
        if !output_budget.executable {
            return Err(RuntimeError::context_capacity(crate::context_ledger::ContextCapacityFailure::RequiredOverflow, format!(
                "provider candidate `{model}` cannot fit fixed and required request components with a viable continuation: fixed={fixed_input_tokens} required={required_input_tokens} window={context_window_tokens} available_output={} output_floor={}",
                output_budget.available_output_tokens, output_budget.floor_output_tokens,
            )));
        }
        let mut budget = crate::context_ledger::RequestBudgetReport::for_attempt(
            model,
            context_window_tokens,
            output_budget.requested_output_tokens,
            protocol_overhead_tokens,
            safety_margin_tokens,
            fixed_input_tokens,
        );
        budget.set_output_policy(
            u64::from(max_output.tokens),
            max_output.source.as_str(),
            output_budget.preferred_output_tokens,
            output_budget.floor_output_tokens,
            required_input_tokens,
        );
        budget.set_context_window_source(window_resolution.source.as_str());
        budget.compiled_provider_registry_revision = Some(inventory.provider_registry_revision);
        budget.compiled_policy_revision = Some(policy_revision);
        if !budget.executable {
            return Err(RuntimeError::new(format!(
                "provider candidate `{model}` cannot fit fixed request components: fixed={} hard_input_cap={} window={} output_reserve={}",
                budget.fixed_input_tokens,
                budget.hard_input_cap_tokens,
                budget.context_window_tokens,
                budget.requested_output_tokens,
            )));
        }
        let (packed_prompt, dynamic_tokens, omitted_packet_ids, omitted_packet_reasons) = prompt
            .pack_for_hard_cap(budget.dynamic_hard_remaining())
            .map_err(|error| {
                RuntimeError::context_capacity(
                    crate::context_ledger::ContextCapacityFailure::RequiredOverflow,
                    error.to_string(),
                )
            })?;
        budget.record_dynamic_packets(dynamic_tokens, omitted_packet_ids, omitted_packet_reasons);
        if !budget.executable {
            return Err(RuntimeError::new(format!(
                "provider candidate `{model}` exceeded its hard request budget after context packing"
            )));
        }
        Ok(ApiRequest {
            bound_step: None,
            prompt: packed_prompt,
            messages: prepared.history,
            model: model.to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: prepared.cache_hit,
            budget,
            preparation: Default::default(),
            provider_evidence_context: None,
        })
    }

    // Memory helpers (private)
    // -----------------------------------------------------------------------

    /// Build an effective system-prompt list that prepends memory context
    /// entries when the memory subsystem is active.
    ///
    /// Returns a clone of `self.system_prompt` when memory is disabled so the
    /// hot path has zero cost.
    #[cfg(test)]
    pub(super) async fn prepare_reality_context(&self, user_input: &str) -> PromptAssembly {
        self.prepare_reality_context_with_budget(user_input, self.context_budget_tokens())
            .await
    }

    #[cfg(test)]
    pub(super) async fn prepare_reality_context_with_budget(
        &self,
        user_input: &str,
        total_budget_tokens: u64,
    ) -> PromptAssembly {
        let next_model_context_items = self.take_next_model_context_items();
        self.prepare_reality_context_with_budget_and_items(
            user_input,
            total_budget_tokens,
            next_model_context_items,
            &crate::prompt::RuntimeClockStamp::now(),
        )
        .await
        .expect("test context preparation succeeds")
    }

    pub(super) async fn prepare_reality_context_with_budget_and_items(
        &self,
        user_input: &str,
        total_budget_tokens: u64,
        next_model_context_items: Vec<ContextItem>,
        clock_stamp: &crate::prompt::RuntimeClockStamp,
    ) -> Result<PromptAssembly, RuntimeError> {
        let _perf_start = std::time::Instant::now();
        let empty_packet = memory::MemoryContextPacket {
            selected: Vec::new(),
            omitted: Vec::new(),
            token_estimate: 0,
            truncated: false,
            recall_report: Default::default(),
        };

        let Some(mgr) = self.memory_manager.as_ref() else {
            let (runtime_reality_context_items, session_context_items) = tokio::join!(
                async {
                    let started = Instant::now();
                    let items = self.runtime_reality_context_items(user_input).await;
                    self.record_context_source_latency("reality", started.elapsed());
                    items
                },
                async {
                    let started = Instant::now();
                    let items = self.runtime_session_context_items(user_input).await;
                    self.record_context_source_latency("session", started.elapsed());
                    items
                }
            );
            let unavailable_sources = vec![ContextSourceKind::Memory];
            let mut dynamic_items = runtime_reality_context_items;
            dynamic_items.extend(session_context_items);
            dynamic_items.extend(next_model_context_items);
            let mut source_readsets = BTreeMap::new();
            let knowledge_report = self.append_knowledge_context(
                user_input,
                &empty_packet,
                &mut dynamic_items,
                memory::MemoryPacketUse::new(self.memory_turn_context(), 8 * 1024 * 1024),
                &mut source_readsets,
            )?;
            let envelope = self.build_context_envelope(
                user_input,
                dynamic_items,
                Vec::new(),
                unavailable_sources,
                total_budget_tokens,
                &clock_stamp,
            );
            return Ok(self
                .finalize_context_prompt(user_input, envelope, knowledge_report, clock_stamp)
                .await?
                .with_memory_source_readsets(&source_readsets));
        };

        let mem_messages = self.memory_context_messages().await;

        let session_id = self.session_id().to_string();
        let memory_ctx = self.memory_turn_context();
        let kernel = MemoryKernel::new(Arc::clone(mgr));
        let memory_budget = self.runtime_budget_plan().memory_retrieval_budget;
        let memory_budget_tokens = memory_budget.retrieval_budget.min(u64::from(u32::MAX));
        let (memory_packet, runtime_reality_context_items, session_context_items) = tokio::join!(
            async {
                let started = Instant::now();
                let packet = kernel
                    .context_packet(
                        &memory_ctx,
                        user_input,
                        mem_messages.as_slice(),
                        memory_budget.candidate_scan_limit,
                        memory_budget_tokens,
                    )
                    .await;
                self.record_context_source_latency("memory", started.elapsed());
                packet
            },
            async {
                let started = Instant::now();
                let items = self.runtime_reality_context_items(user_input).await;
                self.record_context_source_latency("reality", started.elapsed());
                items
            },
            async {
                let started = Instant::now();
                let items = self.runtime_session_context_items(user_input).await;
                self.record_context_source_latency("session", started.elapsed());
                items
            },
        );
        match memory_packet {
            Ok(packet) => {
                let memory_use = kernel
                    .validate_packet_for_turn(&memory_ctx, &packet, 8 * 1024 * 1024)
                    .map_err(|error| {
                        RuntimeError::new(format!("Memory packet use validation failed: {error}"))
                    })?;
                let packet =
                    crate::knowledge_activation::filter_packet_for_turn_intent(&packet, user_input);
                if let Some(cb) = &self.memory_callback {
                    let entries: Vec<(String, String, f64)> = packet
                        .selected
                        .iter()
                        .map(|item| {
                            (
                                format!("{:?}", item.atom.layer),
                                item.atom.title.clone(),
                                item.atom.confidence as f64,
                            )
                        })
                        .collect();
                    let status = format!("{} memory entries loaded", entries.len());
                    cb.on_memory_update(entries, &status);
                }

                tracing::debug!(
                    selected = packet.selected.len(),
                    omitted = packet.omitted.len(),
                    "memory context packet prepared"
                );
                let dynamic_items = packet
                    .selected
                    .iter()
                    .map(|item| {
                        let role = match item.role {
                            memory::MemoryPacketRole::Orientation => ContextRole::Orientation,
                            memory::MemoryPacketRole::Supporting => ContextRole::Evidence,
                            memory::MemoryPacketRole::Warning
                            | memory::MemoryPacketRole::Conflict => ContextRole::Warning,
                        };
                        let mut context_item = ContextItem::new(
                            item.atom.id.to_string(),
                            ContextSourceKind::Memory,
                            role,
                            format!(
                                "{}\ncontent: {}\nreason: {}\nevidence: {}",
                                item.atom.title,
                                item.content_preview,
                                item.reason,
                                item.atom.evidence_pointer.as_deref().unwrap_or("")
                            ),
                        );
                        context_item.authority = ContextAuthority::Session;
                        context_item.visibility = ContextVisibility::Private;
                        context_item.score = item.atom.confidence;
                        context_item.source_id = Some(item.atom.id.to_string());
                        context_item.source_reason = Some(item.reason.clone());
                        context_item.source_version =
                            Some(item.source_revision.body_digest.clone());
                        if let Some(evidence) = item.atom.evidence_pointer.as_ref() {
                            context_item.evidence.push(evidence.clone());
                        }
                        context_item
                    })
                    .collect::<Vec<_>>();
                let omissions = packet
                    .omitted
                    .iter()
                    .map(|omitted| ContextOmission {
                        source: ContextSourceKind::Memory,
                        reason: format!("{}: {}", omitted.reason, omitted.title),
                        token_estimate: 0,
                    })
                    .collect::<Vec<_>>();
                let mut dynamic_items = dynamic_items;
                dynamic_items.extend(runtime_reality_context_items);
                dynamic_items.extend(session_context_items);
                dynamic_items.extend(next_model_context_items);
                let mut source_readsets = packet
                    .selected
                    .iter()
                    .map(|item| (item.atom.id.to_string(), vec![item.source_revision.clone()]))
                    .collect::<BTreeMap<_, _>>();
                let knowledge_report = self.append_knowledge_context(
                    user_input,
                    &packet,
                    &mut dynamic_items,
                    memory_use,
                    &mut source_readsets,
                )?;
                let envelope = self.build_context_envelope(
                    user_input,
                    dynamic_items,
                    omissions,
                    Vec::new(),
                    total_budget_tokens,
                    &clock_stamp,
                );
                Ok(self
                    .finalize_context_prompt(user_input, envelope, knowledge_report, clock_stamp)
                    .await?
                    .with_memory_source_readsets(&source_readsets))
            }
            Err(err) => {
                tracing::warn!(%err, "memory: prepare_context failed, using base system prompt");
                if let Some(cb) = &self.memory_callback {
                    cb.on_memory_update(Vec::new(), &format!("memory error: {err}"));
                }
                let unavailable_sources = vec![ContextSourceKind::Memory];
                let mut dynamic_items = runtime_reality_context_items;
                dynamic_items.extend(session_context_items);
                dynamic_items.extend(next_model_context_items);
                let mut source_readsets = BTreeMap::new();
                let knowledge_report = self.append_knowledge_context(
                    user_input,
                    &empty_packet,
                    &mut dynamic_items,
                    memory::MemoryPacketUse::new(self.memory_turn_context(), 8 * 1024 * 1024),
                    &mut source_readsets,
                )?;
                let envelope = self.build_context_envelope(
                    user_input,
                    dynamic_items,
                    Vec::new(),
                    unavailable_sources,
                    total_budget_tokens,
                    &clock_stamp,
                );
                Ok(self
                    .finalize_context_prompt(user_input, envelope, knowledge_report, clock_stamp)
                    .await?
                    .with_memory_source_readsets(&source_readsets))
            }
        }
    }

    fn append_knowledge_context(
        &self,
        user_input: &str,
        packet: &memory::MemoryContextPacket,
        dynamic_items: &mut Vec<ContextItem>,
        memory_use: memory::MemoryPacketUse,
        source_readsets: &mut BTreeMap<String, Vec<memory::store::MemoryEntryRevision>>,
    ) -> Result<Option<harness_contract::knowledge::KnowledgeTurnReport>, RuntimeError> {
        let memory_project = memory_use.context().and_then(|ctx| ctx.project_id.clone());
        let activation = self
            .knowledge_activation
            .as_ref()
            .map(|runtime| {
                runtime.activate_from_packet_for_project(
                    &self.session_id().to_string(),
                    user_input,
                    &format!("{:?}", self.context_profile()),
                    memory_project.as_deref(),
                    packet,
                    memory_use,
                )
            })
            .transpose()
            .map_err(|error| {
                RuntimeError::new(format!("Knowledge activation read failed: {error}"))
            })?
            .flatten();
        let Some(activation) = activation else {
            return Ok(None);
        };
        source_readsets.extend(activation.source_readsets);
        dynamic_items.extend(activation.items);
        self.set_turn_knowledge_report(activation.report.clone());
        Ok(Some(activation.report))
    }

    pub(super) async fn runtime_reality_context_items(&self, user_input: &str) -> Vec<ContextItem> {
        let Some((port, binding)) = &self.reality_recall else {
            return Vec::new();
        };
        let report = port.recall_for_binding_async(binding, user_input, 64).await;
        for source in &report.sources {
            if source.status == "degraded" {
                tracing::warn!(
                    source = ?source.source,
                    detail = ?source.detail,
                    "Runtime Fact/Matrix recall degraded"
                );
                // The model must be able to tell a degraded Fact/Matrix backend
                // from "no facts exist"; record it so the prompt marker fires.
                self.degrade_context_source(source.source);
            }
        }
        if let Ok(mut last_report) = self.last_reality_recall_report.lock() {
            *last_report = Some(report.clone());
        }
        report.items
    }

    /// Recall only the current Session automatically. Cross-Session history is
    /// available through the explicit `context_retrieve` tool and is never
    /// passively injected into another conversation.
    pub(super) async fn runtime_session_context_items(&self, user_input: &str) -> Vec<ContextItem> {
        let Some(history) = self.session_history_reader.as_ref() else {
            return Vec::new();
        };
        let session_id = self.session_id().to_string();
        let hot_projection = self.hot_state.as_ref().and_then(|hot_state| {
            hot_state.sessions().get(&session_id).and_then(|snapshot| {
                snapshot
                    .context_manifest
                    .clone()
                    .map(|manifest| (manifest, snapshot.context_cards.clone()))
            })
        });
        let (manifest, cards) = match hot_projection {
            Some(projection) => projection,
            None => match history.page_in_context(&session_id, 512).await {
                Ok(Some(page)) => {
                    let projection = (page.manifest.clone(), page.context_cards.clone());
                    if let Some(hot_state) = &self.hot_state {
                        hot_state.sessions().update(&session_id, |snapshot| {
                            snapshot.context_manifest = Some(page.manifest);
                            snapshot.context_cards = page.context_cards;
                            snapshot.context_refs = vec![format!(
                                "session-context:{}:{}",
                                session_id, projection.0.projection_generation
                            )];
                        });
                    }
                    projection
                }
                Ok(None) => return Vec::new(),
                Err(error) => {
                    tracing::warn!(%error, session_id, "current Session context page-in failed");
                    self.degrade_context_source(ContextSourceKind::Conversation);
                    return Vec::new();
                }
            },
        };
        let query_terms = context_query_terms(user_input);
        if query_terms.is_empty() {
            return Vec::new();
        }
        let binding_fingerprint = self
            .reality_recall
            .as_ref()
            .and_then(|(_, binding)| serde_json::to_vec(binding).ok())
            .map(|bytes| format!("{:x}", Sha256::digest(&bytes)))
            .unwrap_or_else(|| "no-reality-binding".to_string());
        let cache_key = SessionContextProjectionCacheKey {
            session_id: session_id.clone(),
            projection_generation: manifest.projection_generation,
            index_revision: manifest.recovery.index_generation,
            memory_revision: self.memory_context_revision.load(Ordering::Acquire),
            reality_snapshot: binding_fingerprint.clone(),
            binding_fingerprint,
            query_digest: format!("{:x}", Sha256::digest(user_input.as_bytes())),
            model_window: self.model_context_window,
        };
        if let Ok(cache) = self.session_context_projection_cache.lock() {
            if let Some(entry) = cache.as_ref().filter(|entry| entry.key == cache_key) {
                self.current_context_cache_hit
                    .store(true, Ordering::Release);
                return entry.items.clone();
            }
        }
        let has_parented_leaves = cards.iter().any(|card| card.parent_card_id.is_some());
        let mut scored = cards
            .into_iter()
            .filter(|card| !has_parented_leaves || card.parent_card_id.is_some())
            .filter_map(|card| {
                let score = context_text_relevance(&card.summary, &query_terms);
                (score > 0.0).then_some((score, card))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|(left, left_card), (right, right_card)| {
            right
                .partial_cmp(left)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right_card.updated_at_ms.cmp(&left_card.updated_at_ms))
        });
        scored.truncate(8);
        let exact_ranges = scored
            .iter()
            .map(|(_, card)| (card.source_start_sequence, card.source_end_sequence))
            .collect::<Vec<_>>();
        let context_messages = if exact_ranges.is_empty() {
            Vec::new()
        } else {
            match history
                .messages_in_ranges(&session_id, &exact_ranges, 1_024)
                .await
            {
                Ok(messages) => messages,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        session_id,
                        "selected current-Session transcript range expansion failed"
                    );
                    Vec::new()
                }
            }
        };

        let mut items = Vec::new();
        for (score, card) in scored {
            let messages = context_messages
                .iter()
                .filter(|message| {
                    message.sequence >= card.source_start_sequence
                        && message.sequence < card.source_end_sequence
                })
                .take(128)
                .cloned()
                .collect::<Vec<_>>();
            if card.session_id != session_id || !card.matches_source(&messages) {
                tracing::warn!(
                    session_id,
                    card_id = card.card_id,
                    "Session navigation source or summary mismatch; suppressed"
                );
                self.degrade_context_source(ContextSourceKind::Conversation);
                continue;
            }
            let mut navigation = ContextItem::new(
                card.card_id.clone(),
                ContextSourceKind::Conversation,
                ContextRole::Orientation,
                format!(
                    "Current Session history card (messages {}..{}):\n{}",
                    card.source_start_sequence, card.source_end_sequence, card.summary
                ),
            );
            navigation.authority = ContextAuthority::Session;
            navigation.visibility = ContextVisibility::Private;
            navigation.score = score;
            navigation.source_id = Some(card.card_id.clone());
            navigation.source_version = Some(format!(
                "generation:{}:digest:{}",
                manifest.projection_generation, card.source_digest
            ));
            navigation.source_lifecycle = crate::context_runtime::ContextSourceLifecycle::Session;
            navigation.source_reason = Some("focused current-Session navigation card".to_string());
            navigation.evidence.push(format!(
                "session://{}/messages/{}..{}#{}",
                session_id,
                card.source_start_sequence,
                card.source_end_sequence,
                card.source_digest
            ));
            items.push(navigation);

            // A strong card match is expanded from the immutable transcript.
            // The card remains a locator; exact rows remain authoritative.
            if score < 0.45 {
                continue;
            }
            for message in messages {
                let content = session_message_context_text(&message.content_json);
                if content.is_empty() {
                    continue;
                }
                let mut exact = ContextItem::new(
                    message.stable_message_id.clone(),
                    ContextSourceKind::Conversation,
                    ContextRole::RecentTurn,
                    format!("{}: {}", message.role, content),
                );
                exact.authority = if message.role == "user" {
                    ContextAuthority::User
                } else {
                    ContextAuthority::Session
                };
                exact.visibility = ContextVisibility::Private;
                exact.score = score;
                exact.source_id = Some(message.stable_message_id.clone());
                exact.source_version = Some(format!("sequence:{}", message.sequence));
                exact.source_lifecycle = crate::context_runtime::ContextSourceLifecycle::Session;
                exact.source_reason = Some("exact expansion of matched Session card".to_string());
                exact.evidence.push(format!(
                    "session://{}/messages/{}",
                    session_id, message.sequence
                ));
                items.push(exact);
            }
        }
        if let Ok(mut cache) = self.session_context_projection_cache.lock() {
            *cache = Some(SessionContextProjectionCacheEntry {
                key: cache_key,
                items: items.clone(),
            });
        }
        items
    }

    pub(super) async fn memory_context_messages(&self) -> Arc<Vec<MemMessage>> {
        let mut projection = self.session_memory_projection.lock().await;
        let session = self.session.read().await;
        let cursor = session.history().cursor();
        let source_count = session.message_count();

        if projection.initialized
            && projection.history_revision == cursor.revision
            && projection.source_count == source_count
        {
            return Arc::clone(&projection.messages);
        }

        let added = source_count.saturating_sub(projection.source_count);
        let append_only = is_append_only_projection(
            projection.initialized,
            projection.history_revision,
            projection.source_count,
            cursor.revision,
            source_count,
        );
        let start_index = if append_only {
            projection.source_count
        } else {
            0
        };
        let source_messages = if append_only {
            session.messages_page(start_index, added).materialize()
        } else {
            session.materialize_messages()
        };
        drop(session);

        let converted =
            conversation_messages_to_context_mem_messages(&source_messages, start_index);
        projection.converted_messages = projection
            .converted_messages
            .saturating_add(converted.len() as u64);
        if append_only {
            Arc::make_mut(&mut projection.messages).extend(converted);
        } else {
            projection.messages = Arc::new(converted);
            projection.rebuilds = projection.rebuilds.saturating_add(1);
        }
        projection.initialized = true;
        projection.history_revision = cursor.revision;
        projection.source_count = source_count;
        tracing::trace!(
            session_history_revision = cursor.revision,
            source_count,
            appended = append_only,
            converted = source_messages.len(),
            total_converted = projection.converted_messages,
            rebuilds = projection.rebuilds,
            "memory context projection updated"
        );
        Arc::clone(&projection.messages)
    }
}

/// Model-visible marker for a context source whose retrieval failed or was
/// skipped. Deliberately a constant per source kind: it must be byte-stable
/// across requests so it cannot fork the provider prefix, and it must never
/// carry the underlying error detail.
fn omitted_source_marker(source: crate::context_runtime::ContextSourceKind) -> String {
    format!(
        "## Context source unavailable: {source:?}\n\
         This source could not be retrieved for the current request and was \
         omitted rather than silently dropped. Treat any related conclusion as \
         unverified and state the gap explicitly; do not assume the source is \
         empty or absent."
    )
}

#[cfg(test)]
mod omitted_marker_tests {
    use super::omitted_source_marker;

    #[test]
    fn root_native_recovery_keeps_historical_source_and_accepts_new_consumer_node() {
        let original: harness_contract::projection::RuntimeActivityBinding = serde_json::from_value(serde_json::json!({
            "root_execution_id":"root","session_id":"session","turn_id":"turn","root_task_id":"task","task_id":"task",
            "activity_id":"original-source","node_id":"original-node","revision":3,"fence":3,"generation":2
        })).unwrap();
        let mut consumer = original.clone();
        consumer.activity_id = "continuation-source".into();
        consumer.node_id = Some("continuation-node".into());
        consumer.revision = 19;
        consumer.fence = 19;
        assert!(super::native_root_continuation_matches(
            &original, &consumer, "session", "root", "turn"
        ));
        assert_eq!(original.node_id.as_deref(), Some("original-node"));
        assert_eq!(original.revision, 3);
        for field in [
            "root",
            "session",
            "turn",
            "task",
            "generation",
            "agent",
            "fence",
        ] {
            let mut foreign = consumer.clone();
            match field {
                "root" => foreign.root_execution_id = "foreign-root".into(),
                "session" => foreign.session_id = "foreign-session".into(),
                "turn" => foreign.turn_id = "foreign-turn".into(),
                "task" => foreign.task_id = "foreign-task".into(),
                "generation" => foreign.generation += 1,
                "agent" => foreign.agent_run_id = Some("foreign-agent".into()),
                "fence" => foreign.fence = 0,
                _ => unreachable!(),
            }
            assert!(
                !super::native_root_continuation_matches(
                    &original, &foreign, "session", "root", "turn"
                ),
                "{field}"
            );
        }
    }

    #[test]
    fn root_native_activity_checks_original_fence_and_keeps_historical_nodes() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let commits = crate::execution_core::graph::ExecutionCommitService::new(store.clone());
        let mut graph = ExecutionGraph::new("original native Root activity");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session".into(),
            turn_id: "turn".into(),
            root_task_id: "task".into(),
            task_id: "task".into(),
            generation: 1,
        });
        for node_id in ["source-node", "consumer-node"] {
            let mut node =
                ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "answer");
            node.id = node_id.into();
            graph.nodes.push(node);
        }
        let mut graph = commits.register_graph(graph).unwrap().graph;
        for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
            graph = commits
                .transition_node(&graph, "source-node", status, None, vec![])
                .unwrap()
                .graph;
        }
        let source_stream = format!("{}:node:source-node", graph.id);
        let original_event = store.latest_for_stream(&source_stream).unwrap().unwrap();
        let original = original_event.activity_binding().unwrap();
        for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
            graph = commits
                .transition_node(&graph, "consumer-node", status, None, vec![])
                .unwrap()
                .graph;
        }
        let consumer = store
            .latest_for_stream(&format!("{}:node:consumer-node", graph.id))
            .unwrap()
            .unwrap()
            .activity_binding()
            .unwrap();
        assert_ne!(original.node_id, consumer.node_id);
        assert_ne!(original.revision, consumer.revision);
        assert!(super::native_root_continuation_matches(
            &original, &consumer, "session", &graph.id, "turn"
        ));
        assert!(super::native_root_activity_is_original(&store, &original).unwrap());
        assert!(super::native_root_activity_is_original(&store, &consumer).unwrap());
        store
            .append(crate::runtime_event_store::RuntimeEventInput {
                stream_id: source_stream.clone(),
                scope: crate::RuntimeEventScope::ExecutionNode,
                kind: "test.unrelated_note".into(),
                status: None,
                actor: Some("native_root_owner_test".into()),
                refs: vec![],
                payload: serde_json::json!({"note":"neutral history growth"}),
            })
            .unwrap();
        assert!(super::native_root_activity_is_original(&store, &original).unwrap());
        let retained = store
            .event_by_idempotency_key(
                &source_stream,
                &format!("{}:source-node:{}", graph.id, original.revision),
            )
            .unwrap()
            .unwrap();
        assert_eq!(retained, original_event);
        for candidate in [&original, &consumer] {
            let mut foreign = (*candidate).clone();
            foreign.fence += 1;
            assert!(!super::native_root_activity_is_original(&store, &foreign).unwrap());
        }
    }

    #[test]
    fn the_marker_is_a_byte_stable_constant() {
        let first = omitted_source_marker(crate::context_runtime::ContextSourceKind::Memory);
        let second = omitted_source_marker(crate::context_runtime::ContextSourceKind::Memory);
        assert_eq!(first, second, "the marker must not fork the prefix");
    }

    #[test]
    fn the_marker_is_explicit_and_leaks_no_detail() {
        let marker = omitted_source_marker(crate::context_runtime::ContextSourceKind::Memory);
        assert!(marker.contains("Context source unavailable"));
        assert!(marker.contains("omitted rather than silently dropped"));
        assert!(!marker.contains("error:"));
        assert!(!marker.contains("Panic"));
    }

    #[test]
    fn distinct_sources_get_distinct_markers() {
        assert_ne!(
            omitted_source_marker(crate::context_runtime::ContextSourceKind::Memory),
            omitted_source_marker(crate::context_runtime::ContextSourceKind::Knowledge)
        );
    }
}

#[cfg(test)]
mod immutable_native_acquisition_tests {
    use super::*;

    struct AcquisitionApi;

    impl ApiClient for AcquisitionApi {
        fn stream(
            &mut self,
            _: ApiRequest,
        ) -> std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
        > {
            Box::pin(futures::stream::empty())
        }
    }

    #[test]
    fn original_readonly_writer_preserves_unscoped_artifact_in_native_acquisition() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        for scoped in [false, true] {
            let workspace = tempfile::tempdir().unwrap();
            let store = Arc::new(crate::RuntimeEventStore::for_test());
            let service = crate::execution_core::graph::ExecutionCommitService::new(store.clone());
            let mut graph = ExecutionGraph::new("immutable Native body");
            graph.id = "scope2-graph".into();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "scope2-session".into(),
                turn_id: "scope2-turn".into(),
                root_task_id: "task".into(),
                task_id: "task".into(),
                generation: 1,
            });
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::ToolBatch,
                "tool_batch",
                "immutable-body",
            );
            node.id = "artifact-node".into();
            graph.nodes.push(node);
            graph = service.register_graph(graph).unwrap().graph;
            graph = service
                .transition_node(
                    &graph,
                    "artifact-node",
                    ExecutionNodeStatus::Ready,
                    None,
                    vec![],
                )
                .unwrap()
                .graph;
            graph = service
                .bind_and_start_node(
                    &graph,
                    "artifact-node",
                    crate::execution_core::graph::ExecutionNodeBinding {
                        executor_kind: "tool_batch".into(),
                        ticket_idempotency_key: "artifact-ticket".into(),
                        attempt: 1,
                        resource_lease_refs: vec![],
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )
                .unwrap()
                .graph;
            let catalog = tools::ToolCatalog::builtin().with_runtime_tools(vec![tools::RuntimeToolDefinition {
                name:"evidence_retrieve".into(), description:Some("Read immutable evidence".into()),
                input_schema:serde_json::json!({"type":"object","properties":{"evidence_ref":{"type":"string"}},
                    "required":["evidence_ref"],"additionalProperties":false}),
                required_permission:tools::permissions::PermissionMode::ReadOnly,
                effect_resolver:harness_contract::tool::ToolEffectResolverSpec { resolver_id:"runtime.readonly".into(), resolver_version:1 },
            }]).unwrap();
            let host = tools::ToolHost::new(
                "immutable-native",
                workspace.path(),
                tools::ToolHostSnapshot::new(
                    Arc::new(catalog),
                    Arc::new(tools::lsp_client::LspRegistry::new()),
                    None,
                ),
            );
            let mut request = crate::bound_tool_batch::scope_admission_tests::bound_request(
                &host,
                "evidence_retrieve",
                serde_json::json!({"evidence_ref":"artifact://original"}),
            );
            request.parent_execution =
                Some(harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: graph.id,
                    node_id: "artifact-node".into(),
                });
            request.parent_execution_attempt = Some(1);
            let content = "immutable original body";
            let output = serde_json::json!({"kind":"evidence_retrieve","available":true,"evidence_ref":"artifact://original",
                "sha256":crate::context_evidence::read_sha256(content.as_bytes()),"bytes":content.len(),
                "total_chunks":1,"chunks":[{"index":0,"content":content}]}).to_string();
            let outcome = crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(output.clone()),
                error: None,
                evidence_ref: "immutable-original".into(),
                observed_evidence: vec![],
            };
            if scoped {
                let resolver =
                    crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                        .unwrap();
                let effect = &request.compiled_invocation.as_ref().unwrap().effect;
                let scope = crate::bound_tool_batch::admitted_effect_scope(
                    &request,
                    effect,
                    Some(host.pin_snapshot()),
                    &resolver,
                )
                .unwrap();
                assert!(!scope.complete);
                service
                    .commit_bound_readonly_tool_receipt(&request, &outcome, effect, &scope)
                    .unwrap();
            } else {
                service
                    .commit_readonly_tool_receipts(&[(request.clone(), outcome)])
                    .unwrap();
            }
            let primary = store
                .event_by_idempotency_key(
                    &format!("execution-effect:{}", request.idempotency_key),
                    &format!("{}:read-receipt", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            let mut session = Session::new();
            session.session_id = "scope2-session".into();
            let runtime = ConversationRuntime::new(
                session,
                AcquisitionApi,
                crate::StaticToolExecutor::new(),
                PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
                vec![],
            )
            .without_memory()
            .with_runtime_event_store(store.clone());
            let source = NativeToolSource {
                call: ModelToolCall {
                    id: request.tool_use_id.clone(),
                    name: request.tool_name.clone(),
                    input: request.input.clone(),
                    depends_on: vec![],
                    binding: request.step_binding.clone(),
                },
                execution: crate::CowdExecutionContext {
                    execution_id: "scope2-graph".into(),
                    session_id: "scope2-session".into(),
                    turn_id: "scope2-turn".into(),
                },
                activity_owner: None,
                tool_owner: None,
            };
            let body =
                crate::context_evidence::read_body_seal("evidence_retrieve", &output).unwrap();
            let acquisition = runtime
                .native_read_acquisition(&source, &body)
                .unwrap()
                .unwrap();
            assert_eq!(acquisition.cursor, primary.commit_cursor);
            assert_eq!(acquisition.event_id, primary.event_id);
            assert_eq!(acquisition.seal.body, body);
            assert_eq!(acquisition.seal.attempt, 1);
            let mut foreign_body = body.clone();
            foreign_body.source_sha256 = crate::context_evidence::read_sha256(b"foreign-body");
            assert!(runtime
                .native_read_acquisition(&source, &foreign_body)
                .unwrap()
                .is_none());
            let mut foreign_source = source.clone();
            foreign_source.call.input =
                serde_json::json!({"evidence_ref":"artifact://foreign"}).to_string();
            assert!(runtime
                .native_read_acquisition(&foreign_source, &body)
                .unwrap()
                .is_none());
            let mut forged = primary.clone();
            forged.payload["resolved_effect_scope"] = serde_json::json!({"version":2});
            assert!(crate::context_evidence::native_acquisition_descriptor(
                &forged,
                &acquisition.seal
            )
            .is_err());
            forged = primary.clone();
            forged.payload["replay"]["parent_execution"] = serde_json::Value::Null;
            if !scoped {
                assert!(crate::context_evidence::native_acquisition_descriptor(
                    &forged,
                    &acquisition.seal
                )
                .is_err());
            }
            forged = primary.clone();
            forged.payload["resolved_effect_scope"] = serde_json::Value::Null;
            forged.payload["replay"]["invocation"]["effect"]["scopes"] =
                serde_json::to_value(vec![harness_contract::policy::PermissionScope {
                    resource: harness_contract::policy::PermissionResource::File,
                    operation: harness_contract::policy::PermissionOperation::Read,
                    target: Some("input.txt".into()),
                }])
                .unwrap();
            forged.payload["effect"] = forged.payload["replay"]["invocation"]["effect"].clone();
            assert!(crate::context_evidence::native_acquisition_descriptor(
                &forged,
                &acquisition.seal
            )
            .is_err());
            let mut file_extent = acquisition.seal.clone();
            file_extent.body.extent = crate::context_evidence::ReadExtent::FileLines {
                start_line: 1,
                line_count: 1,
                total_lines: 1,
                ends_with_newline: false,
                body_sha256: crate::context_evidence::read_sha256(content.as_bytes()),
                body_bytes: content.len() as u64,
                source_omitted: false,
            };
            forged = primary.clone();
            forged.payload["resolved_effect_scope"] = serde_json::Value::Null;
            assert!(
                crate::context_evidence::native_acquisition_descriptor(&forged, &file_extent)
                    .is_err()
            );
        }
    }
}

#[cfg(test)]
mod degraded_source_marker_tests {
    use super::{degraded_source_marker_tests_marker, ContextSourceKind};

    #[test]
    fn a_degraded_fact_and_matrix_source_are_marked_separately() {
        assert_ne!(
            degraded_source_marker_tests_marker(ContextSourceKind::Fact),
            degraded_source_marker_tests_marker(ContextSourceKind::Matrix)
        );
    }
}

fn degraded_source_marker_tests_marker(
    source: crate::context_runtime::ContextSourceKind,
) -> String {
    omitted_source_marker(source)
}
