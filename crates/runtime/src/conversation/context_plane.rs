//! Context assembly, activation, memory projection, and budget governance.

use super::*;

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

    pub(super) async fn activate_skills_for_turn(
        &self,
        user_input: &str,
    ) -> Result<(), RuntimeError> {
        if let Ok(mut tool_refs) = self.active_skill_tool_refs.lock() {
            tool_refs.clear();
        }
        if self.skill_profiles.is_empty() {
            return Ok(());
        }

        let turn_index = self.session_head().await.message_count;
        let activation = SkillActivationEngine::activate(SkillActivationInput {
            session_id: self.session_id().to_string(),
            turn_index,
            query: user_input.to_string(),
            capability_refs: Vec::new(),
            available_profiles: self.skill_profiles.clone(),
            agent_profile: self.agent_skill_profile.clone(),
        });

        if let Some(invocation) = activation.selected_invocation.as_ref() {
            let strategy = self.active_turn_strategy().ok_or_else(|| {
                RuntimeError::new("Skill invocation requires the Host-admitted turn strategy owner")
            })?;
            let evaluation_isolated = strategy.resource_snapshot.sample_source.contains("corpus=");
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
                Some(source) => source
                    .load_instruction(invocation, &usage_context)
                    .await
                    .map_err(|error| {
                        RuntimeError::new(format!(
                            "runtime skill `{}` instruction page-in failed: {error}",
                            invocation.skill_id
                        ))
                    })?,
                None => self
                    .skill_prompt_assets
                    .iter()
                    .find(|asset| asset.skill_id == invocation.skill_id)
                    .cloned(),
            };
            if let Some(asset) = asset {
                if let Ok(mut tool_refs) = self.active_skill_tool_refs.lock() {
                    tool_refs.extend(asset.tool_refs.iter().cloned());
                }
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
                self.push_next_model_context_item(item);
            }
        }

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
            return Ok(());
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
    pub(crate) fn begin_turn_runtime_epoch(&self) {
        if let Ok(mut guard) = self.turn_tool_observations.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_evidence_audits.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_generated_model_receipts.lock() {
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
    ) -> Result<(), RuntimeError> {
        if !requirement.is_exact() || is_error {
            return Ok(());
        }
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

    pub(super) fn confirm_model_observations(
        &self,
        mut candidates: Vec<harness_contract::context::ProviderModelObservationAttestation>,
        effective_model: &str,
    ) -> Result<(), RuntimeError> {
        if candidates.is_empty() {
            return Ok(());
        }
        let mut observations = self
            .turn_model_observations
            .lock()
            .map_err(|_| RuntimeError::new("model observation ledger is poisoned"))?;
        for candidate in &mut candidates {
            candidate.model = effective_model.to_string();
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
            observations.push(candidate.clone());
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

        let catalog_tool_names = object
            .remove("available_tool_names")
            .unwrap_or_else(|| serde_json::json!([]));
        let active_function_schemas = exposure.active_ids.clone();
        let runtime_orchestrate_active = active_function_schemas
            .iter()
            .any(|name| name == "runtime_orchestrate");
        let tool_search_active = active_function_schemas
            .iter()
            .any(|name| name == "tool_search");

        object.insert("catalog_tool_names".to_string(), catalog_tool_names);
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

        if let Some(strategy) = object
            .get_mut("strategy")
            .and_then(serde_json::Value::as_object_mut)
        {
            strategy.insert(
                "model_callable_tools".to_string(),
                serde_json::json!(exposure.active_ids),
            );
        }

        let orchestration_backend_available = object
            .get("runtime_orchestrate")
            .and_then(|value| value.get("available"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if let Some(orchestration) = object
            .get_mut("runtime_orchestrate")
            .and_then(serde_json::Value::as_object_mut)
        {
            orchestration.insert(
                "schema_active".to_string(),
                serde_json::Value::Bool(runtime_orchestrate_active),
            );
            orchestration.insert(
                "available".to_string(),
                serde_json::Value::Bool(
                    orchestration_backend_available && runtime_orchestrate_active,
                ),
            );
            if !runtime_orchestrate_active {
                let reasons = orchestration
                    .entry("blocked_reasons")
                    .or_insert_with(|| serde_json::json!([]));
                if let Some(reasons) = reasons.as_array_mut() {
                    if !reasons
                        .iter()
                        .any(|reason| reason == "runtime_orchestrate_not_active_in_current_schema")
                    {
                        reasons.push(serde_json::json!(
                            "runtime_orchestrate_not_active_in_current_schema"
                        ));
                    }
                }
            }
        }
        let base_can_execute_now = object
            .get("action_plane")
            .and_then(|value| value.get("can_execute_now"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if let Some(action_plane) = object
            .get_mut("action_plane")
            .and_then(serde_json::Value::as_object_mut)
        {
            action_plane.insert(
                "can_execute_now".to_string(),
                serde_json::Value::Bool(base_can_execute_now && runtime_orchestrate_active),
            );
            if !runtime_orchestrate_active {
                action_plane.insert(
                    "recommended_next_tool".to_string(),
                    serde_json::Value::String(if tool_search_active {
                        "tool_search".to_string()
                    } else {
                        "none".to_string()
                    }),
                );
            }
        }

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
            evidence_ids: vec![tool_use_id.clone()],
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
    ) -> PromptAssembly {
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
        let prompt = Self::provider_prompt_from_envelope(&envelope);
        self.remember_context_envelope(envelope).await;
        prompt
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
            plan.tool_result_budget.max_total_tokens = exact_total;
            plan.tool_result_budget.per_tool_max_tokens = exact_total;
        }
        plan
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
        let project_id = self.with_session_read_blocking(memory_project_id_for_session);
        let task_id = Some(format!("session-task-{}", self.session_id()));
        MemoryTurnContext::new(self.session_id().to_string(), self.memory_agent_id.clone())
            .with_definition_lineage_id(self.memory_definition_lineage_id.clone())
            .with_project_id(project_id)
            .with_task_id(task_id)
            .with_team_id(self.memory_team_id.clone())
            .with_cognitive_read_scopes(self.memory_read_scopes.clone())
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
    ) -> ContextEnvelope {
        let session_id = self.session_id().to_string();
        let workspace_root = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf));
        let profile = self.context_profile();
        let mut identity = ContextIdentity::main(session_id.clone());
        identity.mode = ContextRuntimeKernel::mode_for_profile(profile);
        let governance_report_id =
            ContextRuntimeKernel::governance_report_id(&session_id, user_input);
        let canonical_prompt = PromptAssembly::new(self.system_prompt.clone());
        let mut runtime_header = canonical_prompt.runtime_system_segments().to_vec();
        runtime_header.extend(ContextRuntimeKernel::runtime_header(&identity, profile));
        runtime_header.push(crate::prompt::runtime_clock_section());
        runtime_header.push(format!(
            "context_governance_report_id:{governance_report_id}"
        ));
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
        envelope.diagnostics.cache_hit =
            self.current_context_cache_hit.swap(false, Ordering::AcqRel);
        if let Ok(mut latency) = self.current_context_source_latency_ms.lock() {
            envelope.diagnostics.source_latency_ms = std::mem::take(&mut *latency);
        }
        envelope
    }

    pub(super) fn record_context_source_latency(&self, source: &str, elapsed: Duration) {
        if let Ok(mut latency) = self.current_context_source_latency_ms.lock() {
            latency.insert(
                source.to_string(),
                elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            );
        }
    }

    pub(super) fn provider_prompt_from_envelope(envelope: &ContextEnvelope) -> PromptAssembly {
        let mut prompt = PromptAssembly::from_stable_system_with_cache_cohort(
            envelope.assembled.stable_head.clone(),
            envelope.assembled.cache_cohort_segment_count,
        );
        for header in &envelope.assembled.runtime_header {
            prompt.push_runtime_context(header.clone());
        }
        for item in &envelope.selected {
            prompt.push_context_item(item);
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
        let window_resolution = self.context_window_resolution_for_model(model);
        let context_window_tokens = u64::from(window_resolution.tokens);
        // Protocol framing is deliberately explicit and conservative. Schema
        // payload itself is accounted separately from fixed wire framing.
        let protocol_overhead_tokens =
            128u64.saturating_add(u64::from(inventory.tool_count as u32).saturating_mul(12));
        let safety_margin_tokens = (context_window_tokens / 100).clamp(128, 2_048);
        let prepared = self.request_compiler.prepare(
            prompt,
            messages,
            inventory,
            self.permission_fingerprint,
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
            return Err(RuntimeError::new(format!(
                "provider candidate `{model}` cannot fit fixed and required request components with a viable continuation: fixed={fixed_input_tokens} required={required_input_tokens} window={context_window_tokens} available_output={} output_floor={}",
                output_budget.available_output_tokens,
                output_budget.floor_output_tokens,
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
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        budget.record_dynamic_packets(dynamic_tokens, omitted_packet_ids, omitted_packet_reasons);
        if !budget.executable {
            return Err(RuntimeError::new(format!(
                "provider candidate `{model}` exceeded its hard request budget after context packing"
            )));
        }
        Ok(ApiRequest {
            prompt: packed_prompt,
            messages: prepared.history,
            model: model.to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: prepared.cache_hit,
            budget,
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
        )
        .await
    }

    pub(super) async fn prepare_reality_context_with_budget_and_items(
        &self,
        user_input: &str,
        total_budget_tokens: u64,
        next_model_context_items: Vec<ContextItem>,
    ) -> PromptAssembly {
        let _perf_start = std::time::Instant::now();

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
            let envelope = self.build_context_envelope(
                user_input,
                dynamic_items,
                Vec::new(),
                unavailable_sources,
                total_budget_tokens,
            );
            return self
                .finalize_context_prompt(user_input, envelope, None)
                .await;
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
                let packet =
                    crate::knowledge_activation::filter_packet_for_turn_intent(&packet, user_input);
                if packet.selected.is_empty() {
                    tracing::debug!(entries = 0, "memory context packet prepared");
                    if let Some(cb) = &self.memory_callback {
                        cb.on_memory_update(Vec::new(), "no memories found");
                    }
                    let omissions = packet
                        .omitted
                        .iter()
                        .map(|omitted| ContextOmission {
                            source: ContextSourceKind::Memory,
                            reason: format!("{}: {}", omitted.reason, omitted.title),
                            token_estimate: 0,
                        })
                        .collect();
                    let mut dynamic_items = runtime_reality_context_items;
                    dynamic_items.extend(session_context_items);
                    dynamic_items.extend(next_model_context_items);
                    let envelope = self.build_context_envelope(
                        user_input,
                        dynamic_items,
                        omissions,
                        Vec::new(),
                        total_budget_tokens,
                    );
                    return self
                        .finalize_context_prompt(user_input, envelope, None)
                        .await;
                }

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
                        context_item.source_version = item
                            .atom
                            .evidence_pointer
                            .as_ref()
                            .map(|evidence| format!("evidence:{evidence}"));
                        if let Some(evidence) = item.atom.evidence_pointer.as_ref() {
                            context_item.evidence.push(evidence.clone());
                        }
                        context_item
                    })
                    .collect::<Vec<_>>();
                let knowledge_activation = self.knowledge_activation.as_ref().and_then(|runtime| {
                    runtime.activate_from_packet_for_project(
                        &session_id,
                        user_input,
                        &format!("{:?}", self.context_profile()),
                        Some(&self.checkpoint_workspace_id),
                        &packet,
                    )
                });
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
                let mut knowledge_report = None;
                if let Some(activation) = knowledge_activation {
                    knowledge_report = Some(activation.report.clone());
                    dynamic_items.extend(activation.items);
                    self.set_turn_knowledge_report(activation.report);
                }
                let envelope = self.build_context_envelope(
                    user_input,
                    dynamic_items,
                    omissions,
                    Vec::new(),
                    total_budget_tokens,
                );
                self.finalize_context_prompt(user_input, envelope, knowledge_report)
                    .await
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
                let envelope = self.build_context_envelope(
                    user_input,
                    dynamic_items,
                    Vec::new(),
                    unavailable_sources,
                    total_budget_tokens,
                );
                self.finalize_context_prompt(user_input, envelope, None)
                    .await
            }
        }
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
            .filter(|(score, _)| *score >= 0.45)
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
            let messages = context_messages
                .iter()
                .filter(|message| {
                    message.sequence >= card.source_start_sequence
                        && message.sequence < card.source_end_sequence
                })
                .take(128)
                .cloned()
                .collect::<Vec<_>>();
            if session::context_index_source_digest(&messages) != card.source_digest {
                tracing::warn!(
                    session_id,
                    card_id = card.card_id,
                    "Session card source digest mismatch; exact expansion suppressed"
                );
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
