//! Provider admission, request execution, and governed terminal synthesis.

use super::frame_refusal::{refusal_event_refs, wholly_refused_frame_reason, FrameRefusal};
use super::*;

#[derive(Debug, Clone)]
pub(crate) struct TerminalProviderAnswer {
    pub text: String,
    pub model: Option<String>,
    pub models_used: Vec<String>,
    pub provider_attempt_id: String,
    pub usage: TokenUsage,
    pub first_token_latency_ms: Option<u64>,
    pub active_stream_duration_ms: Option<u64>,
    pub wall_duration_ms: u64,
}

fn add_token_usage(total: &mut TokenUsage, usage: TokenUsage) {
    total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
    total.cache_creation_input_tokens = total
        .cache_creation_input_tokens
        .saturating_add(usage.cache_creation_input_tokens);
    total.cache_read_input_tokens = total
        .cache_read_input_tokens
        .saturating_add(usage.cache_read_input_tokens);
}

const INTERACTIVE_PROVIDER_ADMISSION_TIMEOUT_MS: u64 = 30_000;
const DELEGATED_PROVIDER_ADMISSION_TIMEOUT_MS: u64 = 300_000;

// Proactive context reduction fires when the fixed transcript already occupies
// this percentage of the smallest configured candidate window. It pre-empts the
// hard "cannot fit" failure through the existing semantic-checkpoint pipeline.
const PROACTIVE_COMPACTION_PERCENT: usize = 90;

fn closure_goal_binding(goal: &harness_contract::goal::GoalContract) -> Option<serde_json::Value> {
    let binding = goal.execution_binding.as_ref()?;
    Some(
        serde_json::json!({"program_ref":binding.agentic_program_id,"goal":{
            "status":"known","goal_ref":goal.id,"spec_revision":goal.spec_revision,"spec_digest":goal.spec_digest,
            "completion":goal.completion,"user_intent_criterion_ref":goal.user_intent_criterion_id,
            "source_intent_ref":goal.source_intent_ref,"participation_requirement":goal.participation_requirement,
            "criteria":goal.criteria.iter().map(|criterion| serde_json::json!({
                "criterion_ref":criterion.id,"status":format!("{:?}",criterion.status).to_lowercase()
            })).collect::<Vec<_>>(),
            "required_obligations":goal.obligations.iter().filter(|obligation| obligation.required).collect::<Vec<_>>(),
            "blockers":goal.blockers,"unresolved":goal.unresolved,"waiting":goal.waiting,
        }}),
    )
}

fn final_closure_tool_projection(
    framing: &serde_json::Value,
    schemas: Option<&BTreeSet<String>>,
    exposed: &BTreeSet<String>,
    discoverable: &BTreeSet<String>,
    inventory: ProviderContextInventory,
) -> serde_json::Value {
    let callable = schemas.map(|names| {
        names
            .intersection(exposed)
            .cloned()
            .collect::<BTreeSet<_>>()
    });
    let rungs = framing["remaining"].as_array().into_iter().flatten().map(|entry| {
        let step = entry["step"].as_str().unwrap_or("unknown");
        let mutations = crate::host::closure_mutation_tool_ids(&[step.to_string()]);
        let tools = mutations.iter().map(|tool| {
            let present = schemas.map(|names| names.contains(tool));
            let allowed = callable.as_ref().is_some_and(|names| names.contains(tool));
            let discovery_possible = callable.as_ref().is_some_and(|names| names.contains("tool_search"))
                && discoverable.contains(tool);
            serde_json::json!({
                "tool":tool,
                "schema_present":present,
                "status":if schemas.is_none() {"unknown"}
                    else if allowed {"callable_schema_actor_grant_unverified"}
                    else if discovery_possible {"discovery_candidate_not_callable"}
                    else {"unavailable_this_request"},
                "discovery_tool":if discovery_possible {Some("tool_search")} else {None},
            })
        }).collect::<Vec<_>>();
        serde_json::json!({"step":step,"state_actionability":entry["actionability"],"tools":tools,
            "mapping_status":if mutations.is_empty() {"unknown"} else {"canonical"}})
    }).collect::<Vec<_>>();
    let prerequisite_reads = [
        "read_ref",
        "state_inspect",
        "read_file",
        "evidence_retrieve",
    ]
    .into_iter()
    .filter(|tool| callable.as_ref().is_some_and(|names| names.contains(*tool)))
    .collect::<Vec<_>>();
    serde_json::json!({
        "tool_inventory_status":if schemas.is_some() {"final_adapter_post_crop"} else {"unknown"},
        "schema_ids":schemas,
        "callable_ids":callable,
        "catalog_revision":inventory.catalog_revision,
        "exposure_revision":inventory.exposure_revision,
        "schema_fingerprint":inventory.schema_fingerprint,
        "provider_registry_revision":inventory.provider_registry_revision,
        "rungs":rungs,
        "prerequisite_reads":prerequisite_reads,
        "note":"Native schema presence and current logical exposure are both required for a call. A schema is not an actor, permission or review grant. Reads are prerequisites, not closing mutations. Discovery is an inspection/activation proposal, never proof of future availability; recheck the next actual request. Missing/unknown tools or actor prerequisites cannot be forced. Goal debt and independent review remain unchanged.",
    })
}

fn bind_final_closure_tool_context(
    items: &mut [ContextItem],
    schemas: Option<&BTreeSet<String>>,
    exposed: &BTreeSet<String>,
    discoverable: &BTreeSet<String>,
    inventory: ProviderContextInventory,
    expected_binding: Option<&serde_json::Value>,
) -> bool {
    let mut goals = BTreeMap::new();
    for item in items.iter_mut() {
        if item.source != ContextSourceKind::Task || item.authority != ContextAuthority::System
            || item.role != ContextRole::Instruction || item.visibility != ContextVisibility::Private {
            continue;
        }
        let Some(suffix) = item.id.strip_prefix("goal-state:") else {
            continue;
        };
        let Some(raw) = item.content.strip_prefix("#GoalState ") else {
            continue;
        };
        let Ok(mut framing) = serde_json::from_str::<serde_json::Value>(raw) else {
            continue;
        };
        if !framing.is_object() {
            continue;
        }
        let binding_known = framing["program_source"]["program_ref"].is_string()
            && framing["program_source"]["revision"].is_number()
            && framing["goal"]["status"] == "known"
            && framing["goal"]["goal_ref"].is_string()
            && framing["goal"]["spec_revision"].is_number()
            && framing["goal"]["spec_digest"].is_string()
            && expected_binding.is_some_and(|expected| {
                expected["program_ref"] == framing["program_source"]["program_ref"]
                    && expected["goal"].as_object().is_some_and(|fields| {
                        fields
                            .iter()
                            .all(|(key, value)| framing["goal"].get(key) == Some(value))
                    })
            });
        let mut projection = final_closure_tool_projection(&framing, schemas, exposed, discoverable, inventory);
        projection["state_binding_status"] = serde_json::json!(if binding_known {"bound_packet"} else {"unknown"});
        framing["final_tool_context"] = projection;
        if goals.insert(suffix.to_string(), (framing.clone(), binding_known)).is_some() {
            goals.insert(suffix.to_string(), (serde_json::Value::Null, false));
        }
        item.content = format!("#GoalState {framing}");
        item.token_estimate = ContextItem::new("estimate", item.source, item.role, item.content.clone()).token_estimate;
    }
    let supports_required_choice = |framing: &serde_json::Value, known: bool| {
        framing["execution_role"] == "DelegatedLeaf"
            || (known
                && framing["blocked"].is_null()
                && framing["final_tool_context"]["rungs"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|rung| {
                        (rung["state_actionability"].is_null()
                            || rung["state_actionability"] == "actionable")
                            && rung["tools"].as_array().into_iter().flatten().any(|tool| {
                                tool["status"] == "callable_schema_actor_grant_unverified"
                            })
                    }))
    };
    let mut permits_required_choice = goals.values().all(|(framing, known)| supports_required_choice(framing, *known));
    for item in items.iter_mut() {
        if item.source != ContextSourceKind::Task || item.authority != ContextAuthority::Tool
            || item.role != ContextRole::TaskState || item.visibility != ContextVisibility::Private {
            continue;
        }
        let Some(suffix) = item.id.strip_prefix("owed-action:") else {
            continue;
        };
        let parsed = serde_json::from_str::<serde_json::Value>(&item.content).ok();
        let pair = goals.get(suffix).filter(|(framing, known)| {
            *known
                && parsed.as_ref().is_some_and(|owed| {
                    owed["program_source"] == framing["program_source"]
                        && owed["goal"] == framing["goal"]
                        && owed["remaining"] == framing["remaining"]
                        && owed["blocked"] == framing["blocked"]
                })
        });
        let mut projection = parsed
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| serde_json::json!({"original_packet":item.content}));
        for key in [
            "candidate_mutations",
            "prerequisite_reads",
            "tool_inventory_status",
            "final_wire_inventory",
        ] {
            projection.as_object_mut().expect("owed object").remove(key);
        }
        match pair {
            Some((framing, _)) => {
                permits_required_choice &= supports_required_choice(framing, true);
                projection["state_binding_status"] = serde_json::json!("bound_packet");
                projection["final_tool_context"] = framing["final_tool_context"].clone();
            }
            None => {
                permits_required_choice = false;
                projection["state_binding_status"] = serde_json::json!("unknown");
                projection["tool_binding_blocked"] = serde_json::json!({
                    "reason":"unpaired_or_mismatched_current_goal_state"});
                projection["final_tool_context"] = serde_json::json!({
                    "tool_inventory_status":if schemas.is_some() {"final_adapter_post_crop"} else {"unknown"},
                    "state_binding_status":"unknown"});
            }
        }
        projection["note"] = serde_json::json!("Tool hints use this request's post-crop adapter inventory and the paired Goal observation, not a live business CAS grant. Unpaired or stale observations cannot force a mutation. All original action, permission, independent-review and manifest/Goal CAS checks remain authoritative.");
        item.content = projection.to_string();
        item.token_estimate = ContextItem::new("estimate", item.source, item.role, item.content.clone()).token_estimate;
    }
    permits_required_choice
}

/// PLAN D1b: turn the liveness verdicts the stream guard recorded into at most one
/// non-forcing push-forward nudge for the next request.
///
/// The ladder is cumulative, so the highest rung already says what to do; taking
/// only that one keeps a perpetually-stalling turn from flooding its own context
/// with its own nudges. The result never disables tools and never asks the
/// runtime to end the turn - only the model declares a task finished or
/// impossible (PLAN D1).
fn liveness_stall_context_item(
    stalls: Vec<crate::liveness::LivenessVerdict>,
) -> Option<ContextItem> {
    use crate::liveness::{LivenessAction, LivenessVerdict};
    let (idle, step, action) = stalls
        .into_iter()
        .filter_map(|verdict| match verdict {
            LivenessVerdict::Stalled { idle, step, action } => Some((idle, step, action)),
            LivenessVerdict::Progressing => None,
        })
        .max_by_key(|(_, step, _)| *step)?;
    let guidance = match action {
        LivenessAction::Refocus => format!(
            "Runtime liveness: no semantic progress for {}s. State concretely what is still \
             missing and take the next concrete step; do not restate what you already know.",
            idle.as_secs()
        ),
        LivenessAction::Summarize => format!(
            "Runtime liveness: no semantic progress for {}s. Summarize what is already known so \
             the next step is cheap, then continue from that summary.",
            idle.as_secs()
        ),
        LivenessAction::RequestCommit => format!(
            "Runtime liveness: no semantic progress for {}s. If the evidence already suffices, \
             commit the final answer now from the receipts you hold. If it does not, say what is \
             still missing and continue.",
            idle.as_secs()
        ),
        LivenessAction::Ask => format!(
            "Runtime liveness: no semantic progress for {}s. Say whether you can continue, and if \
             the task is finished or impossible say so plainly. Only you decide - the runtime does \
             not end the turn for you.",
            idle.as_secs()
        ),
    };
    let mut item = ContextItem::new(
        format!("runtime-liveness-{step}"),
        ContextSourceKind::Task,
        ContextRole::Instruction,
        guidance,
    );
    item.authority = ContextAuthority::System;
    item.visibility = ContextVisibility::Private;
    Some(item)
}

/// Make Runtime's checked receipts legible to the terminal narrator even when
/// the provider transport represents them only as native tool messages.
///
/// Native ToolUse/ToolResult pairs remain the source of truth and are kept in
/// the request to satisfy strict provider protocols. The duplicate ledger is
/// deliberate: a zero-tool terminal narrator must not infer that an effect is
/// absent merely because it did not reconstruct a conclusion from the native
/// transport representation. This records only already-checked receipt bytes;
/// it does not infer task success, fabricate a result, or constrain the
/// model's conclusion beyond prohibiting contradictions of observed facts.
fn clean_terminal_evidence_instruction(evidence: &str, has_native_history: bool) -> String {
    let evidence = evidence.trim();
    match (evidence.is_empty(), has_native_history) {
        (true, _) => {
            "No checked tool receipt was available; give an honest bounded answer and name the missing evidence.\n\n"
                .to_string()
        }
        (false, false) => format!(
            "Runtime-authoritative checked evidence ledger:\n{evidence}\n\n"
        ),
        (false, true) => format!(
            "Runtime-authoritative checked evidence ledger (the exact same receipts also appear as the preceding native ToolResult messages):\n{evidence}\n\n\
             Treat every completed receipt in this ledger as an observed fact. Do not say that a completed effect, command, or verification is absent or unverified. \
             Decide what remains unresolved from the objective, but distinguish it from facts that Runtime has already observed.\n\n"
        ),
    }
}

const fn provider_admission_timeout_ms(
    service_class: crate::execution_core::graph::ExecutionServiceClass,
) -> u64 {
    match service_class {
        crate::execution_core::graph::ExecutionServiceClass::Interactive => {
            INTERACTIVE_PROVIDER_ADMISSION_TIMEOUT_MS
        }
        crate::execution_core::graph::ExecutionServiceClass::Foreground
        | crate::execution_core::graph::ExecutionServiceClass::Background
        | crate::execution_core::graph::ExecutionServiceClass::Maintenance => {
            DELEGATED_PROVIDER_ADMISSION_TIMEOUT_MS
        }
    }
}

/// Record a cut-off frame Runtime salvaged, and tell the model what was lost.
///
/// The provider output ceiling cuts a tool frame off mid-value, which leaves
/// the JSON unbalanced rather than invalid. Salvaging the complete prefix is
/// the right repair - one live run wrote three actions, had the third cut away,
/// and Runtime discarded all three with "value has the wrong JSON type" - but a
/// silent salvage is its own defect: the model would believe every item it
/// wrote ran. So the salvage is durable evidence and it is answered in-step
/// through the same context channel the protocol-recovery and liveness paths
/// already use.
impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    fn record_truncated_frame_salvage(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        coercion: &harness_contract::tool::ToolInputCoercion,
    ) {
        self.append_execution_runtime_event(
            crate::RuntimeEventScope::Tool,
            "tool.call.repaired_before_dispatch",
            Some("repaired".to_string()),
            vec![
                crate::RuntimeEventRef {
                    kind: "tool_call".to_string(),
                    id: tool_call_id.to_string(),
                },
                crate::RuntimeEventRef {
                    kind: "tool".to_string(),
                    id: tool_name.to_string(),
                },
            ],
            serde_json::json!({
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
                "field": coercion.field,
                "original_len": coercion.original_len,
                "salvaged_items": coercion.salvaged_items,
                "dropped_items": coercion.dropped_items,
                "truncated": true,
            }),
        );
        let mut item = crate::ContextItem::new(
            format!("runtime-truncated-frame:{tool_call_id}"),
            crate::ContextSourceKind::Task,
            crate::ContextRole::Instruction,
            format!(
                "Runtime notice: your previous `{tool_name}` frame did not parse as a whole, so \
                 Runtime salvaged it entry by entry instead of discarding your work. {} item(s) \
                 of `{}` were valid and were applied; {} item(s) were not valid JSON and were \
                 dropped. Re-emit ONLY the dropped entries, each as {{\"tool\": \"<action>\", \
                 \"input\": {{...}}}} with every brace closed, and keep each field short - a few \
                 sentences per mission - so the whole frame parses. Do not repeat the entries \
                 that already ran.",
                coercion.salvaged_items, coercion.field, coercion.dropped_items
            ),
        );
        item.authority = crate::ContextAuthority::System;
        item.visibility = crate::ContextVisibility::Private;
        item.evidence = vec![format!("tool_call:{tool_call_id}")];
        self.push_next_model_context_item(item);
    }

    /// Answer every refused call in one place, and file the durable record for it.
    ///
    /// A call Runtime refused before dispatch produced nothing, so the model has to
    /// be told that in its own transcript or it will conclude the tool is broken and
    /// retry the same bytes. Every refusal branch routes through here so that
    /// obligation is structural rather than something each branch has to remember -
    /// the unexposed-tool branch was the one that did not, and a model that asked
    /// sixteen times for a tool outside its lease was never once shown its own call.
    fn record_refused_calls(&self, refusals: &[FrameRefusal]) {
        let session_id = self.session_id().to_string();
        for refusal in refusals {
            let invocation_id = refusal.invocation_id(&session_id);
            self.append_execution_runtime_event(
                crate::RuntimeEventScope::Tool,
                "tool.call.rejected_before_dispatch",
                Some("failed".to_string()),
                refusal_event_refs(&invocation_id, refusal),
                refusal.event_payload(&invocation_id),
            );
        }
    }

    /// Record every refusal, then answer each with the whole-frame message.
    fn answer_refused_calls(&self, refusals: &[FrameRefusal]) -> Vec<ConversationMessage> {
        self.record_refused_calls(refusals);
        refusals.iter().map(FrameRefusal::tool_result).collect()
    }
}

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
            self.provider_resource_handles()
                .config
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
        let policy = self.permission_policy.execution_policy_control().snapshot();
        let ceiling = self
            .permission_policy
            .bound_to_snapshot(&policy)
            .active_mode();
        let discovery = match self.tool_executor.pin_step_catalog() {
            Some(lease) => self
                .tool_executor
                .tool_discovery_receipt_for_step(&lease, ceiling),
            None => self.tool_executor.tool_discovery_receipt(),
        };
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_catalog_lookup(started.elapsed());
        }
        discovery
    }

    /// Build the exposure for a Runtime-owned terminal request.
    fn configure_terminal_tool_exposure(&mut self, reason: &str) {
        let discovery = self.discover_tools_with_metrics();
        let known = discovery
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let mut exposure = ToolExposureState {
            catalog_revision: discovery.catalog_revision,
            bootstrap: Default::default(),
            active: Default::default(),
            deferred: known,
            reason: reason.to_string(),
            revision: 0,
            fallback_full: false,
        };
        self.stamp_tool_exposure_revision(&mut exposure);
        self.api_client
            .configure_tool_exposure(exposure.projection(0));
    }

    fn stamp_tool_exposure_revision(&self, exposure: &mut ToolExposureState) {
        let digest = model_protocol::fingerprint::hash_serializable(&(
            &exposure.bootstrap,
            &exposure.active,
            exposure.fallback_full,
        ));
        let previous = self.tool_exposure_digest.swap(digest, Ordering::SeqCst);
        exposure.revision = if previous == digest && previous != 0 {
            self.tool_exposure_revision.load(Ordering::SeqCst).max(1)
        } else {
            self.tool_exposure_revision
                .fetch_add(1, Ordering::SeqCst)
                .saturating_add(1)
        };
    }

    fn memory_source_observation_turn(
        &self,
        cuts: &[memory::store::MemoryEntryRevision],
    ) -> Result<String, RuntimeError> {
        if cuts.is_empty() {
            return Ok(String::new());
        }
        self.active_turn_strategy()
            .map(|turn| turn.turn_ref)
            .ok_or_else(|| {
                RuntimeError::new("Memory-bearing Provider request has no original admitted turn")
            })
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

    async fn prepare_timed_provider_budget(
        &self,
        request: &mut ApiRequest,
    ) -> Result<(), RuntimeError> {
        let started = Instant::now();
        request
            .preparation
            .bind_preparation_scope(self.execution_service_class, self.session_id());
        let result = self.api_client.prepare_request_budget(request).await;
        crate::execution_core::performance::observe_duration(
            "request_wire_budget_ms",
            started.elapsed(),
        );
        result
    }

    fn active_step_goal(
        &self,
        execution: Option<&crate::CowdExecutionContext>,
    ) -> Result<Option<harness_contract::goal::GoalContract>, RuntimeError> {
        let execution_id = execution
            .map(|context| context.execution_id.as_str())
            .or_else(|| {
                self.execution_identity
                    .as_ref()
                    .and_then(|identity| identity.graph_id())
            });
        let Some((execution_id, store)) = execution_id.zip(self.runtime_event_store.as_ref())
        else {
            return Ok(None);
        };
        let goal = crate::execution_core::GoalStore::new(Arc::clone(store))
            .get(&format!("goal:{execution_id}"))
            .map_err(RuntimeError::new)?;
        if let Some(goal) = &goal {
            if goal.session_id != self.session_id()
                || goal.execution_binding.as_ref().is_some_and(|binding| {
                    binding.root_execution_id != execution_id
                        || binding.session_id != self.session_id()
                        || execution.is_some_and(|execution| binding.turn_id != execution.turn_id)
                })
            {
                return Err(RuntimeError::new("step_goal_lineage_mismatch"));
            }
        }
        Ok(goal)
    }

    pub(crate) fn bind_system_tool_call(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
        parent_step_id: &str,
    ) -> Result<Option<crate::step_snapshot::StepCallBinding>, RuntimeError> {
        let Some(lease) = self.tool_executor.pin_step_catalog() else {
            return Ok(None);
        };
        let execution = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_execution_context());
        let goal = self.active_step_goal(execution.as_ref())?;
        let execution_id = execution
            .as_ref()
            .map(|context| context.execution_id.clone())
            .or_else(|| {
                self.execution_identity
                    .as_ref()
                    .and_then(|identity| identity.graph_id().map(str::to_string))
            })
            .ok_or_else(|| {
                RuntimeError::new("system tool call requires a live parent execution")
            })?;
        let turn_id = crate::step_snapshot::admitted_turn_id(
            execution.as_ref(),
            self.session_input_stream
                .active_turn_id()
                .map(|turn| turn.to_string()),
        )?;
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == tool_name)
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        if definitions.len() != 1 {
            return Err(RuntimeError::new("system tool contract is unavailable"));
        }
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                harness_contract::step::StepIdentity {
                    step_id: uuid::Uuid::new_v4().to_string(),
                    session_id: self.session_id().to_string(),
                    turn_id,
                    execution_id: Some(execution_id),
                    actor_ref: Some("runtime:checkpoint-guard".into()),
                    run_ref: None,
                    attempt: None,
                    epoch: execution.as_ref().map(|context| context.turn_id.clone()),
                    goal_ref: goal.as_ref().map(|goal| goal.id.clone()),
                    program_ref: goal.as_ref().and_then(|goal| {
                        goal.execution_binding
                            .as_ref()
                            .map(|binding| binding.agentic_program_id.clone())
                    }),
                    task_ref: self
                        .execution_identity
                        .as_ref()
                        .and_then(|identity| identity.task_id().map(str::to_string)),
                    build_ref: option_env!("COWD_GIT_SHA")
                        .unwrap_or("unverified")
                        .to_string(),
                },
                lease,
                &definitions,
                &serde_json::json!({"source":"runtime_system"}),
                input,
                BTreeMap::from([(
                    "policy".to_string(),
                    self.permission_policy
                        .execution_policy_control()
                        .snapshot()
                        .revision
                        .to_string(),
                )])
                .into_iter()
                .chain(goal.as_ref().map(|goal| {
                    (
                        "goal_spec_observed".to_string(),
                        format!("{}:{}", goal.spec_revision, goal.spec_digest),
                    )
                }))
                .collect(),
                chrono::Utc::now()
                    .timestamp_millis()
                    .saturating_add(3_600_000),
            )?
            .with_parent(parent_step_id),
        );
        self.runtime_event_store
            .as_ref()
            .ok_or_else(|| RuntimeError::new("system tool requires durable origin"))?
            .append(crate::runtime_event_store::RuntimeEventInput {
                stream_id: format!("session:{}", self.session_id()),
                scope: crate::runtime_event_store::RuntimeEventScope::Session,
                kind: "system.step_bound".to_string(),
                status: Some("prepared".into()),
                actor: Some("runtime".into()),
                refs: vec![],
                payload: serde_json::to_value(step.snapshot())
                    .map_err(|error| RuntimeError::new(error.to_string()))?,
            })
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let call_id = format!("system:{}:{tool_name}", step.snapshot().identity.step_id);
        Ok(Some(crate::step_snapshot::StepCallBinding::system(
            step,
            parent_step_id,
            &call_id,
        )))
    }

    fn bind_provider_step(
        &self,
        request: &mut ApiRequest,
    ) -> Result<Option<Arc<crate::step_snapshot::BoundStep>>, RuntimeError> {
        let started = Instant::now();
        let mut pins = request.prompt.skill_instruction_pins()?;
        let mut tool_ids = self
            .skill_exposed_tool_ids
            .lock()
            .map_err(|_| RuntimeError::new("Skill tool provenance is poisoned"))?
            .clone();
        tool_ids.extend(self.skill_binding_tool_refs.iter().cloned());
        let definitions = self
            .api_client
            .step_tool_definitions()
            .unwrap_or_default()
            .into_iter()
            .map(|definition| definition.name)
            .collect::<BTreeSet<_>>();
        let actual_tool_ids = tool_ids
            .intersection(&definitions)
            .cloned()
            .collect::<BTreeSet<_>>();
        for (_, pin) in self
            .skill_instruction_pins
            .lock()
            .map_err(|_| RuntimeError::new("Skill pin provenance is poisoned"))?
            .values()
        {
            if pin
                .tool_refs
                .iter()
                .any(|tool| actual_tool_ids.contains(tool))
                && !pins.contains(pin)
            {
                pins.push(pin.clone());
            }
        }
        for pin in &self.skill_binding_pins {
            if pin
                .tool_refs
                .iter()
                .any(|tool| actual_tool_ids.contains(tool))
                && !pins.contains(pin)
            {
                pins.push(pin.clone());
            }
        }
        if actual_tool_ids
            .iter()
            .any(|tool| !pins.iter().any(|pin| pin.tool_refs.contains(tool)))
        {
            return Err(RuntimeError::new(
                "Skill-derived wire schema has no original pin",
            ));
        }
        request.preparation.bind_skill_tool_refs(actual_tool_ids);
        if !pins.is_empty() {
            let source = self.skill_instruction_source.clone().ok_or_else(|| {
                RuntimeError::new("Skill pins require the original approval source")
            })?;
            source.validate_pins(&pins).map_err(RuntimeError::new)?;
            request.preparation.bind_skill_source(source, pins);
        }
        let result = self.bind_provider_step_inner(request);
        crate::execution_core::performance::observe_duration(
            "request_step_binding_ms",
            started.elapsed(),
        );
        result
    }

    #[cfg(test)]
    pub(crate) fn r1_assert_replay_activation_for_test(&self, output: &str) {
        let catalog = self.tool_executor.tool_discovery_receipt();
        let historical: harness_contract::tool::ToolDiscoveryReceipt =
            serde_json::from_str(output).unwrap();
        assert!(historical.activation_candidates.is_empty());
        for revision in [catalog.catalog_revision, historical.catalog_revision] {
            *self.tool_exposure_state.lock().unwrap() = Some(ToolExposureState {
                catalog_revision: revision,
                bootstrap: BTreeSet::from(["tool_search".to_string()]),
                active: BTreeSet::new(),
                deferred: catalog.activation_candidates.iter().cloned().collect(),
                reason: "retained replay activation boundary".into(),
                revision: 1,
                fallback_full: false,
            });
            let activation = self.activate_tool_discovery(output).unwrap();
            assert!(activation.decisions.is_empty());
            assert!(self
                .tool_exposure_state
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .active
                .is_empty());
        }
        *self.tool_exposure_state.lock().unwrap() = Some(ToolExposureState {
            catalog_revision: catalog.catalog_revision,
            bootstrap: BTreeSet::from(["tool_search".to_string()]),
            active: BTreeSet::new(),
            deferred: catalog.activation_candidates.iter().cloned().collect(),
            reason: "live activation positive control".into(),
            revision: 1,
            fallback_full: false,
        });
        let mut live = catalog;
        live.activation_candidates = vec!["read_file".into()];
        let activation = self
            .activate_tool_discovery(&serde_json::to_string(&live).unwrap())
            .unwrap();
        assert!(activation.activated_ids().any(|name| name == "read_file"));
    }

    #[cfg(test)]
    pub(crate) async fn r1_dispatch_provider_step_for_test(
        &mut self,
        mut request: ApiRequest,
    ) -> Result<Option<Arc<crate::step_snapshot::BoundStep>>, RuntimeError> {
        let step = self.bind_provider_step(&mut request)?;
        let mut stream = self.api_client.stream(request);
        while let Some(event) = futures::StreamExt::next(&mut stream).await {
            event?;
        }
        Ok(step)
    }

    fn bind_provider_step_inner(
        &self,
        request: &mut ApiRequest,
    ) -> Result<Option<Arc<crate::step_snapshot::BoundStep>>, RuntimeError> {
        let policy = self.permission_policy.execution_policy_control().snapshot();
        if request
            .budget
            .compiled_policy_revision
            .is_some_and(|revision| revision != policy.revision)
        {
            return Err(RuntimeError::new(
                "context_policy_changed_after_compilation",
            ));
        }
        let ceiling = self
            .permission_policy
            .bound_to_snapshot(&policy)
            .active_mode();
        let Some(lease) = self
            .tool_executor
            .prepare_step_catalog(ceiling)
            .map_err(|error| RuntimeError::new(error.to_string()))?
        else {
            return Ok(None);
        };
        let advertised = self.api_client.step_tool_definitions();
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=step_catalog lease_present=true advertised_present={}", advertised.is_some());
        }
        let advertised = advertised.ok_or_else(|| {
            RuntimeError::new("production provider does not expose its actual step tool contracts")
        })?;
        let discovery = self
            .tool_executor
            .tool_discovery_receipt_for_step(&lease, ceiling);
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            let all_names_match = advertised.iter().all(|definition| discovery.descriptors.iter().any(|descriptor| descriptor.canonical_id == definition.name));
            eprintln!("r622_toolbatch phase=step_catalog_match advertised={} descriptors={} all_names_match={}", advertised.len(), discovery.descriptors.len(), all_names_match);
        }
        if advertised.iter().any(|definition| {
            !discovery
                .descriptors
                .iter()
                .any(|descriptor| descriptor.canonical_id == definition.name)
        }) {
            return Err(RuntimeError::new(
                "agent_tool_inventory_drift: advertised tool is outside actual host exposure",
            ));
        }
        let execution = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_execution_context());
        let goal = self.active_step_goal(execution.as_ref())?;
        let turn_id = crate::step_snapshot::admitted_turn_id(
            execution.as_ref(),
            self.session_input_stream
                .active_turn_id()
                .map(|turn| turn.to_string()),
        )?;
        let inventory = self.api_client.context_inventory();
        if request
            .budget
            .compiled_provider_registry_revision
            .is_some_and(|revision| revision != inventory.provider_registry_revision)
        {
            return Err(RuntimeError::new("provider_profile_changed_after_compile"));
        }
        let identity = harness_contract::step::StepIdentity {
            step_id: uuid::Uuid::new_v4().to_string(),
            session_id: self.session_id().to_string(),
            turn_id,
            execution_id: execution
                .as_ref()
                .map(|context| context.execution_id.clone()),
            actor_ref: self
                .execution_identity
                .as_ref()
                .map(|identity| identity.principal_id().to_string()),
            run_ref: self
                .execution_identity
                .as_ref()
                .and_then(|identity| identity.agent_run_id().map(str::to_string)),
            attempt: None,
            epoch: execution.as_ref().map(|context| context.turn_id.clone()),
            goal_ref: goal.as_ref().map(|goal| goal.id.clone()),
            program_ref: goal.as_ref().and_then(|goal| {
                goal.execution_binding
                    .as_ref()
                    .map(|binding| binding.agentic_program_id.clone())
            }),
            task_ref: self
                .execution_identity
                .as_ref()
                .and_then(|identity| identity.task_id().map(str::to_string)),
            build_ref: option_env!("COWD_GIT_SHA")
                .unwrap_or("unverified")
                .to_string(),
        };
        let mut revisions = std::collections::BTreeMap::from([
            ("policy".to_string(), policy.revision.to_string()),
            (
                "provider_registry".to_string(),
                inventory.provider_registry_revision.to_string(),
            ),
            (
                "history".to_string(),
                request.messages.cursor().revision.to_string(),
            ),
        ]);
        if let Some(goal) = goal {
            revisions.insert(
                "goal_spec_observed".to_string(),
                format!("{}:{}", goal.spec_revision, goal.spec_digest),
            );
        }
        let request_material = crate::step_snapshot::request_material(request);
        let step = Arc::new(crate::step_snapshot::BoundStep::bind(
            identity,
            lease,
            &advertised,
            &serde_json::json!({"model": request.model, "reasoning_effort": request.reasoning_effort_override}),
            &request_material,
            revisions,
            chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(3_600_000),
        )?);
        let store = self.runtime_event_store.as_ref().ok_or_else(|| {
            RuntimeError::new(
                "production step requires durable request binding before provider dispatch",
            )
        })?;
        store
            .append(crate::runtime_event_store::RuntimeEventInput {
                stream_id: format!("session:{}", self.session_id()),
                scope: crate::runtime_event_store::RuntimeEventScope::Session,
                kind: "model.step_bound".to_string(),
                status: Some("prepared".to_string()),
                actor: Some("conversation_runtime".to_string()),
                refs: vec![crate::runtime_event_store::RuntimeEventRef {
                    kind: "step".to_string(),
                    id: step.snapshot().identity.step_id.clone(),
                }],
                payload: serde_json::to_value(step.snapshot())
                    .map_err(|error| RuntimeError::new(error.to_string()))?,
            })
            .map_err(|error| {
                RuntimeError::new(format!(
                    "persist step binding before provider dispatch: {error}"
                ))
            })?;
        request.preparation.bind_policy_source(
            self.permission_policy.execution_policy_control().clone(),
            policy.revision,
        );
        request.bound_step = Some(Arc::clone(&step));
        Ok(Some(step))
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

    /// Make one real tool call mandatory without restricting which tool.
    ///
    /// The allowlist and the required flag are independent fields, and conflating
    /// them is what broke run one26. Forcing a tool call is how Runtime says "a
    /// prose answer will not be delivered"; narrowing the tool set is how it says
    /// "you may only use these". The second is a much stronger claim, and it was
    /// being made on the strength of the first's justification.
    ///
    /// one26 needed `agent_invite` to staff a Team before it could publish the
    /// owed Task. The allowlist held only the rung's closing tool plus two reads,
    /// so the call was refused, the whole frame was discarded with no per-call
    /// record, and the model re-issued it sixteen times.
    pub(crate) fn require_next_model_tool_call(&self) {
        self.next_model_tool_required.store(true, Ordering::SeqCst);
    }

    /// Test support for proving that every small Agent action remains
    /// available when a collaboration obligation requires a real action.
    #[cfg(test)]
    pub(crate) fn require_next_agent_action(&self) {
        self.require_next_model_tool_action(
            harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                .iter()
                .map(|tool| (*tool).to_string()),
        );
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
    /// path, this request carries only the minimal paired ToolUse/ToolResult
    /// evidence history, not exploratory text or failed continuation state.
    /// This lets Runtime prove ProviderModel observation without reintroducing
    /// the loop-inducing transcript.
    pub(crate) async fn execute_clean_terminal_synthesis(
        &mut self,
        objective: &str,
        evidence: &str,
        evidence_history: Vec<ConversationMessage>,
    ) -> Result<ModelStepResult, RuntimeError> {
        // This owner creates an isolated reduction request directly rather
        // than passing through `execute_model_step`, so it must apply the
        // request-local reasoning overlay itself. Otherwise reasoning-capable
        // providers may spend the terminal budget on private reasoning and
        // produce no user-visible answer after evidence has been committed.
        self.require_next_model_reasoning_effort("none");
        let evidence_instruction =
            clean_terminal_evidence_instruction(evidence, !evidence_history.is_empty());
        let mut messages = evidence_history;
        messages.push(ConversationMessage::user_text(format!(
            "Original objective:\n{objective}\n\n{evidence_instruction}Return the final answer now."
        )));
        let messages: HistoryView = messages.into();
        let system_section = "## Clean terminal synthesis\n\
                 Produce the final user-facing answer for the supplied objective from the checked \
                 evidence receipts only. This request exposes no executable tools, so answer from \
                 the receipts you already hold: do not emit function calls, simulated tool markup, \
                 or plans to inspect more data. Give the best supported conclusion now and state \
                 unresolved facts explicitly.";
        self.execute_terminal_provider_step(
            objective,
            system_section,
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
        self.next_model_tool_required.store(false, Ordering::SeqCst);
        self.api_client.configure_tool_choice(false, None);

        let mut prompt = PromptAssembly::new(self.system_prompt.clone());
        // A terminal synthesis is its own request with its own system section, so
        // it shares no prefix with the model steps that preceded it; it keeps its
        // own stamp rather than reaching for a turn carrier it cannot access.
        let clock_stamp = crate::prompt::RuntimeClockStamp::now();
        prompt.push_volatile_tail(crate::prompt::runtime_clock_section_at(&clock_stamp));
        prompt.push_runtime_context(system_section);
        let inventory = self.api_client.context_inventory();
        let mut last_error = None;
        let mut models_tried = Vec::new();
        let mut presentation_attempt_sequence = 0_u32;
        let mut provider_attempt_sequence = 0_u32;
        let mut billed_usage = TokenUsage::default();
        let mut billed_usage_observed = true;
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
                if let Err(error) = self.prepare_timed_provider_budget(&mut request).await {
                    last_error = Some(error);
                    break 'candidate_attempt;
                }
                let prepared_output = request.budget.requested_output_tokens;
                let reserved_input = request
                    .budget
                    .input_total_tokens()
                    .saturating_add(request.budget.protocol_overhead_tokens)
                    .saturating_add(request.budget.safety_margin_tokens);
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

                if request.budget.requested_output_tokens != prepared_output {
                    request.preparation.invalidate();
                    if let Err(error) = self.prepare_timed_provider_budget(&mut request).await {
                        last_error = Some(error);
                        break 'candidate_attempt;
                    }
                    if request
                        .budget
                        .input_total_tokens()
                        .saturating_add(request.budget.protocol_overhead_tokens)
                        > reserved_input
                    {
                        last_error = Some(RuntimeError::context_capacity(
                            crate::context_ledger::ContextCapacityFailure::RenderOverflow,
                            "reprepared wire exceeds original token input reservation",
                        ));
                        break 'candidate_attempt;
                    }
                }
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
                            .input_total_tokens()
                            .saturating_add(request.budget.protocol_overhead_tokens),
                    });
                }
                let request_sequence = self.session_head().await.message_count;
                provider_attempt_sequence = provider_attempt_sequence.saturating_add(1);
                let execution_epoch = self
                    .cowd_bus
                    .as_ref()
                    .and_then(|bus| bus.current_execution_context());
                request.provider_evidence_context = Some(crate::ProviderRequestEvidenceContext {
                    session_id: self.session_id().to_string(),
                    execution_id: execution_epoch
                        .as_ref()
                        .map(|context| context.execution_id.clone())
                        .unwrap_or_default(),
                    turn_id: execution_epoch
                        .as_ref()
                        .map(|context| context.turn_id.clone())
                        .unwrap_or_default(),
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
                tracing::info!(
                    target: "cowd::terminal_provider",
                    model = %model,
                    attempt = provider_attempt_sequence,
                    phase = "admission",
                    "terminal provider phase entered"
                );
                let (provider_lease, provider_queue_wait) =
                    self.acquire_provider_capacity(&model, &request).await?;
                let provider_lease = provider_lease.map(Arc::new);
                request
                    .preparation
                    .retain_dispatch_lease(provider_lease.clone());
                tracing::info!(
                    target: "cowd::terminal_provider",
                    model = %model,
                    attempt = provider_attempt_sequence,
                    phase = "stream",
                    queue_wait_ms = u64::try_from(provider_queue_wait.as_millis()).unwrap_or(u64::MAX),
                    idle_timeout_ms = u64::try_from(transport_policy.idle_timeout.as_millis()).unwrap_or(u64::MAX),
                    heartbeat_grace_ms = u64::try_from(transport_policy.heartbeat_grace.as_millis()).unwrap_or(u64::MAX),
                    "terminal provider phase entered"
                );
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
                let bound_step = self.bind_provider_step(&mut request)?;
                let mut reducer = ModelStreamReducer::new(
                    self.cowd_bus.clone(),
                    self.runtime_event_store.clone(),
                    self.session_id().to_string(),
                )
                .with_bound_step(bound_step);
                if let (Some((presentation_id, _, _, _)), Some(attempt_id)) =
                    (presentation, terminal_attempt_id.as_deref())
                {
                    reducer = reducer.with_terminal_presentation(presentation_id, attempt_id);
                }
                let source_readset = request.prompt.memory_source_readset();
                let source_turn = self.memory_source_observation_turn(&source_readset)?;
                token_reservations.mark_dispatched();
                request
                    .preparation
                    .retain_cancellation(cancellation.clone());
                let ApiClientStream {
                    events,
                    transport_activity,
                    execution,
                } = self.api_client.stream_with_transport_activity(request);
                let events = observe_memory_source_stream(
                    events,
                    self.memory_source_uses.clone(),
                    source_turn,
                    source_readset,
                );
                let stream_run = consume_owned_provider_stream(
                    events,
                    cancellation,
                    Some(ProviderStreamTimeoutPolicy {
                        idle: transport_policy.idle_timeout,
                        heartbeat_grace: transport_policy.heartbeat_grace,
                    }),
                    reducer,
                    None,
                    transport_activity,
                    execution,
                )
                .await;
                tracing::info!(
                    target: "cowd::terminal_provider",
                    model = %model,
                    attempt = provider_attempt_sequence,
                    phase = "stream_finished",
                    elapsed_ms = u64::try_from(stream_started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    failed = stream_run.failure.is_some(),
                    "terminal provider phase finished"
                );
                self.record_provider_resource_outcome(
                    provider_lease.as_deref(),
                    provider_queue_wait,
                    stream_started.elapsed(),
                    stream_run.resource_result_class,
                    stream_run
                        .failure
                        .as_ref()
                        .map(RuntimeError::provider_failure_scope),
                );
                drop(provider_lease);
                let observed_usage = stream_run
                    .collected
                    .observed_usage(stream_run.failure.is_none());
                let CollectedProviderStream {
                    text,
                    public_reasoning,
                    private_reasoning,
                    signature,
                    calls,
                    usage,
                    usage_observed: _,
                    effective_provider_identity,
                    first_event_at,
                    first_text_at: _,
                    stop_reason: _,
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
                add_token_usage(&mut billed_usage, usage);
                billed_usage_observed &= observed_usage.is_some();
                token_reservations
                    .reconcile(observed_usage)
                    .map_err(|error| {
                        error.with_provider_usage(billed_usage_observed.then_some(billed_usage))
                    })?;
                if let Some(observed) = observed_usage {
                    self.reconcile_provider_context_usage(observed);
                }
                if let Some(error) = stream_run.failure {
                    let error = error
                        .with_provider_account_key(provider_account_key.clone())
                        .with_provider_usage(billed_usage_observed.then_some(billed_usage));
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
                    })
                    .with_provider_usage(billed_usage_observed.then_some(billed_usage));
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
                        input_interpretation: None,
                        intent: classify_model_step_intent(text, calls),
                        assistant_message: ConversationMessage {
                            role: crate::session::MessageRole::Assistant,
                            blocks,
                            usage: observed_usage,
                        },
                        preflight_tool_results: Vec::new(),
                        usage: billed_usage,
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
    ) -> Result<TerminalProviderAnswer, RuntimeError> {
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
            return Err(
                RuntimeError::new("failure explanation provider returned no text")
                    .with_provider_usage(step.assistant_message.usage),
            );
        }
        Ok(TerminalProviderAnswer {
            text,
            model: step.model,
            models_used: step.models_used,
            provider_attempt_id: terminal_attempt_id.unwrap_or_else(|| attempt_id.to_string()),
            usage: step.usage,
            first_token_latency_ms: step.first_token_latency_ms,
            active_stream_duration_ms: step.active_stream_duration_ms,
            wall_duration_ms: step.wall_duration_ms,
        })
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
    ) -> Result<TerminalProviderAnswer, RuntimeError> {
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
            return Err(
                RuntimeError::new("collaboration synthesizer returned no text")
                    .with_provider_usage(step.assistant_message.usage),
            );
        }
        Ok(TerminalProviderAnswer {
            text,
            model: step.model,
            models_used: step.models_used,
            provider_attempt_id: terminal_attempt_id.unwrap_or_else(|| attempt_id.to_string()),
            usage: step.usage,
            first_token_latency_ms: step.first_token_latency_ms,
            active_stream_duration_ms: step.active_stream_duration_ms,
            wall_duration_ms: step.wall_duration_ms,
        })
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
        let resource_handles = self.provider_resource_handles();
        let lease = if let Some(manager) = &resource_handles.manager {
            let estimated_tokens = request
                .budget
                .input_total_tokens()
                .saturating_add(request.budget.protocol_overhead_tokens)
                .saturating_add(request.budget.requested_output_tokens);
            let admission = crate::provider_resources::provider_leaf_admission(
                &resource_handles
                    .config
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                self.api_client.provider_name_for_model(model).as_deref(),
                model,
                estimated_tokens,
                self.execution_service_class,
                self.session_id(),
                now_ms()
                    .saturating_add(provider_admission_timeout_ms(self.execution_service_class)),
            );
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
        failure_scope: Option<model_protocol::provider_failure::ProviderFailureScope>,
    ) {
        let handles = self.provider_resource_handles();
        let (Some(manager), Some(lease)) = (&handles.manager, lease) else {
            return;
        };
        // Balance/credentials/configuration are route availability, not
        // evidence that the shared provider pool is overloaded. Preserve the
        // failure in provider/turn evidence without teaching the capacity
        // controller to throttle unrelated, healthy accounts. Explicit
        // overload and timeout signals remain authoritative even with scope.
        if failure_scope.is_some_and(|scope| scope.route_is_unavailable())
            && result_class == crate::execution_core::graph::ResourceResultClass::Failed
        {
            crate::execution_core::performance::observe_count(
                "provider_noncapacity_failure_excluded_total",
                1,
            );
            return;
        }
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
        let forbids_tool_use = self
            .active_turn_strategy()
            .is_some_and(|strategy| strategy.decision.strategy.understanding.forbids_tool_use);
        self.execute_model_step_with_early_dispatch(
            user_input,
            first_step,
            None,
            false,
            forbids_tool_use,
        )
        .await
    }

    /// One clock for a whole model-step invocation.
    ///
    /// Semantic compaction can rebuild the prompt mid-step (the second
    /// `prepare_*` call), and a stamp minted there would re-render a moving clock
    /// segment between two requests that were supposed to share a prefix. Kept as
    /// its own function so the reasoning does not add lines to the step itself.
    fn turn_step_clock(&self) -> crate::prompt::RuntimeClockStamp {
        crate::prompt::RuntimeClockStamp::now()
    }

    /// Prepare this step's prompt from the step's shared clock stamp.
    ///
    /// Extracted so the two preparations a step makes (the initial one and the
    /// post-compaction rebuild) cannot drift apart on the clock, and so the call
    /// sites stop paying five lines each for the same four arguments.
    async fn prepare_step_prompt(
        &self,
        user_input: &str,
        budget: u64,
        items: Vec<ContextItem>,
        clock_stamp: &crate::prompt::RuntimeClockStamp,
    ) -> Result<PromptAssembly, RuntimeError> {
        let policy_revision = self
            .permission_policy
            .execution_policy_control()
            .snapshot()
            .revision;
        let mut prompt = self
            .prepare_reality_context_with_budget_and_items(user_input, budget, items, clock_stamp)
            .await?;
        prompt.context_policy_revision = Some(policy_revision);
        Ok(prompt)
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
        explicitly_forbids_tool_use: bool,
    ) -> Result<ModelStepResult, RuntimeError> {
        self.execute_model_step_for_purpose(
            user_input,
            first_step,
            early_dispatcher,
            provider_retry_fenced,
            explicitly_forbids_tool_use,
            None,
        )
        .await
    }

    pub(crate) async fn execute_user_input_interpretation(
        &mut self,
        user_input: &str,
        first_step: bool,
        provider_retry_fenced: bool,
        slots: usize,
        framing: &str,
    ) -> Result<ModelStepResult, RuntimeError> {
        let mut step = self
            .execute_model_step_for_purpose(
                user_input,
                first_step,
                None,
                provider_retry_fenced,
                false,
                Some((slots, framing)),
            )
            .await?;
        step.input_interpretation = Some(match &step.intent {
            ModelStepIntent::FinalAnswer { text }
                if step.early_tool_receipts.is_empty() && step.preflight_tool_results.is_empty() =>
                harness_contract::input_disposition::ModelInputDispositionBatch::decode_business_proposal(text, slots),
            _ => Err("input interpretation emitted execution instead of a business proposal".into()),
        });
        Ok(step)
    }

    pub(crate) async fn execute_model_step_for_purpose(
        &mut self,
        user_input: &str,
        first_step: bool,
        early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
        provider_retry_fenced: bool,
        explicitly_forbids_tool_use: bool,
        interpretation: Option<(usize, &str)>,
    ) -> Result<ModelStepResult, RuntimeError> {
        let clock_stamp = self.turn_step_clock();
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new(
                "turn cancelled before provider execution",
            ));
        }
        // PLAN D1b: verdicts the stream guard recorded while the previous provider
        // stream was polled. Each becomes a NON-FORCING push-forward nudge on this
        // request - refocus, summarize, request commit, or ask. The runtime never
        // disables tools and never ends the turn here: only the model declares a
        // task finished or impossible (PLAN D1).
        if let Some(item) = liveness_stall_context_item(self.api_client.take_liveness_stalls()) {
            self.push_next_model_context_item(item);
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
        if interpretation.is_none() && !decision.executable {
            return Err(RuntimeError::new(format!(
                "runtime strategy is not executable: {}",
                decision.blocked_reasons.join("; ")
            )));
        }

        let requested_text_only = self.next_model_text_only.swap(false, Ordering::SeqCst);
        let text_only_response = interpretation.is_none() && requested_text_only;
        let interpretation_only = interpretation.is_some();
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
        let one_shot_reasoning_effort = self
            .next_model_reasoning_effort
            .lock()
            .ok()
            .and_then(|mut effort| effort.take());
        // Stable delegated wire schemas may intentionally include tools that
        // are outside this request's logical exposure. A terminal or
        // user-forbidden tool boundary must therefore carry an explicit empty
        // activation ceiling instead of treating hidden schemas as eligible
        // deferred calls.
        let tool_activation_ceiling =
            if text_only_response || explicitly_forbids_tool_use || interpretation_only {
                Some(BTreeSet::new())
            } else {
                one_shot_tool_allowlist.clone()
            };
        let discovery_activation_notice =
            if text_only_response || explicitly_forbids_tool_use || interpretation_only {
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
        if first_step {
            self.skill_exposed_tool_ids
                .lock()
                .map_err(|_| RuntimeError::new("Skill tool provenance is poisoned"))?
                .clear();
        }
        if !active_skill_tool_refs.is_empty() {
            // Pure exposure candidate only. bind_provider_step binds the
            // actual surviving schema contribution to its original pins.
            let prior = exposure
                .bootstrap
                .iter()
                .chain(exposure.active.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
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
            let current = exposure
                .bootstrap
                .iter()
                .chain(exposure.active.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            self.skill_exposed_tool_ids
                .lock()
                .map_err(|_| RuntimeError::new("Skill tool provenance is poisoned"))?
                .extend(current.difference(&prior).cloned());
            tracing::info!(
                activated = ?activation.activated_ids().collect::<Vec<_>>(),
                "runtime Skill tool references applied to the current provider request"
            );
        }
        let active_strategy = self.active_turn_strategy();
        let collaboration_obligation = active_strategy
            .as_ref()
            .and_then(|state| state.decision.collaboration_obligation.clone());
        // Expose the Agent-first actions whenever collaboration is in play, not
        // only when the operator froze a count: the model must always be able
        // to see the capability it may choose, even when no obligation exists.
        let collaboration_visible = active_strategy.as_ref().is_some_and(|state| {
            collaboration_obligation.is_some()
                || state.decision.strategy.understanding.requests_multi_agent
                || state.decision.pattern() == harness_contract::core::ExecutionPattern::Collaborate
        });
        if collaboration_visible {
            for tool in harness_contract::agent_action::AGENT_ACTION_TOOL_IDS {
                exposure.active.insert((*tool).to_string());
                exposure.deferred.remove(*tool);
            }
            exposure.reason = match collaboration_obligation.as_ref() {
                Some(obligation) => format!(
                    "operator Team obligation (minimum {}) activates Agent-first actions",
                    obligation.minimum_team_count
                ),
                None => {
                    "collaboration is available; Agent-first actions exposed for the model to choose"
                        .to_string()
                }
            };
            tracing::info!(
                team_required = collaboration_obligation.is_some(),
                minimum_team_count = collaboration_obligation.as_ref().map(|o| o.minimum_team_count),
                active = ?exposure.active,
                "Agent-first actions exposed for a collaboration-capable turn"
            );
        }
        let one_shot_tool_overlay =
            one_shot_tool_allowlist.is_some() || discovery_activation_notice.is_some();
        let mut exposure =
            if text_only_response || explicitly_forbids_tool_use || interpretation_only {
                ToolExposureState {
                    catalog_revision: exposure.catalog_revision,
                    bootstrap: Default::default(),
                    active: Default::default(),
                    deferred: available_tools.iter().cloned().collect(),
                    reason: if interpretation_only {
                        "nonterminal user-input interpretation has no executable tools".into()
                    } else if text_only_response {
                        "governed low-novelty checkpoint requires a text-only conclusion"
                            .to_string()
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
        self.stamp_tool_exposure_revision(&mut exposure);
        // A text-only checkpoint is a one-request overlay. Keep the normal
        // catalog state for discovery/projection, while still sending an
        // explicit empty schema set for this provider request.
        if !text_only_response
            && !explicitly_forbids_tool_use
            && !interpretation_only
            && !one_shot_tool_overlay
        {
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
        // A `required` tool choice is invalid (`400 tool_choice requires at
        // least one tool`) when no tool schema is exposed; fail closed to an
        // omitted choice instead of sending an invalid request.
        let final_tool_ids = self.api_client.step_tool_definitions().map(|definitions| {
            definitions.into_iter().map(|definition| definition.name).collect::<BTreeSet<_>>()
        });
        let final_callable_ids = final_tool_ids.as_ref().map(|schemas| {
            schemas
                .intersection(&exposed_tool_ids)
                .cloned()
                .collect::<BTreeSet<_>>()
        });
        let mut tool_choice_required = one_shot_tool_required
            && final_callable_ids.as_ref().is_some_and(|names| !names.is_empty())
            && one_shot_required_tool_name.as_ref().is_none_or(|name| {
                final_callable_ids.as_ref().is_some_and(|names| names.contains(name))
            });
        let mut tool_choice_required_tool_name = if tool_choice_required {
            one_shot_required_tool_name
        } else {
            None
        };
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
        let execution_at_binding = self
            .cowd_bus
            .as_ref()
            .and_then(|bus| bus.current_execution_context());
        let current_goal_binding = self
            .active_step_goal(execution_at_binding.as_ref())?
            .as_ref()
            .and_then(closure_goal_binding);
        let discoverable = exposure
            .deferred
            .iter()
            .filter(|tool| {
                tool_activation_ceiling
                    .as_ref()
                    .is_none_or(|ceiling| ceiling.contains(*tool))
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        if !bind_final_closure_tool_context(
            &mut one_shot_context_items,
            final_tool_ids.as_ref(),
            &exposed_tool_ids,
            &discoverable,
            inventory,
            current_goal_binding.as_ref(),
        ) {
            tool_choice_required = false;
            tool_choice_required_tool_name = None;
        }
        let context_select_started = Instant::now();
        let mut prompt = self
            .prepare_step_prompt(
                user_input,
                collection_budget,
                one_shot_context_items.clone(),
                &clock_stamp,
            )
            .await?;
        crate::execution_core::performance::observe_duration(
            "context_select_ms",
            context_select_started.elapsed(),
        );
        let evidence = crate::evidence_planner::plan_evidence_with_understanding(
            user_input,
            &decision.strategy.understanding,
        );
        // A small model window cannot afford repeating the full deferred
        // catalog contract on every continuation. Keep the callable schema
        // and authority invariants, while compacting duplicated discovery
        // prose only under measured pressure.
        let compact_runtime_guidance = collection_budget <= 32_768;
        let apply_runtime_controls = |prompt: &mut PromptAssembly| {
            // Capability-conditional usage guidance: only for tools that are
            // ACTUALLY callable on this request. It lives after the dynamic
            // boundary by construction, because the callable set varies per
            // request — the small capability-independent core stays in the
            // shared cache prefix.
            let callable = exposure
                .projection(0)
                .active_ids
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            let tool_shapes = crate::capability_manifest::render_tool_guidance(&callable);
            if !tool_shapes.is_empty() {
                prompt.push_runtime_context(tool_shapes);
            }
            prompt.push_runtime_context(crate::evidence_planner::evidence_plan_prompt(&evidence));
            prompt.push_runtime_context(
                crate::execution_core::runtime_execution_guidance_prompt_with_tool_exposure_mode(
                    &decision,
                    Some(&exposure.projection(0)),
                    compact_runtime_guidance,
                ),
            );
            if let Some(activated_ids) = discovery_activation_notice.as_ref() {
                prompt.push_runtime_context(format!(
                    "## Tool discovery handoff\nThis is the immediate automatic continuation of the same user turn. tool_search already completed successfully and is intentionally unavailable for this request. Newly activated native function schemas: [{}]. Continue the original task now by invoking the relevant activated schema directly when evidence or action is still required. Do not ask the user to resend the request and do not claim that a new user turn is needed.",
                    activated_ids.iter().cloned().collect::<Vec<_>>().join(", ")
                ));
            }
            if let Some((_, framing)) = interpretation {
                prompt.push_runtime_context(format!(
                    "## Internal user-input interpretation (NOT a final response)\nInterpret the Runtime-labelled authenticated user inputs against the current Goal. Return exactly one strict JSON ModelInputDispositionBatch, no markdown or tools. Cover every input_slot once. Use amend_current_turn/supplement with business_revision containing the complete CURRENT commitments and constraints, plus retired_criteria/retired_obligations (only known refs); objective is the new full current business objective. Retain still-effective requirements; remove only those the user changed. Use progress_or_control/progress without business_revision for observation-only input, or clarify without business_revision for genuine ambiguity. Never emit graph_plan, session_target, permissions or execution identities. Commitment kinds: workspace_write (path string or null for autonomous filename), forbid_workspace_write (path string for an object or null for all writes), forbid_tools, team_count (minimum, maximum null unless exact); each has source byte span start/end in the supplied source text (citation only, not permission). Ordinary business interpretation changes no tool authorization.\n{framing}"
                ));
            }
            if text_only_response {
                prompt.push_runtime_context(
                    "## Terminal response boundary\nThis request is a text-only terminal checkpoint. The executable tool set for this request is empty, regardless of any earlier capability inventory or historical tool receipts in the context. Do not emit native function calls, simulated tool markup, JSON commands, new plans, or more work. Use only retained evidence receipts to produce the best final answer now. State unresolved facts explicitly instead of performing another search.".to_string(),
                );
            }
            if explicitly_forbids_tool_use {
                prompt.push_runtime_context(
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
                "{}: explicit_collaboration_constraint={}",
                decision.pattern().as_str(),
                decision.collaboration_obligation.is_some()
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
        // Proactive context reduction (K3): when the fixed history already
        // occupies most of the smallest candidate window, the next turn has no
        // room and a hard "cannot fit" failure is imminent. Compact ahead of
        // that failure through the same semantic-checkpoint pipeline so long
        // autonomous runs keep a smaller, cheaper working set. This is a
        // measured pre-emption, not a fixed transcript-ratio timer.
        let session_tokens = {
            let session = self.session.read().await;
            crate::compact::estimate_session_tokens(&session)
        };
        let smallest_window = model_candidates
            .iter()
            .map(|model| self.context_window_for_model(model))
            .min()
            .unwrap_or(0);
        let proactive_compaction_due = smallest_window > 0
            && session_tokens.saturating_mul(100)
                >= (smallest_window as usize).saturating_mul(PROACTIVE_COMPACTION_PERCENT);
        if no_candidate_can_fit || proactive_compaction_due {
            if let Some(turn_id) = self.session_input_stream.active_turn_id() {
                let consumed = self
                    .consume_runtime_input_records(&turn_id, TurnInputCheckpoint::BeforeCompaction);
                one_shot_context_items.extend(crate::turn_inbox::checkpoint_context_items(
                    TurnInputCheckpoint::BeforeCompaction,
                    &consumed,
                ));
            }
            let threshold = if no_candidate_can_fit {
                1
            } else {
                (smallest_window as usize).saturating_mul(PROACTIVE_COMPACTION_PERCENT) / 100
            };
            let compaction = self
                .compact_session_with_checkpoint(self.compaction_config_for_session(threshold))
                .await?;
            match compaction {
                None if no_candidate_can_fit => {
                    return Err(RuntimeError::new(
                        "all provider candidates reject the required request context and no semantic compaction boundary is available",
                    ));
                }
                None => {
                    // Proactive trigger found no compaction boundary; the request
                    // still fits, so keep the original (uncompacted) request.
                }
                Some(compaction) => {
                    request_messages = self.session.read().await.messages_view();
                    prompt = self
                        .prepare_step_prompt(
                            user_input,
                            collection_budget,
                            one_shot_context_items,
                            &clock_stamp,
                        )
                        .await?;
                    apply_runtime_controls(&mut prompt);
                    self.record_context_event(
                        "context_preflight_compaction",
                        "runtime",
                        if no_candidate_can_fit {
                            "all provider candidates required semantic compaction before request dispatch"
                        } else {
                            "history approached the smallest candidate window; semantic compaction pre-empted the hard limit"
                        },
                        9,
                    );
                    if let Ok(mut preflight_compaction) = self.turn_preflight_compaction.lock() {
                        *preflight_compaction = Some(compaction);
                    }
                }
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
            self.api_client.configure_tool_choice(
                tool_choice_required,
                tool_choice_required_tool_name.clone(),
            );
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
            if let Err(error) = self.prepare_timed_provider_budget(&mut request).await {
                last_error = Some(error);
                continue;
            }
            let prepared_output = request.budget.requested_output_tokens;
            let reserved_input = request
                .budget
                .input_total_tokens()
                .saturating_add(request.budget.protocol_overhead_tokens)
                .saturating_add(request.budget.safety_margin_tokens);
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
            if request.budget.requested_output_tokens != prepared_output {
                request.preparation.invalidate();
                if let Err(error) = self.prepare_timed_provider_budget(&mut request).await {
                    last_error = Some(error);
                    continue;
                }
                if request
                    .budget
                    .input_total_tokens()
                    .saturating_add(request.budget.protocol_overhead_tokens)
                    > reserved_input
                {
                    last_error = Some(RuntimeError::context_capacity(
                        crate::context_ledger::ContextCapacityFailure::RenderOverflow,
                        "reprepared wire exceeds original token input reservation",
                    ));
                    continue;
                }
            }

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
                        .input_total_tokens()
                        .saturating_add(request.budget.protocol_overhead_tokens),
                });
            }
            let request_sequence = self.session_head().await.message_count;
            provider_attempt_sequence = provider_attempt_sequence.saturating_add(1);
            let execution_epoch = self
                .cowd_bus
                .as_ref()
                .and_then(|bus| bus.current_execution_context());
            request.provider_evidence_context = Some(crate::ProviderRequestEvidenceContext {
                session_id: self.session_id().to_string(),
                execution_id: execution_epoch
                    .as_ref()
                    .map(|context| context.execution_id.clone())
                    .unwrap_or_default(),
                turn_id: execution_epoch
                    .as_ref()
                    .map(|context| context.turn_id.clone())
                    .unwrap_or_default(),
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
            let provider_lease = provider_lease.map(Arc::new);
            request
                .preparation
                .retain_dispatch_lease(provider_lease.clone());
            let provider_started = Instant::now();
            let stream_started = Instant::now();
            let bound_step = self.bind_provider_step(&mut request)?;
            let native_context = request.provider_evidence_context.clone();
            let native_step = bound_step.as_ref().map(|step| step.snapshot().clone());
            self.native_wire_receipts
                .lock()
                .map_err(|_| RuntimeError::new("native wire receipt projection poisoned"))?
                .clear();
            let reducer = ModelStreamReducer::new(
                self.cowd_bus.clone(),
                self.runtime_event_store.clone(),
                self.session_id().to_string(),
            )
            .with_bound_step(bound_step);
            let source_readset = request.prompt.memory_source_readset();
            let source_turn = self.memory_source_observation_turn(&source_readset)?;
            token_reservations.mark_dispatched();
            request
                .preparation
                .retain_cancellation(cancellation.clone());
            let ApiClientStream {
                events,
                transport_activity,
                execution,
            } = self.api_client.stream_with_transport_activity(request);
            let events = observe_memory_source_stream(
                events,
                self.memory_source_uses.clone(),
                source_turn,
                source_readset,
            );
            let stream_run = consume_owned_provider_stream(
                events,
                cancellation,
                Some(ProviderStreamTimeoutPolicy {
                    idle: idle_timeout,
                    heartbeat_grace,
                }),
                reducer,
                early_dispatcher.clone(),
                transport_activity,
                execution,
            )
            .await;
            let resource_result_class = stream_run.resource_result_class;
            let observed_usage = stream_run
                .collected
                .observed_usage(stream_run.failure.is_none());
            let CollectedProviderStream {
                text,
                public_reasoning,
                private_reasoning,
                signature,
                mut calls,
                usage,
                usage_observed: _,
                effective_provider_identity,
                first_event_at,
                first_text_at,
                stop_reason,
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
            // The inputs this request delivered are observed facts, and they
            // are settled here rather than at the end of the response
            // handling: a refused or malformed output frame ends the step
            // early, and it must not un-read an input the model did read.
            if let (Some(context), None) = (native_context.as_ref(), stream_run.failure.as_ref()) {
                self.settle_observed_inputs(
                    packed_model_observations.clone(),
                    effective_model.as_deref().unwrap_or(model.as_str()),
                    context,
                    native_step.as_ref(),
                )?;
            }
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
            if let Some(observed) = observed_usage {
                crate::execution_core::performance::observe_count(
                    "provider_input_tokens",
                    u64::from(observed.input_tokens),
                );
                crate::execution_core::performance::observe_count(
                    "provider_output_tokens",
                    u64::from(observed.output_tokens),
                );
                let service_ms = provider_started.elapsed().as_millis().max(1) as u64;
                crate::execution_core::performance::observe_count(
                    "provider_output_tokens_per_second",
                    u64::from(observed.output_tokens).saturating_mul(1_000) / service_ms,
                );
            } else {
                crate::execution_core::performance::observe_count("provider_usage_missing", 1);
            }
            Self::observe_provider_downstream_overload(resource_result_class);
            self.record_provider_resource_outcome(
                provider_lease.as_deref(),
                provider_queue_wait,
                provider_started.elapsed(),
                resource_result_class,
                stream_run
                    .failure
                    .as_ref()
                    .map(RuntimeError::provider_failure_scope),
            );
            drop(provider_lease);
            token_reservations
                .reconcile(observed_usage)
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

            // A `length` truncation used to be silently returned. The terminal
            // reason is recorded on the collected stream and propagated to the
            // model-step result, so the caller (turn assembly) can decide
            // whether to allow more output headroom. Deliberately no prompt
            // reshaping: injecting an assistant prefix makes models repeat or
            // contradict themselves, which costs capability (PLAN W2 Step4).
            if stop_reason.as_deref() == Some("length") {
                crate::execution_core::performance::observe_count(
                    "provider_step_truncated_at_output_ceiling_total",
                    1,
                );
                tracing::debug!(
                    model,
                    "model step hit the provider output ceiling; terminal reason propagated"
                );
            }
            canonicalize_model_tool_names(&mut calls, self.tool_executor.as_ref());
            // Repair the one-shot shape corruption before any consumer reads
            // these bytes. The coercion lives in the executor that owns the
            // pinned schema, and this boundary is the first place that schema
            // is consulted for the frame - the transcript clone below, the
            // contract check further down and the governed plan all read
            // `calls`. Leaving the repair in the dispatch path instead meant
            // the contract check refused a frame the repair existed to fix.
            //
            // The check below validates these same bytes again. That is one
            // extra in-memory schema walk per call against a provider round
            // trip, paid so that no consumer can ever observe an unrepaired
            // frame regardless of what is inserted between here and there.
            for call in &mut calls {
                if let Ok(repaired) = self.tool_executor.validate_step_tool_input(
                    call.binding.as_ref(),
                    &call.name,
                    &call.input,
                ) {
                    if repaired != call.input {
                        // A repair that only closes a cut-off frame is not the
                        // same as one that fixes a quoting mistake: items were
                        // dropped. Record it and tell the model in-step, or the
                        // salvage becomes the swallowed work it was meant to
                        // prevent.
                        let salvaged = self
                            .tool_executor
                            .tool_input_coercions(&call.name, &call.input)
                            .into_iter()
                            .filter(|coercion| coercion.truncated)
                            .collect::<Vec<_>>();
                        tracing::info!(
                            tool_name = %call.name,
                            tool_call_id = %call.id,
                            "repaired the model's tool input shape at the schema boundary"
                        );
                        for coercion in &salvaged {
                            self.record_truncated_frame_salvage(&call.id, &call.name, coercion);
                        }
                        call.input = repaired;
                    }
                }
            }
            let requested_tool_call_count = calls.len();
            self.remember_native_tool_calls(&calls)?;
            let transcript_calls = calls.clone();
            // One shape for "the model asked for something that did not run",
            // shared with the whole-frame branches so a refusal is answered and
            // recorded the same way whichever branch found it.
            let mut rejected_tool_calls = Vec::<FrameRefusal>::new();
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
                // Unknown, unhealthy, or overlay-denied names remain closed:
                // they are never dispatched. If the same provider frame also
                // contains valid independent calls, reject only the invalid
                // members and preserve the useful work. Treating one invented
                // name as an atomic failure for the whole batch discards
                // valid Agent actions and can create a costly retry loop.
                if denied_by_overlay.is_empty() && activated.len() == unexposed_tool_names.len() {
                    // Fall through and execute the parsed calls. Activation
                    // remains durable for subsequent provider requests.
                } else {
                    let activated = activated.into_iter().collect::<BTreeSet<_>>();
                    let rejected_names = unexposed_tool_names
                        .iter()
                        .filter(|name| !activated.contains(*name))
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let has_executable_call = calls
                        .iter()
                        .any(|call| !rejected_names.contains(&call.name));
                    if has_executable_call {
                        let mut executable_calls = Vec::with_capacity(calls.len());
                        for call in calls {
                            if rejected_names.contains(&call.name) {
                                let overlay = denied_by_overlay.contains(&call.name);
                                rejected_tool_calls.push(FrameRefusal::new(
                                    call,
                                    if overlay {
                                        "the action is outside this request's governed tool allowlist"
                                    } else {
                                        "the action is unknown, unavailable, or outside this request's exposure lease"
                                    },
                                ));
                            } else {
                                executable_calls.push(call);
                            }
                        }
                        calls = executable_calls;
                    } else {
                        // Every call in this frame was refused, so the frame is
                        // discarded whole. Record each one before returning.
                        //
                        // The `has_executable_call` branch above already files a
                        // per-call reason; this branch used to file nothing, which
                        // left the model with a protocol-violation error that named
                        // the tools but not the calls, and no Runtime record of what
                        // it had asked for. Run one26 re-issued the same
                        // `agent_invite` sixteen times against exactly that silence.
                        //
                        // It also used to answer the model with a bare error and no
                        // tool result, which is the defect the invalid-arguments
                        // branch already had fixed: the model's own frame left no
                        // assistant message and no tool result, so nothing in its
                        // transcript said the call had not run. Same obligation,
                        // same treatment - the frame is answered in-step and the
                        // available-tool list still reaches the model.
                        let refusals = calls
                            .iter()
                            .map(|call| {
                                let overlay = denied_by_overlay.contains(&call.name);
                                FrameRefusal::new(
                                    call.clone(),
                                    if overlay {
                                        "the action is outside this request's governed tool allowlist"
                                    } else {
                                        "the action is unknown, unavailable, or outside this request's exposure lease"
                                    },
                                )
                            })
                            .collect::<Vec<_>>();
                        for refusal in &refusals {
                            tracing::warn!(
                                target: "cowd::tool_exposure",
                                tool = %refusal.call.name,
                                tool_call_id = %refusal.call.id,
                                input = %refusal.call.input.chars().take(200).collect::<String>(),
                                overlay_denied = denied_by_overlay.contains(&refusal.call.name),
                                "{}; the whole frame was discarded",
                                refusal.reason
                            );
                        }
                        let refusal_results = self.answer_refused_calls(&refusals);
                        if let Some(observed) = observed_usage {
                            self.reconcile_provider_context_usage(observed);
                            self.usage_tracker.record(observed);
                            if let Some(callback) = &self.tool_callback {
                                callback.on_usage(&observed);
                            }
                        }
                        // Name what IS available, not only what is not. The message
                        // used to list the refused names and stop, so a model that
                        // asked for a tool outside the lease learned nothing about
                        // where to go - one20 was refused four different Agent
                        // actions this way and one24 `grep_search`.
                        let available = {
                            let mut names = exposed_tool_ids
                                .iter()
                                .filter(|name| !unexposed_tool_names.contains(name))
                                .cloned()
                                .collect::<Vec<_>>();
                            names.sort();
                            if names.len() > 20 {
                                format!(
                                    "{} (and {} more)",
                                    names[..20].join(", "),
                                    names.len() - 20
                                )
                            } else if names.is_empty() {
                                "none".to_string()
                            } else {
                                names.join(", ")
                            }
                        };
                        // A deliberate text-only checkpoint is a different situation,
                        // not a stricter case of this one. Runtime asked for prose and
                        // the model emitted a call, so a tool result describing a shape
                        // to re-emit is advice it cannot take: the host's text-only
                        // recovery ladder owns that step and must keep it. Answering
                        // in-step is for a model that had schemas and picked one
                        // outside its lease.
                        //
                        // Gated on the checkpoint flag, not on an empty exposure set:
                        // a runtime that simply registers no tools still benefits from
                        // being told which tools exist, and one that was asked for text
                        // does not.
                        if text_only_response || interpretation_only {
                            return Err(RuntimeError::with_provider_failure_metadata(
                                format!(
                                    "tool_protocol_violation: provider emitted a tool call during a request that exposed no tool schemas: [{}]. Produce a normal visible final answer instead.",
                                    unexposed_tool_names.join(", ")
                                ),
                                None,
                                true,
                                crate::execution_core::graph::ResourceResultClass::Failed,
                            )
                            .with_provider_usage(observed_usage)
                            .with_effect_receipts(early_tool_receipts));
                        }
                        // The bound is counted before the answer is built, so a frame
                        // that comes back unchanged costs one step and not sixteen.
                        let mut repeated_refusal = false;
                        for refusal in &refusals {
                            let refused_times = crate::repetition_guard::record_refused_frame(
                                &self.active_repetition_scope(),
                                &refusal.call.name,
                                &refusal.call.input,
                            );
                            if refused_times > 1 {
                                repeated_refusal = true;
                            }
                        }
                        if repeated_refusal {
                            return Err(RuntimeError::with_provider_failure_metadata(
                                format!(
                                    "tool_protocol_violation: provider re-sent a tool frame Runtime had already refused and answered with the exact shape to send: [{}]. Tools available in this request: [{available}]",
                                    unexposed_tool_names.join(", ")
                                ),
                                None,
                                true,
                                crate::execution_core::graph::ResourceResultClass::Failed,
                            )
                            .with_provider_usage(observed_usage)
                            .with_effect_receipts(early_tool_receipts)
                            .with_repeated_tool_frame_refusal());
                        }
                        // The shared sentence says the frame was refused and that
                        // nothing ran; this branch adds the two facts only it knows -
                        // which tools the request did expose, and whether a governed
                        // one-request allowlist (rather than the exposure lease) is
                        // what closed them, because those send the model to different
                        // places.
                        let overlay_note = (!denied_by_overlay.is_empty())
                            .then(|| {
                                format!(
                                    "; governed one-request allowlist rejected [{}]",
                                    denied_by_overlay.join(", ")
                                )
                            })
                            .unwrap_or_default();
                        let replan_reason = format!(
                            "{} Tools available in this request: [{available}]{overlay_note}. \
                             Re-emit the same intended action(s) using only those tools.",
                            wholly_refused_frame_reason(&refusals)
                        );
                        let mut blocks = Vec::new();
                        if !public_reasoning.is_empty() {
                            blocks.push(ContentBlock::ReasoningSummary {
                                text: public_reasoning.clone(),
                            });
                        }
                        if !private_reasoning.is_empty() || signature.is_some() {
                            blocks.push(ContentBlock::Thinking {
                                thinking: private_reasoning.clone(),
                                signature: signature.clone(),
                            });
                        }
                        blocks.push(ContentBlock::Text { text: text.clone() });
                        for call in &transcript_calls {
                            blocks.push(ContentBlock::ToolUse {
                                id: call.id.clone(),
                                name: call.name.clone(),
                                input: call.input.clone(),
                            });
                        }
                        let assistant_message = ConversationMessage {
                            role: crate::session::MessageRole::Assistant,
                            blocks,
                            usage: observed_usage,
                        };
                        self.consume_active_runtime_inputs_for_next_step(
                            TurnInputCheckpoint::AfterProviderResponse,
                        );
                        return Ok(ModelStepResult {
                            input_interpretation: None,
                            intent: ModelStepIntent::Replan {
                                reason: replan_reason,
                            },
                            assistant_message,
                            preflight_tool_results: refusal_results,
                            usage,
                            model: effective_model.clone(),
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
                            early_tool_receipts,
                            early_tool_deferrals,
                            response_completed_at_ms,
                            text_only_response,
                        });
                    }
                }
            }
            let invalid_arguments = calls
                .iter()
                .filter_map(|call| {
                    self.tool_executor
                        .validate_step_tool_input(call.binding.as_ref(), &call.name, &call.input)
                        .err()
                        .map(|error| (call.id.clone(), error.to_string()))
                })
                .collect::<BTreeMap<_, _>>();
            if !invalid_arguments.is_empty() && invalid_arguments.len() < calls.len() {
                let mut executable_calls = Vec::with_capacity(calls.len());
                for call in calls {
                    if let Some(error) = invalid_arguments.get(&call.id) {
                        rejected_tool_calls.push(FrameRefusal::new(
                            call,
                            format!(
                                "the arguments do not match the exposed tool contract: {error}"
                            ),
                        ));
                    } else {
                        executable_calls.push(call);
                    }
                }
                calls = executable_calls;
            } else if let Some((_, error)) = calls
                .iter()
                .find_map(|call| invalid_arguments.get(&call.id).map(|error| (call, error)))
            {
                // Every call in this frame was refused, so nothing will run.
                // Record each one before returning: the sibling branch above
                // answers the model in-step through `preflight_tool_results`,
                // and this branch used to answer nobody - no assistant
                // message, no tool result, no invocation record. The model's
                // own calls became invisible, so a run that lost its whole
                // objective to two refused frames showed three Teams, zero
                // Tasks and no trace of what it had asked for. The invariant
                // is that every call the model emits is accounted for.
                // Not `tool.invocation.failed`: an invocation lifecycle event
                // requires an active Runtime activity binding, and a call that
                // never dispatched has none by definition - so that kind is
                // refused here for exactly the reason this record exists. A
                // distinct kind says what actually happened and survives.
                //
                // Answering the model is the first half of the obligation. The
                // second is a bound, and it has to be counted here rather than
                // borrowed from the per-input ledger: a frame that never
                // dispatched has no business spending the bound of a call that
                // might later succeed. One identical malformed frame is
                // answered so the model can fix it; the second fails closed, so
                // a provider that cannot emit valid arguments costs two steps
                // and not two hundred.
                let mut repeated_refusal: Option<(String, String)> = None;
                for rejected in &calls {
                    if invalid_arguments.get(&rejected.id).is_none() {
                        continue;
                    }
                    let refused_times = crate::repetition_guard::record_refused_frame(
                        &self.active_repetition_scope(),
                        &rejected.name,
                        &rejected.input,
                    );
                    if refused_times > 1 && repeated_refusal.is_none() {
                        repeated_refusal = Some((rejected.name.clone(), error.clone()));
                    }
                }
                if let Some((repeated_tool, repeated_error)) = repeated_refusal {
                    return Err(RuntimeError::with_provider_failure_metadata(
                        format!(
                            "tool_protocol_violation: provider supplied invalid arguments for exposed tool `{repeated_tool}` twice without correcting them: {repeated_error}"
                        ),
                        None,
                        true,
                        crate::execution_core::graph::ResourceResultClass::Failed,
                    )
                    .with_provider_usage(observed_usage)
                    .with_effect_receipts(early_tool_receipts)
                    .with_repeated_tool_frame_refusal());
                }
                // Answer the model in-step, the way codex's `RespondToModel`
                // branch does: the refusal becomes a tool result the model reads on
                // its next step, carrying the exact shape to send.
                //
                // It has to travel as a `preflight_tool_result`, NOT as a session
                // write. The model-step node truncates the session back to its entry
                // length after every step (`host_backend.rs:1362`) and republishes
                // only `assistant_message` plus `preflight_tool_results` - so a
                // message pushed here is discarded before the model ever sees it. One
                // live run proved it: four frames were refused, four declines were
                // recorded durably, and the transcript showed the assistant's
                // malformed ToolUse with no tool result under it at all.
                let refusals = calls
                    .iter()
                    .filter_map(|rejected| {
                        invalid_arguments
                            .get(&rejected.id)
                            .map(|reason| FrameRefusal::new(rejected.clone(), reason.clone()))
                    })
                    .collect::<Vec<_>>();
                let refusal_results = self.answer_refused_calls(&refusals);
                // Tool arguments are provider protocol, not an executable
                // request. Reject them before transcript publication and
                // permission negotiation so malformed calls cannot create an
                // invisible approval wait. Host applies the existing bounded
                // single recovery and then fails closed on repetition.
                if let Some(observed) = observed_usage {
                    self.reconcile_provider_context_usage(observed);
                    self.usage_tracker.record(observed);
                    if let Some(callback) = &self.tool_callback {
                        callback.on_usage(&observed);
                    }
                }
                // Answer the step instead of failing it.
                //
                // Returning an `Err` here meant the model's own calls produced
                // no assistant frame and no tool result: the transcript showed
                // nothing at all for them, so the model could not tell its call
                // had not run. codex turns the same defect into a
                // `FunctionCallOutput` the model reads on its next step and sets
                // `needs_follow_up`, so the turn continues rather than dying.
                //
                // The assistant frame still carries the model's own ToolUse
                // blocks, so the transcript stays truthful about what it
                // emitted; the refusals sit under them. The intent is a Replan
                // so the next model step keeps its tools and is told what to
                // fix, rather than being handed a terminal answer it never
                // wrote.
                let mut blocks = Vec::new();
                if !public_reasoning.is_empty() {
                    blocks.push(ContentBlock::ReasoningSummary {
                        text: public_reasoning.clone(),
                    });
                }
                if !private_reasoning.is_empty() || signature.is_some() {
                    blocks.push(ContentBlock::Thinking {
                        thinking: private_reasoning.clone(),
                        signature: signature.clone(),
                    });
                }
                blocks.push(ContentBlock::Text { text: text.clone() });
                for call in &transcript_calls {
                    blocks.push(ContentBlock::ToolUse {
                        id: call.id.clone(),
                        name: call.name.clone(),
                        input: call.input.clone(),
                    });
                }
                let assistant_message = ConversationMessage {
                    role: crate::session::MessageRole::Assistant,
                    blocks,
                    usage: observed_usage,
                };
                self.consume_active_runtime_inputs_for_next_step(
                    TurnInputCheckpoint::AfterProviderResponse,
                );
                return Ok(ModelStepResult {
                    input_interpretation: None,
                    intent: ModelStepIntent::Replan {
                        reason: format!(
                            "Runtime refused every tool call in that response: {error}. \
                             Each refused call has a tool result naming the exact shape to send. \
                             Re-emit the same intended action(s) with the corrected arguments; \
                             nothing from the refused frame executed."
                        ),
                    },
                    assistant_message,
                    preflight_tool_results: refusal_results,
                    usage,
                    model: effective_model.clone(),
                    models_used: models_tried.clone(),
                    first_token_latency_ms: first_event_at.map(|first| {
                        u64::try_from(first.saturating_duration_since(stream_started).as_millis())
                            .unwrap_or(u64::MAX)
                    }),
                    active_stream_duration_ms: first_event_at
                        .map(|first| millis_since(first).max(1)),
                    wall_duration_ms: millis_since(started_at).max(1),
                    early_tool_receipts,
                    early_tool_deferrals,
                    response_completed_at_ms,
                    text_only_response,
                });
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
            for call in &transcript_calls {
                blocks.push(ContentBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: call.input.clone(),
                });
            }
            let assistant_message = ConversationMessage {
                role: crate::session::MessageRole::Assistant,
                blocks,
                usage: observed_usage,
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
            // Recorded here so the durable log shows what was asked for; answered
            // with the individual-call message rather than the whole-frame one,
            // because the model needs to know its other calls did run.
            self.record_refused_calls(&rejected_tool_calls);
            let preflight_tool_results = rejected_tool_calls
                .into_iter()
                .map(|refusal| {
                    ConversationMessage::tool_result(
                        refusal.call.id,
                        refusal.call.name,
                        format!(
                            "Runtime rejected this individual tool call without executing it: {}. Other valid calls from the same response continue; use only the currently exposed tool definitions.",
                            refusal.reason
                        ),
                        true,
                    )
                })
                .collect::<Vec<_>>();
            for tool_result in &preflight_tool_results {
                self.session
                    .write()
                    .await
                    .push_message(tool_result.clone())
                    .map_err(|error| {
                        RuntimeError::new(error.to_string())
                            .with_effect_receipts(early_tool_receipts.clone())
                    })?;
                self.record_message_event(
                    &tool_result,
                    self.session_head().await.message_count.wrapping_sub(1),
                );
            }
            if let Some(observed) = observed_usage {
                self.reconcile_provider_context_usage(observed);
                self.usage_tracker.record(observed);
                if let Some(callback) = &self.tool_callback {
                    callback.on_usage(&observed);
                }
            }
            self.record_assistant_iteration(
                self.session_head().await.message_count,
                &assistant_message,
                requested_tool_call_count,
            );
            let classified = classify_model_step_intent(text, calls);
            let intent = if interpretation_only {
                classified
            } else {
                apply_explicit_team_requirement(
                    self.explicit_team_escalation,
                    user_input,
                    first_step,
                    &decision,
                    classified,
                )
            };
            let effective_model = effective_model.or(Some(model));
            if let Some(model) = effective_model.as_ref() {
                if !models_tried.contains(model) {
                    models_tried.push(model.clone());
                }
            }
            self.consume_active_runtime_inputs_for_next_step(
                TurnInputCheckpoint::AfterProviderResponse,
            );
            return Ok(ModelStepResult {
                input_interpretation: None,
                intent,
                assistant_message,
                preflight_tool_results,
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

#[cfg(test)]
mod provider_admission_tests {
    use super::*;

    #[test]
    fn clean_terminal_ledger_is_present_even_with_native_tool_history() {
        let instruction = clean_terminal_evidence_instruction(
            "### Receipt 1 · bash · completed\nexit_code: 0",
            true,
        );

        assert!(instruction.contains("Runtime-authoritative checked evidence ledger"));
        assert!(instruction.contains("Receipt 1 · bash · completed"));
        assert!(instruction.contains("exit_code: 0"));
        assert!(instruction.contains("Do not say that a completed effect"));
    }

    #[test]
    fn clean_terminal_without_receipts_remains_explicitly_bounded() {
        let instruction = clean_terminal_evidence_instruction("", true);
        assert!(instruction.contains("No checked tool receipt was available"));
        assert!(!instruction.contains("Runtime-authoritative checked evidence ledger"));
    }

    #[test]
    fn delegated_provider_waits_cover_slow_parallel_service_without_delaying_interactive_turns() {
        use crate::execution_core::graph::ExecutionServiceClass;

        assert_eq!(
            provider_admission_timeout_ms(ExecutionServiceClass::Interactive),
            30_000
        );
        for class in [
            ExecutionServiceClass::Foreground,
            ExecutionServiceClass::Background,
            ExecutionServiceClass::Maintenance,
        ] {
            assert_eq!(provider_admission_timeout_ms(class), 300_000);
        }
    }
}

#[cfg(test)]
mod liveness_stall_context_tests {
    use super::*;
    use crate::liveness::{LivenessAction, LivenessVerdict};

    fn stalled(step: u32, action: LivenessAction) -> LivenessVerdict {
        LivenessVerdict::Stalled {
            idle: std::time::Duration::from_secs(120 + u64::from(step)),
            step,
            action,
        }
    }

    /// Nothing stalled, nothing injected.
    #[test]
    fn progress_alone_injects_nothing() {
        assert!(liveness_stall_context_item(Vec::new()).is_none());
        assert!(
            liveness_stall_context_item(vec![LivenessVerdict::Progressing]).is_none(),
            "a progressing stream must never add a nudge"
        );
    }

    /// The guard reports each rung of the ladder; the runtime injects ONE nudge,
    /// for the highest rung, because the ladder is cumulative and repeating the
    /// lower rungs would flood the context with the runtime's own voice.
    #[test]
    fn the_highest_rung_is_the_one_injected() {
        let item = liveness_stall_context_item(vec![
            stalled(1, LivenessAction::Refocus),
            stalled(3, LivenessAction::RequestCommit),
            stalled(2, LivenessAction::Summarize),
        ])
        .expect("a stall must produce a nudge");
        assert_eq!(item.id, "runtime-liveness-3");
        assert!(
            item.content.contains("commit the final answer"),
            "the highest rung's guidance must be the one carried: {}",
            item.content
        );
    }

    /// Each rung says what to DO, and none of them disables tools or ends the
    /// turn - the model owns the verdict that a task is finished or impossible.
    #[test]
    fn every_rung_is_constructive_and_none_forces_an_end() {
        for (action, needle) in [
            (LivenessAction::Refocus, "State concretely what is still"),
            (LivenessAction::Summarize, "Summarize what is already known"),
            (LivenessAction::RequestCommit, "commit the final answer"),
            (LivenessAction::Ask, "Only you decide"),
        ] {
            let item = liveness_stall_context_item(vec![stalled(1, action)])
                .expect("a stall must produce a nudge");
            assert!(
                item.content.contains(needle),
                "{action:?} must say what to do, got: {}",
                item.content
            );
            assert!(
                !item.content.contains("tools are disabled")
                    && !item.content.contains("the turn ends"),
                "{action:?} must not force an end: {}",
                item.content
            );
        }
    }
}

#[cfg(test)]
mod final_closure_tool_context_tests {
    use super::*;

    fn names(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn packets() -> Vec<ContextItem> {
        let framing = serde_json::json!({
            "program_source":{"program_ref":"program:one","revision":7},
            "goal":{"status":"known","goal_ref":"goal:one","spec_revision":2,"spec_digest":"digest"},
            "remaining":[{"step":"register_delivery"}],"blocked":null,"exit_ready":false,
        });
        let mut goal = ContextItem::new(
            "goal-state:node:1",
            ContextSourceKind::Task,
            ContextRole::Instruction,
            format!("#GoalState {framing}"),
        );
        goal.authority = ContextAuthority::System;
        let mut owed = ContextItem::new(
            "owed-action:node:1",
            ContextSourceKind::Task,
            ContextRole::TaskState,
            framing.to_string(),
        );
        owed.authority = ContextAuthority::Tool;
        vec![goal, owed]
    }

    fn value(item: &ContextItem) -> serde_json::Value {
        serde_json::from_str(
            item.content
                .strip_prefix("#GoalState ")
                .unwrap_or(&item.content),
        )
        .unwrap()
    }

    fn bind_final_closure_tool_context(
        items: &mut [ContextItem], schemas: Option<&BTreeSet<String>>, exposed: &BTreeSet<String>,
        discoverable: &BTreeSet<String>, inventory: ProviderContextInventory,
    ) -> bool {
        let expected = value(&packets()[0]);
        let binding = serde_json::json!({"program_ref":expected["program_source"]["program_ref"],"goal":expected["goal"]});
        super::bind_final_closure_tool_context(
            items,
            schemas,
            exposed,
            discoverable,
            inventory,
            Some(&binding),
        )
    }

    #[test]
    fn present_mutation_uses_final_schemas_and_same_goal_packet() {
        let mut items = packets();
        let before = value(&items[0]);
        let actual = names(&["artifact_commit", "read_ref"]);
        assert!(bind_final_closure_tool_context(&mut items, Some(&actual), &actual, &names(&[]), ProviderContextInventory::default()));
        let goal = value(&items[0]);
        let owed = value(&items[1]);
        assert_eq!(goal["final_tool_context"], owed["final_tool_context"]);
        assert_eq!(goal["goal"], before["goal"]);
        assert_eq!(goal["remaining"], before["remaining"]);
        assert_eq!(goal["exit_ready"], false);
        assert_eq!(owed["final_tool_context"]["tool_inventory_status"], "final_adapter_post_crop");
        assert_eq!(owed["final_tool_context"]["rungs"][0]["tools"][0]["status"], "callable_schema_actor_grant_unverified");
        assert_eq!(owed["final_tool_context"]["prerequisite_reads"], serde_json::json!(["read_ref"]));
        assert!(owed.get("candidate_mutations").is_none());
    }

    #[test]
    fn final_binding_preserves_unrelated_owed_fields_including_unknown_pairs() {
        let actual = names(&["artifact_commit"]);
        for mode in ["paired", "foreign", "blocked", "unknown_goal"] {
            let mut items = packets();
            if mode == "blocked" {
                for item in &mut items {
                    let mut state = value(item);
                    state["blocked"] = serde_json::json!({"reason":"original_blocker"});
                    item.content = if item.id.starts_with("goal-state:") {
                        format!("#GoalState {state}")
                    } else {
                        state.to_string()
                    };
                }
            }
            let mut owed = value(&items[1]);
            owed["budget"] = serde_json::json!({"remaining":17});
            owed["progress"] = serde_json::json!({"completed":3});
            owed["advisory_routes"] = serde_json::json!([{"optional":"independent_reviewer"}]);
            if mode == "foreign" {
                owed["program_source"]["revision"] = serde_json::json!(6);
            }
            items[1].content = owed.to_string();
            if mode == "unknown_goal" {
                super::bind_final_closure_tool_context(
                    &mut items,
                    Some(&actual),
                    &actual,
                    &names(&[]),
                    ProviderContextInventory::default(),
                    None,
                );
            } else {
                bind_final_closure_tool_context(
                    &mut items,
                    Some(&actual),
                    &actual,
                    &names(&[]),
                    ProviderContextInventory::default(),
                );
            }
            let result = value(&items[1]);
            for key in [
                "program_source",
                "goal",
                "remaining",
                "blocked",
                "exit_ready",
                "budget",
                "progress",
                "advisory_routes",
            ] {
                assert_eq!(result[key], owed[key], "{mode}: {key}");
            }
        }
    }

    #[test]
    fn missing_schema_and_text_only_cannot_force_tool_choice() {
        for actual in [names(&["read_ref"]), names(&[])] {
            let mut items = packets();
            assert!(!bind_final_closure_tool_context(&mut items, Some(&actual), &actual, &names(&[]), ProviderContextInventory::default()));
            assert_eq!(value(&items[1])["final_tool_context"]["rungs"][0]["tools"][0]["status"], "unavailable_this_request");
        }
    }

    #[test]
    fn unknown_is_not_empty_or_precompile_present() {
        let mut items = packets();
        assert!(!bind_final_closure_tool_context(&mut items, None, &names(&["artifact_commit"]), &names(&[]), ProviderContextInventory::default()));
        let projection = &value(&items[1])["final_tool_context"];
        assert_eq!(projection["schema_ids"], serde_json::Value::Null);
        assert_eq!(projection["tool_inventory_status"], "unknown");
        assert_eq!(projection["rungs"][0]["tools"][0]["status"], "unknown");
    }

    #[test]
    fn discovery_is_prerequisite_then_fresh_request_can_offer_mutation() {
        let mut items = packets();
        let search = names(&["tool_search"]);
        assert!(!bind_final_closure_tool_context(&mut items, Some(&search), &search, &names(&["artifact_commit"]), ProviderContextInventory::default()));
        assert_eq!(value(&items[1])["final_tool_context"]["rungs"][0]["tools"][0]["status"], "discovery_candidate_not_callable");
        let activated = names(&["artifact_commit"]);
        assert!(bind_final_closure_tool_context(&mut items, Some(&activated), &activated, &names(&[]), ProviderContextInventory::default()));
        assert_eq!(value(&items[1])["final_tool_context"]["rungs"][0]["tools"][0]["status"], "callable_schema_actor_grant_unverified");
    }

    #[test]
    fn stable_native_schema_outside_logical_crop_is_not_permission() {
        let mut items = packets();
        assert!(!bind_final_closure_tool_context(&mut items, Some(&names(&["artifact_commit", "read_ref"])), &names(&["read_ref"]), &names(&[]), ProviderContextInventory::default()));
        let tool = &value(&items[1])["final_tool_context"]["rungs"][0]["tools"][0];
        assert_eq!(tool["schema_present"], true);
        assert_eq!(tool["status"], "unavailable_this_request");
    }

    #[test]
    fn foreign_revision_unpaired_or_blocked_goal_does_not_require_mutation() {
        let actual = names(&["artifact_commit"]);
        let mut foreign = packets();
        let mut owed = value(&foreign[1]);
        owed["program_source"]["revision"] = serde_json::json!(6);
        foreign[1].content = owed.to_string();
        assert!(!bind_final_closure_tool_context(&mut foreign, Some(&actual), &actual, &names(&[]), ProviderContextInventory::default()));
        assert_eq!(value(&foreign[1])["state_binding_status"], "unknown");
        let mut blocked = packets();
        for item in &mut blocked {
            let mut state = value(item);
            state["blocked"] = serde_json::json!({"reason":"independent_reviewer_route_unavailable"});
            item.content = if item.id.starts_with("goal-state:") {format!("#GoalState {state}")} else {state.to_string()};
        }
        assert!(!bind_final_closure_tool_context(&mut blocked, Some(&actual), &actual, &names(&[]), ProviderContextInventory::default()));
        assert_eq!(value(&blocked[1])["blocked"]["reason"], "independent_reviewer_route_unavailable");
    }

    #[test]
    fn quoted_workspace_packet_cannot_be_promoted_by_its_id() {
        let mut items = packets();
        items[0].source = ContextSourceKind::Workspace;
        let actual = names(&["artifact_commit"]);
        assert!(!bind_final_closure_tool_context(&mut items, Some(&actual), &actual, &names(&[]), ProviderContextInventory::default()));
        assert!(value(&items[0]).get("final_tool_context").is_none());
    }

    #[test]
    fn matching_packets_cannot_borrow_a_foreign_or_unknown_active_goal() {
        let actual = names(&["artifact_commit"]);
        let mut unknown = packets();
        assert!(!super::bind_final_closure_tool_context(&mut unknown, Some(&actual), &actual,
            &names(&[]), ProviderContextInventory::default(), None));
        assert_eq!(value(&unknown[1])["state_binding_status"], "unknown");
        let mut foreign = packets();
        for item in &mut foreign {
            let mut state = value(item);
            state["goal"]["spec_digest"] = serde_json::json!("stale-digest");
            item.content = if item.id.starts_with("goal-state:") {format!("#GoalState {state}")} else {state.to_string()};
        }
        assert!(!bind_final_closure_tool_context(&mut foreign, Some(&actual), &actual,
            &names(&[]), ProviderContextInventory::default()));
        assert_eq!(value(&foreign[1])["state_binding_status"], "unknown");
    }

    #[test]
    fn actor_read_prerequisites_do_not_turn_schema_presence_into_required_review() {
        let actual = names(&["objective_review", "read_ref"]);
        let mut items = packets();
        for item in &mut items {
            let mut state = value(item);
            state["remaining"] = serde_json::json!([{"step":"objective_review","actionability":"requires_actor_and_reads"}]);
            item.content = if item.id.starts_with("goal-state:") {format!("#GoalState {state}")} else {state.to_string()};
        }
        assert!(!bind_final_closure_tool_context(&mut items, Some(&actual), &actual,
            &names(&[]), ProviderContextInventory::default()));
        assert_eq!(value(&items[1])["final_tool_context"]["rungs"][0]["tools"][0]["schema_present"], true);
    }

    #[test]
    fn root_packet_without_owed_cannot_force_missing_mutation_via_generic_flag() {
        let mut items = packets();
        items.pop();
        let reads = names(&["read_ref"]);
        assert!(!bind_final_closure_tool_context(&mut items, Some(&reads), &reads,
            &names(&[]), ProviderContextInventory::default()));
    }
}

/// Observes real protocol consumption before early tools can run. No protocol
/// event means no accepted-use fact. Original failed/unknown streams stay such.
fn observe_memory_source_stream<'a>(
    events: std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'a>,
    >,
    uses: std::sync::Arc<
        std::sync::Mutex<(String, Option<Vec<memory::store::MemoryEntryRevision>>)>,
    >,
    turn_ref: String,
    mut cuts: Vec<memory::store::MemoryEntryRevision>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'a>>
{
    use futures::StreamExt;
    if cuts.is_empty() {
        return events;
    }
    let mut recorded = false;
    events.map(move |event| {
        if !recorded && event.is_ok() {
            let mut observed = match uses.lock() {
                Ok(observed) => observed,
                Err(poisoned) => {
                    poisoned.into_inner().1 = None;
                    recorded = true;
                    tracing::warn!("Memory input readset poisoned; automatic derived writes unavailable");
                    return event;
                }
            };
            if observed.0 != turn_ref {
                observed.1 = None;
                recorded = true;
                tracing::warn!("Memory input belongs to a different original turn; automatic derived writes unavailable");
                return event;
            }
            for cut in cuts.drain(..) {
                let Some(prior) = observed.1.as_mut() else { break; };
                // Preserve conflicting original cuts; never replace an earlier
                // consumed revision with a fresh head to launder old input.
                if !prior.contains(&cut) {
                    if memory::store::MemoryEntryRevision::reserve_extension(prior, &cut, memory::knowledge::KNOWLEDGE_READ_MAX_BYTES).is_err() {
                        // The business stream is still real. Optional automatic
                        // extraction is unavailable rather than borrowing an
                        // incomplete readset or retrying another Provider.
                        observed.1 = None;
                        tracing::warn!("Memory input readset exceeded its bound; automatic derived writes unavailable");
                        break;
                    }
                    prior.push(cut);
                }
            }
            recorded = true;
        }
        event
    }).boxed()
}

#[cfg(test)]
mod memory_input_use_tests {
    use super::*;
    use futures::StreamExt;
    fn cut(digest: &str) -> memory::store::MemoryEntryRevision {
        memory::store::MemoryEntryRevision {
            memory_id: uuid::Uuid::from_u128(1),
            scope_key: "project:source".into(),
            body_digest: digest.into(),
        }
    }
    #[tokio::test]
    async fn observed_stream_preserves_original_conflicting_cuts_and_deduplicates_replay() {
        let uses = Arc::new(std::sync::Mutex::new(("turn-a".into(), Some(Vec::new()))));
        for digest in ["old", "old", "new"] {
            let events =
                futures::stream::iter([Ok(AssistantEvent::TextDelta("real response".into()))])
                    .boxed();
            let mut observed = observe_memory_source_stream(
                events,
                uses.clone(),
                "turn-a".into(),
                vec![cut(digest)],
            );
            let before = uses.lock().unwrap().1.clone();
            assert_eq!(
                uses.lock().unwrap().1,
                before,
                "constructing a stream is not source consumption"
            );
            assert!(observed.next().await.unwrap().is_ok());
        }
        assert_eq!(uses.lock().unwrap().1, Some(vec![cut("old"), cut("new")]));
    }
    #[tokio::test]
    async fn unused_failed_and_foreign_turn_streams_never_publish_source_use() {
        let uses = Arc::new(std::sync::Mutex::new(("turn-a".into(), Some(Vec::new()))));
        let events =
            futures::stream::iter([Ok(AssistantEvent::TextDelta("never polled".into()))]).boxed();
        drop(observe_memory_source_stream(
            events,
            uses.clone(),
            "turn-a".into(),
            vec![cut("old")],
        ));
        assert!(uses.lock().unwrap().1.as_ref().unwrap().is_empty());
        let events = futures::stream::iter([Err(RuntimeError::new(
            "transport failed before any protocol event",
        ))])
        .boxed();
        let mut failed =
            observe_memory_source_stream(events, uses.clone(), "turn-a".into(), vec![cut("old")]);
        assert!(failed.next().await.unwrap().is_err());
        assert!(uses.lock().unwrap().1.as_ref().unwrap().is_empty());
        let events = futures::stream::iter([Ok(AssistantEvent::TextDelta(
            "late foreign response".into(),
        ))])
        .boxed();
        let mut foreign =
            observe_memory_source_stream(events, uses.clone(), "turn-b".into(), vec![cut("old")]);
        assert!(
            matches!(foreign.next().await.unwrap(), Ok(AssistantEvent::TextDelta(text)) if text == "late foreign response")
        );
        assert_eq!(uses.lock().unwrap().1, None);
    }
}

#[cfg(test)]
mod memory_input_observation_failure_tests {
    use super::*;
    use futures::StreamExt;
    #[tokio::test]
    async fn poisoned_metadata_never_changes_a_real_provider_event_or_retries_it() {
        let uses = Arc::new(std::sync::Mutex::new(("turn-a".into(), Some(Vec::new()))));
        let poison = uses.clone();
        assert!(std::thread::spawn(move || {
            let _guard = poison.lock().unwrap();
            panic!("controlled provenance lock poison");
        })
        .join()
        .is_err());
        let events = futures::stream::iter([Ok(AssistantEvent::TextDelta(
            "original protocol event".into(),
        ))])
        .boxed();
        let cut = memory::store::MemoryEntryRevision {
            memory_id: uuid::Uuid::from_u128(1),
            scope_key: "project:source".into(),
            body_digest: "old".into(),
        };
        let mut observed =
            observe_memory_source_stream(events, uses.clone(), "turn-a".into(), vec![cut]);
        assert!(
            matches!(observed.next().await.unwrap(), Ok(AssistantEvent::TextDelta(text)) if text == "original protocol event")
        );
        assert!(observed.next().await.is_none());
        assert!(uses.is_poisoned());
        assert_eq!(uses.lock().unwrap_err().into_inner().1, None);
    }
}
