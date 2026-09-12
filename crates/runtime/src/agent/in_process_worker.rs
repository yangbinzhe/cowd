use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommandRequest, AgentInput, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
};
use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};
use sha2::{Digest, Sha256};

use crate::{
    ContextProfile, PermissionMode, PermissionPolicy, RuntimeExecutionHost, RuntimeServices,
    RuntimeToolExecutionRequest, RuntimeToolExecutionStatus, Session, SharedPrompter,
    StandardRuntimeHost, StandardRuntimeHostConfig, ToolError, ToolExecutor,
};

use crate::agent_model_selector::AgentModelSelection;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
use crate::agent_runtime::AgentRuntimeBackend;
use crate::execution_core::graph::ScopeLockManager;

#[path = "in_process/stages.rs"]
mod stages;
use stages::*;
#[path = "in_process/terminal.rs"]
mod terminal;
use terminal::*;
#[path = "in_process/model_loop.rs"]
mod model_loop;
#[path = "in_process/tool_turn.rs"]
mod tool_turn;
use tool_turn::*;
#[path = "in_process/path_scope.rs"]
mod path_scope;
#[path = "in_process/tool_scope.rs"]
mod tool_scope;
use path_scope::*;
#[path = "in_process/prompt.rs"]
mod prompt;
use prompt::*;
#[path = "in_process/evidence_collector.rs"]
mod evidence_collector;
use evidence_collector::*;
pub(crate) use evidence_collector::{
    structured_agent_output, structured_agent_output_for_fields,
    structured_contract_field_materialized,
};

/// Runtime-owned ToolHost bridge for an approved ProcessJsonl worker.
///
/// The external process may decide which allowed tool to request, but every
/// effect, receipt, scope lock and acceptance fact remains in the exact same
/// `ScopedRuntimeToolExecutor` used by the native worker. The process never
/// receives a raw ToolHost capability and therefore cannot mint evidence or
/// change receipts in its terminal JSON.
pub(crate) struct ProcessJsonlToolSession {
    services: Arc<RuntimeServices>,
    executor: Arc<ScopedRuntimeToolExecutor>,
    artifact_store: Arc<crate::ArtifactStore>,
    external_model_profile: String,
    topic_actions: crate::AgentActionService,
    topic_packet: AgentTaskPacket,
    topic_transport: Mutex<crate::agentic::topic_delivery::TopicTransport>,
    permission_policy: PermissionPolicy,
    authorization_negotiator: crate::AuthorizationNegotiator,
    event_store: Arc<crate::RuntimeEventStore>,
}

impl ProcessJsonlToolSession {
    pub(crate) fn prepare(
        services: &Arc<RuntimeServices>,
        packet: &AgentTaskPacket,
        selection: &AgentModelSelection,
    ) -> Result<Self, String> {
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "ProcessJsonl tool bridge requires a Runtime-compiled Binding".to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        if packet
            .allowed_tools
            .iter()
            .any(|tool| !binding.tool_contract_refs.contains(tool))
        {
            return Err("AgentTaskPacket tool allow-list exceeds its Binding contract".to_string());
        }
        let execution_graph = services
            .graph_state_store()
            .load(packet.graph_id())
            .map_err(|error| {
                format!(
                    "ProcessJsonl Agent graph `{}` is unavailable: {error}",
                    packet.graph_id()
                )
            })?;
        let lineage = execution_graph.lineage.as_ref().ok_or_else(|| {
            format!(
                "ProcessJsonl Agent graph `{}` has no canonical Session/Turn/Task lineage",
                packet.graph_id()
            )
        })?;
        lineage.validate().map_err(str::to_string)?;
        if lineage.session_id != packet.session_id()
            || lineage.root_task_id != packet.assignment.root_task_id
        {
            return Err(format!(
                "ProcessJsonl AgentTaskPacket lineage does not match parent graph `{}`",
                packet.graph_id()
            ));
        }
        let host = services.tool_execution_host().cloned().ok_or_else(|| {
            "RuntimeServices has no ToolHost for the ProcessJsonl bridge".to_string()
        })?;
        let packet_allowed_tools = packet
            .allowed_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let bounded_resource_lease = packet.team_id().is_some()
            || packet.agentic_binding.is_some()
            || binding.evaluation.is_some();
        let requested_tool_names = packet_allowed_tools.iter().cloned().collect::<Vec<_>>();
        let allowed_tools = host
            .delegated_tool_definitions(&requested_tool_names)
            .into_iter()
            .map(|definition| definition.name)
            .filter(|tool| packet_allowed_tools.contains(tool))
            .filter(|tool| {
                !bounded_resource_lease
                    || delegated_tool_supports_bounded_scope(host.as_ref(), tool)
            })
            .collect::<BTreeSet<_>>();
        let unavailable_tools = packet_allowed_tools
            .difference(&allowed_tools)
            .filter(|tool| !matches!(tool.as_str(), "context_retrieve" | "evidence_retrieve"))
            .cloned()
            .collect::<Vec<_>>();
        if !unavailable_tools.is_empty() {
            return Err(format!(
                "agent_tool_inventory_drift: admitted Tool contracts are unavailable or no longer bounded on the active host: {}",
                unavailable_tools.join(", ")
            ));
        }
        let memory_context = memory::MemoryTurnContext::new(
            packet.session_id(),
            binding.instance.instance_id.clone(),
        )
        .with_definition_lineage_id(Some(
            binding.definition_ref.definition_id.as_str().to_string(),
        ))
        .with_project_id(Some(crate::memory_project_id_for_workspace(
            services.workspace_root(),
        )))
        .with_task_id(Some(binding.data_lease.task_id.clone()))
        .with_team_id(binding.data_lease.team_id.clone())
        .with_cognitive_read_scopes(binding.data_lease.read_scopes.clone());
        let live_control = services.session_execution_policy_control(packet.session_id());
        let session_policy = live_control
            .as_ref()
            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
            .ok_or_else(|| {
                format!(
                    "agent_session_policy_missing: session `{}` has no executable policy snapshot",
                    packet.session_id()
                )
            })?;
        if packet.policy_revision != 0 && packet.policy_revision != session_policy.revision {
            return Err(format!(
                "agent_policy_revision_stale: packet rev {} current rev {}; replan before tool execution",
                packet.policy_revision, session_policy.revision
            ));
        }
        let commit_service = crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(
            services.event_store(),
        ));
        let durable_receipts = commit_service
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| {
                format!(
                    "ProcessJsonl tool receipt recovery is invalid for {}:{}:{}: {error}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )
            })?
            .into_iter()
            .map(scoped_receipt_from_durable)
            .collect::<Vec<_>>();
        let recovered_sequence = durable_receipts
            .iter()
            .map(|receipt| receipt.sequence)
            .max()
            .unwrap_or(0);
        let provider_model_obligations = packet
            .required_acceptance
            .evidence_obligations
            .iter()
            .filter(|obligation| {
                obligation.observation_requirement
                    == harness_contract::context::EvidenceObservationRequirement::ProviderModel
            })
            .cloned()
            .collect();
        let process_permission_policy = permission_policy(
            live_control.clone(),
            packet.permission_ceiling,
            &allowed_tools,
        );
        Ok(Self {
            services: Arc::clone(services),
            executor: Arc::new(ScopedRuntimeToolExecutor {
                tool_batch: Some(crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(services, packet)),
                host,
                allowed_tools,
                session_id: packet.session_id().to_string(),
                sandbox_posture: session_policy.sandbox_posture,
                policy_revision: session_policy.revision,
                memory_context,
                reality_context: Some(binding.data_lease.clone()),
                model_lease: selection.model.clone(),
                execution_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                workspace_root: services.workspace_root().to_path_buf(),
                path_identity_resolver: Arc::clone(services.path_identity_resolver()),
                scope_locks: Arc::clone(services.scope_locks()),
                commit_service: Some(commit_service),
                resource_scopes: bounded_resource_lease.then(|| packet.resource_scopes.clone()),
                managed_invocation: packet.managed_invocation.clone(),
                next_receipt_sequence: AtomicU64::new(recovered_sequence),
                receipts: Mutex::new(durable_receipts),
                provider_model_obligations,
            }),
            artifact_store: Arc::clone(services.artifact_store()),
            external_model_profile: selection.model.clone(),
            topic_actions: services.agent_action_service(),
            topic_packet: packet.clone(),
            topic_transport: Mutex::new(Default::default()),
            permission_policy: process_permission_policy,
            authorization_negotiator: crate::AuthorizationNegotiator::new(),
            event_store: Arc::clone(services.event_store()),
        })
    }

    pub(crate) fn working_context_delta(
        &self,
        handle: &tokio::runtime::Handle,
    ) -> Result<Option<serde_json::Value>, String> {
        self.validate_current_policy()?;
        let window = handle.block_on(
            self.services
                .working_context_window(&self.executor.memory_context),
        )?;
        Ok((window["coverage"]["active_references"].as_u64() != Some(0)).then_some(window))
    }

    pub(crate) fn topic_delta(&self) -> Result<Option<serde_json::Value>, String> {
        self.topic_transport
            .lock()
            .map_err(|_| "Topic transport lock poisoned".to_string())?
            .issue(&self.topic_actions, &self.topic_packet)
    }

    pub(crate) fn acknowledge_topic_delta(&self, delivery_id: &str) -> Result<(), String> {
        self.topic_transport
            .lock()
            .map_err(|_| "Topic transport lock poisoned".to_string())?
            .acknowledge(&self.topic_actions, delivery_id)
    }

    pub(crate) fn validate_current_policy(&self) -> Result<(), String> {
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        if snapshot.revision != self.executor.policy_revision {
            return Err(
                "ProcessJsonl packet policy revision is stale; reauthorize before continuing"
                    .into(),
            );
        }
        Ok(())
    }

    pub(crate) async fn execute_tool(
        &self,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        self.validate_current_policy()?;
        if tool_name == "checkpoint_create" {
            return Err("checkpoint_create is Runtime-internal".into());
        }
        if tool_name == "tool_search" {
            return self
                .executor
                .execute_output(tool_name, input)
                .await
                .map(|output| output.model_text().to_string())
                .map_err(|e| e.to_string());
        }
        let value = serde_json::from_str(input)
            .map_err(|e| format!("invalid ProcessJsonl tool input: {e}"))?;
        let effect = self
            .executor
            .registered_tool_effect(tool_name, &value)
            .ok_or_else(|| {
                format!("tool `{tool_name}` is outside the admitted Agent tool contracts")
            })?;
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        let policy = self.permission_policy.bound_to_snapshot(&snapshot);
        let authorization_id = format!(
            "process-tool:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    &self.executor.execution_id,
                    self.executor.attempt,
                    request_id
                ))
                .map_err(|e| e.to_string())?
            )
        );
        let evaluated = self.authorization_negotiator.assess_effective(
            &policy,
            &crate::AuthorizationRequest {
                principal_id: format!("agent:{}", self.topic_packet.agent_id()),
                capability: tool_name.into(),
                input: input.into(),
                idempotency_key: authorization_id.clone(),
                effect,
                parent_ceiling: self.topic_packet.permission_ceiling,
                parent_lease_id: None,
                policy_revision: snapshot.revision,
                recovery_scope: format!("execution:{}", self.executor.execution_id),
                context: crate::PermissionContext::default(),
                safe_alternatives: vec![],
            },
        );
        let _ = self
            .authorization_negotiator
            .take_transitions_for_persistence();
        for transition in self
            .authorization_negotiator
            .transitions_awaiting_persistence()
        {
            crate::authorization_negotiator::persist_authorization_transition(
                &self.event_store,
                &format!("authorization-lease:{}", transition.lease.lease_id),
                "runtime.process_jsonl",
                &transition,
            )?;
            self.authorization_negotiator
                .acknowledge_persisted_transitions(std::slice::from_ref(&transition.transition_id));
        }
        let lease = evaluated.assessment.lease.clone().ok_or_else(|| {
            evaluated.assessment.gap.as_ref().map_or_else(
                || "ProcessJsonl tool authorization unavailable".to_string(),
                |gap| format!("ProcessJsonl tool authorization denied: {}", gap.reason),
            )
        })?;
        let decision = crate::ToolPolicy
            .authorize(
                &evaluated.effective,
                &evaluated.assessment,
                authorization_id.clone(),
                lease,
                60,
            )
            .map_err(|e| e.to_string())?;
        self.executor
            .execute_authorized_invocation_output(
                &authorization_id,
                &decision.authorization,
                tool_name,
                input,
            )
            .await
            .map(|output| output.model_text().to_string())
            .map_err(|error| error.to_string())
    }

    fn transport_request_identity(&self, request_id: &str) -> (String, String) {
        let packet = &self.topic_packet;
        let scope = serde_json::json!([
            packet.session_id(),
            packet.graph_id(),
            packet.node_id(),
            packet.run_id(),
            packet.agent_id(),
            self.executor.attempt,
            packet
                .binding
                .as_ref()
                .map(|binding| binding.binding_digest.as_str())
        ]);
        (
            format!(
                "agent-process-transport:{:x}",
                Sha256::digest(scope.to_string().as_bytes())
            ),
            format!("{:x}", Sha256::digest(request_id.as_bytes())),
        )
    }

    /// Protocol replay metadata only. Physical effect and acceptance authority
    /// remains with the original ToolHost invocation and Session receipts.
    pub(crate) fn replay_or_begin_transport_request(
        &self,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<Option<serde_json::Value>, String> {
        self.validate_current_policy()?;
        if request_id.trim().is_empty() {
            return Err("ProcessJsonl request_id must not be empty".into());
        }
        let (stream, key) = self.transport_request_identity(request_id);
        for stage in ["completed", "started"] {
            if let Some(record) = self
                .event_store
                .event_by_idempotency_key(&stream, &format!("{stage}:{key}"))
                .map_err(|error| error.to_string())?
            {
                if record.payload["fingerprint"].as_str() != Some(fingerprint)
                    || record.payload["request_id"].as_str() != Some(request_id)
                {
                    return Err(
                        "ProcessJsonl request_id was reused for a different tool invocation".into(),
                    );
                }
                if stage == "completed" {
                    let response = record
                        .payload
                        .get("response")
                        .filter(|response| response.is_object())
                        .cloned()
                        .ok_or("ProcessJsonl durable transport response is corrupt")?;
                    return Ok(Some(response));
                }
                return Err("ProcessJsonl prior request has unresolved transport completion; reconcile its recorded effects before retrying, do not blindly repeat the tool".into());
            }
        }
        if self.record_transport_request("started", request_id, fingerprint, None)? {
            return Err("ProcessJsonl prior request has unresolved transport completion; another reader already owns this request, reconcile its recorded effects before retrying".into());
        }
        Ok(None)
    }

    pub(crate) fn complete_transport_request(
        &self,
        request_id: &str,
        fingerprint: &str,
        response: &serde_json::Value,
    ) -> Result<(), String> {
        self.record_transport_request("completed", request_id, fingerprint, Some(response))
            .map(|_| ())
    }

    fn record_transport_request(
        &self,
        stage: &str,
        request_id: &str,
        fingerprint: &str,
        response: Option<&serde_json::Value>,
    ) -> Result<bool, String> {
        let (stream, key) = self.transport_request_identity(request_id);
        let head = self
            .event_store
            .stream_revision(&stream)
            .map_err(|error| error.to_string())?;
        self.event_store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("{stream}:{stage}:{key}"),
            expected_streams: vec![crate::ExpectedStreamRevision {stream_id: stream.clone(), expected_revision: head}],
            events: vec![crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream, scope: crate::RuntimeEventScope::Agent,
                    kind: format!("agent.process_request_{stage}"), status: Some(stage.into()),
                    actor: Some(self.topic_packet.agent_id().into()),
                    refs: vec![crate::RuntimeEventRef {kind:"agent_run".into(), id:self.topic_packet.run_id().into()},
                        crate::RuntimeEventRef {kind:"session".into(), id:self.topic_packet.session_id().into()},
                        crate::RuntimeEventRef {kind:"execution_graph".into(), id:self.topic_packet.graph_id().into()}],
                    payload: serde_json::json!({"request_id":request_id,"fingerprint":fingerprint,
                        "response":response,"authority":"transport_replay_only"}),
                }, idempotency_key: Some(format!("{stage}:{key}")), schema_version: 1,
            }],
        }).map(|receipt| receipt.duplicate).map_err(|error| error.to_string())
    }

    pub(crate) fn artifact_store(&self) -> &Arc<crate::ArtifactStore> {
        &self.artifact_store
    }

    /// Replace all child-authored receipt/evidence claims with facts derived
    /// from canonical ToolHost receipts. Provider-model observation is left
    /// unsatisfied for this bridge because an arbitrary child process cannot
    /// attest that it semantically consumed a source; the evaluator records
    /// that gap instead of pretending the external model saw it.
    pub(crate) fn apply_terminal(
        &self,
        packet: &AgentTaskPacket,
        terminal: &mut AgentReturnPacket,
    ) {
        let receipts = self
            .executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let observed_evidence =
            model_observed_evidence(&packet.required_acceptance, &[], &receipts);
        let evidence_refs = agent_evidence_refs(packet, &[], &receipts);
        let (acceptance, runtime_change_receipts) = derive_receipt_backed_satisfied_criteria(
            packet,
            &terminal.outcome,
            &evidence_refs,
            &self.executor,
            &observed_evidence,
        );
        let required = crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
            &packet.required_acceptance,
            &packet.acceptance,
        );
        let (observed_acceptance, acceptance_evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(
                crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                    required,
                    acceptance.clone(),
                    observed_evidence,
                ),
            );
        let mut runtime_write_attempt_paths = receipts
            .iter()
            .filter(|receipt| receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write)
            .flat_map(|receipt| receipt.paths.iter().cloned())
            .collect::<Vec<_>>();
        runtime_write_attempt_paths.sort();
        runtime_write_attempt_paths.dedup();
        terminal.observed_acceptance = observed_acceptance;
        terminal.acceptance_evaluation = Some(acceptance_evaluation);
        terminal.acceptance = acceptance;
        terminal.evidence_refs = evidence_refs;
        terminal.changes = runtime_change_receipts
            .iter()
            .map(|receipt| receipt.path.clone())
            .collect();
        terminal.runtime_change_receipts = runtime_change_receipts;
        terminal.tool_calls = u64::try_from(receipts.len()).unwrap_or(u64::MAX);
        terminal.duplicate_tool_calls = 0;
        terminal.max_tool_concurrency_observed = 1;
        terminal.parallel_tool_batches = 0;
        terminal.runtime_write_attempt_paths = runtime_write_attempt_paths;
        terminal.runtime_observed_resource_scopes = Vec::new();
        terminal.model.clone_from(&self.external_model_profile);
        terminal.provider = "external_process".to_string();
    }
}

/// Executes a delegated task through the same RuntimeServices/Runner/provider
/// path as a primary turn. It never calls `ConversationRuntime` directly.
pub struct InProcessAgentWorker {
    services: Weak<RuntimeServices>,
    active_runs: Mutex<BTreeMap<String, ActiveInProcessRun>>,
    pending_cancellations: Mutex<BTreeSet<String>>,
    completed_runs: Mutex<VecDeque<String>>,
}
#[cfg(test)]
#[path = "in_process/tests.rs"]
mod tests;

const COMPLETED_RUN_TOMBSTONE_LIMIT: usize = 1_024;

#[derive(Clone)]
struct ActiveInProcessRun {
    cancellation: crate::CancellationToken,
    session_id: String,
    input_stream: crate::SessionInputStream,
}

struct ActiveRunCleanup<'a> {
    worker: &'a InProcessAgentWorker,
    run_id: String,
}

impl Drop for ActiveRunCleanup<'_> {
    fn drop(&mut self) {
        // The tombstone is visible before the active handle disappears; the
        // completion flag/notification are published only after all maps are
        // clean. This also runs when the execute future is aborted.
        self.worker.record_completed_run(&self.run_id);
        self.worker
            .active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
        self.worker
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
    }
}

impl InProcessAgentWorker {
    #[must_use]
    pub fn new(services: Weak<RuntimeServices>) -> Self {
        Self {
            services,
            active_runs: Mutex::new(BTreeMap::new()),
            pending_cancellations: Mutex::new(BTreeSet::new()),
            completed_runs: Mutex::new(VecDeque::new()),
        }
    }

    fn record_completed_run(&self, run_id: &str) {
        let mut completed = self
            .completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !completed.iter().any(|candidate| candidate == run_id) {
            completed.push_back(run_id.to_string());
        }
        while completed.len() > COMPLETED_RUN_TOMBSTONE_LIMIT {
            completed.pop_front();
        }
    }

    fn run_completed(&self, run_id: &str) -> bool {
        self.completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|candidate| candidate == run_id)
    }
}
