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
use crate::execution_core::graph::{
    ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
};

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
    executor: Arc<ScopedRuntimeToolExecutor>,
    artifact_store: Arc<crate::ArtifactStore>,
    external_model_profile: String,
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
        let bounded_resource_lease = packet.team_id().is_some() || packet.agentic_binding.is_some();
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
        let session_policy = services
            .session_execution_policy_control(packet.session_id())
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
        Ok(Self {
            executor: Arc::new(ScopedRuntimeToolExecutor {
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
        })
    }

    pub(crate) async fn execute_tool(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        self.executor
            .execute_scoped(tool_name, input, None, None)
            .await
            .map_err(|error| error.to_string())
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
