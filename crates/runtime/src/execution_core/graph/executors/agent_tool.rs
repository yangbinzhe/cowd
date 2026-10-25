//! Durable Agent ToolBatch tickets reconstructed by the original scoped executor.
use super::{ScopedNodeBackend, ScopedNodeBackendResolver};
use crate::execution_core::graph::{NodeExecutionOutcome, NodeExecutionTicket, NodeExecutorError};
use crate::{RuntimeServices, RuntimeToolExecutionOutcome, RuntimeToolExecutionRequest, ToolError};
use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Weak};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoDispatchCall {
    pub(crate) call_id: String,
    pub(crate) tool_name: String,
    pub(crate) input_sha256: String,
    pub(crate) effect_key: Option<String>,
    pub(crate) request_sha256: Option<String>,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoDispatchFact {
    pub(crate) ticket: NodeExecutionTicket,
    pub(crate) calls: Vec<NoDispatchCall>,
    pub(crate) never_polled: bool,
    pub(crate) result_sha256: Option<String>,
    pub(crate) historical_sources: std::collections::BTreeMap<String, u64>,
    pub(crate) start: Option<(u64, u64, u32)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NoDispatchWitness {
    fact: NoDispatchFact,
}

#[derive(Debug, Clone)]
pub(crate) struct NoDispatchObservation {
    ticket: NodeExecutionTicket,
    calls: Vec<NoDispatchCall>,
    state: Arc<std::sync::Mutex<NoDispatchState>>,
}

#[derive(Debug, Default)]
struct NoDispatchState {
    entered: bool,
    closed: bool,
    uncertain: Vec<String>,
    refused: Vec<NoDispatchCall>,
    completed: Option<NoDispatchWitness>,
}

fn no_dispatch_digest(value: &impl Serialize) -> Result<String, serde_json::Error> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(value)?)
    ))
}

pub(crate) fn no_dispatch_calls(
    ticket: &NodeExecutionTicket,
) -> Result<Vec<NoDispatchCall>, serde_json::Error> {
    if ticket.executor_kind != "tool_batch" {
        return Err(bound_carrier_error(
            "NoDispatch requires original ToolBatch owner",
        ));
    }
    if bound_payload_identity_shape(&ticket.graph_id, &ticket.node_id) {
        let spec = ExecutionNodeSpec {
            id: ticket.node_id.clone(),
            kind: ExecutionNodeKind::ToolBatch,
            executor_kind: ticket.executor_kind.clone(),
            payload_ref: ticket.payload_ref.clone(),
            idempotency_key: ticket.idempotency_key.clone(),
            ..ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", "")
        };
        let request = decode_bound_agent_tool_node(&ticket.graph_id, &spec)?.request;
        return Ok(vec![NoDispatchCall {
            call_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input_sha256: format!("sha256:{:x}", Sha256::digest(request.input.as_bytes())),
            effect_key: Some(request.idempotency_key.clone()),
            request_sha256: Some(no_dispatch_digest(&request)?),
            reason: String::new(),
        }]);
    }
    let (calls, _) = crate::host::decode_tool_batch(&ticket.payload_ref)?;
    let mut seen = std::collections::BTreeSet::new();
    calls
        .iter()
        .map(|call| {
            let id = call.id.as_str();
            if id.is_empty() {
                return Err(bound_carrier_error("original call id missing"));
            }
            if !seen.insert(id) {
                return Err(bound_carrier_error("duplicate original call id"));
            }
            let effect_key = call
                .binding
                .as_ref()
                .map(|binding| {
                    if binding.origin.call_id != call.id
                        || binding.origin.origin_step_id != binding.snapshot.identity.step_id
                        || binding.origin.catalog_digest != binding.snapshot.catalog_digest
                    {
                        return Err(bound_carrier_error("original call Step identity mismatch"));
                    }
                    Ok(binding.effect_idempotency_key())
                })
                .transpose()?;
            Ok(NoDispatchCall {
                call_id: id.into(),
                tool_name: call.name.clone(),
                input_sha256: format!("sha256:{:x}", Sha256::digest(call.input.as_bytes())),
                effect_key,
                request_sha256: None,
                reason: String::new(),
            })
        })
        .collect()
}

impl NoDispatchObservation {
    pub(super) fn new(ticket: &NodeExecutionTicket) -> Result<Arc<Self>, NodeExecutorError> {
        let calls = no_dispatch_calls(ticket).map_err(|error| NodeExecutorError::Invalid {
            node_id: ticket.node_id.clone(),
            reason: error.to_string(),
        })?;
        Ok(Arc::new(Self {
            ticket: ticket.clone(),
            calls,
            state: Arc::new(std::sync::Mutex::new(NoDispatchState::default())),
        }))
    }

    pub(crate) fn ticket_matches(&self, ticket: &NodeExecutionTicket) -> bool {
        self.ticket == *ticket
    }

    pub(super) fn enter(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return false;
        }
        state.entered = true;
        true
    }

    pub(super) fn abandon_unadmitted(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.entered || !state.uncertain.is_empty() || !state.refused.is_empty()
        {
            return false;
        }
        state.closed = true;
        state.completed = None;
        true
    }

    pub(super) fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        if !state.entered && state.completed.is_none() {
            let mut calls = self.calls.clone();
            for call in &mut calls {
                call.reason = "cancelled_before_first_poll".into();
            }
            state.completed = Some(NoDispatchWitness {
                fact: NoDispatchFact {
                    ticket: self.ticket.clone(),
                    calls,
                    never_polled: true,
                    result_sha256: None,
                    historical_sources: Default::default(),
                    start: None,
                },
            });
        }
    }

    pub(crate) fn uncertain(&self, request: &RuntimeToolExecutionRequest) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.uncertain.contains(&request.tool_use_id) {
            state.uncertain.push(request.tool_use_id.clone());
        }
        state
            .refused
            .retain(|call| call.call_id != request.tool_use_id);
        state.completed = None;
    }

    pub(crate) fn refuse(&self, call_id: &str, reason: String) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.uncertain.iter().any(|id| id == call_id) || reason.is_empty() {
            return;
        }
        if let Some(original) = self.calls.iter().find(|call| call.call_id == call_id) {
            if !state.refused.iter().any(|call| call.call_id == call_id) {
                let mut refusal = original.clone();
                refusal.reason = reason;
                state.refused.push(refusal);
            }
        }
    }

    pub(crate) fn refusal(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Option<RuntimeToolExecutionOutcome> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let refusal = state.refused.iter().find(|call| {
            call.call_id == request.tool_use_id
                && call.request_sha256.as_ref().is_some_and(|digest| {
                    no_dispatch_digest(request).is_ok_and(|actual| &actual == digest)
                })
                && !state.uncertain.contains(&call.call_id)
        })?;
        Some(RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Failed,
            category: request.category,
            output: None,
            error: Some(refusal.reason.clone()),
            evidence_ref: String::new(),
            observed_evidence: Vec::new(),
        })
    }

    pub(super) fn finish(
        &self,
        outcome: &mut NodeExecutionOutcome,
    ) -> Result<(), serde_json::Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.refused.is_empty() {
            return Ok(());
        }
        let witness = NoDispatchWitness {
            fact: NoDispatchFact {
                ticket: self.ticket.clone(),
                calls: state.refused.clone(),
                never_polled: false,
                result_sha256: Some(no_dispatch_digest(&outcome.result)?),
                historical_sources: Default::default(),
                start: None,
            },
        };
        state.completed = Some(witness.clone());
        outcome.no_dispatch = Some(witness);
        Ok(())
    }

    pub(crate) fn finalize_duration(
        &self,
        ticket: &NodeExecutionTicket,
        outcome: &mut NodeExecutionOutcome,
        actual_duration_ms: u64,
    ) -> Result<(), String> {
        if !self.ticket_matches(ticket) {
            return Err("NoDispatch duration original ticket mismatch".into());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(original) = outcome.no_dispatch.as_ref() else {
            if state.completed.is_some() {
                return Err("NoDispatch duration lost original witness".into());
            }
            outcome.result.usage.duration_ms = outcome
                .result
                .usage
                .duration_ms
                .max(actual_duration_ms.max(1));
            return Ok(());
        };
        original.authenticate(ticket, Some(&outcome.result))?;
        if state.completed.as_ref() != Some(original)
            || original
                .fact
                .calls
                .iter()
                .any(|call| state.uncertain.contains(&call.call_id))
        {
            return Err("NoDispatch duration original observation changed".into());
        }
        let mut result = outcome.result.clone();
        result.usage.duration_ms = result.usage.duration_ms.max(actual_duration_ms.max(1));
        let mut finalized = original.clone();
        finalized.fact.result_sha256 =
            Some(no_dispatch_digest(&result).map_err(|error| error.to_string())?);
        state.completed = Some(finalized.clone());
        outcome.result = result;
        outcome.no_dispatch = Some(finalized);
        Ok(())
    }

    pub(crate) fn witness(&self) -> Option<NoDispatchWitness> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .completed
            .clone()
    }
}

impl NoDispatchWitness {
    pub(crate) fn fact(&self) -> &NoDispatchFact {
        &self.fact
    }

    pub(crate) fn authenticate(
        &self,
        ticket: &NodeExecutionTicket,
        result: Option<&ExecutionNodeResult>,
    ) -> Result<(), String> {
        self.fact.validate_identity(ticket)?;
        if self.fact.never_polled {
            if result.is_some() || self.fact.result_sha256.is_some() {
                return Err("NeverPolled cannot claim a node result".into());
            }
        } else if let Some(result) = result {
            if self.fact.result_sha256.as_ref()
                != Some(&no_dispatch_digest(result).map_err(|error| error.to_string())?)
            {
                return Err("NoDispatch sealed result mismatch".into());
            }
        }
        Ok(())
    }
}

impl NoDispatchFact {
    pub(crate) fn index_stream(&self) -> Result<String, String> {
        if is_bound_ticket(&self.ticket) {
            let bound = decode_bound_agent_tool(&self.ticket.payload_ref)
                .map_err(|error| error.to_string())?;
            let parent = bound
                .request
                .parent_execution
                .as_ref()
                .ok_or("NoDispatch parent missing")?;
            let attempt = bound
                .request
                .parent_execution_attempt
                .ok_or("NoDispatch parent attempt missing")?;
            Ok(format!(
                "execution-agent-receipts:{}:{}:{attempt}",
                parent.execution_id, parent.node_id
            ))
        } else {
            Ok(format!(
                "execution-agent-receipts:{}:{}:{}",
                self.ticket.graph_id, self.ticket.node_id, self.ticket.attempt
            ))
        }
    }

    pub(crate) fn validate_identity(&self, ticket: &NodeExecutionTicket) -> Result<(), String> {
        if self.ticket != *ticket || self.calls.is_empty() {
            return Err("NoDispatch original ticket or call cardinality mismatch".into());
        }
        let originals = no_dispatch_calls(ticket).map_err(|error| error.to_string())?;
        let mut seen = std::collections::BTreeSet::new();
        for call in &self.calls {
            let mut identity = call.clone();
            identity.reason.clear();
            if call.reason.is_empty()
                || !seen.insert(&call.call_id)
                || !originals.contains(&identity)
            {
                return Err("NoDispatch original call identity mismatch".into());
            }
        }
        if self.never_polled && self.calls.len() != originals.len() {
            return Err("NeverPolled must close every original call".into());
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BoundAgentTool {
    kind: String,
    pub(crate) agent_id: String,
    pub(crate) run_id: String,
    pub(crate) request: RuntimeToolExecutionRequest,
}

fn bound_carrier_error(reason: &str) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::InvalidData, reason))
}

fn validate_bound_agent_tool(bound: &BoundAgentTool) -> Result<(), serde_json::Error> {
    if bound.kind != "agent_tool_batch.v1"
        || bound.agent_id.trim().is_empty()
        || bound.run_id.trim().is_empty()
        || bound
            .request
            .session_id
            .as_deref()
            .is_none_or(|session| session.trim().is_empty())
        || bound
            .request
            .parent_execution
            .as_ref()
            .is_none_or(|parent| {
                parent.execution_id.trim().is_empty() || parent.node_id.trim().is_empty()
            })
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch typed identity invalid",
        ));
    }
    Ok(())
}

pub(crate) fn encode_bound_agent_tool(
    agent_id: &str,
    run_id: &str,
    request: RuntimeToolExecutionRequest,
) -> Result<String, serde_json::Error> {
    let bound = BoundAgentTool {
        kind: "agent_tool_batch.v1".into(),
        agent_id: agent_id.into(),
        run_id: run_id.into(),
        request,
    };
    validate_bound_agent_tool(&bound)?;
    crate::step_snapshot::encode_persisted_bound_agent_tool(&serde_json::to_value(bound)?)
}

pub(crate) fn decode_bound_agent_tool(payload: &str) -> Result<BoundAgentTool, serde_json::Error> {
    let value = crate::step_snapshot::decode_persisted_bound_agent_tool_value(payload)?;
    let bound: BoundAgentTool = serde_json::from_value(value)?;
    validate_bound_agent_tool(&bound)?;
    Ok(bound)
}

pub(crate) fn is_bound_ticket(ticket: &NodeExecutionTicket) -> bool {
    bound_payload_identity_shape(&ticket.graph_id, &ticket.node_id)
}

fn bound_payload_identity_shape(graph_id: &str, node_id: &str) -> bool {
    let Some(digest) = graph_id.strip_prefix("agent-tool-batch:") else {
        return false;
    };
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && node_id.strip_prefix("agent-tool:") == Some(digest)
}

pub(crate) fn decode_bound_agent_tool_node(
    graph_id: &str,
    node: &ExecutionNodeSpec,
) -> Result<BoundAgentTool, serde_json::Error> {
    if node.kind != ExecutionNodeKind::ToolBatch
        || node.executor_kind != "tool_batch"
        || !bound_payload_identity_shape(graph_id, &node.id)
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch stored payload hash mismatch",
        ));
    }
    let bound = decode_bound_agent_tool(&node.payload_ref)?;
    let digest = format!("{:x}", Sha256::digest(node.payload_ref.as_bytes()));
    if graph_id != format!("agent-tool-batch:{digest}") || node.id != format!("agent-tool:{digest}")
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch stored payload hash mismatch",
        ));
    }
    Ok(bound)
}

pub(crate) fn decode_bound_agent_tool_graph(
    graph: &ExecutionGraph,
) -> Result<BoundAgentTool, serde_json::Error> {
    if graph.nodes.len() != 1 {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch requires one original node",
        ));
    }
    let bound = decode_bound_agent_tool_node(&graph.id, &graph.nodes[0])?;
    if graph.parent_execution != bound.request.parent_execution || graph.parent_execution.is_none()
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch original parent mismatch",
        ));
    }
    Ok(bound)
}

pub(crate) fn is_bound_agent_tool_graph(graph: &ExecutionGraph) -> bool {
    decode_bound_agent_tool_graph(graph).is_ok()
}

pub(crate) struct AgentToolBatchDispatcher {
    services: Weak<RuntimeServices>,
    agent_id: String,
    run_id: String,
}
impl AgentToolBatchDispatcher {
    pub(crate) fn new(
        services: &Arc<RuntimeServices>,
        packet: &harness_contract::agent::AgentTaskPacket,
    ) -> Self {
        Self {
            services: Arc::downgrade(services),
            agent_id: packet.agent_id().into(),
            run_id: packet.run_id().into(),
        }
    }
    pub(crate) fn execute(
        &self,
        request: RuntimeToolExecutionRequest,
    ) -> futures::future::BoxFuture<'_, Result<RuntimeToolExecutionOutcome, ToolError>> {
        // Tool dispatch is nested inside model/graph futures. Keep its durable
        // registration state off callers' stacks, including concurrent replay.
        Box::pin(async move {
            let services = self
                .services
                .upgrade()
                .ok_or_else(|| ToolError::new("Agent ToolBatch Runtime is unavailable"))?;
            let parent = request
                .parent_execution
                .clone()
                .ok_or_else(|| ToolError::new("Agent ToolBatch omitted parent execution"))?;
            // `parent.execution_id` is the id the Agent was admitted under, which is
            // an agent-run id (`run:<graph_id>`). The store keys on the graph id, so
            // looking the run id up there always fails - and every agent action
            // runs through this load, so every one of them would report a graph
            // that exists as missing.
            let parent_graph = services
                .graph_state_store()
                .load_async(crate::agentic::execution::dispatch::execution_graph_id(
                    &parent.execution_id,
                ))
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let preflight_request = request.clone();
            let payload = encode_bound_agent_tool(&self.agent_id, &self.run_id, request)
                .map_err(|e| ToolError::new(e.to_string()))?;
            let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
            let mut graph = ExecutionGraph::new("Runtime-bound Agent tool invocation");
            graph.id = format!("agent-tool-batch:{digest}");
            graph.parent_execution = Some(parent);
            graph.lineage = parent_graph.lineage;
            graph.service_class = parent_graph.service_class;
            let mut node =
                ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
            node.id = format!("agent-tool:{digest}");
            node.idempotency_key = node.id.clone();
            let node_id = node.id.clone();
            graph.nodes.push(node);
            let graph_id = graph.id.clone();
            match services.graph_state_store().load_async(&graph_id).await {
                Ok(_) => {}
                Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound {
                    ..
                }) => {
                    if let Some(invocation) = &preflight_request.compiled_invocation {
                        services
                            .commit_service()
                            .preflight_fresh_bound_tool(&preflight_request, &invocation.effect)?;
                    }
                }
                Err(error) => return Err(ToolError::new(error.to_string())),
            }
            let supervisor = services.execution_supervisor();
            if let Err(error) = supervisor.register_graph(graph.clone()).await {
                use crate::execution_core::graph::{ExecutionCommitError, ExecutionRunnerError};
                if !matches!(
                    &error,
                    ExecutionRunnerError::Commit(
                        ExecutionCommitError::EventStore(_)
                            | ExecutionCommitError::StaleRevision { .. }
                    )
                ) {
                    return Err(ToolError::new(error.to_string()));
                }
                // Registration races/retries may only attach to the exact durable
                // winner. Never hide policy, validation, or identity conflicts.
                let existing = services
                    .graph_state_store()
                    .load_async(&graph_id)
                    .await
                    .map_err(|_| ToolError::new(error.to_string()))?;
                if existing.nodes != graph.nodes
                    || existing.parent_execution != graph.parent_execution
                    || existing.lineage != graph.lineage
                    || existing.service_class != graph.service_class
                {
                    return Err(ToolError::new("Agent ToolBatch durable identity conflict"));
                }
            }
            let stop = crate::bound_tool_batch::current_tool_stop();
            if let Some(stop) = &stop {
                let mut child = stop
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let identity = (graph_id.clone(), digest.clone());
                if child.as_ref().is_some_and(|prior| prior != &identity) {
                    return Err(ToolError::new(
                        "original call cannot bind a foreign child graph",
                    ));
                }
                *child = Some(identity);
                stop.child_drained
                    .store(false, std::sync::atomic::Ordering::Release);
                #[cfg(test)]
                if stop.stop_after_child_registration {
                    stop.request();
                }
            }
            let drive = async {
                if let Some(stop) = &stop {
                    stop.check()?;
                }
                supervisor
                    .drive_registered(&graph_id)
                    .await
                    .map_err(|e| ToolError::new(e.to_string()))?;
                supervisor
                    .wait_for_terminal(&graph_id)
                    .await
                    .map_err(|e| ToolError::new(e.to_string()))?;
                Ok::<_, ToolError>(())
            };
            tokio::pin!(drive);
            let driven = if let Some(stop) = &stop {
                if stop.check().is_err() {
                    services
                        .cancel_execution_tree(
                            &graph_id,
                            "original Process call stopped after registration",
                        )
                        .await
                        .map_err(|e| ToolError::new(e.to_string()))?;
                    Err(ToolError::new(
                        "original tool call stopped before child drive",
                    ))
                } else {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {
                            // Keep the original driver alive while the original
                            // supervisor cancels this exact child, never parent.
                            let (cancelled, driven) = tokio::join!(
                                services.cancel_execution_tree(&graph_id,"original Process call stopped"),
                                &mut drive);
                            cancelled.map_err(|e|ToolError::new(e.to_string()))?;
                            driven
                        },
                        result = &mut drive => result,
                    }
                }
            } else {
                drive.await
            };
            let settled = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            if settled.nodes != graph.nodes || settled.parent_execution != graph.parent_execution {
                return Err(ToolError::new(
                    "original Agent child binding changed during drain",
                ));
            }
            if let Some(attempt) = settled
                .recovery_cursor
                .node_attempts
                .get(&node_id)
                .filter(|attempt| **attempt > 0)
            {
                let node = graph
                    .nodes
                    .first()
                    .ok_or_else(|| ToolError::new("original Agent child node missing"))?;
                let ticket = NodeExecutionTicket {
                    graph_id: graph_id.clone(),
                    node_id: node.id.clone(),
                    executor_kind: node.executor_kind.clone(),
                    service_class: graph.service_class,
                    attempt: *attempt,
                    idempotency_key: node.idempotency_key.clone(),
                    payload_ref: node.payload_ref.clone(),
                };
                services
                    .tool_batch_executor()
                    .close_and_drain(&ticket, false)
                    .await
                    .map_err(|error| ToolError::new(error.to_string()))?;
                if let Some(stop) = &stop {
                    let no_dispatch = services
                        .tool_batch_executor()
                        .binding_observation(&ticket)
                        .and_then(|observation| observation.witness())
                        .is_some_and(|witness| witness.authenticate(&ticket, None).is_ok());
                    let receipted = if let Some(invocation) = &preflight_request.compiled_invocation
                    {
                        services
                            .commit_service()
                            .completed_tool_effect(&preflight_request, &invocation.effect)
                            .map_err(|error| ToolError::new(error.to_string()))?
                            .is_some()
                    } else {
                        false
                    };
                    stop.effect_settled.store(
                        no_dispatch || receipted,
                        std::sync::atomic::Ordering::Release,
                    );
                }
            } else if let Some(stop) = &stop {
                if stop.check().is_err() {
                    supervisor
                        .confirm_cancelled_bound_child_never_admitted(&graph)
                        .await
                        .map_err(|error| ToolError::new(error.to_string()))?;
                    stop.effect_settled
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
            if let Some(stop) = &stop {
                if !settled.nodes.iter().all(|node| {
                    settled
                        .node_statuses
                        .get(&node.id)
                        .is_some_and(|state| state.is_terminal())
                }) {
                    return Err(ToolError::new(
                        "original Agent child is not terminal after drain",
                    ));
                }
                stop.child_drained
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            driven?;
            let graph = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let result = graph
                .node_results
                .get(&node_id)
                .and_then(|result| result.result_ref.as_deref())
                .and_then(|value| value.strip_prefix("agent-tool-outcome:"))
                .ok_or_else(|| {
                    ToolError::new(
                        graph
                            .node_results
                            .get(&node_id)
                            .and_then(|result| result.failure.as_ref())
                            .map(|failure| failure.message.clone())
                            .unwrap_or_else(|| {
                                "Agent ToolBatch has no committed tool outcome".into()
                            }),
                    )
                })?;
            serde_json::from_str(result).map_err(|e| ToolError::new(e.to_string()))
        })
    }
}

pub(crate) struct AgentToolBatchResolver {
    services: Weak<RuntimeServices>,
}
impl AgentToolBatchResolver {
    pub(crate) fn install(services: &Arc<RuntimeServices>) {
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(Self {
                services: Arc::downgrade(services),
            }));
    }
}
impl ScopedNodeBackendResolver for AgentToolBatchResolver {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        if ticket.executor_kind != "tool_batch"
            || !bound_payload_identity_shape(&ticket.graph_id, &ticket.node_id)
        {
            return None;
        }
        let bound = decode_bound_agent_tool(&ticket.payload_ref).ok()?;
        let digest = format!("{:x}", Sha256::digest(ticket.payload_ref.as_bytes()));
        if ticket.graph_id != format!("agent-tool-batch:{digest}")
            || ticket.node_id != format!("agent-tool:{digest}")
        {
            return None;
        }
        Some(Arc::new(AgentToolBatchBackend {
            services: self.services.upgrade()?,
            bound,
            stop: Default::default(),
        }))
    }
}
struct AgentToolBatchBackend {
    services: Arc<RuntimeServices>,
    bound: BoundAgentTool,
    stop: crate::bound_tool_batch::BoundToolStop,
}

fn agent_tool_outcome(
    outcome: &RuntimeToolExecutionOutcome,
) -> Result<NodeExecutionOutcome, serde_json::Error> {
    agent_tool_outcome_at(
        outcome,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    )
}

pub(crate) fn agent_tool_outcome_at(
    outcome: &RuntimeToolExecutionOutcome,
    finished_at_ms: u64,
) -> Result<NodeExecutionOutcome, serde_json::Error> {
    let encoded = serde_json::to_string(outcome)?;
    Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some(format!("agent-tool-outcome:{encoded}")),
        wait_class: None,
        summary: None,
        evidence_refs: vec![],
        failure: None,
        usage: Default::default(),
        finished_at_ms,
    }))
}

pub(super) fn finish_observed_outcome(
    ticket: &NodeExecutionTicket,
    observation: Option<&NoDispatchObservation>,
    result: Result<NodeExecutionOutcome, NodeExecutorError>,
) -> Result<NodeExecutionOutcome, NodeExecutorError> {
    let mut outcome = match result {
        Ok(outcome) => outcome,
        Err(error) => {
            let refusal = decode_bound_agent_tool(&ticket.payload_ref)
                .ok()
                .and_then(|bound| {
                    observation.and_then(|observation| observation.refusal(&bound.request))
                });
            match refusal {
                Some(refusal) => {
                    agent_tool_outcome(&refusal).map_err(|error| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: error.to_string(),
                    })?
                }
                None => return Err(error),
            }
        }
    };
    if let Some(observation) = observation {
        observation
            .finish(&mut outcome)
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
    }
    Ok(outcome)
}

fn agent_tool_leaf_result(
    result: Result<Result<RuntimeToolExecutionOutcome, ToolError>, crate::ToolExecutionPlaneError>,
    on_leaf_error: impl FnOnce(ToolError) -> Result<RuntimeToolExecutionOutcome, ToolError>,
) -> Result<RuntimeToolExecutionOutcome, ToolError> {
    match result.map_err(|error| ToolError::new(error.to_string()))? {
        Ok(outcome) => Ok(outcome),
        Err(error) => on_leaf_error(error),
    }
}

fn original_process_step(
    services: &RuntimeServices,
    request: &RuntimeToolExecutionRequest,
) -> Result<(), ToolError> {
    original_process_step_from_store(services.event_store(), request)
}

fn original_process_step_from_store(
    store: &crate::RuntimeEventStore,
    request: &RuntimeToolExecutionRequest,
) -> Result<(), ToolError> {
    let denied = || ToolError::new("stale Step rejection has no original Process Step");
    let binding = request.step_binding.as_ref().ok_or_else(denied)?;
    let snapshot = serde_json::to_value(binding.snapshot.as_ref())
        .map_err(|error| ToolError::new(error.to_string()))?;
    let through = store
        .durable_commit_cursor()
        .map_err(|error| ToolError::new(error.to_string()))?;
    let mut after = None;
    loop {
        let page = store
            .position_page(
                crate::RuntimeEventScope::Session,
                &binding.snapshot.identity.session_id,
                Some("process.step_bound"),
                after,
                through,
                100,
                1024 * 1024,
            )
            .map_err(ToolError::new)?;
        if page.is_empty() {
            return Err(denied());
        }
        for event in &page {
            if event.payload["identity"]["step_id"] == binding.snapshot.identity.step_id {
                if event.stream_id != format!("session:{}", binding.snapshot.identity.session_id)
                    || event.scope != crate::RuntimeEventScope::Session
                    || event.kind != "process.step_bound"
                    || event.schema_version != 1
                    || event.actor.as_deref() != binding.snapshot.identity.actor_ref.as_deref()
                    || event.status.as_deref() != Some("prepared")
                    || event.payload != snapshot
                {
                    return Err(denied());
                }
                return Ok(());
            }
        }
        let last = page.last().ok_or_else(denied)?;
        let next = (last.commit_cursor, last.transaction_index);
        if after.is_some_and(|previous| next <= previous) {
            return Err(denied());
        }
        after = Some(next);
    }
}

fn agent_read_terminal(
    services: &RuntimeServices,
    request: &RuntimeToolExecutionRequest,
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Result<Option<RuntimeToolExecutionOutcome>, ToolError> {
    agent_read_terminal_from_store(
        services.event_store(),
        services.commit_service(),
        request,
        descriptor,
    )
}

pub(crate) fn agent_read_terminal_from_store(
    store: &crate::RuntimeEventStore,
    commits: &crate::ExecutionCommitService,
    request: &RuntimeToolExecutionRequest,
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Result<Option<RuntimeToolExecutionOutcome>, ToolError> {
    let denied = || ToolError::new("Agent read terminal primary/index identity conflict");
    let parent = request.parent_execution.as_ref().ok_or_else(denied)?;
    let attempt = request
        .parent_execution_attempt
        .filter(|attempt| *attempt > 0)
        .ok_or_else(denied)?;
    let primary_stream = format!("execution-effect:{}", request.idempotency_key);
    let primary_key = format!("{}:read-receipt", request.idempotency_key);
    let index_stream = format!(
        "execution-agent-receipts:{}:{}:{attempt}",
        parent.execution_id, parent.node_id
    );
    let index_key = format!("agent-tool-receipt:{}", request.idempotency_key);
    store.with_stream_locks(&[primary_stream.clone(), index_stream.clone()], || {
        let primary = store
            .event_by_idempotency_key(&primary_stream, &primary_key)
            .map_err(|error| ToolError::new(error.to_string()))?;
        let index = store
            .event_by_idempotency_key(&index_stream, &index_key)
            .map_err(|error| ToolError::new(error.to_string()))?;
        let (primary, index) = match (primary, index) {
            (None, None) => return Ok(None),
            (Some(primary), Some(index)) => (primary, index),
            _ => return Err(denied()),
        };
        let binding = request.step_binding.as_ref().ok_or_else(denied)?;
        let mut invocation = request.compiled_invocation.clone().ok_or_else(denied)?;
        let input = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|error| ToolError::new(error.to_string()))?;
        if invocation.origin.as_ref() != Some(&binding.origin)
            || invocation.invocation_id != request.tool_use_id
            || invocation.intent.tool_name != request.tool_name
            || invocation.intent.normalized_input != input
            || invocation.effect != *descriptor
            || binding.origin.call_id != request.tool_use_id
            || binding.effect_idempotency_key() != request.idempotency_key
        {
            return Err(denied());
        }
        invocation.intent.normalized_input = serde_json::Value::Null;
        let digest = harness_contract::step::canonical_digest("tool-effect-payload", &input)
            .map_err(|error| ToolError::new(error.to_string()))?;
        let replay = serde_json::json!({
            "session_id":request.session_id, "parent_execution":request.parent_execution,
            "call_id":request.tool_use_id, "origin":binding.origin,
            "payload_digest":digest, "invocation":invocation,
        });
        let expected_refs = [
            ("tool_invocation", request.tool_use_id.as_str()),
            ("execution_graph", parent.execution_id.as_str()),
            ("execution_node", parent.node_id.as_str()),
        ];
        for event in [&primary, &index] {
            if event.scope != crate::RuntimeEventScope::ExecutionNode
                || event.schema_version != 1
                || event.actor.as_deref() != Some("governed_tool")
                || event.status.as_deref() != Some("completed")
                || event.payload["idempotency_key"] != request.idempotency_key
                || expected_refs.iter().any(|(kind, id)| {
                    !event
                        .refs
                        .iter()
                        .any(|reference| reference.kind == *kind && reference.id == *id)
                })
                || !event.refs.iter().any(|reference| {
                    reference.kind == "agent_attempt" && reference.id == attempt.to_string()
                })
                || event.refs.len() != 4
            {
                return Err(denied());
            }
        }
        if primary.kind != "execution.read.receipt"
            || primary.stream_id != primary_stream
            || primary.idempotency_key.as_deref() != Some(primary_key.as_str())
            || index.kind != "execution.agent_tool.receipt"
            || index.stream_id != index_stream
            || index.idempotency_key.as_deref() != Some(index_key.as_str())
            || primary.transaction_id != index.transaction_id
            || primary.commit_cursor != index.commit_cursor
            || index.transaction_index <= primary.transaction_index
            || primary.payload["tool_use_id"] != request.tool_use_id
            || primary.payload["tool_name"] != request.tool_name
            || primary.payload["replay"] != replay
            || primary.payload["effect"]
                != serde_json::to_value(descriptor)
                    .map_err(|error| ToolError::new(error.to_string()))?
            || primary.payload["effect"] != index.payload["effect"]
            || primary.payload["outcome"] != index.payload["outcome"]
            || primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"]
            || primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
                != index.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
            || index.payload["effect_kind"] != "read"
            || crate::ExecutionCommitService::receipt_observation_sequence(&index).is_none()
            || index.payload["sequence"].as_u64().is_none_or(|sequence| sequence == 0)
            || index.payload["effect_scope"]
                != serde_json::to_value(
                    request
                        .authorization
                        .as_ref()
                        .map(|authorization| &authorization.scope),
                )
                .map_err(|error| ToolError::new(error.to_string()))?
            || index.payload["authorized_scopes"]
                != serde_json::to_value(&request.authorized_scopes)
                    .map_err(|error| ToolError::new(error.to_string()))?
        {
            return Err(denied());
        }
        let outcome = commits
            .completed_tool_effect(request, descriptor)
            .map_err(|error| ToolError::new(error.to_string()))?
            .ok_or_else(denied)?;
        if outcome.tool_use_id != request.tool_use_id
            || outcome.tool_name != request.tool_name
            || outcome.category != request.category
        {
            return Err(denied());
        }
        if primary.payload["resolved_effect_scope"].is_null() {
            if outcome.status != crate::RuntimeToolExecutionStatus::Failed
                || outcome.error.as_deref() != Some("step_binding_stale")
                || outcome.output.is_some()
                || !outcome.observed_evidence.is_empty()
                || outcome.evidence_ref != format!("tool-effect:{}", request.idempotency_key)
                || primary.payload["outcome_truncated"] != false
                || !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null()
                || [&primary, &index].iter().any(|event| {
                    !event.payload["process_admission_ref"].is_null()
                        || !event.payload["root_effect_owner"].is_null()
                        || !event.payload["root_effect_context"].is_null()
                })
            {
                return Err(denied());
            }
            original_process_step_from_store(store, request)?;
        }
        Ok(Some(outcome))
    })
}

impl AgentToolBatchBackend {
    fn stale_read_rejection(
        &self,
        request: &RuntimeToolExecutionRequest,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        agent: &harness_contract::agent::AgentTaskPacket,
        turn_id: &str,
        error: ToolError,
    ) -> Result<RuntimeToolExecutionOutcome, ToolError> {
        let reject = || error.clone();
        if error.to_string() != "step_binding_stale" {
            return Err(error);
        }
        let binding = request.step_binding.as_ref().ok_or_else(reject)?;
        let snapshot = binding.snapshot.as_ref();
        let identity = &snapshot.identity;
        let invocation = request.compiled_invocation.as_ref().ok_or_else(reject)?;
        let effect = &invocation.effect;
        if !matches!(&binding.origin.source,
                harness_contract::step::CallSource::Process { worker_id, sequence }
                    if worker_id == agent.run_id() && *sequence > 0)
            || !agent.allowed_tools.contains(&request.tool_name)
            || identity.execution_id.as_deref() != Some(agent.graph_id())
            || identity.session_id != agent.session_id()
            || identity.turn_id != turn_id
            || identity.run_ref.as_deref() != Some(agent.run_id())
            || identity.actor_ref.as_deref() != Some(agent.agent_id())
            || identity.attempt != Some(agent.attempt)
            || binding.origin.origin_step_id != identity.step_id
            || binding.origin.call_id != request.tool_use_id
            || binding.effect_idempotency_key() != request.idempotency_key
            || binding.origin.catalog_digest != snapshot.catalog_digest
            || snapshot
                .related_revisions
                .get("worker_run")
                .map(String::as_str)
                != Some(agent.run_id())
            || invocation.origin.as_ref() != Some(&binding.origin)
            || invocation.invocation_id != request.tool_use_id
            || invocation.intent.tool_name != request.tool_name
            || invocation.intent.normalized_input
                != harness_contract::step::parse_strict_json(&request.input)
                    .map_err(|_| reject())?
            || effect != descriptor
            || effect.tool_id != request.tool_name
            || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
            || effect.required_permission != harness_contract::tool::ToolPermissionMode::ReadOnly
            || effect.uses_network
            || effect.spawns_process
            || effect.mutates_packages
            || effect.mutates_system
            || effect
                .scopes
                .iter()
                .any(|scope| scope.operation != harness_contract::policy::PermissionOperation::Read)
            || snapshot.contract_version != harness_contract::step::STEP_CONTRACT_VERSION
            || snapshot.provider_attempt.origin_step_id != identity.step_id
            || snapshot.provider_attempt.attempt_id.is_empty()
            || !snapshot.tools.iter().any(|tool| {
                tool.tool_id == request.tool_name
                    && tool.availability != harness_contract::step::CatalogAvailability::Unavailable
            })
        {
            return Err(error);
        }
        if let Some(winner) = agent_read_terminal(&self.services, request, descriptor)? {
            return Ok(winner);
        }
        let bounded = agent.team_id().is_some()
            || agent.agentic_binding.is_some()
            || agent
                .binding
                .as_ref()
                .is_some_and(|binding| binding.evaluation.is_some());
        let mut expected_scopes = if bounded {
            agent.resource_scopes.clone()
        } else {
            vec![]
        };
        let session_scope = format!("session:{}", agent.session_id());
        if !expected_scopes.contains(&session_scope) {
            expected_scopes.push(session_scope);
        }
        let policy = self
            .services
            .session_execution_policy(agent.session_id())
            .ok_or_else(reject)?;
        let authorization = request.authorization.as_ref().ok_or_else(reject)?;
        let lease = self
            .services
            .tool_execution_host()
            .and_then(|host| host.pin_step_catalog())
            .ok_or_else(reject)?;
        let input =
            harness_contract::step::parse_strict_json(&request.input).map_err(|_| reject())?;
        if request.authorized_scopes != expected_scopes
            || request.category != crate::ToolSafetyCategory::from_effect(descriptor)
            || !policy
                .permission_mode
                .permits(descriptor.required_permission)
            || !agent
                .permission_ceiling
                .permits(descriptor.required_permission)
            || policy.revision != request.policy_revision
            || policy.sandbox_posture != request.sandbox_posture
            || request.policy_revision == 0
            || request.policy_revision != agent.policy_revision
            || authorization.policy_revision != request.policy_revision
            || authorization.authorization_lease.principal_id
                != format!("agent:{}", agent.agent_id())
            || snapshot.related_revisions.get("policy")
                != Some(&request.policy_revision.to_string())
            || snapshot.related_revisions.get("attempt") != Some(&agent.attempt.to_string())
            || chrono::Utc::now().timestamp_millis() >= snapshot.expires_at_ms
            || lease.describe_effect(&request.tool_name, &input) != *descriptor
            || lease
                .validate_authorization(authorization, &request.tool_name, &input)
                .is_err()
        {
            return Err(error);
        }
        original_process_step(&self.services, request)?;
        let outcome = RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Failed,
            category: request.category,
            output: None,
            error: Some(error.to_string()),
            evidence_ref: format!("tool-effect:{}", request.idempotency_key),
            observed_evidence: vec![],
        };
        let committed = self
            .services
            .commit_service()
            .commit_readonly_tool_receipts(&[(request.clone(), outcome)]);
        match agent_read_terminal(&self.services, request, descriptor)? {
            Some(winner) => Ok(winner),
            None => Err(ToolError::new(match committed {
                Err(error) => format!("stale Step rejection receipt commit failed: {error}"),
                Ok(()) => "stale Step rejection receipt missing after commit".into(),
            })),
        }
    }
}

#[async_trait]
impl ScopedNodeBackend for AgentToolBatchBackend {
    fn retains_physical_execution(&self) -> bool {
        true
    }
    fn request_stop(&self, _ticket: &NodeExecutionTicket) {
        self.stop.request();
    }
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let observation = self
            .services
            .tool_batch_executor()
            .binding_observation(ticket);
        let fail = |reason: String| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        };
        let request = &self.bound.request;
        let refuse = |reason: String| {
            if let Some(observation) = &observation {
                observation.refuse(&self.bound.request.tool_use_id, reason.clone());
            }
            fail(reason)
        };
        let graph = self
            .services
            .graph_state_store()
            .load_async(&ticket.graph_id)
            .await
            .map_err(|e| refuse(e.to_string()))?;
        if !is_bound_agent_tool_graph(&graph) || graph.service_class != ticket.service_class {
            return Err(refuse(
                "Agent ToolBatch ticket does not match its durable graph".into(),
            ));
        }
        let parent = request
            .parent_execution
            .as_ref()
            .ok_or_else(|| refuse("missing Agent parent".into()))?;
        let parent_graph = self
            .services
            .graph_state_store()
            .load_async(&parent.execution_id)
            .await
            .map_err(|e| refuse(e.to_string()))?;
        if graph.lineage != parent_graph.lineage
            || graph.service_class != parent_graph.service_class
        {
            return Err(refuse(
                "Agent ToolBatch cannot change its parent's lineage or service class".into(),
            ));
        }
        let parent_node = parent_graph
            .nodes
            .iter()
            .find(|node| node.id == parent.node_id && node.kind == ExecutionNodeKind::AgentTask)
            .ok_or_else(|| refuse("Agent ToolBatch parent is not a durable AgentTask".into()))?;
        let agent: harness_contract::agent::AgentTaskPacket =
            serde_json::from_str(&parent_node.payload_ref)
                .map_err(|e| refuse(format!("invalid durable Agent packet: {e}")))?;
        if agent.run_id() != self.bound.run_id
            || agent.agent_id() != self.bound.agent_id
            || agent.graph_id() != parent.execution_id
            || agent.node_id() != parent.node_id
            || Some(agent.attempt) != request.parent_execution_attempt
            || request.session_id.as_deref() != Some(agent.session_id())
            || request.governed_plan_id != parent.execution_id
        {
            return Err(refuse(
                "tool_effect_authority: Agent ToolBatch focus does not match its canonical run"
                    .into(),
            ));
        }
        if !agent.allowed_tools.contains(&request.tool_name) && request.tool_name != "tool_search" {
            return Err(refuse(
                "tool_effect_authority: Agent ToolBatch focus does not match its canonical run"
                    .into(),
            ));
        }
        let host = self
            .services
            .tool_execution_host()
            .ok_or_else(|| refuse("Agent ToolBatch ToolHost unavailable".into()))?;
        let input = serde_json::from_str(&request.input)
            .map_err(|e| refuse(format!("invalid tool input: {e}")))?;
        let descriptor = host
            .delegated_tool_effect_descriptor(&request.tool_name, &input)
            .ok_or_else(|| refuse("missing enforceable tool descriptor".into()))?;
        if !agent.allowed_tools.contains(&request.tool_name) {
            let reject = || {
                refuse("tool_effect_authority: Agent ToolBatch bootstrap does not match its canonical run or readonly catalog".into())
            };
            if request.tool_name != "tool_search" {
                return Err(reject());
            }
            let lease = host.pin_step_catalog().ok_or_else(reject)?;
            let catalog = &lease.snapshot().catalog;
            let metadata = lease.catalog_receipt();
            let registered = metadata
                .descriptors
                .iter()
                .find(|entry| entry.canonical_id == "tool_search")
                .ok_or_else(reject)?;
            let definitions = catalog.definitions(Some(&std::collections::BTreeSet::from([
                "tool_search".into(),
            ])));
            let definition = definitions.first().ok_or_else(reject)?;
            let exposed = host.delegated_tool_definitions(&["tool_search".into()]);
            let mut actual = exposed.iter().filter(|actual| actual.name == "tool_search");
            let actual_definition = actual.next().ok_or_else(reject)?;
            if catalog.canonical_name("tool_search").as_deref() != Some("tool_search")
                || registered.source != "builtin"
                || registered.health != harness_contract::tool::ToolDescriptorHealth::Healthy
                || registered.required_permission
                    != harness_contract::tool::ToolPermissionMode::ReadOnly
                || catalog.required_permission("tool_search")
                    != Some(harness_contract::tool::ToolPermissionMode::ReadOnly)
                || catalog.effect_resolver("tool_search")
                    != tools::builtin_effect_resolver_spec("tool_search")
                || definitions.len() != 1
                || actual.next().is_some()
                || actual_definition.input_schema != definition.input_schema
                || actual_definition.description != definition.description
                || descriptor != lease.describe_effect("tool_search", &input)
                || descriptor.tool_id != "tool_search"
                || descriptor.effect_kind != harness_contract::tool::ToolEffectKind::Read
                || descriptor.required_permission
                    != harness_contract::tool::ToolPermissionMode::ReadOnly
                || descriptor.uses_network
                || descriptor.spawns_process
                || descriptor.mutates_packages
                || descriptor.mutates_system
                || descriptor.scopes.iter().any(|scope| {
                    scope.operation != harness_contract::policy::PermissionOperation::Read
                })
            {
                return Err(reject());
            }
            let binding = request.step_binding.as_ref().ok_or_else(reject)?;
            let step = binding
                .resolve(Some(lease))
                .map_err(|error| refuse(error.to_string()))?;
            step.validate_call(
                &binding.origin,
                agent.session_id(),
                &request.tool_use_id,
                &request.tool_name,
                &request.input,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|error| refuse(error.to_string()))?;
            let identity = &step.snapshot().identity;
            let source_matches = match &binding.origin.source {
                harness_contract::step::CallSource::Provider { .. } => {
                    identity.execution_id.as_deref() == Some(agent.run_id())
                        && identity
                            .attempt
                            .is_none_or(|attempt| attempt == agent.attempt)
                }
                harness_contract::step::CallSource::Process { worker_id, .. } => {
                    identity.execution_id.as_deref() == Some(agent.graph_id())
                        && identity.attempt == Some(agent.attempt)
                        && worker_id == agent.run_id()
                }
                _ => false,
            };
            if identity.run_ref.as_deref() != Some(agent.run_id()) || !source_matches {
                return Err(reject());
            }
        }
        let demand = crate::governed_tool_plan::resource_demand_from_effect(&descriptor);
        let (outcome, admission) = crate::bound_tool_batch::with_tool_stop(
            self.stop.clone(),
            self.services
                .tool_execution_plane()
                .execute_async_classified_retained(
                    &demand,
                    None,
                    ticket.service_class,
                    Some(ticket.service_class),
                    request.session_id.as_deref(),
                    crate::bound_tool_batch::execute_bound_agent_tool_observed(
                        host.as_ref(),
                        Some(self.services.commit_service()),
                        self.services.path_identity_resolver(),
                        self.services.scope_locks(),
                        request,
                        &descriptor,
                        observation.as_deref(),
                    ),
                ),
        )
        .await;
        if let Err(error) = &outcome {
            if let Some(observation) = &observation {
                match error {
                    crate::ToolExecutionPlaneError::Admission(_) => {
                        observation.refuse(&request.tool_use_id, error.to_string())
                    }
                    _ => observation.uncertain(request),
                }
            }
        }
        let outcome = agent_tool_leaf_result(outcome, |error| {
            if error.to_string() == "step_binding_stale"
                && agent.binding.as_ref().is_some_and(|binding| {
                    matches!(
                        &binding.executor,
                        harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }
                    )
                })
            {
                if let Some(observation) = &observation {
                    observation.uncertain(request);
                }
                let rejection = self.stale_read_rejection(
                    request,
                    &descriptor,
                    &agent,
                    graph
                        .lineage
                        .as_ref()
                        .map(|lineage| lineage.turn_id.as_str())
                        .unwrap_or(""),
                    error,
                )?;
                return Ok(rejection);
            }
            if let Some(refusal) = observation
                .as_ref()
                .and_then(|observation| observation.refusal(request))
            {
                return Ok(refusal);
            }
            self.stale_read_rejection(
                request,
                &descriptor,
                &agent,
                graph
                    .lineage
                    .as_ref()
                    .map(|lineage| lineage.turn_id.as_str())
                    .unwrap_or(""),
                error,
            )
        })
        .map_err(|error| fail(error.to_string()))?;
        let outcome = if outcome.status == crate::RuntimeToolExecutionStatus::Failed
            && outcome.error.as_deref() == Some("step_binding_stale")
            && observation
                .as_ref()
                .and_then(|observation| observation.refusal(request))
                .is_none()
            && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read
        {
            agent_read_terminal(&self.services, request, &descriptor)
                .map_err(|error| fail(error.to_string()))?
                .ok_or_else(|| fail("stale Step rejection has no canonical terminal".into()))?
        } else {
            outcome
        };
        // Leaf effect/receipt has committed under its admission. The graph
        // outcome below is the delivery fence; callers only read it after commit.
        let result = agent_tool_outcome(&outcome).map_err(|error| fail(error.to_string()))?;
        drop(admission);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn agent_tool_366_outer_resource_error_never_enters_leaf_rejection() {
        use crate::execution_core::graph::{
            ExecutionResourceKind, ExecutionResourceManager, ResourceQuota, ScopeLockManager,
        };
        use harness_contract::tool::{ResourceAccess, ResourceDemand, ResourceScopeDemand};
        use std::sync::atomic::{AtomicBool, Ordering};
        let plane = crate::ToolExecutionPlane::new(
            Arc::new(ExecutionResourceManager::new([(
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 1, 1).unwrap(),
            )])),
            Arc::new(ScopeLockManager::new()),
        );
        let demand = ResourceDemand {
            scopes: vec![ResourceScopeDemand {
                key: String::new(),
                access: ResourceAccess::Read,
            }],
            ..ResourceDemand::default()
        };
        let polled = AtomicBool::new(false);
        let handled = AtomicBool::new(false);
        let (result, admission) = plane
            .execute_async_classified_retained(
                &demand,
                None,
                harness_contract::execution_graph::ExecutionServiceClass::Foreground,
                None,
                Some("outer-resource-366"),
                async {
                    polled.store(true, Ordering::SeqCst);
                    Err::<RuntimeToolExecutionOutcome, _>(ToolError::new("step_binding_stale"))
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(crate::ToolExecutionPlaneError::Admission(_))
        ));
        assert!(admission.is_none());
        let error = agent_tool_leaf_result(result, |_| {
            handled.store(true, Ordering::SeqCst);
            panic!("outer resource error must not create a stale rejection receipt")
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .starts_with("tool resource admission failed:"));
        assert!(!polled.load(Ordering::SeqCst));
        assert!(!handled.load(Ordering::SeqCst));
        assert_eq!(plane.stats().active, 0);
    }

    #[test]
    fn typed_tool_graph_cannot_borrow_a_foreign_parent_or_payload_identity() {
        let mut request =
            RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
                tool_use_id: "invocation".into(),
                tool_name: "read_file".into(),
                input: "{}".into(),
                depends_on: vec![],
            });
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "parent".into(),
                node_id: "agent".into(),
            });
        let parent = request.parent_execution.clone();
        request.session_id = Some("session".into());
        let payload = encode_bound_agent_tool("agent", "run", request).unwrap();
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let mut graph = ExecutionGraph::new("typed fixture");
        graph.id = format!("agent-tool-batch:{digest}");
        graph.parent_execution = parent;
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = format!("agent-tool:{digest}");
        graph.nodes.push(node);
        assert!(is_bound_agent_tool_graph(&graph));
        let mut foreign = graph.clone();
        foreign.nodes[0].executor_kind = "inline".into();
        foreign.nodes[0].payload_ref = "{invalid-json".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.nodes[0].kind = ExecutionNodeKind::InlineModel;
        foreign.nodes[0].payload_ref = "{invalid-json".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.id = "unrelated-root".into();
        foreign.nodes[0].payload_ref = "{invalid-json".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.parent_execution.as_mut().unwrap().execution_id = "foreign-parent".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes[0].payload_ref = "{\"kind\":\"turn_tool_batch\"}".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes[0].id = "foreign-node".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes.push(graph.nodes[0].clone());
        assert!(!is_bound_agent_tool_graph(&foreign));
    }
}

#[cfg(test)]
pub(crate) mod carrier_437_tests {
    use super::*;
    use base64::Engine;
    use harness_contract::policy::PermissionMode;

    pub(crate) fn real_request(workspace: &std::path::Path) -> RuntimeToolExecutionRequest {
        std::fs::write(workspace.join("proof.txt"), "original input").unwrap();
        let host = tools::ToolHost::builtin("carrier-437", workspace);
        let lease = host.pin_snapshot();
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == "read_file")
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(definitions.len(), 1);
        let identity = serde_json::from_value(serde_json::json!({
            "step_id":"step-437","session_id":"session-437","turn_id":"turn-437",
            "execution_id":"run-437","actor_ref":"agent:agent-437","run_ref":"run-437","attempt":3,
            "goal_ref":"goal-437","program_ref":"program-437","task_ref":"task-437","build_ref":"build-437"
        })).unwrap();
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                identity,
                lease.clone(),
                &definitions,
                &serde_json::json!({"profile":"original"}),
                &serde_json::json!({"request":"original"}),
                std::collections::BTreeMap::from([("goal".into(), "7".into())]),
                chrono::Utc::now()
                    .timestamp_millis()
                    .saturating_add(3_600_000),
            )
            .unwrap(),
        );
        let binding = crate::step_snapshot::StepCallBinding::provider(step, "call-437");
        let input = " { \"path\" : \"proof.txt\" } ";
        let invocation = binding
            .prepare_invocation(
                Some(lease),
                "read_file",
                &serde_json::from_str(input).unwrap(),
            )
            .unwrap();
        let policy = crate::PermissionPolicy::new(PermissionMode::ReadOnly);
        let assessment = crate::AuthorizationNegotiator::new().assess_effective(
            &policy,
            &crate::AuthorizationRequest {
                principal_id: "agent:agent-437".into(),
                capability: "read_file".into(),
                input: input.into(),
                idempotency_key: binding.effect_idempotency_key(),
                effect: invocation.effect.clone(),
                parent_ceiling: PermissionMode::ReadOnly,
                parent_lease_id: None,
                policy_revision: 7,
                recovery_scope: "run-437".into(),
                context: crate::PermissionContext::default(),
                safe_alternatives: Vec::new(),
            },
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &assessment.effective,
                &assessment.assessment,
                binding.origin.call_id.clone(),
                assessment.assessment.lease.clone().unwrap(),
                30,
            )
            .unwrap()
            .authorization;
        let mut request =
            RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
                tool_use_id: binding.origin.call_id.clone(),
                tool_name: "read_file".into(),
                input: input.into(),
                depends_on: vec!["original-dependency".into()],
            });
        request.idempotency_key = binding.effect_idempotency_key();
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "parent-437".into(),
                node_id: "agent-437".into(),
            });
        request.parent_execution_attempt = Some(3);
        request.governed_plan_id = "parent-437".into();
        request.governed_plan_revision = 7;
        request.observation_wave_sequence = 11;
        request.session_id = Some("session-437".into());
        request.policy_revision = 7;
        request.category = crate::ToolSafetyCategory::ReadOnly;
        request.authorized_scopes = vec!["session:session-437".into()];
        request.model_lease = Some("original-model".into());
        request.compiled_invocation = Some(invocation);
        request.step_binding = Some(binding);
        request.authorization = Some(authorization);
        request
    }

    pub(crate) fn graph_for(request: RuntimeToolExecutionRequest) -> ExecutionGraph {
        let parent = request.parent_execution.clone();
        let payload = encode_bound_agent_tool("agent-437", "run-437", request).unwrap();
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let mut graph = ExecutionGraph::new("Title is not a carrier discriminator");
        graph.id = format!("agent-tool-batch:{digest}");
        graph.parent_execution = parent;
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = format!("agent-tool:{digest}");
        node.idempotency_key = node.id.clone();
        graph.nodes.push(node);
        graph
    }

    fn unchecked_test_pack(value: &serde_json::Value, session: &str) -> String {
        let bytes = serde_json::to_vec(value).unwrap();
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut encoder, &bytes).unwrap();
        let compressed = encoder.finish().unwrap();
        serde_json::json!({"kind":"cowd_bound_agent_tool_zlib_v1","session_id":session,
            "decoded_bytes":bytes.len(),"sha256":format!("sha256:{:x}",Sha256::digest(&bytes)),
            "body":base64::engine::general_purpose::STANDARD.encode(compressed)})
        .to_string()
    }

    #[test]
    fn bound_437_real_session_step_grant_request_roundtrip() {
        let workspace = tempfile::tempdir().unwrap();
        let original = real_request(workspace.path());
        let original_json = serde_json::to_value(&original).unwrap();
        let input_digest = format!("{:x}", Sha256::digest(original.input.as_bytes()));
        let graph = graph_for(original.clone());
        let packed: serde_json::Value = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
        assert_eq!(packed["kind"], "cowd_bound_agent_tool_zlib_v1");
        assert_eq!(packed["session_id"], "session-437");
        let bound = decode_bound_agent_tool_graph(&graph).unwrap();
        assert_eq!(serde_json::to_value(&bound.request).unwrap(), original_json);
        assert_eq!(bound.request, original);
        assert_eq!(
            format!("{:x}", Sha256::digest(bound.request.input.as_bytes())),
            input_digest
        );
        let old_step = original.step_binding.as_ref().unwrap();
        let new_step = bound.request.step_binding.as_ref().unwrap();
        assert_eq!(
            new_step.snapshot.provider_attempt.request_digest,
            old_step.snapshot.provider_attempt.request_digest
        );
        assert_eq!(new_step.origin, old_step.origin);
        assert_eq!(bound.request.authorization, original.authorization);
        assert_eq!(bound.request.parent_execution_attempt, Some(3));
        let digest = format!(
            "{:x}",
            Sha256::digest(graph.nodes[0].payload_ref.as_bytes())
        );
        assert_eq!(graph.id, format!("agent-tool-batch:{digest}"));
        assert_eq!(graph.nodes[0].id, format!("agent-tool:{digest}"));
        assert!(is_bound_agent_tool_graph(&graph));
    }

    #[test]
    fn bound_437_foreign_parent_stored_hash_and_body_kind_reject_at_exact_layer() {
        let workspace = tempfile::tempdir().unwrap();
        let graph = graph_for(real_request(workspace.path()));
        assert!(is_bound_agent_tool_graph(&graph));
        let mut foreign = graph.clone();
        foreign.parent_execution.as_mut().unwrap().execution_id = "foreign-parent".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("original parent mismatch"));
        let mut foreign = graph.clone();
        foreign.id = "agent-tool-batch:foreign".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.nodes[0].id = "agent-tool:foreign".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut body = crate::step_snapshot::decode_persisted_bound_agent_tool_value(
            &graph.nodes[0].payload_ref,
        )
        .unwrap();
        body["request"]["input"] = serde_json::json!("{\"path\":\"different.txt\"}");
        let mut foreign = graph.clone();
        foreign.nodes[0].payload_ref =
            crate::step_snapshot::encode_persisted_bound_agent_tool(&body).unwrap();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        body["kind"] = serde_json::json!("turn_tool_batch");
        let payload = crate::step_snapshot::encode_persisted_bound_agent_tool(&body).unwrap();
        assert!(decode_bound_agent_tool(&payload)
            .unwrap_err()
            .to_string()
            .contains("typed identity invalid"));
        let mut renamed = graph.clone();
        renamed.objective = "Ordinary root: do not infer authority from this title".into();
        assert!(is_bound_agent_tool_graph(&renamed));
    }

    #[test]
    fn bound_437_empty_session_oversize_raw_malformed_and_cross_kind_reject() {
        let workspace = tempfile::tempdir().unwrap();
        let request = real_request(workspace.path());
        let packed = encode_bound_agent_tool("agent-437", "run-437", request.clone()).unwrap();
        assert!(decode_bound_agent_tool(&packed).is_ok());
        let body = crate::step_snapshot::decode_persisted_bound_agent_tool_value(&packed).unwrap();
        assert!(decode_bound_agent_tool(&body.to_string()).is_err());
        assert!(crate::step_snapshot::encode_persisted_tool_batch(&body).is_err());
        let mut top_only = body.clone();
        top_only["session_id"] = serde_json::json!("session-437");
        top_only["request"]
            .as_object_mut()
            .unwrap()
            .remove("session_id");
        assert!(crate::step_snapshot::encode_persisted_bound_agent_tool(&top_only).is_err());
        let normal = crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
            "session_id":"session-437","calls":[],"step_snapshots":{}
        }))
        .unwrap();
        assert!(crate::step_snapshot::decode_persisted_tool_batch_value(&normal).is_ok());
        assert!(decode_bound_agent_tool(&normal).is_err());
        assert!(crate::step_snapshot::decode_persisted_tool_batch_value(&packed).is_err());
        for session in [None, Some(""), Some("   ")] {
            let mut empty = request.clone();
            empty.session_id = session.map(str::to_string);
            assert!(encode_bound_agent_tool("agent-437", "run-437", empty)
                .unwrap_err()
                .to_string()
                .contains("typed identity invalid"));
            let mut empty_body = body.clone();
            empty_body["request"]["session_id"] = serde_json::to_value(session).unwrap();
            let forged = unchecked_test_pack(&empty_body, "session-437");
            assert!(decode_bound_agent_tool(&forged)
                .unwrap_err()
                .to_string()
                .contains("Session conflict"));
        }
        let original: serde_json::Value = serde_json::from_str(&packed).unwrap();
        for (field, value) in [
            ("session_id", serde_json::json!("foreign")),
            ("sha256", serde_json::json!("sha256:foreign")),
            ("decoded_bytes", serde_json::json!(usize::MAX)),
            ("body", serde_json::json!("!!!!")),
            ("kind", serde_json::json!("cowd_tool_batch_zlib_v1")),
        ] {
            let mut forged = original.clone();
            forged[field] = value;
            assert!(
                decode_bound_agent_tool(&forged.to_string()).is_err(),
                "{field}"
            );
        }
        let duplicate = packed.replacen("{", "{\"kind\":\"duplicate\",", 1);
        assert!(decode_bound_agent_tool(&duplicate).is_err());
        let mut oversized = request;
        oversized.input = "x".repeat(1024 * 1024);
        assert!(encode_bound_agent_tool("agent-437", "run-437", oversized)
            .unwrap_err()
            .to_string()
            .contains("decoded budget exceeded"));
        assert!(decode_bound_agent_tool(&" ".repeat(1024 * 1024 + 1))
            .unwrap_err()
            .to_string()
            .contains("encoded budget exceeded"));
    }

    #[tokio::test]
    async fn bound_437_real_resolver_requires_codec_and_actual_ticket_payload_hash() {
        let workspace = tempfile::tempdir().unwrap();
        let graph = graph_for(real_request(workspace.path()));
        let services = RuntimeServices::in_memory().unwrap();
        let resolver = AgentToolBatchResolver {
            services: Arc::downgrade(&services),
        };
        let node = &graph.nodes[0];
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: "tool_batch".into(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        assert!(resolver.resolve(&ticket).is_some());
        for mutation in ["graph", "node", "raw", "normal", "session"] {
            let mut invalid = ticket.clone();
            match mutation {
                "graph" => invalid.graph_id = "agent-tool-batch:foreign".into(),
                "node" => invalid.node_id = "agent-tool:foreign".into(),
                "raw" => {
                    invalid.payload_ref =
                        crate::step_snapshot::decode_persisted_bound_agent_tool_value(
                            &ticket.payload_ref,
                        )
                        .unwrap()
                        .to_string()
                }
                "normal" => {
                    invalid.payload_ref =
                        crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
                            "session_id":"session-437","calls":[],"step_snapshots":{}
                        }))
                        .unwrap()
                }
                _ => {
                    let mut envelope: serde_json::Value =
                        serde_json::from_str(&ticket.payload_ref).unwrap();
                    envelope["session_id"] = serde_json::json!("foreign");
                    invalid.payload_ref = envelope.to_string();
                }
            }
            assert!(resolver.resolve(&invalid).is_none(), "{mutation}");
        }
        services.shutdown_execution().await;
    }

    #[test]
    fn r1_450_bound_pure_builder_preserves_raw_failure_and_never_authors_goal_or_replan() {
        let workspace = tempfile::tempdir().unwrap();
        let request = real_request(workspace.path());
        for status in [
            crate::RuntimeToolExecutionStatus::Executed,
            crate::RuntimeToolExecutionStatus::Failed,
        ] {
            let raw = RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: status.clone(),
                category: request.category,
                output: Some("original UTF8 body".repeat(1024)),
                error: (status == crate::RuntimeToolExecutionStatus::Failed)
                    .then(|| "original rejection".into()),
                evidence_ref: "original-effect-reference".into(),
                observed_evidence: Vec::new(),
            };
            let delivered = agent_tool_outcome_at(&raw, 123456).unwrap();
            let decoded: RuntimeToolExecutionOutcome = serde_json::from_str(
                delivered
                    .result
                    .result_ref
                    .as_ref()
                    .unwrap()
                    .strip_prefix("agent-tool-outcome:")
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(decoded, raw);
            assert_eq!(delivered.result.finished_at_ms, 123456);
            assert_eq!(delivered.result.status, ExecutionNodeStatus::Completed);
            assert!(delivered.domain_events.is_empty());
            assert!(delivered.replan.is_none());
            assert!(delivered.delivery_envelope.is_none());
            assert!(delivered.terminal_presentation.is_none());
        }
    }

    #[cfg(test)]
    mod nodispatch_520_tests {
        use super::*;

        fn observed_request() -> (
            tempfile::TempDir,
            RuntimeToolExecutionRequest,
            NodeExecutionTicket,
            Arc<NoDispatchObservation>,
        ) {
            let workspace = tempfile::tempdir().unwrap();
            let request = real_request(workspace.path());
            let graph = graph_for(request.clone());
            let node = &graph.nodes[0];
            let ticket = NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                service_class: graph.service_class,
                attempt: 1,
                idempotency_key: node.idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            let observation = NoDispatchObservation::new(&ticket).unwrap();
            (workspace, request, ticket, observation)
        }

        fn failed_outcome() -> NodeExecutionOutcome {
            NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Failed,
                result_ref: None,
                wait_class: None,
                summary: Some("admission refused before dispatch".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            })
        }

        #[test]
        fn own834_canonical_rejection_failure_cannot_become_local_refusal() {
            for reason in [
                "step_binding_stale",
                "stale Step rejection receipt commit failed",
                "stale Step rejection has no original Process Step",
            ] {
                let (_workspace, request, ticket, observation) = observed_request();
                assert!(observation.enter());
                observation.refuse(&request.tool_use_id, "step_binding_stale".into());
                observation.uncertain(&request);
                let returned = finish_observed_outcome(
                    &ticket,
                    Some(&observation),
                    Err(NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: reason.into(),
                    }),
                );
                assert!(returned.is_err(), "{reason}");
                assert!(observation.refusal(&request).is_none());
                assert!(observation.witness().is_none());
            }
        }

        #[test]
        fn nodispatch_520_close_before_first_poll_seals_original_call() {
            let (_workspace, request, ticket, observation) = observed_request();
            observation.close();
            let witness = observation
                .witness()
                .expect("unpolled owner must be sealed");
            assert!(witness.fact().never_polled);
            assert_eq!(witness.fact().calls.len(), 1);
            assert_eq!(witness.fact().calls[0].call_id, request.tool_use_id);
            assert_eq!(
                witness.fact().calls[0].request_sha256,
                Some(no_dispatch_digest(&request).unwrap())
            );
            assert_eq!(
                witness.fact().calls[0].reason,
                "cancelled_before_first_poll"
            );
            assert!(witness.fact().result_sha256.is_none());
            witness.authenticate(&ticket, None).unwrap();
            assert!(!observation.enter());
        }

        #[test]
        fn nodispatch_520_enter_then_close_cannot_claim_never_polled() {
            let (_workspace, _request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.close();
            observation.close();
            assert!(
                observation.witness().is_none(),
                "entered owner has no proof of non-dispatch"
            );
            assert!(!observation.enter());
        }

        #[test]
        fn nodispatch_520_uncertain_absorbs_prior_and_later_refusals() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "admission closed".into());
            assert!(observation.refusal(&request).is_some());
            let mut refused = failed_outcome();
            observation.finish(&mut refused).unwrap();
            refused
                .no_dispatch
                .as_ref()
                .unwrap()
                .authenticate(&ticket, Some(&refused.result))
                .unwrap();
            observation.uncertain(&request);
            assert!(observation.witness().is_none());
            observation.refuse(&request.tool_use_id, "later refusal".into());
            assert!(observation.refusal(&request).is_none());
            let mut after_uncertainty = failed_outcome();
            observation.finish(&mut after_uncertainty).unwrap();
            assert!(after_uncertainty.no_dispatch.is_none());
            observation.close();
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_520_refusal_requires_original_full_request_digest() {
            let (_workspace, request, _ticket, observation) = observed_request();
            observation.refuse(&request.tool_use_id, "binding admission closed".into());
            let refusal = observation
                .refusal(&request)
                .expect("exact request is refused");
            assert_eq!(refusal.status, crate::RuntimeToolExecutionStatus::Failed);
            assert!(refusal.output.is_none());
            assert_eq!(refusal.error.as_deref(), Some("binding admission closed"));
            for field in ["input", "tool", "effect", "attempt", "policy", "session"] {
                let mut altered = request.clone();
                match field {
                    "input" => altered.input.push(' '),
                    "tool" => altered.tool_name = "write_file".into(),
                    "effect" => altered.idempotency_key.push_str("-copied"),
                    "attempt" => altered.parent_execution_attempt = Some(4),
                    "policy" => altered.policy_revision += 1,
                    "session" => altered.session_id = Some("foreign-session".into()),
                    _ => unreachable!(),
                }
                assert!(observation.refusal(&altered).is_none(), "{field}");
            }
            assert!(observation.refusal(&request).is_some());
        }

        fn original_mixed_ticket622() -> NodeExecutionTicket {
            NodeExecutionTicket {
                graph_id: "legacy-mixed-622".into(),
                node_id: "tools-mixed-622".into(),
                executor_kind: "tool_batch".into(),
                service_class: Default::default(),
                attempt: 1,
                idempotency_key: "original-mixed-622".into(),
                payload_ref: crate::step_snapshot::encode_persisted_tool_batch(
                    &serde_json::json!({
                        "session_id":"session-mixed-622", "step_snapshots":{},
                        "calls":[
                            {"id":"refused-622","name":"read_file","input":"{}","depends_on":[]},
                            {"id":"dispatched-622","name":"read_file","input":"{}","depends_on":[]}
                        ]
                    }),
                )
                .unwrap(),
            }
        }

        #[tokio::test]
        async fn nodispatch_622_legacy_mixed_duration_preserves_subset_and_dispatched_leaf() {
            use crate::execution_core::NodeExecutor;

            let ticket = original_mixed_ticket622();
            let executor = crate::execution_core::graph::ScopedNodeExecutor::new("tool_batch");
            let observation = executor.observe_binding(&ticket).unwrap();
            assert!(observation.enter());
            let request = RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: "dispatched-622".into(),
                    tool_name: "read_file".into(),
                    input: "{}".into(),
                    depends_on: Vec::new(),
                },
            );
            observation.uncertain(&request);
            let workspace = tempfile::tempdir().unwrap();
            let path = workspace.path().join("actual-leaf.txt");
            std::fs::write(&path, "actual original mixed read").unwrap();
            let leaf = executor
                .execute_physical_tool(
                    &ticket,
                    "dispatched-622",
                    Box::pin(async move {
                        let output = std::fs::read_to_string(&path).unwrap();
                        Ok(RuntimeToolExecutionOutcome {
                            tool_use_id: "dispatched-622".into(),
                            tool_name: "read_file".into(),
                            status: crate::RuntimeToolExecutionStatus::Executed,
                            category: crate::ToolSafetyCategory::ReadOnly,
                            output: Some(output),
                            error: None,
                            evidence_ref: String::new(),
                            observed_evidence: Vec::new(),
                        })
                    }),
                )
                .await
                .unwrap();
            assert_eq!(leaf.status, crate::RuntimeToolExecutionStatus::Executed);
            assert_eq!(leaf.output.as_deref(), Some("actual original mixed read"));
            observation.refuse("refused-622", "original scope refusal".into());
            observation.refuse("dispatched-622", "must not hide the actual dispatch".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let sealed = outcome.clone();
            let original = outcome.no_dispatch.as_ref().unwrap().clone();
            assert_eq!(original.fact().calls.len(), 1);
            assert_eq!(original.fact().calls[0].call_id, "refused-622");
            observation
                .finalize_duration(&ticket, &mut outcome, 37)
                .unwrap();
            let finalized = outcome.no_dispatch.as_ref().unwrap();
            finalized
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert!(original
                .authenticate(&ticket, Some(&outcome.result))
                .is_err());
            let mut expected = sealed.result;
            expected.usage.duration_ms = 37;
            assert_eq!(outcome.result, expected);
            let mut expected_fact = original.fact().clone();
            expected_fact.result_sha256 = finalized.fact().result_sha256.clone();
            assert_eq!(*finalized.fact(), expected_fact);
            assert_eq!(observation.witness().as_ref(), Some(finalized));
            assert_eq!(
                observation.state.lock().unwrap().uncertain,
                vec!["dispatched-622".to_string()]
            );
            observation.refuse("dispatched-622", "late dispatch refusal".into());
            assert_eq!(observation.witness().as_ref(), Some(finalized));
            executor.cancel(&ticket).await.unwrap();
            assert_eq!(observation.witness().as_ref(), Some(finalized));
            observation.uncertain(&request);
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 41)
                .is_err());
            assert_eq!(outcome, before);
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_622_duration_rejects_overlapping_uncertainty_with_retained_seal() {
            let ticket = original_mixed_ticket622();
            let observation = NoDispatchObservation::new(&ticket).unwrap();
            assert!(observation.enter());
            observation.refuse("refused-622", "original scope refusal".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = observation.witness();
            observation
                .state
                .lock()
                .unwrap()
                .uncertain
                .push("refused-622".into());
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 43)
                .is_err());
            assert_eq!(outcome, before);
            assert_eq!(observation.witness(), witness);
            assert_eq!(
                observation.state.lock().unwrap().uncertain,
                vec!["refused-622".to_string()]
            );
        }

        #[test]
        fn nodispatch_617_duration_finalization_preserves_exact_fact_and_rejects_old_seal() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "original denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let original_result = outcome.result.clone();
            let original = outcome.no_dispatch.clone().unwrap();
            observation
                .finalize_duration(&ticket, &mut outcome, 17)
                .unwrap();
            let finalized = outcome.no_dispatch.clone().unwrap();
            finalized
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert!(original
                .authenticate(&ticket, Some(&outcome.result))
                .is_err());
            assert!(finalized
                .authenticate(&ticket, Some(&original_result))
                .is_err());
            let mut expected = original_result;
            expected.usage.duration_ms = 17;
            assert_eq!(outcome.result, expected);
            let mut expected_fact = original.fact().clone();
            expected_fact.result_sha256 = finalized.fact().result_sha256.clone();
            assert_eq!(*finalized.fact(), expected_fact);
            assert_eq!(observation.witness(), Some(finalized.clone()));
            observation
                .finalize_duration(&ticket, &mut outcome, 1)
                .unwrap();
            assert_eq!(outcome.result.usage.duration_ms, 17);
            assert_eq!(outcome.no_dispatch, Some(finalized));
            observation
                .finalize_duration(&ticket, &mut outcome, 23)
                .unwrap();
            outcome
                .no_dispatch
                .as_ref()
                .unwrap()
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert_eq!(outcome.result.usage.duration_ms, 23);
        }

        #[test]
        fn nodispatch_617_duration_rejects_tampered_result_without_resealing() {
            for field in [
                "summary", "status", "result", "usage", "duration", "finished", "failure",
            ] {
                let (_workspace, request, ticket, observation) = observed_request();
                assert!(observation.enter());
                observation.refuse(&request.tool_use_id, "original denied".into());
                let mut outcome = failed_outcome();
                observation.finish(&mut outcome).unwrap();
                let witness = observation.witness();
                match field {
                    "summary" => outcome.result.summary = Some("foreign".into()),
                    "status" => outcome.result.status = ExecutionNodeStatus::Completed,
                    "result" => outcome.result.result_ref = Some("foreign".into()),
                    "usage" => outcome.result.usage.tool_calls += 1,
                    "duration" => outcome.result.usage.duration_ms += 1,
                    "finished" => outcome.result.finished_at_ms += 1,
                    _ => {
                        outcome.result.failure =
                            Some(harness_contract::execution_graph::ExecutionFailure {
                                kind: "foreign".into(),
                                message: "foreign".into(),
                                retryable: false,
                                evidence_refs: Vec::new(),
                            })
                    }
                }
                let before = outcome.clone();
                assert!(
                    observation
                        .finalize_duration(&ticket, &mut outcome, 19)
                        .is_err(),
                    "{field}"
                );
                assert_eq!(outcome, before);
                assert_eq!(observation.witness(), witness);
            }
        }

        #[test]
        fn nodispatch_617_duration_rejects_foreign_ticket_witness_and_uncertain_reset() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "original denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let original = outcome.clone();
            let witness = observation.witness();
            let mut foreign_ticket = ticket.clone();
            foreign_ticket.attempt += 1;
            assert!(observation
                .finalize_duration(&foreign_ticket, &mut outcome, 29)
                .is_err());
            assert_eq!(outcome, original);
            assert_eq!(observation.witness(), witness);
            let other = NoDispatchObservation::new(&ticket).unwrap();
            assert!(other.finalize_duration(&ticket, &mut outcome, 29).is_err());
            assert!(other.witness().is_none());
            assert_eq!(outcome, original);
            assert!(other.enter());
            other.refuse(
                &request.tool_use_id,
                "different original classification".into(),
            );
            let mut foreign = failed_outcome();
            other.finish(&mut foreign).unwrap();
            foreign
                .no_dispatch
                .as_ref()
                .unwrap()
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            outcome.no_dispatch = foreign.no_dispatch;
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 29)
                .is_err());
            assert_eq!(outcome, before);
            assert_eq!(observation.witness(), witness);
            outcome = original;
            observation.uncertain(&request);
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 29)
                .is_err());
            assert_eq!(outcome, before);
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_617_duration_never_mints_or_drops_a_witness() {
            let (_workspace, request, ticket, observation) = observed_request();
            let mut outcome = failed_outcome();
            observation
                .finalize_duration(&ticket, &mut outcome, 0)
                .unwrap();
            assert_eq!(outcome.result.usage.duration_ms, 1);
            assert!(outcome.no_dispatch.is_none());
            assert!(observation.witness().is_none());
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "original denied".into());
            observation.finish(&mut outcome).unwrap();
            let witness = observation.witness();
            outcome.no_dispatch = None;
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 31)
                .is_err());
            assert_eq!(outcome, before);
            assert_eq!(observation.witness(), witness);
        }

        #[test]
        fn nodispatch_520_cloned_witness_rejects_mutated_ticket_and_result() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "admission denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = outcome.no_dispatch.as_ref().unwrap().clone();
            witness
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            for field in [
                "graph",
                "node",
                "attempt",
                "payload",
                "idempotency",
                "executor",
            ] {
                let mut foreign = ticket.clone();
                match field {
                    "graph" => foreign.graph_id.push_str("-foreign"),
                    "node" => foreign.node_id.push_str("-foreign"),
                    "attempt" => foreign.attempt += 1,
                    "payload" => foreign.payload_ref.push(' '),
                    "idempotency" => foreign.idempotency_key.push_str("-foreign"),
                    "executor" => foreign.executor_kind = "inline_model".into(),
                    _ => unreachable!(),
                }
                assert!(
                    witness
                        .authenticate(&foreign, Some(&outcome.result))
                        .is_err(),
                    "{field}"
                );
            }
            let mut copied_result = outcome.result.clone();
            copied_result.summary = Some("copied result".into());
            assert!(witness.authenticate(&ticket, Some(&copied_result)).is_err());
            let mut copied_result = outcome.result.clone();
            copied_result.finished_at_ms += 1;
            assert!(witness.authenticate(&ticket, Some(&copied_result)).is_err());
            witness
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
        }

        #[test]
        fn nodispatch_520_finish_without_refusal_creates_no_closure() {
            let (_workspace, _request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            assert!(outcome.no_dispatch.is_none());
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_520_finish_refused_call_seals_exact_failed_result() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "resource admission denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = outcome.no_dispatch.as_ref().unwrap();
            assert!(!witness.fact().never_polled);
            assert_eq!(witness.fact().calls.len(), 1);
            assert_eq!(witness.fact().calls[0].call_id, request.tool_use_id);
            assert_eq!(witness.fact().calls[0].reason, "resource admission denied");
            assert_eq!(
                witness.fact().result_sha256,
                Some(no_dispatch_digest(&outcome.result).unwrap())
            );
            witness
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert_eq!(observation.witness().as_ref(), Some(witness));
        }

        #[test]
        fn nodispatch_520_unknown_or_empty_refusal_cannot_close_original_call() {
            let (_workspace, request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse("foreign-call", "admission denied".into());
            observation.refuse(&request.tool_use_id, String::new());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            assert!(observation.refusal(&request).is_none());
            assert!(outcome.no_dispatch.is_none());
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_520_never_polled_witness_rejects_any_node_result() {
            let (_workspace, _request, ticket, observation) = observed_request();
            observation.close();
            let witness = observation.witness().unwrap();
            witness.authenticate(&ticket, None).unwrap();
            assert!(witness
                .authenticate(&ticket, Some(&failed_outcome().result))
                .is_err());
        }

        #[test]
        fn nodispatch_520_serde_cannot_inject_or_roundtrip_private_witness() {
            let (_workspace, request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "admission denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = outcome.no_dispatch.as_ref().unwrap();
            let encoded = serde_json::to_value(&outcome).unwrap();
            assert!(encoded.get("no_dispatch").is_none());
            let roundtrip: NodeExecutionOutcome = serde_json::from_value(encoded.clone()).unwrap();
            assert!(roundtrip.no_dispatch.is_none());
            assert_eq!(roundtrip.result, outcome.result);
            let fact = serde_json::to_value(witness.fact()).unwrap();
            for injection in [fact.clone(), serde_json::json!({"fact": fact})] {
                let mut forged = encoded.clone();
                forged["no_dispatch"] = injection;
                match serde_json::from_value::<NodeExecutionOutcome>(forged) {
                    Err(_) => {}
                    Ok(decoded) => {
                        assert!(
                            decoded.no_dispatch.is_none(),
                            "serialized facts must not become owner-authored witnesses"
                        );
                        assert_eq!(decoded.result, outcome.result);
                    }
                }
            }
        }
    }
}
