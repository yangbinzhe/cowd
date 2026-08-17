use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{
    apply_node_transition, validate_execution_graph, ExecutionEdge, ExecutionGraph,
    ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionTransitionError,
};
use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind, ToolIdempotency};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::execution_core::hot_state::{
    DerivedMaterialization, HotExecutionGraphRegistry, RuntimeHotStatePlane,
};
use crate::runtime_event_store::{
    AppendTransactionReceipt, AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError,
    RuntimeTransactionEventInput, SessionTerminalInput,
};

use super::events::{ExecutionGraphDelta, ExecutionGraphEvent, ExecutionNodeBinding};

const MAX_TOOL_EFFECT_RECEIPT_CHARS: usize = 16 * 1024;

#[derive(Debug, Error)]
pub enum ExecutionCommitError {
    #[error(transparent)]
    EventStore(#[from] RuntimeEventStoreError),
    #[error(transparent)]
    Transition(#[from] ExecutionTransitionError),
    #[error("execution graph commit serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error(
        "execution graph `{graph_id}` revision mismatch: expected {expected}, actual {actual}"
    )]
    StaleRevision {
        graph_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("execution graph command is not valid in the current state: {0}")]
    InvalidCommand(String),
    #[error("domain event attempted to mutate canonical graph stream `{0}`")]
    GraphStreamCollision(String),
    #[error("domain event for stream `{0}` has no stable idempotency key")]
    MissingDomainIdempotency(String),
    #[error("graph executor cannot emit protected domain scope `{0}`")]
    ProtectedDomainScope(String),
    #[error("execution commit blocking task failed: {0}")]
    BlockingTask(String),
    #[error("execution graph replan is invalid: {0}")]
    InvalidReplan(String),
}

pub struct ExecutionCommitReceipt {
    pub graph: ExecutionGraph,
    pub transaction: AppendTransactionReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionEffectState {
    Fresh,
    Completed(super::registry::NodeExecutionOutcome),
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolEffectState {
    NotRequired,
    Fresh,
    Completed(crate::RuntimeToolExecutionOutcome),
    Uncertain,
}

#[derive(Clone)]
pub struct ExecutionCommitService {
    event_store: Arc<RuntimeEventStore>,
    hot_state: Arc<RuntimeHotStatePlane>,
    hot_graphs: Arc<HotExecutionGraphRegistry>,
}

impl ExecutionCommitService {
    pub fn commit_readonly_tool_receipts(
        &self,
        receipts: &[(
            crate::RuntimeToolExecutionRequest,
            crate::RuntimeToolExecutionOutcome,
        )],
    ) -> Result<(), ExecutionCommitError> {
        if receipts.is_empty() {
            return Ok(());
        }
        let mut expected_streams = Vec::with_capacity(receipts.len());
        let mut events = Vec::with_capacity(receipts.len());
        let mut receipt_keys = Vec::with_capacity(receipts.len());
        for (request, outcome) in receipts {
            let stream_id = format!("execution-effect:{}", request.idempotency_key);
            let receipt_key = format!("{}:read-receipt", request.idempotency_key);
            if let Some(existing) = self
                .event_store
                .event_by_idempotency_key(&stream_id, &receipt_key)?
            {
                validate_readonly_tool_receipt(request, &existing.payload)?;
                continue;
            }
            expected_streams.push(ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: self.event_store.stream_revision(&stream_id)?,
            });
            events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.read.receipt".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("governed_tool".to_string()),
                    refs: tool_effect_refs(request),
                    payload: json!({
                        "idempotency_key": request.idempotency_key,
                        "tool_use_id": request.tool_use_id,
                        "tool_name": request.tool_name,
                        "input_sha256": format!(
                            "sha256:{:x}",
                            Sha256::digest(request.input.as_bytes())
                        ),
                        "outcome_truncated": tool_effect_outcome_requires_truncation(outcome),
                        "outcome": bounded_tool_effect_outcome(outcome),
                    }),
                },
                idempotency_key: Some(receipt_key.clone()),
                schema_version: 1,
            });
            receipt_keys.push(receipt_key);
        }
        if events.is_empty() {
            return Ok(());
        }
        receipt_keys.sort();
        let digest = format!("{:x}", Sha256::digest(receipt_keys.join("\n").as_bytes()));
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("readonly-tool-wave:{digest}"),
                expected_streams,
                events,
            })?;
        Ok(())
    }

    pub fn begin_tool_effect(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        if effect.effect_kind == ToolEffectKind::Read {
            let stream_id = format!("execution-effect:{}", request.idempotency_key);
            let receipt_key = format!("{}:read-receipt", request.idempotency_key);
            let Some(receipt) = self
                .event_store
                .event_by_idempotency_key(&stream_id, &receipt_key)?
            else {
                return Ok(ToolEffectState::NotRequired);
            };
            validate_readonly_tool_receipt(request, &receipt.payload)?;
            if receipt.payload["outcome_truncated"]
                .as_bool()
                .unwrap_or(false)
            {
                // Re-running a read is safe and preserves full information.
                // A bounded durable receipt is evidence, not a lossy cache.
                return Ok(ToolEffectState::NotRequired);
            }
            return serde_json::from_value(receipt.payload["outcome"].clone())
                .map(ToolEffectState::Completed)
                .map_err(ExecutionCommitError::Serialization);
        }
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        if let Some(receipt) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:receipt", request.idempotency_key))?
        {
            validate_mutation_tool_fingerprint(request, effect, &receipt.payload, "receipt")?;
            return serde_json::from_value(receipt.payload["outcome"].clone())
                .map(ToolEffectState::Completed)
                .map_err(ExecutionCommitError::Serialization);
        }
        if let Some(intent) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:intent", request.idempotency_key))?
        {
            validate_mutation_tool_fingerprint(request, effect, &intent.payload, "intent")?;
            return Ok(match effect.idempotency {
                ToolIdempotency::Idempotent | ToolIdempotency::IdempotentWithKey => {
                    ToolEffectState::Fresh
                }
                ToolIdempotency::NonIdempotent | ToolIdempotency::Unknown => {
                    ToolEffectState::Uncertain
                }
            });
        }
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!("{}:intent", request.idempotency_key),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.intent".to_string(),
                    status: Some("inflight".to_string()),
                    actor: Some("governed_tool".to_string()),
                    refs: tool_effect_refs(request),
                    payload: json!({
                        "idempotency_key": request.idempotency_key,
                        "tool_use_id": request.tool_use_id,
                        "tool_name": request.tool_name,
                        "input_sha256": format!("sha256:{:x}", Sha256::digest(request.input.as_bytes())),
                        "effect": effect,
                    }),
                },
                idempotency_key: Some(format!("{}:intent", request.idempotency_key)),
                schema_version: 1,
            }],
        )?;
        Ok(ToolEffectState::Fresh)
    }

    pub fn commit_tool_effect(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        outcome: &crate::RuntimeToolExecutionOutcome,
    ) -> Result<(), ExecutionCommitError> {
        if effect.effect_kind == ToolEffectKind::Read {
            return Ok(());
        }
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:intent", request.idempotency_key))?
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(format!(
                    "mutation receipt has no durable intent for idempotency key `{}`",
                    request.idempotency_key
                ))
            })?;
        validate_mutation_tool_fingerprint(request, effect, &intent.payload, "intent")?;
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!("{}:receipt", request.idempotency_key),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.receipt".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("governed_tool".to_string()),
                    refs: tool_effect_refs(request),
                    payload: json!({
                        "idempotency_key": request.idempotency_key,
                        "tool_name": request.tool_name,
                        "input_sha256": format!(
                            "sha256:{:x}",
                            Sha256::digest(request.input.as_bytes())
                        ),
                        "descriptor_hash": effect.descriptor_hash,
                        "outcome": bounded_tool_effect_outcome(outcome),
                    }),
                },
                idempotency_key: Some(format!("{}:receipt", request.idempotency_key)),
                schema_version: 1,
            }],
        )?;
        Ok(())
    }

    pub fn begin_execution_effect(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<ExecutionEffectState, ExecutionCommitError> {
        let stream_id = format!("execution-effect:{}", ticket.idempotency_key);
        if let Some(receipt) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:receipt", ticket.idempotency_key))?
        {
            return serde_json::from_value(receipt.payload)
                .map(ExecutionEffectState::Completed)
                .map_err(ExecutionCommitError::Serialization);
        }
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:intent", ticket.idempotency_key))?
            .is_some()
        {
            return Ok(ExecutionEffectState::Uncertain);
        }
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!("{}:intent", ticket.idempotency_key),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.intent".to_string(),
                    status: Some("inflight".to_string()),
                    actor: Some(ticket.executor_kind.clone()),
                    refs: vec![RuntimeEventRef {
                        kind: "execution_node".to_string(),
                        id: ticket.node_id.clone(),
                    }],
                    payload: serde_json::to_value(ticket)?,
                },
                idempotency_key: Some(format!("{}:intent", ticket.idempotency_key)),
                schema_version: 1,
            }],
        )?;
        Ok(ExecutionEffectState::Fresh)
    }

    pub fn commit_execution_effect(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
        outcome: &super::registry::NodeExecutionOutcome,
    ) -> Result<(), ExecutionCommitError> {
        let stream_id = format!("execution-effect:{}", ticket.idempotency_key);
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!("{}:receipt", ticket.idempotency_key),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.receipt".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some(ticket.executor_kind.clone()),
                    refs: vec![RuntimeEventRef {
                        kind: "execution_node".to_string(),
                        id: ticket.node_id.clone(),
                    }],
                    payload: serde_json::to_value(outcome)?,
                },
                idempotency_key: Some(format!("{}:receipt", ticket.idempotency_key)),
                schema_version: 1,
            }],
        )?;
        Ok(())
    }

    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self::with_hot_state(event_store, Arc::new(RuntimeHotStatePlane::default()))
    }

    #[must_use]
    pub fn with_hot_state(
        event_store: Arc<RuntimeEventStore>,
        hot_state: Arc<RuntimeHotStatePlane>,
    ) -> Self {
        Self {
            event_store,
            hot_graphs: Arc::clone(hot_state.graphs()),
            hot_state,
        }
    }

    pub fn register_graph(
        &self,
        mut graph: ExecutionGraph,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        graph.revision = 1;
        graph.node_statuses.clear();
        graph.node_results.clear();
        graph.recovery_cursor = Default::default();
        for node in &graph.nodes {
            graph
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        let transaction_id = format!("{}:planned", graph.id);
        let lineage_event =
            graph
                .parent_execution
                .as_ref()
                .map(|parent| RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: execution_lineage_stream_id(&parent.execution_id),
                        scope: RuntimeEventScope::Relation,
                        kind: "execution.lineage.child_registered.v1".to_string(),
                        status: Some("registered".to_string()),
                        actor: Some("execution_commit_service".to_string()),
                        refs: vec![
                            RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: parent.execution_id.clone(),
                            },
                            RuntimeEventRef {
                                kind: "execution_node".to_string(),
                                id: parent.node_id.clone(),
                            },
                            RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: graph.id.clone(),
                            },
                        ],
                        payload: json!({
                            "parent_execution_id": parent.execution_id,
                            "parent_node_id": parent.node_id,
                            "child_execution_id": graph.id,
                            "child_objective": graph.objective,
                        }),
                    },
                    idempotency_key: Some(format!("{}:child:{}", parent.execution_id, graph.id)),
                    schema_version: 1,
                });
        // Several child graphs may be compiled concurrently for the same
        // parent tool batch. Their graph streams are independent, but their
        // durable lineage registrations share one parent stream. Retry only
        // that optimistic-concurrency collision: the transaction is atomic
        // and uses stable graph/child idempotency keys, so no child can be
        // double-registered. A collision on the graph's own stream remains a
        // real duplicate-graph error and is returned unchanged.
        let lineage_stream = graph
            .parent_execution
            .as_ref()
            .map(|parent| execution_lineage_stream_id(&parent.execution_id));
        let domain_events = lineage_event.into_iter().collect::<Vec<_>>();
        let mut last_lineage_conflict = None;
        for _ in 0..8 {
            match self.append_graph_event(
                &graph,
                0,
                transaction_id.clone(),
                ExecutionGraphEvent::Planned {
                    graph: graph.clone(),
                },
                domain_events.clone(),
            ) {
                Ok(receipt) => return Ok(receipt),
                Err(error)
                    if is_lineage_registration_conflict(&error, lineage_stream.as_deref()) =>
                {
                    last_lineage_conflict = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_lineage_conflict.unwrap_or_else(|| {
            ExecutionCommitError::InvalidReplan(
                "lineage registration retry exhausted without a conflict receipt".to_string(),
            )
        }))
    }

    pub async fn register_graph_async(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.register_graph(graph))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn transition_node(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        domain_events: Vec<RuntimeTransactionEventInput>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        validate_executor_domain_events(&domain_events)?;
        let from = *graph
            .node_statuses
            .get(node_id)
            .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.to_string()))?;
        let mut next = apply_node_transition(graph, graph.revision, node_id, to, result.clone())?;
        let node_attempt = next
            .recovery_cursor
            .node_attempts
            .entry(node_id.to_string())
            .or_default();
        if to == ExecutionNodeStatus::Running {
            *node_attempt = node_attempt.saturating_add(1);
        }
        let transaction_id = format!("{}:{}:{}:{}", graph.id, node_id, next.revision, to as u8);
        let graph_event = ExecutionGraphEvent::NodeTransitioned {
            node_id: node_id.to_string(),
            from,
            to,
            result: result.clone(),
            binding: None,
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        let node_event = node_transition_event(&next, node_id, from, to, result)?;
        let mut events = vec![node_event];
        events.extend(domain_events);
        if let Some(working_state) = crate::team_working_state::terminal_working_state_event(
            &next,
            node_id,
            to,
            next.node_results.get(node_id),
        ) {
            events.push(working_state);
        }
        if let Some(candidate) = crate::knowledge_candidate_projector::team_terminal_candidate(
            &next,
            node_id,
            to,
            next.node_results.get(node_id),
        ) {
            events.push(
                crate::knowledge_candidate_projector::candidate_proposal_event(candidate)
                    .map_err(ExecutionCommitError::InvalidCommand)?,
            );
        }
        self.append_graph_event(&next, graph.revision, transaction_id, graph_event, events)
    }

    pub fn bind_and_start_node(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        binding: ExecutionNodeBinding,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let from = *graph
            .node_statuses
            .get(node_id)
            .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.to_string()))?;
        let mut next = apply_node_transition(
            graph,
            graph.revision,
            node_id,
            ExecutionNodeStatus::Running,
            None,
        )?;
        *next
            .recovery_cursor
            .node_attempts
            .entry(node_id.to_string())
            .or_default() = binding.attempt;
        let graph_event = ExecutionGraphEvent::NodeTransitioned {
            node_id: node_id.to_string(),
            from,
            to: ExecutionNodeStatus::Running,
            result: None,
            binding: Some(binding),
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        let node_event =
            node_transition_event(&next, node_id, from, ExecutionNodeStatus::Running, None)?;
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:{}:{}:bound", graph.id, node_id, next.revision),
            graph_event,
            vec![node_event],
        )
    }

    pub async fn bind_and_start_node_async(
        &self,
        graph: ExecutionGraph,
        node_id: String,
        binding: ExecutionNodeBinding,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.bind_and_start_node(&graph, &node_id, binding))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub async fn transition_node_async(
        &self,
        graph: ExecutionGraph,
        node_id: String,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        domain_events: Vec<RuntimeTransactionEventInput>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.transition_node(&graph, &node_id, to, result, domain_events)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn transition_node_with_replan(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        result: ExecutionNodeResult,
        domain_events: Vec<RuntimeTransactionEventInput>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        validate_executor_domain_events(&domain_events)?;
        validate_replan(graph, &nodes)?;
        let from = *graph
            .node_statuses
            .get(node_id)
            .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.to_string()))?;
        let to = result.status;
        let mut next =
            apply_node_transition(graph, graph.revision, node_id, to, Some(result.clone()))?;
        let added_node_ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();
        for node in &nodes {
            next.node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        next.nodes.extend(nodes);
        next.edges.extend(edges);
        validate_execution_graph(&next)
            .map_err(|error| ExecutionCommitError::InvalidReplan(error.to_string()))?;
        let mut events = vec![node_transition_event(
            &next,
            node_id,
            from,
            to,
            Some(result.clone()),
        )?];
        events.extend(domain_events);
        if let Some(working_state) = crate::team_working_state::terminal_working_state_event(
            &next,
            node_id,
            to,
            next.node_results.get(node_id),
        ) {
            events.push(working_state);
        }
        if let Some(candidate) = crate::knowledge_candidate_projector::team_terminal_candidate(
            &next,
            node_id,
            to,
            next.node_results.get(node_id),
        ) {
            events.push(
                crate::knowledge_candidate_projector::candidate_proposal_event(candidate)
                    .map_err(ExecutionCommitError::InvalidCommand)?,
            );
        }
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:{}:{}:terminal-replan", graph.id, node_id, next.revision),
            ExecutionGraphEvent::NodeTransitionedAndReplanned {
                node_id: node_id.to_string(),
                from,
                to,
                result,
                reason,
                added_node_ids,
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            events,
        )
    }

    pub async fn transition_node_with_replan_async(
        &self,
        graph: ExecutionGraph,
        node_id: String,
        result: ExecutionNodeResult,
        domain_events: Vec<RuntimeTransactionEventInput>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.transition_node_with_replan(
                &graph,
                &node_id,
                result,
                domain_events,
                nodes,
                edges,
                reason,
            )
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn replan(
        &self,
        graph: &ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        validate_replan(graph, &nodes)?;
        let added_node_ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();
        let mut next = graph.clone();
        for node in &nodes {
            next.node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        next.nodes.extend(nodes);
        next.edges.extend(edges);
        next.revision = next.revision.saturating_add(1);
        validate_execution_graph(&next)
            .map_err(|error| ExecutionCommitError::InvalidReplan(error.to_string()))?;
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:replan:{}", graph.id, next.revision),
            ExecutionGraphEvent::Replanned {
                reason,
                added_node_ids,
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            Vec::new(),
        )
    }

    pub async fn replan_async(
        &self,
        graph: ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.replan(&graph, nodes, edges, reason))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn replan_semantic(
        &self,
        graph: &ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
        mutation_id: String,
        completion: harness_contract::execution_graph::ExecutionCompletionContract,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if mutation_id.trim().is_empty() {
            return Err(ExecutionCommitError::InvalidReplan(
                "semantic mutation id is empty".to_string(),
            ));
        }
        validate_replan(graph, &nodes)?;
        let added_node_ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();
        let mut next = graph.clone();
        for node in &nodes {
            next.node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        next.nodes.extend(nodes);
        next.edges.extend(edges);
        next.revision = next.revision.saturating_add(1);
        let orchestration = next.orchestration.get_or_insert_with(|| {
            harness_contract::execution_graph::ExecutionOrchestrationMetadata {
                mutation_id: String::new(),
                applied_mutation_ids: Vec::new(),
                semantic_revision: 0,
                source_generation: 0,
                completion: Default::default(),
            }
        });
        if orchestration
            .applied_mutation_ids
            .iter()
            .any(|applied| applied == &mutation_id)
        {
            return Err(ExecutionCommitError::InvalidReplan(format!(
                "semantic mutation `{mutation_id}` is already applied"
            )));
        }
        orchestration.mutation_id = mutation_id.clone();
        orchestration.applied_mutation_ids.push(mutation_id.clone());
        orchestration.applied_mutation_ids.sort();
        orchestration.applied_mutation_ids.dedup();
        orchestration.semantic_revision = orchestration.semantic_revision.saturating_add(1);
        orchestration.source_generation = orchestration.source_generation.saturating_add(1);
        if !completion.required_node_ids.is_empty() {
            orchestration
                .completion
                .required_node_ids
                .extend(completion.required_node_ids);
        }
        orchestration
            .completion
            .required_artifact_kinds
            .extend(completion.required_artifact_kinds);
        orchestration.completion.allow_unresolved_conflicts = completion.allow_unresolved_conflicts;
        orchestration.completion.required_node_ids.sort();
        orchestration.completion.required_node_ids.dedup();
        orchestration.completion.required_artifact_kinds.sort();
        orchestration.completion.required_artifact_kinds.dedup();
        validate_execution_graph(&next)
            .map_err(|error| ExecutionCommitError::InvalidReplan(error.to_string()))?;
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:semantic-mutation:{mutation_id}", graph.id),
            ExecutionGraphEvent::Replanned {
                reason,
                added_node_ids,
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            Vec::new(),
        )
    }

    pub async fn replan_semantic_async(
        &self,
        graph: ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
        mutation_id: String,
        completion: harness_contract::execution_graph::ExecutionCompletionContract,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.replan_semantic(&graph, nodes, edges, reason, mutation_id, completion)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    /// Replace an admitted graph topology before any node starts.
    ///
    /// Strategy downgrade uses this boundary when the initially selected Team
    /// cannot start. Replacing the still-planned snapshot keeps the durable
    /// graph ID and Surface subscription stable while ensuring the executed
    /// topology matches the revised strategy compile target.
    pub fn retarget_planned_graph(
        &self,
        graph: &ExecutionGraph,
        mut replacement: ExecutionGraph,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if graph.nodes.is_empty()
            || graph
                .node_statuses
                .values()
                .any(|status| *status != ExecutionNodeStatus::Planned)
            || !graph.node_results.is_empty()
            || replacement.nodes.is_empty()
            || replacement.id != graph.id
        {
            return Err(ExecutionCommitError::InvalidReplan(
                "strategy retarget requires the same non-empty graph id before any node starts"
                    .to_string(),
            ));
        }
        replacement.revision = graph.revision.saturating_add(1);
        replacement.recovery_cursor = graph.recovery_cursor.clone();
        replacement.node_results.clear();
        replacement.node_statuses.clear();
        for node in &replacement.nodes {
            replacement
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        validate_execution_graph(&replacement)
            .map_err(|error| ExecutionCommitError::InvalidReplan(error.to_string()))?;
        let added_node_ids = replacement
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect();
        let replacement_snapshot = replacement.clone();
        self.append_graph_event(
            &replacement_snapshot,
            graph.revision,
            format!("{}:strategy-retarget:{}", graph.id, replacement.revision),
            ExecutionGraphEvent::Replanned {
                reason,
                added_node_ids,
                delta: ExecutionGraphDelta::between(graph, &replacement),
            },
            Vec::new(),
        )
    }

    pub async fn retarget_planned_graph_async(
        &self,
        graph: ExecutionGraph,
        replacement: ExecutionGraph,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.retarget_planned_graph(&graph, replacement, reason)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn apply_command(
        &self,
        graph: &ExecutionGraph,
        command: &ExecutionGraphCommand,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let expected = command_revision(command);
        if expected != graph.revision {
            return Err(ExecutionCommitError::StaleRevision {
                graph_id: graph.id.clone(),
                expected,
                actual: graph.revision,
            });
        }
        let (command_name, reason) = command_metadata(command);
        let mut next = graph.clone();
        let mut external_resolution: Option<(String, String, String)> = None;
        let mut child_resolution: Option<(String, String, u64, String)> = None;
        match command {
            ExecutionGraphCommand::Pause { .. } => {
                for status in next.node_statuses.values_mut() {
                    if matches!(
                        status,
                        ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running
                    ) {
                        *status = ExecutionNodeStatus::Paused;
                    }
                }
            }
            ExecutionGraphCommand::Resume { .. } | ExecutionGraphCommand::Advance { .. } => {
                for status in next.node_statuses.values_mut() {
                    if *status == ExecutionNodeStatus::Paused {
                        *status = ExecutionNodeStatus::Ready;
                    }
                }
            }
            ExecutionGraphCommand::Cancel { .. } => {
                for status in next.node_statuses.values_mut() {
                    if !status.is_terminal() {
                        *status = ExecutionNodeStatus::Cancelled;
                    }
                }
            }
            ExecutionGraphCommand::CancelNode { node_id, .. } => {
                let status = next.node_statuses.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "cancel target node `{node_id}` does not exist"
                    ))
                })?;
                if !status.is_terminal() {
                    *status = ExecutionNodeStatus::Cancelled;
                }
            }
            ExecutionGraphCommand::Start { .. } => {}
            ExecutionGraphCommand::SubmitApproval {
                node_id, decision, ..
            } => {
                let status = next.node_statuses.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "approval node `{node_id}` does not exist"
                    ))
                })?;
                if *status != ExecutionNodeStatus::WaitingApproval {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "node `{node_id}` is not waiting for approval"
                    )));
                }
                *status = if decision.approved {
                    ExecutionNodeStatus::Ready
                } else {
                    ExecutionNodeStatus::Cancelled
                };
            }
            ExecutionGraphCommand::ResolveExternal {
                node_id,
                result_ref,
                correlation_id,
                ..
            } => {
                let status = next.node_statuses.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "external result node `{node_id}` does not exist"
                    ))
                })?;
                if *status != ExecutionNodeStatus::WaitingExternal {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "node `{node_id}` is not waiting for an external result"
                    )));
                }
                *status = ExecutionNodeStatus::Completed;
                next.node_results.insert(
                    node_id.clone(),
                    ExecutionNodeResult {
                        status: ExecutionNodeStatus::Completed,
                        result_ref: Some(result_ref.clone()),
                        summary: None,
                        evidence_refs: Vec::new(),
                        failure: None,
                        usage: Default::default(),
                        finished_at_ms: now_ms(),
                    },
                );
                external_resolution =
                    Some((node_id.clone(), result_ref.clone(), correlation_id.clone()));
            }
            ExecutionGraphCommand::ResolveChildExecution { receipt, .. } => {
                let node_id = &receipt.parent_node_id;
                let child_execution_id = &receipt.child_execution_id;
                let child_revision = receipt.child_revision;
                let parent_attempt = receipt.parent_attempt;
                let result = &receipt.result;
                let correlation_id = &receipt.correlation_id;
                if receipt.parent_execution_id != graph.id {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "child join receipt parent `{}` does not match graph `{}`",
                        receipt.parent_execution_id, graph.id
                    )));
                }
                let parent_node = next
                    .nodes
                    .iter()
                    .find(|node| node.id == *node_id)
                    .ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(format!(
                            "child join node `{node_id}` does not exist"
                        ))
                    })?;
                if parent_node.kind
                    != harness_contract::execution_graph::ExecutionNodeKind::Subgraph
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "node `{node_id}` is not a Subgraph child join"
                    )));
                }
                let request = serde_json::from_str::<
                    harness_contract::team::TeamInstantiationRequest,
                >(&parent_node.payload_ref)
                .map_err(|error| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "child join node `{node_id}` has invalid Team payload: {error}"
                    ))
                })?;
                if format!("team-graph:{}", request.team_id) != *child_execution_id
                    || request.parent_execution.as_ref().is_none_or(|parent| {
                        parent.execution_id != graph.id || parent.node_id != *node_id
                    })
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "child execution `{child_execution_id}` does not match node `{node_id}` durable binding"
                    )));
                }
                let status = next.node_statuses.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "child join node `{node_id}` does not exist"
                    ))
                })?;
                if *status != ExecutionNodeStatus::WaitingExternal {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "node `{node_id}` is not waiting for a child execution"
                    )));
                }
                let observed_attempt = next
                    .recovery_cursor
                    .node_attempts
                    .get(node_id)
                    .copied()
                    .unwrap_or_default();
                if observed_attempt != parent_attempt {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "child join attempt mismatch for `{node_id}`: expected {parent_attempt}, observed {observed_attempt}"
                    )));
                }
                let expected_result_ref = format!("execution-graph:{child_execution_id}");
                if graph
                    .node_results
                    .get(node_id)
                    .and_then(|result| result.result_ref.as_deref())
                    != Some(expected_result_ref.as_str())
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "child join node `{node_id}` is not bound to `{child_execution_id}`"
                    )));
                }
                let expected_correlation = super::runner::child_resolution_correlation(
                    &graph.id,
                    node_id,
                    child_execution_id,
                    parent_attempt,
                    child_revision,
                );
                if *correlation_id != expected_correlation {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "child join correlation mismatch for `{node_id}`"
                    )));
                }
                if !result.status.is_terminal() {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "child execution `{child_execution_id}` did not provide a terminal result"
                    )));
                }
                *status = result.status;
                next.node_results.insert(node_id.clone(), result.clone());
                child_resolution = Some((
                    node_id.clone(),
                    child_execution_id.clone(),
                    child_revision,
                    correlation_id.clone(),
                ));
            }
            ExecutionGraphCommand::Replan { .. } => {
                return Err(ExecutionCommitError::InvalidCommand(
                    "replan requires the graph compiler and cannot be applied as a status mutation"
                        .to_string(),
                ));
            }
        }
        next.revision = next.revision.saturating_add(1);
        let mut expected_domain_revisions = BTreeMap::new();
        let mut node_events: Vec<RuntimeTransactionEventInput> = next
            .node_statuses
            .iter()
            .filter_map(|(node_id, to)| {
                let from = graph.node_statuses[node_id];
                (from != *to).then(|| node_transition_event(&next, node_id, from, *to, None))
            })
            .collect::<Result<_, _>>()?;
        if let Some((node_id, result_ref, correlation_id)) = external_resolution {
            node_events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("session-handoff-correlation:{correlation_id}"),
                    scope: RuntimeEventScope::SessionInput,
                    kind: "session.handoff.source_resolved.v1".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("ExecutionGraphRunner".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: graph.id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "execution_node".to_string(),
                            id: node_id.clone(),
                        },
                    ],
                    payload: json!({
                        "result_ref": result_ref,
                        "correlation_id": correlation_id,
                    }),
                },
                idempotency_key: Some(format!(
                    "session-handoff-source-resolved:{}:{node_id}:{correlation_id}",
                    graph.id,
                )),
                schema_version: 1,
            });
        }
        if let Some((node_id, child_execution_id, child_revision, correlation_id)) =
            child_resolution
        {
            node_events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("execution-lineage:{}", graph.id),
                    scope: RuntimeEventScope::Relation,
                    kind: "execution.lineage.child_terminal.v1".to_string(),
                    status: next
                        .node_statuses
                        .get(&node_id)
                        .copied()
                        .map(status_name)
                        .map(str::to_string),
                    actor: Some("RuntimeExecutionSupervisor".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: graph.id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "execution_node".to_string(),
                            id: node_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: child_execution_id.clone(),
                        },
                    ],
                    payload: json!({
                        "parent_execution_id": graph.id,
                        "parent_node_id": node_id,
                        "child_execution_id": child_execution_id,
                        "child_revision": child_revision,
                        "correlation_id": correlation_id,
                    }),
                },
                idempotency_key: Some(format!(
                    "child-terminal:{}:{node_id}:{child_execution_id}:{child_revision}",
                    graph.id,
                )),
                schema_version: 1,
            });
        }
        if let ExecutionGraphCommand::SubmitApproval {
            node_id, decision, ..
        } = command
        {
            let expected_approval_id = super::executors::graph_approval_id(&graph.id, node_id);
            if decision.approval_id != expected_approval_id {
                return Err(ExecutionCommitError::InvalidCommand(format!(
                    "approval decision `{}` does not own graph node `{}`",
                    decision.approval_id, node_id
                )));
            }
            let prepared = crate::approval_queue::canonical_graph_decision_events(
                &self.event_store,
                decision,
                now_ms(),
            )
            .map_err(ExecutionCommitError::InvalidCommand)?;
            expected_domain_revisions.insert(
                prepared.stream_id.clone(),
                prepared.expected_stream_revision,
            );
            node_events.extend(prepared.events);
        }
        let event = ExecutionGraphEvent::CommandApplied {
            command: command_name.to_string(),
            reason: reason.map(str::to_string),
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        self.append_graph_event_with_expected_domain_revisions(
            &next,
            graph.revision,
            format!("{}:command:{}:{}", graph.id, command_name, next.revision),
            event,
            node_events,
            &expected_domain_revisions,
        )
    }

    pub(crate) fn validate_command_revision(
        &self,
        graph: &ExecutionGraph,
        command: &ExecutionGraphCommand,
    ) -> Result<(), ExecutionCommitError> {
        let expected = command_revision(command);
        if expected != graph.revision {
            return Err(ExecutionCommitError::StaleRevision {
                graph_id: graph.id.clone(),
                expected,
                actual: graph.revision,
            });
        }
        Ok(())
    }

    pub async fn apply_command_async(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.apply_command(&graph, &command))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub(crate) fn commit_recovery(
        &self,
        graph: &ExecutionGraph,
        mut next: ExecutionGraph,
        recovered_nodes: Vec<String>,
        blocked_nodes: Vec<String>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        next.revision = graph.revision.saturating_add(1);
        let node_events = next
            .node_statuses
            .iter()
            .filter_map(|(node_id, to)| {
                let from = graph.node_statuses[node_id];
                (from != *to).then(|| node_transition_event(&next, node_id, from, *to, None))
            })
            .collect::<Result<_, _>>()?;
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:recovery:{}", graph.id, next.revision),
            ExecutionGraphEvent::Recovered {
                recovered_nodes,
                blocked_nodes,
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            node_events,
        )
    }

    pub(crate) async fn commit_recovery_async(
        &self,
        graph: ExecutionGraph,
        next: ExecutionGraph,
        recovered_nodes: Vec<String>,
        blocked_nodes: Vec<String>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.commit_recovery(&graph, next, recovered_nodes, blocked_nodes)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    fn append_graph_event(
        &self,
        graph: &ExecutionGraph,
        expected_graph_revision: u64,
        transaction_id: String,
        graph_event: ExecutionGraphEvent,
        domain_events: Vec<RuntimeTransactionEventInput>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        self.append_graph_event_with_expected_domain_revisions(
            graph,
            expected_graph_revision,
            transaction_id,
            graph_event,
            domain_events,
            &BTreeMap::new(),
        )
    }

    fn append_graph_event_with_expected_domain_revisions(
        &self,
        graph: &ExecutionGraph,
        expected_graph_revision: u64,
        transaction_id: String,
        graph_event: ExecutionGraphEvent,
        domain_events: Vec<RuntimeTransactionEventInput>,
        expected_domain_revisions: &BTreeMap<String, u64>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        validated_graph_lineage(graph)?;
        if domain_events
            .iter()
            .any(|event| event.event.stream_id == graph.id)
        {
            return Err(ExecutionCommitError::GraphStreamCollision(graph.id.clone()));
        }
        if let Some(event) = domain_events
            .iter()
            .find(|event| event.idempotency_key.is_none())
        {
            return Err(ExecutionCommitError::MissingDomainIdempotency(
                event.event.stream_id.clone(),
            ));
        }
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: graph.id.clone(),
            expected_revision: expected_graph_revision,
        }];
        let mut seen = BTreeSet::from([graph.id.clone()]);
        for event in &domain_events {
            if seen.insert(event.event.stream_id.clone()) {
                let idempotency_key = event.idempotency_key.as_deref().ok_or_else(|| {
                    ExecutionCommitError::MissingDomainIdempotency(event.event.stream_id.clone())
                })?;
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: event.event.stream_id.clone(),
                    expected_revision: match expected_domain_revisions.get(&event.event.stream_id) {
                        Some(revision) => *revision,
                        None => expected_domain_revision(
                            &self.event_store,
                            &event.event.stream_id,
                            &transaction_id,
                            idempotency_key,
                        )?,
                    },
                });
            }
        }
        let graph_event = maybe_checkpoint(graph, graph_event)?;
        let graph_input = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: graph.id.clone(),
                scope: RuntimeEventScope::ExecutionGraph,
                kind: graph_event.kind().to_string(),
                status: graph_status(graph).map(str::to_string),
                actor: Some("execution_commit_service".to_string()),
                refs: graph_identity_refs(graph),
                payload: serde_json::to_value(&graph_event)?,
            }
            .with_activity_binding(root_activity_binding(graph)?)?,
            idempotency_key: Some(format!("{}:revision:{}", graph.id, graph.revision)),
            schema_version: 1,
        };
        let terminal = domain_events.iter().find_map(|event| {
            (event.event.kind == "runtime.session.terminal_requested")
                .then(|| {
                    serde_json::from_value::<SessionTerminalInput>(event.event.payload.clone()).ok()
                })
                .flatten()
        });
        let mut events = vec![graph_input];
        events.extend(domain_events);
        let request = AppendTransactionRequest {
            transaction_id,
            expected_streams,
            events,
        };
        let transaction = match terminal {
            Some(terminal) => self
                .event_store
                .append_transaction_with_terminal(request, terminal)?,
            None => self.event_store.append_transaction(request)?,
        };
        let mut committed_graph = graph.clone();
        committed_graph.recovery_cursor.commit_cursor = transaction.commit_cursor;
        self.hot_graphs.publish(committed_graph.clone());
        let _ = self
            .hot_state
            .materializer()
            .enqueue(DerivedMaterialization {
                key: format!("execution-graph:{}", committed_graph.id),
                revision: committed_graph.revision,
                commit_cursor: committed_graph.recovery_cursor.commit_cursor,
            });
        Ok(ExecutionCommitReceipt {
            graph: committed_graph,
            transaction,
        })
    }
}

fn maybe_checkpoint(
    graph: &ExecutionGraph,
    event: ExecutionGraphEvent,
) -> Result<ExecutionGraphEvent, ExecutionCommitError> {
    if matches!(
        event,
        ExecutionGraphEvent::Planned { .. } | ExecutionGraphEvent::Checkpoint { .. }
    ) {
        return Ok(event);
    }
    let delta_bytes = event.estimated_delta_bytes();
    let snapshot_bytes = crate::execution_core::hot_state::estimate_graph_bytes(graph).max(1);
    let topology_interval = (256 / graph.nodes.len().max(1)).clamp(8, 64) as u64;
    if delta_bytes.saturating_mul(4) >= snapshot_bytes.saturating_mul(3)
        || graph.revision % topology_interval == 0
    {
        return Ok(ExecutionGraphEvent::Checkpoint {
            cause: event.kind().to_string(),
            graph: graph.clone(),
        });
    }
    Ok(event)
}

fn validate_executor_domain_events(
    domain_events: &[RuntimeTransactionEventInput],
) -> Result<(), ExecutionCommitError> {
    if let Some(event) = domain_events.iter().find(|event| {
        !matches!(
            event.event.scope,
            RuntimeEventScope::ExecutionNode
                | RuntimeEventScope::Goal
                | RuntimeEventScope::SessionInput
                | RuntimeEventScope::Relation
                | RuntimeEventScope::Team
                // Approval decisions are generated from a canonical
                // ExecutionGraph command and must commit atomically with the
                // node transition.
                | RuntimeEventScope::Approval
        ) && !crate::authorization_negotiator::is_controlled_recovery_terminal_event(&event.event)
    }) {
        return Err(ExecutionCommitError::ProtectedDomainScope(
            event.event.scope.as_str().to_string(),
        ));
    }
    Ok(())
}

fn graph_identity_refs(graph: &ExecutionGraph) -> Vec<RuntimeEventRef> {
    let mut refs = vec![RuntimeEventRef {
        kind: "execution_graph".to_string(),
        id: graph.id.clone(),
    }];
    for packet in graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .filter_map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok())
    {
        let identity = &packet.assignment.execution_identity;
        refs.extend([
            RuntimeEventRef {
                kind: "principal".to_string(),
                id: identity.principal_id().to_string(),
            },
            RuntimeEventRef {
                kind: "workspace".to_string(),
                id: identity.workspace_id().to_string(),
            },
            RuntimeEventRef {
                kind: "mission".to_string(),
                id: packet.mission_id().to_string(),
            },
            RuntimeEventRef {
                kind: "task".to_string(),
                id: packet.task_id().to_string(),
            },
            RuntimeEventRef {
                kind: "session".to_string(),
                id: packet.session_id().to_string(),
            },
            RuntimeEventRef {
                kind: "agent_run".to_string(),
                id: packet.run_id().to_string(),
            },
            RuntimeEventRef {
                kind: "execution_node".to_string(),
                id: packet.node_id().to_string(),
            },
        ]);
        if let Some(turn_id) = identity.turn_id() {
            refs.push(RuntimeEventRef {
                kind: "turn".to_string(),
                id: turn_id.to_string(),
            });
        }
        if let Some(team_run_id) = packet.team_id() {
            refs.push(RuntimeEventRef {
                kind: "team_run".to_string(),
                id: team_run_id.to_string(),
            });
        }
    }
    refs.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.id.cmp(&right.id))
    });
    refs.dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
    refs
}

fn tool_effect_refs(request: &crate::RuntimeToolExecutionRequest) -> Vec<RuntimeEventRef> {
    let mut refs = vec![RuntimeEventRef {
        kind: "tool_invocation".to_string(),
        id: request.tool_use_id.clone(),
    }];
    if let Some(parent) = &request.parent_execution {
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: parent.execution_id.clone(),
        });
        refs.push(RuntimeEventRef {
            kind: "execution_node".to_string(),
            id: parent.node_id.clone(),
        });
    }
    refs
}

fn bounded_tool_effect_outcome(
    outcome: &crate::RuntimeToolExecutionOutcome,
) -> crate::RuntimeToolExecutionOutcome {
    let mut bounded = outcome.clone();
    bounded.output = bounded.output.map(|output| {
        if output.chars().count() <= MAX_TOOL_EFFECT_RECEIPT_CHARS {
            output
        } else {
            let prefix = output
                .chars()
                .take(MAX_TOOL_EFFECT_RECEIPT_CHARS)
                .collect::<String>();
            format!(
                "{prefix}\n[effect receipt truncated; full output requires its artifact receipt]"
            )
        }
    });
    bounded.error = bounded
        .error
        .map(|error| error.chars().take(MAX_TOOL_EFFECT_RECEIPT_CHARS).collect());
    bounded
}

fn tool_effect_outcome_requires_truncation(outcome: &crate::RuntimeToolExecutionOutcome) -> bool {
    outcome
        .output
        .as_deref()
        .is_some_and(|output| output.chars().count() > MAX_TOOL_EFFECT_RECEIPT_CHARS)
        || outcome
            .error
            .as_deref()
            .is_some_and(|error| error.chars().count() > MAX_TOOL_EFFECT_RECEIPT_CHARS)
}

fn validate_readonly_tool_receipt(
    request: &crate::RuntimeToolExecutionRequest,
    payload: &serde_json::Value,
) -> Result<(), ExecutionCommitError> {
    let expected_hash = format!("sha256:{:x}", Sha256::digest(request.input.as_bytes()));
    let actual_hash = payload
        .get("input_sha256")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let actual_tool = payload
        .get("tool_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if actual_hash != expected_hash || actual_tool != request.tool_name {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "read-only receipt collision for idempotency key `{}`",
            request.idempotency_key
        )));
    }
    Ok(())
}

fn validate_mutation_tool_fingerprint(
    request: &crate::RuntimeToolExecutionRequest,
    effect: &ToolEffectDescriptor,
    payload: &serde_json::Value,
    phase: &str,
) -> Result<(), ExecutionCommitError> {
    let expected_input = format!("sha256:{:x}", Sha256::digest(request.input.as_bytes()));
    let actual_input = payload
        .get("input_sha256")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let actual_tool = payload
        .get("tool_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let actual_descriptor = payload
        .get("descriptor_hash")
        .or_else(|| payload.pointer("/effect/descriptor_hash"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if actual_input != expected_input
        || actual_tool != request.tool_name
        || actual_tool != effect.tool_id
        || actual_descriptor != effect.descriptor_hash
    {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "mutation {phase} collision for idempotency key `{}`",
            request.idempotency_key
        )));
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_lineage_registration_conflict(
    error: &ExecutionCommitError,
    lineage_stream: Option<&str>,
) -> bool {
    matches!(
        (error, lineage_stream),
        (
            ExecutionCommitError::EventStore(RuntimeEventStoreError::StaleRevision {
                stream_id,
                ..
            }),
            Some(expected_stream),
        ) if stream_id == expected_stream
    )
}

fn validate_replan(
    graph: &ExecutionGraph,
    nodes: &[ExecutionNodeSpec],
) -> Result<(), ExecutionCommitError> {
    let existing = graph
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    let unique = nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    if nodes.is_empty()
        || unique.len() != nodes.len()
        || nodes.iter().any(|node| existing.contains(node.id.as_str()))
    {
        return Err(ExecutionCommitError::InvalidReplan(
            "replan must add at least one uniquely identified node".to_string(),
        ));
    }
    Ok(())
}

fn expected_domain_revision(
    event_store: &RuntimeEventStore,
    stream_id: &str,
    transaction_id: &str,
    idempotency_key: &str,
) -> Result<u64, RuntimeEventStoreError> {
    let Some(existing_event) = event_store.event_by_idempotency_key(stream_id, idempotency_key)?
    else {
        return event_store.stream_revision(stream_id);
    };
    if existing_event.transaction_id != transaction_id {
        return event_store.stream_revision(stream_id);
    }
    let existing = event_store
        .list_stream(stream_id)
        .map_err(RuntimeEventStoreError::Corrupt)?;
    existing
        .iter()
        .filter(|event| event.transaction_id == transaction_id)
        .map(|event| event.sequence)
        .min()
        .map_or_else(
            || event_store.stream_revision(stream_id),
            |first_sequence| Ok(first_sequence.saturating_sub(1)),
        )
}

fn node_stream_id(graph_id: &str, node_id: &str) -> String {
    format!("{graph_id}:node:{node_id}")
}

pub(crate) fn execution_lineage_stream_id(parent_execution_id: &str) -> String {
    format!("execution-lineage:{parent_execution_id}")
}

fn node_transition_event(
    graph: &ExecutionGraph,
    node_id: &str,
    from: ExecutionNodeStatus,
    to: ExecutionNodeStatus,
    result: Option<ExecutionNodeResult>,
) -> Result<RuntimeTransactionEventInput, ExecutionCommitError> {
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: node_stream_id(&graph.id, node_id),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution_node.transitioned".to_string(),
            status: Some(status_name(to).to_string()),
            actor: Some("execution_commit_service".to_string()),
            refs: vec![RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: graph.id.clone(),
            }],
            payload: json!({
                "graph_id": graph.id,
                "node_id": node_id,
                "from": from,
                "to": to,
                "result": result,
                "graph_revision": graph.revision,
            }),
        }
        .with_activity_binding(node_activity_binding(graph, node_id)?)?,
        idempotency_key: Some(format!("{}:{}:{}", graph.id, node_id, graph.revision)),
        schema_version: 1,
    })
}

fn root_activity_binding(
    graph: &ExecutionGraph,
) -> Result<harness_contract::projection::RuntimeActivityBinding, ExecutionCommitError> {
    let lineage = validated_graph_lineage(graph)?;
    let parent_activity_id = graph.parent_execution.as_ref().map(|parent| {
        format!(
            "activity:execution:{}:node:{}",
            parent.execution_id, parent.node_id
        )
    });
    Ok(harness_contract::projection::RuntimeActivityBinding {
        root_execution_id: graph.id.clone(),
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
        root_task_id: lineage.root_task_id.clone(),
        task_id: lineage.task_id.clone(),
        activity_id: format!("activity:execution:{}", graph.id),
        node_id: None,
        parent_activity_id: parent_activity_id.clone(),
        initiator_activity_id: parent_activity_id,
        team_run_id: None,
        agent_instance_id: None,
        agent_run_id: None,
        skill_id: None,
        skill_revision: None,
        skill_activation_id: None,
        tool_contract_id: None,
        tool_call_id: None,
        approval_id: None,
        parallel_group_id: None,
        revision: graph.revision.max(1),
        fence: graph.revision.max(1),
        generation: lineage.generation,
    })
}

fn node_activity_binding(
    graph: &ExecutionGraph,
    node_id: &str,
) -> Result<harness_contract::projection::RuntimeActivityBinding, ExecutionCommitError> {
    let lineage = validated_graph_lineage(graph)?;
    let root_activity_id = format!("activity:execution:{}", graph.id);
    Ok(harness_contract::projection::RuntimeActivityBinding {
        root_execution_id: graph.id.clone(),
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
        root_task_id: lineage.root_task_id.clone(),
        task_id: lineage.task_id.clone(),
        activity_id: format!("activity:execution:{}:node:{node_id}", graph.id),
        node_id: Some(node_id.to_string()),
        parent_activity_id: Some(root_activity_id.clone()),
        initiator_activity_id: Some(root_activity_id),
        team_run_id: None,
        agent_instance_id: None,
        agent_run_id: None,
        skill_id: None,
        skill_revision: None,
        skill_activation_id: None,
        tool_contract_id: None,
        tool_call_id: None,
        approval_id: None,
        parallel_group_id: None,
        revision: graph.revision.max(1),
        fence: graph.revision.max(1),
        generation: lineage.generation,
    })
}

fn validated_graph_lineage(
    graph: &ExecutionGraph,
) -> Result<&harness_contract::execution_graph::ExecutionGraphLineage, ExecutionCommitError> {
    let lineage = graph.lineage.as_ref().ok_or_else(|| {
        ExecutionCommitError::InvalidCommand(format!(
            "execution graph `{}` is missing canonical business lineage",
            graph.id
        ))
    })?;
    lineage.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "execution graph `{}` has invalid canonical business lineage: {error}",
            graph.id
        ))
    })?;
    Ok(lineage)
}

fn command_revision(command: &ExecutionGraphCommand) -> u64 {
    match command {
        ExecutionGraphCommand::Start { expected_revision }
        | ExecutionGraphCommand::Advance { expected_revision }
        | ExecutionGraphCommand::Pause {
            expected_revision, ..
        }
        | ExecutionGraphCommand::Resume { expected_revision }
        | ExecutionGraphCommand::Cancel {
            expected_revision, ..
        }
        | ExecutionGraphCommand::CancelNode {
            expected_revision, ..
        }
        | ExecutionGraphCommand::SubmitApproval {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ResolveExternal {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ResolveChildExecution {
            expected_revision, ..
        }
        | ExecutionGraphCommand::Replan {
            expected_revision, ..
        } => *expected_revision,
    }
}

fn command_metadata(command: &ExecutionGraphCommand) -> (&'static str, Option<&str>) {
    match command {
        ExecutionGraphCommand::Start { .. } => ("start", None),
        ExecutionGraphCommand::Advance { .. } => ("advance", None),
        ExecutionGraphCommand::Pause { reason, .. } => ("pause", Some(reason)),
        ExecutionGraphCommand::Resume { .. } => ("resume", None),
        ExecutionGraphCommand::Cancel { reason, .. } => ("cancel", Some(reason)),
        ExecutionGraphCommand::CancelNode { reason, .. } => ("cancel_node", Some(reason)),
        ExecutionGraphCommand::SubmitApproval { .. } => ("submit_approval", None),
        ExecutionGraphCommand::ResolveExternal { .. } => ("resolve_external", None),
        ExecutionGraphCommand::ResolveChildExecution { .. } => ("resolve_child_execution", None),
        ExecutionGraphCommand::Replan { reason, .. } => ("replan", Some(reason)),
    }
}

fn status_name(status: ExecutionNodeStatus) -> &'static str {
    match status {
        ExecutionNodeStatus::Planned => "planned",
        ExecutionNodeStatus::Ready => "ready",
        ExecutionNodeStatus::Running => "running",
        ExecutionNodeStatus::WaitingInput => "waiting_input",
        ExecutionNodeStatus::WaitingApproval => "waiting_approval",
        ExecutionNodeStatus::WaitingExternal => "waiting_external",
        ExecutionNodeStatus::Paused => "paused",
        ExecutionNodeStatus::Completed => "completed",
        ExecutionNodeStatus::Blocked => "blocked",
        ExecutionNodeStatus::Failed => "failed",
        ExecutionNodeStatus::Cancelled => "cancelled",
    }
}

fn graph_status(graph: &ExecutionGraph) -> Option<&'static str> {
    let statuses = graph.node_statuses.values().copied().collect::<Vec<_>>();
    if statuses.is_empty() {
        return Some("planned");
    }
    if statuses.iter().all(|status| status.is_terminal()) {
        if statuses.contains(&ExecutionNodeStatus::Failed) {
            Some("failed")
        } else if statuses.contains(&ExecutionNodeStatus::Blocked) {
            Some("blocked")
        } else if statuses.contains(&ExecutionNodeStatus::Cancelled) {
            Some("cancelled")
        } else {
            Some("completed")
        }
    } else if statuses.contains(&ExecutionNodeStatus::Paused) {
        Some("paused")
    } else {
        Some("running")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::context::ChildExecutionBudgetReservation;
    use harness_contract::tool::{
        ToolApprovalClass, ToolEffectKind, ToolIdempotency, ToolPermissionMode,
    };

    #[test]
    fn controlled_recovery_terminal_is_the_only_tool_scope_graph_event() {
        let record = crate::authorization_negotiator::ControlledRecoveryTerminalRecord {
            recovery_scope: "turn:turn-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            execution_id: "execution-1".to_string(),
            fingerprints: Vec::new(),
        };
        let terminal = crate::authorization_negotiator::controlled_recovery_terminal_event(&record)
            .expect("canonical terminal");
        validate_executor_domain_events(std::slice::from_ref(&terminal))
            .expect("canonical controlled recovery terminal is atomic with graph terminal");

        let mut forged = terminal.clone();
        forged.event.kind = "tool.invocation.completed".to_string();
        assert!(matches!(
            validate_executor_domain_events(&[forged]),
            Err(ExecutionCommitError::ProtectedDomainScope(scope)) if scope == "tool"
        ));

        let mut wrong_turn = terminal;
        wrong_turn.event.stream_id = "authorization-recovery:session-1:turn:other-turn".to_string();
        assert!(matches!(
            validate_executor_domain_events(&[wrong_turn]),
            Err(ExecutionCommitError::ProtectedDomainScope(scope)) if scope == "tool"
        ));
    }

    fn request(id: &str) -> crate::RuntimeToolExecutionRequest {
        crate::RuntimeToolExecutionRequest {
            governed_plan_id: "plan".to_string(),
            governed_plan_revision: 1,
            observation_wave_sequence: 1,
            idempotency_key: format!("idem-{id}"),
            tool_use_id: id.to_string(),
            tool_name: "fixture_tool".to_string(),
            input: format!(r#"{{"id":"{id}"}}"#),
            category: crate::ToolSafetyCategory::ReadOnly,
            authorization: None,
            session_id: Some("session".to_string()),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 0,
            authorized_scopes: Vec::new(),
            memory_context: None,
            model_lease: None,
            parent_execution: None,
            execution_decision: None,
            evaluation_isolated: false,
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        }
    }

    fn outcome(id: &str, output: &str) -> crate::RuntimeToolExecutionOutcome {
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: id.to_string(),
            tool_name: "fixture_tool".to_string(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: crate::ToolSafetyCategory::ReadOnly,
            output: Some(output.to_string()),
            error: None,
            evidence_ref: format!("tool://{id}"),
            observed_evidence: Vec::new(),
        }
    }

    fn mutation_effect(idempotency: ToolIdempotency) -> ToolEffectDescriptor {
        ToolEffectDescriptor {
            tool_id: "fixture_tool".to_string(),
            descriptor_hash: "fixture-effect-v1".to_string(),
            effect_kind: ToolEffectKind::Write,
            idempotency,
            scopes: Vec::new(),
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: harness_contract::policy::EffectAssessment::default(),
        }
    }

    fn readonly_effect() -> ToolEffectDescriptor {
        let mut effect = mutation_effect(ToolIdempotency::Idempotent);
        effect.effect_kind = ToolEffectKind::Read;
        effect.required_permission = ToolPermissionMode::ReadOnly;
        effect.approval_class = ToolApprovalClass::None;
        effect
    }

    fn agent_task_graph() -> ExecutionGraph {
        let packet = AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "agent-instance",
                "agent-run",
                "task",
                "session",
                "mission",
                Some("team-run"),
                "graph",
                "agent-node",
            ),
            attempt: 1,
            expected_graph_revision: 0,
            policy_revision: 1,
            objective: "verify canonical reverse lineage".to_string(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "fast".to_string(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget",
                "agent-instance",
                "agent",
                1_000,
                75_000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "agent-task-idempotency".to_string(),
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent",
            serde_json::to_string(&packet).expect("serialize Agent task packet"),
        );
        node.id = "agent-node".to_string();
        node.idempotency_key = "agent-node-idempotency".to_string();
        let mut graph = ExecutionGraph::new("lineage");
        graph.id = "graph".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        graph.nodes.push(node);
        graph
    }

    fn waiting_child_join_graph() -> ExecutionGraph {
        let graph_id = "parent-graph";
        let node_id = "child-team";
        let child_id = "team-graph:team-child";
        let request = harness_contract::team::TeamInstantiationRequest {
            request_id: "child-request".to_string(),
            team_id: "team-child".to_string(),
            mission_id: "mission".to_string(),
            lineage: harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "session".to_string(),
                turn_id: "turn".to_string(),
                root_task_id: "root-task".to_string(),
                task_id: "task".to_string(),
                generation: 1,
            },
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: graph_id.to_string(),
                node_id: node_id.to_string(),
            }),
            selection_mode: harness_contract::team::TeamSelectionMode::Explicit,
            strategy_binding: None,
            template_selector: harness_contract::team::TeamTemplateSelector::LatestStable {
                template_id: harness_contract::team::TeamTemplateDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/direct-executor",
                )
                .expect("template id"),
            },
            objective: "resolve child".to_string(),
            acceptance: Vec::new(),
            risk: None,
            role_binding_overrides: Vec::new(),
            display_name: None,
            role_display_overrides: Vec::new(),
            cardinality_overrides: Vec::new(),
            focus_partition_plans: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "fixture-model".to_string(),
            execution_budget: harness_contract::context::ParentExecutionBudget::new(
                "fixture-team-budget",
                65_536,
                4_915_200,
                u64::MAX,
                32,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            resource_scopes: Vec::new(),
            upstream_evidence_refs: Vec::new(),
            upstream_artifact_refs: Vec::new(),
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Subgraph,
            "team_subgraph",
            serde_json::to_string(&request).expect("serialize child request"),
        );
        node.id = node_id.to_string();
        node.idempotency_key = "child-request".to_string();
        let mut graph = ExecutionGraph::new("parent");
        graph.id = graph_id.to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.nodes.push(node);
        graph
            .node_statuses
            .insert(node_id.to_string(), ExecutionNodeStatus::WaitingExternal);
        graph.node_results.insert(
            node_id.to_string(),
            ExecutionNodeResult {
                status: ExecutionNodeStatus::WaitingExternal,
                result_ref: Some(format!("execution-graph:{child_id}")),
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            },
        );
        graph
            .recovery_cursor
            .node_attempts
            .insert(node_id.to_string(), 1);
        graph
    }

    fn register_waiting_child_join(service: &ExecutionCommitService) -> ExecutionGraph {
        let registered = service
            .register_graph(waiting_child_join_graph())
            .expect("register parent")
            .graph;
        let ready = service
            .transition_node(
                &registered,
                "child-team",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .expect("ready child join")
            .graph;
        let running = service
            .transition_node(
                &ready,
                "child-team",
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .expect("start child join")
            .graph;
        service
            .transition_node(
                &running,
                "child-team",
                ExecutionNodeStatus::WaitingExternal,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::WaitingExternal,
                    result_ref: Some("execution-graph:team-graph:team-child".to_string()),
                    summary: None,
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                }),
                Vec::new(),
            )
            .expect("persist child join")
            .graph
    }

    #[test]
    fn graph_events_expose_complete_execution_identity_reverse_refs() {
        let refs = graph_identity_refs(&agent_task_graph());
        let pairs = refs
            .iter()
            .map(|reference| (reference.kind.as_str(), reference.id.as_str()))
            .collect::<BTreeSet<_>>();
        for expected in [
            ("execution_graph", "graph"),
            ("principal", "test.principal"),
            ("workspace", "test-workspace"),
            ("mission", "mission"),
            ("task", "task"),
            ("session", "session"),
            ("turn", "test-turn"),
            ("team_run", "team-run"),
            ("agent_run", "agent-run"),
            ("execution_node", "agent-node"),
        ] {
            assert!(
                pairs.contains(&expected),
                "missing reverse lineage ref {expected:?}: {pairs:?}"
            );
        }
    }

    #[test]
    fn readonly_wave_receipts_commit_atomically_and_replay_idempotently() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(Arc::clone(&store));
        let receipts = vec![
            (request("read-1"), outcome("read-1", "one")),
            (request("read-2"), outcome("read-2", "two")),
        ];
        service
            .commit_readonly_tool_receipts(&receipts)
            .expect("commit read wave");
        let first = store
            .event_by_idempotency_key("execution-effect:idem-read-1", "idem-read-1:read-receipt")
            .unwrap()
            .expect("first receipt");
        let second = store
            .event_by_idempotency_key("execution-effect:idem-read-2", "idem-read-2:read-receipt")
            .unwrap()
            .expect("second receipt");
        assert_eq!(first.transaction_id, second.transaction_id);
        service
            .commit_readonly_tool_receipts(&receipts)
            .expect("idempotent replay");
        assert_eq!(
            store
                .list_stream("execution-effect:idem-read-1")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn readonly_receipt_rehydrates_only_for_the_same_tool_and_input_fingerprint() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(store);
        let original = request("read-recovery");
        service
            .commit_readonly_tool_receipts(&[(
                original.clone(),
                outcome("read-recovery", "durable"),
            )])
            .expect("commit bounded read receipt");

        assert!(matches!(
            service
                .begin_tool_effect(&original, &readonly_effect())
                .expect("rehydrate read"),
            ToolEffectState::Completed(crate::RuntimeToolExecutionOutcome {
                output: Some(ref output),
                ..
            }) if output == "durable"
        ));

        let mut collision = original;
        collision.input = r#"{"id":"changed"}"#.to_string();
        assert!(matches!(
            service.begin_tool_effect(&collision, &readonly_effect()),
            Err(ExecutionCommitError::InvalidCommand(_))
        ));
    }

    #[test]
    fn mutation_intent_blocks_uncertain_replay_and_completed_receipt_rehydrates() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(store);
        let mutation_request = request("mutation");
        let non_idempotent = mutation_effect(ToolIdempotency::NonIdempotent);
        assert_eq!(
            service
                .begin_tool_effect(&mutation_request, &non_idempotent)
                .unwrap(),
            ToolEffectState::Fresh
        );
        assert_eq!(
            service
                .begin_tool_effect(&mutation_request, &non_idempotent)
                .unwrap(),
            ToolEffectState::Uncertain
        );
        let mut outcome = outcome("mutation", &"x".repeat(32 * 1024));
        outcome.observed_evidence = vec![harness_contract::context::ObservedEvidence {
            obligation_id: "write:fixture".to_string(),
            target: harness_contract::context::EvidenceTargetIdentity::Workspace {
                scope: harness_contract::context::WorkspaceScopeIdentity {
                    access_mode: harness_contract::context::WorkspaceAccessMode::Write,
                    path: harness_contract::context::WorkspacePathIdentity {
                        workspace_id: "workspace".to_string(),
                        repository_id: "repository".to_string(),
                        workspace_relative_path: "fixture.txt".to_string(),
                        repository_relative_path: "fixture.txt".to_string(),
                        object_kind: harness_contract::context::WorkspaceObjectKind::File,
                        observed_revision_or_digest: Some("after".to_string()),
                    },
                    coverage: harness_contract::context::EvidenceCoverageKind::WriteEffect,
                },
            },
            observed_at_sequence: 1,
            tool_name: "fixture_tool".to_string(),
            provenance: harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
            evidence_ref: None,
            workspace_prior_state: Some(harness_contract::context::WorkspacePriorState::Existing {
                sha256: "before".to_string(),
            }),
        }];
        service
            .commit_tool_effect(&mutation_request, &non_idempotent, &outcome)
            .unwrap();
        let ToolEffectState::Completed(rehydrated) = service
            .begin_tool_effect(&mutation_request, &non_idempotent)
            .unwrap()
        else {
            panic!("completed mutation must rehydrate its receipt");
        };
        assert!(rehydrated.output.unwrap().len() < 20 * 1024);
        assert_eq!(
            rehydrated.observed_evidence[0].workspace_prior_state,
            outcome.observed_evidence[0].workspace_prior_state
        );

        let mut wrong_tool = mutation_request.clone();
        wrong_tool.tool_name = "other_tool".to_string();
        assert!(matches!(
            service.begin_tool_effect(&wrong_tool, &non_idempotent),
            Err(ExecutionCommitError::InvalidCommand(_))
        ));
        let mut wrong_input = mutation_request.clone();
        wrong_input.input = r#"{"id":"other-input"}"#.to_string();
        assert!(matches!(
            service.begin_tool_effect(&wrong_input, &non_idempotent),
            Err(ExecutionCommitError::InvalidCommand(_))
        ));
        let mut wrong_descriptor = non_idempotent.clone();
        wrong_descriptor.descriptor_hash = "fixture-effect-v2".to_string();
        assert!(matches!(
            service.begin_tool_effect(&mutation_request, &wrong_descriptor),
            Err(ExecutionCommitError::InvalidCommand(_))
        ));

        let idempotent_request = request("idempotent-mutation");
        let idempotent = mutation_effect(ToolIdempotency::IdempotentWithKey);
        assert_eq!(
            service
                .begin_tool_effect(&idempotent_request, &idempotent)
                .unwrap(),
            ToolEffectState::Fresh
        );
        let mut changed_idempotent_input = idempotent_request.clone();
        changed_idempotent_input.input = r#"{"id":"changed"}"#.to_string();
        assert!(matches!(
            service.begin_tool_effect(&changed_idempotent_input, &idempotent),
            Err(ExecutionCommitError::InvalidCommand(_))
        ));
        assert_eq!(
            service
                .begin_tool_effect(&idempotent_request, &idempotent)
                .unwrap(),
            ToolEffectState::Fresh
        );
    }

    #[test]
    fn semantic_replan_is_revision_checked_idempotent_and_atomic() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(store);
        let mut graph = agent_task_graph();
        graph.orchestration = Some(
            harness_contract::execution_graph::ExecutionOrchestrationMetadata {
                mutation_id: "initial-mutation".to_string(),
                applied_mutation_ids: vec!["initial-mutation".to_string()],
                semantic_revision: 1,
                source_generation: 1,
                completion: Default::default(),
            },
        );
        let registered = service.register_graph(graph).expect("register graph").graph;
        let mut added =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "bounded-payload");
        added.id = "agent-node-2".to_string();
        added.idempotency_key = "agent-node-2-idempotency".to_string();
        let first = service
            .replan_semantic(
                &registered,
                vec![added.clone()],
                Vec::new(),
                "add bounded reviewer".to_string(),
                "revision-2".to_string(),
                Default::default(),
            )
            .expect("semantic revision commits");
        assert_eq!(first.graph.revision, registered.revision + 1);
        assert_eq!(first.graph.nodes.len(), registered.nodes.len() + 1);
        assert_eq!(
            first
                .graph
                .orchestration
                .as_ref()
                .expect("orchestration")
                .applied_mutation_ids,
            vec!["initial-mutation", "revision-2"]
        );

        let duplicate = match service.replan_semantic(
            &first.graph,
            vec![added.clone()],
            Vec::new(),
            "duplicate".to_string(),
            "revision-2".to_string(),
            Default::default(),
        ) {
            Ok(_) => panic!("same mutation id cannot commit twice"),
            Err(error) => error,
        };
        assert!(matches!(duplicate, ExecutionCommitError::InvalidReplan(_)));

        let mut stale_added =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "stale-payload");
        stale_added.id = "stale-agent-node".to_string();
        stale_added.idempotency_key = "stale-agent-node-idempotency".to_string();
        let stale = match service.replan_semantic(
            &registered,
            vec![stale_added],
            Vec::new(),
            "stale proposal".to_string(),
            "revision-stale".to_string(),
            Default::default(),
        ) {
            Ok(_) => panic!("stale graph revision cannot partially commit"),
            Err(error) => error,
        };
        assert!(matches!(stale, ExecutionCommitError::EventStore(_)));
    }

    #[test]
    fn scoped_cancel_changes_only_the_authorized_node() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(store);
        let mut graph = agent_task_graph();
        let mut peer =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "peer-payload");
        peer.id = "peer-agent-node".to_string();
        peer.idempotency_key = "peer-agent-node-idempotency".to_string();
        graph.nodes.push(peer);
        let registered = service.register_graph(graph).expect("register graph").graph;
        let cancelled = service
            .apply_command(
                &registered,
                &ExecutionGraphCommand::CancelNode {
                    expected_revision: registered.revision,
                    node_id: "agent-node".to_string(),
                    reason: "cancel one Team lane".to_string(),
                },
            )
            .expect("scoped cancel commits")
            .graph;
        assert_eq!(
            cancelled.node_statuses["agent-node"],
            ExecutionNodeStatus::Cancelled
        );
        assert_eq!(
            cancelled.node_statuses["peer-agent-node"],
            ExecutionNodeStatus::Planned
        );
    }

    #[test]
    fn typed_child_receipt_preserves_failure_evidence_and_usage_atomically() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(Arc::clone(&store));
        let registered = register_waiting_child_join(&service);
        let evidence = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed("child_result", "child-proof"),
            "a".repeat(64),
            1,
            "application/json",
            "artifact://child-proof",
            "mission:mission",
        );
        let mut result = ExecutionNodeResult {
            status: ExecutionNodeStatus::Failed,
            result_ref: Some("artifact://child-failure".to_string()),
            summary: Some("child failed after producing evidence".to_string()),
            evidence_refs: vec![evidence.clone()],
            failure: Some(harness_contract::execution_graph::ExecutionFailure {
                kind: "child_failure".to_string(),
                message: "bounded fixture failure".to_string(),
                retryable: false,
                evidence_refs: vec![evidence],
            }),
            usage: Default::default(),
            finished_at_ms: 2,
        };
        result.usage.input_tokens = 11;
        result.usage.output_tokens = 7;
        let child_revision = 9;
        let correlation = super::super::runner::child_resolution_correlation(
            &registered.id,
            "child-team",
            "team-graph:team-child",
            1,
            child_revision,
        );
        let committed = service
            .apply_command(
                &registered,
                &ExecutionGraphCommand::ResolveChildExecution {
                    expected_revision: registered.revision,
                    receipt: Box::new(
                        harness_contract::execution_graph::ChildExecutionTerminalReceipt {
                            parent_execution_id: registered.id.clone(),
                            parent_node_id: "child-team".to_string(),
                            parent_attempt: 1,
                            child_execution_id: "team-graph:team-child".to_string(),
                            child_revision,
                            result: result.clone(),
                            correlation_id: correlation.clone(),
                        },
                    ),
                },
            )
            .expect("resolve child")
            .graph;
        assert_eq!(
            committed.node_statuses["child-team"],
            ExecutionNodeStatus::Failed
        );
        assert_eq!(committed.node_results["child-team"], result);
        let lineage = store
            .list_stream("execution-lineage:parent-graph")
            .expect("lineage stream");
        assert!(lineage.iter().any(|event| {
            event.kind == "execution.lineage.child_terminal.v1"
                && event.payload["correlation_id"] == correlation
        }));
    }

    #[test]
    fn typed_child_receipt_fails_closed_for_wrong_attempt_child_or_correlation() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(store);
        let registered = register_waiting_child_join(&service);
        let base = harness_contract::execution_graph::ChildExecutionTerminalReceipt {
            parent_execution_id: registered.id.clone(),
            parent_node_id: "child-team".to_string(),
            parent_attempt: 1,
            child_execution_id: "team-graph:team-child".to_string(),
            child_revision: 2,
            result: ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("assistant_json:done".to_string()),
                summary: Some("done".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 2,
            },
            correlation_id: super::super::runner::child_resolution_correlation(
                &registered.id,
                "child-team",
                "team-graph:team-child",
                1,
                2,
            ),
        };
        let mut wrong_attempt = base;
        wrong_attempt.parent_attempt = 2;
        let error = match service.apply_command(
            &registered,
            &ExecutionGraphCommand::ResolveChildExecution {
                expected_revision: registered.revision,
                receipt: Box::new(wrong_attempt),
            },
        ) {
            Ok(_) => panic!("mismatched attempt must fail closed"),
            Err(error) => error,
        };
        assert!(matches!(error, ExecutionCommitError::InvalidCommand(_)));

        let mut wrong_child = harness_contract::execution_graph::ChildExecutionTerminalReceipt {
            parent_execution_id: registered.id.clone(),
            parent_node_id: "child-team".to_string(),
            parent_attempt: 1,
            child_execution_id: "team-graph:wrong".to_string(),
            child_revision: 2,
            result: ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 2,
            },
            correlation_id: String::new(),
        };
        wrong_child.correlation_id = super::super::runner::child_resolution_correlation(
            &registered.id,
            "child-team",
            &wrong_child.child_execution_id,
            1,
            2,
        );
        assert!(matches!(
            service.apply_command(
                &registered,
                &ExecutionGraphCommand::ResolveChildExecution {
                    expected_revision: registered.revision,
                    receipt: Box::new(wrong_child),
                },
            ),
            Err(ExecutionCommitError::InvalidCommand(_))
        ));

        let mut wrong_correlation =
            harness_contract::execution_graph::ChildExecutionTerminalReceipt {
                parent_execution_id: registered.id.clone(),
                parent_node_id: "child-team".to_string(),
                parent_attempt: 1,
                child_execution_id: "team-graph:team-child".to_string(),
                child_revision: 2,
                result: ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: None,
                    summary: None,
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 2,
                },
                correlation_id: "wrong".to_string(),
            };
        wrong_correlation.correlation_id.push_str("-correlation");
        assert!(matches!(
            service.apply_command(
                &registered,
                &ExecutionGraphCommand::ResolveChildExecution {
                    expected_revision: registered.revision,
                    receipt: Box::new(wrong_correlation),
                },
            ),
            Err(ExecutionCommitError::InvalidCommand(_))
        ));
    }
}
