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

#[path = "commit_pipeline.rs"]
mod commit_pipeline;
pub(crate) use commit_pipeline::execution_lineage_stream_id;
use commit_pipeline::*;

const MAX_TOOL_EFFECT_RECEIPT_CHARS: usize = 16 * 1024;

fn execution_effect_intent_event(
    ticket: &super::registry::NodeExecutionTicket,
) -> Result<RuntimeTransactionEventInput, serde_json::Error> {
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: format!("execution-effect:{}", ticket.idempotency_key),
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
    })
}

fn execution_effect_receipt_event(
    ticket: &super::registry::NodeExecutionTicket,
    outcome: &super::registry::NodeExecutionOutcome,
) -> Result<RuntimeTransactionEventInput, serde_json::Error> {
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: format!("execution-effect:{}", ticket.idempotency_key),
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
    })
}

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
    #[error("execution graph registration was already applied for continuation root `{graph_id}`")]
    AlreadyAppliedSame { graph_id: String },
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

pub(crate) struct ExecutionTerminalCommit {
    pub(crate) node_id: String,
    pub(crate) outcome: super::registry::NodeExecutionOutcome,
    pub(crate) effect_ticket: Option<super::registry::NodeExecutionTicket>,
}

#[cfg(test)]
#[path = "../tests/commit.rs"]
mod tests;

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

/// Canonical ToolHost receipt indexed by the immutable delegated Agent
/// attempt.  The effect stream remains the idempotency/fencing owner; this
/// compact index only gives recovery an exact, bounded way to reload the
/// receipt set that an acceptance verdict was based on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableAgentToolReceipt {
    pub sequence: u64,
    pub effect_kind: ToolEffectKind,
    pub authorized_scopes: Vec<String>,
    pub outcome: crate::RuntimeToolExecutionOutcome,
}

#[derive(Clone)]
pub struct ExecutionCommitService {
    event_store: Arc<RuntimeEventStore>,
    hot_state: Arc<RuntimeHotStatePlane>,
    hot_graphs: Arc<HotExecutionGraphRegistry>,
}

impl ExecutionCommitService {
    /// Reload the exact ToolHost receipts for one delegated Agent attempt.
    /// The stream name is a durable parent graph/node/attempt key, so restart
    /// never scans unrelated effect streams or reconstructs facts from the
    /// live filesystem.
    pub fn load_delegated_agent_tool_receipts(
        &self,
        graph_id: &str,
        node_id: &str,
        attempt: u32,
    ) -> Result<Vec<DurableAgentToolReceipt>, ExecutionCommitError> {
        let stream_id = format!("execution-agent-receipts:{graph_id}:{node_id}:{attempt}");
        let mut receipts = self
            .event_store
            .list_stream(&stream_id)
            .map_err(RuntimeEventStoreError::Corrupt)?
            .into_iter()
            .filter(|event| event.kind == "execution.agent_tool.receipt")
            .map(|event| {
                let sequence = event.payload["sequence"].as_u64().ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "delegated Agent tool receipt has no sequence".to_string(),
                    )
                })?;
                let effect_kind = serde_json::from_value(event.payload["effect_kind"].clone())?;
                let authorized_scopes =
                    serde_json::from_value(event.payload["authorized_scopes"].clone())?;
                let outcome = serde_json::from_value(event.payload["outcome"].clone())?;
                Ok(DurableAgentToolReceipt {
                    sequence,
                    effect_kind,
                    authorized_scopes,
                    outcome,
                })
            })
            .collect::<Result<Vec<_>, ExecutionCommitError>>()?;
        receipts.sort_by_key(|receipt| receipt.sequence);
        if receipts
            .windows(2)
            .any(|window| window[0].sequence == window[1].sequence)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "delegated Agent receipt index contains duplicate causal sequence".to_string(),
            ));
        }
        Ok(receipts)
    }

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
        let mut expected_streams = BTreeMap::<String, u64>::new();
        let mut events = Vec::with_capacity(receipts.len().saturating_mul(2));
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
            expected_streams.insert(
                stream_id.clone(),
                self.event_store.stream_revision(&stream_id)?,
            );
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
            if let Some(agent_receipt) =
                delegated_agent_receipt_event(request, ToolEffectKind::Read, outcome)
            {
                let agent_stream = agent_receipt.event.stream_id.clone();
                expected_streams
                    .entry(agent_stream.clone())
                    .or_insert(self.event_store.stream_revision(&agent_stream)?);
                events.push(agent_receipt);
            }
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
                expected_streams: expected_streams
                    .into_iter()
                    .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                        stream_id,
                        expected_revision,
                    })
                    .collect(),
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
        let mut expected_streams = BTreeMap::<String, u64>::new();
        expected_streams.insert(
            stream_id.clone(),
            self.event_store.stream_revision(&stream_id)?,
        );
        let mut events = vec![RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream_id.clone(),
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
        }];
        if let Some(agent_receipt) =
            delegated_agent_receipt_event(request, effect.effect_kind, outcome)
        {
            let agent_stream = agent_receipt.event.stream_id.clone();
            expected_streams
                .entry(agent_stream.clone())
                .or_insert(self.event_store.stream_revision(&agent_stream)?);
            events.push(agent_receipt);
        }
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("tool-effect-receipt:{}", request.idempotency_key),
                expected_streams: expected_streams
                    .into_iter()
                    .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                        stream_id,
                        expected_revision,
                    })
                    .collect(),
                events,
            })?;
        Ok(())
    }

    pub fn begin_execution_effect(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<ExecutionEffectState, ExecutionCommitError> {
        let state = self.inspect_execution_effect(ticket)?;
        if state != ExecutionEffectState::Fresh {
            return Ok(state);
        }
        let stream_id = format!("execution-effect:{}", ticket.idempotency_key);
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id,
            revision,
            format!("{}:intent", ticket.idempotency_key),
            vec![execution_effect_intent_event(ticket)?],
        )?;
        Ok(ExecutionEffectState::Fresh)
    }

    pub(crate) fn inspect_execution_effect(
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
            stream_id,
            revision,
            format!("{}:receipt", ticket.idempotency_key),
            vec![execution_effect_receipt_event(ticket, outcome)?],
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
        graph: ExecutionGraph,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        self.register_graph_with_continuation(graph, None)
    }

    pub(crate) fn register_graph_with_continuation(
        &self,
        mut graph: ExecutionGraph,
        continuation_actor: Option<harness_contract::agent_action::AgentActorBinding>,
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
        let continuation_event = graph
            .continuation_binding
            .as_ref()
            .map(|binding| {
                crate::session_continuation::graph_continuation_claim_event(binding, &graph.id)
                    .map_err(ExecutionCommitError::InvalidCommand)
            })
            .transpose()?;
        if let Some(event) = continuation_event.as_ref() {
            if let Some(graph_id) = self.existing_continuation_root(event)? {
                return Err(ExecutionCommitError::AlreadyAppliedSame { graph_id });
            }
        }
        let (seed_event, continuation_revisions) = match continuation_actor.as_ref() {
            Some(actor) => {
                let (event, revisions) =
                    crate::agentic::continuation::prepare(&self.event_store, &graph, actor)
                        .map_err(ExecutionCommitError::InvalidCommand)?;
                (event, revisions)
            }
            None => {
                if let Some(source_id) = graph
                    .continuation_binding
                    .as_ref()
                    .and_then(|binding| binding.team_set_ref.strip_prefix("agentic_program:"))
                {
                    if crate::AgentActionService::new(Arc::clone(&self.event_store))
                        .project_if_exists(source_id)
                        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
                        .is_some_and(|source| {
                            source.status != crate::AgenticProgramStatus::Verified
                        })
                    {
                        return Err(ExecutionCommitError::InvalidCommand(
                            "unfinished continuation requires an atomic Program seed".into(),
                        ));
                    }
                }
                (Vec::new(), BTreeMap::new())
            }
        };
        let domain_events = lineage_event
            .into_iter()
            .chain(continuation_event.clone())
            .chain(seed_event)
            .collect::<Vec<_>>();
        let fenced_parent = graph.parent_execution.as_ref().filter(|_| {
            graph.nodes.iter().any(|node| {
                node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                    && node
                        .resource_scopes
                        .iter()
                        .any(|scope| scope.starts_with("program:"))
            })
        });
        let mut last_lineage_conflict = None;
        for _ in 0..8 {
            let mut expected_revisions = continuation_revisions.clone();
            if let Some(parent) = fenced_parent {
                let source = super::ExecutionGraphStateStore::new(Arc::clone(&self.event_store))
                    .load(&parent.execution_id)
                    .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
                if !source.node_statuses.is_empty()
                    && source
                        .node_statuses
                        .values()
                        .all(|status| status.is_terminal())
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "Agentic parent is terminal; dispatch authority has ended".into(),
                    ));
                }
                expected_revisions.insert(source.id, source.revision);
            }
            match self.append_graph_event_with_expected_domain_revisions(
                &graph,
                0,
                transaction_id.clone(),
                ExecutionGraphEvent::Planned {
                    graph: graph.clone(),
                },
                domain_events.clone(),
                &expected_revisions,
            ) {
                Ok(receipt) => return Ok(receipt),
                Err(error)
                    if is_lineage_registration_conflict(&error, lineage_stream.as_deref())
                        || is_lineage_registration_conflict(
                            &error,
                            fenced_parent.map(|parent| parent.execution_id.as_str()),
                        ) =>
                {
                    last_lineage_conflict = Some(error);
                }
                Err(error) => {
                    // A racing retry can lose after the preflight lookup.
                    // Re-read the immutable continuation claim before
                    // exposing a failure, so callers resume the durable
                    // winner rather than creating or reporting a second root.
                    if let Some(event) = continuation_event.as_ref() {
                        if let Some(graph_id) = self.existing_continuation_root(event)? {
                            return Err(ExecutionCommitError::AlreadyAppliedSame { graph_id });
                        }
                    }
                    return Err(error);
                }
            }
        }
        Err(last_lineage_conflict.unwrap_or_else(|| {
            ExecutionCommitError::InvalidReplan(
                "lineage registration retry exhausted without a conflict receipt".to_string(),
            )
        }))
    }

    /// Return the root selected by an immutable continuation CAS claim. The
    /// idempotency key already includes ingress and binding digest, so this is
    /// only an `AlreadyAppliedSame` result, never a fuzzy duplicate match.
    fn existing_continuation_root(
        &self,
        event: &RuntimeTransactionEventInput,
    ) -> Result<Option<String>, ExecutionCommitError> {
        let key = event.idempotency_key.as_deref().ok_or_else(|| {
            ExecutionCommitError::MissingDomainIdempotency(event.event.stream_id.clone())
        })?;
        let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&event.event.stream_id, key)?
        else {
            return Ok(None);
        };
        let graph_id = existing
            .payload
            .pointer("/root_graph_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(
                    "existing continuation claim has no durable root graph id".to_string(),
                )
            })?;
        Ok(Some(graph_id.to_string()))
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

    pub(crate) async fn register_continuation_graph_async(
        &self,
        graph: ExecutionGraph,
        actor: Option<harness_contract::agent_action::AgentActorBinding>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.register_graph_with_continuation(graph, actor))
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
        self.append_graph_event_retrying_lineage_conflicts(
            &next,
            graph.revision,
            transaction_id,
            graph_event,
            events,
        )
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

    /// Durably starts one scheduler wave in a single graph revision.
    ///
    /// Resource leases and executor tickets are acquired before this boundary.
    /// Committing the whole independent wave atomically avoids turning the
    /// graph event stream into a per-node dispatch lock while preserving an
    /// exact recovery binding for every Running node.
    pub fn bind_and_start_nodes(
        &self,
        graph: &ExecutionGraph,
        bindings: BTreeMap<String, ExecutionNodeBinding>,
        effect_tickets: Vec<super::registry::NodeExecutionTicket>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if bindings.is_empty() {
            return Err(ExecutionCommitError::InvalidCommand(
                "cannot start an empty execution wave".to_string(),
            ));
        }
        let mut next = graph.clone();
        let mut node_events = Vec::with_capacity(bindings.len());
        for (node_id, binding) in &bindings {
            let from = *next
                .node_statuses
                .get(node_id)
                .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.clone()))?;
            next = apply_node_transition(
                &next,
                next.revision,
                node_id,
                ExecutionNodeStatus::Running,
                None,
            )?;
            *next
                .recovery_cursor
                .node_attempts
                .entry(node_id.clone())
                .or_default() = binding.attempt;
            node_events.push((node_id.clone(), from));
        }
        // A wave is one atomic graph mutation even though the contract helper
        // validates each node transition independently above.
        next.revision = graph.revision.saturating_add(1);
        let mut domain_events = node_events
            .into_iter()
            .map(|(node_id, from)| {
                node_transition_event(&next, &node_id, from, ExecutionNodeStatus::Running, None)
            })
            .collect::<Result<Vec<_>, _>>()?;
        domain_events.extend(
            effect_tickets
                .iter()
                .map(execution_effect_intent_event)
                .collect::<Result<Vec<_>, _>>()?,
        );
        let graph_event = ExecutionGraphEvent::NodesStarted {
            bindings,
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:wave:{}:started", graph.id, next.revision),
            graph_event,
            domain_events,
        )
    }

    pub async fn bind_and_start_nodes_async(
        &self,
        graph: ExecutionGraph,
        bindings: BTreeMap<String, ExecutionNodeBinding>,
        effect_tickets: Vec<super::registry::NodeExecutionTicket>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.bind_and_start_nodes(&graph, bindings, effect_tickets)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    /// Commits a set of independently completed Running nodes in one graph
    /// revision together with their effect receipts and domain events.
    pub(crate) fn transition_nodes(
        &self,
        graph: &ExecutionGraph,
        transitions: Vec<ExecutionTerminalCommit>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if transitions.is_empty() {
            return Err(ExecutionCommitError::InvalidCommand(
                "cannot commit an empty terminal wave".to_string(),
            ));
        }
        for transition in &transitions {
            validate_executor_domain_events(&transition.outcome.domain_events)?;
        }
        let mut next = graph.clone();
        let mut transition_facts = Vec::with_capacity(transitions.len());
        for transition in &transitions {
            if transition.outcome.replan.is_some()
                || transition.outcome.delivery_envelope.is_some()
                || transition.outcome.terminal_presentation.is_some()
            {
                return Err(ExecutionCommitError::InvalidCommand(format!(
                    "node `{}` carries a non-batchable terminal mutation",
                    transition.node_id
                )));
            }
            let from = *next.node_statuses.get(&transition.node_id).ok_or_else(|| {
                ExecutionTransitionError::NodeNotFound(transition.node_id.clone())
            })?;
            next = apply_node_transition(
                &next,
                next.revision,
                &transition.node_id,
                transition.outcome.result.status,
                Some(transition.outcome.result.clone()),
            )?;
            transition_facts.push((
                transition.node_id.clone(),
                from,
                transition.outcome.result.status,
            ));
        }
        next.revision = graph.revision.saturating_add(1);
        let mut domain_events = Vec::new();
        for (transition, (node_id, from, to)) in transitions.iter().zip(transition_facts.iter()) {
            domain_events.push(node_transition_event(
                &next,
                node_id,
                *from,
                *to,
                Some(transition.outcome.result.clone()),
            )?);
            domain_events.extend(transition.outcome.domain_events.clone());
            if let Some(candidate) = crate::knowledge_candidate_projector::team_terminal_candidate(
                &next,
                node_id,
                *to,
                next.node_results.get(node_id),
            ) {
                domain_events.push(
                    crate::knowledge_candidate_projector::candidate_proposal_event(candidate)
                        .map_err(ExecutionCommitError::InvalidCommand)?,
                );
            }
            if let Some(ticket) = &transition.effect_ticket {
                domain_events.push(execution_effect_receipt_event(ticket, &transition.outcome)?);
            }
        }
        let node_ids = transitions
            .iter()
            .map(|transition| transition.node_id.clone())
            .collect::<Vec<_>>();
        let graph_event = ExecutionGraphEvent::NodesTransitioned {
            node_ids,
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:wave:{}:terminal", graph.id, next.revision),
            graph_event,
            domain_events,
        )
    }

    pub(crate) async fn transition_nodes_async(
        &self,
        graph: ExecutionGraph,
        transitions: Vec<ExecutionTerminalCommit>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.transition_nodes(&graph, transitions))
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
        self.append_graph_event_retrying_lineage_conflicts(
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
        match command {
            ExecutionGraphCommand::Start { .. } => {}
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
            ExecutionGraphCommand::Replan { .. } => {
                return Err(ExecutionCommitError::InvalidCommand(
                    "replan requires the graph compiler and cannot be applied as a status mutation"
                        .to_string(),
                ));
            }
        }
        next.revision = next.revision.saturating_add(1);
        validate_execution_graph(&next).map_err(|error| {
            ExecutionCommitError::InvalidCommand(format!(
                "graph command violates execution invariants: {error}"
            ))
        })?;
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

    /// Independent child graph admissions may share a parent lineage stream.
    /// Their graph streams remain disjoint, while their lineage relation events
    /// intentionally share the parent stream. Retrying that *domain-stream*
    /// CAS is safe: the graph revision and every domain idempotency key stay
    /// immutable, and the append transaction is atomic. Do not use this for
    /// graph-stream conflicts — those are real competing graph mutations.
    fn append_graph_event_retrying_lineage_conflicts(
        &self,
        graph: &ExecutionGraph,
        expected_graph_revision: u64,
        transaction_id: String,
        graph_event: ExecutionGraphEvent,
        domain_events: Vec<RuntimeTransactionEventInput>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let lineage_streams = domain_events
            .iter()
            .map(|event| event.event.stream_id.as_str())
            .filter(|stream_id| stream_id.starts_with("execution-lineage:"))
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        if lineage_streams.is_empty() {
            return self.append_graph_event(
                graph,
                expected_graph_revision,
                transaction_id,
                graph_event,
                domain_events,
            );
        }
        let mut last_conflict = None;
        for _ in 0..8 {
            match self.append_graph_event(
                graph,
                expected_graph_revision,
                transaction_id.clone(),
                graph_event.clone(),
                domain_events.clone(),
            ) {
                Ok(receipt) => return Ok(receipt),
                Err(error) if is_lineage_stream_conflict(&error, &lineage_streams) => {
                    last_conflict = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_conflict.unwrap_or_else(|| {
            ExecutionCommitError::InvalidReplan(
                "lineage transition retry exhausted without a conflict receipt".to_string(),
            )
        }))
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
        // Include read-only causal dependencies (source Program/graphs) in
        // the same transaction fence even when this command appends no event
        // to their streams.
        for (stream_id, revision) in expected_domain_revisions {
            if seen.insert(stream_id.clone()) {
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: *revision,
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
