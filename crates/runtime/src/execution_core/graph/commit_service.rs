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

/// Merge an additive semantic revision into the graph-owned collaboration
/// program. A replan can add Team obligations but cannot silently replace or
/// delete already-admitted Team instances; destructive topology changes need
/// a separately governed cancellation/replacement command.
fn merge_collaboration_program(
    current: &mut Option<harness_contract::execution_graph::CollaborationProgram>,
    delta: Option<harness_contract::execution_graph::CollaborationProgram>,
) -> Result<(), ExecutionCommitError> {
    let Some(delta) = delta else {
        return Ok(());
    };
    let Some(program) = current.as_mut() else {
        delta
            .validate()
            .map_err(ExecutionCommitError::InvalidReplan)?;
        *current = Some(delta);
        return Ok(());
    };

    let existing_ids = program
        .team_instances
        .iter()
        .map(|instance| instance.instance_id.as_str())
        .collect::<BTreeSet<_>>();
    if delta
        .team_instances
        .iter()
        .any(|instance| existing_ids.contains(instance.instance_id.as_str()))
    {
        return Err(ExecutionCommitError::InvalidReplan(
            "collaboration revision reuses an existing Team instance id".to_string(),
        ));
    }
    let existing_edge_ids = program
        .edges
        .iter()
        .map(|edge| edge.edge_id.as_str())
        .collect::<BTreeSet<_>>();
    if delta
        .edges
        .iter()
        .any(|edge| existing_edge_ids.contains(edge.edge_id.as_str()))
    {
        return Err(ExecutionCommitError::InvalidReplan(
            "collaboration revision reuses an existing cross-Team edge id".to_string(),
        ));
    }
    // Validate a complete candidate rather than the incoming delta alone:
    // an additive review Team may legitimately consume a prior Team instance
    // from the same durable program.
    let mut candidate = program.clone();
    let active = !candidate.control.lifecycle.is_terminal()
        && candidate.control.lifecycle
            != harness_contract::execution_graph::CollaborationProgramLifecycle::Planning;
    if active && delta.control.obligations.len() != delta.team_instances.len() {
        return Err(ExecutionCommitError::InvalidReplan(
            "active Program revision is missing exact Team admission obligations".to_string(),
        ));
    }
    candidate.team_instances.extend(delta.team_instances);
    candidate.edges.extend(delta.edges);
    if active {
        candidate
            .control
            .obligations
            .extend(delta.control.obligations);
        candidate.control.resource_ledger.context_reservation_tokens = candidate
            .control
            .resource_ledger
            .context_reservation_tokens
            .saturating_add(delta.control.resource_ledger.context_reservation_tokens);
        candidate.control.resource_ledger.output_reservation_tokens = candidate
            .control
            .resource_ledger
            .output_reservation_tokens
            .saturating_add(delta.control.resource_ledger.output_reservation_tokens);
        candidate.control.resource_ledger.parallel_demand = candidate
            .control
            .resource_ledger
            .parallel_demand
            .saturating_add(delta.control.resource_ledger.parallel_demand);
        candidate.control.resource_ledger.deadline_at_ms = candidate
            .control
            .resource_ledger
            .deadline_at_ms
            .max(delta.control.resource_ledger.deadline_at_ms);
    }
    for (semantic_id, physical_nodes) in delta.semantic_node_instances {
        if candidate
            .semantic_node_instances
            .insert(semantic_id.clone(), physical_nodes)
            .is_some()
        {
            return Err(ExecutionCommitError::InvalidReplan(format!(
                "collaboration revision reuses semantic Team node `{semantic_id}`"
            )));
        }
    }
    candidate.required_team_count = u16::try_from(
        candidate
            .team_instances
            .iter()
            .filter(|team| team.required)
            .count(),
    )
    .map_err(|_| {
        ExecutionCommitError::InvalidReplan(
            "collaboration revision exceeds u16 Team instance capacity".to_string(),
        )
    })?;
    candidate.revision = candidate.revision.saturating_add(1);
    if active {
        // An obligation is fenced by the Program revision, not by the point
        // at which its Team was first admitted.  Advancing only newly-added
        // obligations would make the candidate internally inconsistent and
        // let a recovery path observe mixed ownership revisions.
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
        candidate.control.resource_ledger.revision = candidate.revision;
        candidate.control.lifecycle =
            harness_contract::execution_graph::CollaborationProgramLifecycle::Admitting;
        candidate.control.waiting_relation = Some("team_admission".to_string());
        candidate.control.blocker_ref = None;
        candidate.control.next_action = Some("admit_exact_team_bindings".to_string());
    }
    candidate
        .validate()
        .map_err(ExecutionCommitError::InvalidReplan)?;
    *program = candidate;
    Ok(())
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
        let continuation_event = graph
            .continuation_binding
            .as_ref()
            .map(|binding| {
                crate::orchestration::collaboration_continuation::graph_continuation_claim_event(
                    binding, &graph.id,
                )
                .map_err(ExecutionCommitError::InvalidCommand)
            })
            .transpose()?;
        if let Some(event) = continuation_event.as_ref() {
            if let Some(graph_id) = self.existing_continuation_root(event)? {
                return Err(ExecutionCommitError::AlreadyAppliedSame { graph_id });
            }
        }
        let domain_events = lineage_event
            .into_iter()
            .chain(continuation_event.clone())
            .collect::<Vec<_>>();
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
        // A cross-Team handoff becomes durable at the producer's terminal
        // transition, not when a consumer happens to wake up later. Keeping
        // this inside the same graph commit makes delivery restart-safe and
        // prevents a completed producer from being observed without its
        // corresponding edge disposition.
        if to.is_terminal() {
            record_terminal_cross_team_edge_deliveries(&mut next, node_id)?;
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

    pub fn replan_semantic(
        &self,
        graph: &ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
        mutation_id: String,
        completion: harness_contract::execution_graph::ExecutionCompletionContract,
        collaboration_program: Option<harness_contract::execution_graph::CollaborationProgram>,
        collaboration_escalation: Option<
            harness_contract::execution_graph::CollaborationEscalationReceipt,
        >,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        self.replan_semantic_with_retirements(
            graph,
            nodes,
            edges,
            reason,
            mutation_id,
            completion,
            collaboration_program,
            collaboration_escalation,
            Vec::new(),
        )
    }

    /// Apply a compiled semantic delta while retiring exact unstarted Team
    /// instances in the very same graph transaction. The caller may only
    /// supply Runtime-derived instance identities; the commit service derives
    /// physical nodes and resource releases from the durable Program.
    pub fn replan_semantic_with_retirements(
        &self,
        graph: &ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
        mutation_id: String,
        completion: harness_contract::execution_graph::ExecutionCompletionContract,
        collaboration_program: Option<harness_contract::execution_graph::CollaborationProgram>,
        collaboration_escalation: Option<
            harness_contract::execution_graph::CollaborationEscalationReceipt,
        >,
        retired_instance_ids: Vec<String>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if mutation_id.trim().is_empty() {
            return Err(ExecutionCommitError::InvalidReplan(
                "semantic mutation id is empty".to_string(),
            ));
        }
        validate_replan(graph, &nodes)?;
        let added_node_ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();
        let mut next = graph.clone();
        retire_program_instances_for_semantic_replan(&mut next, &retired_instance_ids)?;
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
                collaboration_escalations: Vec::new(),
                semantic_revision: 0,
                source_generation: 0,
                completion: Default::default(),
                collaboration_program: None,
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
        if let Some(mut escalation) = collaboration_escalation {
            if escalation.applied_graph_revision != 0 {
                return Err(ExecutionCommitError::InvalidReplan(
                    "collaboration escalation receipt already has an applied graph revision"
                        .to_string(),
                ));
            }
            if orchestration
                .collaboration_escalations
                .iter()
                .any(|existing| existing.escalation_id == escalation.escalation_id)
            {
                return Err(ExecutionCommitError::InvalidReplan(
                    "collaboration escalation receipt is already applied".to_string(),
                ));
            }
            escalation.applied_graph_revision = next.revision;
            orchestration.collaboration_escalations.push(escalation);
            orchestration
                .collaboration_escalations
                .sort_by(|left, right| left.escalation_id.cmp(&right.escalation_id));
        }
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
        merge_collaboration_program(
            &mut orchestration.collaboration_program,
            collaboration_program,
        )?;
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
        collaboration_program: Option<harness_contract::execution_graph::CollaborationProgram>,
        collaboration_escalation: Option<
            harness_contract::execution_graph::CollaborationEscalationReceipt,
        >,
        retired_instance_ids: Vec<String>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.replan_semantic_with_retirements(
                &graph,
                nodes,
                edges,
                reason,
                mutation_id,
                completion,
                collaboration_program,
                collaboration_escalation,
                retired_instance_ids,
            )
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
            ExecutionGraphCommand::UpdateCollaborationProgramControl { control, .. } => {
                let program = next
                    .orchestration
                    .as_mut()
                    .and_then(|metadata| metadata.collaboration_program.as_mut())
                    .ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(
                            "graph has no collaboration program control plane".to_string(),
                        )
                    })?;
                let mut candidate = program.clone();
                candidate.control = (**control).clone();
                candidate.validate().map_err(|error| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "invalid collaboration program control update: {error}"
                    ))
                })?;
                program.control = (**control).clone();
            }
            ExecutionGraphCommand::RecordCrossTeamEdgeDelivery {
                edge_id,
                producer_node_id,
                producer_attempt,
                ..
            } => {
                let program = next
                    .orchestration
                    .as_mut()
                    .and_then(|metadata| metadata.collaboration_program.as_mut())
                    .ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(
                            "graph has no collaboration program control plane".to_string(),
                        )
                    })?;
                let edge_index = program
                    .edges
                    .iter_mut()
                    .position(|edge| edge.edge_id == *edge_id)
                    .ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(format!(
                            "cross-Team edge `{edge_id}` does not exist"
                        ))
                    })?;
                let edge = &program.edges[edge_index];
                if !matches!(
                    edge.state,
                    harness_contract::execution_graph::CrossTeamEdgeState::Pending
                        | harness_contract::execution_graph::CrossTeamEdgeState::AwaitingProducer
                ) {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` cannot record a producer receipt from {:?}",
                        edge.state
                    )));
                }
                let expected_node = physical_node_for_team_instance(program, &edge.from)?;
                if expected_node != *producer_node_id {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` producer node `{producer_node_id}` does not match `{expected_node}`"
                    )));
                }
                let observed_attempt = next
                    .recovery_cursor
                    .node_attempts
                    .get(producer_node_id)
                    .copied()
                    .unwrap_or_default();
                if *producer_attempt == 0 || observed_attempt != *producer_attempt {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` producer attempt mismatch: expected {producer_attempt}, observed {observed_attempt}"
                    )));
                }
                let status = next
                    .node_statuses
                    .get(producer_node_id)
                    .copied()
                    .ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` producer node `{producer_node_id}` is absent"
                    ))
                    })?;
                let result = next.node_results.get(producer_node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` producer node `{producer_node_id}` has no terminal result"
                    ))
                })?;
                if !status.is_terminal()
                    || result.status != status
                    || !cross_team_input_contract_is_satisfied(&edge.input_contract, result)
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` producer receipt does not satisfy its typed input contract"
                    )));
                }
                let producer_result_ref = result
                    .result_ref
                    .clone()
                    .unwrap_or_else(|| format!("execution-node:{producer_node_id}"));
                let receipt = harness_contract::execution_graph::CrossTeamEdgeDeliveryReceipt {
                    receipt_ref: format!(
                        "cross-team-edge:{graph_id}:{edge_id}:producer:{producer_node_id}:attempt:{producer_attempt}",
                        graph_id = graph.id,
                    ),
                    producer_node_id: producer_node_id.clone(),
                    producer_attempt: *producer_attempt,
                    producer_result_ref,
                    evidence_refs: result.evidence_refs.clone(),
                };
                let edge = &mut program.edges[edge_index];
                edge.delivery_receipt = Some(receipt);
                edge.claim_receipt = None;
                edge.state = harness_contract::execution_graph::CrossTeamEdgeState::Delivered;
                program.validate().map_err(|error| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "invalid cross-Team edge delivery update: {error}"
                    ))
                })?;
            }
            ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery {
                edge_id,
                consumer_node_id,
                consumer_attempt,
                ..
            } => {
                let program = next
                    .orchestration
                    .as_mut()
                    .and_then(|metadata| metadata.collaboration_program.as_mut())
                    .ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(
                            "graph has no collaboration program control plane".to_string(),
                        )
                    })?;
                let edge_index = program
                    .edges
                    .iter_mut()
                    .position(|edge| edge.edge_id == *edge_id)
                    .ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(format!(
                            "cross-Team edge `{edge_id}` does not exist"
                        ))
                    })?;
                let edge = &program.edges[edge_index];
                if edge.state != harness_contract::execution_graph::CrossTeamEdgeState::Delivered {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` cannot be claimed from {:?}",
                        edge.state
                    )));
                }
                let expected_node = physical_node_for_team_instance(program, &edge.to)?;
                if expected_node != *consumer_node_id {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` consumer node `{consumer_node_id}` does not match `{expected_node}`"
                    )));
                }
                let observed_attempt = next
                    .recovery_cursor
                    .node_attempts
                    .get(consumer_node_id)
                    .copied()
                    .unwrap_or_default();
                if *consumer_attempt == 0 || observed_attempt != *consumer_attempt {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "cross-Team edge `{edge_id}` consumer attempt mismatch: expected {consumer_attempt}, observed {observed_attempt}"
                    )));
                }
                let claim = harness_contract::execution_graph::CrossTeamEdgeClaimReceipt {
                    claim_ref: format!(
                        "cross-team-edge:{graph_id}:{edge_id}:consumer:{consumer_node_id}:attempt:{consumer_attempt}",
                        graph_id = graph.id,
                    ),
                    consumer_node_id: consumer_node_id.clone(),
                    consumer_attempt: *consumer_attempt,
                };
                let edge = &mut program.edges[edge_index];
                edge.claim_receipt = Some(claim);
                edge.state = harness_contract::execution_graph::CrossTeamEdgeState::Claimed;
                program.validate().map_err(|error| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "invalid cross-Team edge claim update: {error}"
                    ))
                })?;
            }
            ExecutionGraphCommand::ApplyCrossTeamEdgePatch { patch, .. } => {
                apply_cross_team_edge_patch(&mut next, patch)?;
            }
            ExecutionGraphCommand::ApplyCollaborationTeamRetirement { patch, .. } => {
                apply_collaboration_team_retirement(&mut next, patch)?;
            }
            ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing { patch, .. } => {
                apply_collaboration_objective_narrowing(&mut next, patch)?;
            }
            ExecutionGraphCommand::ApplyCollaborationParallelismHint { patch, .. } => {
                apply_collaboration_parallelism_hint(&mut next, patch)?;
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

    /// A parent may admit several independent Subgraph nodes at once. Their
    /// graph streams remain disjoint, while their lineage relation events
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

fn delegated_agent_receipt_stream_id(
    request: &crate::RuntimeToolExecutionRequest,
) -> Option<String> {
    let parent = request.parent_execution.as_ref()?;
    let attempt = request.parent_execution_attempt?;
    Some(format!(
        "execution-agent-receipts:{}:{}:{attempt}",
        parent.execution_id, parent.node_id
    ))
}

fn delegated_agent_receipt_key(request: &crate::RuntimeToolExecutionRequest) -> String {
    format!("agent-tool-receipt:{}", request.idempotency_key)
}

fn delegated_agent_receipt_event(
    request: &crate::RuntimeToolExecutionRequest,
    effect_kind: ToolEffectKind,
    outcome: &crate::RuntimeToolExecutionOutcome,
) -> Option<RuntimeTransactionEventInput> {
    let stream_id = delegated_agent_receipt_stream_id(request)?;
    let mut refs = tool_effect_refs(request);
    if let Some(attempt) = request.parent_execution_attempt {
        refs.push(RuntimeEventRef {
            kind: "agent_attempt".to_string(),
            id: attempt.to_string(),
        });
    }
    Some(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id,
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.agent_tool.receipt".to_string(),
            status: Some("completed".to_string()),
            actor: Some("governed_tool".to_string()),
            refs,
            payload: json!({
                "sequence": request.observation_wave_sequence,
                "effect_kind": effect_kind,
                "authorized_scopes": request.authorized_scopes,
                "outcome": bounded_tool_effect_outcome(outcome),
            }),
        },
        idempotency_key: Some(delegated_agent_receipt_key(request)),
        schema_version: 1,
    })
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

fn is_lineage_stream_conflict(
    error: &ExecutionCommitError,
    lineage_streams: &BTreeSet<String>,
) -> bool {
    matches!(
        error,
        ExecutionCommitError::EventStore(RuntimeEventStoreError::StaleRevision { stream_id, .. })
            if lineage_streams.contains(stream_id)
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

fn retire_program_instances_for_semantic_replan(
    graph: &mut ExecutionGraph,
    instance_ids: &[String],
) -> Result<(), ExecutionCommitError> {
    if instance_ids.is_empty() {
        return Ok(());
    }
    let unique = instance_ids.iter().collect::<BTreeSet<_>>();
    if unique.len() != instance_ids.len() || instance_ids.iter().any(|id| id.trim().is_empty()) {
        return Err(ExecutionCommitError::InvalidReplan(
            "semantic topology replacement has invalid retired Team instances".to_string(),
        ));
    }
    let program = graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidReplan(
                "semantic topology replacement requires a collaboration Program".to_string(),
            )
        })?;
    if program.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidReplan(
            "semantic topology replacement targets a terminal Program".to_string(),
        ));
    }

    let mut retired_nodes = Vec::with_capacity(instance_ids.len());
    let mut released_context = 0u64;
    let mut released_output = 0u64;
    let mut released_parallel = 0u16;
    for instance_id in instance_ids {
        let instance = program
            .team_instances
            .iter()
            .find(|instance| &instance.instance_id == instance_id)
            .ok_or_else(|| {
                ExecutionCommitError::InvalidReplan(format!(
                    "semantic topology replacement references unknown Team instance `{instance_id}`"
                ))
            })?;
        let node_id = physical_node_for_team_instance(program, instance_id)?;
        if graph.node_statuses.get(&node_id).copied() != Some(ExecutionNodeStatus::Planned) {
            return Err(ExecutionCommitError::InvalidReplan(format!(
                "semantic topology replacement requires planned Team `{instance_id}`"
            )));
        }
        if program.control.lifecycle
            != harness_contract::execution_graph::CollaborationProgramLifecycle::Planning
        {
            let reservation = program
                .control
                .obligations
                .iter()
                .find(|obligation| obligation.instance_id == *instance_id)
                .map(|obligation| obligation.reservation.clone())
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidReplan(format!(
                        "semantic topology replacement has no obligation for `{instance_id}`"
                    ))
                })?;
            if reservation == Default::default()
                && (program.control.resource_ledger.context_reservation_tokens > 0
                    || program.control.resource_ledger.output_reservation_tokens > 0
                    || program.control.resource_ledger.parallel_demand > 0)
            {
                return Err(ExecutionCommitError::InvalidReplan(
                    "semantic topology replacement requires exact durable resource reservations"
                        .to_string(),
                ));
            }
            released_context =
                released_context.saturating_add(reservation.context_reservation_tokens);
            released_output = released_output.saturating_add(reservation.output_reservation_tokens);
            released_parallel = released_parallel.saturating_add(reservation.parallel_demand);
        }
        retired_nodes.push((instance.semantic_node_id.clone(), node_id));
    }

    let retired_set = instance_ids.iter().collect::<BTreeSet<_>>();
    program
        .team_instances
        .retain(|instance| !retired_set.contains(&instance.instance_id));
    program
        .edges
        .retain(|edge| !retired_set.contains(&edge.from) && !retired_set.contains(&edge.to));
    program
        .control
        .obligations
        .retain(|obligation| !retired_set.contains(&obligation.instance_id));
    program.control.resource_ledger.context_reservation_tokens = program
        .control
        .resource_ledger
        .context_reservation_tokens
        .saturating_sub(released_context);
    program.control.resource_ledger.output_reservation_tokens = program
        .control
        .resource_ledger
        .output_reservation_tokens
        .saturating_sub(released_output);
    program.control.resource_ledger.parallel_demand = program
        .control
        .resource_ledger
        .parallel_demand
        .saturating_sub(released_parallel);
    for (semantic_id, node_id) in &retired_nodes {
        let remove_mapping = program
            .semantic_node_instances
            .get_mut(semantic_id)
            .is_some_and(|nodes| {
                nodes.retain(|candidate| candidate != node_id);
                nodes.is_empty()
            });
        if remove_mapping {
            program.semantic_node_instances.remove(semantic_id);
        }
    }
    program.required_team_count = u16::try_from(
        program
            .team_instances
            .iter()
            .filter(|instance| instance.required)
            .count(),
    )
    .unwrap_or(u16::MAX);
    for (_, node_id) in &retired_nodes {
        graph
            .node_statuses
            .insert(node_id.clone(), ExecutionNodeStatus::Cancelled);
    }
    let retired_node_set = retired_nodes
        .iter()
        .map(|(_, node_id)| node_id.as_str())
        .collect::<BTreeSet<_>>();
    graph.edges.retain(|edge| {
        !(edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::CrossTeamHandoff
            && (retired_node_set.contains(edge.from.as_str())
                || retired_node_set.contains(edge.to.as_str())))
    });
    if let Some(metadata) = graph.orchestration.as_mut() {
        metadata
            .completion
            .required_node_ids
            .retain(|node_id| !retired_node_set.contains(node_id.as_str()));
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
        | ExecutionGraphCommand::UpdateCollaborationProgramControl {
            expected_revision, ..
        }
        | ExecutionGraphCommand::RecordCrossTeamEdgeDelivery {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCrossTeamEdgePatch {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCollaborationParallelismHint {
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
        ExecutionGraphCommand::UpdateCollaborationProgramControl { .. } => {
            ("update_collaboration_program_control", None)
        }
        ExecutionGraphCommand::RecordCrossTeamEdgeDelivery { .. } => {
            ("record_cross_team_edge_delivery", None)
        }
        ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery { .. } => {
            ("claim_cross_team_edge_delivery", None)
        }
        ExecutionGraphCommand::ApplyCrossTeamEdgePatch { .. } => {
            ("apply_cross_team_edge_patch", None)
        }
        ExecutionGraphCommand::ApplyCollaborationTeamRetirement { .. } => {
            ("apply_collaboration_team_retirement", None)
        }
        ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing { .. } => {
            ("apply_collaboration_objective_narrowing", None)
        }
        ExecutionGraphCommand::ApplyCollaborationParallelismHint { .. } => {
            ("apply_collaboration_parallelism_hint", None)
        }
        ExecutionGraphCommand::Replan { reason, .. } => ("replan", Some(reason)),
    }
}

fn physical_node_for_team_instance(
    program: &harness_contract::execution_graph::CollaborationProgram,
    instance_id: &str,
) -> Result<String, ExecutionCommitError> {
    let instance = program
        .team_instances
        .iter()
        .find(|instance| instance.instance_id == instance_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge references unknown Team instance `{instance_id}`"
            ))
        })?;
    let sibling_index = program
        .team_instances
        .iter()
        .filter(|candidate| candidate.semantic_node_id == instance.semantic_node_id)
        .position(|candidate| candidate.instance_id == instance.instance_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge cannot derive physical Team mapping for `{instance_id}`"
            ))
        })?;
    program
        .semantic_node_instances
        .get(&instance.semantic_node_id)
        .and_then(|nodes| nodes.get(sibling_index))
        .cloned()
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge has no physical Team node for `{instance_id}`"
            ))
        })
}

/// Apply the only currently-safe in-place Program mutation: replacing a
/// pending cross-Team relation before either endpoint has started. The Program
/// contract and its physical `CrossTeamHandoff` edge change together, so a
/// recovery projection can never observe one without the other.
fn apply_cross_team_edge_patch(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CrossTeamEdgeState, ExecutionEdgeKind,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let CollaborationIntentPatchOperation::ChangeEdge {
        edge_id,
        from_instance_id,
        to_instance_id,
        edge_kind,
        input_contract,
    } = &patch.operation
    else {
        return Err(ExecutionCommitError::InvalidCommand(
            "cross-Team edge command requires a change_edge patch".to_string(),
        ));
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "cross-Team edge patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "cross-Team edge patch targets a terminal Program".to_string(),
        ));
    }
    let edge_index = current
        .edges
        .iter()
        .position(|edge| edge.edge_id == *edge_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge patch references unknown edge `{edge_id}`"
            ))
        })?;
    let previous = &current.edges[edge_index];
    if !matches!(
        previous.state,
        CrossTeamEdgeState::Pending | CrossTeamEdgeState::AwaitingProducer
    ) {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "cross-Team edge `{edge_id}` is already {:?} and cannot be changed",
            previous.state
        )));
    }
    let old_from_node = physical_node_for_team_instance(current, &previous.from)?;
    let old_to_node = physical_node_for_team_instance(current, &previous.to)?;
    let new_from_node = physical_node_for_team_instance(current, from_instance_id)?;
    let new_to_node = physical_node_for_team_instance(current, to_instance_id)?;
    for node_id in [&old_from_node, &old_to_node, &new_from_node, &new_to_node] {
        let status = graph.node_statuses.get(node_id).copied().ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge patch node `{node_id}` is absent"
            ))
        })?;
        if status != ExecutionNodeStatus::Planned {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge patch requires planned endpoints; `{node_id}` is {status:?}"
            )));
        }
    }

    let mut candidate = current.clone();
    candidate.edges[edge_index].from = from_instance_id.clone();
    candidate.edges[edge_index].to = to_instance_id.clone();
    candidate.edges[edge_index].kind = *edge_kind;
    candidate.edges[edge_index].input_contract = input_contract.clone();
    candidate.edges[edge_index].state = CrossTeamEdgeState::Pending;
    candidate.edges[edge_index].delivery_receipt = None;
    candidate.edges[edge_index].claim_receipt = None;
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle
        != harness_contract::execution_graph::CollaborationProgramLifecycle::Planning
    {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "invalid cross-Team edge patch candidate: {error}"
        ))
    })?;

    let old_pair_still_exists = candidate.edges.iter().enumerate().any(|(index, edge)| {
        index != edge_index
            && physical_node_for_team_instance(&candidate, &edge.from)
                .is_ok_and(|from| from == old_from_node)
            && physical_node_for_team_instance(&candidate, &edge.to)
                .is_ok_and(|to| to == old_to_node)
    });
    if !old_pair_still_exists {
        graph.edges.retain(|edge| {
            !(edge.kind == ExecutionEdgeKind::CrossTeamHandoff
                && edge.from == old_from_node
                && edge.to == old_to_node)
        });
    }
    if !graph.edges.iter().any(|edge| {
        edge.kind == ExecutionEdgeKind::CrossTeamHandoff
            && edge.from == new_from_node
            && edge.to == new_to_node
    }) {
        graph.edges.push(ExecutionEdge {
            from: new_from_node,
            to: new_to_node,
            kind: ExecutionEdgeKind::CrossTeamHandoff,
        });
    }
    graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .expect("current Program exists")
        .clone_from(&candidate);
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Retire exactly one Team before it has acquired any execution or admission
/// effect.  This is intentionally narrower than a generic graph deletion:
/// the Team node remains durably visible as cancelled, while the Program's
/// semantic contract, resource obligations, physical handoffs and completion
/// gate are revised in the same graph transaction.
fn apply_collaboration_team_retirement(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CollaborationProgramLifecycle, ExecutionEdgeKind,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let CollaborationIntentPatchOperation::RetireTeam { instance_id } = &patch.operation else {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement command requires a retire_team patch".to_string(),
        ));
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement patch targets a terminal Program".to_string(),
        ));
    }
    let instance = current
        .team_instances
        .iter()
        .find(|candidate| candidate.instance_id == *instance_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "Team retirement patch references unknown Team instance `{instance_id}`"
            ))
        })?;
    if instance.required
        && patch
            .user_confirmation_ref
            .as_deref()
            .is_none_or(|reference| reference.trim().is_empty())
    {
        return Err(ExecutionCommitError::InvalidCommand(
            "retiring a required Team requires an explicit user confirmation reference".to_string(),
        ));
    }
    let retired_node_id = physical_node_for_team_instance(current, instance_id)?;
    let status = graph
        .node_statuses
        .get(&retired_node_id)
        .copied()
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "Team retirement node `{retired_node_id}` is absent"
            ))
        })?;
    if status != ExecutionNodeStatus::Planned {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "Team retirement requires a planned Team; `{retired_node_id}` is {status:?}"
        )));
    }
    let reservation = current
        .control
        .obligations
        .iter()
        .find(|obligation| obligation.instance_id == *instance_id)
        .map(|obligation| obligation.reservation.clone())
        .unwrap_or_default();
    if current.control.lifecycle != CollaborationProgramLifecycle::Planning
        && reservation == Default::default()
        && (current.control.resource_ledger.context_reservation_tokens > 0
            || current.control.resource_ledger.output_reservation_tokens > 0
            || current.control.resource_ledger.parallel_demand > 0)
    {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement requires an exact durable resource reservation; legacy aggregate-only Programs are read-only"
                .to_string(),
        ));
    }

    let mut candidate = current.clone();
    candidate
        .team_instances
        .retain(|team| team.instance_id != *instance_id);
    candidate
        .edges
        .retain(|edge| edge.from != *instance_id && edge.to != *instance_id);
    candidate
        .control
        .obligations
        .retain(|obligation| obligation.instance_id != *instance_id);
    candidate.control.resource_ledger.context_reservation_tokens = candidate
        .control
        .resource_ledger
        .context_reservation_tokens
        .saturating_sub(reservation.context_reservation_tokens);
    candidate.control.resource_ledger.output_reservation_tokens = candidate
        .control
        .resource_ledger
        .output_reservation_tokens
        .saturating_sub(reservation.output_reservation_tokens);
    candidate.control.resource_ledger.parallel_demand = candidate
        .control
        .resource_ledger
        .parallel_demand
        .saturating_sub(reservation.parallel_demand);
    let semantic_id = instance.semantic_node_id.clone();
    let remove_mapping = candidate
        .semantic_node_instances
        .get_mut(&semantic_id)
        .is_some_and(|nodes| {
            nodes.retain(|node_id| node_id != &retired_node_id);
            nodes.is_empty()
        });
    if remove_mapping {
        candidate.semantic_node_instances.remove(&semantic_id);
    }
    candidate.required_team_count = u16::try_from(
        candidate
            .team_instances
            .iter()
            .filter(|team| team.required)
            .count(),
    )
    .unwrap_or(u16::MAX);
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle != CollaborationProgramLifecycle::Planning {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!("invalid Team retirement candidate: {error}"))
    })?;

    graph
        .node_statuses
        .insert(retired_node_id.clone(), ExecutionNodeStatus::Cancelled);
    graph.edges.retain(|edge| {
        !(edge.kind == ExecutionEdgeKind::CrossTeamHandoff
            && (edge.from == retired_node_id || edge.to == retired_node_id))
    });
    if let Some(orchestration) = graph.orchestration.as_mut() {
        orchestration
            .completion
            .required_node_ids
            .retain(|node_id| node_id != &retired_node_id);
        orchestration
            .collaboration_program
            .as_mut()
            .expect("current Program exists")
            .clone_from(&candidate);
    }
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Change a semantic Team objective before any mapped physical instance has
/// started. Team identity, template, scope, acceptance, effects and graph
/// topology are intentionally untouched: only the serialized immutable Team
/// request that admission will consume is replaced atomically with the
/// Program revision. A broadening requires a durable user confirmation at
/// contract validation; no objective-only patch can acquire new authority.
fn apply_collaboration_objective_narrowing(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CollaborationProgramLifecycle, ExecutionNodeKind,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let (semantic_node_id, objective) = match &patch.operation {
        CollaborationIntentPatchOperation::NarrowObjective {
            semantic_node_id,
            objective,
        }
        | CollaborationIntentPatchOperation::ExpandObjective {
            semantic_node_id,
            objective,
        } => (semantic_node_id, objective),
        _ => {
            return Err(ExecutionCommitError::InvalidCommand(
                "objective mutation command requires an objective patch".to_string(),
            ));
        }
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "objective narrowing patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "objective narrowing patch targets a terminal Program".to_string(),
        ));
    }
    let node_ids = current
        .semantic_node_instances
        .get(semantic_node_id)
        .filter(|nodes| !nodes.is_empty())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing patch references unknown Team semantic `{semantic_node_id}`"
            ))
        })?
        .clone();
    for node_id in &node_ids {
        let status = graph.node_statuses.get(node_id).copied().ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing node `{node_id}` is absent"
            ))
        })?;
        if status != ExecutionNodeStatus::Planned {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing requires planned Team nodes; `{node_id}` is {status:?}"
            )));
        }
    }

    let mut updated_payloads = BTreeMap::new();
    for node_id in &node_ids {
        let node = graph
            .nodes
            .iter()
            .find(|candidate| candidate.id == *node_id)
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(format!(
                    "objective narrowing node `{node_id}` is absent from node specs"
                ))
            })?;
        if node.kind != ExecutionNodeKind::Subgraph {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing node `{node_id}` is not a Team subgraph"
            )));
        }
        let mut request = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &node.payload_ref,
        )
        .map_err(|error| {
            ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing node `{node_id}` has invalid Team payload: {error}"
            ))
        })?;
        request.objective = objective.clone();
        let payload_ref =
            serde_json::to_string(&request).map_err(ExecutionCommitError::Serialization)?;
        updated_payloads.insert(node_id.clone(), payload_ref);
    }

    let mut candidate = current.clone();
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle != CollaborationProgramLifecycle::Planning {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "invalid objective narrowing candidate: {error}"
        ))
    })?;
    for node in &mut graph.nodes {
        if let Some(payload_ref) = updated_payloads.remove(&node.id) {
            node.payload_ref = payload_ref;
        }
    }
    graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .expect("current Program exists")
        .clone_from(&candidate);
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Apply a model-proposed soft parallelism hint or priority only to unstarted
/// Team work. The value becomes a durable work scheduling priority; it
/// deliberately does not change Team multiplicity, resource demands, permits,
/// or the Program resource ledger.
fn apply_collaboration_parallelism_hint(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CollaborationProgramLifecycle,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let (semantic_node_id, priority) = match &patch.operation {
        CollaborationIntentPatchOperation::SetParallelismHint {
            semantic_node_id,
            parallelism_hint,
        } => (
            semantic_node_id,
            u8::try_from(*parallelism_hint).unwrap_or(u8::MAX),
        ),
        CollaborationIntentPatchOperation::Reprioritize {
            semantic_node_id,
            priority,
        } => (semantic_node_id, *priority),
        _ => {
            return Err(ExecutionCommitError::InvalidCommand(
                "parallelism hint command requires a soft scheduling patch".to_string(),
            ));
        }
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "parallelism hint patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "parallelism hint patch targets a terminal Program".to_string(),
        ));
    }
    let node_ids = current
        .semantic_node_instances
        .get(semantic_node_id)
        .filter(|nodes| !nodes.is_empty())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "parallelism hint patch references unknown Team semantic `{semantic_node_id}`"
            ))
        })?
        .clone();
    for node_id in &node_ids {
        if graph.node_statuses.get(node_id) != Some(&ExecutionNodeStatus::Planned) {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "parallelism hint requires planned Team nodes; `{node_id}` is not planned"
            )));
        }
    }
    let mut candidate = current.clone();
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle != CollaborationProgramLifecycle::Planning {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!("invalid parallelism hint candidate: {error}"))
    })?;
    for node in &mut graph.nodes {
        if node_ids.contains(&node.id) {
            if node.kind != ExecutionNodeKind::Subgraph {
                return Err(ExecutionCommitError::InvalidCommand(format!(
                    "parallelism hint node `{}` is not a Team subgraph",
                    node.id
                )));
            }
            let work = node.work.get_or_insert_with(|| {
                harness_contract::execution_graph::ExecutionWorkContract::new(
                    harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze,
                )
            });
            work.scheduling_priority = priority;
        }
    }
    graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .expect("current Program exists")
        .clone_from(&candidate);
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Derive the immutable producer receipt as part of its terminal graph
/// transition. A terminal result that fails the typed input contract records a
/// `Blocked` edge instead of leaving an apparently-pending handoff that a
/// consumer could mistake for recoverable work.
fn record_terminal_cross_team_edge_deliveries(
    graph: &mut ExecutionGraph,
    producer_node_id: &str,
) -> Result<(), ExecutionCommitError> {
    let producer_attempt = graph
        .recovery_cursor
        .node_attempts
        .get(producer_node_id)
        .copied()
        .unwrap_or_default();
    let result = graph.node_results.get(producer_node_id).cloned();
    let Some(program) = graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
    else {
        return Ok(());
    };
    let mut affected = Vec::new();
    for (index, edge) in program.edges.iter().enumerate() {
        if !matches!(
            edge.state,
            harness_contract::execution_graph::CrossTeamEdgeState::Pending
                | harness_contract::execution_graph::CrossTeamEdgeState::AwaitingProducer
        ) {
            continue;
        }
        if physical_node_for_team_instance(program, &edge.from)? == producer_node_id {
            affected.push(index);
        }
    }

    for index in affected {
        let edge = &mut program.edges[index];
        edge.claim_receipt = None;
        if producer_attempt > 0
            && result.as_ref().is_some_and(|result| {
                cross_team_input_contract_is_satisfied(&edge.input_contract, result)
            })
        {
            let result = result.as_ref().expect("checked above");
            edge.delivery_receipt = Some(
                harness_contract::execution_graph::CrossTeamEdgeDeliveryReceipt {
                    receipt_ref: format!(
                        "cross-team-edge:{}:{}:producer:{}:attempt:{}",
                        graph.id, edge.edge_id, producer_node_id, producer_attempt
                    ),
                    producer_node_id: producer_node_id.to_string(),
                    producer_attempt,
                    producer_result_ref: result
                        .result_ref
                        .clone()
                        .unwrap_or_else(|| format!("execution-node:{producer_node_id}")),
                    evidence_refs: result.evidence_refs.clone(),
                },
            );
            edge.state = harness_contract::execution_graph::CrossTeamEdgeState::Delivered;
        } else {
            edge.delivery_receipt = None;
            edge.state = harness_contract::execution_graph::CrossTeamEdgeState::Blocked;
        }
    }
    program.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "invalid automatic cross-Team edge delivery update: {error}"
        ))
    })
}

fn cross_team_input_contract_is_satisfied(
    contract: &harness_contract::execution_graph::CrossTeamInputContract,
    result: &ExecutionNodeResult,
) -> bool {
    use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};

    let has_fact = |kind| match kind {
        TerminalFactKind::CommittedEffect => {
            result
                .usage
                .observed_acceptance
                .observed_evidence
                .iter()
                .any(|evidence| evidence.workspace_prior_state.is_some())
                || result
                    .evidence_refs
                    .iter()
                    .any(|reference| reference.evidence_ref.ref_type == "runtime_change")
        }
        TerminalFactKind::ObservedEvidence => {
            !result
                .usage
                .observed_acceptance
                .observed_evidence
                .is_empty()
                || result
                    .evidence_refs
                    .iter()
                    .any(harness_contract::context::EvidenceAccessRef::is_durable)
        }
        TerminalFactKind::Artifact => result.evidence_refs.iter().any(|reference| {
            reference.evidence_ref.ref_type == "artifact" || reference.is_durable()
        }),
        TerminalFactKind::AcceptanceVerdict => result
            .usage
            .acceptance_evaluation
            .as_ref()
            .is_some_and(|evaluation| evaluation.verdict != AcceptanceVerdict::Unresolved),
    };
    contract
        .required_fact_kinds
        .iter()
        .all(|kind| has_fact(*kind))
        && contract.required_artifact_kinds.iter().all(|kind| {
            result
                .evidence_refs
                .iter()
                .any(|reference| reference.evidence_ref.ref_type == *kind)
        })
        && (!contract.require_committed_effect || has_fact(TerminalFactKind::CommittedEffect))
        && (!contract.require_satisfied_acceptance
            || result
                .usage
                .acceptance_evaluation
                .as_ref()
                .is_some_and(|evaluation| evaluation.verdict == AcceptanceVerdict::Satisfied))
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
            parent_execution_attempt: None,
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
            requires_managed_collaboration_escalation: false,
            acceptance: Vec::new(),
            team_role_identity: None,
            team_role: None,
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
            requires_managed_collaboration_escalation: false,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "fixture-model".to_string(),
            execution_budget: harness_contract::context::ParentExecutionBudget::new(
                "fixture-team-budget",
                65_536,
                u64::MAX,
                32,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            resource_scopes: Vec::new(),
            allow_whole_workspace_scope: false,
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

    #[test]
    fn planned_graph_and_continuation_claim_commit_in_one_transaction() {
        let store = Arc::new(crate::RuntimeEventStore::try_open_in_memory().expect("store"));
        let service = ExecutionCommitService::new(Arc::clone(&store));
        let candidate = crate::ContinuationCandidate {
            source_session_id: "session".to_string(),
            source_turn_id: "turn-previous".to_string(),
            source_root_id: "root-previous".to_string(),
            team_set_ref: "team_graph:team-previous".to_string(),
            delivery_revision: 9,
            result_refs: vec!["team_graph:team-previous".to_string()],
            handoff_id: None,
        };
        let binding = crate::compile_continuation_binding(
            &candidate,
            "ingress-current",
            9,
            harness_contract::turn::ContinuationAuthorization::Authorized,
            1,
        )
        .expect("binding");
        let mut graph = ExecutionGraph::new("continue verified Team work");
        graph.id = "root-current".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.continuation_binding = Some(binding.clone());

        let receipt = service.register_graph(graph).expect("atomic registration");
        assert_eq!(receipt.graph.continuation_binding, Some(binding.clone()));
        let claim = store
            .list_stream("continuation-cas")
            .expect("claim stream")
            .into_iter()
            .next()
            .expect("claim event");
        let planned = store
            .list_stream("root-current")
            .expect("graph stream")
            .into_iter()
            .next()
            .expect("planned graph");
        assert_eq!(claim.transaction_id, planned.transaction_id);
        assert_eq!(claim.commit_cursor, planned.commit_cursor);
        assert_eq!(
            claim
                .payload
                .pointer("/root_graph_id")
                .and_then(serde_json::Value::as_str),
            Some("root-current")
        );

        let mut retry = ExecutionGraph::new("continue verified Team work");
        retry.id = "root-retry-must-not-exist".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut retry);
        retry.continuation_binding = Some(binding);
        let error = match service.register_graph(retry) {
            Ok(_) => panic!("continuation retry must return the existing root"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ExecutionCommitError::AlreadyAppliedSame { ref graph_id } if graph_id == "root-current"
        ));
        assert!(store
            .list_stream("root-retry-must-not-exist")
            .expect("retry graph stream")
            .is_empty());
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
    fn delegated_agent_receipts_are_indexed_atomically_and_reload_without_scanning_effects() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let service = ExecutionCommitService::new(Arc::clone(&store));
        let request = crate::RuntimeToolExecutionRequest {
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "graph-agent-receipts".to_string(),
                node_id: "agent-node".to_string(),
            }),
            parent_execution_attempt: Some(3),
            authorized_scopes: vec!["read:src/lib.rs".to_string()],
            ..request("agent-receipt")
        };
        let outcome = outcome("agent-receipt", "durable observation");
        service
            .commit_readonly_tool_receipts(&[(request.clone(), outcome.clone())])
            .expect("commit exact receipt and index atomically");

        let index_stream = "execution-agent-receipts:graph-agent-receipts:agent-node:3";
        let indexed = store.list_stream(index_stream).expect("indexed stream");
        assert_eq!(indexed.len(), 1);
        let effect = store
            .list_stream(&format!("execution-effect:{}", request.idempotency_key))
            .expect("effect stream");
        assert_eq!(effect.len(), 1);
        assert_eq!(indexed[0].transaction_id, effect[0].transaction_id);

        let recovered = ExecutionCommitService::new(store)
            .load_delegated_agent_tool_receipts("graph-agent-receipts", "agent-node", 3)
            .expect("reload exact attempt receipts");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].sequence, request.observation_wave_sequence);
        assert_eq!(recovered[0].authorized_scopes, request.authorized_scopes);
        assert_eq!(recovered[0].outcome, outcome);
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
                collaboration_escalations: Vec::new(),
                semantic_revision: 1,
                source_generation: 1,
                completion: Default::default(),
                collaboration_program: None,
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
                None,
                None,
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
            None,
            None,
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
            None,
            None,
        ) {
            Ok(_) => panic!("stale graph revision cannot partially commit"),
            Err(error) => error,
        };
        assert!(matches!(stale, ExecutionCommitError::EventStore(_)));
    }

    #[test]
    fn collaboration_program_revision_keeps_prior_obligations_and_adds_new_teams() {
        use harness_contract::execution_graph::{
            CollaborationEdgeKind, CollaborationProgram, CollaborationProgramControlState,
            CollaborationProgramEdge, CollaborationProgramLifecycle, CollaborationTeamInstance,
            ProgramResourceLedger, TeamAdmissionObligation, TeamAdmissionResourceReservation,
            TeamAdmissionState,
        };

        let mut current = Some(CollaborationProgram {
            program_id: "program-root".to_string(),
            revision: 1,
            required_team_count: 1,
            team_instances: vec![CollaborationTeamInstance {
                instance_id: "research:1".to_string(),
                semantic_node_id: "research".to_string(),
                required: true,
            }],
            edges: Vec::new(),
            semantic_node_instances: BTreeMap::from([(
                "research".to_string(),
                vec!["graph:research:1".to_string()],
            )]),
            control: CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Running,
                obligations: vec![TeamAdmissionObligation {
                    instance_id: "research:1".to_string(),
                    binding_ref: "team-binding:sha256:research".to_string(),
                    state: TeamAdmissionState::Admitted,
                    child_graph_ref: Some("team-graph:research".to_string()),
                    reason_kind: None,
                    terminal: None,
                    reservation: TeamAdmissionResourceReservation {
                        context_reservation_tokens: 100,
                        output_reservation_tokens: 50,
                        parallel_demand: 1,
                    },
                    revision: 1,
                }],
                resource_ledger: ProgramResourceLedger {
                    context_reservation_tokens: 100,
                    output_reservation_tokens: 50,
                    parallel_demand: 1,
                    deadline_at_ms: 1000,
                    confidence_basis_points: 10_000,
                    revision: 1,
                },
                waiting_relation: None,
                blocker_ref: None,
                next_action: Some("await_graph_transitions".to_string()),
            },
            semantic_intent: None,
        });
        let delta = CollaborationProgram {
            program_id: "ignored-delta-id".to_string(),
            revision: 1,
            required_team_count: 1,
            team_instances: vec![CollaborationTeamInstance {
                instance_id: "review:1".to_string(),
                semantic_node_id: "review".to_string(),
                required: true,
            }],
            edges: vec![CollaborationProgramEdge {
                edge_id: "research:1->review:1".to_string(),
                from: "research:1".to_string(),
                to: "review:1".to_string(),
                kind: CollaborationEdgeKind::ReviewOf,
                input_contract: Default::default(),
                state: Default::default(),
                delivery_receipt: None,
                claim_receipt: None,
            }],
            semantic_node_instances: BTreeMap::from([(
                "review".to_string(),
                vec!["graph:review:1".to_string()],
            )]),
            control: CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Admitting,
                obligations: vec![TeamAdmissionObligation {
                    instance_id: "review:1".to_string(),
                    binding_ref: "team-binding:sha256:review".to_string(),
                    state: TeamAdmissionState::Admitting,
                    child_graph_ref: None,
                    reason_kind: None,
                    terminal: None,
                    reservation: TeamAdmissionResourceReservation {
                        context_reservation_tokens: 70,
                        output_reservation_tokens: 30,
                        parallel_demand: 1,
                    },
                    revision: 1,
                }],
                resource_ledger: ProgramResourceLedger {
                    context_reservation_tokens: 70,
                    output_reservation_tokens: 30,
                    parallel_demand: 1,
                    deadline_at_ms: 2000,
                    confidence_basis_points: 10_000,
                    revision: 1,
                },
                waiting_relation: Some("team_admission".to_string()),
                blocker_ref: None,
                next_action: Some("admit_exact_team_bindings".to_string()),
            },
            semantic_intent: None,
        };
        merge_collaboration_program(&mut current, Some(delta)).expect("merge additive revision");
        let program = current.expect("program");
        assert_eq!(program.program_id, "program-root");
        assert_eq!(program.revision, 2);
        assert_eq!(program.required_team_count, 2);
        assert_eq!(program.edges.len(), 1);
        assert_eq!(program.semantic_node_instances.len(), 2);
        assert_eq!(
            program.control.lifecycle,
            CollaborationProgramLifecycle::Admitting
        );
        assert_eq!(program.control.obligations.len(), 2);
        assert_eq!(
            program.control.obligations[0].state,
            TeamAdmissionState::Admitted
        );
        assert_eq!(
            program.control.obligations[1].state,
            TeamAdmissionState::Admitting
        );
        assert!(program
            .control
            .obligations
            .iter()
            .all(|obligation| obligation.revision == 2));
        assert_eq!(program.control.resource_ledger.revision, 2);
        assert_eq!(
            program.control.resource_ledger.context_reservation_tokens,
            170
        );
        assert_eq!(
            program.control.resource_ledger.output_reservation_tokens,
            80
        );
        assert_eq!(program.control.resource_ledger.parallel_demand, 2);
        assert_eq!(program.control.resource_ledger.deadline_at_ms, 2000);
        assert_eq!(
            program
                .team_instances
                .iter()
                .map(|team| team.instance_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["research:1", "review:1"])
        );
    }

    #[test]
    fn cross_team_edge_delivery_and_claim_are_fenced_by_node_attempts() {
        use harness_contract::execution_graph::{
            CollaborationEdgeKind, CollaborationProgram, CollaborationProgramEdge,
            CollaborationProgramLifecycle, CollaborationTeamInstance, ExecutionGraphCommand,
            ExecutionOrchestrationMetadata,
        };

        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let service = ExecutionCommitService::new(store);
        let mut graph = agent_task_graph();
        graph.id = "cross-team-root".to_string();
        let mut consumer = graph.nodes[0].clone();
        consumer.id = "consumer-team".to_string();
        consumer.idempotency_key = "consumer-team-key".to_string();
        graph.nodes[0].id = "producer-team".to_string();
        graph.nodes[0].idempotency_key = "producer-team-key".to_string();
        graph.node_statuses.clear();
        graph
            .node_statuses
            .insert("producer-team".to_string(), ExecutionNodeStatus::Planned);
        graph
            .node_statuses
            .insert("consumer-team".to_string(), ExecutionNodeStatus::Planned);
        graph.nodes.push(consumer);
        graph.orchestration = Some(ExecutionOrchestrationMetadata {
            mutation_id: "cross-team-test".to_string(),
            applied_mutation_ids: Vec::new(),
            collaboration_escalations: Vec::new(),
            semantic_revision: 1,
            source_generation: 1,
            completion: Default::default(),
            collaboration_program: Some(CollaborationProgram {
                program_id: "program-cross-team".to_string(),
                revision: 1,
                required_team_count: 2,
                team_instances: vec![
                    CollaborationTeamInstance {
                        instance_id: "producer:1".to_string(),
                        semantic_node_id: "producer".to_string(),
                        required: true,
                    },
                    CollaborationTeamInstance {
                        instance_id: "consumer:1".to_string(),
                        semantic_node_id: "consumer".to_string(),
                        required: true,
                    },
                ],
                edges: vec![CollaborationProgramEdge {
                    edge_id: "producer:1->consumer:1".to_string(),
                    from: "producer:1".to_string(),
                    to: "consumer:1".to_string(),
                    kind: CollaborationEdgeKind::Handoff,
                    input_contract: Default::default(),
                    state: Default::default(),
                    delivery_receipt: None,
                    claim_receipt: None,
                }],
                semantic_node_instances: BTreeMap::from([
                    ("producer".to_string(), vec!["producer-team".to_string()]),
                    ("consumer".to_string(), vec!["consumer-team".to_string()]),
                ]),
                control: harness_contract::execution_graph::CollaborationProgramControlState {
                    lifecycle: CollaborationProgramLifecycle::Planning,
                    ..Default::default()
                },
                semantic_intent: None,
            }),
        });
        graph.edges = vec![ExecutionEdge {
            from: "producer-team".to_string(),
            to: "consumer-team".to_string(),
            kind: harness_contract::execution_graph::ExecutionEdgeKind::CrossTeamHandoff,
        }];
        let registered = service.register_graph(graph).expect("register graph").graph;
        let registered = service
            .apply_command(
                &registered,
                &ExecutionGraphCommand::ApplyCrossTeamEdgePatch {
                    expected_revision: registered.revision,
                    patch: Box::new(
                        harness_contract::execution_graph::CollaborationIntentPatch {
                            program_id: "program-cross-team".to_string(),
                            base_revision: 1,
                            source_attempt: "producer-team:attempt:0".to_string(),
                            reason: "review the same bounded producer result".to_string(),
                            evidence_refs: Vec::new(),
                            canonical_digest: "e".repeat(64),
                            user_confirmation_ref: None,
                            escalation: None,
                            operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::ChangeEdge {
                                edge_id: "producer:1->consumer:1".to_string(),
                                from_instance_id: "producer:1".to_string(),
                                to_instance_id: "consumer:1".to_string(),
                                edge_kind: CollaborationEdgeKind::ReviewOf,
                                input_contract: Default::default(),
                            },
                        },
                    ),
                },
            )
            .expect("pending edge patch commits atomically")
            .graph;
        let patched_edge = &registered
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("program")
            .edges[0];
        assert_eq!(patched_edge.kind, CollaborationEdgeKind::ReviewOf);
        assert_eq!(
            registered
                .edges
                .iter()
                .filter(|edge| edge.kind.is_dependency())
                .count(),
            1
        );
        let ready = service
            .transition_node(
                &registered,
                "producer-team",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .expect("producer ready")
            .graph;
        let running = service
            .transition_node(
                &ready,
                "producer-team",
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .expect("producer running")
            .graph;
        let completed = service
            .transition_node(
                &running,
                "producer-team",
                ExecutionNodeStatus::Completed,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: Some("artifact://producer-result".to_string()),
                    summary: Some("durable producer outcome".to_string()),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                }),
                Vec::new(),
            )
            .expect("producer completed")
            .graph;
        let delivered = completed;
        let edge = &delivered
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("program")
            .edges[0];
        assert_eq!(
            edge.state,
            harness_contract::execution_graph::CrossTeamEdgeState::Delivered
        );
        assert_eq!(
            edge.delivery_receipt
                .as_ref()
                .map(|receipt| receipt.producer_result_ref.as_str()),
            Some("artifact://producer-result")
        );

        let consumer_ready = service
            .transition_node(
                &delivered,
                "consumer-team",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .expect("consumer ready")
            .graph;
        let consumer_running = service
            .transition_node(
                &consumer_ready,
                "consumer-team",
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .expect("consumer running")
            .graph;
        let claimed = service
            .apply_command(
                &consumer_running,
                &ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery {
                    expected_revision: consumer_running.revision,
                    edge_id: "producer:1->consumer:1".to_string(),
                    consumer_node_id: "consumer-team".to_string(),
                    consumer_attempt: 1,
                },
            )
            .expect("claim commits")
            .graph;
        let edge = &claimed
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("program")
            .edges[0];
        assert_eq!(
            edge.state,
            harness_contract::execution_graph::CrossTeamEdgeState::Claimed
        );
        assert_eq!(
            edge.claim_receipt
                .as_ref()
                .map(|receipt| receipt.consumer_attempt),
            Some(1)
        );
    }

    #[test]
    fn retirement_cancels_only_a_confirmed_unstarted_team_and_revises_program_atomically() {
        use harness_contract::execution_graph::{
            CollaborationEdgeKind, CollaborationProgram, CollaborationProgramEdge,
            CollaborationProgramLifecycle, CollaborationTeamInstance, ExecutionGraphCommand,
            ExecutionOrchestrationMetadata,
        };

        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let service = ExecutionCommitService::new(store);
        let mut graph = agent_task_graph();
        graph.id = "retire-team-root".to_string();
        let mut consumer = graph.nodes[0].clone();
        consumer.id = "consumer-team".to_string();
        consumer.idempotency_key = "consumer-team-key".to_string();
        graph.nodes[0].id = "producer-team".to_string();
        graph.nodes[0].idempotency_key = "producer-team-key".to_string();
        graph.nodes.push(consumer);
        graph.node_statuses = BTreeMap::from([
            ("producer-team".to_string(), ExecutionNodeStatus::Planned),
            ("consumer-team".to_string(), ExecutionNodeStatus::Planned),
        ]);
        graph.edges = vec![ExecutionEdge {
            from: "producer-team".to_string(),
            to: "consumer-team".to_string(),
            kind: harness_contract::execution_graph::ExecutionEdgeKind::CrossTeamHandoff,
        }];
        graph.orchestration = Some(ExecutionOrchestrationMetadata {
            mutation_id: "retire-team-test".to_string(),
            applied_mutation_ids: Vec::new(),
            collaboration_escalations: Vec::new(),
            semantic_revision: 1,
            source_generation: 1,
            completion: harness_contract::execution_graph::ExecutionCompletionContract {
                required_node_ids: vec!["producer-team".to_string(), "consumer-team".to_string()],
                ..Default::default()
            },
            collaboration_program: Some(CollaborationProgram {
                program_id: "program-retire-team".to_string(),
                revision: 1,
                required_team_count: 2,
                team_instances: vec![
                    CollaborationTeamInstance {
                        instance_id: "producer:1".to_string(),
                        semantic_node_id: "producer".to_string(),
                        required: true,
                    },
                    CollaborationTeamInstance {
                        instance_id: "consumer:1".to_string(),
                        semantic_node_id: "consumer".to_string(),
                        required: true,
                    },
                ],
                edges: vec![CollaborationProgramEdge {
                    edge_id: "producer:1->consumer:1".to_string(),
                    from: "producer:1".to_string(),
                    to: "consumer:1".to_string(),
                    kind: CollaborationEdgeKind::Handoff,
                    input_contract: Default::default(),
                    state: Default::default(),
                    delivery_receipt: None,
                    claim_receipt: None,
                }],
                semantic_node_instances: BTreeMap::from([
                    ("producer".to_string(), vec!["producer-team".to_string()]),
                    ("consumer".to_string(), vec!["consumer-team".to_string()]),
                ]),
                control: harness_contract::execution_graph::CollaborationProgramControlState {
                    lifecycle: CollaborationProgramLifecycle::Planning,
                    ..Default::default()
                },
                semantic_intent: None,
            }),
        });
        let mut started_graph = graph.clone();
        started_graph.id = "retire-team-started-root".to_string();
        let mut active_graph = graph.clone();
        active_graph.id = "retire-team-active-root".to_string();
        let active_program = active_graph
            .orchestration
            .as_mut()
            .and_then(|metadata| metadata.collaboration_program.as_mut())
            .expect("Program");
        active_program.control =
            harness_contract::execution_graph::CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Admitting,
                obligations: vec![
                    harness_contract::execution_graph::TeamAdmissionObligation {
                        instance_id: "producer:1".to_string(),
                        binding_ref: "team-binding:sha256:producer".to_string(),
                        state: harness_contract::execution_graph::TeamAdmissionState::Admitting,
                        child_graph_ref: None,
                        reason_kind: None,
                        terminal: None,
                        reservation:
                            harness_contract::execution_graph::TeamAdmissionResourceReservation {
                                context_reservation_tokens: 30,
                                output_reservation_tokens: 20,
                                parallel_demand: 1,
                            },
                        revision: 1,
                    },
                    harness_contract::execution_graph::TeamAdmissionObligation {
                        instance_id: "consumer:1".to_string(),
                        binding_ref: "team-binding:sha256:consumer".to_string(),
                        state: harness_contract::execution_graph::TeamAdmissionState::Admitting,
                        child_graph_ref: None,
                        reason_kind: None,
                        terminal: None,
                        reservation:
                            harness_contract::execution_graph::TeamAdmissionResourceReservation {
                                context_reservation_tokens: 10,
                                output_reservation_tokens: 5,
                                parallel_demand: 1,
                            },
                        revision: 1,
                    },
                ],
                resource_ledger: harness_contract::execution_graph::ProgramResourceLedger {
                    context_reservation_tokens: 40,
                    output_reservation_tokens: 25,
                    parallel_demand: 2,
                    deadline_at_ms: 1,
                    confidence_basis_points: 10_000,
                    revision: 1,
                },
                waiting_relation: Some("team_admission".to_string()),
                blocker_ref: None,
                next_action: Some("admit_exact_team_bindings".to_string()),
            };
        let registered = service.register_graph(graph).expect("register graph").graph;
        let started_registered = service
            .register_graph(started_graph)
            .expect("register started graph")
            .graph;
        let active_registered = service
            .register_graph(active_graph)
            .expect("register active graph")
            .graph;
        let patch = |confirmation: Option<&str>| {
            Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                program_id: "program-retire-team".to_string(),
                base_revision: 1,
                source_attempt: "producer-team:attempt:0".to_string(),
                reason: "the bounded consumer branch is no longer required".to_string(),
                evidence_refs: Vec::new(),
                canonical_digest: "r".repeat(64),
                user_confirmation_ref: confirmation.map(str::to_string),
                escalation: None,
                operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::RetireTeam {
                    instance_id: "consumer:1".to_string(),
                },
            })
        };
        let missing_confirmation = service.apply_command(
            &registered,
            &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
                expected_revision: registered.revision,
                patch: patch(None),
            },
        );
        assert!(matches!(
            missing_confirmation,
            Err(ExecutionCommitError::InvalidCommand(message))
                if message.contains("explicit user confirmation")
        ));

        let retired = service
            .apply_command(
                &registered,
                &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
                    expected_revision: registered.revision,
                    patch: patch(Some("approval:retire-consumer")),
                },
            )
            .expect("confirmed pending Team retires atomically")
            .graph;
        let program = retired
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("program");
        assert_eq!(program.revision, 2);
        assert_eq!(program.required_team_count, 1);
        assert_eq!(program.team_instances.len(), 1);
        assert_eq!(program.team_instances[0].instance_id, "producer:1");
        assert!(program.edges.is_empty());
        assert!(!program.semantic_node_instances.contains_key("consumer"));
        assert_eq!(
            retired.node_statuses["consumer-team"],
            ExecutionNodeStatus::Cancelled
        );
        assert!(retired.edges.is_empty());
        assert_eq!(
            retired
                .orchestration
                .as_ref()
                .expect("metadata")
                .completion
                .required_node_ids,
            vec!["producer-team"]
        );
        assert!(validate_execution_graph(&retired).is_ok());

        let mut topology_graph = registered.clone();
        topology_graph.id = "retire-team-topology-root".to_string();
        let topology_registered = service
            .register_graph(topology_graph)
            .expect("register topology graph")
            .graph;
        let mut replacement = topology_registered.nodes[0].clone();
        replacement.id = "replacement-team".to_string();
        replacement.idempotency_key = "replacement-team-key".to_string();
        let replacement_program = CollaborationProgram {
            program_id: "program-retire-team".to_string(),
            revision: 1,
            required_team_count: 1,
            team_instances: vec![CollaborationTeamInstance {
                instance_id: "replacement:1".to_string(),
                semantic_node_id: "replacement".to_string(),
                required: true,
            }],
            edges: Vec::new(),
            semantic_node_instances: BTreeMap::from([(
                "replacement".to_string(),
                vec!["replacement-team".to_string()],
            )]),
            control: Default::default(),
            semantic_intent: None,
        };
        let replaced = service
            .replan_semantic_with_retirements(
                &topology_registered,
                vec![replacement],
                Vec::new(),
                "split consumer into a replacement workstream".to_string(),
                "replace-consumer-with-replacement".to_string(),
                harness_contract::execution_graph::ExecutionCompletionContract {
                    required_node_ids: vec!["replacement-team".to_string()],
                    ..Default::default()
                },
                Some(replacement_program),
                None,
                vec!["consumer:1".to_string()],
            )
            .expect("topology replacement commits atomically")
            .graph;
        let replaced_program = replaced
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("Program");
        assert!(replaced_program
            .team_instances
            .iter()
            .all(|instance| instance.instance_id != "consumer:1"));
        assert!(replaced_program
            .semantic_node_instances
            .contains_key("replacement"));
        assert_eq!(
            replaced.node_statuses["consumer-team"],
            ExecutionNodeStatus::Cancelled
        );
        assert_eq!(
            replaced.node_statuses["replacement-team"],
            ExecutionNodeStatus::Planned
        );
        assert!(replaced.edges.is_empty());
        assert_eq!(
            replaced
                .orchestration
                .as_ref()
                .expect("metadata")
                .completion
                .required_node_ids,
            vec!["producer-team".to_string(), "replacement-team".to_string()]
        );
        assert!(validate_execution_graph(&replaced).is_ok());

        let active_retired = service
            .apply_command(
                &active_registered,
                &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
                    expected_revision: active_registered.revision,
                    patch: patch(Some("approval:retire-consumer")),
                },
            )
            .expect("active Team retirement releases its exact reservation")
            .graph;
        let active_ledger = &active_retired
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("Program")
            .control
            .resource_ledger;
        assert_eq!(active_ledger.context_reservation_tokens, 30);
        assert_eq!(active_ledger.output_reservation_tokens, 20);
        assert_eq!(active_ledger.parallel_demand, 1);

        let started = service
            .transition_node(
                &started_registered,
                "consumer-team",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .expect("consumer becomes ready")
            .graph;
        let started_rejection = service.apply_command(
            &started,
            &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
                expected_revision: started.revision,
                patch: patch(Some("approval:retire-consumer")),
            },
        );
        assert!(matches!(
            started_rejection,
            Err(ExecutionCommitError::InvalidCommand(message)) if message.contains("requires a planned Team")
        ));
    }

    #[test]
    fn objective_narrowing_rewrites_only_a_planned_team_request_atomically() {
        use harness_contract::execution_graph::{
            CollaborationProgram, CollaborationProgramLifecycle, CollaborationTeamInstance,
            ExecutionGraphCommand, ExecutionOrchestrationMetadata,
        };

        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let service = ExecutionCommitService::new(store);
        let mut graph = waiting_child_join_graph();
        graph
            .node_statuses
            .insert("child-team".to_string(), ExecutionNodeStatus::Planned);
        graph.node_results.clear();
        graph.orchestration = Some(ExecutionOrchestrationMetadata {
            mutation_id: "narrow-objective-test".to_string(),
            applied_mutation_ids: Vec::new(),
            collaboration_escalations: Vec::new(),
            semantic_revision: 1,
            source_generation: 1,
            completion: Default::default(),
            collaboration_program: Some(CollaborationProgram {
                program_id: "program-narrow-objective".to_string(),
                revision: 1,
                required_team_count: 1,
                team_instances: vec![CollaborationTeamInstance {
                    instance_id: "research:1".to_string(),
                    semantic_node_id: "research".to_string(),
                    required: true,
                }],
                edges: Vec::new(),
                semantic_node_instances: BTreeMap::from([(
                    "research".to_string(),
                    vec!["child-team".to_string()],
                )]),
                control: harness_contract::execution_graph::CollaborationProgramControlState {
                    lifecycle: CollaborationProgramLifecycle::Planning,
                    ..Default::default()
                },
                semantic_intent: None,
            }),
        });
        let registered = service.register_graph(graph).expect("register graph").graph;
        let prioritised = service
            .apply_command(
                &registered,
                &ExecutionGraphCommand::ApplyCollaborationParallelismHint {
                    expected_revision: registered.revision,
                    patch: Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                        program_id: "program-narrow-objective".to_string(),
                        base_revision: 1,
                        source_attempt: "child-team:attempt:0".to_string(),
                        reason: "the independent evidence lane should be scheduled first".to_string(),
                        evidence_refs: Vec::new(),
                        canonical_digest: "p".repeat(64),
                        user_confirmation_ref: None,
                        escalation: None,
                        operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::SetParallelismHint {
                            semantic_node_id: "research".to_string(),
                            parallelism_hint: 200,
                        },
                    }),
                },
            )
            .expect("planned Team soft priority updates atomically")
            .graph;
        assert_eq!(
            prioritised.nodes[0]
                .work
                .as_ref()
                .expect("Team has a work contract")
                .scheduling_priority,
            200
        );
        assert_eq!(
            prioritised.node_statuses["child-team"],
            ExecutionNodeStatus::Planned
        );
        assert_eq!(
            prioritised
                .orchestration
                .as_ref()
                .expect("metadata")
                .collaboration_program
                .as_ref()
                .expect("Program")
                .control
                .resource_ledger
                .parallel_demand,
            0,
            "soft priority must not become a resource reservation"
        );
        let reprioritised = service
            .apply_command(
                &prioritised,
                &ExecutionGraphCommand::ApplyCollaborationParallelismHint {
                    expected_revision: prioritised.revision,
                    patch: Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                        program_id: "program-narrow-objective".to_string(),
                        base_revision: 2,
                        source_attempt: "child-team:attempt:0".to_string(),
                        reason: "the verified evidence lane is now urgent".to_string(),
                        evidence_refs: Vec::new(),
                        canonical_digest: "r".repeat(64),
                        user_confirmation_ref: None,
                        escalation: None,
                        operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::Reprioritize {
                            semantic_node_id: "research".to_string(),
                            priority: 240,
                        },
                    }),
                },
            )
            .expect("planned Team reprioritizes atomically")
            .graph;
        assert_eq!(
            reprioritised.nodes[0]
                .work
                .as_ref()
                .expect("Team has a work contract")
                .scheduling_priority,
            240
        );
        let patch = harness_contract::execution_graph::CollaborationIntentPatch {
            program_id: "program-narrow-objective".to_string(),
            base_revision: 3,
            source_attempt: "child-team:attempt:0".to_string(),
            reason: "the user constrained this branch to a single source".to_string(),
            evidence_refs: Vec::new(),
            canonical_digest: "n".repeat(64),
            user_confirmation_ref: None,
            escalation: None,
            operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::NarrowObjective {
                semantic_node_id: "research".to_string(),
                objective: "inspect only the declared source and report its evidence".to_string(),
            },
        };
        let narrowed = service
            .apply_command(
                &reprioritised,
                &ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing {
                    expected_revision: reprioritised.revision,
                    patch: Box::new(patch),
                },
            )
            .expect("planned Team objective narrows atomically")
            .graph;
        let request = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &narrowed.nodes[0].payload_ref,
        )
        .expect("Team request stays decodable");
        assert_eq!(
            request.objective,
            "inspect only the declared source and report its evidence"
        );
        assert_eq!(
            narrowed
                .orchestration
                .as_ref()
                .expect("metadata")
                .collaboration_program
                .as_ref()
                .expect("program")
                .revision,
            4
        );
        assert_eq!(
            narrowed.node_statuses["child-team"],
            ExecutionNodeStatus::Planned
        );
        let expanded = service
            .apply_command(
                &narrowed,
                &ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing {
                    expected_revision: narrowed.revision,
                    patch: Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                        program_id: "program-narrow-objective".to_string(),
                        base_revision: 4,
                        source_attempt: "child-team:attempt:0".to_string(),
                        reason: "the user approved comparison of one additional source".to_string(),
                        evidence_refs: Vec::new(),
                        canonical_digest: "x".repeat(64),
                        user_confirmation_ref: Some("approval:objective-expand".to_string()),
                        escalation: None,
                        operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::ExpandObjective {
                            semantic_node_id: "research".to_string(),
                            objective: "compare the declared source with the approved second source".to_string(),
                        },
                    }),
                },
            )
            .expect("confirmed objective expansion preserves the same Team contract")
            .graph;
        let expanded_request = serde_json::from_str::<
            harness_contract::team::TeamInstantiationRequest,
        >(&expanded.nodes[0].payload_ref)
        .expect("expanded Team request stays decodable");
        assert_eq!(
            expanded_request.objective,
            "compare the declared source with the approved second source"
        );
        assert_eq!(
            expanded
                .orchestration
                .as_ref()
                .expect("metadata")
                .collaboration_program
                .as_ref()
                .expect("program")
                .revision,
            5
        );
        assert!(validate_execution_graph(&narrowed).is_ok());
        assert!(validate_execution_graph(&expanded).is_ok());
    }

    #[test]
    fn terminal_producer_without_required_cross_team_facts_blocks_edge_durably() {
        use harness_contract::acceptance::TerminalFactKind;
        use harness_contract::execution_graph::{
            CollaborationEdgeKind, CollaborationProgram, CollaborationProgramEdge,
            CollaborationProgramLifecycle, CollaborationTeamInstance, CrossTeamInputContract,
            ExecutionOrchestrationMetadata,
        };

        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let service = ExecutionCommitService::new(store);
        let mut graph = agent_task_graph();
        graph.id = "cross-team-blocked-root".to_string();
        let mut consumer = graph.nodes[0].clone();
        consumer.id = "consumer-team".to_string();
        consumer.idempotency_key = "consumer-team-key".to_string();
        graph.nodes[0].id = "producer-team".to_string();
        graph.nodes[0].idempotency_key = "producer-team-key".to_string();
        graph.nodes.push(consumer);
        graph.node_statuses = BTreeMap::from([
            ("producer-team".to_string(), ExecutionNodeStatus::Planned),
            ("consumer-team".to_string(), ExecutionNodeStatus::Planned),
        ]);
        graph.orchestration = Some(ExecutionOrchestrationMetadata {
            mutation_id: "cross-team-blocked-test".to_string(),
            applied_mutation_ids: Vec::new(),
            collaboration_escalations: Vec::new(),
            semantic_revision: 1,
            source_generation: 1,
            completion: Default::default(),
            collaboration_program: Some(CollaborationProgram {
                program_id: "program-cross-team-blocked".to_string(),
                revision: 1,
                required_team_count: 2,
                team_instances: vec![
                    CollaborationTeamInstance {
                        instance_id: "producer:1".to_string(),
                        semantic_node_id: "producer".to_string(),
                        required: true,
                    },
                    CollaborationTeamInstance {
                        instance_id: "consumer:1".to_string(),
                        semantic_node_id: "consumer".to_string(),
                        required: true,
                    },
                ],
                edges: vec![CollaborationProgramEdge {
                    edge_id: "producer:1->consumer:1".to_string(),
                    from: "producer:1".to_string(),
                    to: "consumer:1".to_string(),
                    kind: CollaborationEdgeKind::Handoff,
                    input_contract: CrossTeamInputContract {
                        required_artifact_kinds: Vec::new(),
                        required_fact_kinds: vec![TerminalFactKind::Artifact],
                        require_committed_effect: false,
                        require_satisfied_acceptance: false,
                    },
                    state: Default::default(),
                    delivery_receipt: None,
                    claim_receipt: None,
                }],
                semantic_node_instances: BTreeMap::from([
                    ("producer".to_string(), vec!["producer-team".to_string()]),
                    ("consumer".to_string(), vec!["consumer-team".to_string()]),
                ]),
                control: harness_contract::execution_graph::CollaborationProgramControlState {
                    lifecycle: CollaborationProgramLifecycle::Planning,
                    ..Default::default()
                },
                semantic_intent: None,
            }),
        });
        let registered = service.register_graph(graph).expect("register graph").graph;
        let ready = service
            .transition_node(
                &registered,
                "producer-team",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .expect("producer ready")
            .graph;
        let running = service
            .transition_node(
                &ready,
                "producer-team",
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .expect("producer running")
            .graph;
        let completed = service
            .transition_node(
                &running,
                "producer-team",
                ExecutionNodeStatus::Completed,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: Some("artifact://producer-result".to_string()),
                    summary: Some("producer omitted the required artifact fact".to_string()),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                }),
                Vec::new(),
            )
            .expect("producer completed")
            .graph;
        let edge = &completed
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("program")
            .edges[0];
        assert_eq!(
            edge.state,
            harness_contract::execution_graph::CrossTeamEdgeState::Blocked
        );
        assert!(edge.delivery_receipt.is_none());
        assert!(edge.claim_receipt.is_none());
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
