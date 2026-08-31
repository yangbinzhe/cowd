use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{
    apply_node_transition, validate_execution_graph, ExecutionEdge, ExecutionGraph,
    ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionTransitionError, ExecutionWorkBid, ExecutionWorkClaim,
    ExecutionWorkReview, ExecutionWorkReviewPolicy, ExecutionWorkReviewVerdict,
    ExecutionWorkRuntimeState, ExecutionWorkRuntimeStatus,
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

fn work_contract<'a>(
    graph: &'a ExecutionGraph,
    node_id: &str,
) -> Result<&'a harness_contract::execution_graph::ExecutionWorkContract, ExecutionCommitError> {
    graph
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .and_then(|node| node.work.as_ref())
        .or_else(|| graph.autonomous_work.get(node_id))
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "work node `{node_id}` does not exist or has no work contract"
            ))
        })
}

fn validate_work_claimant(
    node_id: &str,
    work: &harness_contract::execution_graph::ExecutionWorkContract,
    claimant_instance_id: &str,
    claimant_role_id: Option<&str>,
    claimant_capabilities: &[String],
) -> Result<(), ExecutionCommitError> {
    if claimant_instance_id.trim().is_empty() {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "work `{node_id}` claimant identity is empty"
        )));
    }
    if !work.eligibility.allowed_agent_instance_ids.is_empty()
        && !work
            .eligibility
            .allowed_agent_instance_ids
            .iter()
            .any(|id| id == claimant_instance_id)
    {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "claimant `{claimant_instance_id}` is not eligible for work `{node_id}`"
        )));
    }
    if !work.eligibility.allowed_role_ids.is_empty()
        && claimant_role_id.is_none_or(|role| {
            !work
                .eligibility
                .allowed_role_ids
                .iter()
                .any(|allowed| allowed == role)
        })
    {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "claimant role is not eligible for work `{node_id}`"
        )));
    }
    if work
        .eligibility
        .required_capabilities
        .iter()
        .any(|required| {
            !claimant_capabilities
                .iter()
                .any(|actual| actual == required)
        })
    {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "claimant lacks a required capability for work `{node_id}`"
        )));
    }
    Ok(())
}

fn claimed_work_state_mut<'a>(
    graph: &'a mut ExecutionGraph,
    node_id: &str,
    claim_token: &str,
) -> Result<&'a mut ExecutionWorkRuntimeState, ExecutionCommitError> {
    let state = graph.work_states.get_mut(node_id).ok_or_else(|| {
        ExecutionCommitError::InvalidCommand(format!("work `{node_id}` has no runtime state"))
    })?;
    if state.status != ExecutionWorkRuntimeStatus::Claimed
        || state
            .claim
            .as_ref()
            .is_none_or(|claim| claim.claim_token != claim_token)
    {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "work `{node_id}` claim token is stale or does not own the current lease"
        )));
    }
    Ok(state)
}

fn validate_reviewer(
    graph: &ExecutionGraph,
    node_id: &str,
    reviewer_instance_id: &str,
    reviewer_role_id: Option<&str>,
) -> Result<(), ExecutionCommitError> {
    if reviewer_instance_id.trim().is_empty() {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "work `{node_id}` reviewer identity is empty"
        )));
    }
    let state = graph.work_states.get(node_id).ok_or_else(|| {
        ExecutionCommitError::InvalidCommand(format!("work `{node_id}` has no runtime state"))
    })?;
    if state
        .claim
        .as_ref()
        .is_some_and(|claim| claim.claimant_instance_id == reviewer_instance_id)
    {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "work `{node_id}` claimant cannot review its own submission"
        )));
    }
    if let ExecutionWorkReviewPolicy::Peer {
        eligible_role_ids, ..
    } = &work_contract(graph, node_id)?.review_policy
    {
        if !eligible_role_ids.is_empty()
            && reviewer_role_id
                .is_none_or(|role| !eligible_role_ids.iter().any(|eligible| eligible == role))
        {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "work `{node_id}` reviewer role is not eligible"
            )));
        }
    }
    Ok(())
}

fn reconcile_terminal_node_work(
    graph: &mut ExecutionGraph,
    node_id: &str,
    terminal_status: ExecutionNodeStatus,
) {
    if terminal_status != ExecutionNodeStatus::Completed {
        return;
    }
    let Some(work) = graph
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .and_then(|node| node.work.as_ref())
    else {
        return;
    };
    let Some(state) = graph.work_states.get_mut(node_id) else {
        return;
    };
    if state.status != ExecutionWorkRuntimeStatus::Claimed {
        return;
    }
    state.submission_ref = Some(
        graph
            .node_results
            .get(node_id)
            .and_then(|result| result.result_ref.clone())
            .unwrap_or_else(|| format!("execution-node:{node_id}")),
    );
    state.status = match &work.review_policy {
        harness_contract::execution_graph::ExecutionWorkReviewPolicy::None => {
            ExecutionWorkRuntimeStatus::Accepted
        }
        harness_contract::execution_graph::ExecutionWorkReviewPolicy::Peer { .. } => {
            ExecutionWorkRuntimeStatus::Submitted
        }
    };
    state.revision = state.revision.saturating_add(1);
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
            reconcile_terminal_node_work(&mut next, node_id, to);
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
            if transition.outcome.result.status.is_terminal() {
                record_terminal_cross_team_edge_deliveries(&mut next, &transition.node_id)?;
                reconcile_terminal_node_work(
                    &mut next,
                    &transition.node_id,
                    transition.outcome.result.status,
                );
            }
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
            if let Some(working_state) = crate::team_working_state::terminal_working_state_event(
                &next,
                node_id,
                *to,
                next.node_results.get(node_id),
            ) {
                domain_events.push(working_state);
            }
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
            ExecutionGraphCommand::OfferWork { node_id, .. } => {
                work_contract(&next, node_id)?;
                if next.work_states.contains_key(node_id) {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` is already offered"
                    )));
                }
                next.work_states
                    .insert(node_id.clone(), ExecutionWorkRuntimeState::default());
            }
            ExecutionGraphCommand::ProposeWork {
                work_id, contract, ..
            } => {
                let objective = contract.objective.as_deref().unwrap_or_default().trim();
                let proposer = contract.proposed_by.as_deref().unwrap_or_default().trim();
                if work_id.trim().is_empty()
                    || work_id.len() > 192
                    || objective.is_empty()
                    || objective.chars().count() > 2_000
                    || proposer.is_empty()
                    || contract.collaboration_work_id.as_deref() != Some(work_id.as_str())
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "Agent work proposal has an invalid identity, objective or proposer"
                            .to_string(),
                    ));
                }
                if next.autonomous_work.len() >= 64
                    || next.autonomous_work.contains_key(work_id)
                    || next.nodes.iter().any(|node| node.id == *work_id)
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "Agent work proposal `{work_id}` is duplicate or exceeds the 64-item Team bound"
                    )));
                }
                if contract.eligibility.required_capabilities.len() > 16
                    || contract.input_artifact_refs.len() > 32
                    || contract.output_artifact_kinds.is_empty()
                    || contract.output_artifact_kinds.len() > 16
                    || contract.proposal_evidence_refs.len() > 32
                    || contract.expected_input_tokens > 500_000
                    || contract.expected_output_tokens > 500_000
                    || contract.expected_duration_ms > 3_600_000
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "Agent work proposal `{work_id}` exceeds bounded capability, artifact or cost policy"
                    )));
                }
                next.autonomous_work
                    .insert(work_id.clone(), (**contract).clone());
                next.work_states.insert(
                    work_id.clone(),
                    ExecutionWorkRuntimeState {
                        revision: 1,
                        ..ExecutionWorkRuntimeState::default()
                    },
                );
            }
            ExecutionGraphCommand::BidWork {
                work_id,
                bidder_instance_id,
                bidder_role_id,
                bidder_capabilities,
                rationale,
                estimated_cost,
                bid_at_ms,
                ..
            } => {
                let contract = next.autonomous_work.get(work_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "bid target `{work_id}` is not Agent-proposed work"
                    ))
                })?;
                validate_work_claimant(
                    work_id,
                    contract,
                    bidder_instance_id,
                    bidder_role_id.as_deref(),
                    bidder_capabilities,
                )?;
                if contract.proposed_by.as_deref() == Some(bidder_instance_id.as_str()) {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work proposer `{bidder_instance_id}` cannot bid on its own proposal"
                    )));
                }
                let rationale = rationale.trim();
                if rationale.is_empty()
                    || rationale.chars().count() > 1_000
                    || *estimated_cost > 1_000_000
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{work_id}` bid exceeds bounded rationale or cost policy"
                    )));
                }
                let state = next.work_states.get_mut(work_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "work `{work_id}` has no runtime state"
                    ))
                })?;
                if !matches!(
                    state.status,
                    ExecutionWorkRuntimeStatus::Offered | ExecutionWorkRuntimeStatus::Challenged
                ) || state
                    .bids
                    .iter()
                    .any(|bid| bid.bidder_instance_id == *bidder_instance_id)
                    || state.bids.len() >= 64
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{work_id}` is not open for this bid"
                    )));
                }
                state.bids.push(ExecutionWorkBid {
                    bidder_instance_id: bidder_instance_id.clone(),
                    bidder_role_id: bidder_role_id.clone(),
                    rationale: rationale.to_string(),
                    estimated_cost: *estimated_cost,
                    bid_at_ms: *bid_at_ms,
                    graph_revision: graph.revision.saturating_add(1),
                });
                state.revision = state.revision.saturating_add(1);
            }
            ExecutionGraphCommand::ClaimWork {
                node_id,
                claimant_instance_id,
                claimant_role_id,
                claimant_capabilities,
                claimed_at_ms,
                lease_expires_at_ms,
                ..
            } => {
                let contract = work_contract(&next, node_id)?.clone();
                validate_work_claimant(
                    node_id,
                    &contract,
                    claimant_instance_id,
                    claimant_role_id.as_deref(),
                    claimant_capabilities,
                )?;
                if contract.proposed_by.as_deref() == Some(claimant_instance_id.as_str()) {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work proposer `{claimant_instance_id}` cannot claim its own proposal"
                    )));
                }
                if contract.proposed_by.is_some()
                    && next.work_states.get(node_id).is_none_or(|state| {
                        !state
                            .bids
                            .iter()
                            .any(|bid| bid.bidder_instance_id == *claimant_instance_id)
                    })
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "Agent-proposed work `{node_id}` may only be claimed by an attested bidder"
                    )));
                }
                if *lease_expires_at_ms <= *claimed_at_ms {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` claim lease must expire after claim time"
                    )));
                }
                let state = next.work_states.entry(node_id.clone()).or_default();
                let reclaimable = match state.status {
                    ExecutionWorkRuntimeStatus::Offered
                    | ExecutionWorkRuntimeStatus::Challenged => true,
                    ExecutionWorkRuntimeStatus::Claimed => state
                        .claim
                        .as_ref()
                        .is_some_and(|claim| claim.lease_expires_at_ms <= *claimed_at_ms),
                    ExecutionWorkRuntimeStatus::Submitted
                    | ExecutionWorkRuntimeStatus::Accepted => false,
                };
                if !reclaimable {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` is not claimable from {:?}",
                        state.status
                    )));
                }
                let token_seed = format!(
                    "{}:{node_id}:{}:{claimant_instance_id}:{claimed_at_ms}:{lease_expires_at_ms}",
                    graph.id,
                    graph.revision.saturating_add(1)
                );
                let claim_token = format!("{:x}", Sha256::digest(token_seed.as_bytes()));
                state.status = ExecutionWorkRuntimeStatus::Claimed;
                state.claim = Some(ExecutionWorkClaim {
                    claimant_instance_id: claimant_instance_id.clone(),
                    claimant_role_id: claimant_role_id.clone(),
                    claim_token,
                    claimed_at_ms: *claimed_at_ms,
                    heartbeat_at_ms: *claimed_at_ms,
                    lease_expires_at_ms: *lease_expires_at_ms,
                    graph_revision: graph.revision.saturating_add(1),
                });
                state.submission_ref = None;
                state.revision = state.revision.saturating_add(1);
            }
            ExecutionGraphCommand::HeartbeatWork {
                node_id,
                claim_token,
                heartbeat_at_ms,
                lease_expires_at_ms,
                ..
            } => {
                let state = claimed_work_state_mut(&mut next, node_id, claim_token)?;
                let claim = state.claim.as_mut().expect("claimed state has claim");
                if *heartbeat_at_ms > claim.lease_expires_at_ms
                    || *heartbeat_at_ms < claim.heartbeat_at_ms
                    || *lease_expires_at_ms <= *heartbeat_at_ms
                    || *lease_expires_at_ms < claim.lease_expires_at_ms
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` heartbeat has an expired or non-monotonic lease"
                    )));
                }
                claim.heartbeat_at_ms = *heartbeat_at_ms;
                claim.lease_expires_at_ms = *lease_expires_at_ms;
                state.revision = state.revision.saturating_add(1);
            }
            ExecutionGraphCommand::ReleaseWork {
                node_id,
                claim_token,
                ..
            } => {
                let state = claimed_work_state_mut(&mut next, node_id, claim_token)?;
                state.status = ExecutionWorkRuntimeStatus::Offered;
                state.claim = None;
                state.submission_ref = None;
                state.revision = state.revision.saturating_add(1);
            }
            ExecutionGraphCommand::SubmitWork {
                node_id,
                claim_token,
                submitted_at_ms,
                submission_ref,
                ..
            } => {
                if submission_ref.trim().is_empty() {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` submission reference is empty"
                    )));
                }
                let state = claimed_work_state_mut(&mut next, node_id, claim_token)?;
                let claim = state.claim.as_ref().expect("claimed state has claim");
                if *submitted_at_ms < claim.claimed_at_ms
                    || *submitted_at_ms > claim.lease_expires_at_ms
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` submission is outside its fenced lease"
                    )));
                }
                state.status = ExecutionWorkRuntimeStatus::Submitted;
                state.submission_ref = Some(submission_ref.clone());
                state.revision = state.revision.saturating_add(1);
            }
            ExecutionGraphCommand::AcceptWork {
                node_id,
                reviewer_instance_id,
                reviewer_role_id,
                reviewed_at_ms,
                ..
            } => {
                validate_reviewer(
                    &next,
                    node_id,
                    reviewer_instance_id,
                    reviewer_role_id.as_deref(),
                )?;
                let minimum_reviewers = match &work_contract(&next, node_id)?.review_policy {
                    ExecutionWorkReviewPolicy::None => 1,
                    ExecutionWorkReviewPolicy::Peer {
                        minimum_reviewers, ..
                    } => usize::from((*minimum_reviewers).max(1)),
                };
                let state = next.work_states.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` has no runtime state"
                    ))
                })?;
                if state.status != ExecutionWorkRuntimeStatus::Submitted {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` can only be accepted after submission"
                    )));
                }
                let submission_ref = state.submission_ref.clone().ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` submitted state has no durable result reference"
                    ))
                })?;
                if *reviewed_at_ms == 0
                    || state
                        .claim
                        .as_ref()
                        .is_some_and(|claim| *reviewed_at_ms < claim.claimed_at_ms)
                    || state.reviews.len() >= 128
                    || state.reviews.iter().any(|review| {
                        review.submission_ref == submission_ref
                            && review.reviewer_instance_id == *reviewer_instance_id
                    })
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` review is stale, duplicate or exceeds the 128-receipt bound"
                    )));
                }
                state.reviews.push(ExecutionWorkReview {
                    reviewer_instance_id: reviewer_instance_id.clone(),
                    reviewer_role_id: reviewer_role_id.clone(),
                    verdict: ExecutionWorkReviewVerdict::Accept,
                    submission_ref: submission_ref.clone(),
                    finding: None,
                    reviewed_at_ms: *reviewed_at_ms,
                    graph_revision: graph.revision.saturating_add(1),
                });
                let accepted_reviewers = state
                    .reviews
                    .iter()
                    .filter(|review| {
                        review.submission_ref == submission_ref
                            && review.verdict == ExecutionWorkReviewVerdict::Accept
                    })
                    .map(|review| review.reviewer_instance_id.as_str())
                    .collect::<BTreeSet<_>>()
                    .len();
                if accepted_reviewers >= minimum_reviewers {
                    state.status = ExecutionWorkRuntimeStatus::Accepted;
                }
                state.revision = state.revision.saturating_add(1);
            }
            ExecutionGraphCommand::ChallengeWork {
                node_id,
                reviewer_instance_id,
                reviewer_role_id,
                finding,
                reviewed_at_ms,
                ..
            } => {
                validate_reviewer(
                    &next,
                    node_id,
                    reviewer_instance_id,
                    reviewer_role_id.as_deref(),
                )?;
                let finding = finding.trim();
                if finding.is_empty() || finding.chars().count() > 4_000 {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` challenge must contain 1..4000 characters"
                    )));
                }
                let state = next.work_states.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` has no runtime state"
                    ))
                })?;
                if state.status != ExecutionWorkRuntimeStatus::Submitted {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` can only be challenged after submission"
                    )));
                }
                let submission_ref = state.submission_ref.clone().ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` submitted state has no durable result reference"
                    ))
                })?;
                if *reviewed_at_ms == 0
                    || state
                        .claim
                        .as_ref()
                        .is_some_and(|claim| *reviewed_at_ms < claim.claimed_at_ms)
                    || state.reviews.len() >= 128
                    || state.reviews.iter().any(|review| {
                        review.submission_ref == submission_ref
                            && review.reviewer_instance_id == *reviewer_instance_id
                    })
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "work `{node_id}` review is stale, duplicate or exceeds the 128-receipt bound"
                    )));
                }
                state.reviews.push(ExecutionWorkReview {
                    reviewer_instance_id: reviewer_instance_id.clone(),
                    reviewer_role_id: reviewer_role_id.clone(),
                    verdict: ExecutionWorkReviewVerdict::Challenge,
                    submission_ref,
                    finding: Some(finding.to_string()),
                    reviewed_at_ms: *reviewed_at_ms,
                    graph_revision: graph.revision.saturating_add(1),
                });
                state.status = ExecutionWorkRuntimeStatus::Challenged;
                state.claim = None;
                state.review_findings.push(finding.to_string());
                state.revision = state.revision.saturating_add(1);
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
        if !next.autonomous_work.is_empty() {
            validate_execution_graph(&next).map_err(|error| {
                ExecutionCommitError::InvalidCommand(format!(
                    "autonomous work command violates graph invariants: {error}"
                ))
            })?;
        }
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
