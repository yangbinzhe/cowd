use std::collections::BTreeSet;
use std::sync::Arc;

use harness_contract::execution_graph::{
    apply_node_transition, validate_execution_graph, ExecutionEdge, ExecutionGraph,
    ExecutionGraphCommand, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
    ExecutionTransitionError,
};
use serde_json::json;
use thiserror::Error;

use crate::runtime_event_store::{
    AppendTransactionReceipt, AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError,
    RuntimeTransactionEventInput, SessionTerminalInput,
};

use super::events::{ExecutionGraphEvent, ExecutionNodeBinding};

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

#[derive(Clone)]
pub struct ExecutionCommitService {
    event_store: Arc<RuntimeEventStore>,
}

impl ExecutionCommitService {
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
        Self { event_store }
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
            graph: next.clone(),
        };
        let node_event = node_transition_event(&next, node_id, from, to, result);
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
            graph: next.clone(),
        };
        let node_event =
            node_transition_event(&next, node_id, from, ExecutionNodeStatus::Running, None);
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
        )];
        events.extend(domain_events);
        if let Some(working_state) = crate::team_working_state::terminal_working_state_event(
            &next,
            node_id,
            to,
            next.node_results.get(node_id),
        ) {
            events.push(working_state);
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
                graph: next.clone(),
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
                graph: next.clone(),
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
            ExecutionGraphCommand::Start { .. } => {}
            ExecutionGraphCommand::SubmitApproval {
                node_id, approved, ..
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
                *status = if *approved {
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
        let mut node_events: Vec<RuntimeTransactionEventInput> = next
            .node_statuses
            .iter()
            .filter_map(|(node_id, to)| {
                let from = graph.node_statuses[node_id];
                (from != *to).then(|| node_transition_event(&next, node_id, from, *to, None))
            })
            .collect();
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
            node_id,
            approved,
            decision_ref,
            ..
        } = command
        {
            let approval_id = decision_ref
                .strip_prefix("approval-decision:")
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "approval decision_ref must use approval-decision:<id>".to_string(),
                    )
                })?;
            let stream_id = format!("approval:{approval_id}");
            node_events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::Approval,
                    kind: "approval.decided".to_string(),
                    status: Some(if *approved { "approved" } else { "denied" }.to_string()),
                    actor: Some("human".to_string()),
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
                        "approved": approved,
                        "decision_ref": decision_ref,
                        "resolved_at_ms": now_ms(),
                    }),
                },
                idempotency_key: Some(decision_ref.clone()),
                schema_version: 1,
            });
        }
        let event = ExecutionGraphEvent::CommandApplied {
            command: command_name.to_string(),
            reason: reason.map(str::to_string),
            graph: next.clone(),
        };
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:command:{}:{}", graph.id, command_name, next.revision),
            event,
            node_events,
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
            .collect();
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:recovery:{}", graph.id, next.revision),
            ExecutionGraphEvent::Recovered {
                recovered_nodes,
                blocked_nodes,
                graph: next.clone(),
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
        if domain_events
            .iter()
            .any(|event| event.event.stream_id == graph.id)
        {
            return Err(ExecutionCommitError::GraphStreamCollision(graph.id.clone()));
        }
        if let Some(event) = domain_events.iter().find(|event| {
            !matches!(
                event.event.scope,
                RuntimeEventScope::ExecutionNode
                    | RuntimeEventScope::Goal
                    | RuntimeEventScope::SessionInput
                    | RuntimeEventScope::Relation
                    | RuntimeEventScope::Team
                    // Approval decisions are generated from a canonical
                    // ExecutionGraph command and must commit atomically with
                    // the node transition. Blocking this scope leaves a
                    // graph approved in neither the graph nor the durable
                    // approval projection.
                    | RuntimeEventScope::Approval
            )
        }) {
            return Err(ExecutionCommitError::ProtectedDomainScope(
                event.event.scope.as_str().to_string(),
            ));
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
                    expected_revision: expected_domain_revision(
                        &self.event_store,
                        &event.event.stream_id,
                        &transaction_id,
                        idempotency_key,
                    )?,
                });
            }
        }
        let graph_input = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: graph.id.clone(),
                scope: RuntimeEventScope::ExecutionGraph,
                kind: graph_event.kind().to_string(),
                status: graph_status(graph).map(str::to_string),
                actor: Some("execution_commit_service".to_string()),
                refs: Vec::new(),
                payload: serde_json::to_value(&graph_event)?,
            },
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
        Ok(ExecutionCommitReceipt {
            graph: committed_graph,
            transaction,
        })
    }
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
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
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
        },
        idempotency_key: Some(format!("{}:{}:{}", graph.id, node_id, graph.revision)),
        schema_version: 1,
    }
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
        | ExecutionGraphCommand::SubmitApproval {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ResolveExternal {
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
        ExecutionGraphCommand::SubmitApproval { .. } => ("submit_approval", None),
        ExecutionGraphCommand::ResolveExternal { .. } => ("resolve_external", None),
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
