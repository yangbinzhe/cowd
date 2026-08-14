use std::sync::{Arc, OnceLock, RwLock};

use async_trait::async_trait;
use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
};

use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError,
};
use crate::validate_agent_return;

#[async_trait]
pub trait AgentTaskBackend: Send + Sync {
    async fn execute(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String>;
    async fn cancel(&self, packet: &AgentTaskPacket) -> Result<(), String>;
    fn cancellation_finalized(&self, packet: &AgentTaskPacket);
}

pub trait AgentTaskBackendResolver: Send + Sync {
    fn resolve(&self, packet: &AgentTaskPacket) -> Option<Arc<dyn AgentTaskBackend>>;
}

/// Stable AgentTask executor resolved from the persistent packet wire.
pub struct AgentTaskExecutor {
    resolvers: RwLock<Vec<Arc<dyn AgentTaskBackendResolver>>>,
    state_store: RwLock<Option<ExecutionGraphStateStore>>,
    path_identity_resolver: OnceLock<Arc<crate::path_identity::WorkspacePathIdentityResolver>>,
}

impl AgentTaskExecutor {
    pub const KIND: &'static str = "agent_task";

    #[must_use]
    pub fn new() -> Self {
        Self {
            resolvers: RwLock::new(Vec::new()),
            state_store: RwLock::new(None),
            path_identity_resolver: OnceLock::new(),
        }
    }

    #[must_use]
    pub fn with_state_store(self, state_store: ExecutionGraphStateStore) -> Self {
        *self
            .state_store
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(state_store);
        self
    }

    pub fn install_resolver(&self, resolver: Arc<dyn AgentTaskBackendResolver>) {
        self.resolvers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(resolver);
    }

    pub(crate) fn bind_path_identity_resolver(
        &self,
        resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    ) -> Result<(), String> {
        self.path_identity_resolver
            .set(resolver)
            .map_err(|_| "AgentTaskExecutor path identity resolver is already bound".to_string())
    }
}

impl Default for AgentTaskExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeExecutor for AgentTaskExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn supports_resumable_pause(&self) -> bool {
        // AgentRuntime owns a terminal run identity. Its Cancel command cannot
        // be resumed as the same run, so GraphRunner must reject active Pause
        // instead of publishing a false Paused state.
        false
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind != Self::KIND {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "AgentTask must use the canonical agent_task executor".into(),
            });
        }
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        let packet: AgentTaskPacket =
            serde_json::from_str(&context.node.payload_ref).map_err(|error| {
                NodeExecutorError::Start {
                    node_id: context.node.id.clone(),
                    reason: format!("canonical AgentTaskPacket payload is invalid: {error}"),
                }
            })?;
        validate_packet(&packet).map_err(|reason| NodeExecutorError::Start {
            node_id: context.node.id.clone(),
            reason,
        })?;
        if packet.expected_graph_revision > context.graph.revision {
            return Err(NodeExecutorError::Start {
                node_id: context.node.id.clone(),
                reason: "agent packet revision or attempt binding is stale".into(),
            });
        }
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: Self::KIND.into(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: packet.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let mut packet: AgentTaskPacket =
            serde_json::from_str(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("persistent AgentTaskPacket is invalid: {error}"),
            })?;
        if packet.team_id().is_some() {
            let state_store = self
                .state_store
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .ok_or_else(|| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: "Team AgentTask execution has no durable graph state reader".into(),
                })?;
            let graph = state_store
                .load_async(ticket.graph_id.clone())
                .await
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("load Team predecessor evidence: {error}"),
                })?;
            let predecessor_ids = graph
                .edges
                .iter()
                .filter(|edge| {
                    edge.to == ticket.node_id
                        && edge.kind
                            == harness_contract::execution_graph::ExecutionEdgeKind::DependsOn
                        && graph.nodes.iter().any(|node| {
                            node.id == edge.from
                                && node.kind
                                    == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                        })
                })
                .map(|edge| edge.from.clone())
                .collect::<Vec<_>>();
            let mut upstream_changes = Vec::new();
            for predecessor_id in predecessor_ids {
                if graph.node_statuses.get(&predecessor_id) != Some(&ExecutionNodeStatus::Completed)
                {
                    return Err(NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: format!(
                            "Team predecessor `{predecessor_id}` is not durably completed"
                        ),
                    });
                }
                let predecessor = graph.node_results.get(&predecessor_id).ok_or_else(|| {
                    NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: format!(
                            "Team predecessor `{predecessor_id}` has no committed result"
                        ),
                    }
                })?;
                packet.evidence_refs.extend(
                    predecessor
                        .evidence_refs
                        .iter()
                        .filter(|evidence| evidence.is_durable())
                        .cloned(),
                );
                upstream_changes.extend(predecessor.evidence_refs.iter().filter_map(|evidence| {
                    (evidence.evidence_ref.ref_type == "runtime_change")
                        .then(|| evidence.evidence_ref.id.clone())
                }));
            }
            packet.evidence_refs.sort_by(|left, right| {
                left.evidence_ref
                    .ref_type
                    .cmp(&right.evidence_ref.ref_type)
                    .then_with(|| left.evidence_ref.id.cmp(&right.evidence_ref.id))
            });
            packet
                .evidence_refs
                .dedup_by(|left, right| left.evidence_ref == right.evidence_ref);
            if !upstream_changes.is_empty() {
                // AgentRuntime materializes the canonical predecessor terminal
                // outcomes exactly once when it starts this task. The graph
                // executor only binds durable evidence and change scopes;
                // copying summaries into the objective here duplicated the
                // same JSON and increased reviewer prompt latency.
                packet.constraints.push(format!(
                    "upstream_committed_evidence_count:{}",
                    packet.evidence_refs.len()
                ));
            }
            upstream_changes.sort();
            upstream_changes.dedup();
            if let Some(resolver) = self.path_identity_resolver.get() {
                for encoded in &upstream_changes {
                    let Ok(change) = serde_json::from_str::<
                        harness_contract::agent::AgentChangeReceipt,
                    >(encoded) else {
                        continue;
                    };
                    let Ok(mut obligation) = resolver
                        .compile_obligation(&format!("verify_upstream_change:{}", change.path))
                    else {
                        continue;
                    };
                    if let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
                        &mut obligation.target
                    {
                        scope.path.observed_revision_or_digest = Some(change.after_sha256);
                    }
                    if packet
                        .required_acceptance
                        .evidence_obligations
                        .iter()
                        .all(|existing| existing.obligation_id != obligation.obligation_id)
                    {
                        packet
                            .required_acceptance
                            .evidence_obligations
                            .push(obligation);
                    }
                }
            }
            packet.constraints.extend(
                upstream_changes
                    .into_iter()
                    .map(|scope| format!("upstream_change_scope:{scope}")),
            );
        }
        let backend = self
            .resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(&packet))
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        let returned =
            backend
                .execute(packet.clone())
                .await
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
        validate_agent_return(&packet, &returned).map_err(|reason| {
            let missing_acceptance = packet
                .acceptance
                .iter()
                .filter(|criterion| {
                    !returned
                        .observed_acceptance
                        .satisfied_criteria
                        .contains(criterion)
                })
                .cloned()
                .collect::<Vec<_>>();
            NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!(
                    "{reason}; missing_acceptance={missing_acceptance:?}; runtime_change_receipts={}; observed_evidence_count={}; unresolved_obligations={:?}",
                    returned.runtime_change_receipts.len(),
                    returned.observed_acceptance.observed_evidence.len(),
                    returned.observed_acceptance.unresolved_obligation_ids,
                ),
            }
        })?;
        let unresolved_tolerated = packet
            .constraints
            .iter()
            .any(|constraint| constraint == "protocol_allows_unresolved:true");
        let status = execution_status_for_agent_terminal(returned.status, unresolved_tolerated);
        let failure = (status != ExecutionNodeStatus::Completed)
            .then(|| returned.failure.clone())
            .flatten()
            .map(|message| ExecutionFailure {
                kind: "agent_backend".into(),
                message,
                retryable: false,
                evidence_refs: returned.evidence_refs.clone(),
            });
        let usage = agent_execution_usage(&returned, &packet.required_acceptance);
        let mut evidence_refs = returned.evidence_refs;
        evidence_refs.extend(returned.observed_acceptance.satisfied_criteria.iter().map(
            |criterion| {
                harness_contract::context::EvidenceAccessRef::unavailable(
                    harness_contract::context::EvidenceRef::observed(
                        "runtime_acceptance",
                        acceptance_marker_id(packet.node_id(), criterion),
                    ),
                    "application/vnd.cowd.runtime-acceptance+json",
                    format!("execution-node:{}", packet.node_id()),
                )
            },
        ));
        evidence_refs.extend(
            returned
                .runtime_change_receipts
                .iter()
                .filter_map(|receipt| {
                    let encoded = serde_json::to_string(receipt).ok()?;
                    Some(harness_contract::context::EvidenceAccessRef::unavailable(
                        harness_contract::context::EvidenceRef::observed("runtime_change", encoded),
                        "application/vnd.cowd.runtime-change+json",
                        format!("execution-node:{}", packet.node_id()),
                    ))
                }),
        );
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: Some(format!(
                "agent-return:{}{}",
                returned.run_id,
                if status == ExecutionNodeStatus::Completed
                    && returned.status != AgentTerminalStatus::Completed
                {
                    ":unresolved"
                } else {
                    ""
                }
            )),
            summary: (!returned.outcome.trim().is_empty())
                .then(|| bounded_semantic_summary(&returned.outcome)),
            evidence_refs,
            failure,
            usage,
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }

    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let packet: AgentTaskPacket =
            serde_json::from_str(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("persistent AgentTaskPacket is invalid during cancel: {error}"),
            })?;
        let backend = self
            .resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(&packet))
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        backend
            .cancel(&packet)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }

    fn cancellation_finalized(&self, ticket: &NodeExecutionTicket) {
        let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&ticket.payload_ref) else {
            return;
        };
        let backend = self
            .resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(&packet));
        if let Some(backend) = backend {
            backend.cancellation_finalized(&packet);
        }
    }
}

fn agent_execution_usage(
    returned: &AgentReturnPacket,
    required_acceptance: &harness_contract::context::RequiredAcceptance,
) -> ExecutionUsage {
    ExecutionUsage {
        required_acceptance: required_acceptance.clone(),
        observed_acceptance: returned.observed_acceptance.clone(),
        model: (!returned.model.trim().is_empty()).then(|| returned.model.clone()),
        input_tokens: returned.input_tokens,
        output_tokens: returned.output_tokens,
        cached_tokens: returned.cached_tokens,
        tool_calls: returned.tool_calls,
        duplicate_tool_calls: returned.duplicate_tool_calls,
        max_tool_concurrency_observed: returned.max_tool_concurrency_observed,
        parallel_tool_batches: returned.parallel_tool_batches,
        runtime_write_attempt_paths: returned.runtime_write_attempt_paths.clone(),
        runtime_observed_resource_scopes: Vec::new(),
        ..ExecutionUsage::default()
    }
}

#[must_use]
pub(crate) fn acceptance_marker_id(node_id: &str, criterion: &str) -> String {
    format!("{node_id}:{criterion}")
}

fn bounded_semantic_summary(value: &str) -> String {
    const MAX_CHARS: usize = 2_000;
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_CHARS {
        return normalized;
    }
    let mut summary = normalized.chars().take(MAX_CHARS).collect::<String>();
    summary.push_str(" ...");
    summary
}

fn execution_status_for_agent_terminal(
    status: AgentTerminalStatus,
    unresolved_tolerated: bool,
) -> ExecutionNodeStatus {
    match status {
        AgentTerminalStatus::Completed => ExecutionNodeStatus::Completed,
        AgentTerminalStatus::Failed | AgentTerminalStatus::Blocked if unresolved_tolerated => {
            ExecutionNodeStatus::Completed
        }
        AgentTerminalStatus::Failed => ExecutionNodeStatus::Failed,
        AgentTerminalStatus::Cancelled => ExecutionNodeStatus::Cancelled,
        AgentTerminalStatus::Blocked => ExecutionNodeStatus::Blocked,
    }
}

fn validate_packet(packet: &AgentTaskPacket) -> Result<(), String> {
    if [
        packet.run_id(),
        packet.agent_id(),
        packet.task_id(),
        packet.session_id(),
        packet.graph_id(),
        packet.node_id(),
        packet.objective.as_str(),
        packet.idempotency_key.as_str(),
    ]
    .iter()
    .any(|value| value.trim().is_empty())
    {
        return Err("AgentTaskPacket contains an empty required binding".into());
    }
    if packet.deadline_at_ms == 0 {
        return Err("AgentTaskPacket has no Runtime-issued absolute deadline".into());
    }
    packet.budget_lease.validate().map_err(str::to_string)?;
    if packet.budget_lease.deadline_at_ms != packet.deadline_at_ms {
        return Err("AgentTaskPacket deadline differs from its parent execution budget".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use harness_contract::context::ChildExecutionBudgetReservation;

    use super::*;

    fn task() -> AgentTaskPacket {
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "agent-1",
                "run-1",
                "task-1",
                "session-1",
                "mission-1",
                None,
                "graph-1",
                "node-1",
            ),
            attempt: 1,
            expected_graph_revision: 2,
            policy_revision: 1,
            objective: "inspect".into(),
            required_acceptance: Default::default(),
            acceptance: vec!["reviewed".into()],
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model-1".into(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget-1",
                "agent-1",
                "agent",
                1_000,
                75_000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "idempotency-1".into(),
        }
    }

    fn returned(task: &AgentTaskPacket) -> AgentReturnPacket {
        AgentReturnPacket {
            run_id: task.run_id().to_string(),
            agent_id: task.agent_id().to_string(),
            task_id: task.task_id().to_string(),
            session_id: task.session_id().to_string(),
            mission_id: task.mission_id().to_string(),
            team_id: None,
            graph_id: task.graph_id().to_string(),
            node_id: task.node_id().to_string(),
            attempt: task.attempt,
            expected_graph_revision: task.expected_graph_revision,
            status: AgentTerminalStatus::Completed,
            outcome: "review complete".into(),
            answer_candidate: None,
            observed_acceptance: harness_contract::context::ObservedAcceptance {
                satisfied_criteria: vec!["reviewed".into()],
                observed_evidence: Vec::new(),
                unresolved_obligation_ids: Vec::new(),
            },
            acceptance: vec!["reviewed".into()],
            evidence_refs: Vec::new(),
            changes: Vec::new(),
            runtime_change_receipts: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 10,
            output_tokens: 5,
            cached_tokens: 0,
            model: "test".into(),
            provider: "test".into(),
            tool_calls: 0,
            duplicate_tool_calls: 0,
            max_tool_concurrency_observed: 0,
            parallel_tool_batches: 0,
            runtime_write_attempt_paths: Vec::new(),
            runtime_observed_resource_scopes: Vec::new(),
            failure: None,
        }
    }

    #[test]
    fn rejects_return_packet_with_stale_graph_binding() {
        let task = task();
        let mut returned = returned(&task);
        returned.expected_graph_revision += 1;
        assert_eq!(
            validate_agent_return(&task, &returned).unwrap_err(),
            crate::AgentResultValidationError::BindingMismatch
        );
    }

    #[test]
    fn accepts_complete_bound_return_packet() {
        let task = task();
        validate_agent_return(&task, &returned(&task)).expect("valid return packet");
    }

    #[test]
    fn duplicate_tool_telemetry_survives_the_agent_return_to_graph_usage_boundary() {
        let task = task();
        let mut returned = returned(&task);
        returned.tool_calls = 5;
        returned.duplicate_tool_calls = 2;

        let usage = agent_execution_usage(&returned, &task.required_acceptance);

        assert_eq!(usage.tool_calls, 5);
        assert_eq!(usage.duplicate_tool_calls, 2);
    }

    #[test]
    fn execution_usage_persists_the_exact_runtime_derived_requirement() {
        let mut task = task();
        task.required_acceptance = harness_contract::context::RequiredAcceptance {
            criteria: vec!["reviewed".to_string()],
            evidence_obligations: vec![harness_contract::context::EvidenceObligation {
                obligation_id: "upstream-after-digest".to_string(),
                kind: harness_contract::context::EvidenceObligationKind::VerifyUpstreamChange,
                target: harness_contract::context::EvidenceTargetIdentity::Network {
                    endpoint: "fixture".to_string(),
                },
            }],
        };

        let usage = agent_execution_usage(&returned(&task), &task.required_acceptance);
        assert_eq!(usage.required_acceptance, task.required_acceptance);
    }

    #[test]
    fn unresolved_protocol_role_keeps_graph_path_open_without_rewriting_lifecycle() {
        assert_eq!(
            execution_status_for_agent_terminal(AgentTerminalStatus::Failed, true),
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            execution_status_for_agent_terminal(AgentTerminalStatus::Blocked, true),
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            execution_status_for_agent_terminal(AgentTerminalStatus::Failed, false),
            ExecutionNodeStatus::Failed
        );
        assert_eq!(
            execution_status_for_agent_terminal(AgentTerminalStatus::Cancelled, true),
            ExecutionNodeStatus::Cancelled
        );
    }
}
