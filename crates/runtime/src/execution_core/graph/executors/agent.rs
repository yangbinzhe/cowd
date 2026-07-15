use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
};

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};
use crate::validate_agent_return;

#[async_trait]
pub trait AgentTaskBackend: Send + Sync {
    async fn execute(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String>;
}

pub trait AgentTaskBackendResolver: Send + Sync {
    fn resolve(&self, packet: &AgentTaskPacket) -> Option<Arc<dyn AgentTaskBackend>>;
}

/// Stable AgentTask executor resolved from the persistent packet wire.
pub struct AgentTaskExecutor {
    resolvers: RwLock<Vec<Arc<dyn AgentTaskBackendResolver>>>,
}

impl AgentTaskExecutor {
    pub const KIND: &'static str = "agent_task";

    #[must_use]
    pub fn new() -> Self {
        Self {
            resolvers: RwLock::new(Vec::new()),
        }
    }

    pub fn install_resolver(&self, resolver: Arc<dyn AgentTaskBackendResolver>) {
        self.resolvers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(resolver);
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
            attempt: context.attempt,
            idempotency_key: packet.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let packet: AgentTaskPacket =
            serde_json::from_str(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("persistent AgentTaskPacket is invalid: {error}"),
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
        let returned =
            backend
                .execute(packet.clone())
                .await
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
        validate_agent_return(&packet, &returned).map_err(|reason| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason: reason.to_string(),
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
            evidence_refs: returned.evidence_refs,
            failure,
            usage: ExecutionUsage {
                model: (!returned.model.trim().is_empty()).then(|| returned.model.clone()),
                input_tokens: returned.input_tokens,
                output_tokens: returned.output_tokens,
                tool_calls: returned.tool_calls,
                ..ExecutionUsage::default()
            },
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
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
        packet.run_id.as_str(),
        packet.agent_id.as_str(),
        packet.task_id.as_str(),
        packet.session_id.as_str(),
        packet.graph_id.as_str(),
        packet.node_id.as_str(),
        packet.objective.as_str(),
        packet.idempotency_key.as_str(),
    ]
    .iter()
    .any(|value| value.trim().is_empty())
    {
        return Err("AgentTaskPacket contains an empty required binding".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use harness_contract::context::ContextBudgetLeaseRef;

    use super::*;

    fn task() -> AgentTaskPacket {
        AgentTaskPacket {
            run_id: "run-1".into(),
            agent_id: "agent-1".into(),
            task_id: "task-1".into(),
            session_id: "session-1".into(),
            mission_id: None,
            team_id: None,
            graph_id: "graph-1".into(),
            node_id: "node-1".into(),
            attempt: 1,
            expected_graph_revision: 2,
            objective: "inspect".into(),
            acceptance: vec!["reviewed".into()],
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "permission-1".into(),
            model_lease: "model-1".into(),
            budget_lease: ContextBudgetLeaseRef::new("budget-1", "agent-1", "agent", 1000, 1),
            binding: None,
            managed_invocation: None,
            idempotency_key: "idempotency-1".into(),
        }
    }

    fn returned(task: &AgentTaskPacket) -> AgentReturnPacket {
        AgentReturnPacket {
            run_id: task.run_id.clone(),
            agent_id: task.agent_id.clone(),
            task_id: task.task_id.clone(),
            session_id: task.session_id.clone(),
            mission_id: None,
            team_id: None,
            graph_id: task.graph_id.clone(),
            node_id: task.node_id.clone(),
            attempt: task.attempt,
            expected_graph_revision: task.expected_graph_revision,
            status: AgentTerminalStatus::Completed,
            outcome: "review complete".into(),
            acceptance: vec!["reviewed".into()],
            evidence_refs: Vec::new(),
            changes: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 10,
            output_tokens: 5,
            model: "test".into(),
            provider: "test".into(),
            tool_calls: 0,
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
