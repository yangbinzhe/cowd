use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use harness_contract::core::TaskRisk;
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
};
use serde::Deserialize;

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};
use crate::{
    ApprovalQueue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, GlobalApprovalStatus,
    SubmitGlobalApprovalRequest,
};

#[derive(Debug, Deserialize)]
struct ApprovalPayload {
    action: String,
    summary: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    team_id: Option<String>,
    #[serde(default)]
    mission_id: Option<String>,
    #[serde(default)]
    evidence_refs: Vec<String>,
}

pub struct ApprovalNodeExecutor {
    queue: Arc<ApprovalQueue>,
}

impl ApprovalNodeExecutor {
    pub const KIND: &'static str = "approval";
    #[must_use]
    pub fn new(queue: Arc<ApprovalQueue>) -> Self {
        Self { queue }
    }
}

/// Stable, unambiguous approval identity for one graph node. Graph and node
/// IDs themselves contain `:` in the canonical Runtime format, so raw string
/// concatenation cannot be parsed safely at Gateway boundaries.
#[must_use]
pub fn graph_approval_id(graph_id: &str, node_id: &str) -> String {
    format!(
        "approval:v1:{}:{}",
        URL_SAFE_NO_PAD.encode(graph_id),
        URL_SAFE_NO_PAD.encode(node_id)
    )
}

/// Decode a Runtime graph approval identity. Legacy colon-concatenated IDs
/// are deliberately rejected: they were ambiguous and could target the wrong
/// graph or node when either identifier contained a colon.
#[must_use]
pub fn parse_graph_approval_id(approval_id: &str) -> Option<(String, String)> {
    let encoded = approval_id.strip_prefix("approval:v1:")?;
    let (graph, node) = encoded.split_once(':')?;
    let graph = String::from_utf8(URL_SAFE_NO_PAD.decode(graph).ok()?).ok()?;
    let node = String::from_utf8(URL_SAFE_NO_PAD.decode(node).ok()?).ok()?;
    (!graph.trim().is_empty() && !node.trim().is_empty()).then_some((graph, node))
}

#[async_trait]
impl NodeExecutor for ApprovalNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }
    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind != Self::KIND {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Approval must use the canonical approval executor".into(),
            });
        }
        serde_json::from_str::<ApprovalPayload>(&node.payload_ref).map_err(|error| {
            NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: format!("invalid approval payload: {error}"),
            }
        })?;
        Ok(())
    }
    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: Self::KIND.into(),
            attempt: context.attempt,
            // Waiting for approval is a completed observation, not the
            // irreversible approved transition. A resumed node must receive
            // a new effect identity so the Runner does not replay the old
            // `WaitingApproval` receipt instead of re-reading the durable
            // approval decision.
            idempotency_key: format!(
                "{}:approval-attempt:{}",
                context.node.idempotency_key, context.attempt
            ),
            payload_ref: context.node.payload_ref,
        })
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        // A graph approval decision is committed atomically with graph state
        // by the Runner. Refresh before inspecting the idempotent request so
        // this executor never reopens an approval that another durable commit
        // has already resolved.
        self.queue.refresh();
        let payload: ApprovalPayload =
            serde_json::from_str(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let approval_id = graph_approval_id(&ticket.graph_id, &ticket.node_id);
        let request = self
            .queue
            .submit_scoped(
                approval_id.clone(),
                SubmitGlobalApprovalRequest {
                    source: ApprovalSource {
                        kind: if payload.agent_id.is_some() {
                            ApprovalSourceKind::Agent
                        } else if payload.team_id.is_some() {
                            ApprovalSourceKind::Team
                        } else if payload.mission_id.is_some() {
                            ApprovalSourceKind::Mission
                        } else {
                            ApprovalSourceKind::Session
                        },
                        session_id: payload.session_id,
                        agent_id: payload.agent_id,
                        team_id: payload.team_id,
                        mission_id: payload.mission_id,
                        resource_ref: None,
                        review_ref: None,
                        application: None,
                    },
                    action: payload.action,
                    summary: payload.summary,
                    risk: TaskRisk::High,
                    evidence_refs: payload.evidence_refs,
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            )
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?;
        let status = match request.status {
            GlobalApprovalStatus::Pending => ExecutionNodeStatus::WaitingApproval,
            GlobalApprovalStatus::Approved => ExecutionNodeStatus::Completed,
            GlobalApprovalStatus::Denied | GlobalApprovalStatus::TimedOut => {
                ExecutionNodeStatus::Blocked
            }
        };
        let failure = (status == ExecutionNodeStatus::Blocked).then(|| ExecutionFailure {
            kind: "approval_denied".into(),
            message: format!("approval `{approval_id}` was not granted"),
            retryable: false,
            evidence_refs: Vec::new(),
        });
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: Some(approval_id),
            summary: failure
                .as_ref()
                .map(|failure| failure.message.clone())
                .or_else(|| Some("Approval decision committed".to_string())),
            evidence_refs: Vec::new(),
            failure,
            usage: ExecutionUsage::default(),
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ApprovalDecisionCommand, RuntimeEventStore};
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind};

    #[tokio::test]
    async fn approval_waits_and_only_completes_after_queue_decision() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let executor = ApprovalNodeExecutor::new(Arc::clone(&queue));
        let mut graph = ExecutionGraph::new("approval");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({"action":"write","summary":"write workspace","session_id":"session-1"}).to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();
        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(waiting.status, ExecutionNodeStatus::WaitingApproval);
        let approval_id = waiting.result_ref.unwrap();
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                ApprovalDecisionCommand {
                    approval_id,
                    approved: true,
                    reason: "reviewed".into(),
                },
            )
            .unwrap();
        let completed = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(completed.status, ExecutionNodeStatus::Completed);
    }
}
