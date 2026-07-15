use std::collections::BTreeSet;

use async_trait::async_trait;
use harness_contract::context::EvidenceAccessRef;
use harness_contract::execution_graph::{
    ExecutionEdgeKind, ExecutionFailure, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionUsage,
};

use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError,
};

/// Verifies one node from the durable results of its graph predecessors.
///
/// Verification has no caller-managed side channel: recovery and a live run
/// therefore evaluate exactly the same committed evidence.
pub struct VerifyNodeExecutor {
    state_store: ExecutionGraphStateStore,
}

impl VerifyNodeExecutor {
    pub const KIND: &'static str = "verify";

    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore) -> Self {
        Self { state_store }
    }
}

#[async_trait]
impl NodeExecutor for VerifyNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind == Self::KIND {
            Ok(())
        } else {
            Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Verify must use canonical verify executor".into(),
            })
        }
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
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let graph = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == ticket.node_id)
            .ok_or_else(|| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "verify node is absent from its execution graph".into(),
            })?;
        let predecessor_ids = graph
            .edges
            .iter()
            .filter(|edge| {
                edge.to == ticket.node_id
                    && matches!(
                        edge.kind,
                        ExecutionEdgeKind::DependsOn
                            | ExecutionEdgeKind::Produces
                            | ExecutionEdgeKind::Verifies
                    )
            })
            .map(|edge| edge.from.as_str())
            .collect::<Vec<_>>();

        let mut evidence = Vec::<EvidenceAccessRef>::new();
        let mut available = BTreeSet::new();
        let mut incomplete = Vec::new();
        for predecessor_id in predecessor_ids {
            if graph.node_statuses.get(predecessor_id) != Some(&ExecutionNodeStatus::Completed) {
                incomplete.push(predecessor_id.to_string());
                continue;
            }
            if let Some(result) = graph.node_results.get(predecessor_id) {
                if let Some(result_ref) = &result.result_ref {
                    available.insert(result_ref.clone());
                }
                for item in &result.evidence_refs {
                    available.insert(item.evidence_ref.0.id.clone());
                    available.insert(item.retrieval_selector.clone());
                    available.insert(item.sha256.clone());
                    evidence.push(item.clone());
                }
            }
        }

        let missing = node
            .acceptance
            .required_evidence
            .iter()
            .filter(|required| !available.contains(required.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let (status, failure) = if !incomplete.is_empty() || !missing.is_empty() {
            let mut detail = Vec::new();
            if !incomplete.is_empty() {
                detail.push(format!(
                    "incomplete predecessors: {}",
                    incomplete.join(", ")
                ));
            }
            if !missing.is_empty() {
                detail.push(format!("missing required evidence: {}", missing.join(", ")));
            }
            (
                ExecutionNodeStatus::Blocked,
                Some(ExecutionFailure {
                    kind: "missing_evidence".into(),
                    message: detail.join("; "),
                    retryable: true,
                    evidence_refs: evidence.clone(),
                }),
            )
        } else {
            (ExecutionNodeStatus::Completed, None)
        };

        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: (status == ExecutionNodeStatus::Completed).then(|| {
                format!(
                    "verification:{}:{}:satisfied",
                    ticket.graph_id, ticket.node_id
                )
            }),
            summary: failure
                .as_ref()
                .map(|failure| failure.message.clone())
                .or_else(|| Some("Required evidence was verified".to_string())),
            evidence_refs: evidence,
            failure,
            usage: ExecutionUsage::default(),
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_core::graph::ExecutionCommitService;
    use crate::RuntimeEventStore;
    use harness_contract::core::{EvidenceRef, KernelRef};
    use harness_contract::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNodeKind};
    use std::sync::Arc;

    fn evidence(id: &str) -> EvidenceAccessRef {
        EvidenceAccessRef::durable(
            EvidenceRef(KernelRef::new("evidence", id)),
            "sha",
            1,
            "text/plain",
            format!("evidence://{id}"),
            "workspace",
        )
    }

    #[tokio::test]
    async fn verification_reads_committed_predecessor_evidence() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let state = ExecutionGraphStateStore::new(Arc::clone(&store));
        let commit = ExecutionCommitService::new(store);
        let executor = VerifyNodeExecutor::new(state.clone());
        let mut graph = ExecutionGraph::new("verify");
        let source = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool", "tool");
        let mut verify = ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            VerifyNodeExecutor::KIND,
            "verify",
        );
        verify.acceptance.required_evidence = vec!["proof".into()];
        graph.edges.push(ExecutionEdge {
            from: source.id.clone(),
            to: verify.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes = vec![source.clone(), verify.clone()];
        let graph = commit.register_graph(graph).unwrap().graph;
        let graph = commit
            .transition_node(
                &graph,
                &source.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let graph = commit
            .transition_node(
                &graph,
                &source.id,
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        commit
            .transition_node(
                &graph,
                &source.id,
                ExecutionNodeStatus::Completed,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: None,
                    summary: Some("Verification fixture completed".to_string()),
                    evidence_refs: vec![evidence("proof")],
                    failure: None,
                    usage: ExecutionUsage::default(),
                    finished_at_ms: 1,
                }),
                Vec::new(),
            )
            .unwrap();
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(state.load(&graph.id).unwrap()),
                node: verify,
                attempt: 1,
            })
            .await
            .unwrap();
        let result = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(result.status, ExecutionNodeStatus::Completed);
        assert_eq!(result.evidence_refs.len(), 1);
    }
}
