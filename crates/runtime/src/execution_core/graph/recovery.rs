use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeStatus};
use thiserror::Error;

use super::commit_service::{ExecutionCommitError, ExecutionCommitService};
use super::registry::{NodeExecutionTicket, NodeExecutorError, NodeExecutorRegistry};
use super::state_store::{ExecutionGraphStateStore, ExecutionStateStoreError};

#[derive(Debug, Error)]
pub enum ExecutionRecoveryError {
    #[error(transparent)]
    State(#[from] ExecutionStateStoreError),
    #[error(transparent)]
    Commit(#[from] ExecutionCommitError),
    #[error(transparent)]
    Executor(#[from] NodeExecutorError),
}

pub struct ExecutionGraphRecovery<'a> {
    state_store: &'a ExecutionGraphStateStore,
    commit_service: &'a ExecutionCommitService,
    registry: &'a NodeExecutorRegistry,
}

impl<'a> ExecutionGraphRecovery<'a> {
    #[must_use]
    pub fn new(
        state_store: &'a ExecutionGraphStateStore,
        commit_service: &'a ExecutionCommitService,
        registry: &'a NodeExecutorRegistry,
    ) -> Self {
        Self {
            state_store,
            commit_service,
            registry,
        }
    }

    pub async fn recover(&self, graph_id: &str) -> Result<ExecutionGraph, ExecutionRecoveryError> {
        let graph = self.state_store.load_async(graph_id).await?;
        let mut next = graph.clone();
        let mut recovered = Vec::new();
        let mut blocked = Vec::new();
        for node in &graph.nodes {
            let status = graph.node_statuses[&node.id];
            if status != ExecutionNodeStatus::Running {
                continue;
            }
            let attempt = graph
                .recovery_cursor
                .node_attempts
                .get(&node.id)
                .copied()
                .unwrap_or(1);
            let Some(executor) = self.registry.get(&node.executor_kind) else {
                next.node_statuses
                    .insert(node.id.clone(), ExecutionNodeStatus::Blocked);
                blocked.push(node.id.clone());
                continue;
            };
            // A process crash does not consume a logical retry. Reattach the same
            // idempotent attempt; the backend must deduplicate any completed side effect.
            let ticket = NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                attempt,
                idempotency_key: node.idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            executor.recover(&ticket).await?;
            next.node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Ready);
            recovered.push(node.id.clone());
        }
        if recovered.is_empty() && blocked.is_empty() {
            return Ok(graph);
        }
        Ok(self
            .commit_service
            .commit_recovery_async(graph, next, recovered, blocked)
            .await?
            .graph)
    }
}
