use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use harness_contract::execution_graph::ExecutionNodeSpec;

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};

#[async_trait]
pub trait ScopedNodeBackend: Send + Sync {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError>;
    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        Ok(())
    }
}

pub trait ScopedNodeBackendResolver: Send + Sync {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>>;
}

/// A fixed executor that reconstructs its backend through durable-ticket resolvers.
pub struct ScopedNodeExecutor {
    kind: &'static str,
    resolvers: RwLock<Vec<Arc<dyn ScopedNodeBackendResolver>>>,
}

impl ScopedNodeExecutor {
    #[must_use]
    pub fn new(kind: &'static str) -> Self {
        Self {
            kind,
            resolvers: RwLock::new(Vec::new()),
        }
    }

    pub fn install_resolver(&self, resolver: Arc<dyn ScopedNodeBackendResolver>) {
        self.resolvers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(resolver);
    }

    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        self.resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(ticket))
    }
}

#[async_trait]
impl NodeExecutor for ScopedNodeExecutor {
    fn kind(&self) -> &str {
        self.kind
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind == self.kind {
            Ok(())
        } else {
            Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: format!("node must use canonical {} executor", self.kind),
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
            executor_kind: self.kind.to_string(),
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: self.kind.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        backend.execute(ticket).await
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: self.kind.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        backend.after_commit(ticket).await
    }
}
