use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use harness_contract::execution_graph::ExecutionNodeSpec;

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};

#[async_trait]
pub trait SynthesizeBackend: Send + Sync {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String>;
    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
        Ok(())
    }
}

pub trait SynthesizeBackendResolver: Send + Sync {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>>;
}

/// The sole publisher of a graph terminal candidate, resolved from its ticket.
pub struct SynthesizeNodeExecutor {
    resolvers: RwLock<Vec<Arc<dyn SynthesizeBackendResolver>>>,
}

impl SynthesizeNodeExecutor {
    pub const KIND: &'static str = "synthesize";
    #[must_use]
    pub fn new() -> Self {
        Self {
            resolvers: RwLock::new(Vec::new()),
        }
    }
    pub fn install_resolver(&self, resolver: Arc<dyn SynthesizeBackendResolver>) {
        self.resolvers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(resolver);
    }

    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        self.resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(ticket))
    }
}
impl Default for SynthesizeNodeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeExecutor for SynthesizeNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }
    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind == Self::KIND {
            Ok(())
        } else {
            Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Synthesize must use canonical synthesize executor".into(),
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
            service_class: context.graph.service_class,
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
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        backend
            .synthesize(ticket)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        backend
            .after_commit(ticket)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }
}
