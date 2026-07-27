use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionGraph, ExecutionNodeResult, ExecutionNodeSpec,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum NodeExecutorError {
    #[error("executor kind `{0}` is already registered")]
    DuplicateExecutor(String),
    #[error("executor `{executor_kind}` is unavailable for node `{node_id}`")]
    Unavailable {
        executor_kind: String,
        node_id: String,
    },
    #[error("executor rejected node `{node_id}`: {reason}")]
    Invalid { node_id: String, reason: String },
    #[error("executor failed to start node `{node_id}`: {reason}")]
    Start { node_id: String, reason: String },
    #[error("executor failed while awaiting node `{node_id}`: {reason}")]
    Poll { node_id: String, reason: String },
    #[error("executor failed to cancel node `{node_id}`: {reason}")]
    Cancel { node_id: String, reason: String },
    #[error("executor failed to recover node `{node_id}`: {reason}")]
    Recover { node_id: String, reason: String },
    #[error("executor effect for node `{node_id}` has uncertain completion: {reason}")]
    Uncertain { node_id: String, reason: String },
}

#[derive(Debug, Clone)]
pub struct NodeExecutionContext {
    pub graph: Arc<ExecutionGraph>,
    pub node: ExecutionNodeSpec,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExecutionTicket {
    pub graph_id: String,
    pub node_id: String,
    pub executor_kind: String,
    #[serde(default)]
    pub service_class: harness_contract::execution_graph::ExecutionServiceClass,
    pub attempt: u32,
    pub idempotency_key: String,
    pub payload_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExecutionOutcome {
    pub result: ExecutionNodeResult,
    /// Internal graph-commit side effects.  Third-party node executors can
    /// return a result, but cannot construct arbitrary ledger transactions
    /// through the public execution outcome API.
    #[serde(skip)]
    pub(crate) domain_events: Vec<crate::runtime_event_store::RuntimeTransactionEventInput>,
    pub replan: Option<ExecutionGraphReplan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphReplan {
    pub nodes: Vec<ExecutionNodeSpec>,
    pub edges: Vec<ExecutionEdge>,
    pub reason: String,
}

impl NodeExecutionOutcome {
    #[must_use]
    pub fn new(result: ExecutionNodeResult) -> Self {
        Self {
            result,
            domain_events: Vec::new(),
            replan: None,
        }
    }

    #[must_use]
    pub fn with_replan(mut self, replan: ExecutionGraphReplan) -> Self {
        self.replan = Some(replan);
        self
    }
}

#[async_trait]
pub trait NodeExecutor: Send + Sync {
    fn kind(&self) -> &str;
    /// Whether cancelling the current attempt and starting a new attempt on
    /// Resume preserves this executor's durable semantics.
    fn supports_resumable_pause(&self) -> bool {
        true
    }
    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError>;
    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError>;
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError>;
    /// Run process-local publication only after the canonical graph transition
    /// and its domain events have committed successfully.
    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        Ok(())
    }
    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        Ok(())
    }
    /// Release process-local cancellation intent after the graph command has
    /// either committed or definitively failed. Implementations must not
    /// mutate durable graph state from this callback.
    fn cancellation_finalized(&self, _ticket: &NodeExecutionTicket) {}
    async fn recover(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(ticket.clone())
    }
}

#[derive(Default)]
pub struct NodeExecutorRegistry {
    executors: RwLock<BTreeMap<String, Arc<dyn NodeExecutor>>>,
}

impl NodeExecutorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, executor: Arc<dyn NodeExecutor>) -> Result<(), NodeExecutorError> {
        let mut executors = self
            .executors
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let kind = executor.kind().to_string();
        if executors.contains_key(&kind) {
            return Err(NodeExecutorError::DuplicateExecutor(kind));
        }
        executors.insert(kind, executor);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, kind: &str) -> Option<Arc<dyn NodeExecutor>> {
        self.executors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(kind)
            .cloned()
    }

    #[must_use]
    pub fn available_kinds(&self) -> BTreeSet<String> {
        self.executors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    pub fn validate_graph(&self, graph: &ExecutionGraph) -> Result<(), NodeExecutorError> {
        self.validate_nodes(&graph.nodes)
    }

    pub fn validate_nodes(&self, nodes: &[ExecutionNodeSpec]) -> Result<(), NodeExecutorError> {
        for node in nodes {
            let executor =
                self.get(&node.executor_kind)
                    .ok_or_else(|| NodeExecutorError::Unavailable {
                        executor_kind: node.executor_kind.clone(),
                        node_id: node.id.clone(),
                    })?;
            executor.validate(node)?;
        }
        Ok(())
    }
}
