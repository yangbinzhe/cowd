use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::FutureExt;
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
    /// Runtime-authored delivery facts committed atomically with this node.
    /// Third-party executors cannot promote presentation wording into facts;
    /// the graph projection remains the durable replay owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_envelope: Option<harness_contract::outcome::DeliveryEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_presentation: Option<harness_contract::outcome::TerminalPresentation>,
    /// Internal graph-commit side effects.  Third-party node executors can
    /// return a result, but cannot construct arbitrary ledger transactions
    /// through the public execution outcome API.
    #[serde(skip)]
    pub(crate) domain_events: Vec<crate::runtime_event_store::RuntimeTransactionEventInput>,
    #[serde(skip)]
    pub(crate) source_revisions: BTreeMap<String, u64>,
    #[serde(skip)]
    pub(crate) no_dispatch: Option<super::executors::agent_tool::NoDispatchWitness>,
    pub replan: Option<ExecutionGraphReplan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphReplan {
    pub nodes: Vec<ExecutionNodeSpec>,
    pub edges: Vec<ExecutionEdge>,
    pub reason: String,
    /// True when the runtime itself authored this replan (for example the
    /// event-driven Agentic Program barrier). Framework-authored replans are
    /// exempt from the external-wait executor gate in `validate_replan`, because
    /// the graph compiler -- not an executor outcome -- is the producer
    /// (PLAN W1 / C4).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub framework_authored: bool,
}

impl NodeExecutionOutcome {
    #[must_use]
    pub fn new(result: ExecutionNodeResult) -> Self {
        Self {
            result,
            delivery_envelope: None,
            terminal_presentation: None,
            domain_events: Vec::new(),
            source_revisions: BTreeMap::new(),
            no_dispatch: None,
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
    /// Retract process-local preview state when an executor produced an
    /// outcome but the canonical graph transition did not commit.
    async fn after_abort(
        &self,
        _ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), NodeExecutorError> {
        Ok(())
    }
    async fn cancel(&self, _ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        Ok(())
    }
    /// Bounded cancellation. Defaults to the unbounded `cancel`; an executor
    /// that owns physical work overrides this to stop waiting after `budget`
    /// without dropping the physical future (PLAN W3 / R-02).
    async fn cancel_bounded(
        &self,
        ticket: &NodeExecutionTicket,
        _budget: std::time::Duration,
    ) -> Result<(), NodeExecutorError> {
        self.cancel(ticket).await
    }
    /// Physical effects cannot be abandoned by a cancellation propagation timeout.
    fn cancellation_requires_quiescence(&self, _ticket: &NodeExecutionTicket) -> bool {
        false
    }
    /// Called after process-local drivers stop. Finish retained physical work
    /// without inventing a business cancellation or terminal graph result.
    async fn drain_physical_execution(&self) -> Result<(), NodeExecutorError> {
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
    builtin_tool_batch: RwLock<Option<std::sync::Weak<super::executors::ScopedNodeExecutor>>>,
}

impl NodeExecutorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn install_builtin_tool_batch(
        &self,
        owner: &Arc<super::executors::ScopedNodeExecutor>,
    ) -> Result<(), NodeExecutorError> {
        let registered = self
            .get("tool_batch")
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: "tool_batch".into(),
                node_id: String::new(),
            })?;
        let expected: Arc<dyn NodeExecutor> = owner.clone();
        if !Arc::ptr_eq(&registered, &expected) {
            return Err(NodeExecutorError::Invalid {
                node_id: String::new(),
                reason: "builtin ToolBatch owner mismatch".into(),
            });
        }
        let mut slot = self
            .builtin_tool_batch
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_some() {
            return Err(NodeExecutorError::DuplicateExecutor(
                "builtin tool_batch owner".into(),
            ));
        }
        *slot = Some(Arc::downgrade(owner));
        Ok(())
    }

    fn builtin_tool_batch(&self) -> Option<Arc<super::executors::ScopedNodeExecutor>> {
        let owner = self
            .builtin_tool_batch
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()?
            .upgrade()?;
        let expected: Arc<dyn NodeExecutor> = owner.clone();
        self.get("tool_batch")
            .filter(|registered| Arc::ptr_eq(registered, &expected))?;
        Some(owner)
    }

    pub(super) fn observe_builtin_binding(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<Option<Arc<super::executors::agent_tool::NoDispatchObservation>>, NodeExecutorError>
    {
        if ticket.executor_kind != "tool_batch"
            || super::executors::agent_tool::no_dispatch_calls(ticket).is_err()
        {
            return Ok(None);
        }
        self.builtin_tool_batch()
            .map(|owner| owner.observe_binding(ticket))
            .transpose()
    }

    pub(super) fn rollback_unadmitted_builtin_binding(
        &self,
        ticket: &NodeExecutionTicket,
        observation: &Arc<super::executors::agent_tool::NoDispatchObservation>,
        commits: &super::commit_service::ExecutionCommitService,
    ) -> Result<(), NodeExecutorError> {
        commits
            .validate_unadmitted_binding_rollback(ticket)
            .map_err(|error| NodeExecutorError::Invalid {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let owner = self
            .builtin_tool_batch()
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: ticket.executor_kind.clone(),
                node_id: ticket.node_id.clone(),
            })?;
        owner.rollback_unadmitted_binding(ticket, observation)
    }

    pub(super) fn forget_builtin_binding(&self, ticket: &NodeExecutionTicket) {
        if let Some(owner) = self.builtin_tool_batch() {
            owner.forget_closed_binding(ticket);
        }
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

    pub(crate) async fn drain_physical_execution(&self) -> Vec<String> {
        let executors = self
            .executors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(kind, executor)| (kind.clone(), executor.clone()))
            .collect::<Vec<_>>();
        futures::future::join_all(executors.into_iter().map(|(kind, executor)| async move {
            // Construct and poll inside the unwind boundary. One broken owner
            // must not abandon another owner's real pending physical work.
            match std::panic::AssertUnwindSafe(
                async move { executor.drain_physical_execution().await },
            )
            .catch_unwind()
            .await
            {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(format!("executor {kind} physical drain failed: {error}")),
                Err(_) => Some(format!("executor {kind} physical drain panicked")),
            }
        }))
        .await
        .into_iter()
        .flatten()
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
