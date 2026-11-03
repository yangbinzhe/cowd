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

#[derive(Clone)]
pub(crate) struct OriginalBatchPrefix(pub(crate) Arc<crate::tool_host::AuthenticatedBatchPrefix>);
impl std::fmt::Debug for OriginalBatchPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("OriginalBatchPrefix") }
}
impl PartialEq for OriginalBatchPrefix {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalBatchPrefix {}

#[derive(Clone)]
pub(crate) struct OriginalPendingBatch {
    pub(crate) carrier: Arc<crate::tool_host::OriginalMemberCallCarrier>,
    pub(crate) publication: Option<Arc<crate::tool_host::OriginalStopPublication>>,
}
impl std::fmt::Debug for OriginalPendingBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("OriginalPendingBatch") }
}
impl PartialEq for OriginalPendingBatch {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.carrier, &other.carrier) && match (&self.publication, &other.publication) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b), (None, None) => true, _ => false,
        }
    }
}
impl Eq for OriginalPendingBatch {}

#[derive(Clone)]
pub(crate) struct OriginalAgentChild(pub(crate) Arc<super::executors::OriginalAgentChildCall>);
impl std::fmt::Debug for OriginalAgentChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("OriginalAgentChild") }
}
impl PartialEq for OriginalAgentChild {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalAgentChild {}

#[derive(Clone)]
pub(crate) struct OriginalReplayedAction(pub(crate) Arc<crate::agentic::OriginalReplayedAction>);
impl std::fmt::Debug for OriginalReplayedAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("OriginalReplayedAction") }
}
impl PartialEq for OriginalReplayedAction {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalReplayedAction {}

#[derive(Clone)]
pub(crate) struct OriginalTerminalOwner(pub(crate) Arc<crate::execution_core::services::OriginalTerminalCommitOwner>);
impl std::fmt::Debug for OriginalTerminalOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("OriginalTerminalOwner") }
}
impl PartialEq for OriginalTerminalOwner { fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) } }
impl Eq for OriginalTerminalOwner {}

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
    pub(crate) original_batch_prefixes: Vec<OriginalBatchPrefix>,
    #[serde(skip)]
    pub(crate) original_pending_batches: Vec<OriginalPendingBatch>,
    #[serde(skip)]
    pub(crate) original_replayed_actions: Vec<OriginalReplayedAction>,
    #[serde(skip)]
    pub(crate) original_agent_child: Option<OriginalAgentChild>,
    #[serde(skip)]
    pub(crate) original_terminal_owner: Option<OriginalTerminalOwner>,
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
    pub(crate) fn attach_original_terminal_owner(&mut self, owner: Arc<crate::execution_core::services::OriginalTerminalCommitOwner>) -> Result<(), String> {
        if self.original_terminal_owner.is_some() { return Err("original terminal owner already attached".into()); }
        self.original_terminal_owner = Some(OriginalTerminalOwner(owner));
        Ok(())
    }

    /// Carry the live Scoped owner; it is not an externally supplied proof.
    pub(crate) fn attach_original_agent_child(&mut self, child: Arc<super::executors::OriginalAgentChildCall>) -> Result<(), String> {
        if self.original_agent_child.is_some() { return Err("original Agent child owner already attached".into()); }
        self.original_agent_child = Some(OriginalAgentChild(child));
        Ok(())
    }

    #[must_use]
    pub fn new(result: ExecutionNodeResult) -> Self {
        Self {
            result,
            delivery_envelope: None,
            terminal_presentation: None,
            domain_events: Vec::new(),
            original_batch_prefixes: Vec::new(),
            original_pending_batches: Vec::new(),
            original_replayed_actions: Vec::new(),
            original_agent_child: None,
            original_terminal_owner: None,
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
    /// Revalidate a post-tool Goal observation using the original owner. This
    /// must never dispatch or replay the physical operation. Unknown remains
    /// unknown and leaves the original source fence in force.
    async fn revalidate_goal_source(
        &self,
        _ticket: &NodeExecutionTicket,
        _outcome: &NodeExecutionOutcome,
        _stale_stream: &str,
        _expected: u64,
        _actual: u64,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        Ok(None)
    }
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
    /// Query an authenticated, already durable outcome of this exact attempt.
    /// Must not start work, invoke a Provider, or re-interpret business input.
    /// The recovery owner still commits through the original source fences and
    /// independently proves physical/callback closure. Unknown stays unknown.
    async fn recover_known_outcome(
        &self,
        _ticket: &NodeExecutionTicket,
    ) -> Result<Option<NodeExecutionOutcome>, NodeExecutorError> {
        Ok(None)
    }
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

    /// Borrow only the still-live builtin owner's same-ticket observation.
    pub(super) fn builtin_binding_witness(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<super::executors::agent_tool::NoDispatchWitness> {
        self.builtin_tool_batch()?.binding_observation(ticket)?.witness()
    }

    pub(crate) fn is_builtin_tool_batch(&self, executor: &Arc<dyn NodeExecutor>) -> bool {
        self.builtin_tool_batch().is_some_and(|owner| {
            let canonical: Arc<dyn NodeExecutor> = owner;
            Arc::ptr_eq(&canonical, executor)
        })
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
