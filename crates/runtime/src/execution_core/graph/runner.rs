use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdgeKind, ExecutionGraph, ExecutionGraphCommand,
    ExecutionGraphValidationError, ExecutionNodeResult, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use super::commit_service::{ExecutionCommitError, ExecutionCommitService, ExecutionEffectState};
use super::events::ExecutionNodeBinding;
use super::registry::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError, NodeExecutorRegistry,
};
use super::resources::{
    ExecutionResourceKind, ExecutionResourceLease, ExecutionResourceManager, ScopeLockLease,
    ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource, WorktreeLease,
    WorktreeLeaseManager, WorktreeLeaseRequest, WorktreeOwnership,
};
use super::state_store::{ExecutionGraphStateStore, ExecutionStateStoreError};

#[derive(Debug, Error)]
pub enum ExecutionRunnerError {
    #[error(transparent)]
    InvalidGraph(#[from] ExecutionGraphValidationError),
    #[error(transparent)]
    Executor(#[from] NodeExecutorError),
    #[error(transparent)]
    Commit(#[from] ExecutionCommitError),
    #[error(transparent)]
    State(#[from] ExecutionStateStoreError),
    #[error("execution node task failed to join: {0}")]
    Join(String),
    #[error("executor returned illegal outcome `{status:?}` for node `{node_id}`")]
    IllegalOutcome {
        node_id: String,
        status: ExecutionNodeStatus,
    },
    #[error("execution graph projection is missing node `{0}`")]
    NodeMissing(String),
    #[error("new execution graphs must be submitted with the Start command")]
    InvalidStartCommand,
    #[error("execution resource acquisition failed for node `{node_id}`: {reason}")]
    Resource { node_id: String, reason: String },
    #[error("execution mutation is blocked: {0}")]
    MutationBlocked(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRunReport {
    pub graph_id: String,
    pub revision: u64,
    pub completed: usize,
    pub failed: usize,
    pub blocked: usize,
    pub cancelled: usize,
    pub waiting: usize,
}

struct ActiveNode {
    executor: Arc<dyn NodeExecutor>,
    ticket: NodeExecutionTicket,
    _resources: NodeResourceGuards,
}

struct NodeResourceGuards {
    resource: ExecutionResourceLease,
    scope: Option<ScopeLockLease>,
    worktree: Option<WorktreeLease>,
}

impl NodeResourceGuards {
    fn binding(&self, ticket: &NodeExecutionTicket) -> ExecutionNodeBinding {
        ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: ticket.attempt,
            resource_lease_refs: vec![self.resource.id().to_string()],
            scope_lease_ref: self.scope.as_ref().map(|lease| lease.id().to_string()),
            worktree_lease_ref: self
                .worktree
                .as_ref()
                .map(|lease| lease.record().lease_id.to_string()),
        }
    }

    fn release_worktree(&mut self) {
        if let Some(lease) = self.worktree.take() {
            let _ = lease.release();
        }
    }
}

impl Drop for NodeResourceGuards {
    fn drop(&mut self) {
        self.release_worktree();
    }
}

#[derive(Clone)]
pub struct ExecutionGraphRunner {
    registry: Arc<NodeExecutorRegistry>,
    state_store: ExecutionGraphStateStore,
    commit_service: ExecutionCommitService,
    resource_manager: Arc<ExecutionResourceManager>,
    scope_locks: Arc<ScopeLockManager>,
    worktree_leases: Arc<WorktreeLeaseManager>,
    workspace_id: String,
    workspace_root: PathBuf,
    active: Arc<Mutex<BTreeMap<(String, String), ActiveNode>>>,
    coordination: Arc<Mutex<()>>,
    mutation_gate: Arc<RwLock<Option<MutationGate>>>,
}

type MutationGate = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

impl ExecutionGraphRunner {
    #[must_use]
    pub fn new(
        registry: Arc<NodeExecutorRegistry>,
        state_store: ExecutionGraphStateStore,
        commit_service: ExecutionCommitService,
        resource_manager: Arc<ExecutionResourceManager>,
        scope_locks: Arc<ScopeLockManager>,
        worktree_leases: Arc<WorktreeLeaseManager>,
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            registry,
            state_store,
            commit_service,
            resource_manager,
            scope_locks,
            worktree_leases,
            workspace_id: workspace_id.into(),
            workspace_root: workspace_root.into(),
            active: Arc::new(Mutex::new(BTreeMap::new())),
            coordination: Arc::new(Mutex::new(())),
            mutation_gate: Arc::new(RwLock::new(None)),
        }
    }

    pub fn install_mutation_gate(
        &self,
        gate: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) {
        *self
            .mutation_gate
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::new(gate));
    }

    fn ensure_mutation_allowed(&self) -> Result<(), ExecutionRunnerError> {
        let gate = self
            .mutation_gate
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        gate.map_or(Ok(()), |gate| {
            gate().map_err(ExecutionRunnerError::MutationBlocked)
        })
    }

    pub async fn start(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        validate_execution_graph(&graph)?;
        self.registry.validate_graph(&graph)?;
        let registered = self.commit_service.register_graph_async(graph).await?.graph;
        self.run_until_quiescent(&registered.id).await
    }

    pub async fn run_until_quiescent(
        &self,
        graph_id: &str,
    ) -> Result<ExecutionRunReport, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        loop {
            let mut graph = self.state_store.load_async(graph_id).await?;
            graph = self.advance_dependencies(graph).await?;
            let ready = graph
                .nodes
                .iter()
                .filter(|node| graph.node_statuses[&node.id] == ExecutionNodeStatus::Ready)
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            if ready.is_empty() {
                return Ok(report(&graph));
            }

            let mut wave = JoinSet::new();
            for node_id in ready {
                let node = graph
                    .nodes
                    .iter()
                    .find(|node| node.id == node_id)
                    .ok_or_else(|| ExecutionRunnerError::NodeMissing(node_id.clone()))?
                    .clone();
                let runner = self.clone();
                let graph_id = graph_id.to_string();
                wave.spawn(async move { runner.start_and_execute_node(&graph_id, node).await });
            }

            while let Some(joined) = wave.join_next().await {
                let result =
                    joined.map_err(|error| ExecutionRunnerError::Join(error.to_string()))?;
                let (node_id, outcome) = match result {
                    Ok(value) => value,
                    Err(ExecutionRunnerError::Executor(error)) => {
                        let node_id = executor_error_node_id(&error).to_string();
                        self.active
                            .lock()
                            .await
                            .remove(&(graph_id.to_string(), node_id.clone()));
                        let result = failed_result(&error);
                        let terminal_status = result.status;
                        let _coordination = self.coordination.lock().await;
                        let current = self.state_store.load_async(graph_id).await?;
                        if current.node_statuses.get(&node_id)
                            == Some(&ExecutionNodeStatus::Running)
                        {
                            self.commit_service
                                .transition_node_async(
                                    current,
                                    node_id,
                                    terminal_status,
                                    Some(result),
                                    Vec::new(),
                                )
                                .await?;
                            continue;
                        }
                        return Err(ExecutionRunnerError::Executor(error));
                    }
                    Err(error) => return Err(error),
                };
                validate_outcome(&node_id, &outcome)?;
                let _coordination = self.coordination.lock().await;
                let current = self.state_store.load_async(graph_id).await?;
                if current.node_statuses.get(&node_id) != Some(&ExecutionNodeStatus::Running) {
                    continue;
                }
                if let Some(replan) = outcome.replan {
                    self.registry.validate_nodes(&replan.nodes)?;
                    self.commit_service
                        .transition_node_with_replan_async(
                            current,
                            node_id.clone(),
                            outcome.result,
                            outcome.domain_events,
                            replan.nodes,
                            replan.edges,
                            replan.reason,
                        )
                        .await?;
                } else {
                    self.commit_service
                        .transition_node_async(
                            current,
                            node_id.clone(),
                            outcome.result.status,
                            Some(outcome.result),
                            outcome.domain_events,
                        )
                        .await?;
                }
                let committed_executor = self
                    .active
                    .lock()
                    .await
                    .get(&(graph_id.to_string(), node_id.clone()))
                    .map(|active| (Arc::clone(&active.executor), active.ticket.clone()));
                if let Some((executor, ticket)) = committed_executor {
                    let after_commit = executor.after_commit(&ticket).await;
                    self.active
                        .lock()
                        .await
                        .remove(&(graph_id.to_string(), node_id.clone()));
                    after_commit?;
                } else {
                    self.active
                        .lock()
                        .await
                        .remove(&(graph_id.to_string(), node_id.clone()));
                }
            }
        }
    }

    async fn start_and_execute_node(
        &self,
        graph_id: &str,
        node: harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<(String, NodeExecutionOutcome), ExecutionRunnerError> {
        let resources = self
            .acquire_node_resources(&self.state_store.load_async(graph_id).await?, &node)
            .await?;
        let _coordination = self.coordination.lock().await;
        let graph = self.state_store.load_async(graph_id).await?;
        if graph.node_statuses.get(&node.id) != Some(&ExecutionNodeStatus::Ready) {
            return Err(ExecutionRunnerError::NodeMissing(node.id));
        }
        let executor = self.registry.get(&node.executor_kind).ok_or_else(|| {
            NodeExecutorError::Unavailable {
                executor_kind: node.executor_kind.clone(),
                node_id: node.id.clone(),
            }
        })?;
        let attempt = graph
            .recovery_cursor
            .node_attempts
            .get(&node.id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph.clone()),
                node,
                attempt,
            })
            .await?;
        let binding = resources.binding(&ticket);
        self.active.lock().await.insert(
            (ticket.graph_id.clone(), ticket.node_id.clone()),
            ActiveNode {
                executor: Arc::clone(&executor),
                ticket: ticket.clone(),
                _resources: resources,
            },
        );
        if let Err(error) = self
            .commit_service
            .bind_and_start_node_async(graph, ticket.node_id.clone(), binding)
            .await
        {
            self.active
                .lock()
                .await
                .remove(&(ticket.graph_id.clone(), ticket.node_id.clone()));
            let _ = executor.cancel(&ticket).await;
            return Err(error.into());
        }
        drop(_coordination);
        let poll_gate = self.coordination.lock().await;
        let current = self.state_store.load_async(graph_id).await?;
        if current.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running) {
            self.active
                .lock()
                .await
                .remove(&(ticket.graph_id.clone(), ticket.node_id.clone()));
            return Ok((
                ticket.node_id.clone(),
                NodeExecutionOutcome::new(ExecutionNodeResult {
                    // This outcome is only an internal wake-up value. The command's
                    // already-committed graph status remains authoritative and the
                    // caller discards this result because the node is no longer running.
                    status: ExecutionNodeStatus::Cancelled,
                    result_ref: None,
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: now_ms(),
                }),
            ));
        }
        let mut poll = Box::pin(executor.poll_or_await(&ticket));
        match self.commit_service.begin_execution_effect(&ticket)? {
            ExecutionEffectState::Completed(outcome) => {
                drop(poll_gate);
                return Ok((ticket.node_id.clone(), outcome));
            }
            ExecutionEffectState::Uncertain => {
                drop(poll_gate);
                return Err(NodeExecutorError::Uncertain {
                    node_id: ticket.node_id.clone(),
                    reason: format!(
                        "effect intent `{}` has no durable receipt",
                        ticket.idempotency_key
                    ),
                }
                .into());
            }
            ExecutionEffectState::Fresh => {}
        }
        let first_poll = futures::poll!(&mut poll);
        drop(poll_gate);
        let outcome = match first_poll {
            std::task::Poll::Ready(outcome) => outcome,
            std::task::Poll::Pending => poll.await,
        };
        let outcome = outcome?;
        self.commit_service
            .commit_execution_effect(&ticket, &outcome)?;
        Ok((ticket.node_id.clone(), outcome))
    }

    async fn acquire_node_resources(
        &self,
        graph: &ExecutionGraph,
        node: &harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<NodeResourceGuards, ExecutionRunnerError> {
        let resource_kind = match node.kind {
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel
            | harness_contract::execution_graph::ExecutionNodeKind::Synthesize
            | harness_contract::execution_graph::ExecutionNodeKind::Verify => {
                ExecutionResourceKind::Provider
            }
            harness_contract::execution_graph::ExecutionNodeKind::AgentTask => {
                ExecutionResourceKind::Agent
            }
            _ => ExecutionResourceKind::Tool,
        };
        let resource = self
            .resource_manager
            .acquire(resource_kind, None)
            .await
            .map_err(|error| ExecutionRunnerError::Resource {
                node_id: node.id.clone(),
                reason: error.to_string(),
            })?;
        let mut scope_requests = Vec::new();
        let mut worktree_path = None;
        for scope in &node.resource_scopes {
            if let Some(path) = scope.strip_prefix("read:") {
                scope_requests.push(ScopeLockRequest {
                    scope: ScopedResource::file(&self.workspace_id, path).map_err(|error| {
                        ExecutionRunnerError::Resource {
                            node_id: node.id.clone(),
                            reason: error.to_string(),
                        }
                    })?,
                    mode: ScopeLockMode::Read,
                });
            } else if let Some(path) = scope.strip_prefix("write:") {
                scope_requests.push(ScopeLockRequest {
                    scope: ScopedResource::file(&self.workspace_id, path).map_err(|error| {
                        ExecutionRunnerError::Resource {
                            node_id: node.id.clone(),
                            reason: error.to_string(),
                        }
                    })?,
                    mode: ScopeLockMode::Write,
                });
            } else if let Some(path) = scope.strip_prefix("worktree:") {
                worktree_path = Some(validate_worktree_path(&self.workspace_root, path).map_err(
                    |reason| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason,
                    },
                )?);
            }
        }
        let scope = if scope_requests.is_empty() {
            None
        } else {
            Some(
                self.scope_locks
                    .acquire(scope_requests, None)
                    .await
                    .map_err(|error| ExecutionRunnerError::Resource {
                        node_id: node.id.clone(),
                        reason: error.to_string(),
                    })?,
            )
        };
        let worktree = worktree_path
            .map(|path| {
                self.worktree_leases.acquire(WorktreeLeaseRequest {
                    workspace_id: self.workspace_id.clone(),
                    task_id: graph.id.clone(),
                    owner_id: node.id.clone(),
                    path,
                    ownership: WorktreeOwnership::UserManaged,
                    ttl: Duration::from_secs(300),
                })
            })
            .transpose()
            .map_err(|error| ExecutionRunnerError::Resource {
                node_id: node.id.clone(),
                reason: error.to_string(),
            })?;
        Ok(NodeResourceGuards {
            resource,
            scope,
            worktree,
        })
    }

    pub async fn command(
        &self,
        graph_id: &str,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        self.ensure_mutation_allowed()?;
        let coordination = self.coordination.lock().await;
        let graph = self.state_store.load_async(graph_id).await?;
        self.commit_service
            .validate_command_revision(&graph, &command)?;
        let active = if matches!(
            command,
            ExecutionGraphCommand::Pause { .. } | ExecutionGraphCommand::Cancel { .. }
        ) {
            self.active
                .lock()
                .await
                .iter()
                .filter(|((active_graph_id, _), _)| active_graph_id == graph_id)
                .map(|(_, node)| (Arc::clone(&node.executor), node.ticket.clone()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        for (executor, ticket) in active {
            executor.cancel(&ticket).await?;
        }
        let graph = self
            .commit_service
            .apply_command_async(graph, command.clone())
            .await?
            .graph;
        drop(coordination);
        if matches!(
            command,
            ExecutionGraphCommand::Resume { .. }
                | ExecutionGraphCommand::Advance { .. }
                | ExecutionGraphCommand::SubmitApproval { approved: true, .. }
        ) {
            self.run_until_quiescent(graph_id).await?;
            return self
                .state_store
                .load_async(graph_id)
                .await
                .map_err(Into::into);
        }
        Ok(graph)
    }

    pub async fn projection(
        &self,
        graph_id: &str,
    ) -> Result<harness_contract::execution_graph::ExecutionGraphProjection, ExecutionRunnerError>
    {
        self.state_store
            .projection_async(graph_id)
            .await
            .map_err(ExecutionRunnerError::from)
    }

    async fn advance_dependencies(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionGraph, ExecutionRunnerError> {
        let _coordination = self.coordination.lock().await;
        let mut graph = self.state_store.load_async(&graph.id).await?;
        loop {
            let mut changed = false;
            let planned = graph
                .nodes
                .iter()
                .filter(|node| graph.node_statuses[&node.id] == ExecutionNodeStatus::Planned)
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            for node_id in planned {
                let predecessors = graph
                    .edges
                    .iter()
                    .filter(|edge| edge.kind == ExecutionEdgeKind::DependsOn && edge.to == node_id)
                    .map(|edge| graph.node_statuses[&edge.from])
                    .collect::<Vec<_>>();
                let target = if predecessors
                    .iter()
                    .any(|status| status.is_terminal() && *status != ExecutionNodeStatus::Completed)
                {
                    Some(ExecutionNodeStatus::Blocked)
                } else if predecessors
                    .iter()
                    .all(|status| *status == ExecutionNodeStatus::Completed)
                {
                    Some(ExecutionNodeStatus::Ready)
                } else {
                    None
                };
                if let Some(target) = target {
                    graph = self
                        .commit_service
                        .transition_node_async(graph, node_id, target, None, Vec::new())
                        .await?
                        .graph;
                    changed = true;
                }
            }
            if !changed {
                return Ok(graph);
            }
        }
    }
}

pub(crate) fn validate_worktree_path(
    workspace_root: &Path,
    requested: &str,
) -> Result<PathBuf, String> {
    let relative = Path::new(requested);
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err("worktree scope must be a non-empty relative path".to_string());
    }
    if relative.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err("worktree scope may not contain parent or root components".to_string());
    }
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace root cannot be canonicalized: {error}"))?;
    let mut candidate = canonical_root.clone();
    for component in relative.components() {
        candidate.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&candidate)
            .map_err(|error| format!("worktree path cannot be inspected: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err("worktree scope may not traverse symbolic links".to_string());
        }
    }
    let canonical_candidate = candidate
        .canonicalize()
        .map_err(|error| format!("worktree path cannot be canonicalized: {error}"))?;
    if canonical_candidate == canonical_root || !canonical_candidate.starts_with(&canonical_root) {
        return Err("worktree scope must resolve to a workspace descendant".to_string());
    }
    Ok(canonical_candidate)
}

fn validate_outcome(
    node_id: &str,
    outcome: &NodeExecutionOutcome,
) -> Result<(), ExecutionRunnerError> {
    if matches!(
        outcome.result.status,
        ExecutionNodeStatus::Completed
            | ExecutionNodeStatus::Failed
            | ExecutionNodeStatus::Blocked
            | ExecutionNodeStatus::Cancelled
            | ExecutionNodeStatus::WaitingInput
            | ExecutionNodeStatus::WaitingApproval
            | ExecutionNodeStatus::WaitingExternal
    ) {
        Ok(())
    } else {
        Err(ExecutionRunnerError::IllegalOutcome {
            node_id: node_id.to_string(),
            status: outcome.result.status,
        })
    }
}

fn executor_error_node_id(error: &NodeExecutorError) -> &str {
    match error {
        NodeExecutorError::DuplicateExecutor(kind) => kind,
        NodeExecutorError::Unavailable { node_id, .. }
        | NodeExecutorError::Invalid { node_id, .. }
        | NodeExecutorError::Start { node_id, .. }
        | NodeExecutorError::Poll { node_id, .. }
        | NodeExecutorError::Cancel { node_id, .. }
        | NodeExecutorError::Recover { node_id, .. }
        | NodeExecutorError::Uncertain { node_id, .. } => node_id,
    }
}

fn failed_result(error: &NodeExecutorError) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: if matches!(error, NodeExecutorError::Uncertain { .. }) {
            ExecutionNodeStatus::Blocked
        } else {
            ExecutionNodeStatus::Failed
        },
        result_ref: None,
        evidence_refs: Vec::new(),
        failure: Some(harness_contract::execution_graph::ExecutionFailure {
            kind: if matches!(error, NodeExecutorError::Uncertain { .. }) {
                "effect_completion_uncertain".to_string()
            } else {
                "executor_error".to_string()
            },
            message: error.to_string(),
            retryable: false,
            evidence_refs: Vec::new(),
        }),
        usage: Default::default(),
        finished_at_ms: now_ms(),
    }
}

fn report(graph: &ExecutionGraph) -> ExecutionRunReport {
    let count = |status| {
        graph
            .node_statuses
            .values()
            .filter(|current| **current == status)
            .count()
    };
    ExecutionRunReport {
        graph_id: graph.id.clone(),
        revision: graph.revision,
        completed: count(ExecutionNodeStatus::Completed),
        failed: count(ExecutionNodeStatus::Failed),
        blocked: count(ExecutionNodeStatus::Blocked),
        cancelled: count(ExecutionNodeStatus::Cancelled),
        waiting: count(ExecutionNodeStatus::WaitingInput)
            + count(ExecutionNodeStatus::WaitingApproval)
            + count(ExecutionNodeStatus::WaitingExternal)
            + count(ExecutionNodeStatus::Paused),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
