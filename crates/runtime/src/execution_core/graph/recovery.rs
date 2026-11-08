use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeStatus};
use thiserror::Error;

use super::commit_service::{ExecutionCommitError, ExecutionCommitService};
use super::registry::{NodeExecutorError, NodeExecutorRegistry};
use super::state_store::{ExecutionGraphStateStore, ExecutionStateStoreError};

use super::PHYSICAL_CLOSURE_BUDGET;

/// The cap on one wait inside the physical-closure budget. The wait is
/// event-driven - a sibling settlement or another binding's closure
/// publishes a durable commit and wakes the re-proof - and this cap keeps
/// the cadence bounded when nothing publishes, so the wait never
/// busy-waits the store and never outlives the budget.
const PHYSICAL_CLOSURE_RETRY_PAUSE: std::time::Duration = std::time::Duration::from_millis(25);

/// Wait for the next durable commit inside the closure budget, capped by
/// the smaller of the remaining budget and the retry pause. The whole wait
/// spends the one physical-closure budget the deadline bounds.
async fn wait_inside_the_closure_budget(
    commits: &mut tokio::sync::watch::Receiver<u64>,
    deadline: std::time::Instant,
) {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    let cap = remaining.min(PHYSICAL_CLOSURE_RETRY_PAUSE);
    if cap.is_zero() {
        return;
    }
    let _ = tokio::time::timeout(cap, commits.changed()).await;
}

#[derive(Debug, Error)]
pub enum ExecutionRecoveryError {
    #[error(transparent)]
    State(#[from] ExecutionStateStoreError),
    #[error(transparent)]
    Commit(#[from] ExecutionCommitError),
    #[error(transparent)]
    Executor(#[from] NodeExecutorError),
    #[error("binding lease reconciliation blocked: {0}")]
    LeaseReconciliation(String),
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

    pub(crate) async fn recover_with_leases(
        &self,
        graph_id: &str,
        scopes: &super::resources::ScopeLockManager,
        worktrees: &super::resources::WorktreeLeaseManager,
        workspace_id: &str,
    ) -> Result<ExecutionGraph, ExecutionRecoveryError> {
        self.recover_with_leases_until(
            graph_id,
            scopes,
            worktrees,
            workspace_id,
            std::time::Instant::now() + PHYSICAL_CLOSURE_BUDGET,
        )
        .await
    }

    /// The same recovery with the closure budget's deadline as a parameter:
    /// production passes `now + PHYSICAL_CLOSURE_BUDGET`, and a test passes
    /// its own bound so the wait inside the budget is witnessed in
    /// milliseconds. The one deadline bounds every wait this recovery
    /// makes, so the total never exceeds it.
    pub(crate) async fn recover_with_leases_until(
        &self,
        graph_id: &str,
        scopes: &super::resources::ScopeLockManager,
        worktrees: &super::resources::WorktreeLeaseManager,
        workspace_id: &str,
        closure_deadline: std::time::Instant,
    ) -> Result<ExecutionGraph, ExecutionRecoveryError> {
        let head = self.state_store.load_async(graph_id).await?.revision;
        let mut commits = self.state_store.subscribe_commits();
        let mut after = 0;
        while after < head {
            let (next, starts) = self
                .state_store
                .binding_starts_page(graph_id, after, head)?;
            for (node_id, binding, position) in starts {
                loop {
                    let graph = self.state_store.load_async(graph_id).await?;
                    let already_closed = self
                        .state_store
                        .binding_lease_closure(&graph, &node_id, &binding, position)?;
                    if already_closed {
                        self.commit_service
                            .recover_closed_binding_publication(
                                &graph,
                                &node_id,
                                &binding,
                                self.registry,
                            )
                            .await?;
                        break;
                    }
                    let mut owner = if !already_closed {
                        let key = super::events::ExecutionBindingLeaseClosure::fence_key(
                            &graph, &node_id, &binding,
                        );
                        Some(scopes.try_recover_binding_owner(&key)
                            .map_err(|error| ExecutionRecoveryError::LeaseReconciliation(error.to_string()))?
                            .ok_or_else(|| ExecutionRecoveryError::LeaseReconciliation(format!(
                                "{graph_id}/{node_id}/{}: original owner alive or unsupported missing fence",binding.attempt)))?)
                    } else {
                        None
                    };
                    let running = graph.node_statuses.get(&node_id)
                        == Some(&ExecutionNodeStatus::Running)
                        && graph.recovery_cursor.node_attempts.get(&node_id)
                            == Some(&binding.attempt);
                    if running {
                        if !self
                            .commit_service
                            .recover_running_bound_tool_batch(
                                &graph,
                                &node_id,
                                &binding,
                                self.registry,
                            )
                            .await?
                            && !self
                                .commit_service
                                .recover_running_known_outcome(
                                    &graph,
                                    &node_id,
                                    &binding,
                                    self.registry,
                                )
                                .await?
                        {
                            return Err(ExecutionRecoveryError::LeaseReconciliation(format!(
                                "{graph_id}/{node_id}/{}: original outcome is not durably committed; provider/Host state unknown; no reexecute or Ready admission",
                                binding.attempt)));
                        }
                    }
                    let current = self.state_store.load_async(graph_id).await?;
                    let owner = owner.as_mut().ok_or_else(|| {
                        ExecutionRecoveryError::LeaseReconciliation(
                            "missing exact recovery owner fence".into(),
                        )
                    })?;
                    let physical = match self
                        .commit_service
                        .authenticated_lease_recovery(&current, &node_id, &binding, self.registry)
                        .await
                    {
                        Ok(Some(physical)) => physical,
                        Ok(None) => {
                            return Err(ExecutionRecoveryError::LeaseReconciliation(format!(
                                "{graph_id}/{node_id}/{}: original backend/effect/after_commit quiescence unproven; no release or requeue",
                                binding.attempt
                            )))
                        }
                        Err(error) if error.keeps_the_closure_waiting() => {
                            // The second movement surface: this proof runs
                            // before the commit step and covers the same
                            // shared streams over a longer window. A source
                            // that moved here is the same wait, so it
                            // re-proves inside the budget instead of naming
                            // an unproven quiescence.
                            if std::time::Instant::now() >= closure_deadline {
                                return Err(error.into());
                            }
                            wait_inside_the_closure_budget(&mut commits, closure_deadline).await;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let released = owner
                        .release_recovered(
                            &current,
                            &node_id,
                            &binding,
                            position,
                            &physical,
                            worktrees,
                            workspace_id,
                        )
                        .map_err(ExecutionRecoveryError::LeaseReconciliation)?;
                    match self
                        .commit_service
                        .commit_binding_lease_closure(&current, &released)
                    {
                        Ok(_) => break,
                        Err(error) if error.keeps_the_closure_waiting() => {
                            // The transaction raced a concurrent writer, or
                            // a source moved under the proof: read again on
                            // the fresh view while the budget holds.
                            if std::time::Instant::now() >= closure_deadline {
                                return Err(error.into());
                            }
                            wait_inside_the_closure_budget(&mut commits, closure_deadline).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            after = next;
        }
        let graph = self.state_store.load_async(graph_id).await?;
        if graph
            .node_statuses
            .values()
            .any(|status| *status == ExecutionNodeStatus::Running)
        {
            return Err(ExecutionRecoveryError::LeaseReconciliation(
                "Running execution has no authenticated recoverable original binding/result".into(),
            ));
        }
        Ok(graph)
    }
}
