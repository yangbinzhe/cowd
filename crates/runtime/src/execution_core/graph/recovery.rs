use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeStatus};
use thiserror::Error;

use super::commit_service::{ExecutionCommitError, ExecutionCommitService};
use super::registry::{NodeExecutorError, NodeExecutorRegistry};
use super::state_store::{ExecutionGraphStateStore, ExecutionStateStoreError};

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
        let head = self.state_store.load_async(graph_id).await?.revision;
        let mut after = 0;
        while after < head {
            let (next, starts) = self
                .state_store
                .binding_starts_page(graph_id, after, head)?;
            for (node_id, binding, position) in starts {
                for retry in 0..8 {
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
                    let physical = self.commit_service.authenticated_lease_recovery(&current,&node_id,&binding,self.registry).await?
                        .ok_or_else(|| ExecutionRecoveryError::LeaseReconciliation(format!(
                            "{graph_id}/{node_id}/{}: original backend/effect/after_commit quiescence unproven; no release or requeue",binding.attempt)))?;
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
                        Err(ExecutionCommitError::EventStore(
                            crate::RuntimeEventStoreError::StaleRevision { .. },
                        )) if retry < 7 => continue,
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
