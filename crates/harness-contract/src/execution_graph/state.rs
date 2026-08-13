use super::{ExecutionGraph, ExecutionNodeResult, ExecutionNodeStatus};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ExecutionTransitionError {
    #[error("execution graph revision mismatch: expected {expected}, actual {actual}")]
    StaleRevision { expected: u64, actual: u64 },
    #[error("execution node `{0}` does not exist")]
    NodeNotFound(String),
    #[error("illegal execution transition for `{node_id}`: {from:?} -> {to:?}")]
    Illegal {
        node_id: String,
        from: ExecutionNodeStatus,
        to: ExecutionNodeStatus,
    },
}

pub fn apply_node_transition(
    graph: &ExecutionGraph,
    expected_revision: u64,
    node_id: &str,
    to: ExecutionNodeStatus,
    result: Option<ExecutionNodeResult>,
) -> Result<ExecutionGraph, ExecutionTransitionError> {
    if graph.revision != expected_revision {
        return Err(ExecutionTransitionError::StaleRevision {
            expected: expected_revision,
            actual: graph.revision,
        });
    }
    let mut next = graph.clone();
    let from = *next
        .node_statuses
        .get(node_id)
        .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.to_string()))?;
    if !legal_transition(from, to) {
        return Err(ExecutionTransitionError::Illegal {
            node_id: node_id.to_string(),
            from,
            to,
        });
    }
    next.node_statuses.insert(node_id.to_string(), to);
    if let Some(result) = result {
        next.node_results.insert(node_id.to_string(), result);
    }
    next.revision = next.revision.saturating_add(1);
    Ok(next)
}

#[must_use]
pub fn legal_transition(from: ExecutionNodeStatus, to: ExecutionNodeStatus) -> bool {
    use ExecutionNodeStatus as S;
    matches!(
        (from, to),
        (S::Planned, S::Ready | S::Cancelled | S::Blocked)
            | (
                S::Ready,
                S::Running | S::Paused | S::Cancelled | S::Blocked | S::Failed
            )
            | (
                S::Running,
                S::WaitingInput
                    | S::WaitingApproval
                    | S::WaitingExternal
                    | S::Completed
                    | S::Failed
                    | S::Blocked
                    | S::Cancelled
                    | S::Paused
            )
            | (
                S::WaitingInput | S::WaitingApproval | S::WaitingExternal | S::Paused,
                S::Ready | S::Running | S::Failed | S::Cancelled
            )
    ) || from == to
}
