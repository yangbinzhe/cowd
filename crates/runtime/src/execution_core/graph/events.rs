use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeResult, ExecutionNodeStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNodeBinding {
    pub executor_kind: String,
    pub ticket_idempotency_key: String,
    pub attempt: u32,
    pub resource_lease_refs: Vec<String>,
    pub scope_lease_ref: Option<String>,
    pub worktree_lease_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ExecutionGraphEvent {
    Planned {
        graph: ExecutionGraph,
    },
    NodeTransitioned {
        node_id: String,
        from: ExecutionNodeStatus,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binding: Option<ExecutionNodeBinding>,
        graph: ExecutionGraph,
    },
    NodeTransitionedAndReplanned {
        node_id: String,
        from: ExecutionNodeStatus,
        to: ExecutionNodeStatus,
        result: ExecutionNodeResult,
        reason: String,
        added_node_ids: Vec<String>,
        graph: ExecutionGraph,
    },
    CommandApplied {
        command: String,
        reason: Option<String>,
        graph: ExecutionGraph,
    },
    Replanned {
        reason: String,
        added_node_ids: Vec<String>,
        graph: ExecutionGraph,
    },
    Recovered {
        recovered_nodes: Vec<String>,
        blocked_nodes: Vec<String>,
        graph: ExecutionGraph,
    },
}

impl ExecutionGraphEvent {
    #[must_use]
    pub fn graph(&self) -> &ExecutionGraph {
        match self {
            Self::Planned { graph }
            | Self::NodeTransitioned { graph, .. }
            | Self::NodeTransitionedAndReplanned { graph, .. }
            | Self::CommandApplied { graph, .. }
            | Self::Replanned { graph, .. }
            | Self::Recovered { graph, .. } => graph,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Planned { .. } => "execution_graph.planned",
            Self::NodeTransitioned { .. } => "execution_graph.node_transitioned",
            Self::NodeTransitionedAndReplanned { .. } => {
                "execution_graph.node_transitioned_and_replanned"
            }
            Self::CommandApplied { .. } => "execution_graph.command_applied",
            Self::Replanned { .. } => "execution_graph.replanned",
            Self::Recovered { .. } => "execution_graph.recovered",
        }
    }
}
