use serde::{Deserialize, Serialize};

use super::{
    ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeStatus,
    ExecutionParentBinding, ExecutionServiceClass, ExecutionUsage,
};
use crate::context::EvidenceAccessRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionNodeProjection {
    pub node_id: String,
    pub kind: ExecutionNodeKind,
    pub status: ExecutionNodeStatus,
    pub executor_kind: String,
    pub result_ref: Option<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    /// Canonical node-level usage. Keeping it on the projection makes
    /// execution metrics traceable across nested graphs without asking a
    /// surface to infer cost from prose timeline events.
    #[serde(default)]
    pub usage: ExecutionUsage,
}

/// Read-only graph relation safe for surfaces. Execution payloads and private
/// prompts stay in Runtime; consumers only need stable topology to render and
/// control the durable graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionEdgeProjection {
    pub from: String,
    pub to: String,
    pub kind: ExecutionEdgeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionGraphProjection {
    pub graph_id: String,
    pub revision: u64,
    pub objective: String,
    #[serde(default)]
    pub service_class: ExecutionServiceClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    pub nodes: Vec<ExecutionNodeProjection>,
    pub edges: Vec<ExecutionEdgeProjection>,
    pub commit_cursor: u64,
    pub terminal_result_ref: Option<String>,
}

#[must_use]
pub fn project_execution_graph(graph: &ExecutionGraph) -> ExecutionGraphProjection {
    ExecutionGraphProjection {
        graph_id: graph.id.clone(),
        revision: graph.revision,
        objective: graph.objective.clone(),
        service_class: graph.service_class,
        parent_execution: graph.parent_execution.clone(),
        nodes: graph
            .nodes
            .iter()
            .map(|node| {
                let result = graph.node_results.get(&node.id);
                ExecutionNodeProjection {
                    node_id: node.id.clone(),
                    kind: node.kind,
                    status: graph
                        .node_statuses
                        .get(&node.id)
                        .copied()
                        .unwrap_or(ExecutionNodeStatus::Planned),
                    executor_kind: node.executor_kind.clone(),
                    result_ref: result.and_then(|value| value.result_ref.clone()),
                    evidence_refs: result
                        .map(|value| value.evidence_refs.clone())
                        .unwrap_or_default(),
                    usage: result.map(|value| value.usage.clone()).unwrap_or_default(),
                }
            })
            .collect(),
        edges: graph
            .edges
            .iter()
            .map(|edge| ExecutionEdgeProjection {
                from: edge.from.clone(),
                to: edge.to.clone(),
                kind: edge.kind,
            })
            .collect(),
        commit_cursor: graph.recovery_cursor.commit_cursor,
        terminal_result_ref: graph
            .nodes
            .iter()
            .rev()
            .filter(|node| node.kind == ExecutionNodeKind::Synthesize)
            .find_map(|node| graph.node_results.get(&node.id))
            .and_then(|result| result.result_ref.clone()),
    }
}
