use std::collections::BTreeMap;

use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionGraph, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
    ExecutionOrchestrationMetadata, ExecutionParentBinding, ExecutionRecoveryCursor,
    ExecutionServiceClass,
};
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

/// Minimal reconstruction payload between durable graph checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphDelta {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_class: Option<ExecutionServiceClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<Option<ExecutionParentBinding>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestration: Option<Option<ExecutionOrchestrationMetadata>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_nodes: Vec<ExecutionNodeSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_node_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_edges: Vec<ExecutionEdge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_edges: Vec<ExecutionEdge>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_status_updates: BTreeMap<String, ExecutionNodeStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_node_statuses: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_result_updates: BTreeMap<String, ExecutionNodeResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_node_results: Vec<String>,
    pub recovery_cursor: ExecutionRecoveryCursor,
}

impl ExecutionGraphDelta {
    #[must_use]
    pub fn between(previous: &ExecutionGraph, next: &ExecutionGraph) -> Self {
        Self {
            revision: next.revision,
            objective: (previous.objective != next.objective).then(|| next.objective.clone()),
            service_class: (previous.service_class != next.service_class)
                .then_some(next.service_class),
            parent_execution: (previous.parent_execution != next.parent_execution)
                .then(|| next.parent_execution.clone()),
            orchestration: (previous.orchestration != next.orchestration)
                .then(|| next.orchestration.clone()),
            added_nodes: next
                .nodes
                .iter()
                .filter(|node| {
                    !previous
                        .nodes
                        .iter()
                        .any(|old| old.id == node.id && old == *node)
                })
                .cloned()
                .collect(),
            removed_node_ids: previous
                .nodes
                .iter()
                .filter(|node| !next.nodes.iter().any(|new| new.id == node.id))
                .map(|node| node.id.clone())
                .collect(),
            added_edges: next
                .edges
                .iter()
                .filter(|edge| !previous.edges.contains(edge))
                .cloned()
                .collect(),
            removed_edges: previous
                .edges
                .iter()
                .filter(|edge| !next.edges.contains(edge))
                .cloned()
                .collect(),
            node_status_updates: next
                .node_statuses
                .iter()
                .filter(|(id, status)| previous.node_statuses.get(*id) != Some(*status))
                .map(|(id, status)| (id.clone(), *status))
                .collect(),
            removed_node_statuses: previous
                .node_statuses
                .keys()
                .filter(|id| !next.node_statuses.contains_key(*id))
                .cloned()
                .collect(),
            node_result_updates: next
                .node_results
                .iter()
                .filter(|(id, result)| previous.node_results.get(*id) != Some(*result))
                .map(|(id, result)| (id.clone(), result.clone()))
                .collect(),
            removed_node_results: previous
                .node_results
                .keys()
                .filter(|id| !next.node_results.contains_key(*id))
                .cloned()
                .collect(),
            recovery_cursor: next.recovery_cursor.clone(),
        }
    }

    pub fn apply(&self, graph: &mut ExecutionGraph) -> Result<(), String> {
        let expected = graph.revision.saturating_add(1);
        if self.revision != expected {
            return Err(format!(
                "delta revision {} does not follow graph revision {}",
                self.revision, graph.revision
            ));
        }
        if let Some(objective) = &self.objective {
            graph.objective.clone_from(objective);
        }
        if let Some(service_class) = self.service_class {
            graph.service_class = service_class;
        }
        if let Some(parent_execution) = &self.parent_execution {
            graph.parent_execution.clone_from(parent_execution);
        }
        if let Some(orchestration) = &self.orchestration {
            graph.orchestration.clone_from(orchestration);
        }
        for id in &self.removed_node_ids {
            graph.nodes.retain(|node| node.id != *id);
        }
        for node in &self.added_nodes {
            graph.nodes.retain(|current| current.id != node.id);
            graph.nodes.push(node.clone());
        }
        graph
            .edges
            .retain(|edge| !self.removed_edges.contains(edge));
        for edge in &self.added_edges {
            if !graph.edges.contains(edge) {
                graph.edges.push(edge.clone());
            }
        }
        for id in &self.removed_node_statuses {
            graph.node_statuses.remove(id);
        }
        graph.node_statuses.extend(self.node_status_updates.clone());
        for id in &self.removed_node_results {
            graph.node_results.remove(id);
        }
        graph.node_results.extend(self.node_result_updates.clone());
        graph.recovery_cursor = self.recovery_cursor.clone();
        graph.revision = self.revision;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ExecutionGraphEvent {
    Planned {
        graph: ExecutionGraph,
    },
    Checkpoint {
        cause: String,
        graph: ExecutionGraph,
    },
    NodeTransitioned {
        node_id: String,
        from: ExecutionNodeStatus,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binding: Option<ExecutionNodeBinding>,
        delta: ExecutionGraphDelta,
    },
    NodeTransitionedAndReplanned {
        node_id: String,
        from: ExecutionNodeStatus,
        to: ExecutionNodeStatus,
        result: ExecutionNodeResult,
        reason: String,
        added_node_ids: Vec<String>,
        delta: ExecutionGraphDelta,
    },
    CommandApplied {
        command: String,
        reason: Option<String>,
        delta: ExecutionGraphDelta,
    },
    Replanned {
        reason: String,
        added_node_ids: Vec<String>,
        delta: ExecutionGraphDelta,
    },
    Recovered {
        recovered_nodes: Vec<String>,
        blocked_nodes: Vec<String>,
        delta: ExecutionGraphDelta,
    },
}

impl ExecutionGraphEvent {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Planned { .. } => "execution_graph.planned",
            Self::Checkpoint { .. } => "execution_graph.checkpoint",
            Self::NodeTransitioned { .. } => "execution_graph.node_transitioned",
            Self::NodeTransitionedAndReplanned { .. } => {
                "execution_graph.node_transitioned_and_replanned"
            }
            Self::CommandApplied { .. } => "execution_graph.command_applied",
            Self::Replanned { .. } => "execution_graph.replanned",
            Self::Recovered { .. } => "execution_graph.recovered",
        }
    }

    pub fn project(&self, current: Option<ExecutionGraph>) -> Result<ExecutionGraph, String> {
        match self {
            Self::Planned { graph } | Self::Checkpoint { graph, .. } => Ok(graph.clone()),
            Self::NodeTransitioned { delta, .. }
            | Self::NodeTransitionedAndReplanned { delta, .. }
            | Self::CommandApplied { delta, .. }
            | Self::Replanned { delta, .. }
            | Self::Recovered { delta, .. } => {
                let mut graph =
                    current.ok_or_else(|| "graph delta has no preceding snapshot".to_string())?;
                delta.apply(&mut graph)?;
                Ok(graph)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{
        ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    };

    use super::*;

    #[test]
    fn delta_round_trip_reconstructs_next_graph() {
        let mut previous = ExecutionGraph::new("test");
        previous.revision = 1;
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "model", "{}");
        previous.nodes.push(node.clone());
        previous
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        let mut next = previous.clone();
        next.revision = 2;
        next.node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Running);
        next.recovery_cursor.node_attempts.insert(node.id, 1);
        let delta = ExecutionGraphDelta::between(&previous, &next);
        let mut projected = previous;
        delta.apply(&mut projected).unwrap();
        assert_eq!(projected, next);
        assert!(serde_json::to_value(delta).unwrap().get("nodes").is_none());
    }
}
