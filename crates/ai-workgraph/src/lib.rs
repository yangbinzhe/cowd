//! Controlled work graph for multi-agent and parallel execution.

use std::collections::{BTreeMap, BTreeSet};

use ai_core::{AiKernelError, AiKernelResult, KernelRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkNodeKind {
    AgentTask,
    ToolTask,
    ReadOnlyFanout,
    Review,
    Synthesis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkNodeStatus {
    Planned,
    Ready,
    Running,
    Completed,
    Blocked,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkEdgeKind {
    DependsOn,
    Verifies,
    Reviews,
    Produces,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkNode {
    pub id: String,
    pub kind: WorkNodeKind,
    pub label: String,
    pub objective: String,
    pub status: WorkNodeStatus,
    pub agent_id: Option<String>,
    pub refs: Vec<KernelRef>,
}

impl WorkNode {
    #[must_use]
    pub fn new(kind: WorkNodeKind, label: impl Into<String>, objective: impl Into<String>) -> Self {
        let label = label.into();
        Self {
            id: format!("work-node-{}", uuid::Uuid::new_v4()),
            kind,
            label,
            objective: objective.into(),
            status: WorkNodeStatus::Planned,
            agent_id: None,
            refs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkEdge {
    pub from: String,
    pub to: String,
    pub kind: WorkEdgeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentContract {
    pub agent_id: String,
    pub objective: String,
    pub allowed_tools: Vec<String>,
    pub required_return: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReturnPacket {
    pub agent_id: String,
    pub result_summary: String,
    pub evidence: Vec<String>,
    pub conflicts: Vec<String>,
    pub failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkGraph {
    pub id: String,
    pub objective: String,
    pub nodes: Vec<WorkNode>,
    pub edges: Vec<WorkEdge>,
}

impl WorkGraph {
    #[must_use]
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            id: format!("workgraph-{}", uuid::Uuid::new_v4()),
            objective: objective.into(),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node: WorkNode) -> AiKernelResult<String> {
        if self.nodes.iter().any(|existing| existing.id == node.id) {
            return Err(AiKernelError::Conflict(format!(
                "duplicate work node {}",
                node.id
            )));
        }
        let id = node.id.clone();
        self.nodes.push(node);
        Ok(id)
    }

    pub fn add_edge(
        &mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        kind: WorkEdgeKind,
    ) -> AiKernelResult<()> {
        let edge = WorkEdge {
            from: from.into(),
            to: to.into(),
            kind,
        };
        self.ensure_node(&edge.from)?;
        self.ensure_node(&edge.to)?;
        self.edges.push(edge);
        self.topological_batches()?;
        Ok(())
    }

    pub fn mark_completed(&mut self, node_id: &str) -> AiKernelResult<()> {
        self.set_status(node_id, WorkNodeStatus::Completed)
    }

    pub fn mark_failed(&mut self, node_id: &str) -> AiKernelResult<()> {
        self.set_status(node_id, WorkNodeStatus::Failed)?;
        let blocked = self
            .edges
            .iter()
            .filter(|edge| edge.from == node_id && edge.kind == WorkEdgeKind::DependsOn)
            .map(|edge| edge.to.clone())
            .collect::<Vec<_>>();
        for id in blocked {
            self.set_status(&id, WorkNodeStatus::Blocked)?;
        }
        Ok(())
    }

    #[must_use]
    pub fn ready_nodes(&self) -> Vec<&WorkNode> {
        let completed = self
            .nodes
            .iter()
            .filter(|node| node.status == WorkNodeStatus::Completed)
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();
        self.nodes
            .iter()
            .filter(|node| {
                matches!(node.status, WorkNodeStatus::Planned | WorkNodeStatus::Ready)
                    && self
                        .dependencies_for(&node.id)
                        .iter()
                        .all(|dependency| completed.contains(dependency.as_str()))
            })
            .collect()
    }

    pub fn topological_batches(&self) -> AiKernelResult<Vec<Vec<String>>> {
        let ids = self
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<BTreeSet<_>>();
        let mut indegree = ids
            .iter()
            .map(|id| (id.clone(), 0usize))
            .collect::<BTreeMap<_, _>>();
        let mut outgoing = BTreeMap::<String, Vec<String>>::new();
        for edge in self
            .edges
            .iter()
            .filter(|edge| edge.kind == WorkEdgeKind::DependsOn)
        {
            if !ids.contains(&edge.from) || !ids.contains(&edge.to) {
                return Err(AiKernelError::InvalidInput(
                    "workgraph edge references missing node".to_string(),
                ));
            }
            *indegree.entry(edge.to.clone()).or_insert(0) += 1;
            outgoing
                .entry(edge.from.clone())
                .or_default()
                .push(edge.to.clone());
        }

        let mut frontier = indegree
            .iter()
            .filter_map(|(id, count)| (*count == 0).then(|| id.clone()))
            .collect::<Vec<_>>();
        let mut visited = 0usize;
        let mut batches = Vec::new();
        while !frontier.is_empty() {
            frontier.sort();
            let batch = frontier;
            visited += batch.len();
            let mut next = Vec::new();
            for id in &batch {
                for target in outgoing.get(id).into_iter().flatten() {
                    let count = indegree
                        .get_mut(target)
                        .ok_or_else(|| AiKernelError::Internal("missing indegree".to_string()))?;
                    *count -= 1;
                    if *count == 0 {
                        next.push(target.clone());
                    }
                }
            }
            batches.push(batch);
            frontier = next;
        }
        if visited == self.nodes.len() {
            Ok(batches)
        } else {
            Err(AiKernelError::Conflict(
                "workgraph dependency cycle detected".to_string(),
            ))
        }
    }

    fn ensure_node(&self, node_id: &str) -> AiKernelResult<()> {
        self.nodes
            .iter()
            .any(|node| node.id == node_id)
            .then_some(())
            .ok_or_else(|| AiKernelError::InvalidInput(format!("work node {node_id} not found")))
    }

    fn set_status(&mut self, node_id: &str, status: WorkNodeStatus) -> AiKernelResult<()> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.id == node_id)
            .ok_or_else(|| AiKernelError::InvalidInput(format!("work node {node_id} not found")))?;
        node.status = status;
        Ok(())
    }

    fn dependencies_for(&self, node_id: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|edge| edge.to == node_id && edge.kind == WorkEdgeKind::DependsOn)
            .map(|edge| edge.from.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(label: &str) -> WorkNode {
        WorkNode::new(WorkNodeKind::AgentTask, label, label)
    }

    #[test]
    fn topological_batches_allow_parallel_ready_nodes() {
        let mut graph = WorkGraph::new("parallel analysis");
        let a = graph.add_node(node("a")).unwrap();
        let b = graph.add_node(node("b")).unwrap();
        let c = graph.add_node(node("c")).unwrap();
        graph.add_edge(&a, &c, WorkEdgeKind::DependsOn).unwrap();
        graph.add_edge(&b, &c, WorkEdgeKind::DependsOn).unwrap();

        let batches = graph.topological_batches().unwrap();
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[1], vec![c]);
    }

    #[test]
    fn cycle_is_rejected() {
        let mut graph = WorkGraph::new("cycle");
        let a = graph.add_node(node("a")).unwrap();
        let b = graph.add_node(node("b")).unwrap();
        graph.add_edge(&a, &b, WorkEdgeKind::DependsOn).unwrap();

        let error = graph.add_edge(&b, &a, WorkEdgeKind::DependsOn).unwrap_err();
        assert_eq!(error.kind(), "conflict");
    }

    #[test]
    fn failed_node_blocks_dependents() {
        let mut graph = WorkGraph::new("failure");
        let a = graph.add_node(node("a")).unwrap();
        let b = graph.add_node(node("b")).unwrap();
        graph.add_edge(&a, &b, WorkEdgeKind::DependsOn).unwrap();

        graph.mark_failed(&a).unwrap();

        assert_eq!(
            graph.nodes.iter().find(|node| node.id == b).unwrap().status,
            WorkNodeStatus::Blocked
        );
    }
}
