// M6: SubAgentExecutor + TaskGraph — agent delegation system.
// Derived from GenericAgent's BaseHandler.dispatch() loop.

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

pub type TaskId = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus { Pending, Running, Completed, Failed(String) }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNode {
    pub id: TaskId,
    pub description: String,
    pub dependencies: Vec<TaskId>,
    pub status: TaskStatus,
    pub allowed_tools: Vec<String>,
    pub max_turns: usize,
    pub budget_tokens: usize,
    pub result: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskGraph {
    pub nodes: Vec<TaskNode>,
}

impl TaskGraph {
    pub fn new() -> Self { Self { nodes: Vec::new() } }
    pub fn add(&mut self, node: TaskNode) { self.nodes.push(node); }

    /// Nodes with all dependencies satisfied and not yet started.
    pub fn ready_nodes(&self) -> Vec<&TaskNode> {
        let completed: HashSet<&TaskId> = self.nodes.iter()
            .filter(|n| n.status == TaskStatus::Completed)
            .map(|n| &n.id).collect();
        self.nodes.iter()
            .filter(|n| n.status == TaskStatus::Pending)
            .filter(|n| n.dependencies.iter().all(|d| completed.contains(d)))
            .collect()
    }

    /// Topological sort by dependency count.
    pub fn execution_order(&self) -> Vec<&TaskNode> {
        let mut order: Vec<&TaskNode> = self.nodes.iter().collect();
        order.sort_by_key(|n| n.dependencies.len());
        order
    }

    pub fn completed_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.status == TaskStatus::Completed).count()
    }

    pub fn total_count(&self) -> usize { self.nodes.len() }
}

#[derive(Debug, Clone)]
pub struct SubAgentConfig {
    pub task_description: String,
    pub allowed_tools: Vec<String>,
    pub max_turns: usize,
    pub budget_tokens: usize,
}

impl Default for SubAgentConfig {
    fn default() -> Self {
        Self { task_description: String::new(), allowed_tools: vec![], max_turns: 10, budget_tokens: 20_000 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m6_topological_sort_by_dependencies() {
        let mut g = TaskGraph::new();
        g.add(TaskNode { id: "A".into(), description: "A".into(), dependencies: vec![], status: TaskStatus::Pending, allowed_tools: vec![], max_turns: 10, budget_tokens: 1000, result: None });
        g.add(TaskNode { id: "B".into(), description: "B".into(), dependencies: vec!["A".into()], status: TaskStatus::Pending, allowed_tools: vec![], max_turns: 10, budget_tokens: 1000, result: None });
        g.add(TaskNode { id: "C".into(), description: "C".into(), dependencies: vec!["B".into()], status: TaskStatus::Pending, allowed_tools: vec![], max_turns: 10, budget_tokens: 1000, result: None });
        let order = g.execution_order();
        let idx_a = order.iter().position(|n| n.id == "A").unwrap();
        let idx_b = order.iter().position(|n| n.id == "B").unwrap();
        assert!(idx_a < idx_b, "A (fewer deps) should come before B in topological sort");
    }

    #[test]
    fn m6_ready_nodes_only_returns_deps_satisfied() {
        let mut g = TaskGraph::new();
        g.add(TaskNode { id: "X".into(), description: "X".into(), dependencies: vec![], status: TaskStatus::Completed, allowed_tools: vec![], max_turns: 10, budget_tokens: 1000, result: None });
        g.add(TaskNode { id: "Y".into(), description: "Y".into(), dependencies: vec!["X".into()], status: TaskStatus::Pending, allowed_tools: vec![], max_turns: 10, budget_tokens: 1000, result: None });
        let ready = g.ready_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "Y");
    }
}
