use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::agent_protocol::{
    AgentEvidence, AgentMergeDecision, AgentMessage, AgentNodeStatus, AgentReview, AgentRole,
    AgentTaskNode, ReviewVerdict,
};
use crate::tool_invocation::ToolInvocationRecord;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentGraphError {
    #[error("agent node id is required")]
    MissingNodeId,
    #[error("agent node {0} already exists")]
    DuplicateNode(String),
    #[error("agent node {0} was not found")]
    NodeNotFound(String),
    #[error("agent dependency {0} was not found")]
    DependencyNotFound(String),
    #[error("agent graph dependency cycle detected")]
    DependencyCycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunGraph {
    pub graph_id: String,
    pub session_id: String,
    pub objective: String,
    pub status: AgentNodeStatus,
    #[serde(default)]
    pub nodes: Vec<AgentTaskNode>,
    #[serde(default)]
    pub messages: Vec<AgentMessage>,
    #[serde(default)]
    pub evidence: Vec<AgentEvidence>,
    #[serde(default)]
    pub reviews: Vec<AgentReview>,
    #[serde(default)]
    pub merge_decisions: Vec<AgentMergeDecision>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AgentRunGraph {
    #[must_use]
    pub fn new(session_id: impl Into<String>, objective: impl Into<String>) -> Self {
        let now = now_ms();
        let session_id = session_id.into();
        Self {
            graph_id: format!("agent-graph-{session_id}"),
            session_id,
            objective: objective.into(),
            status: AgentNodeStatus::Running,
            nodes: Vec::new(),
            messages: Vec::new(),
            evidence: Vec::new(),
            reviews: Vec::new(),
            merge_decisions: Vec::new(),
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    #[must_use]
    pub fn from_objective(session_id: impl Into<String>, objective: impl Into<String>) -> Self {
        let mut graph = Self::new(session_id, objective);
        let planner = AgentTaskNode {
            id: "planner".to_string(),
            role: AgentRole::Planner,
            title: "Plan".to_string(),
            objective: graph.objective.clone(),
            depends_on: Vec::new(),
            status: AgentNodeStatus::Ready,
            assigned_agent: None,
            result: None,
            error: None,
            created_at_ms: graph.created_at_ms,
            updated_at_ms: graph.created_at_ms,
        };
        graph.nodes.push(planner);
        graph
    }

    pub fn add_node(&mut self, mut node: AgentTaskNode) -> Result<(), AgentGraphError> {
        if node.id.trim().is_empty() {
            return Err(AgentGraphError::MissingNodeId);
        }
        if self.nodes.iter().any(|existing| existing.id == node.id) {
            return Err(AgentGraphError::DuplicateNode(node.id));
        }
        for dependency in &node.depends_on {
            if !self.nodes.iter().any(|existing| &existing.id == dependency) {
                return Err(AgentGraphError::DependencyNotFound(dependency.clone()));
            }
        }
        if node.status == AgentNodeStatus::Pending && node.depends_on.is_empty() {
            node.status = AgentNodeStatus::Ready;
        }
        self.nodes.push(node);
        self.validate_acyclic()?;
        self.updated_at_ms = now_ms();
        Ok(())
    }

    pub fn upsert_phase_node(
        &mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        objective: impl Into<String>,
    ) -> Result<(), AgentGraphError> {
        let now = now_ms();
        let id = id.into();
        if let Some(node) = self.nodes.iter_mut().find(|node| node.id == id) {
            node.title = title.into();
            node.objective = objective.into();
            node.updated_at_ms = now;
            self.updated_at_ms = now;
            return Ok(());
        }
        let dependency = if self.nodes.iter().any(|node| node.id == "planner") {
            vec!["planner".to_string()]
        } else {
            Vec::new()
        };
        self.add_node(AgentTaskNode {
            id,
            role: AgentRole::Executor,
            title: title.into(),
            objective: objective.into(),
            depends_on: dependency,
            status: AgentNodeStatus::Pending,
            assigned_agent: None,
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        })
    }

    pub fn validate_acyclic(&self) -> Result<(), AgentGraphError> {
        let ids = self
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<HashSet<_>>();
        let mut indegree = HashMap::<&str, usize>::new();
        let mut outgoing = HashMap::<&str, Vec<&str>>::new();
        for node in &self.nodes {
            indegree.entry(node.id.as_str()).or_insert(0);
            for dependency in &node.depends_on {
                if !ids.contains(dependency.as_str()) {
                    return Err(AgentGraphError::DependencyNotFound(dependency.clone()));
                }
                *indegree.entry(node.id.as_str()).or_insert(0) += 1;
                outgoing
                    .entry(dependency.as_str())
                    .or_default()
                    .push(node.id.as_str());
            }
        }

        let mut queue = indegree
            .iter()
            .filter_map(|(id, count)| if *count == 0 { Some(*id) } else { None })
            .collect::<VecDeque<_>>();
        let mut visited = 0usize;
        while let Some(id) = queue.pop_front() {
            visited += 1;
            for target in outgoing.get(id).into_iter().flatten() {
                if let Some(count) = indegree.get_mut(target) {
                    *count -= 1;
                    if *count == 0 {
                        queue.push_back(target);
                    }
                }
            }
        }
        if visited == self.nodes.len() {
            Ok(())
        } else {
            Err(AgentGraphError::DependencyCycle)
        }
    }

    #[must_use]
    pub fn ready_nodes(&self) -> Vec<&AgentTaskNode> {
        let completed = self
            .nodes
            .iter()
            .filter(|node| node.status == AgentNodeStatus::Completed)
            .map(|node| node.id.as_str())
            .collect::<HashSet<_>>();
        self.nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.status,
                    AgentNodeStatus::Pending | AgentNodeStatus::Ready
                ) && node
                    .depends_on
                    .iter()
                    .all(|dependency| completed.contains(dependency.as_str()))
            })
            .collect()
    }

    pub fn record_result(
        &mut self,
        node_id: &str,
        result: impl Into<String>,
    ) -> Result<(), AgentGraphError> {
        let now = now_ms();
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.id == node_id)
            .ok_or_else(|| AgentGraphError::NodeNotFound(node_id.to_string()))?;
        node.result = Some(result.into());
        node.status = AgentNodeStatus::Reviewing;
        node.updated_at_ms = now;
        self.updated_at_ms = now;
        Ok(())
    }

    pub fn record_failure(
        &mut self,
        node_id: &str,
        error: impl Into<String>,
    ) -> Result<(), AgentGraphError> {
        let now = now_ms();
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.id == node_id)
            .ok_or_else(|| AgentGraphError::NodeNotFound(node_id.to_string()))?;
        node.error = Some(error.into());
        node.status = AgentNodeStatus::Failed;
        node.updated_at_ms = now;
        self.status = AgentNodeStatus::Blocked;
        self.updated_at_ms = now;
        Ok(())
    }

    pub fn add_evidence(
        &mut self,
        node_id: &str,
        kind: impl Into<String>,
        reference: impl Into<String>,
        summary: impl Into<String>,
    ) -> Result<AgentEvidence, AgentGraphError> {
        if !self.nodes.iter().any(|node| node.id == node_id) {
            return Err(AgentGraphError::NodeNotFound(node_id.to_string()));
        }
        let evidence = AgentEvidence {
            id: format!("evidence-{}", Uuid::new_v4()),
            node_id: node_id.to_string(),
            kind: kind.into(),
            reference: reference.into(),
            summary: summary.into(),
            created_at_ms: now_ms(),
        };
        self.evidence.push(evidence.clone());
        self.updated_at_ms = evidence.created_at_ms;
        Ok(evidence)
    }

    pub fn add_tool_invocation_evidence(
        &mut self,
        node_id: &str,
        invocation: &ToolInvocationRecord,
    ) -> Result<AgentEvidence, AgentGraphError> {
        self.add_evidence(
            node_id,
            "tool_invocation",
            invocation.evidence_reference(),
            invocation.evidence_summary(),
        )
    }

    pub fn add_review(
        &mut self,
        node_id: &str,
        reviewer: impl Into<String>,
        verdict: ReviewVerdict,
        comment: impl Into<String>,
    ) -> Result<AgentReview, AgentGraphError> {
        let now = now_ms();
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.id == node_id)
            .ok_or_else(|| AgentGraphError::NodeNotFound(node_id.to_string()))?;
        node.status = match verdict {
            ReviewVerdict::Accept => AgentNodeStatus::Completed,
            ReviewVerdict::Challenge => AgentNodeStatus::Blocked,
            ReviewVerdict::Reject => AgentNodeStatus::Failed,
        };
        node.updated_at_ms = now;
        let review = AgentReview {
            id: format!("review-{}", Uuid::new_v4()),
            node_id: node_id.to_string(),
            reviewer: reviewer.into(),
            verdict,
            comment: comment.into(),
            created_at_ms: now,
        };
        self.reviews.push(review.clone());
        if self.nodes.iter().all(|node| {
            matches!(
                node.status,
                AgentNodeStatus::Completed | AgentNodeStatus::Cancelled
            )
        }) {
            self.status = AgentNodeStatus::Completed;
        }
        self.updated_at_ms = now;
        Ok(review)
    }

    pub fn record_merge_decision(
        &mut self,
        node_ids: Vec<String>,
        decision: impl Into<String>,
        conflicts: Vec<String>,
    ) -> AgentMergeDecision {
        let decision = AgentMergeDecision {
            id: format!("merge-{}", Uuid::new_v4()),
            node_ids,
            decision: decision.into(),
            conflicts,
            created_at_ms: now_ms(),
        };
        self.merge_decisions.push(decision.clone());
        self.updated_at_ms = decision.created_at_ms;
        decision
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_invocation::ToolInvocationRecord;
    use crate::tool_orchestrator::ToolSafetyCategory;

    fn node(id: &str, deps: Vec<&str>) -> AgentTaskNode {
        AgentTaskNode {
            id: id.to_string(),
            role: AgentRole::Executor,
            title: id.to_string(),
            objective: id.to_string(),
            depends_on: deps.into_iter().map(str::to_string).collect(),
            status: AgentNodeStatus::Pending,
            assigned_agent: None,
            result: None,
            error: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn agent_graph_rejects_cycle() {
        let mut graph = AgentRunGraph::new("s1", "ship");
        graph.nodes.push(node("a", vec!["b"]));
        graph.nodes.push(node("b", vec!["a"]));

        assert_eq!(
            graph.validate_acyclic().unwrap_err(),
            AgentGraphError::DependencyCycle
        );
    }

    #[test]
    fn agent_task_waits_for_dependencies() {
        let mut graph = AgentRunGraph::new("s1", "ship");
        graph.add_node(node("a", vec![])).unwrap();
        graph.add_node(node("b", vec!["a"])).unwrap();

        let ready = graph
            .ready_nodes()
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ready, vec!["a"]);

        graph
            .add_review("a", "reviewer", ReviewVerdict::Accept, "ok")
            .unwrap();
        let ready = graph
            .ready_nodes()
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ready, vec!["b"]);
    }

    #[test]
    fn agent_review_can_challenge_result() {
        let mut graph = AgentRunGraph::new("s1", "ship");
        graph.add_node(node("a", vec![])).unwrap();
        graph.record_result("a", "done").unwrap();
        let review = graph
            .add_review("a", "reviewer", ReviewVerdict::Challenge, "missing test")
            .unwrap();

        assert_eq!(review.verdict, ReviewVerdict::Challenge);
        assert_eq!(graph.nodes[0].status, AgentNodeStatus::Blocked);
    }

    #[test]
    fn agent_merge_records_conflict() {
        let mut graph = AgentRunGraph::new("s1", "ship");
        graph.add_node(node("a", vec![])).unwrap();
        graph.add_node(node("b", vec![])).unwrap();

        let decision = graph.record_merge_decision(
            vec!["a".to_string(), "b".to_string()],
            "prefer a",
            vec!["different API contracts".to_string()],
        );

        assert_eq!(decision.conflicts.len(), 1);
        assert_eq!(graph.merge_decisions.len(), 1);
    }

    #[test]
    fn agent_failure_isolated_to_node() {
        let mut graph = AgentRunGraph::new("s1", "ship");
        graph.add_node(node("a", vec![])).unwrap();
        graph.add_node(node("b", vec![])).unwrap();
        graph.record_failure("a", "timeout").unwrap();

        assert_eq!(graph.nodes[0].status, AgentNodeStatus::Failed);
        assert_eq!(graph.nodes[1].status, AgentNodeStatus::Ready);
        assert_eq!(graph.status, AgentNodeStatus::Blocked);
    }

    #[test]
    fn agent_tool_invocation_evidence_uses_reference_without_output_copy() {
        let mut graph = AgentRunGraph::new("s1", "ship");
        graph.add_node(node("a", vec![])).unwrap();
        let output = (0..80)
            .map(|idx| format!("line {idx} {}", "x".repeat(24)))
            .collect::<Vec<_>>()
            .join("\n");
        let invocation = ToolInvocationRecord::started(
            "s1",
            1,
            "toolu-large",
            "bash",
            "generate",
            ToolSafetyCategory::WriteLocal,
            100,
        )
        .completed_with_output_policy(&output, 150, 3);

        let evidence = graph
            .add_tool_invocation_evidence("a", &invocation)
            .unwrap();

        assert_eq!(evidence.kind, "tool_invocation");
        assert!(evidence.reference.starts_with("tool-output:toolu-large:"));
        assert!(evidence
            .summary
            .contains("large output indexed by reference"));
        assert!(!evidence.summary.contains("line 79"));
        assert_eq!(graph.evidence.len(), 1);
    }
}
