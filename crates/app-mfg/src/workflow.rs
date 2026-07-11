use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use matrix_core::MatrixEvidencePacket;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{skill_agent_node_id, MfgIncident, MfgSkillPlan, MfgSkillRun};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MfgWorkflowGraphError {
    #[error("MFG workflow id is required")]
    MissingWorkflowId,
    #[error("MFG incident id is required")]
    MissingIncidentId,
    #[error("MFG workflow node id is required")]
    MissingNodeId,
    #[error("MFG workflow node {0} already exists")]
    DuplicateNode(String),
    #[error("MFG workflow node {0} was not found")]
    NodeNotFound(String),
    #[error("MFG workflow dependency {0} was not found")]
    DependencyNotFound(String),
    #[error("MFG workflow dependency cycle detected")]
    DependencyCycle,
    #[error("MFG workflow node {node_id} is not ready; incomplete dependencies: {dependencies:?}")]
    DependenciesIncomplete {
        node_id: String,
        dependencies: Vec<String>,
    },
    #[error("MFG workflow node {node_id} cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        node_id: String,
        from: MfgWorkflowNodeStatus,
        to: MfgWorkflowNodeStatus,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgWorkflowStatus {
    Active,
    Blocked,
    Completed,
    Cancelled,
}

impl MfgWorkflowStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgWorkflowNodeStatus {
    Pending,
    Ready,
    Running,
    Reviewing,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}

impl MfgWorkflowNodeStatus {
    fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Pending, Self::Ready | Self::Cancelled)
                    | (Self::Ready, Self::Running | Self::Cancelled)
                    | (
                        Self::Running,
                        Self::Reviewing | Self::Completed | Self::Failed | Self::Blocked
                    )
                    | (
                        Self::Reviewing,
                        Self::Completed | Self::Blocked | Self::Failed
                    )
                    | (Self::Blocked, Self::Ready | Self::Cancelled)
                    | (Self::Failed, Self::Ready | Self::Cancelled)
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgWorkflowNodeKind {
    Planning,
    EvidenceResearch,
    GovernanceReview,
    DecisionMerge,
    Skill,
    Action,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgWorkflowNode {
    pub node_id: String,
    pub kind: MfgWorkflowNodeKind,
    pub title: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub status: MfgWorkflowNodeStatus,
    #[serde(default)]
    pub assigned_capability: Option<String>,
    #[serde(default)]
    pub result_summary: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MfgWorkflowNode {
    #[must_use]
    pub fn new(
        node_id: impl Into<String>,
        kind: MfgWorkflowNodeKind,
        title: impl Into<String>,
        objective: impl Into<String>,
        depends_on: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            node_id: node_id.into(),
            kind,
            title: title.into(),
            objective: objective.into(),
            status: if depends_on.is_empty() {
                MfgWorkflowNodeStatus::Ready
            } else {
                MfgWorkflowNodeStatus::Pending
            },
            depends_on,
            assigned_capability: None,
            result_summary: None,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgWorkflowEvidence {
    pub evidence_id: String,
    pub node_id: String,
    pub kind: String,
    pub reference: String,
    pub summary: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgWorkflowReviewVerdict {
    Accept,
    Challenge,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgWorkflowReview {
    pub review_id: String,
    pub node_id: String,
    pub reviewer: String,
    pub verdict: MfgWorkflowReviewVerdict,
    pub comment: String,
    pub created_at: DateTime<Utc>,
}

/// Domain-owned workflow state for the MFG application.
///
/// Runtime execution graphs may execute work on behalf of this graph, but are
/// not its persistence model or mutation authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgWorkflowGraph {
    pub workflow_id: String,
    pub incident_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    pub objective: String,
    pub status: MfgWorkflowStatus,
    pub revision: u64,
    #[serde(default)]
    pub nodes: Vec<MfgWorkflowNode>,
    #[serde(default)]
    pub evidence: Vec<MfgWorkflowEvidence>,
    #[serde(default)]
    pub reviews: Vec<MfgWorkflowReview>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MfgWorkflowGraph {
    pub fn for_incident(incident: &MfgIncident) -> Result<Self, MfgWorkflowGraphError> {
        if incident.incident_id.trim().is_empty() {
            return Err(MfgWorkflowGraphError::MissingIncidentId);
        }
        let now = Utc::now();
        let mut graph = Self {
            workflow_id: format!("mfg-workflow-{}", incident.incident_id),
            incident_id: incident.incident_id.clone(),
            task_id: incident.task_id.clone(),
            objective: incident.title.clone(),
            status: MfgWorkflowStatus::Active,
            revision: 0,
            nodes: Vec::new(),
            evidence: Vec::new(),
            reviews: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        graph.add_node(MfgWorkflowNode::new(
            "planner",
            MfgWorkflowNodeKind::Planning,
            "Incident plan",
            incident.title.clone(),
            Vec::new(),
        ))?;
        Ok(graph)
    }

    pub fn validate(&self) -> Result<(), MfgWorkflowGraphError> {
        if self.workflow_id.trim().is_empty() {
            return Err(MfgWorkflowGraphError::MissingWorkflowId);
        }
        if self.incident_id.trim().is_empty() {
            return Err(MfgWorkflowGraphError::MissingIncidentId);
        }
        let ids = self
            .nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<HashSet<_>>();
        if ids.len() != self.nodes.len() {
            let duplicate = self
                .nodes
                .iter()
                .find(|node| {
                    self.nodes
                        .iter()
                        .filter(|candidate| candidate.node_id == node.node_id)
                        .count()
                        > 1
                })
                .map(|node| node.node_id.clone())
                .unwrap_or_default();
            return Err(MfgWorkflowGraphError::DuplicateNode(duplicate));
        }
        let mut indegree = HashMap::<&str, usize>::new();
        let mut outgoing = HashMap::<&str, Vec<&str>>::new();
        for node in &self.nodes {
            if node.node_id.trim().is_empty() {
                return Err(MfgWorkflowGraphError::MissingNodeId);
            }
            indegree.entry(node.node_id.as_str()).or_insert(0);
            for dependency in &node.depends_on {
                if !ids.contains(dependency.as_str()) {
                    return Err(MfgWorkflowGraphError::DependencyNotFound(
                        dependency.clone(),
                    ));
                }
                *indegree.entry(node.node_id.as_str()).or_insert(0) += 1;
                outgoing
                    .entry(dependency.as_str())
                    .or_default()
                    .push(node.node_id.as_str());
            }
        }
        let mut queue = indegree
            .iter()
            .filter_map(|(id, count)| (*count == 0).then_some(*id))
            .collect::<VecDeque<_>>();
        let mut visited = 0;
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
            Err(MfgWorkflowGraphError::DependencyCycle)
        }
    }

    pub fn add_node(&mut self, node: MfgWorkflowNode) -> Result<(), MfgWorkflowGraphError> {
        if node.node_id.trim().is_empty() {
            return Err(MfgWorkflowGraphError::MissingNodeId);
        }
        if self.nodes.iter().any(|item| item.node_id == node.node_id) {
            return Err(MfgWorkflowGraphError::DuplicateNode(node.node_id));
        }
        for dependency in &node.depends_on {
            if !self.nodes.iter().any(|item| &item.node_id == dependency) {
                return Err(MfgWorkflowGraphError::DependencyNotFound(
                    dependency.clone(),
                ));
            }
        }
        self.nodes.push(node);
        if let Err(error) = self.validate() {
            self.nodes.pop();
            return Err(error);
        }
        self.touch();
        Ok(())
    }

    pub fn attach_evidence_packet(
        &mut self,
        packet: &MatrixEvidencePacket,
    ) -> Result<(), MfgWorkflowGraphError> {
        self.ensure_node(MfgWorkflowNode::new(
            "mfg_researcher",
            MfgWorkflowNodeKind::EvidenceResearch,
            "MFG evidence research",
            "Validate structured evidence and identify missing evidence",
            vec!["planner".to_string()],
        ))?;
        self.ensure_node(MfgWorkflowNode::new(
            "mfg_reviewer",
            MfgWorkflowNodeKind::GovernanceReview,
            "MFG insight review",
            "Review confidence, conflicts, and governance readiness",
            vec!["mfg_researcher".to_string()],
        ))?;
        self.ensure_node(MfgWorkflowNode::new(
            "mfg_merger",
            MfgWorkflowNodeKind::DecisionMerge,
            "MFG decision merge",
            "Merge findings into one governed operating decision",
            vec!["mfg_reviewer".to_string()],
        ))?;
        let reference = format!("mfg:evidence:{}", packet.packet_id);
        self.add_evidence(
            "planner",
            "structured_evidence_packet",
            reference.clone(),
            packet.problem_statement.clone(),
        )?;
        self.add_evidence(
            "mfg_researcher",
            "structured_evidence_packet",
            reference,
            format!(
                "metric_evidence={}, change_evidence={}, missing_evidence={}",
                packet.metric_evidence.len(),
                packet.change_evidence.len(),
                packet.missing_evidence.len()
            ),
        )?;
        Ok(())
    }

    pub fn plan_skills(&mut self, plan: &MfgSkillPlan) -> Result<(), MfgWorkflowGraphError> {
        let dependency = if self.nodes.iter().any(|node| node.node_id == "mfg_reviewer") {
            "mfg_reviewer"
        } else {
            "planner"
        };
        for skill in &plan.selected_skills {
            let node_id = skill_agent_node_id(&skill.skill_id);
            let mut node = MfgWorkflowNode::new(
                node_id.clone(),
                MfgWorkflowNodeKind::Skill,
                skill.role.clone(),
                skill.analysis_method.clone(),
                vec![dependency.to_string()],
            );
            node.assigned_capability = Some(skill.skill_id.clone());
            self.ensure_node(node)?;
            self.add_evidence(
                &node_id,
                "mfg_skill_manifest",
                format!("mfg:skill:{}", skill.skill_id),
                format!(
                    "inputs={}, metrics={}, evidence={}",
                    skill.input_fact_types.join(","),
                    skill.input_metric_keys.join(","),
                    skill.required_evidence.join(",")
                ),
            )?;
        }
        Ok(())
    }

    pub fn complete_skill(&mut self, run: &MfgSkillRun) -> Result<(), MfgWorkflowGraphError> {
        let node_id = run
            .agent_node_id
            .clone()
            .unwrap_or_else(|| skill_agent_node_id(&run.skill_id));
        if !self.nodes.iter().any(|node| node.node_id == node_id) {
            let planner_completed = self.nodes.iter().any(|node| {
                node.node_id == "planner" && node.status == MfgWorkflowNodeStatus::Completed
            });
            if !planner_completed {
                return Err(MfgWorkflowGraphError::DependenciesIncomplete {
                    node_id,
                    dependencies: vec!["planner".to_string()],
                });
            }
            let mut node = MfgWorkflowNode::new(
                node_id.clone(),
                MfgWorkflowNodeKind::Skill,
                run.skill_id.clone(),
                format!("Execute MFG skill {}", run.skill_id),
                vec!["planner".to_string()],
            );
            node.assigned_capability = Some(run.skill_id.clone());
            self.add_node(node)?;
        }
        if let Some(node) = self.nodes.iter().find(|node| node.node_id == node_id) {
            if node.status == MfgWorkflowNodeStatus::Completed {
                return if node.result_summary.as_deref() == Some(run.summary.as_str()) {
                    Ok(())
                } else {
                    Err(MfgWorkflowGraphError::InvalidTransition {
                        node_id,
                        from: MfgWorkflowNodeStatus::Completed,
                        to: MfgWorkflowNodeStatus::Completed,
                    })
                };
            }
        }
        self.ensure_dependencies_completed(&node_id)?;
        let status = self
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| node.status)
            .ok_or_else(|| MfgWorkflowGraphError::NodeNotFound(node_id.clone()))?;
        if !matches!(
            status,
            MfgWorkflowNodeStatus::Pending
                | MfgWorkflowNodeStatus::Ready
                | MfgWorkflowNodeStatus::Running
        ) {
            return Err(MfgWorkflowGraphError::InvalidTransition {
                node_id,
                from: status,
                to: MfgWorkflowNodeStatus::Completed,
            });
        }
        self.set_node_terminal_result(&node_id, run.summary.clone())?;
        self.add_evidence(
            &node_id,
            "mfg_skill_run",
            run.execution_id.as_ref().map_or_else(
                || format!("mfg:skill-run:{}:{}", self.incident_id, run.skill_id),
                |execution_id| format!("mfg:skill-run:{execution_id}"),
            ),
            run.summary.clone(),
        )?;
        Ok(())
    }

    pub fn transition_node(
        &mut self,
        node_id: &str,
        next: MfgWorkflowNodeStatus,
    ) -> Result<(), MfgWorkflowGraphError> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| MfgWorkflowGraphError::NodeNotFound(node_id.to_string()))?;
        if !node.status.can_transition_to(next) {
            return Err(MfgWorkflowGraphError::InvalidTransition {
                node_id: node_id.to_string(),
                from: node.status,
                to: next,
            });
        }
        node.status = next;
        node.updated_at = Utc::now();
        self.recalculate_status();
        self.touch();
        Ok(())
    }

    pub fn set_node_terminal_result(
        &mut self,
        node_id: &str,
        summary: impl Into<String>,
    ) -> Result<(), MfgWorkflowGraphError> {
        self.ensure_dependencies_completed(node_id)?;
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| MfgWorkflowGraphError::NodeNotFound(node_id.to_string()))?;
        let summary = summary.into();
        if node.status == MfgWorkflowNodeStatus::Completed {
            return if node.result_summary.as_deref() == Some(summary.as_str()) {
                Ok(())
            } else {
                Err(MfgWorkflowGraphError::InvalidTransition {
                    node_id: node_id.to_string(),
                    from: node.status,
                    to: MfgWorkflowNodeStatus::Completed,
                })
            };
        }
        if !matches!(
            node.status,
            MfgWorkflowNodeStatus::Pending
                | MfgWorkflowNodeStatus::Ready
                | MfgWorkflowNodeStatus::Running
                | MfgWorkflowNodeStatus::Reviewing
        ) {
            return Err(MfgWorkflowGraphError::InvalidTransition {
                node_id: node_id.to_string(),
                from: node.status,
                to: MfgWorkflowNodeStatus::Completed,
            });
        }
        node.result_summary = Some(summary);
        node.error = None;
        node.status = MfgWorkflowNodeStatus::Completed;
        node.updated_at = Utc::now();
        self.recalculate_status();
        self.touch();
        Ok(())
    }

    pub fn record_failure(
        &mut self,
        node_id: &str,
        error: impl Into<String>,
    ) -> Result<(), MfgWorkflowGraphError> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| MfgWorkflowGraphError::NodeNotFound(node_id.to_string()))?;
        node.error = Some(error.into());
        node.status = MfgWorkflowNodeStatus::Failed;
        node.updated_at = Utc::now();
        self.recalculate_status();
        self.touch();
        Ok(())
    }

    pub fn add_evidence(
        &mut self,
        node_id: &str,
        kind: impl Into<String>,
        reference: impl Into<String>,
        summary: impl Into<String>,
    ) -> Result<MfgWorkflowEvidence, MfgWorkflowGraphError> {
        if !self.nodes.iter().any(|node| node.node_id == node_id) {
            return Err(MfgWorkflowGraphError::NodeNotFound(node_id.to_string()));
        }
        let kind = kind.into();
        let reference = reference.into();
        if let Some(existing) = self.evidence.iter().find(|item| {
            item.node_id == node_id && item.kind == kind && item.reference == reference
        }) {
            return Ok(existing.clone());
        }
        let evidence = MfgWorkflowEvidence {
            evidence_id: format!("mfg-evidence-{}", Uuid::new_v4()),
            node_id: node_id.to_string(),
            kind,
            reference,
            summary: summary.into(),
            created_at: Utc::now(),
        };
        self.evidence.push(evidence.clone());
        self.touch();
        Ok(evidence)
    }

    pub fn add_review(
        &mut self,
        node_id: &str,
        reviewer: impl Into<String>,
        verdict: MfgWorkflowReviewVerdict,
        comment: impl Into<String>,
    ) -> Result<MfgWorkflowReview, MfgWorkflowGraphError> {
        let next = match verdict {
            MfgWorkflowReviewVerdict::Accept => MfgWorkflowNodeStatus::Completed,
            MfgWorkflowReviewVerdict::Challenge => MfgWorkflowNodeStatus::Blocked,
            MfgWorkflowReviewVerdict::Reject => MfgWorkflowNodeStatus::Failed,
        };
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| MfgWorkflowGraphError::NodeNotFound(node_id.to_string()))?;
        node.status = next;
        node.updated_at = Utc::now();
        let review = MfgWorkflowReview {
            review_id: format!("mfg-review-{}", Uuid::new_v4()),
            node_id: node_id.to_string(),
            reviewer: reviewer.into(),
            verdict,
            comment: comment.into(),
            created_at: Utc::now(),
        };
        self.reviews.push(review.clone());
        self.recalculate_status();
        self.touch();
        Ok(review)
    }

    #[must_use]
    pub fn ready_nodes(&self) -> Vec<&MfgWorkflowNode> {
        let completed = self
            .nodes
            .iter()
            .filter(|node| node.status == MfgWorkflowNodeStatus::Completed)
            .map(|node| node.node_id.as_str())
            .collect::<HashSet<_>>();
        self.nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.status,
                    MfgWorkflowNodeStatus::Pending | MfgWorkflowNodeStatus::Ready
                ) && node
                    .depends_on
                    .iter()
                    .all(|dependency| completed.contains(dependency.as_str()))
            })
            .collect()
    }

    fn ensure_node(&mut self, node: MfgWorkflowNode) -> Result<(), MfgWorkflowGraphError> {
        if self.nodes.iter().any(|item| item.node_id == node.node_id) {
            return Ok(());
        }
        self.add_node(node)
    }

    fn ensure_dependencies_completed(&self, node_id: &str) -> Result<(), MfgWorkflowGraphError> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| MfgWorkflowGraphError::NodeNotFound(node_id.to_string()))?;
        let incomplete = node
            .depends_on
            .iter()
            .filter(|dependency| {
                !self.nodes.iter().any(|candidate| {
                    candidate.node_id == **dependency
                        && candidate.status == MfgWorkflowNodeStatus::Completed
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if incomplete.is_empty() {
            Ok(())
        } else {
            Err(MfgWorkflowGraphError::DependenciesIncomplete {
                node_id: node_id.to_string(),
                dependencies: incomplete,
            })
        }
    }

    fn recalculate_status(&mut self) {
        self.status = if self.nodes.iter().all(|node| {
            matches!(
                node.status,
                MfgWorkflowNodeStatus::Completed | MfgWorkflowNodeStatus::Cancelled
            )
        }) {
            MfgWorkflowStatus::Completed
        } else if self.nodes.iter().any(|node| {
            matches!(
                node.status,
                MfgWorkflowNodeStatus::Blocked | MfgWorkflowNodeStatus::Failed
            )
        }) {
            MfgWorkflowStatus::Blocked
        } else {
            MfgWorkflowStatus::Active
        };
    }

    fn touch(&mut self) {
        self.revision = self.revision.saturating_add(1);
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run_server_manufacturing_skill, server_manufacturing_skill_pack};

    #[test]
    fn graph_uses_mfg_domain_types_and_rejects_cross_incident_identity() {
        let first = MfgIncident::new("GPU shortage");
        let second = MfgIncident::new("Quality escape");
        let first_graph = MfgWorkflowGraph::for_incident(&first).unwrap();
        let second_graph = MfgWorkflowGraph::for_incident(&second).unwrap();

        assert_ne!(first_graph.workflow_id, second_graph.workflow_id);
        assert_ne!(first_graph.incident_id, second_graph.incident_id);
        assert_eq!(first_graph.nodes[0].kind, MfgWorkflowNodeKind::Planning);
        let json = serde_json::to_value(&first_graph).unwrap();
        assert!(json.get("session_id").is_none());
        assert!(json.get("messages").is_none());
        assert!(json.get("merge_decisions").is_none());
    }

    #[test]
    fn graph_rejects_invalid_dependency_and_cycle() {
        let incident = MfgIncident::new("Capacity risk");
        let mut graph = MfgWorkflowGraph::for_incident(&incident).unwrap();
        let error = graph
            .add_node(MfgWorkflowNode::new(
                "analysis",
                MfgWorkflowNodeKind::EvidenceResearch,
                "Analysis",
                "Analyze capacity",
                vec!["missing".to_string()],
            ))
            .unwrap_err();
        assert_eq!(
            error,
            MfgWorkflowGraphError::DependencyNotFound("missing".to_string())
        );

        graph.nodes[0].depends_on = vec!["planner".to_string()];
        assert_eq!(
            graph.validate().unwrap_err(),
            MfgWorkflowGraphError::DependencyCycle
        );
    }

    #[test]
    fn terminal_completion_requires_dependencies_and_replay_is_idempotent() {
        let incident = MfgIncident::new("Dependency guard");
        let mut graph = MfgWorkflowGraph::for_incident(&incident).unwrap();
        graph
            .add_node(MfgWorkflowNode::new(
                "worker",
                MfgWorkflowNodeKind::Skill,
                "Worker",
                "Run after planning",
                vec!["planner".to_string()],
            ))
            .unwrap();

        assert!(matches!(
            graph
                .set_node_terminal_result("worker", "done")
                .unwrap_err(),
            MfgWorkflowGraphError::DependenciesIncomplete { .. }
        ));
        graph
            .set_node_terminal_result("planner", "planned")
            .unwrap();
        graph.set_node_terminal_result("worker", "done").unwrap();
        let revision = graph.revision;
        graph.set_node_terminal_result("worker", "done").unwrap();
        assert_eq!(graph.revision, revision);
        assert!(graph
            .set_node_terminal_result("worker", "different")
            .is_err());
    }

    #[test]
    fn rejected_unplanned_skill_completion_does_not_mutate_graph() {
        let incident = MfgIncident::new("Dependency guard");
        let mut graph = MfgWorkflowGraph::for_incident(&incident).unwrap();
        let before = graph.clone();
        let skill = server_manufacturing_skill_pack().remove(0);
        let run = run_server_manufacturing_skill(&incident, &skill, None, None);
        assert!(matches!(
            graph.complete_skill(&run).unwrap_err(),
            MfgWorkflowGraphError::DependenciesIncomplete { .. }
        ));
        assert_eq!(graph, before);
    }

    #[test]
    fn domain_mutators_cover_evidence_skill_plan_and_skill_completion() {
        let incident = MfgIncident::new("GPU shortage");
        let mut graph = MfgWorkflowGraph::for_incident(&incident).unwrap();
        let packet = MatrixEvidencePacket::new("GPU shortage affects weekly build");
        graph.attach_evidence_packet(&packet).unwrap();

        let skill = server_manufacturing_skill_pack().remove(0);
        let plan = MfgSkillPlan {
            incident_id: incident.incident_id.clone(),
            selected_skills: vec![skill.clone()],
            evidence_requirements: skill.required_evidence.clone(),
            planned_agent_nodes: vec![skill_agent_node_id(&skill.skill_id)],
        };
        graph.plan_skills(&plan).unwrap();
        graph
            .set_node_terminal_result("planner", "planned")
            .unwrap();
        graph
            .set_node_terminal_result("mfg_researcher", "researched")
            .unwrap();
        graph
            .set_node_terminal_result("mfg_reviewer", "reviewed")
            .unwrap();
        let run = run_server_manufacturing_skill(&incident, &skill, None, Some(&packet));
        graph.complete_skill(&run).unwrap();

        let skill_node_id = skill_agent_node_id(&skill.skill_id);
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.node_id == "mfg_researcher"));
        assert_eq!(
            graph
                .nodes
                .iter()
                .find(|node| node.node_id == skill_node_id)
                .unwrap()
                .status,
            MfgWorkflowNodeStatus::Completed
        );
        assert!(graph
            .evidence
            .iter()
            .any(|item| item.kind == "mfg_skill_run"));
    }
}
