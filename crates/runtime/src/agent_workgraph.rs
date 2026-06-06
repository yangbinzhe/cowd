//! Agent work graph projection.
//!
//! This module turns the existing collaboration board, subtasks, and agent
//! traces into a compact graph that can be persisted or exposed to UI without
//! coupling consumers to the orchestration internals.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::agent_collaboration::{
    AgentTaskTrace, CollaborationReviewPacket, CollaborationScorecard, CollaborationTask,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkGraphStatus {
    Planned,
    Running,
    Completed,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkGraphNodeKind {
    AgentTask,
    ToolTask,
    Review,
    MemoryTask,
    Synthesis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkGraphEdgeKind {
    DependsOn,
    Verifies,
    Reviews,
    Blocks,
    Produces,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkGraphRef {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkGraphNode {
    pub node_id: String,
    pub kind: WorkGraphNodeKind,
    pub label: String,
    pub objective: String,
    pub agent_id: Option<String>,
    pub status: String,
    #[serde(default)]
    pub refs: Vec<WorkGraphRef>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkGraphEdge {
    pub from_node_id: String,
    pub to_node_id: String,
    pub kind: WorkGraphEdgeKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentWorkGraph {
    pub graph_id: String,
    pub session_id: String,
    pub objective: String,
    pub nodes: Vec<WorkGraphNode>,
    pub edges: Vec<WorkGraphEdge>,
    pub board_id: Option<String>,
    pub scorecard: Option<CollaborationScorecard>,
    pub status: WorkGraphStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AgentWorkGraph {
    #[must_use]
    pub fn from_collaboration_task(
        session_id: impl Into<String>,
        task: &CollaborationTask,
    ) -> Self {
        let now = current_time_ms();
        let graph_id = format!("workgraph-{}", Uuid::new_v4());
        let nodes = task
            .subtasks
            .iter()
            .map(|subtask| WorkGraphNode {
                node_id: subtask.id.clone(),
                kind: WorkGraphNodeKind::AgentTask,
                label: subtask.id.clone(),
                objective: subtask.description.clone(),
                agent_id: None,
                status: "planned".to_string(),
                refs: vec![WorkGraphRef {
                    ref_type: "workgraph".to_string(),
                    id: graph_id.clone(),
                }],
                created_at_ms: now,
                updated_at_ms: now,
            })
            .collect::<Vec<_>>();
        let edges = task
            .subtasks
            .iter()
            .flat_map(|subtask| {
                subtask.depends_on.iter().map(|dependency| WorkGraphEdge {
                    from_node_id: dependency.clone(),
                    to_node_id: subtask.id.clone(),
                    kind: WorkGraphEdgeKind::DependsOn,
                })
            })
            .collect();

        Self {
            graph_id,
            session_id: session_id.into(),
            objective: task.description.clone(),
            nodes,
            edges,
            board_id: None,
            scorecard: None,
            status: WorkGraphStatus::Planned,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    #[must_use]
    pub fn with_review_packet(mut self, packet: &CollaborationReviewPacket) -> Self {
        let now = current_time_ms();
        self.board_id = Some(packet.board_id.clone());
        self.scorecard = Some(packet.scorecard.clone());
        self.updated_at_ms = now;
        self.status = if packet
            .agent_tasks
            .iter()
            .any(|trace| trace.status.eq_ignore_ascii_case("failed"))
        {
            WorkGraphStatus::Degraded
        } else {
            WorkGraphStatus::Completed
        };

        for trace in &packet.agent_tasks {
            self.upsert_trace_node(trace, now);
        }
        self.ensure_synthesis_node(&packet.board_id, now);
        self
    }

    fn upsert_trace_node(&mut self, trace: &AgentTaskTrace, now: u64) {
        let node_id = trace.task_id.clone();
        let refs = trace_refs(trace, self.graph_id.as_str());
        if let Some(node) = self.nodes.iter_mut().find(|node| node.node_id == node_id) {
            node.agent_id = trace.agent_run_id.clone();
            node.status = trace.status.clone();
            node.refs = refs;
            node.updated_at_ms = now;
            return;
        }

        self.nodes.push(WorkGraphNode {
            node_id,
            kind: WorkGraphNodeKind::AgentTask,
            label: trace.role.clone(),
            objective: trace.objective.clone(),
            agent_id: trace.agent_run_id.clone(),
            status: trace.status.clone(),
            refs,
            created_at_ms: trace.created_at_ms,
            updated_at_ms: now,
        });
    }

    fn ensure_synthesis_node(&mut self, board_id: &str, now: u64) {
        let node_id = format!("synthesis-{board_id}");
        if self.nodes.iter().any(|node| node.node_id == node_id) {
            return;
        }
        self.nodes.push(WorkGraphNode {
            node_id: node_id.clone(),
            kind: WorkGraphNodeKind::Synthesis,
            label: "synthesis".to_string(),
            objective: "merge agent outputs and score collaboration lift".to_string(),
            agent_id: None,
            status: "completed".to_string(),
            refs: vec![
                WorkGraphRef {
                    ref_type: "workgraph".to_string(),
                    id: self.graph_id.clone(),
                },
                WorkGraphRef {
                    ref_type: "collaboration_board".to_string(),
                    id: board_id.to_string(),
                },
            ],
            created_at_ms: now,
            updated_at_ms: now,
        });

        for node in &self.nodes {
            if node.kind == WorkGraphNodeKind::AgentTask && node.node_id != node_id {
                self.edges.push(WorkGraphEdge {
                    from_node_id: node.node_id.clone(),
                    to_node_id: node_id.clone(),
                    kind: WorkGraphEdgeKind::Produces,
                });
            }
        }
    }
}

fn trace_refs(trace: &AgentTaskTrace, graph_id: &str) -> Vec<WorkGraphRef> {
    let mut refs = vec![
        WorkGraphRef {
            ref_type: "workgraph".to_string(),
            id: graph_id.to_string(),
        },
        WorkGraphRef {
            ref_type: "collaboration_board".to_string(),
            id: trace.collaboration_board_id.clone(),
        },
    ];
    if let Some(parent_run_id) = &trace.parent_run_id {
        refs.push(WorkGraphRef {
            ref_type: "parent_runtime_run".to_string(),
            id: parent_run_id.clone(),
        });
    }
    if let Some(agent_run_id) = &trace.agent_run_id {
        refs.push(WorkGraphRef {
            ref_type: "agent_runtime_run".to_string(),
            id: agent_run_id.clone(),
        });
    }
    if let Some(envelope_id) = &trace.context_envelope_id {
        refs.push(WorkGraphRef {
            ref_type: "context_envelope".to_string(),
            id: envelope_id.clone(),
        });
    }
    refs
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_collaboration::{CollaborationScorecard, SubTask};

    fn scorecard() -> CollaborationScorecard {
        CollaborationScorecard {
            completion_rate: 1.0,
            synthesis_lift: 1.2,
            complementarity_score: 0.8,
            active_memory_score: 0.4,
            conflict_count: 0,
            memory_pulse_count: 0,
            surfaced_conflicts: Vec::new(),
        }
    }

    #[test]
    fn agent_workgraph_builds_from_subtasks() {
        let task = CollaborationTask {
            description: "plan then implement".to_string(),
            required_skills: vec!["rust".to_string()],
            subtasks: vec![
                SubTask {
                    id: "plan".to_string(),
                    description: "make plan".to_string(),
                    required_skills: vec!["design".to_string()],
                    depends_on: Vec::new(),
                },
                SubTask {
                    id: "implement".to_string(),
                    description: "write code".to_string(),
                    required_skills: vec!["rust".to_string()],
                    depends_on: vec!["plan".to_string()],
                },
            ],
            review_criteria: None,
        };

        let graph = AgentWorkGraph::from_collaboration_task("session-1", &task);
        assert_eq!(graph.session_id, "session-1");
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].kind, WorkGraphEdgeKind::DependsOn);
        assert_eq!(graph.status, WorkGraphStatus::Planned);
    }

    #[test]
    fn agent_workgraph_binds_review_packet_and_runtime_refs() {
        let task = CollaborationTask {
            description: "review implementation".to_string(),
            required_skills: vec!["review".to_string()],
            subtasks: Vec::new(),
            review_criteria: None,
        };
        let trace = AgentTaskTrace {
            task_id: "review-node".to_string(),
            parent_run_id: Some("turn-run".to_string()),
            agent_run_id: Some("agent-run".to_string()),
            role: "reviewer".to_string(),
            objective: "review code".to_string(),
            status: "completed".to_string(),
            context_envelope_id: Some("env-1".to_string()),
            result_summary: "ok".to_string(),
            evidence_refs: Vec::new(),
            collaboration_board_id: "board-1".to_string(),
            confidence: 0.9,
            conflicts: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 2,
        };
        let packet = CollaborationReviewPacket {
            board_id: "board-1".to_string(),
            parent_run_id: Some("turn-run".to_string()),
            scorecard: scorecard(),
            agent_tasks: vec![trace],
            maintenance_candidates: Vec::new(),
        };

        let graph =
            AgentWorkGraph::from_collaboration_task("session-1", &task).with_review_packet(&packet);
        assert_eq!(graph.status, WorkGraphStatus::Completed);
        assert_eq!(graph.board_id.as_deref(), Some("board-1"));
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.kind == WorkGraphNodeKind::Synthesis));
        let review_node = graph
            .nodes
            .iter()
            .find(|node| node.node_id == "review-node")
            .expect("review node");
        assert!(review_node
            .refs
            .iter()
            .any(|reference| reference.ref_type == "agent_runtime_run"
                && reference.id == "agent-run"));
    }

    #[test]
    fn agent_workgraph_marks_failed_agent_as_degraded_graph() {
        let task = CollaborationTask {
            description: "parallel work".to_string(),
            required_skills: Vec::new(),
            subtasks: Vec::new(),
            review_criteria: None,
        };
        let trace = AgentTaskTrace {
            task_id: "worker".to_string(),
            parent_run_id: None,
            agent_run_id: Some("agent-run".to_string()),
            role: "worker".to_string(),
            objective: "do work".to_string(),
            status: "failed".to_string(),
            context_envelope_id: None,
            result_summary: "failed".to_string(),
            evidence_refs: Vec::new(),
            collaboration_board_id: "board-2".to_string(),
            confidence: 0.1,
            conflicts: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 2,
        };
        let packet = CollaborationReviewPacket {
            board_id: "board-2".to_string(),
            parent_run_id: None,
            scorecard: scorecard(),
            agent_tasks: vec![trace],
            maintenance_candidates: Vec::new(),
        };

        let graph =
            AgentWorkGraph::from_collaboration_task("session-1", &task).with_review_packet(&packet);
        assert_eq!(graph.status, WorkGraphStatus::Degraded);
    }
}
