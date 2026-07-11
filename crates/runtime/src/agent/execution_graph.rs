//! Pure collaboration projections into the canonical execution graph.
//!
//! This module deliberately owns no execution state. The graph runner remains
//! the only writer; collaboration code may only build a graph or project an
//! observed review packet into a replacement graph value.

use std::collections::BTreeMap;

use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionNodeStatus,
};

use crate::agent_collaboration::{CollaborationReviewPacket, CollaborationTask};

#[must_use]
pub fn execution_graph_from_collaboration_task(
    session_id: &str,
    task: &CollaborationTask,
) -> ExecutionGraph {
    let mut graph = ExecutionGraph::new(task.description.clone());
    graph.id = format!("execution-graph-{}", uuid::Uuid::new_v4());

    for subtask in &task.subtasks {
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "runtime.agent",
            format!("collaboration-task:{}", subtask.id),
        );
        node.id = subtask.id.clone();
        node.idempotency_key = format!("{}:{}", graph.id, subtask.id);
        node.resource_scopes = vec![format!("session:{session_id}")];
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        graph.nodes.push(node);
    }
    graph.edges = task
        .subtasks
        .iter()
        .flat_map(|subtask| {
            subtask.depends_on.iter().map(|dependency| ExecutionEdge {
                from: dependency.clone(),
                to: subtask.id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            })
        })
        .collect();
    graph
}

/// Project a completed collaboration review into a new graph value.
///
/// This is intentionally pure: persistence and revision arbitration belong to
/// the execution graph commit service, not the collaboration adapter.
#[must_use]
pub fn project_collaboration_review(
    graph: &ExecutionGraph,
    packet: &CollaborationReviewPacket,
) -> ExecutionGraph {
    let mut projected = graph.clone();
    let traces = packet
        .agent_tasks
        .iter()
        .map(|trace| (trace.task_id.as_str(), trace))
        .collect::<BTreeMap<_, _>>();

    for node in &projected.nodes {
        let Some(trace) = traces.get(node.id.as_str()) else {
            continue;
        };
        let status = if trace.status.eq_ignore_ascii_case("failed") {
            ExecutionNodeStatus::Failed
        } else {
            ExecutionNodeStatus::Completed
        };
        projected.node_statuses.insert(node.id.clone(), status);
    }

    for trace in &packet.agent_tasks {
        if projected.nodes.iter().any(|node| node.id == trace.task_id) {
            continue;
        }
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "runtime.agent",
            format!("agent-trace:{}", trace.task_id),
        );
        node.id = trace.task_id.clone();
        node.idempotency_key = format!("{}:{}", projected.id, trace.task_id);
        projected.node_statuses.insert(
            node.id.clone(),
            if trace.status.eq_ignore_ascii_case("failed") {
                ExecutionNodeStatus::Failed
            } else {
                ExecutionNodeStatus::Completed
            },
        );
        projected.nodes.push(node);
    }

    let synthesis_id = format!("synthesis-{}", packet.board_id);
    if !projected.nodes.iter().any(|node| node.id == synthesis_id) {
        let mut synthesis = ExecutionNodeSpec::new(
            ExecutionNodeKind::Synthesize,
            "runtime.synthesize",
            format!("collaboration-board:{}", packet.board_id),
        );
        synthesis.id = synthesis_id.clone();
        synthesis.idempotency_key = format!("{}:{synthesis_id}", projected.id);
        projected
            .node_statuses
            .insert(synthesis_id.clone(), ExecutionNodeStatus::Completed);
        for node in projected
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        {
            projected.edges.push(ExecutionEdge {
                from: node.id.clone(),
                to: synthesis_id.clone(),
                kind: ExecutionEdgeKind::Produces,
            });
        }
        projected.nodes.push(synthesis);
    }
    projected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_collaboration::{AgentTaskTrace, CollaborationScorecard, SubTask};

    fn task() -> CollaborationTask {
        CollaborationTask {
            description: "implement and review".to_string(),
            required_capabilities: Vec::new(),
            subtasks: vec![
                SubTask {
                    id: "implement".to_string(),
                    description: "implement".to_string(),
                    required_capabilities: Vec::new(),
                    depends_on: Vec::new(),
                },
                SubTask {
                    id: "review".to_string(),
                    description: "review".to_string(),
                    required_capabilities: Vec::new(),
                    depends_on: vec!["implement".to_string()],
                },
            ],
            review_criteria: None,
            collaboration_decision: None,
        }
    }

    #[test]
    fn builds_canonical_dependency_graph() {
        let graph = execution_graph_from_collaboration_task("session-1", &task());
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert!(graph.nodes.iter().all(|node| {
            node.resource_scopes == ["session:session-1"]
                && node.kind == ExecutionNodeKind::AgentTask
        }));
    }

    #[test]
    fn review_projection_does_not_mutate_source_graph() {
        let graph = execution_graph_from_collaboration_task("session-1", &task());
        let packet = CollaborationReviewPacket {
            board_id: "board-1".to_string(),
            parent_run_id: None,
            scorecard: CollaborationScorecard {
                completion_rate: 1.0,
                synthesis_lift: 1.1,
                complementarity_score: 0.8,
                active_memory_score: 0.5,
                conflict_count: 0,
                memory_pulse_count: 0,
                surfaced_conflicts: Vec::new(),
            },
            agent_tasks: vec![AgentTaskTrace {
                task_id: "implement".to_string(),
                role: "worker".to_string(),
                objective: "implement".to_string(),
                status: "completed".to_string(),
                result_summary: "implemented".to_string(),
                parent_run_id: None,
                agent_run_id: None,
                context_envelope_id: None,
                evidence_refs: Vec::new(),
                collaboration_board_id: "board-1".to_string(),
                confidence: 1.0,
                conflicts: Vec::new(),
                created_at_ms: 0,
                updated_at_ms: 0,
            }],
            maintenance_candidates: Vec::new(),
        };
        let projected = project_collaboration_review(&graph, &packet);

        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(projected.nodes.len(), 3);
        assert_eq!(
            projected.node_statuses["implement"],
            ExecutionNodeStatus::Completed
        );
        assert!(projected
            .nodes
            .iter()
            .any(|node| node.kind == ExecutionNodeKind::Synthesize));
    }
}
