use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::rewoo_plan::RewooEvidencePlan;
use crate::execution_scheduler::{schedule_tool_requests, ToolSchedule};
use crate::tool_dispatch::ToolRequest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDagPlan {
    pub dag_id: String,
    pub tasks: Vec<ToolDagTask>,
    pub edges: Vec<ToolDagEdge>,
    pub safety_summary: ToolDagSafetySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDagTask {
    pub id: String,
    pub tool_name: String,
    pub input: Value,
    pub purpose: String,
    pub expected_output: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDagEdge {
    pub from: String,
    pub to: String,
    pub kind: ToolDagEdgeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolDagEdgeKind {
    DataDependency,
    Ordering,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDagSafetySummary {
    pub task_count: usize,
    pub schedule: ToolSchedule,
}

impl ToolDagPlan {
    #[must_use]
    pub fn new(tasks: Vec<ToolDagTask>, edges: Vec<ToolDagEdge>) -> Self {
        let requests = to_tool_requests(&tasks, &edges);
        let schedule = schedule_tool_requests(&requests);
        Self {
            dag_id: format!("tool-dag-{}", Uuid::new_v4()),
            safety_summary: ToolDagSafetySummary {
                task_count: tasks.len(),
                schedule,
            },
            tasks,
            edges,
        }
    }

    #[must_use]
    pub fn to_tool_requests(&self) -> Vec<ToolRequest> {
        to_tool_requests(&self.tasks, &self.edges)
    }
}

#[must_use]
pub fn tool_dag_from_rewoo(plan: &RewooEvidencePlan) -> ToolDagPlan {
    let tasks = plan
        .steps
        .iter()
        .map(|step| ToolDagTask {
            id: step.id.clone(),
            tool_name: step.tool_name.clone(),
            input: step.input.clone(),
            purpose: step.purpose.clone(),
            expected_output: step.output_ref.clone(),
        })
        .collect::<Vec<_>>();
    let edges = plan
        .steps
        .iter()
        .flat_map(|step| {
            step.depends_on.iter().map(|from| ToolDagEdge {
                from: from.clone(),
                to: step.id.clone(),
                kind: ToolDagEdgeKind::DataDependency,
            })
        })
        .collect::<Vec<_>>();
    ToolDagPlan::new(tasks, edges)
}

fn to_tool_requests(tasks: &[ToolDagTask], edges: &[ToolDagEdge]) -> Vec<ToolRequest> {
    tasks
        .iter()
        .map(|task| {
            let depends_on = edges
                .iter()
                .filter(|edge| edge.to == task.id)
                .map(|edge| edge.from.clone())
                .collect::<Vec<_>>();
            ToolRequest {
                tool_use_id: task.id.clone(),
                tool_name: task.tool_name.clone(),
                input: serde_json::to_string(&task.input).unwrap_or_else(|_| "{}".to_string()),
                depends_on,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_scheduler::ExecutionBatchMode;

    #[test]
    fn tool_dag_schedules_independent_read_tools_in_parallel() {
        let dag = ToolDagPlan::new(
            vec![
                ToolDagTask {
                    id: "a".to_string(),
                    tool_name: "read_file".to_string(),
                    input: serde_json::json!({"path": "README.md"}),
                    purpose: "read".to_string(),
                    expected_output: "a".to_string(),
                },
                ToolDagTask {
                    id: "b".to_string(),
                    tool_name: "grep_search".to_string(),
                    input: serde_json::json!({"pattern": "runtime"}),
                    purpose: "grep".to_string(),
                    expected_output: "b".to_string(),
                },
            ],
            vec![],
        );
        assert!(dag
            .safety_summary
            .schedule
            .batches
            .iter()
            .any(|batch| batch.mode == ExecutionBatchMode::ParallelRead));
    }
}
