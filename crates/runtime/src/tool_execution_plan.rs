//! Explainable tool execution planning for batched tool requests.

use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::{ToolSafetyCategory, ToolSafetyRegistry};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionMode {
    ParallelRead,
    LimitedParallel,
    SerialDestructive,
    Wave,
}

impl ToolExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParallelRead => "parallel_read",
            Self::LimitedParallel => "limited_parallel",
            Self::SerialDestructive => "serial_destructive",
            Self::Wave => "wave",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionPlanTask {
    pub tool_call_id: String,
    pub tool_name: String,
    pub safety_category: ToolSafetyCategory,
    pub execution_mode: ToolExecutionMode,
    pub depends_on: Vec<String>,
    pub max_concurrency: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionPlan {
    pub plan_id: String,
    pub task_count: usize,
    pub parallel_read_count: usize,
    pub limited_count: usize,
    pub destructive_count: usize,
    pub wave_count: usize,
    pub tasks: Vec<ToolExecutionPlanTask>,
}

impl ToolExecutionPlan {
    #[must_use]
    pub fn from_requests(requests: &[ToolRequest]) -> Self {
        let registry = ToolSafetyRegistry::global();
        let mut parallel_read_count = 0;
        let mut limited_count = 0;
        let mut destructive_count = 0;
        let mut wave_count = 0;

        let tasks = requests
            .iter()
            .map(|request| {
                let safety_category = registry.classify(&request.tool_name);
                let execution_mode = if !request.depends_on.is_empty() {
                    wave_count += 1;
                    ToolExecutionMode::Wave
                } else {
                    match safety_category {
                        ToolSafetyCategory::ReadOnly => {
                            parallel_read_count += 1;
                            ToolExecutionMode::ParallelRead
                        }
                        ToolSafetyCategory::Destructive => {
                            destructive_count += 1;
                            ToolExecutionMode::SerialDestructive
                        }
                        ToolSafetyCategory::WriteLocal | ToolSafetyCategory::Network => {
                            limited_count += 1;
                            ToolExecutionMode::LimitedParallel
                        }
                    }
                };

                ToolExecutionPlanTask {
                    tool_call_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    safety_category,
                    execution_mode,
                    depends_on: request.depends_on.clone(),
                    max_concurrency: match execution_mode {
                        ToolExecutionMode::Wave => 8,
                        _ => safety_category.max_concurrency(),
                    },
                }
            })
            .collect::<Vec<_>>();

        Self {
            plan_id: format!("tool-plan-{}", Uuid::new_v4()),
            task_count: tasks.len(),
            parallel_read_count,
            limited_count,
            destructive_count,
            wave_count,
            tasks,
        }
    }

    #[must_use]
    pub fn to_runtime_event(
        &self,
        session_id: impl Into<String>,
        sequence: usize,
        created_at_ms: u64,
    ) -> RuntimeEvent {
        let payload = serde_json::json!({
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "parallel_read_count": self.parallel_read_count,
            "limited_count": self.limited_count,
            "destructive_count": self.destructive_count,
            "wave_count": self.wave_count,
            "tasks": self.tasks,
        });
        let mut event = RuntimeEvent::new(
            session_id,
            sequence,
            RuntimeEventScope::Tool,
            "tool.execution_plan.created",
            payload,
            created_at_ms,
        );
        event.status = Some("planned".to_string());
        event.span_id = Some(self.plan_id.clone());
        event.refs = self
            .tasks
            .iter()
            .map(|task| RuntimeRef {
                ref_type: "tool_call".to_string(),
                id: task.tool_call_id.clone(),
                label: Some(task.tool_name.clone()),
            })
            .collect();
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: &str, tool_name: &str, depends_on: Vec<String>) -> ToolRequest {
        ToolRequest {
            tool_use_id: id.to_string(),
            tool_name: tool_name.to_string(),
            input: "{}".to_string(),
            depends_on,
        }
    }

    #[test]
    fn plan_classifies_parallel_limited_and_destructive_tools() {
        let plan = ToolExecutionPlan::from_requests(&[
            request("read-1", "read", Vec::new()),
            request("write-1", "write", Vec::new()),
            request("rm-1", "rm", Vec::new()),
        ]);

        assert_eq!(plan.task_count, 3);
        assert_eq!(plan.parallel_read_count, 1);
        assert_eq!(plan.limited_count, 1);
        assert_eq!(plan.destructive_count, 1);
        assert_eq!(
            plan.tasks[0].execution_mode,
            ToolExecutionMode::ParallelRead
        );
        assert_eq!(
            plan.tasks[2].execution_mode,
            ToolExecutionMode::SerialDestructive
        );
    }

    #[test]
    fn dependency_tasks_are_planned_as_wave_tasks() {
        let plan = ToolExecutionPlan::from_requests(&[request(
            "write-2",
            "write",
            vec!["read-1".to_string()],
        )]);

        assert_eq!(plan.wave_count, 1);
        assert_eq!(plan.tasks[0].execution_mode, ToolExecutionMode::Wave);
        assert_eq!(plan.tasks[0].max_concurrency, 8);
    }

    #[test]
    fn plan_event_refs_all_tool_calls() {
        let plan = ToolExecutionPlan::from_requests(&[
            request("read-1", "read", Vec::new()),
            request("write-1", "write", Vec::new()),
        ]);
        let event = plan.to_runtime_event("session-1", 7, 123);

        assert_eq!(event.scope, RuntimeEventScope::Tool);
        assert_eq!(event.kind, "tool.execution_plan.created");
        assert_eq!(event.status.as_deref(), Some("planned"));
        assert_eq!(event.refs.len(), 2);
        assert_eq!(event.payload["task_count"], 2);
    }
}
