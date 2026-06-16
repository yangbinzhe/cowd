//! Conservative execution scheduler for model-requested tool calls.

use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};

use crate::tool_dispatch::ToolRequest;
use crate::tool_execution_plan::{ToolExecutionMode, ToolExecutionPlan};
use crate::tool_orchestrator::ToolSafetyCategory;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBatchMode {
    ParallelRead,
    LimitedWrite,
    LimitedNetwork,
    SerialDestructive,
    Wave,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBatch {
    pub mode: ExecutionBatchMode,
    pub indices: Vec<usize>,
    pub max_concurrency: usize,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSchedule {
    pub batches: Vec<ExecutionBatch>,
}

impl ToolSchedule {
    #[must_use]
    pub fn parallel_read_indices(&self) -> Vec<usize> {
        self.batches
            .iter()
            .filter(|batch| batch.mode == ExecutionBatchMode::ParallelRead)
            .flat_map(|batch| batch.indices.iter().copied())
            .collect()
    }

    #[must_use]
    pub fn remaining_indices(&self) -> Vec<usize> {
        self.batches
            .iter()
            .filter(|batch| batch.mode != ExecutionBatchMode::ParallelRead)
            .flat_map(|batch| batch.indices.iter().copied())
            .collect()
    }

    #[must_use]
    pub fn to_runtime_event(
        &self,
        session_id: impl Into<String>,
        sequence: usize,
        created_at_ms: u64,
        requests: &[ToolRequest],
    ) -> RuntimeEvent {
        let payload = serde_json::json!({
            "batch_count": self.batches.len(),
            "batches": self.batches,
            "tool_count": requests.len(),
        });
        let mut event = RuntimeEvent::new(
            session_id,
            sequence,
            RuntimeEventScope::Tool,
            "tool.schedule.created",
            payload,
            created_at_ms,
        );
        event.status = Some("planned".to_string());
        event.refs = requests
            .iter()
            .map(|request| RuntimeRef {
                ref_type: "tool_call".to_string(),
                id: request.tool_use_id.clone(),
                label: Some(request.tool_name.clone()),
            })
            .collect();
        event
    }
}

#[must_use]
pub fn schedule_tool_requests(requests: &[ToolRequest]) -> ToolSchedule {
    let plan = ToolExecutionPlan::from_requests(requests);
    let mut parallel_read = Vec::new();
    let mut limited_write = Vec::new();
    let mut limited_network = Vec::new();
    let mut serial_destructive = Vec::new();
    let mut wave = Vec::new();

    for (index, task) in plan.tasks.iter().enumerate() {
        if task.execution_mode == ToolExecutionMode::Wave || !task.depends_on.is_empty() {
            wave.push(index);
            continue;
        }
        match task.safety_category {
            ToolSafetyCategory::ReadOnly => parallel_read.push(index),
            ToolSafetyCategory::Network => limited_network.push(index),
            ToolSafetyCategory::WriteLocal => limited_write.push(index),
            ToolSafetyCategory::Destructive => serial_destructive.push(index),
        }
    }

    let mut batches = Vec::new();
    push_batch(
        &mut batches,
        ExecutionBatchMode::Wave,
        wave,
        8,
        "dependency constrained tools run through wave orchestration",
    );
    push_batch(
        &mut batches,
        ExecutionBatchMode::ParallelRead,
        parallel_read,
        usize::MAX,
        "read-only idempotent tools can run concurrently",
    );
    push_batch(
        &mut batches,
        ExecutionBatchMode::LimitedNetwork,
        limited_network,
        ToolSafetyCategory::Network.max_concurrency(),
        "network tools are rate limited",
    );
    push_batch(
        &mut batches,
        ExecutionBatchMode::LimitedWrite,
        limited_write,
        ToolSafetyCategory::WriteLocal.max_concurrency(),
        "local mutation tools require resource-aware limits",
    );
    push_batch(
        &mut batches,
        ExecutionBatchMode::SerialDestructive,
        serial_destructive,
        ToolSafetyCategory::Destructive.max_concurrency(),
        "runtime side-effect tools are serialized",
    );

    ToolSchedule { batches }
}

fn push_batch(
    batches: &mut Vec<ExecutionBatch>,
    mode: ExecutionBatchMode,
    indices: Vec<usize>,
    max_concurrency: usize,
    reason: &str,
) {
    if !indices.is_empty() {
        batches.push(ExecutionBatch {
            mode,
            indices,
            max_concurrency,
            reason: reason.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: &str, name: &str, input: &str) -> ToolRequest {
        ToolRequest {
            tool_use_id: id.to_string(),
            tool_name: name.to_string(),
            input: input.to_string(),
            depends_on: Vec::new(),
        }
    }

    #[test]
    fn scheduler_groups_read_network_write_and_destructive_tools() {
        let schedule = schedule_tool_requests(&[
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
            request("net-1", "WebSearch", r#"{"query":"rust"}"#),
            request(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
            ),
            request("shell-1", "bash", r#"{"command":"rm -rf target"}"#),
        ]);

        assert_eq!(schedule.batches.len(), 4);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::ParallelRead);
        assert_eq!(schedule.batches[0].indices, vec![0]);
        assert_eq!(schedule.batches[1].mode, ExecutionBatchMode::LimitedNetwork);
        assert_eq!(schedule.batches[1].indices, vec![1]);
        assert_eq!(schedule.batches[2].mode, ExecutionBatchMode::LimitedWrite);
        assert_eq!(schedule.batches[2].indices, vec![2]);
        assert_eq!(
            schedule.batches[3].mode,
            ExecutionBatchMode::SerialDestructive
        );
        assert_eq!(schedule.batches[3].indices, vec![3]);
    }

    #[test]
    fn scheduler_keeps_dependency_tasks_in_wave_batch() {
        let mut req = request("edit-1", "edit_file", r#"{"path":"src/lib.rs"}"#);
        req.depends_on.push("read-1".to_string());
        let schedule = schedule_tool_requests(&[req]);

        assert_eq!(schedule.batches.len(), 1);
        assert_eq!(schedule.batches[0].mode, ExecutionBatchMode::Wave);
        assert_eq!(schedule.remaining_indices(), vec![0]);
    }

    #[test]
    fn schedule_runtime_event_refs_all_tools() {
        let requests = vec![
            request("read-1", "read_file", r#"{"path":"README.md"}"#),
            request("net-1", "WebSearch", r#"{"query":"rust"}"#),
        ];
        let schedule = schedule_tool_requests(&requests);
        let event = schedule.to_runtime_event("session-1", 9, 123, &requests);

        assert_eq!(event.scope, RuntimeEventScope::Tool);
        assert_eq!(event.kind, "tool.schedule.created");
        assert_eq!(event.refs.len(), 2);
        assert_eq!(event.payload["batch_count"], 2);
    }
}
