use std::sync::Arc;

use runtime::AgentRunGraph;

use super::ServiceEnvelope;
use crate::task_kernel::{TaskKernel, TaskRecord, TaskStatus};

#[derive(Clone)]
pub(crate) struct TaskService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    kernel: Option<Arc<TaskKernel>>,
}

impl TaskService {
    pub(crate) fn new() -> Self {
        Self {
            label: "task",
            owner: "0.9.296 Task service boundary",
            kernel: None,
        }
    }

    pub(crate) fn with_kernel(kernel: Arc<TaskKernel>) -> Self {
        Self {
            kernel: Some(kernel),
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.kernel.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "0618_final_boundary",
        }
    }

    fn kernel(&self) -> Result<&Arc<TaskKernel>, String> {
        self.kernel
            .as_ref()
            .ok_or_else(|| "task service not configured".to_string())
    }

    pub(crate) fn list_records(&self) -> Result<Vec<TaskRecord>, String> {
        Ok(self.kernel()?.list())
    }

    pub(crate) fn current(&self) -> Result<Option<TaskRecord>, String> {
        Ok(self.kernel()?.current())
    }

    pub(crate) fn list_agent_graphs(&self) -> Result<Vec<runtime::AgentRunGraph>, String> {
        Ok(self.kernel()?.list_agent_graphs())
    }

    pub(crate) fn agent_graph(&self, task_id: &str) -> Result<Option<AgentRunGraph>, String> {
        Ok(self.kernel()?.agent_graph(task_id))
    }

    pub(crate) fn start_goal(
        &self,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.start_goal(objective, yolo_mode)
    }

    pub(crate) fn start_phase(
        &self,
        id: &str,
        name: String,
        objective: String,
        plan: Vec<String>,
        acceptance: Vec<String>,
        test_commands: Vec<String>,
    ) -> Result<TaskRecord, String> {
        self.kernel()?
            .start_phase(id, name, objective, plan, acceptance, test_commands)
    }

    pub(crate) fn record_phase_artifact(
        &self,
        id: &str,
        phase_id: &str,
        kind: String,
        label: String,
        value: String,
    ) -> Result<TaskRecord, String> {
        self.kernel()?
            .record_phase_artifact(id, phase_id, kind, label, value)
    }

    pub(crate) fn review_phase(
        &self,
        id: &str,
        phase_id: &str,
        result: String,
        completed: bool,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.review_phase(id, phase_id, result, completed)
    }

    pub(crate) fn transition(
        &self,
        id: &str,
        status: TaskStatus,
        current_phase: Option<String>,
        note: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.transition(id, status, current_phase, note)
    }

    pub(crate) fn record_failure(
        &self,
        id: &str,
        reason: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.record_failure(id, reason)
    }

    pub(crate) fn upsert_agent_graph(
        &self,
        task_id: &str,
        graph: AgentRunGraph,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.upsert_agent_graph(task_id, graph)
    }
}
