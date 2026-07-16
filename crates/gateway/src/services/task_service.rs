use std::sync::Arc;

use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdge, ExecutionGraph, ExecutionGraphProjection,
    ExecutionNodeSpec,
};

use super::ServiceEnvelope;
use crate::task_kernel::{TaskKernel, TaskRecord, TaskStatus};

#[derive(Clone)]
pub(crate) struct TaskService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    kernel: Option<Arc<TaskKernel>>,
    runtime_services: Option<Arc<runtime::RuntimeServices>>,
}

impl TaskService {
    pub(crate) fn new() -> Self {
        Self {
            label: "task",
            owner: "0.9.296 Task service boundary",
            kernel: None,
            runtime_services: None,
        }
    }

    pub(crate) fn with_kernel(kernel: Arc<TaskKernel>) -> Self {
        Self {
            kernel: Some(kernel),
            ..Self::new()
        }
    }

    pub(crate) fn with_kernel_and_runtime(
        kernel: Arc<TaskKernel>,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> Self {
        Self {
            kernel: Some(kernel),
            runtime_services: Some(runtime_services),
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
            boundary_status: "0620_final_boundary",
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

    fn runtime_services(&self) -> Result<&Arc<runtime::RuntimeServices>, String> {
        self.runtime_services
            .as_ref()
            .ok_or_else(|| "runtime services not configured".to_string())
    }

    pub(crate) async fn execution_graphs(&self) -> Result<Vec<ExecutionGraphProjection>, String> {
        let mut graphs = Vec::new();
        for task in self.list_records()? {
            if task.execution_graph.is_some() {
                if let Some(projection) = self.execution_graph(&task.id).await? {
                    graphs.push(projection);
                }
            }
        }
        Ok(graphs)
    }

    pub(crate) async fn execution_graph(
        &self,
        task_id: &str,
    ) -> Result<Option<ExecutionGraphProjection>, String> {
        let Some(cached) = self.kernel()?.execution_graph(task_id) else {
            return Ok(None);
        };
        let projection = self
            .runtime_services()?
            .graph_state_store()
            .projection_async(cached.graph_id)
            .await
            .map_err(|error| error.to_string())?;
        self.record_execution_graph_projection(task_id, projection.clone())?;
        Ok(Some(projection))
    }

    pub(crate) async fn register_execution_graph(
        &self,
        task_id: &str,
        objective: Option<String>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
    ) -> Result<ExecutionGraphProjection, String> {
        if nodes.is_empty() {
            return Err("execution graph requires at least one node".to_string());
        }
        if self.kernel()?.execution_graph(task_id).is_some() {
            return Err(format!(
                "task {task_id} already has an execution graph; use RuntimeHost commands"
            ));
        }
        let task = self
            .list_records()?
            .into_iter()
            .find(|task| task.id == task_id)
            .ok_or_else(|| format!("task {task_id} not found"))?;
        let mut graph = ExecutionGraph::new(objective.unwrap_or(task.objective));
        graph.id = format!("execution-graph-task-{task_id}");
        graph.nodes = nodes;
        graph.edges = edges;
        validate_execution_graph(&graph).map_err(|error| error.to_string())?;
        let receipt = self
            .runtime_services()?
            .commit_service()
            .register_graph_async(graph)
            .await
            .map_err(|error| error.to_string())?;
        let projection = self
            .runtime_services()?
            .graph_state_store()
            .projection_async(receipt.graph.id)
            .await
            .map_err(|error| error.to_string())?;
        self.record_execution_graph_projection(task_id, projection.clone())?;
        Ok(projection)
    }

    pub(crate) fn start_goal(
        &self,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.start_goal(objective, yolo_mode)
    }

    pub(crate) fn start_goal_idempotent(
        &self,
        task_id: &str,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        self.kernel()?
            .start_goal_idempotent(task_id, objective, yolo_mode)
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

    pub(crate) fn record_execution_graph_projection(
        &self,
        task_id: &str,
        projection: ExecutionGraphProjection,
    ) -> Result<TaskRecord, String> {
        self.kernel()?
            .record_execution_graph_projection(task_id, projection)
    }

    pub(crate) fn record_lifecycle_event(
        &self,
        task: &TaskRecord,
        kind: &'static str,
    ) -> Result<(), String> {
        self.record_lifecycle_event_with_correlation(task, kind, None)
            .map(|_| ())
    }

    pub(crate) fn latest_terminal_runtime_receipt(
        &self,
        task_id: &str,
    ) -> Result<Option<runtime::DurableRuntimeEvent>, String> {
        self.runtime_services()?
            .latest_task_terminal_receipt(task_id)
    }

    pub(crate) fn record_assignment_terminal_observation(
        &self,
        task_id: &str,
        terminal_status: &str,
        source_receipt_ref: &str,
        correlation_id: &str,
    ) -> Result<runtime::DurableRuntimeEvent, String> {
        self.runtime_services()?.record_task_terminal_observation(
            task_id,
            terminal_status,
            source_receipt_ref,
            correlation_id,
        )
    }

    fn record_lifecycle_event_with_correlation(
        &self,
        task: &TaskRecord,
        kind: &'static str,
        correlation_id: Option<&str>,
    ) -> Result<runtime::DurableRuntimeEvent, String> {
        let kind = match kind {
            "task.started" => runtime::TaskLifecycleKind::Started,
            "task.phase.started" => runtime::TaskLifecycleKind::PhaseStarted,
            "task.phase.artifact.recorded" => runtime::TaskLifecycleKind::PhaseArtifactRecorded,
            "task.phase.reviewed" => runtime::TaskLifecycleKind::PhaseReviewed,
            "task.cancelled" => runtime::TaskLifecycleKind::Cancelled,
            "task.completed" => runtime::TaskLifecycleKind::Completed,
            "task.failure.recorded" => runtime::TaskLifecycleKind::FailureRecorded,
            "task.blocked" => runtime::TaskLifecycleKind::Blocked,
            _ => return Err(format!("unsupported task lifecycle event `{kind}`")),
        };
        let latest_audit = task.audit.last();
        let payload = serde_json::json!({
            "task": task,
            "task_id": task.id,
            "objective": task.objective,
            "status": task.status.as_str(),
            "current_phase": task.current_phase,
            "failure_count": task.failure_count,
            "latest_audit": latest_audit,
            "correlation_id": correlation_id,
        });
        let event = runtime::TaskLifecycleEvent {
            task_id: task.id.clone(),
            kind,
            payload,
        };
        let result = if kind == runtime::TaskLifecycleKind::Completed {
            if let Some(correlation_id) = correlation_id.filter(|value| !value.trim().is_empty()) {
                self.runtime_services()?
                    .record_task_lifecycle_once(event, &format!("task-completed:{correlation_id}"))
            } else {
                self.runtime_services()?.record_task_lifecycle(event)
            }
        } else {
            self.runtime_services()?.record_task_lifecycle(event)
        };
        result.map_err(|error| format!("failed to append task runtime event: {error}"))
    }
}
