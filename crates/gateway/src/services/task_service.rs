use std::sync::Arc;

use memory::store::session::SessionRecord;
use runtime::AgentRunGraph;

use super::{ServiceEnvelope, SessionService};
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

    pub(crate) async fn append_runtime_event(
        &self,
        session_service: &SessionService,
        task: &TaskRecord,
        kind: &'static str,
    ) -> Result<(), String> {
        ensure_task_session_record(session_service, task)
            .await
            .map_err(|error| format!("failed to prepare task runtime session: {error}"))?;
        let latest_audit = task.audit.last();
        let payload = serde_json::json!({
            "task": task,
            "task_id": task.id,
            "objective": task.objective,
            "status": task.status.as_str(),
            "current_phase": task.current_phase,
            "failure_count": task.failure_count,
            "latest_audit": latest_audit,
        });
        session_service
            .append_runtime_event(&task.id, memory::RuntimeEventScope::Task, kind, payload)
            .await
            .map(|_| ())
            .map_err(|error| format!("failed to append task runtime event: {error}"))
    }
}

async fn ensure_task_session_record(
    session_service: &SessionService,
    task: &TaskRecord,
) -> Result<(), String> {
    let Some(store) = session_service.unified_store() else {
        return Ok(());
    };
    let now = chrono::Utc::now().to_rfc3339();
    let metadata_json = serde_json::json!({
        "kind": "task",
        "task_id": task.id,
        "objective": task.objective,
        "yolo_mode": task.yolo_mode,
        "current_phase": task.current_phase,
    })
    .to_string();
    let mut record = SessionRecord {
        session_id: task.id.clone(),
        platform: "task".to_string(),
        chat_id: task.id.clone(),
        user_id: None,
        model: None,
        created_at: now.clone(),
        last_activity: now,
        message_count: task.audit.len() as i64,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata_json),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: task.status.as_str().to_string(),
    };
    if let Some(existing) = store
        .get_session(&task.id)
        .await
        .map_err(|error| error.to_string())?
    {
        record.created_at = existing.created_at;
        store
            .update_session(&record)
            .await
            .map_err(|error| error.to_string())?;
    } else {
        store
            .create_session(&record)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
