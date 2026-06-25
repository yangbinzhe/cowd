//! Runtime-owned agent task mailbox.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::{record_runtime_event, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskStatus {
    Pending,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTask {
    pub task_id: String,
    pub team_id: String,
    pub session_id: String,
    pub role_id: String,
    pub agent_id: Option<String>,
    pub objective: String,
    pub expected_output: String,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub status: AgentTaskStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskReceipt {
    pub task_id: String,
    pub team_id: String,
    pub agent_id: Option<String>,
    pub status: AgentTaskStatus,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct AgentTaskMailboxService {
    tasks: Mutex<BTreeMap<String, AgentTask>>,
}

impl AgentTaskMailboxService {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assign(&self, mut task: AgentTask) -> AgentTaskReceipt {
        if task.task_id.trim().is_empty() {
            task.task_id = format!("agent-task-{}", uuid::Uuid::new_v4());
        }
        let now = now_ms();
        if task.created_at_ms == 0 {
            task.created_at_ms = now;
        }
        task.updated_at_ms = now;
        task.status = AgentTaskStatus::Pending;
        let receipt = AgentTaskReceipt {
            task_id: task.task_id.clone(),
            team_id: task.team_id.clone(),
            agent_id: task.agent_id.clone(),
            status: task.status,
            message: "agent task assigned".to_string(),
        };
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(task.task_id.clone(), task.clone());
        record_task_event(&task, "agent_task.assigned");
        receipt
    }

    pub fn set_status(
        &self,
        task_id: &str,
        status: AgentTaskStatus,
        message: impl Into<String>,
    ) -> Result<AgentTaskReceipt, String> {
        let message = message.into();
        let mut tasks = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = tasks
            .get_mut(task_id)
            .ok_or_else(|| format!("agent task not found: {task_id}"))?;
        task.status = status;
        task.updated_at_ms = now_ms();
        let task = task.clone();
        drop(tasks);
        record_task_event(&task, format!("agent_task.{}", status_label(status)));
        Ok(AgentTaskReceipt {
            task_id: task.task_id,
            team_id: task.team_id,
            agent_id: task.agent_id,
            status,
            message,
        })
    }

    #[must_use]
    pub fn list_for_team(&self, team_id: &str) -> Vec<AgentTask> {
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|task| task.team_id == team_id)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn list_for_agent(&self, agent_id: &str) -> Vec<AgentTask> {
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|task| task.agent_id.as_deref() == Some(agent_id))
            .cloned()
            .collect()
    }
}

pub fn global_agent_task_mailbox() -> &'static AgentTaskMailboxService {
    static SERVICE: OnceLock<AgentTaskMailboxService> = OnceLock::new();
    SERVICE.get_or_init(AgentTaskMailboxService::new)
}

fn record_task_event(task: &AgentTask, kind: impl Into<String>) {
    let _ = record_runtime_event(RuntimeEventInput {
        stream_id: format!("agent-task:{}", task.task_id),
        scope: RuntimeEventScope::Agent,
        kind: kind.into(),
        status: Some(status_label(task.status).to_string()),
        actor: Some("agent_task_mailbox".to_string()),
        refs: vec![
            RuntimeEventRef {
                kind: "team".to_string(),
                id: task.team_id.clone(),
            },
            RuntimeEventRef {
                kind: "session".to_string(),
                id: task.session_id.clone(),
            },
        ],
        payload: serde_json::json!(task),
    });
}

fn status_label(status: AgentTaskStatus) -> &'static str {
    match status {
        AgentTaskStatus::Pending => "pending",
        AgentTaskStatus::Claimed => "claimed",
        AgentTaskStatus::Running => "running",
        AgentTaskStatus::Completed => "completed",
        AgentTaskStatus::Failed => "failed",
        AgentTaskStatus::Cancelled => "cancelled",
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
