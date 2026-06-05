use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskStatus {
    Pending,
    Running,
    Reviewing,
    Completed,
    Blocked,
    Cancelled,
    Failed,
}

impl TaskStatus {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Reviewing => "reviewing",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskAuditEvent {
    pub event_type: String,
    pub message: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskPhaseStatus {
    Running,
    Reviewing,
    Completed,
    Failed,
}

impl TaskPhaseStatus {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Reviewing => "reviewing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskPhaseArtifact {
    pub kind: String,
    pub label: String,
    pub value: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskPhaseRecord {
    pub id: String,
    pub name: String,
    pub objective: String,
    pub plan: Vec<String>,
    pub acceptance: Vec<String>,
    pub test_commands: Vec<String>,
    pub artifacts: Vec<TaskPhaseArtifact>,
    pub review_result: Option<String>,
    pub status: TaskPhaseStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskRecord {
    pub id: String,
    pub objective: String,
    pub status: TaskStatus,
    pub current_phase: Option<String>,
    #[serde(default)]
    pub phases: Vec<TaskPhaseRecord>,
    pub yolo_mode: bool,
    pub failure_count: u32,
    pub blocker_reason: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub audit: Vec<TaskAuditEvent>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TaskStore {
    tasks: Vec<TaskRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskKernel {
    path: PathBuf,
    store: Arc<Mutex<TaskStore>>,
}

impl TaskKernel {
    pub(crate) fn open(path: PathBuf) -> Result<Self, String> {
        let store = if path.is_file() {
            let raw = fs::read_to_string(&path).map_err(|e| e.to_string())?;
            serde_json::from_str(&raw).map_err(|e| e.to_string())?
        } else {
            TaskStore::default()
        };
        Ok(Self {
            path,
            store: Arc::new(Mutex::new(store)),
        })
    }

    #[must_use]
    pub(crate) fn list(&self) -> Vec<TaskRecord> {
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .clone()
    }

    #[must_use]
    pub(crate) fn current(&self) -> Option<TaskRecord> {
        self.list().into_iter().rev().find(|task| {
            matches!(
                task.status,
                TaskStatus::Pending | TaskStatus::Running | TaskStatus::Reviewing
            )
        })
    }

    pub(crate) fn start_goal(
        &self,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        let now = now_ms();
        let objective = objective.into();
        if objective.trim().is_empty() {
            return Err("task objective is required".to_string());
        }
        let initial_phase = TaskPhaseRecord {
            id: format!("phase-{}", uuid::Uuid::new_v4()),
            name: "implementation".to_string(),
            objective: objective.clone(),
            plan: Vec::new(),
            acceptance: Vec::new(),
            test_commands: Vec::new(),
            artifacts: Vec::new(),
            review_result: None,
            status: TaskPhaseStatus::Running,
            created_at_ms: now,
            updated_at_ms: now,
        };
        let mut task = TaskRecord {
            id: format!("task-{}", uuid::Uuid::new_v4()),
            objective,
            status: TaskStatus::Running,
            current_phase: Some("implementation".to_string()),
            phases: vec![initial_phase],
            yolo_mode,
            failure_count: 0,
            blocker_reason: None,
            created_at_ms: now,
            updated_at_ms: now,
            audit: Vec::new(),
        };
        task.audit.push(TaskAuditEvent {
            event_type: "started".to_string(),
            message: "task started".to_string(),
            created_at_ms: now,
        });
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .push(task.clone());
        self.persist()?;
        Ok(task)
    }

    pub(crate) fn transition(
        &self,
        task_id: &str,
        status: TaskStatus,
        phase: Option<String>,
        message: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        let now = now_ms();
        let mut store = self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = store
            .tasks
            .iter_mut()
            .find(|task| task.id == task_id)
            .ok_or_else(|| format!("task {task_id} not found"))?;
        task.status = status;
        task.current_phase = phase;
        task.updated_at_ms = now;
        task.audit.push(TaskAuditEvent {
            event_type: format!("{:?}", status).to_lowercase(),
            message: message.into(),
            created_at_ms: now,
        });
        let updated = task.clone();
        drop(store);
        self.persist()?;
        Ok(updated)
    }

    pub(crate) fn start_phase(
        &self,
        task_id: &str,
        name: impl Into<String>,
        objective: impl Into<String>,
        plan: Vec<String>,
        acceptance: Vec<String>,
        test_commands: Vec<String>,
    ) -> Result<TaskRecord, String> {
        let now = now_ms();
        let name = name.into();
        let objective = objective.into();
        if name.trim().is_empty() {
            return Err("phase name is required".to_string());
        }
        if objective.trim().is_empty() {
            return Err("phase objective is required".to_string());
        }

        let mut store = self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = store
            .tasks
            .iter_mut()
            .find(|task| task.id == task_id)
            .ok_or_else(|| format!("task {task_id} not found"))?;
        let phase = TaskPhaseRecord {
            id: format!("phase-{}", uuid::Uuid::new_v4()),
            name: name.clone(),
            objective,
            plan,
            acceptance,
            test_commands,
            artifacts: Vec::new(),
            review_result: None,
            status: TaskPhaseStatus::Running,
            created_at_ms: now,
            updated_at_ms: now,
        };
        task.status = TaskStatus::Running;
        task.current_phase = Some(name.clone());
        task.updated_at_ms = now;
        task.phases.push(phase);
        task.audit.push(TaskAuditEvent {
            event_type: "phase_started".to_string(),
            message: format!("phase started: {name}"),
            created_at_ms: now,
        });
        let updated = task.clone();
        drop(store);
        self.persist()?;
        Ok(updated)
    }

    pub(crate) fn record_phase_artifact(
        &self,
        task_id: &str,
        phase_id: &str,
        kind: impl Into<String>,
        label: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        let now = now_ms();
        let kind = kind.into();
        let label = label.into();
        let value = value.into();
        if label.trim().is_empty() {
            return Err("artifact label is required".to_string());
        }
        if value.trim().is_empty() {
            return Err("artifact value is required".to_string());
        }

        let mut store = self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = store
            .tasks
            .iter_mut()
            .find(|task| task.id == task_id)
            .ok_or_else(|| format!("task {task_id} not found"))?;
        let phase = task
            .phases
            .iter_mut()
            .find(|phase| phase.id == phase_id)
            .ok_or_else(|| format!("phase {phase_id} not found"))?;
        phase.artifacts.push(TaskPhaseArtifact {
            kind,
            label: label.clone(),
            value,
            created_at_ms: now,
        });
        phase.updated_at_ms = now;
        task.updated_at_ms = now;
        task.audit.push(TaskAuditEvent {
            event_type: "phase_artifact".to_string(),
            message: format!("phase artifact recorded: {label}"),
            created_at_ms: now,
        });
        let updated = task.clone();
        drop(store);
        self.persist()?;
        Ok(updated)
    }

    pub(crate) fn review_phase(
        &self,
        task_id: &str,
        phase_id: &str,
        result: impl Into<String>,
        completed: bool,
    ) -> Result<TaskRecord, String> {
        let now = now_ms();
        let result = result.into();
        if result.trim().is_empty() {
            return Err("review result is required".to_string());
        }

        let mut store = self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = store
            .tasks
            .iter_mut()
            .find(|task| task.id == task_id)
            .ok_or_else(|| format!("task {task_id} not found"))?;
        let phase = task
            .phases
            .iter_mut()
            .find(|phase| phase.id == phase_id)
            .ok_or_else(|| format!("phase {phase_id} not found"))?;
        phase.review_result = Some(result.clone());
        phase.status = if completed {
            TaskPhaseStatus::Completed
        } else {
            TaskPhaseStatus::Reviewing
        };
        phase.updated_at_ms = now;
        task.status = if completed {
            TaskStatus::Reviewing
        } else {
            TaskStatus::Running
        };
        task.current_phase = Some(phase.name.clone());
        task.updated_at_ms = now;
        task.audit.push(TaskAuditEvent {
            event_type: "phase_reviewed".to_string(),
            message: result,
            created_at_ms: now,
        });
        let updated = task.clone();
        drop(store);
        self.persist()?;
        Ok(updated)
    }

    pub(crate) fn record_failure(
        &self,
        task_id: &str,
        reason: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        let reason = reason.into();
        let now = now_ms();
        let mut store = self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = store
            .tasks
            .iter_mut()
            .find(|task| task.id == task_id)
            .ok_or_else(|| format!("task {task_id} not found"))?;
        task.failure_count += 1;
        task.updated_at_ms = now;
        if task.failure_count >= 3 {
            task.status = TaskStatus::Blocked;
            task.blocker_reason = Some(reason.clone());
            task.current_phase = Some("blocked".to_string());
        }
        task.audit.push(TaskAuditEvent {
            event_type: "failure".to_string(),
            message: reason,
            created_at_ms: now,
        });
        let updated = task.clone();
        drop(store);
        self.persist()?;
        Ok(updated)
    }

    fn persist(&self) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let raw = {
            let store = self
                .store
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            serde_json::to_string_pretty(&*store).map_err(|e| e.to_string())?
        };
        fs::write(&self.path, raw).map_err(|e| e.to_string())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{TaskKernel, TaskStatus};

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cowd-task-{label}-{}.json", uuid::Uuid::new_v4()))
    }

    #[test]
    fn task_kernel_persists_and_restores_started_goal() {
        let path = temp_path("persist");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Ship v0.8.10", true).unwrap();

        let restored = TaskKernel::open(path.clone()).unwrap();
        let current = restored.current().expect("current task should restore");
        assert_eq!(current.id, task.id);
        assert_eq!(current.status, TaskStatus::Running);
        assert!(current.yolo_mode);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn task_kernel_blocks_after_three_failures() {
        let path = temp_path("blocked");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Recover failing task", true).unwrap();

        kernel.record_failure(&task.id, "first").unwrap();
        kernel.record_failure(&task.id, "second").unwrap();
        let blocked = kernel
            .record_failure(&task.id, "external input required")
            .unwrap();

        assert_eq!(blocked.status, TaskStatus::Blocked);
        assert_eq!(
            blocked.blocker_reason.as_deref(),
            Some("external input required")
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn task_kernel_can_cancel_and_complete_tasks() {
        let path = temp_path("transition");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Review", false).unwrap();

        let reviewing = kernel
            .transition(
                &task.id,
                TaskStatus::Reviewing,
                Some("review".to_string()),
                "tests passed",
            )
            .unwrap();
        assert_eq!(reviewing.status, TaskStatus::Reviewing);

        let completed = kernel
            .transition(&task.id, TaskStatus::Completed, None, "accepted")
            .unwrap();
        assert_eq!(completed.status, TaskStatus::Completed);
        assert!(kernel.current().is_none());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn task_kernel_records_phase_artifacts_and_review() {
        let path = temp_path("phase");
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("Ship enterprise workflow", true).unwrap();

        let with_phase = kernel
            .start_phase(
                &task.id,
                "webui-e2e",
                "Cover task workbench browser scenario",
                vec!["Add Playwright fixture".to_string()],
                vec!["E2E passes".to_string()],
                vec!["cd webui && npm run test:e2e".to_string()],
            )
            .unwrap();
        let phase = with_phase
            .phases
            .last()
            .expect("phase should exist")
            .clone();
        assert_eq!(phase.name, "webui-e2e");
        assert_eq!(phase.status.as_str(), "running");
        assert_eq!(with_phase.current_phase.as_deref(), Some("webui-e2e"));

        let with_artifact = kernel
            .record_phase_artifact(&task.id, &phase.id, "test", "playwright", "2 passed")
            .unwrap();
        let phase = with_artifact
            .phases
            .iter()
            .find(|candidate| candidate.id == phase.id)
            .unwrap()
            .clone();
        assert_eq!(phase.artifacts[0].label, "playwright");

        let reviewed = kernel
            .review_phase(&task.id, &phase.id, "accepted after gate", true)
            .unwrap();
        let reviewed_phase = reviewed
            .phases
            .iter()
            .find(|candidate| candidate.id == phase.id)
            .unwrap();
        assert_eq!(reviewed_phase.status.as_str(), "completed");
        assert_eq!(
            reviewed_phase.review_result.as_deref(),
            Some("accepted after gate")
        );
        assert!(
            reviewed
                .audit
                .iter()
                .any(|event| event.event_type == "phase_reviewed")
        );

        let restored = TaskKernel::open(path.clone()).unwrap();
        let restored_task = restored
            .list()
            .into_iter()
            .find(|t| t.id == task.id)
            .unwrap();
        assert!(restored_task.phases.iter().any(|p| p.id == phase.id));

        let _ = std::fs::remove_file(path);
    }
}
