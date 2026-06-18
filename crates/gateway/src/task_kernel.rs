use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use runtime::{AgentNodeStatus, AgentRunGraph, ReviewVerdict};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use storage::{SqliteConnectionFactory, StorageHandle};

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
    #[serde(default)]
    pub agent_graph: Option<AgentRunGraph>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TaskStore {
    tasks: Vec<TaskRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskKernel {
    path: PathBuf,
    storage_handle: Option<StorageHandle>,
    store: Arc<Mutex<TaskStore>>,
}

impl TaskKernel {
    pub(crate) fn open(path: PathBuf) -> Result<Self, String> {
        let path = normalize_task_db_path(path);
        ensure_schema_path(&path)?;
        let store = load_store_path(&path)?;
        Ok(Self {
            path,
            storage_handle: None,
            store: Arc::new(Mutex::new(store)),
        })
    }

    pub(crate) fn open_storage_handle(handle: &StorageHandle) -> Result<Self, String> {
        let path = normalize_task_db_path(handle.path.clone());
        ensure_schema_handle(handle)?;
        let store = load_store_handle(handle)?;
        Ok(Self {
            path,
            storage_handle: Some(handle.clone()),
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
            agent_graph: None,
        };
        task.agent_graph = Some(AgentRunGraph::from_objective(
            task.id.clone(),
            task.objective.clone(),
        ));
        sync_agent_phase_node(&mut task, "implementation")?;
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
        sync_agent_phase_node(task, &name)?;
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
        if let Some(graph) = &mut task.agent_graph {
            graph
                .add_evidence(
                    phase_id,
                    phase
                        .artifacts
                        .last()
                        .map(|artifact| artifact.kind.as_str())
                        .unwrap_or("note"),
                    phase
                        .artifacts
                        .last()
                        .map(|artifact| artifact.label.as_str())
                        .unwrap_or("artifact"),
                    phase
                        .artifacts
                        .last()
                        .map(|artifact| artifact.value.as_str())
                        .unwrap_or(""),
                )
                .map_err(|error| error.to_string())?;
        }
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
        if let Some(graph) = &mut task.agent_graph {
            if graph.nodes.iter().any(|node| node.id == phase_id) {
                let verdict = if completed {
                    ReviewVerdict::Accept
                } else {
                    ReviewVerdict::Challenge
                };
                graph
                    .add_review(phase_id, "task-reviewer", verdict, result.clone())
                    .map_err(|error| error.to_string())?;
            }
        }
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
        if let Some(graph) = &mut task.agent_graph {
            if let Some(current_phase) = task.current_phase.as_deref() {
                if let Some(node_id) = task
                    .phases
                    .iter()
                    .rev()
                    .find(|phase| phase.name == current_phase)
                    .map(|phase| phase.id.clone())
                {
                    if graph.nodes.iter().any(|node| node.id == node_id) {
                        graph
                            .record_failure(&node_id, reason.clone())
                            .map_err(|error| error.to_string())?;
                    }
                }
            }
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

    #[must_use]
    pub(crate) fn list_agent_graphs(&self) -> Vec<AgentRunGraph> {
        self.list()
            .into_iter()
            .filter_map(|task| task.agent_graph)
            .collect()
    }

    #[must_use]
    pub(crate) fn agent_graph(&self, task_id: &str) -> Option<AgentRunGraph> {
        self.list()
            .into_iter()
            .find(|task| task.id == task_id)
            .and_then(|task| task.agent_graph)
    }

    pub(crate) fn upsert_agent_graph(
        &self,
        task_id: &str,
        graph: AgentRunGraph,
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
        task.agent_graph = Some(graph);
        task.updated_at_ms = now;
        task.audit.push(TaskAuditEvent {
            event_type: "agent_graph_updated".to_string(),
            message: "agent graph updated".to_string(),
            created_at_ms: now,
        });
        let updated = task.clone();
        drop(store);
        self.persist()?;
        Ok(updated)
    }

    fn persist(&self) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        if let Some(handle) = &self.storage_handle {
            ensure_schema_handle(handle)?;
        } else {
            ensure_schema_path(&self.path)?;
        }
        let store = {
            let store = self
                .store
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            store.tasks.clone()
        };
        let mut conn = if let Some(handle) = &self.storage_handle {
            SqliteConnectionFactory::default()
                .open_handle(handle)
                .map_err(|e| e.to_string())?
        } else {
            open_task_connection_path(&self.path)?
        };
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM tasks", [])
            .map_err(|e| e.to_string())?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO tasks (id, status, created_at_ms, updated_at_ms, record_json)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .map_err(|e| e.to_string())?;
            for task in &store {
                let record_json = serde_json::to_string(task).map_err(|e| e.to_string())?;
                stmt.execute(params![
                    task.id.as_str(),
                    task.status.as_str(),
                    task.created_at_ms as i64,
                    task.updated_at_ms as i64,
                    record_json,
                ])
                .map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())
    }
}

fn normalize_task_db_path(path: PathBuf) -> PathBuf {
    if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
        return path.with_extension("db");
    }
    path
}

fn ensure_schema_path(path: &PathBuf) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let conn = open_task_connection_path(path)?;
    ensure_schema_connection(&conn)
}

fn ensure_schema_handle(handle: &StorageHandle) -> Result<(), String> {
    if let Some(parent) = handle.path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let conn = SqliteConnectionFactory::default()
        .open_handle(handle)
        .map_err(|e| e.to_string())?;
    ensure_schema_connection(&conn)
}

fn ensure_schema_connection(conn: &rusqlite::Connection) -> Result<(), String> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            record_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_status_updated
            ON tasks(status, updated_at_ms DESC);",
    )
    .map_err(|e| e.to_string())
}

fn load_store_path(path: &PathBuf) -> Result<TaskStore, String> {
    let conn = open_task_connection_path(path)?;
    load_store_connection(&conn)
}

fn load_store_handle(handle: &StorageHandle) -> Result<TaskStore, String> {
    let conn = SqliteConnectionFactory::default()
        .open_handle(handle)
        .map_err(|e| e.to_string())?;
    load_store_connection(&conn)
}

fn load_store_connection(conn: &rusqlite::Connection) -> Result<TaskStore, String> {
    let mut stmt = conn
        .prepare("SELECT record_json FROM tasks ORDER BY created_at_ms ASC, id ASC")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    let mut tasks = Vec::new();
    for row in rows {
        let raw = row.map_err(|e| e.to_string())?;
        tasks.push(serde_json::from_str::<TaskRecord>(&raw).map_err(|e| e.to_string())?);
    }
    Ok(TaskStore { tasks })
}

fn open_task_connection_path(path: &PathBuf) -> Result<rusqlite::Connection, String> {
    SqliteConnectionFactory::default()
        .open(path)
        .map_err(|e| e.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn sync_agent_phase_node(task: &mut TaskRecord, phase_name: &str) -> Result<(), String> {
    let Some(graph) = &mut task.agent_graph else {
        return Ok(());
    };
    let Some(phase) = task.phases.last() else {
        return Ok(());
    };
    graph
        .upsert_phase_node(
            phase.id.clone(),
            phase_name.to_string(),
            phase.objective.clone(),
        )
        .map_err(|error| error.to_string())?;
    if let Some(planner) = graph.nodes.iter_mut().find(|node| node.id == "planner") {
        planner.status = AgentNodeStatus::Completed;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TaskKernel, TaskStatus};

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cowd-task-{label}-{}.db", uuid::Uuid::new_v4()))
    }

    fn legacy_temp_path(label: &str) -> std::path::PathBuf {
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
    fn task_kernel_maps_legacy_json_path_to_sqlite_db_without_json_write() {
        let legacy_path = legacy_temp_path("legacy-map");
        let db_path = legacy_path.with_extension("db");
        let kernel = TaskKernel::open(legacy_path.clone()).unwrap();
        kernel.start_goal("Use sqlite task store", true).unwrap();

        assert!(db_path.is_file());
        assert!(!legacy_path.exists());
        let restored = TaskKernel::open(legacy_path.clone()).unwrap();
        assert_eq!(restored.list().len(), 1);

        let _ = std::fs::remove_file(db_path);
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
                vec!["cargo test -p gateway task_kernel -- --nocapture".to_string()],
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
        assert!(reviewed
            .audit
            .iter()
            .any(|event| event.event_type == "phase_reviewed"));
        let graph = reviewed.agent_graph.as_ref().expect("agent graph");
        assert!(graph.nodes.iter().any(|node| node.id == phase.id));
        assert!(graph.evidence.iter().any(|evidence| {
            evidence.node_id == phase.id
                && evidence.reference == "playwright"
                && evidence.summary == "2 passed"
        }));
        assert!(graph.reviews.iter().any(|review| {
            review.node_id == phase.id && review.comment == "accepted after gate"
        }));

        let restored = TaskKernel::open(path.clone()).unwrap();
        let restored_task = restored
            .list()
            .into_iter()
            .find(|t| t.id == task.id)
            .unwrap();
        assert!(restored_task.phases.iter().any(|p| p.id == phase.id));
        assert_eq!(restored.list_agent_graphs().len(), 1);

        let _ = std::fs::remove_file(path);
    }
}
