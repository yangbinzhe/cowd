//! Task control plane for Cowd AI work kernel.
//!
//! This crate owns task lifecycle and persistence. It deliberately does not
//! depend on `runtime` or `gateway`; graph/workgraph data is stored as a typed
//! attachment so upper layers can adapt their own projection without creating
//! dependency cycles.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use ai_strategy::StrategyDecision;
use rusqlite::params;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use storage::{SqliteConnectionFactory, StorageHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
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
    pub const fn as_str(self) -> &'static str {
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
pub struct TaskAuditEvent {
    pub event_type: String,
    pub message: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhaseStatus {
    Running,
    Reviewing,
    Completed,
    Failed,
}

impl TaskPhaseStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Reviewing => "reviewing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPhaseArtifact {
    pub kind: String,
    pub label: String,
    pub value: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPhaseRecord {
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
pub struct TaskRecord<Graph = serde_json::Value> {
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
    pub agent_graph: Option<Graph>,
    #[serde(default)]
    pub strategy: Option<StrategyDecision>,
}

impl TaskRecord<serde_json::Value> {
    pub fn decode_graph<Graph>(self) -> Result<TaskRecord<Graph>, String>
    where
        Graph: DeserializeOwned,
    {
        let agent_graph = self
            .agent_graph
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok(TaskRecord {
            id: self.id,
            objective: self.objective,
            status: self.status,
            current_phase: self.current_phase,
            phases: self.phases,
            yolo_mode: self.yolo_mode,
            failure_count: self.failure_count,
            blocker_reason: self.blocker_reason,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            audit: self.audit,
            agent_graph,
            strategy: self.strategy,
        })
    }
}

impl<Graph> TaskRecord<Graph>
where
    Graph: Serialize,
{
    pub fn encode_graph(self) -> Result<TaskRecord<serde_json::Value>, String> {
        let agent_graph = self
            .agent_graph
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok(TaskRecord {
            id: self.id,
            objective: self.objective,
            status: self.status,
            current_phase: self.current_phase,
            phases: self.phases,
            yolo_mode: self.yolo_mode,
            failure_count: self.failure_count,
            blocker_reason: self.blocker_reason,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            audit: self.audit,
            agent_graph,
            strategy: self.strategy,
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TaskStore {
    tasks: Vec<TaskRecord>,
}

#[derive(Debug, Clone)]
pub struct TaskKernel {
    path: PathBuf,
    storage_handle: Option<StorageHandle>,
    store: Arc<Mutex<TaskStore>>,
}

impl TaskKernel {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let path = normalize_task_db_path(path);
        ensure_schema_path(&path)?;
        let store = load_store_path(&path)?;
        Ok(Self {
            path,
            storage_handle: None,
            store: Arc::new(Mutex::new(store)),
        })
    }

    pub fn open_storage_handle(handle: &StorageHandle) -> Result<Self, String> {
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
    pub fn list(&self) -> Vec<TaskRecord> {
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .clone()
    }

    pub fn list_as<Graph>(&self) -> Result<Vec<TaskRecord<Graph>>, String>
    where
        Graph: DeserializeOwned,
    {
        self.list()
            .into_iter()
            .map(TaskRecord::decode_graph)
            .collect()
    }

    #[must_use]
    pub fn current(&self) -> Option<TaskRecord> {
        self.list().into_iter().rev().find(|task| {
            matches!(
                task.status,
                TaskStatus::Pending | TaskStatus::Running | TaskStatus::Reviewing
            )
        })
    }

    pub fn current_as<Graph>(&self) -> Result<Option<TaskRecord<Graph>>, String>
    where
        Graph: DeserializeOwned,
    {
        self.current().map(TaskRecord::decode_graph).transpose()
    }

    pub fn start_goal(
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
        let task = TaskRecord {
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
            audit: vec![TaskAuditEvent {
                event_type: "started".to_string(),
                message: "task started".to_string(),
                created_at_ms: now,
            }],
            agent_graph: None,
            strategy: None,
        };
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .push(task.clone());
        self.persist()?;
        Ok(task)
    }

    pub fn attach_strategy(
        &self,
        task_id: &str,
        strategy: StrategyDecision,
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
        task.strategy = Some(strategy);
        task.updated_at_ms = now;
        task.audit.push(TaskAuditEvent {
            event_type: "strategy_attached".to_string(),
            message: "strategy decision attached".to_string(),
            created_at_ms: now,
        });
        let updated = task.clone();
        drop(store);
        self.persist()?;
        Ok(updated)
    }

    pub fn transition(
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

    pub fn start_phase(
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
        task.status = TaskStatus::Running;
        task.current_phase = Some(name.clone());
        task.updated_at_ms = now;
        task.phases.push(TaskPhaseRecord {
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
        });
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

    pub fn record_phase_artifact(
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

    pub fn review_phase(
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

    pub fn record_failure(
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

    pub fn upsert_agent_graph<Graph>(
        &self,
        task_id: &str,
        graph: Graph,
    ) -> Result<TaskRecord, String>
    where
        Graph: Serialize,
    {
        let now = now_ms();
        let graph = serde_json::to_value(graph).map_err(|error| error.to_string())?;
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

    pub fn agent_graph_as<Graph>(&self, task_id: &str) -> Result<Option<Graph>, String>
    where
        Graph: DeserializeOwned,
    {
        self.list()
            .into_iter()
            .find(|task| task.id == task_id)
            .and_then(|task| task.agent_graph)
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| error.to_string())
    }

    pub fn list_agent_graphs_as<Graph>(&self) -> Result<Vec<Graph>, String>
    where
        Graph: DeserializeOwned,
    {
        self.list()
            .into_iter()
            .filter_map(|task| task.agent_graph)
            .map(serde_json::from_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())
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

#[cfg(test)]
mod tests {
    use super::{TaskKernel, TaskStatus};
    use storage::StorageHandle;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestGraph {
        id: String,
    }

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cowd-ai-task-{label}-{}.db", uuid::Uuid::new_v4()))
    }

    fn temp_handle(label: &str) -> StorageHandle {
        StorageHandle::sqlite(
            "tasks",
            temp_path(label),
            "ai-task-test",
            "ai-task-test-schema",
        )
    }

    #[test]
    fn task_kernel_persists_and_restores_started_goal() {
        let handle = temp_handle("persist");
        let path = handle.path.clone();
        let kernel = TaskKernel::open_storage_handle(&handle).unwrap();
        let task = kernel.start_goal("Ship v0.9.329", true).unwrap();

        let restored = TaskKernel::open_storage_handle(&handle).unwrap();
        let current = restored.current().expect("current task should restore");
        assert_eq!(current.id, task.id);
        assert_eq!(current.status, TaskStatus::Running);
        assert!(current.yolo_mode);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn blocks_after_three_failures() {
        let handle = temp_handle("blocked");
        let path = handle.path.clone();
        let kernel = TaskKernel::open_storage_handle(&handle).unwrap();
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
    fn records_phase_artifacts_and_review() {
        let handle = temp_handle("phase");
        let path = handle.path.clone();
        let kernel = TaskKernel::open_storage_handle(&handle).unwrap();
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

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn stores_graph_attachment_without_runtime_dependency() {
        let handle = temp_handle("graph");
        let path = handle.path.clone();
        let kernel = TaskKernel::open_storage_handle(&handle).unwrap();
        let task = kernel.start_goal("Attach graph", false).unwrap();
        kernel
            .upsert_agent_graph(
                &task.id,
                TestGraph {
                    id: "graph-1".to_string(),
                },
            )
            .unwrap();

        let restored = TaskKernel::open_storage_handle(&handle).unwrap();
        let graph = restored
            .agent_graph_as::<TestGraph>(&task.id)
            .unwrap()
            .expect("graph should restore");
        assert_eq!(graph.id, "graph-1");
        assert_eq!(
            restored.list_agent_graphs_as::<TestGraph>().unwrap().len(),
            1
        );

        let _ = std::fs::remove_file(path);
    }
}
