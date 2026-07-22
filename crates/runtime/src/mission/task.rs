//! Runtime task lifecycle, phase audit, and task persistence.
//!
//! Gateway may cache canonical execution projections for UI rendering, but the
//! task lifecycle itself belongs to the runtime harness and graph state is
//! committed exclusively by Runtime's execution services.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use harness_contract::{execution_graph::ExecutionGraphProjection, strategy::StrategyDecision};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use storage::{SqliteExecutor, StorageHandle};

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
pub struct TaskRecord {
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
    pub execution_graph: Option<ExecutionGraphProjection>,
    #[serde(default)]
    pub strategy: Option<StrategyDecision>,
}

/// Complete persistence contract for the Task control plane.
///
/// The updater executes inside the backend's transaction for one task. A
/// backend must either commit the returned record as one unit or leave the
/// prior record unchanged. This keeps lifecycle decisions in `TaskKernel`
/// while letting SQLite and PostgreSQL enforce their own locking semantics.
pub trait TaskStoreBackend: std::fmt::Debug + Send + Sync {
    fn list(&self) -> Result<Vec<TaskRecord>, String>;

    fn get(&self, task_id: &str) -> Result<Option<TaskRecord>, String> {
        Ok(self.list()?.into_iter().find(|task| task.id == task_id))
    }

    fn current(&self) -> Result<Option<TaskRecord>, String> {
        Ok(self.list()?.into_iter().rev().find(|task| {
            matches!(
                task.status,
                TaskStatus::Pending | TaskStatus::Running | TaskStatus::Reviewing
            )
        }))
    }

    fn update_task(
        &self,
        task_id: &str,
        updater: &mut dyn FnMut(Option<TaskRecord>) -> Result<TaskRecord, String>,
    ) -> Result<TaskRecord, String>;

    fn export_migration_snapshot(&self) -> Result<TaskStoreSnapshot, String> {
        let snapshot = TaskStoreSnapshot {
            tasks: self.list()?,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn import_migration_snapshot(&self, _snapshot: &TaskStoreSnapshot) -> Result<(), String> {
        Err("task backend does not support migration import".to_string())
    }
}

/// Backend-neutral Task control-plane data used only for explicit one-time
/// backend migration. It is deliberately a whole-record snapshot because the
/// task record itself is the stable Gateway/TUI projection contract.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskStoreSnapshot {
    pub tasks: Vec<TaskRecord>,
}

impl TaskStoreSnapshot {
    pub fn validate(&self) -> Result<(), String> {
        let mut ids = std::collections::BTreeSet::new();
        for task in &self.tasks {
            if task.id.trim().is_empty() {
                return Err("task migration snapshot contains an empty task id".to_string());
            }
            if !ids.insert(&task.id) {
                return Err(format!(
                    "task migration snapshot contains duplicate task id `{}`",
                    task.id
                ));
            }
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<String, String> {
        self.validate()?;
        let mut canonical = self.clone();
        canonical
            .tasks
            .sort_by(|left, right| left.id.cmp(&right.id));
        let payload = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
        use sha2::{Digest, Sha256};
        Ok(format!("{:x}", Sha256::digest(payload)))
    }
}

#[derive(Debug, Clone)]
struct SqliteTaskStore {
    executor: SqliteExecutor,
}

impl SqliteTaskStore {
    fn new(executor: SqliteExecutor) -> Result<Self, String> {
        let connection = executor.checkout().map_err(|error| error.to_string())?;
        ensure_schema_connection(&connection)?;
        Ok(Self { executor })
    }
}

impl TaskStoreBackend for SqliteTaskStore {
    fn list(&self) -> Result<Vec<TaskRecord>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_tasks_connection(&connection)
    }

    fn get(&self, task_id: &str) -> Result<Option<TaskRecord>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_task_connection(&connection, task_id)
    }

    fn current(&self) -> Result<Option<TaskRecord>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let record_json = connection
            .query_row(
                "SELECT record_json FROM tasks
                 WHERE status IN ('pending', 'running', 'reviewing')
                 ORDER BY created_at_ms DESC, id DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        record_json
            .map(|value| serde_json::from_str(&value).map_err(|error| error.to_string()))
            .transpose()
    }

    fn update_task(
        &self,
        task_id: &str,
        updater: &mut dyn FnMut(Option<TaskRecord>) -> Result<TaskRecord, String>,
    ) -> Result<TaskRecord, String> {
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let current = transaction
            .query_row(
                "SELECT record_json FROM tasks WHERE id = ?1",
                params![task_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?
            .map(|record_json| {
                serde_json::from_str::<TaskRecord>(&record_json).map_err(|error| error.to_string())
            })
            .transpose()?;
        let next = updater(current)?;
        validate_updated_task(task_id, &next)?;
        transaction
            .execute(
                "INSERT INTO tasks (id, status, created_at_ms, updated_at_ms, record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(id) DO UPDATE SET
                    status=excluded.status,
                    created_at_ms=excluded.created_at_ms,
                    updated_at_ms=excluded.updated_at_ms,
                    record_json=excluded.record_json",
                params![
                    next.id,
                    next.status.as_str(),
                    next.created_at_ms as i64,
                    next.updated_at_ms as i64,
                    serde_json::to_string(&next).map_err(|error| error.to_string())?,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(next)
    }

    fn import_migration_snapshot(&self, snapshot: &TaskStoreSnapshot) -> Result<(), String> {
        snapshot.validate()?;
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let existing: i64 = transaction
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .map_err(|error| error.to_string())?;
        if existing != 0 {
            return Err("task migration target must be empty".to_string());
        }
        let mut statement = transaction
            .prepare(
                "INSERT INTO tasks (id, status, created_at_ms, updated_at_ms, record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .map_err(|error| error.to_string())?;
        for task in &snapshot.tasks {
            statement
                .execute(params![
                    task.id,
                    task.status.as_str(),
                    task.created_at_ms as i64,
                    task.updated_at_ms as i64,
                    serde_json::to_string(task).map_err(|error| error.to_string())?,
                ])
                .map_err(|error| error.to_string())?;
        }
        drop(statement);
        transaction.commit().map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct TaskKernel {
    backend: Arc<dyn TaskStoreBackend>,
}

impl TaskKernel {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let path = normalize_task_db_path(path);
        let handle = StorageHandle::sqlite("tasks", path, "task", "task_executor");
        Self::from_executor(SqliteExecutor::for_handle(&handle).map_err(|error| error.to_string())?)
    }

    pub fn open_storage_handle(handle: &StorageHandle) -> Result<Self, String> {
        Self::from_executor(SqliteExecutor::for_handle(handle).map_err(|error| error.to_string())?)
    }

    fn from_executor(executor: SqliteExecutor) -> Result<Self, String> {
        Ok(Self::from_backend(Arc::new(SqliteTaskStore::new(
            executor,
        )?)))
    }

    #[must_use]
    pub fn from_backend(backend: Arc<dyn TaskStoreBackend>) -> Self {
        Self { backend }
    }

    pub fn list(&self) -> Result<Vec<TaskRecord>, String> {
        self.backend.list()
    }

    pub fn current(&self) -> Result<Option<TaskRecord>, String> {
        self.backend.current()
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
            execution_graph: None,
            strategy: None,
        };
        let task_id = task.id.clone();
        self.backend.update_task(&task_id, &mut |current| {
            if current.is_some() {
                return Err(format!("generated task id {task_id} already exists"));
            }
            Ok(task.clone())
        })
    }

    pub fn start_goal_idempotent(
        &self,
        task_id: &str,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        let task_id = task_id.trim();
        let objective = objective.into();
        if task_id.is_empty() || objective.trim().is_empty() {
            return Err("task id and objective are required".to_string());
        }
        let now = now_ms();
        let task = TaskRecord {
            id: task_id.to_string(),
            objective: objective.clone(),
            status: TaskStatus::Running,
            current_phase: Some("implementation".to_string()),
            phases: vec![TaskPhaseRecord {
                id: format!("phase-{task_id}-implementation"),
                name: "implementation".to_string(),
                objective,
                plan: Vec::new(),
                acceptance: Vec::new(),
                test_commands: Vec::new(),
                artifacts: Vec::new(),
                review_result: None,
                status: TaskPhaseStatus::Running,
                created_at_ms: now,
                updated_at_ms: now,
            }],
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
            execution_graph: None,
            strategy: None,
        };
        self.backend.update_task(task_id, &mut |current| {
            if let Some(existing) = current {
                if existing.objective != task.objective || existing.yolo_mode != task.yolo_mode {
                    return Err(format!(
                        "task id {task_id} is already bound to another governed objective"
                    ));
                }
                return Ok(existing);
            }
            Ok(task.clone())
        })
    }

    pub fn attach_strategy(
        &self,
        task_id: &str,
        strategy: StrategyDecision,
    ) -> Result<TaskRecord, String> {
        self.update_existing(task_id, move |task| {
            let now = now_ms();
            task.strategy = Some(strategy.clone());
            task.updated_at_ms = now;
            task.audit.push(TaskAuditEvent {
                event_type: "strategy_attached".to_string(),
                message: "strategy decision attached".to_string(),
                created_at_ms: now,
            });
            Ok(())
        })
    }

    pub fn transition(
        &self,
        task_id: &str,
        status: TaskStatus,
        phase: Option<String>,
        message: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        let message = message.into();
        self.update_existing(task_id, move |task| {
            let now = now_ms();
            task.status = status;
            task.current_phase = phase.clone();
            task.updated_at_ms = now;
            task.audit.push(TaskAuditEvent {
                event_type: format!("{:?}", status).to_lowercase(),
                message: message.clone(),
                created_at_ms: now,
            });
            Ok(())
        })
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

        self.update_existing(task_id, move |task| {
            task.status = TaskStatus::Running;
            task.current_phase = Some(name.clone());
            task.updated_at_ms = now;
            task.phases.push(TaskPhaseRecord {
                id: format!("phase-{}", uuid::Uuid::new_v4()),
                name: name.clone(),
                objective: objective.clone(),
                plan: plan.clone(),
                acceptance: acceptance.clone(),
                test_commands: test_commands.clone(),
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
            Ok(())
        })
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

        let phase_id = phase_id.to_string();
        self.update_existing(task_id, move |task| {
            let phase = task
                .phases
                .iter_mut()
                .find(|phase| phase.id == phase_id)
                .ok_or_else(|| format!("phase {phase_id} not found"))?;
            phase.artifacts.push(TaskPhaseArtifact {
                kind: kind.clone(),
                label: label.clone(),
                value: value.clone(),
                created_at_ms: now,
            });
            phase.updated_at_ms = now;
            task.updated_at_ms = now;
            task.audit.push(TaskAuditEvent {
                event_type: "phase_artifact".to_string(),
                message: format!("phase artifact recorded: {label}"),
                created_at_ms: now,
            });
            Ok(())
        })
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

        let phase_id = phase_id.to_string();
        self.update_existing(task_id, move |task| {
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
                message: result.clone(),
                created_at_ms: now,
            });
            Ok(())
        })
    }

    pub fn record_failure(
        &self,
        task_id: &str,
        reason: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        let reason = reason.into();
        let now = now_ms();
        self.update_existing(task_id, move |task| {
            task.failure_count += 1;
            task.updated_at_ms = now;
            if task.failure_count >= 3 {
                task.status = TaskStatus::Blocked;
                task.blocker_reason = Some(reason.clone());
                task.current_phase = Some("blocked".to_string());
            }
            task.audit.push(TaskAuditEvent {
                event_type: "failure".to_string(),
                message: reason.clone(),
                created_at_ms: now,
            });
            Ok(())
        })
    }

    /// Cache a read-only projection produced by the canonical execution store.
    ///
    /// This method never advances graph state. Callers must first commit through
    /// `ExecutionCommitService` or `ExecutionGraphHost` and only then persist the
    /// returned projection for task-list rendering.
    pub fn record_execution_graph_projection(
        &self,
        task_id: &str,
        projection: ExecutionGraphProjection,
    ) -> Result<TaskRecord, String> {
        self.update_existing(task_id, move |task| {
            let now = now_ms();
            task.execution_graph = Some(projection.clone());
            task.updated_at_ms = now;
            task.audit.push(TaskAuditEvent {
                event_type: "execution_graph_projected".to_string(),
                message: "execution graph projection refreshed".to_string(),
                created_at_ms: now,
            });
            Ok(())
        })
    }

    #[must_use]
    pub fn execution_graph(
        &self,
        task_id: &str,
    ) -> Result<Option<ExecutionGraphProjection>, String> {
        Ok(self
            .backend
            .get(task_id)?
            .and_then(|task| task.execution_graph))
    }

    pub fn execution_graphs(&self) -> Result<Vec<ExecutionGraphProjection>, String> {
        Ok(self
            .list()?
            .into_iter()
            .filter_map(|task| task.execution_graph)
            .collect())
    }

    pub fn export_migration_snapshot(&self) -> Result<TaskStoreSnapshot, String> {
        self.backend.export_migration_snapshot()
    }

    pub fn import_migration_snapshot(&self, snapshot: &TaskStoreSnapshot) -> Result<(), String> {
        self.backend.import_migration_snapshot(snapshot)
    }

    fn update_existing(
        &self,
        task_id: &str,
        mut mutation: impl FnMut(&mut TaskRecord) -> Result<(), String>,
    ) -> Result<TaskRecord, String> {
        self.backend.update_task(task_id, &mut |current| {
            let mut task = current.ok_or_else(|| format!("task {task_id} not found"))?;
            mutation(&mut task)?;
            Ok(task)
        })
    }
}

fn validate_updated_task(task_id: &str, task: &TaskRecord) -> Result<(), String> {
    if task_id.trim().is_empty() || task.id.trim().is_empty() || task.id != task_id {
        return Err("task backend updater returned a record for another task id".to_string());
    }
    Ok(())
}

fn normalize_task_db_path(path: PathBuf) -> PathBuf {
    if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
        return path.with_extension("db");
    }
    path
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
            ON tasks(status, updated_at_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_tasks_status_created
            ON tasks(status, created_at_ms DESC, id DESC);",
    )
    .map_err(|e| e.to_string())
}

fn load_tasks_connection(conn: &rusqlite::Connection) -> Result<Vec<TaskRecord>, String> {
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
    Ok(tasks)
}

fn load_task_connection(
    conn: &rusqlite::Connection,
    task_id: &str,
) -> Result<Option<TaskRecord>, String> {
    let record_json = conn
        .query_row(
            "SELECT record_json FROM tasks WHERE id = ?1",
            params![task_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    record_json
        .map(|value| serde_json::from_str(&value).map_err(|error| error.to_string()))
        .transpose()
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
    use harness_contract::execution_graph::ExecutionGraphProjection;
    use storage::StorageHandle;

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "cowd-runtime-task-{label}-{}.db",
            uuid::Uuid::new_v4()
        ))
    }

    fn temp_handle(label: &str) -> StorageHandle {
        StorageHandle::sqlite(
            "tasks",
            temp_path(label),
            "runtime-task-test",
            "runtime-task-test-schema",
        )
    }

    #[test]
    fn task_kernel_persists_and_restores_started_goal() {
        let handle = temp_handle("persist");
        let path = handle.path.clone();
        let kernel = TaskKernel::open_storage_handle(&handle).unwrap();
        let task = kernel.start_goal("Ship v0.9.329", true).unwrap();

        let restored = TaskKernel::open_storage_handle(&handle).unwrap();
        let current = restored
            .current()
            .expect("task backend should read")
            .expect("current task should restore");
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
    fn stores_read_only_execution_projection() {
        let handle = temp_handle("graph");
        let path = handle.path.clone();
        let kernel = TaskKernel::open_storage_handle(&handle).unwrap();
        let task = kernel.start_goal("Attach graph", false).unwrap();
        kernel
            .record_execution_graph_projection(
                &task.id,
                ExecutionGraphProjection {
                    graph_id: "graph-1".to_string(),
                    revision: 4,
                    objective: "Attach graph".to_string(),
                    parent_execution: None,
                    nodes: Vec::new(),
                    edges: Vec::new(),
                    commit_cursor: 9,
                    terminal_result_ref: None,
                },
            )
            .unwrap();

        let restored = TaskKernel::open_storage_handle(&handle).unwrap();
        let graph = restored
            .execution_graph(&task.id)
            .expect("task backend should read")
            .expect("graph should restore");
        assert_eq!(graph.graph_id, "graph-1");
        assert_eq!(
            restored
                .execution_graphs()
                .expect("task backend should read")
                .len(),
            1
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn migration_snapshot_round_trip_preserves_digest_and_rejects_nonempty_target() {
        let source_handle = temp_handle("snapshot-source");
        let source_path = source_handle.path.clone();
        let source = TaskKernel::open_storage_handle(&source_handle).unwrap();
        let task = source
            .start_goal_idempotent("task-snapshot", "Migrate task control plane", true)
            .unwrap();
        source
            .start_phase(
                &task.id,
                "verification",
                "prove task migration",
                vec!["copy snapshot".to_string()],
                vec!["digest equality".to_string()],
                vec!["cargo test -p runtime task::tests".to_string()],
            )
            .unwrap();
        let snapshot = source.export_migration_snapshot().unwrap();
        let source_digest = snapshot.canonical_digest().unwrap();

        let target_handle = temp_handle("snapshot-target");
        let target_path = target_handle.path.clone();
        let target = TaskKernel::open_storage_handle(&target_handle).unwrap();
        target.import_migration_snapshot(&snapshot).unwrap();
        assert_eq!(
            target
                .export_migration_snapshot()
                .unwrap()
                .canonical_digest()
                .unwrap(),
            source_digest
        );
        assert!(target.import_migration_snapshot(&snapshot).is_err());

        let _ = std::fs::remove_file(source_path);
        let _ = std::fs::remove_file(target_path);
    }
}
