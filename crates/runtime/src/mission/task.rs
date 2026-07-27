//! Runtime-owned Task aggregate, persistence contract, and evidence outbox.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub use harness_contract::task::{
    TaskAggregate, TaskCommandOutcome, TaskCommandReceipt, TaskExecutionPolicy, TaskGraphRef,
    TaskPhase, TaskPhaseArtifact, TaskPhaseStatus, TaskPhaseTerminalReceipt, TaskSpec, TaskStatus,
};
use harness_contract::{
    reality::{EvidenceRef, RealityBoundary},
    task::{TaskCreateCommand, TaskPhaseSpec},
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use storage::{SqliteExecutor, StorageHandle};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskEvidenceOutboxRecord {
    pub outbox_id: String,
    pub task_id: String,
    pub revision: u64,
    pub event_kind: String,
    pub status: TaskStatus,
    pub evidence_refs: Vec<EvidenceRef>,
    pub created_at_ms: u64,
    pub projected_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TaskMutation {
    pub expected_revision: u64,
    pub event_kind: String,
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone)]
pub struct TaskMutationResult {
    pub aggregate: TaskAggregate,
    pub receipt: TaskCommandReceipt,
    pub outbox: Option<TaskEvidenceOutboxRecord>,
}

impl TaskMutationResult {
    #[must_use]
    pub fn from_backend_commit(
        aggregate: TaskAggregate,
        mutation: &TaskMutation,
        outbox: Option<TaskEvidenceOutboxRecord>,
    ) -> Self {
        let receipt = task_command_receipt(&aggregate, mutation, outbox.as_ref());
        Self {
            aggregate,
            receipt,
            outbox,
        }
    }
}

/// Backend-neutral, per-task transaction boundary.
///
/// A changed aggregate and its evidence outbox row commit together. The
/// callback may return an unchanged aggregate for an idempotent replay; that
/// path never writes a duplicate outbox row.
pub trait TaskStoreBackend: std::fmt::Debug + Send + Sync {
    fn list(&self) -> Result<Vec<TaskAggregate>, String>;

    fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        Ok(self
            .list()?
            .into_iter()
            .find(|task| task.task_id == task_id))
    }

    fn current(&self) -> Result<Option<TaskAggregate>, String> {
        Ok(self.list()?.into_iter().rev().find(|task| {
            matches!(
                task.status,
                TaskStatus::Pending
                    | TaskStatus::Running
                    | TaskStatus::Reviewing
                    | TaskStatus::Blocked
            )
        }))
    }

    fn mutate_task(
        &self,
        task_id: &str,
        mutation: &TaskMutation,
        updater: &mut dyn FnMut(Option<TaskAggregate>) -> Result<TaskAggregate, String>,
    ) -> Result<TaskMutationResult, String>;

    fn pending_outbox(
        &self,
        task_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskEvidenceOutboxRecord>, String>;

    fn list_outbox(&self) -> Result<Vec<TaskEvidenceOutboxRecord>, String>;

    fn mark_outbox_projected(&self, outbox_id: &str, projected_at_ms: u64) -> Result<(), String>;

    fn export_migration_snapshot(&self) -> Result<TaskStoreSnapshot, String> {
        let snapshot = TaskStoreSnapshot {
            tasks: self.list()?,
            outbox: self.list_outbox()?,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn import_migration_snapshot(&self, _snapshot: &TaskStoreSnapshot) -> Result<(), String> {
        Err("task backend does not support migration import".to_string())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskStoreSnapshot {
    pub tasks: Vec<TaskAggregate>,
    pub outbox: Vec<TaskEvidenceOutboxRecord>,
}

impl TaskStoreSnapshot {
    pub fn validate(&self) -> Result<(), String> {
        let mut task_ids = std::collections::BTreeSet::new();
        for task in &self.tasks {
            validate_task_aggregate_for_backend(task)?;
            if !task_ids.insert(&task.task_id) {
                return Err(format!(
                    "task migration snapshot contains duplicate task id `{}`",
                    task.task_id
                ));
            }
        }
        let mut outbox_ids = std::collections::BTreeSet::new();
        for record in &self.outbox {
            if !outbox_ids.insert(&record.outbox_id) {
                return Err(format!(
                    "task migration snapshot contains duplicate outbox id `{}`",
                    record.outbox_id
                ));
            }
            let task = self
                .tasks
                .iter()
                .find(|task| task.task_id == record.task_id)
                .ok_or_else(|| {
                    format!(
                        "task migration outbox `{}` has no aggregate",
                        record.outbox_id
                    )
                })?;
            if record.revision > task.revision {
                return Err(format!(
                    "task migration outbox `{}` is ahead of aggregate revision",
                    record.outbox_id
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
            .sort_by(|left, right| left.task_id.cmp(&right.task_id));
        canonical
            .outbox
            .sort_by(|left, right| left.outbox_id.cmp(&right.outbox_id));
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
    fn list(&self) -> Result<Vec<TaskAggregate>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_tasks_connection(&connection)
    }

    fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_task_connection(&connection, task_id)
    }

    fn current(&self) -> Result<Option<TaskAggregate>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let record_json = connection
            .query_row(
                "SELECT record_json FROM tasks
                 WHERE status IN ('pending', 'running', 'reviewing', 'blocked')
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

    fn mutate_task(
        &self,
        task_id: &str,
        mutation: &TaskMutation,
        updater: &mut dyn FnMut(Option<TaskAggregate>) -> Result<TaskAggregate, String>,
    ) -> Result<TaskMutationResult, String> {
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
                serde_json::from_str::<TaskAggregate>(&record_json)
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        let next = updater(current.clone())?;
        if current.as_ref() == Some(&next) {
            validate_task_aggregate_for_backend(&next)?;
            let record_json = transaction
                .query_row(
                    "SELECT record_json FROM task_evidence_outbox
                     WHERE task_id=?1 AND revision=?2",
                    params![task_id, to_i64(next.revision, "revision")?],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!(
                        "idempotent task replay `{task_id}` revision {} has no durable outbox",
                        next.revision
                    )
                })?;
            let outbox = serde_json::from_str(&record_json).map_err(|error| error.to_string())?;
            transaction.commit().map_err(|error| error.to_string())?;
            return Ok(TaskMutationResult::from_backend_commit(
                next,
                mutation,
                Some(outbox),
            ));
        }
        let outbox = validate_backend_mutation(task_id, current.as_ref(), &next, mutation)?;
        if outbox.is_none() {
            transaction.commit().map_err(|error| error.to_string())?;
            return Ok(TaskMutationResult::from_backend_commit(
                next, mutation, None,
            ));
        }
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
                    next.task_id,
                    next.status.as_str(),
                    to_i64(next.created_at_ms, "created_at_ms")?,
                    to_i64(next.updated_at_ms, "updated_at_ms")?,
                    serde_json::to_string(&next).map_err(|error| error.to_string())?,
                ],
            )
            .map_err(|error| error.to_string())?;
        let outbox = outbox.ok_or_else(|| {
            format!("task `{task_id}` changed without a durable evidence outbox record")
        })?;
        transaction
            .execute(
                "INSERT INTO task_evidence_outbox
                    (outbox_id, task_id, revision, event_kind, created_at_ms, record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    outbox.outbox_id,
                    outbox.task_id,
                    to_i64(outbox.revision, "revision")?,
                    outbox.event_kind,
                    to_i64(outbox.created_at_ms, "created_at_ms")?,
                    serde_json::to_string(&outbox).map_err(|error| error.to_string())?,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(TaskMutationResult::from_backend_commit(
            next,
            mutation,
            Some(outbox),
        ))
    }

    fn pending_outbox(
        &self,
        task_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_pending_outbox_sqlite(&connection, task_id, limit)
    }

    fn list_outbox(&self) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare(
                "SELECT record_json FROM task_evidence_outbox
                 ORDER BY created_at_ms ASC, outbox_id ASC",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            row.map_err(|error| error.to_string())
                .and_then(|raw| serde_json::from_str(&raw).map_err(|error| error.to_string()))
        })
        .collect()
    }

    fn mark_outbox_projected(&self, outbox_id: &str, projected_at_ms: u64) -> Result<(), String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let raw = connection
            .query_row(
                "SELECT record_json FROM task_evidence_outbox WHERE outbox_id=?1",
                params![outbox_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("task evidence outbox `{outbox_id}` not found"))?;
        let mut record: TaskEvidenceOutboxRecord =
            serde_json::from_str(&raw).map_err(|error| error.to_string())?;
        record.projected_at_ms = Some(projected_at_ms);
        connection
            .execute(
                "UPDATE task_evidence_outbox
                 SET projected_at_ms=?2, record_json=?3 WHERE outbox_id=?1",
                params![
                    outbox_id,
                    to_i64(projected_at_ms, "projected_at_ms")?,
                    serde_json::to_string(&record).map_err(|error| error.to_string())?,
                ],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
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
        let existing_tasks: i64 = transaction
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .map_err(|error| error.to_string())?;
        let existing_outbox: i64 = transaction
            .query_row("SELECT COUNT(*) FROM task_evidence_outbox", [], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())?;
        if existing_tasks != 0 || existing_outbox != 0 {
            return Err("task migration target must be empty".to_string());
        }
        for task in &snapshot.tasks {
            transaction
                .execute(
                    "INSERT INTO tasks (id, status, created_at_ms, updated_at_ms, record_json)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        task.task_id,
                        task.status.as_str(),
                        to_i64(task.created_at_ms, "created_at_ms")?,
                        to_i64(task.updated_at_ms, "updated_at_ms")?,
                        serde_json::to_string(task).map_err(|error| error.to_string())?,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        for record in &snapshot.outbox {
            transaction
                .execute(
                    "INSERT INTO task_evidence_outbox
                        (outbox_id, task_id, revision, event_kind, created_at_ms,
                         projected_at_ms, record_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        record.outbox_id,
                        record.task_id,
                        to_i64(record.revision, "revision")?,
                        record.event_kind,
                        to_i64(record.created_at_ms, "created_at_ms")?,
                        record
                            .projected_at_ms
                            .map(|value| to_i64(value, "projected_at_ms"))
                            .transpose()?,
                        serde_json::to_string(record).map_err(|error| error.to_string())?,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct TaskAggregateService {
    backend: Arc<dyn TaskStoreBackend>,
}

impl TaskAggregateService {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let handle = StorageHandle::sqlite("tasks", path, "task", "task_executor");
        Self::open_storage_handle(&handle)
    }

    pub fn open_storage_handle(handle: &StorageHandle) -> Result<Self, String> {
        let executor = SqliteExecutor::for_handle(handle).map_err(|error| error.to_string())?;
        Ok(Self::from_backend(Arc::new(SqliteTaskStore::new(
            executor,
        )?)))
    }

    #[must_use]
    pub fn from_backend(backend: Arc<dyn TaskStoreBackend>) -> Self {
        Self { backend }
    }

    pub fn list(&self) -> Result<Vec<TaskAggregate>, String> {
        self.backend.list()
    }

    pub fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        self.backend.get(task_id)
    }

    pub fn current(&self) -> Result<Option<TaskAggregate>, String> {
        self.backend.current()
    }

    pub fn create(&self, command: TaskCreateCommand) -> Result<TaskMutationResult, String> {
        validate_required("task_id", &command.task_id)?;
        validate_required("mission_id", &command.mission_id)?;
        validate_required("source_session_id", &command.source_session_id)?;
        validate_required("source_turn_id", &command.source_turn_id)?;
        validate_required("objective", &command.spec.objective)?;
        let now = now_ms();
        let phases = if command.spec.phases.is_empty() {
            vec![phase_from_spec(
                TaskPhaseSpec {
                    name: "implementation".to_string(),
                    objective: command.spec.objective.clone(),
                    dependency_refs: Vec::new(),
                    plan: Vec::new(),
                    acceptance: Vec::new(),
                    test_commands: Vec::new(),
                },
                now,
                TaskPhaseStatus::Running,
            )]
        } else {
            command
                .spec
                .phases
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, phase)| {
                    phase_from_spec(
                        phase,
                        now,
                        if index == 0 {
                            TaskPhaseStatus::Running
                        } else {
                            TaskPhaseStatus::Pending
                        },
                    )
                })
                .collect()
        };
        let aggregate = TaskAggregate {
            task_id: command.task_id.clone(),
            mission_id: command.mission_id,
            source_session_id: command.source_session_id,
            source_turn_id: command.source_turn_id,
            objective: command.spec.objective,
            status: TaskStatus::Running,
            revision: 1,
            current_phase_id: phases.first().map(|phase| phase.phase_id.clone()),
            phases,
            execution_policy: command.spec.execution_policy,
            failure_count: 0,
            blocker_reason: None,
            strategy_ref: None,
            graph_refs: Vec::new(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let mutation = TaskMutation {
            expected_revision: 0,
            event_kind: "task.created".to_string(),
            evidence_refs: command.evidence_refs,
        };
        let task_id = aggregate.task_id.clone();
        self.backend
            .mutate_task(&task_id, &mutation, &mut |current| {
                if let Some(existing) = current {
                    if existing.mission_id == aggregate.mission_id
                        && existing.source_session_id == aggregate.source_session_id
                        && existing.source_turn_id == aggregate.source_turn_id
                        && existing.objective == aggregate.objective
                        && existing.execution_policy == aggregate.execution_policy
                    {
                        return Ok(existing);
                    }
                    return Err(format!(
                        "task id `{task_id}` is already bound to different immutable creation data"
                    ));
                }
                Ok(aggregate.clone())
            })
    }

    pub fn transition(
        &self,
        task_id: &str,
        expected_revision: u64,
        status: TaskStatus,
        evidence_refs: Vec<EvidenceRef>,
        note: impl Into<String>,
    ) -> Result<TaskMutationResult, String> {
        let note = note.into();
        validate_required("transition note", &note)?;
        self.update_existing(
            task_id,
            expected_revision,
            format!("task.{}", status.as_str()),
            evidence_refs,
            move |task| {
                validate_task_transition(task.status, status)?;
                task.status = status;
                task.updated_at_ms = now_ms();
                task.revision = task.revision.saturating_add(1);
                if status == TaskStatus::Running {
                    task.blocker_reason = None;
                }
                if status.is_terminal() {
                    task.current_phase_id = None;
                }
                Ok(())
            },
        )
    }

    pub fn start_phase(
        &self,
        task_id: &str,
        expected_revision: u64,
        spec: TaskPhaseSpec,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<TaskMutationResult, String> {
        validate_required("phase name", &spec.name)?;
        validate_required("phase objective", &spec.objective)?;
        self.update_existing(
            task_id,
            expected_revision,
            "task.phase.started".to_string(),
            evidence_refs,
            move |task| {
                if task.status.is_terminal() {
                    return Err("terminal task cannot start a phase".to_string());
                }
                let now = now_ms();
                let phase = phase_from_spec(spec.clone(), now, TaskPhaseStatus::Running);
                task.current_phase_id = Some(phase.phase_id.clone());
                task.phases.push(phase);
                task.status = TaskStatus::Running;
                task.revision = task.revision.saturating_add(1);
                task.updated_at_ms = now;
                Ok(())
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_phase_artifact(
        &self,
        task_id: &str,
        expected_revision: u64,
        phase_id: &str,
        kind: impl Into<String>,
        label: impl Into<String>,
        value: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<TaskMutationResult, String> {
        let phase_id = phase_id.to_string();
        let kind = kind.into();
        let label = label.into();
        let value = value.into();
        validate_required("artifact label", &label)?;
        validate_required("artifact value", &value)?;
        self.update_existing(
            task_id,
            expected_revision,
            "task.phase.artifact.recorded".to_string(),
            evidence_refs,
            move |task| {
                let now = now_ms();
                let phase = task
                    .phases
                    .iter_mut()
                    .find(|phase| phase.phase_id == phase_id)
                    .ok_or_else(|| format!("phase `{phase_id}` not found"))?;
                phase.artifacts.push(TaskPhaseArtifact {
                    kind: kind.clone(),
                    label: label.clone(),
                    value: value.clone(),
                    created_at_ms: now,
                });
                phase.revision = phase.revision.saturating_add(1);
                phase.updated_at_ms = now;
                task.revision = task.revision.saturating_add(1);
                task.updated_at_ms = now;
                Ok(())
            },
        )
    }

    pub fn review_phase(
        &self,
        task_id: &str,
        expected_revision: u64,
        phase_id: &str,
        result: impl Into<String>,
        completed: bool,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<TaskMutationResult, String> {
        let phase_id = phase_id.to_string();
        let result = result.into();
        validate_required("review result", &result)?;
        self.update_existing(
            task_id,
            expected_revision,
            "task.phase.reviewed".to_string(),
            evidence_refs.clone(),
            move |task| {
                let now = now_ms();
                let phase = task
                    .phases
                    .iter_mut()
                    .find(|phase| phase.phase_id == phase_id)
                    .ok_or_else(|| format!("phase `{phase_id}` not found"))?;
                phase.review_result = Some(result.clone());
                phase.status = if completed {
                    TaskPhaseStatus::Completed
                } else {
                    TaskPhaseStatus::Reviewing
                };
                if completed {
                    phase.terminal_receipt = Some(TaskPhaseTerminalReceipt {
                        outcome: result.clone(),
                        evidence_refs: evidence_refs.clone(),
                        completed_at_ms: now,
                    });
                }
                phase.revision = phase.revision.saturating_add(1);
                phase.updated_at_ms = now;
                task.status = TaskStatus::Reviewing;
                task.current_phase_id = Some(phase.phase_id.clone());
                task.revision = task.revision.saturating_add(1);
                task.updated_at_ms = now;
                Ok(())
            },
        )
    }

    pub fn record_failure(
        &self,
        task_id: &str,
        expected_revision: u64,
        reason: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<TaskMutationResult, String> {
        let reason = reason.into();
        validate_required("failure reason", &reason)?;
        self.update_existing(
            task_id,
            expected_revision,
            "task.failure.recorded".to_string(),
            evidence_refs,
            move |task| {
                if task.status.is_terminal() {
                    return Err("terminal task cannot record another failure".to_string());
                }
                task.failure_count = task.failure_count.saturating_add(1);
                if task.failure_count >= task.execution_policy.max_failures_before_block.max(1) {
                    task.status = TaskStatus::Blocked;
                    task.blocker_reason = Some(reason.clone());
                }
                task.revision = task.revision.saturating_add(1);
                task.updated_at_ms = now_ms();
                Ok(())
            },
        )
    }

    pub fn link_graph(
        &self,
        task_id: &str,
        expected_revision: u64,
        graph_id: impl Into<String>,
        graph_revision: u64,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<TaskMutationResult, String> {
        let graph_id = graph_id.into();
        validate_required("graph_id", &graph_id)?;
        self.update_existing(
            task_id,
            expected_revision,
            "task.graph.linked".to_string(),
            evidence_refs,
            move |task| {
                let now = now_ms();
                if let Some(reference) = task
                    .graph_refs
                    .iter_mut()
                    .find(|reference| reference.graph_id == graph_id)
                {
                    reference.revision = reference.revision.max(graph_revision);
                } else {
                    task.graph_refs.push(TaskGraphRef {
                        graph_id: graph_id.clone(),
                        revision: graph_revision,
                        linked_at_ms: now,
                    });
                }
                task.revision = task.revision.saturating_add(1);
                task.updated_at_ms = now;
                Ok(())
            },
        )
    }

    pub fn pending_outbox(
        &self,
        task_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        self.backend.pending_outbox(task_id, limit)
    }

    pub fn mark_outbox_projected(
        &self,
        outbox_id: &str,
        projected_at_ms: u64,
    ) -> Result<(), String> {
        self.backend
            .mark_outbox_projected(outbox_id, projected_at_ms)
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
        expected_revision: u64,
        event_kind: String,
        evidence_refs: Vec<EvidenceRef>,
        mut updater: impl FnMut(&mut TaskAggregate) -> Result<(), String>,
    ) -> Result<TaskMutationResult, String> {
        let mutation = TaskMutation {
            expected_revision,
            event_kind,
            evidence_refs,
        };
        self.backend
            .mutate_task(task_id, &mutation, &mut |current| {
                let mut task = current.ok_or_else(|| format!("task `{task_id}` not found"))?;
                updater(&mut task)?;
                Ok(task)
            })
    }
}

fn phase_from_spec(spec: TaskPhaseSpec, now: u64, status: TaskPhaseStatus) -> TaskPhase {
    TaskPhase {
        phase_id: format!("phase-{}", uuid::Uuid::new_v4()),
        name: spec.name,
        objective: spec.objective,
        status,
        revision: 1,
        dependency_refs: spec.dependency_refs,
        plan: spec.plan,
        acceptance: spec.acceptance,
        test_commands: spec.test_commands,
        artifacts: Vec::new(),
        review_result: None,
        terminal_receipt: None,
        created_at_ms: now,
        updated_at_ms: now,
    }
}

fn task_command_receipt(
    aggregate: &TaskAggregate,
    mutation: &TaskMutation,
    outbox: Option<&TaskEvidenceOutboxRecord>,
) -> TaskCommandReceipt {
    TaskCommandReceipt {
        task_id: aggregate.task_id.clone(),
        accepted_revision: aggregate.revision,
        status: aggregate.status,
        outbox_id: outbox.map_or_else(
            || format!("task:{}:revision:{}", aggregate.task_id, aggregate.revision),
            |record| record.outbox_id.clone(),
        ),
        evidence_refs: outbox.map_or_else(
            || mutation.evidence_refs.clone(),
            |record| record.evidence_refs.clone(),
        ),
    }
}

pub fn validate_backend_mutation(
    task_id: &str,
    current: Option<&TaskAggregate>,
    next: &TaskAggregate,
    mutation: &TaskMutation,
) -> Result<Option<TaskEvidenceOutboxRecord>, String> {
    validate_task_aggregate_for_backend(next)?;
    if next.task_id != task_id {
        return Err("task backend updater returned another task id".to_string());
    }
    let changed = match current {
        Some(current) => {
            if current.revision != mutation.expected_revision {
                return Err(format!(
                    "stale task revision: expected {}, actual {}",
                    mutation.expected_revision, current.revision
                ));
            }
            if current == next {
                false
            } else {
                if next.revision != current.revision.saturating_add(1) {
                    return Err("task mutation must advance revision exactly once".to_string());
                }
                true
            }
        }
        None => {
            if mutation.expected_revision != 0 || next.revision != 1 {
                return Err("new task requires expected revision 0 and revision 1".to_string());
            }
            true
        }
    };
    if !changed {
        return Ok(None);
    }
    if next.status.is_terminal() && mutation.evidence_refs.is_empty() {
        return Err("terminal task transition requires evidence".to_string());
    }
    let outbox_id = format!("task:{}:revision:{}", next.task_id, next.revision);
    Ok(Some(TaskEvidenceOutboxRecord {
        outbox_id,
        task_id: next.task_id.clone(),
        revision: next.revision,
        event_kind: mutation.event_kind.clone(),
        status: next.status,
        evidence_refs: mutation.evidence_refs.clone(),
        created_at_ms: next.updated_at_ms,
        projected_at_ms: None,
    }))
}

pub fn validate_task_aggregate_for_backend(task: &TaskAggregate) -> Result<(), String> {
    validate_required("task_id", &task.task_id)?;
    validate_required("mission_id", &task.mission_id)?;
    validate_required("source_session_id", &task.source_session_id)?;
    validate_required("source_turn_id", &task.source_turn_id)?;
    validate_required("objective", &task.objective)?;
    if task.revision == 0 {
        return Err("task revision must be positive".to_string());
    }
    Ok(())
}

fn validate_task_transition(from: TaskStatus, to: TaskStatus) -> Result<(), String> {
    let allowed = matches!(
        (from, to),
        (TaskStatus::Pending, TaskStatus::Running)
            | (TaskStatus::Running, TaskStatus::Reviewing)
            | (TaskStatus::Reviewing, TaskStatus::Completed)
            | (
                TaskStatus::Pending | TaskStatus::Running | TaskStatus::Reviewing,
                TaskStatus::Blocked
            )
            | (TaskStatus::Blocked, TaskStatus::Running)
            | (
                TaskStatus::Pending
                    | TaskStatus::Running
                    | TaskStatus::Reviewing
                    | TaskStatus::Blocked,
                TaskStatus::Cancelled | TaskStatus::Failed
            )
    );
    if !allowed {
        return Err(format!(
            "illegal task transition {} -> {}",
            from.as_str(),
            to.as_str()
        ));
    }
    Ok(())
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
            ON tasks(status, created_at_ms DESC, id DESC);
        CREATE TABLE IF NOT EXISTS task_evidence_outbox (
            outbox_id TEXT PRIMARY KEY NOT NULL,
            task_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            event_kind TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            projected_at_ms INTEGER,
            record_json TEXT NOT NULL,
            UNIQUE(task_id, revision),
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_task_evidence_outbox_pending
            ON task_evidence_outbox(projected_at_ms, created_at_ms, outbox_id);",
    )
    .map_err(|error| error.to_string())
}

fn load_tasks_connection(conn: &rusqlite::Connection) -> Result<Vec<TaskAggregate>, String> {
    let mut statement = conn
        .prepare("SELECT record_json FROM tasks ORDER BY created_at_ms ASC, id ASC")
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?;
    rows.map(|row| {
        row.map_err(|error| error.to_string())
            .and_then(|raw| serde_json::from_str(&raw).map_err(|error| error.to_string()))
    })
    .collect()
}

fn load_task_connection(
    conn: &rusqlite::Connection,
    task_id: &str,
) -> Result<Option<TaskAggregate>, String> {
    conn.query_row(
        "SELECT record_json FROM tasks WHERE id=?1",
        params![task_id],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(|error| error.to_string())?
    .map(|raw| serde_json::from_str(&raw).map_err(|error| error.to_string()))
    .transpose()
}

fn load_pending_outbox_sqlite(
    conn: &rusqlite::Connection,
    task_id: Option<&str>,
    limit: usize,
) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
    let limit = i64::try_from(limit.min(i64::MAX as usize)).unwrap_or(i64::MAX);
    let mut records = Vec::new();
    if let Some(task_id) = task_id {
        let mut statement = conn
            .prepare(
                "SELECT record_json FROM task_evidence_outbox
                 WHERE projected_at_ms IS NULL AND task_id=?1
                 ORDER BY revision ASC LIMIT ?2",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(params![task_id, limit], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?;
        for row in rows {
            records.push(
                serde_json::from_str(&row.map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())?,
            );
        }
    } else {
        let mut statement = conn
            .prepare(
                "SELECT record_json FROM task_evidence_outbox
                 WHERE projected_at_ms IS NULL
                 ORDER BY created_at_ms ASC, outbox_id ASC LIMIT ?1",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(params![limit], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?;
        for row in rows {
            records.push(
                serde_json::from_str(&row.map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())?,
            );
        }
    }
    Ok(records)
}

fn validate_required(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("task `{field}` is required"));
    }
    Ok(())
}

fn to_i64(value: u64, field: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("task `{field}` exceeds i64"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[must_use]
pub fn synthetic_evidence(ref_type: &str, id: impl Into<String>) -> EvidenceRef {
    EvidenceRef::new(ref_type, id, RealityBoundary::Observed).with_source("runtime.task")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_handle(label: &str) -> StorageHandle {
        StorageHandle::sqlite(
            "tasks",
            std::env::temp_dir().join(format!(
                "cowd-runtime-task-{label}-{}.db",
                uuid::Uuid::new_v4()
            )),
            "runtime-task-test",
            "runtime-task-test-schema",
        )
    }

    fn create(service: &TaskAggregateService, id: &str) -> TaskMutationResult {
        service
            .create(TaskCreateCommand {
                task_id: id.to_string(),
                mission_id: "mission-1".to_string(),
                source_session_id: "session-a".to_string(),
                source_turn_id: "turn-a".to_string(),
                spec: TaskSpec {
                    objective: "prove canonical task state".to_string(),
                    phases: Vec::new(),
                    execution_policy: TaskExecutionPolicy {
                        yolo_mode: true,
                        max_failures_before_block: 3,
                    },
                },
                evidence_refs: vec![synthetic_evidence("task_input", "input-1")],
            })
            .expect("create task")
    }

    #[test]
    fn state_and_outbox_commit_with_one_revision() {
        let handle = temp_handle("outbox");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let created = create(&service, "task-outbox");
        assert_eq!(created.aggregate.revision, 1);
        assert_eq!(
            created.outbox.as_ref().map(|record| record.revision),
            Some(1)
        );
        assert_eq!(created.receipt.accepted_revision, 1);
        assert_eq!(
            created.receipt.outbox_id,
            created.outbox.as_ref().expect("outbox").outbox_id
        );
        assert_eq!(
            service
                .pending_outbox(Some("task-outbox"), 10)
                .expect("pending outbox")
                .len(),
            1
        );
    }

    #[test]
    fn create_replay_returns_the_original_durable_receipt() {
        let handle = temp_handle("create-replay");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let first = create(&service, "task-create-replay");
        let replay = create(&service, "task-create-replay");

        assert_eq!(replay.aggregate, first.aggregate);
        assert_eq!(replay.receipt, first.receipt);
        assert_eq!(replay.outbox, first.outbox);
        assert_eq!(
            service
                .pending_outbox(Some("task-create-replay"), 10)
                .expect("pending outbox")
                .len(),
            1
        );
    }

    #[test]
    fn stale_revision_and_terminal_without_evidence_are_rejected() {
        let handle = temp_handle("cas");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let created = create(&service, "task-cas");
        assert!(service
            .transition(
                "task-cas",
                created.aggregate.revision + 1,
                TaskStatus::Reviewing,
                Vec::new(),
                "review"
            )
            .is_err());
        let reviewing = service
            .transition(
                "task-cas",
                created.aggregate.revision,
                TaskStatus::Reviewing,
                Vec::new(),
                "review",
            )
            .expect("review transition");
        assert!(service
            .transition(
                "task-cas",
                reviewing.aggregate.revision,
                TaskStatus::Completed,
                Vec::new(),
                "complete"
            )
            .is_err());
    }

    #[test]
    fn graph_link_is_a_reference_not_a_projection_copy() {
        let handle = temp_handle("graph-ref");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let created = create(&service, "task-graph");
        let linked = service
            .link_graph(
                "task-graph",
                created.aggregate.revision,
                "graph-1",
                7,
                vec![synthetic_evidence("execution_graph", "graph-1")],
            )
            .expect("link graph");
        assert_eq!(linked.aggregate.graph_refs[0].graph_id, "graph-1");
        let value = serde_json::to_value(linked.aggregate).expect("serialize task");
        assert!(value.get("execution_graph").is_none());
    }

    #[test]
    fn thirty_two_same_revision_writers_commit_exactly_once() {
        use std::sync::{Arc, Barrier};

        let handle = temp_handle("concurrent-cas");
        let service =
            Arc::new(TaskAggregateService::open_storage_handle(&handle).expect("task service"));
        let created = create(service.as_ref(), "task-concurrent-cas");
        let expected_revision = created.aggregate.revision;
        let barrier = Arc::new(Barrier::new(32));
        let writers = (0..32)
            .map(|writer| {
                let service = Arc::clone(&service);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    service.record_failure(
                        "task-concurrent-cas",
                        expected_revision,
                        format!("writer-{writer}"),
                        Vec::new(),
                    )
                })
            })
            .collect::<Vec<_>>();
        let results = writers
            .into_iter()
            .map(|writer| writer.join().expect("writer joins"))
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    result
                        .as_ref()
                        .is_err_and(|error| error.contains("stale task revision"))
                })
                .count(),
            31
        );
        let task = service
            .get("task-concurrent-cas")
            .expect("task lookup")
            .expect("task exists");
        assert_eq!(task.revision, expected_revision + 1);
        assert_eq!(task.failure_count, 1);
        assert_eq!(
            service
                .pending_outbox(Some("task-concurrent-cas"), 64)
                .expect("outbox")
                .len(),
            2,
            "create and the single winning mutation each own one outbox row"
        );
    }

    #[test]
    fn outbox_insert_failure_rolls_back_the_aggregate_revision() {
        let handle = temp_handle("outbox-rollback");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let created = create(&service, "task-outbox-rollback");
        let executor = SqliteExecutor::for_handle(&handle).expect("task executor");
        executor
            .checkout()
            .expect("task connection")
            .execute_batch(
                "CREATE TRIGGER fail_task_outbox_insert
                 BEFORE INSERT ON task_evidence_outbox
                 BEGIN
                   SELECT RAISE(ABORT, 'injected task outbox failure');
                 END;",
            )
            .expect("failure trigger");

        let error = service
            .record_failure(
                "task-outbox-rollback",
                created.aggregate.revision,
                "injected failure",
                Vec::new(),
            )
            .expect_err("outbox failure must abort the mutation");
        assert!(error.contains("injected task outbox failure"));
        let restored = service
            .get("task-outbox-rollback")
            .expect("task lookup")
            .expect("task exists");
        assert_eq!(restored.revision, created.aggregate.revision);
        assert_eq!(restored.failure_count, 0);
        assert_eq!(
            service
                .pending_outbox(Some("task-outbox-rollback"), 10)
                .expect("outbox")
                .len(),
            1,
            "only the original create outbox remains"
        );
    }

    #[test]
    fn migration_snapshot_preserves_projected_outbox_history() {
        let handle = temp_handle("projected-outbox-snapshot");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let created = create(&service, "task-projected-outbox-snapshot");
        let outbox_id = created.receipt.outbox_id;
        service
            .mark_outbox_projected(&outbox_id, 42)
            .expect("mark projected");

        assert!(service
            .pending_outbox(Some("task-projected-outbox-snapshot"), 10)
            .expect("pending outbox")
            .is_empty());
        let snapshot = service
            .export_migration_snapshot()
            .expect("migration snapshot");
        assert_eq!(snapshot.outbox.len(), 1);
        assert_eq!(snapshot.outbox[0].outbox_id, outbox_id);
        assert_eq!(snapshot.outbox[0].projected_at_ms, Some(42));
    }

    #[test]
    fn task_can_be_loaded_from_another_session_context() {
        let handle = temp_handle("cross-session");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        create(&service, "task-cross-session");
        let loaded = service
            .get("task-cross-session")
            .expect("read task")
            .expect("task exists");
        assert_eq!(loaded.source_session_id, "session-a");
        assert_eq!(loaded.mission_id, "mission-1");
    }
}
